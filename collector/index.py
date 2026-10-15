"""Disposable index of append-only records. Git records, never this cache, establish coverage."""
import gzip
import json
import sqlite3
import time
from pathlib import Path

from . import collect


class Index:
    def __init__(self, path, records):
        self.records = Path(records)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS files (path TEXT PRIMARY KEY, size INTEGER);
            CREATE TABLE IF NOT EXISTS runs (
              repo TEXT, id INTEGER, attempt INTEGER, created TEXT, workflow TEXT,
              status TEXT, updated TEXT, source TEXT, jobs TEXT, rank INTEGER DEFAULT -1,
              jobs_done INTEGER DEFAULT 0, detail_done INTEGER DEFAULT 0,
              retry_after REAL DEFAULT 0,
              required INTEGER, build INTEGER, batch INTEGER, head TEXT,
              PRIMARY KEY(repo,id,attempt));
            CREATE INDEX IF NOT EXISTS pending_jobs ON runs(jobs_done,build,created);
            CREATE INDEX IF NOT EXISTS pending_details ON runs(detail_done,build,created);
            CREATE TABLE IF NOT EXISTS batches (
              repo TEXT, batch INTEGER, head TEXT, members TEXT,
              PRIMARY KEY(repo,batch,head));
        ''')

    def close(self):
        self.db.close()

    @staticmethod
    def source(record):
        """Enough API provenance for deferred enrichment, without a second run lookup."""
        if record.get('_listing'):
            return record['_listing']
        return {'id': record['run_id'], 'run_attempt': record['run_attempt'],
                'path': record['workflow'], 'event': record['event'],
                'head_sha': record['head_sha'], 'head_branch': record.get('head_branch'),
                'head_repository': {'full_name': record.get('head_repo')},
                'repository': {'full_name': record.get('workflow_repository')},
                'referenced_workflows': record.get('referenced_workflows', []),
                'display_title': record.get('title'), 'created_at': record['created_at'],
                'updated_at': record.get('updated_at'), 'conclusion': record.get('conclusion'),
                'status': 'completed', 'actor': {'login': record.get('actor')},
                'run_started_at': record.get('run_started_at')}

    def ingest(self, kind, r):
        if kind == 'merge-members':
            self.db.execute('INSERT OR REPLACE INTO batches VALUES (?,?,?,?)',
                            (r['repo'], r['batch_id'], r['head_sha'], json.dumps(r['members'])))
            return
        source = self.source(r)
        workflow = r['workflow']
        build = workflow in collect.BUILD_WORKFLOWS
        required = r.get('conclusion') != 'skipped' and collect.needs_jobs(source, 'all')
        # Historical backfill/sampling decisions remain valid. Newly deferred
        # jobs are explicit inventory records, not silently abandoned samples.
        jobs_done = kind != 'inventory' or not required
        detail_done = (kind == 'runs' or r.get('conclusion') == 'skipped'
                       or not (build and r['repo'] in collect.DETAIL_REPOS))
        tested = r.get('tested') or {}
        self.db.execute('''INSERT INTO runs
            (repo,id,attempt,created,workflow,status,updated,source,required,build,batch,head)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(repo,id,attempt) DO UPDATE SET
            source=CASE WHEN excluded.updated >= COALESCE(runs.updated,'') THEN excluded.source ELSE runs.source END,
            status=CASE WHEN excluded.updated >= COALESCE(runs.updated,'') THEN excluded.status ELSE runs.status END,
            updated=MAX(COALESCE(runs.updated,''),COALESCE(excluded.updated,'')),
            batch=COALESCE(excluded.batch,runs.batch),head=COALESCE(excluded.head,runs.head)
            ''', (r['repo'], r['run_id'], r['run_attempt'], r['created_at'], workflow,
                  source.get('status', 'completed'), r.get('updated_at') or r['created_at'],
                  json.dumps(source), int(required), int(build), tested.get('batch_id'),
                  tested.get('head_sha') if tested.get('batch_id') else None))
        key = (r['repo'], r['run_id'], r['run_attempt'])
        rank = {'inventory': 0, 'jobs': 1, 'runs': 2}[kind]
        self.db.execute('''UPDATE runs SET jobs=?,rank=?
            WHERE repo=? AND id=? AND attempt=? AND rank <= ?''',
                        (json.dumps(r), rank, *key, rank))
        self.db.execute('''UPDATE runs SET jobs_done=MAX(jobs_done,?),detail_done=MAX(detail_done,?)
            WHERE repo=? AND id=? AND attempt=?''', (int(jobs_done), int(detail_done), *key))
        if tested.get('batch_members') and tested.get('batch_id'):
            self.ingest('merge-members', {'repo': r['repo'], 'batch_id': tested['batch_id'],
                        'head_sha': tested['head_sha'], 'members': tested['batch_members']})

    def sync(self):
        """Read only new/grown files. Reject cache entries absent from this checkout."""
        kinds = ('inventory', 'jobs', 'runs', 'merge-members')
        current = {str(p.relative_to(self.records)): (p, p.stat().st_size)
                   for kind in kinds for p in (self.records/kind).rglob('*.ndjson.gz')}
        old = dict(self.db.execute('SELECT path,size FROM files'))
        if any(path not in current or current[path][1] < size for path, size in old.items()):
            # A cache from a newer/different checkout cannot lend it records.
            for table in ('files', 'runs', 'batches'):
                self.db.execute(f'DELETE FROM {table}')
            old = {}
        read = 0
        for name, (path, size) in sorted(current.items()):
            if old.get(name) == size:
                continue
            with gzip.open(path, 'rt') as source:
                for line in source:
                    self.ingest(name.split('/')[0], json.loads(line))
            self.db.execute('INSERT OR REPLACE INTO files VALUES (?,?)', (name, size))
            read += 1
        self.db.commit()
        return read

    def pending(self, *, build, detail=False, recent=None, limit=32):
        done = 'detail_done' if detail else 'jobs_done'
        condition = 'AND jobs_done=1' if detail else ''
        args = [int(build)]
        if recent:
            condition += ' AND created >= ?'
            args.append(recent)
        # Every attempt has its own work item. Never refetch previous attempts'
        # job pages when a new attempt appears.
        rows = self.db.execute(f'''SELECT repo,source,jobs FROM runs
            WHERE {done}=0 AND build=? AND status='completed' {condition}
              AND retry_after <= ?
            ORDER BY created,id,attempt LIMIT ?''', (*args, time.time(), limit)).fetchall()
        return [(repo, json.loads(source), json.loads(jobs)) for repo, source, jobs in rows]

    def coverage(self, repo, through, *, build=False):
        condition = 'AND build=1' if build else ''
        # Active workflows are not finished-run coverage. A historical stuck
        # workflow remains an explicit inventory item, but cannot pin forever.
        stuck = collect.iso(collect.parse_time(through) - collect.OVERLAP)
        row = self.db.execute(f'''SELECT MIN(created) FROM runs WHERE repo=?
            AND (status='completed' AND jobs_done=0 AND required=1
                 OR status!='completed' AND created >= ?)
            {condition}''', (repo, stuck)).fetchone()[0]
        return min(through, row) if row else through
