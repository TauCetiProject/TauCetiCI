"""Bounded, checkpointed run inventory followed by independently resumable enrichment.

API pages are durable before job requests start. A disposable SQLite index avoids
re-reading historical gzip files and is rebuilt from committed records on a cache
miss. Coverage for inventory, build jobs, all sampled jobs and bors observations
is independent. No request waits for a rate-limit reset.
"""
import argparse
import copy
import datetime as dt
import json
import signal
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from . import collect
from .gh import GitHub, RateLimitUnavailable
from .index import Index
from .observations import batch_members, read_days
from .merge_identity import apply_dispatch_title


class CollectionStopped(BaseException):
    """Cooperative stop, like KeyboardInterrupt: optional enrichment must not swallow it."""


class BoundedGitHub(GitHub):
    def __init__(self, max_calls, max_seconds):
        super().__init__(wait_for_rate_limit=False)
        self.max_calls = max_calls
        self.deadline = time.monotonic() + max_seconds
        self.phase_limit = max_calls
        self.phase_deadline = self.deadline
        self.started = 0
        self.stop_requested = False
        self.cache = {}
        self.cache_lock = threading.Lock()

    def check(self):
        if self.stop_requested or time.monotonic() >= min(self.deadline, self.phase_deadline):
            raise CollectionStopped('collection time budget reached; checkpointing')

    def before_request(self):
        self.check()
        with self._lock:
            if self.started >= min(self.max_calls, self.phase_limit):
                raise CollectionStopped('collection API budget reached; checkpointing')
            self.started += 1

    def _request(self, *args, **kwargs):
        try:
            return super()._request(*args, **kwargs)
        except RateLimitUnavailable as e:
            raise CollectionStopped(str(e)) from e

    def raw(self, *args, **kwargs):
        try:
            return super().raw(*args, **kwargs)
        except RateLimitUnavailable as e:
            raise CollectionStopped(str(e)) from e

    def get(self, path, **params):
        # Completed-run artifacts are queried by telemetry and both resource
        # readers. Immutable comparisons are also shared within an invocation.
        cacheable = '/compare/' in path or path.endswith('/artifacts')
        key = (path, tuple(sorted(params.items())))
        with self.cache_lock:
            if cacheable and key in self.cache:
                return copy.deepcopy(self.cache[key])
        result = super().get(path, **params)
        if cacheable:
            with self.cache_lock:
                if len(self.cache) >= 256:
                    self.cache.clear()
                self.cache[key] = copy.deepcopy(result)
        return result

    def phase(self, calls, seconds=None):
        self.phase_limit = min(self.max_calls, self.started + calls)
        self.phase_deadline = min(self.deadline, time.monotonic()+seconds) if seconds else self.deadline


def read_state(name):
    path = collect.ROOT / 'state' / name
    return json.loads(path.read_text()) if path.exists() else {}


def save_state(name, value):
    path = collect.ROOT / 'state' / name
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    try:
        temporary.write_text(json.dumps(value, indent=1, sort_keys=True) + '\n')
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


class Checkpoint:
    def __init__(self, push=False, interval=120):
        self.push, self.interval = push, interval
        self.last = time.monotonic()
        self.prepare = None

    def __call__(self, force=False):
        if not self.push or not force and time.monotonic()-self.last < self.interval:
            return
        if self.prepare:
            self.prepare()
        # Fail rather than silently collect another half hour after a failed
        # checkpoint. The workflow's final step retries this same publication.
        subprocess.run(['python3', '-m', 'collector.checkpoint'], check=True)
        self.last = time.monotonic()


def listing_source(run):
    keys = ('id', 'run_attempt', 'path', 'name', 'event', 'status', 'conclusion',
            'head_sha', 'head_branch', 'created_at', 'updated_at', 'run_started_at',
            'display_title', 'referenced_workflows')
    out = {k: run[k] for k in keys if k in run}
    for k in ('repository', 'head_repository'):
        out[k] = {'full_name': (run.get(k) or {}).get('full_name')}
    out['actor'] = {'login': (run.get('actor') or {}).get('login')}
    out['pull_requests'] = [{'number': p['number']} for p in run.get('pull_requests', [])]
    return out


def inventory_record(repo, run):
    r = {'schema': collect.SCHEMA, 'repo': repo, 'run_id': run['id'],
         'run_attempt': run.get('run_attempt', 1), 'workflow': run.get('path') or run.get('name'),
         'event': run['event'], 'trigger': collect.trigger_class(run),
         'conclusion': run.get('conclusion'), 'head_sha': run['head_sha'],
         'head_branch': run.get('head_branch'), 'head_repo': (run.get('head_repository') or {}).get('full_name'),
         'created_at': run['created_at'], 'updated_at': run.get('updated_at'),
         'title': (run.get('display_title') or '')[:200],
         'jobs': [], 'jobs_fetched': False, '_listing': listing_source(run)}
    apply_dispatch_title(r, r['title'])
    return r


def persist(index, kind, repo, records, stamp):
    if not records:
        return
    collect.write_records(kind, repo, records, 'created_at', stamp)
    for r in records:
        index.ingest(kind, r)
    index.db.commit()


def list_window(gh, repo, since, until, page_done):
    """List all statuses in [since,until), persisting each page exactly once.

    Reuse the first page's count instead of a separate count request. Split down
    to seconds, not five-minute windows that can exceed GitHub's 1,000-result cap.
    Never certify an unenumerable single-second burst as complete.
    """
    if since >= until:
        return
    query = f'{collect.iso(since)}..{collect.iso(until-dt.timedelta(seconds=1))}'
    path = f'repos/{collect.ORG}/{repo}/actions/runs'
    first = gh.get(path, created=query, per_page=100, page=1)
    total = first['total_count']
    if total >= 1000:
        seconds = int((until-since).total_seconds())
        if seconds <= 1:
            raise RuntimeError(f'{repo}: >=1000 runs in one second; inventory coverage left open')
        middle = since + dt.timedelta(seconds=seconds//2)
        list_window(gh, repo, since, middle, page_done)
        list_window(gh, repo, middle, until, page_done)
        return
    count = 0
    for page in range(1, (total+99)//100 + 1):
        value = first if page == 1 else gh.get(path, created=query, per_page=100, page=page)
        items = value['workflow_runs']
        if not items:
            raise RuntimeError(f'{repo}: listing page disappeared; coverage left open')
        page_done(items)
        count += len(items)
    if count < total:
        raise RuntimeError(f'{repo}: incomplete listing; coverage left open')


def scan(gh, index, repo, since, until, stamp, checkpoint, cursor=None):
    """Small chronological windows; interrupted windows are replayed and deduplicated."""
    def page_done(items):
        records = []
        for run in items:
            old = index.db.execute('SELECT updated,status FROM runs WHERE repo=? AND id=? AND attempt=?',
                                   (repo, run['id'], run.get('run_attempt', 1))).fetchone()
            if old == (run.get('updated_at'), run.get('status')):
                continue
            records.append(inventory_record(repo, run))
        persist(index, 'inventory', repo, records, stamp)
        checkpoint()

    # A receipt covers a contiguous sequence of completed windows. Group the
    # windows into one immutable file instead of creating dozens of tiny files
    # for every overlapping scan. Even a cooperative stop preserves the prefix.
    covered_from, covered_to = since, since
    try:
        while since < until:
            gh.check()
            end = min(until, since+dt.timedelta(minutes=30))
            list_window(gh, repo, since, end, page_done)
            covered_to = end
            if cursor is not None:
                cursor[repo] = collect.iso(end)
                save_state('inventory-cursor.json', cursor)
            since = end
            checkpoint()
    finally:
        if covered_to > covered_from:
            collect.write_records('inventory-coverage', repo, [{
                'repo': repo, 'created_at': collect.iso(covered_from), 'until': collect.iso(covered_to),
                'collected_at': stamp}], 'created_at', stamp)
        checkpoint()


def observations(gh, stamp, checkpoint, until):
    cursor = read_state('observations-cursor.json')
    if 'TauCeti' in cursor and until <= collect.parse_time(cursor['TauCeti']):
        return
    since = collect.parse_time(cursor['TauCeti'])-dt.timedelta(minutes=2) if 'TauCeti' in cursor else until-dt.timedelta(days=1)
    batch = []
    def page_done(position):
        nonlocal batch
        if batch:
            collect.write_records('observations', None, batch, 'observed_at', stamp)
            batch = []
        # Records precede pagination state in every checkpoint. Replaying the
        # final page catches new observations without re-downloading the day.
        save_state('observations-pages.json', position)
        checkpoint()
    def day_done(covered):
        nonlocal batch
        if batch:
            collect.write_records('observations', None, batch, 'observed_at', stamp)
            batch = []
        cursor['TauCeti'] = collect.iso(covered)
        save_state('observations-cursor.json', cursor)
        checkpoint()
    # Records are durable in small batches; the cursor advances only at a
    # completed archive day. Interrupted pages are replayed safely.
    for observation in read_days(since, until, before_request=gh.check, day_done=day_done,
                                 resume=read_state('observations-pages.json') or None, page_done=page_done):
        gh.check()
        batch.append(observation)
        if len(batch) >= 200:
            collect.write_records('observations', None, batch, 'observed_at', stamp)
            batch = []
            checkpoint()
    if batch:
        collect.write_records('observations', None, batch, 'observed_at', stamp)
    cursor['TauCeti'] = collect.iso(until)
    save_state('observations-cursor.json', cursor)
    checkpoint()


def merge_members(gh, index, stamp, checkpoint, limit=80):
    rows = index.db.execute('''SELECT r.repo,r.id,r.attempt,r.created,r.batch,r.head
        FROM runs r LEFT JOIN batches b ON b.repo=r.repo AND b.batch=r.batch AND b.head=r.head
        WHERE r.status='completed' AND r.batch IS NOT NULL AND r.head IS NOT NULL AND b.members IS NULL
        ORDER BY r.created DESC LIMIT ?''', (limit,)).fetchall()
    tried = set()
    for repo, run_id, attempt, created, batch, head in rows:
        gh.check()
        if (repo, batch, head) in tried:
            continue
        tried.add((repo, batch, head))
        try:
            members = batch_members(batch, head)
        except Exception as e:
            print(f'batch {batch}: membership deferred ({type(e).__name__})', flush=True)
            continue
        r = {'repo': repo, 'run_id': run_id, 'run_attempt': attempt,
             'created_at': created, 'batch_id': batch, 'head_sha': head, 'members': members}
        persist(index, 'merge-members', repo, [r], stamp)
        checkpoint()


def enrich(gh, index, stamp, checkpoint, *, build, detail=False, recent=None):
    enrichers = {}
    completed = {}
    last_flush = time.monotonic()
    def flush():
        nonlocal last_flush
        for repo, records in completed.items():
            persist(index, 'runs' if detail else 'jobs', repo, records, stamp)
        completed.clear()
        last_flush = time.monotonic()
        checkpoint()
    while True:
        gh.check()
        pending = index.pending(build=build, detail=detail, recent=recent)
        if not pending:
            return
        def one(item):
            repo, run, previous = item
            if detail:
                enricher = enrichers.setdefault(repo, collect.Enricher(gh, repo))
                record = collect.run_record(gh, repo, run, enricher, 'all',
                                            preloaded_record=previous, batch_fallback=False)
            else:
                record = collect.run_record(gh, repo, run, None, 'all',
                                            current_attempt_only=True, keep_steps=build)
                record['_listing'] = run
                # Preserve cheap dispatch identity for job-only snapshots.
                record['title'] = (run.get('display_title') or '')[:200]
                apply_dispatch_title(record, record['title'])
            return repo, record
        stopped = None
        # Only a bounded batch is queued. Persist small completed batches even
        # when another future reaches a limit; avoid a gzip/git object per run.
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(one, item) for item in pending]
            for future in as_completed(futures):
                try:
                    repo, record = future.result()
                except CollectionStopped as e:
                    stopped = e
                    continue
                except Exception as e:
                    item = pending[futures.index(future)]
                    repo, run, _ = item
                    index.db.execute('''UPDATE runs SET retry_after=?
                        WHERE repo=? AND id=? AND attempt=?''',
                        (time.time()+3600, repo, run['id'], run.get('run_attempt', 1)))
                    index.db.commit()
                    print(f'{repo} run {run["id"]}: deferred {type(e).__name__}', flush=True)
                    continue
                completed.setdefault(repo, []).append(record)
                if sum(map(len, completed.values())) >= 16 or time.monotonic()-last_flush >= 5:
                    flush()
        flush()
        if stopped:
            raise stopped


def run(gh, index, repos, until, stamp, checkpoint, since=None, discover=False):
    index.sync()
    legacy = read_state('cursor.json')
    inventory = read_state('inventory-cursor.json') or dict(legacy)
    errors = []
    def coverage():
        if not since:
            full = {r: end for r, end in legacy.items() if r in repos}
            builds = {r: end for r, end in read_state('build-cursor.json').items() if r in repos}
            for repo, end in inventory.items():
                if repo not in repos:
                    continue
                full[repo] = index.coverage(repo, end)
                builds[repo] = index.coverage(repo, end, build=True)
            save_state('cursor.json', full)
            save_state('build-cursor.json', builds)
    if isinstance(checkpoint, Checkpoint):
        checkpoint.prepare = coverage
    def phase(label, calls, work, seconds=None):
        gh.phase(calls, seconds=seconds)
        try:
            work()
        except (CollectionStopped, RateLimitUnavailable) as e:
            print(f'{label}: {e}', flush=True)
        except Exception as e:
            errors.append(f'{label}: {type(e).__name__}')
            print(errors[-1], flush=True)
        finally:
            checkpoint()

    phase('observations', 0, lambda: observations(gh, stamp, checkpoint, until), seconds=120)
    if discover:
        def discover_repositories():
            active = [r['name'] for r in gh.paginate(f'orgs/{collect.ORG}/repos') if not r.get('archived')]
            repos[:] = active
        # Repository discovery is optional when GitHub's quota is exhausted;
        # saved repositories and the independent Bors stream remain collectable.
        phase('repository discovery', 20, discover_repositories, seconds=30)
    # Give every repository a recent scan before spending the discovery budget
    # on an older backlog. This never advances the historical cursor across a gap.
    inventory_budget = max(1, gh.max_calls*2//5)
    gh.phase(inventory_budget, seconds=600)
    for repo in sorted(repos, key=lambda r: r not in collect.DETAIL_REPOS):
        try:
            scan(gh, index, repo, since or until-collect.OVERLAP, until, stamp, checkpoint)
        except CollectionStopped as e:
            print(f'recent inventory: {e}', flush=True)
            break
        except Exception as e:
            errors.append(f'{repo} recent inventory: {type(e).__name__}')
            print(errors[-1], flush=True)
    for repo in repos:
        start = since or collect.parse_time(inventory.get(repo, collect.iso(until-dt.timedelta(days=1))))
        try:
            scan(gh, index, repo, start, until, stamp, checkpoint, None if since else inventory)
        except CollectionStopped as e:
            print(f'historical inventory: {e}', flush=True)
            break
        except Exception as e:
            errors.append(f'{repo} historical inventory: {type(e).__name__}')
            print(errors[-1], flush=True)
    phase('merge membership', 0, lambda: merge_members(gh, index, stamp, checkpoint), seconds=60)
    # Main history is needed for landing dates and throughput, so reserve its
    # small budget before optional artifact/log enrichment.
    def main_history():
        for repo in set(repos) & collect.DETAIL_REPOS:
            start = since or collect.parse_time(legacy.get(repo, collect.iso(until-collect.OVERLAP)))
            collect.collect_main(gh, repo, start, until, stamp, False)
            collect.snapshot_settings(gh, repo, stamp, False)
    phase('main history', max(1, gh.max_calls//20), main_history, seconds=120)
    remaining = max(0, gh.max_calls-gh.started)
    phase('recent build jobs', remaining//4, lambda: enrich(
        gh, index, stamp, checkpoint, build=True, recent=collect.iso(until-collect.OVERLAP)))
    phase('historical build jobs', remaining//4,
          lambda: enrich(gh, index, stamp, checkpoint, build=True))
    phase('sampled auxiliary jobs', max(0, gh.max_calls-gh.started-min(200, remaining//10)),
          lambda: enrich(gh, index, stamp, checkpoint, build=False))
    phase('optional details', max(0, gh.max_calls-gh.started),
          lambda: enrich(gh, index, stamp, checkpoint, build=True, detail=True))
    coverage()
    checkpoint(force=True)
    index.sync()
    print(f'GitHub request attempts: {gh.started}; pending jobs: '
          f'{index.db.execute("SELECT COUNT(*) FROM runs WHERE jobs_done=0").fetchone()[0]}', flush=True)
    if errors:
        raise RuntimeError('; '.join(errors))


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--max-calls', type=int, default=4500)
    ap.add_argument('--max-seconds', type=int, default=1800)
    ap.add_argument('--index', default='.cache/collector.sqlite')
    ap.add_argument('--push', action='store_true', help='publish checkpoints at most two minutes apart')
    ap.add_argument('--repos', default=None)
    ap.add_argument('--since')
    ap.add_argument('--until')
    args = ap.parse_args(argv)
    gh = BoundedGitHub(args.max_calls, args.max_seconds)
    # Signals request a cooperative stop; no signal handler performs I/O or
    # commits half a record. Leave time for checkpoint/push before the hard timeout.
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: setattr(gh, 'stop_requested', True))
    index = Index(args.index, collect.RECORDS)
    checkpoint = Checkpoint(args.push)
    try:
        repos = args.repos.split(',') if args.repos else sorted(
            set(read_state('cursor.json')) | set(read_state('inventory-cursor.json')) | {'TauCeti'})
        until = collect.parse_time(args.until) if args.until else dt.datetime.now(collect.UTC).replace(microsecond=0)
        stamp = dt.datetime.now(collect.UTC).strftime('%Y%m%dT%H%M%S%fZ')
        run(gh, index, repos, until, stamp, checkpoint,
            collect.parse_time(args.since).replace(microsecond=0) if args.since else None,
            discover=not args.repos)
    finally:
        checkpoint(force=True)
        index.close()


if __name__ == '__main__':
    main()
