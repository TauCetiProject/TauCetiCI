import contextlib
import copy
import datetime as dt
import gzip
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

from collector import build_db, collect, incremental as inc
from collector.gh import GitHub
from collector.index import Index


def run(number=1, *, workflow='.github/workflows/pr-build.yml', attempt=1,
        stamp='2026-10-15T00:01:00Z', status='completed', conclusion='success'):
    return {'id': number, 'run_attempt': attempt, 'path': workflow,
            'event': 'pull_request_target', 'status': status, 'conclusion': conclusion,
            'head_sha': 'a'*40, 'head_branch': 'feature',
            'created_at': stamp, 'updated_at': stamp,
            'pull_requests': [{'number': 42}],
            'repository': {'full_name': 'TauCetiProject/TauCeti'},
            'head_repository': {'full_name': 'TauCetiProject/TauCeti'}}


class ListingAPI:
    def __init__(self, runs, fail_page=None):
        self.runs, self.fail_page, self.requests = runs, fail_page, []
    def check(self):
        pass
    def get(self, path, *, created, per_page, page):
        self.requests.append((created, page, per_page))
        if page == self.fail_page:
            raise RuntimeError('injected interruption')
        lo, hi = created.split('..')
        items = [r for r in self.runs if lo <= r['created_at'] <= hi]
        return {'total_count': len(items), 'workflow_runs': items[(page-1)*per_page:page*per_page]}


class CollectionAPI(ListingAPI):
    def __init__(self, runs, budget):
        super().__init__(runs)
        self.max_calls, self.started, self.phase_limit = budget, 0, budget
    def phase(self, calls, seconds=None):
        self.phase_limit = min(self.max_calls, self.started+calls)
    def request(self):
        if self.started >= self.phase_limit:
            raise inc.CollectionStopped('injected phase limit')
        self.started += 1
    def get(self, *args, **kwargs):
        self.request()
        return super().get(*args, **kwargs)
    def paginate(self, *args, **kwargs):
        self.request()
        return []


class IncrementalTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.records = self.root/'records'
        for name, value in (('ROOT', self.root), ('RECORDS', self.records)):
            p = patch.object(collect, name, value)
            p.start(); self.addCleanup(p.stop)
        self.index = Index(self.root/'index.sqlite', self.records)
        self.addCleanup(self.index.close)
        self.since = collect.parse_time('2026-10-15T00:00:00Z')
        self.until = self.since+dt.timedelta(minutes=30)
        self.checkpoint = Mock()

    def inventory(self, value, repo='TauCeti'):
        record = inc.inventory_record(repo, value)
        inc.persist(self.index, 'inventory', repo, [record], 'inventory')
        return record

    def read(self, kind):
        return [json.loads(line) for p in (self.records/kind).rglob('*.ndjson.gz')
                for line in gzip.open(p, 'rt')]

    def test_interrupted_page_is_durable_and_resume_deduplicates_without_advancing_a_gap(self):
        items = [run(n) for n in range(1, 151)]
        cursor = {'TauCeti': collect.iso(self.since)}
        inc.save_state('inventory-cursor.json', cursor)
        with self.assertRaisesRegex(RuntimeError, 'interruption'):
            inc.scan(ListingAPI(items, fail_page=2), self.index, 'TauCeti', self.since,
                     self.until, 'first', self.checkpoint, cursor)
        self.assertEqual(len(self.read('inventory')), 100)
        self.assertEqual(inc.read_state('inventory-cursor.json')['TauCeti'], collect.iso(self.since))
        self.index.close()
        self.index = Index(self.root/'index.sqlite', self.records)
        self.addCleanup(self.index.close)
        self.index.sync()
        api = ListingAPI(items)
        inc.scan(api, self.index, 'TauCeti', self.since, self.until, 'resume', self.checkpoint, cursor)
        self.assertEqual(len(self.read('inventory')), 150)
        self.assertEqual(inc.read_state('inventory-cursor.json')['TauCeti'], collect.iso(self.until))
        self.assertEqual(len(api.requests), 2)  # No separate count request.

    def test_dense_window_splits_to_seconds_without_losing_boundaries(self):
        items = [run(n+1, stamp=collect.iso(self.since+dt.timedelta(seconds=n//100)))
                 for n in range(2000)]
        api, found = ListingAPI(items), []
        inc.list_window(api, 'TauCeti', self.since, self.since+dt.timedelta(seconds=20), found.extend)
        self.assertEqual({r['id'] for r in found}, set(range(1, 2001)))
        self.assertEqual(len(found), 2000)

    def test_unenumerable_burst_never_certifies_coverage(self):
        items = [run(n, stamp=collect.iso(self.since)) for n in range(1000)]
        with self.assertRaisesRegex(RuntimeError, 'one second'):
            inc.scan(ListingAPI(items), self.index, 'TauCeti', self.since,
                     self.since+dt.timedelta(seconds=1), 'dense', self.checkpoint, {})
        self.assertFalse(self.read('inventory-coverage'))
        self.assertFalse(inc.read_state('inventory-cursor.json'))

    def test_cache_reads_only_new_files_and_rejects_records_absent_from_checkout(self):
        self.inventory(run())
        self.assertEqual(self.index.sync(), 1)
        with patch('collector.index.gzip.open', side_effect=AssertionError('old gzip reread')):
            self.assertEqual(self.index.sync(), 0)
        for p in (self.records/'inventory').rglob('*.ndjson.gz'):
            p.unlink()
        self.index.sync()
        self.assertEqual(self.index.db.execute('SELECT COUNT(*) FROM runs').fetchone()[0], 0)

    def test_optional_details_and_auxiliary_jobs_do_not_pin_build_coverage(self):
        self.inventory(run(4, workflow='.github/workflows/label.yml'))
        value = self.inventory(run(8, stamp='2026-10-15T00:10:00Z'))
        value['jobs_fetched'] = True
        inc.persist(self.index, 'jobs', 'TauCeti', [value], 'jobs')
        self.assertEqual(self.index.coverage('TauCeti', collect.iso(self.until)), '2026-10-15T00:01:00Z')
        self.assertEqual(self.index.coverage('TauCeti', collect.iso(self.until), build=True), collect.iso(self.until))
        self.assertEqual(len(self.index.pending(build=True, detail=True)), 1)

    def test_new_attempt_has_separate_work_and_inventory_cannot_downgrade_enrichment(self):
        value = self.inventory(run())
        value['jobs_fetched'] = True
        inc.persist(self.index, 'runs', 'TauCeti', [value], 'full')
        self.inventory(run())
        self.inventory(run(attempt=2))
        self.assertEqual([v[1]['run_attempt'] for v in self.index.pending(build=True)], [2])
        self.assertEqual(self.index.db.execute('SELECT rank FROM runs WHERE attempt=1').fetchone()[0], 2)

    def test_live_runs_hold_coverage_only_within_the_documented_overlap(self):
        self.inventory(run(status='in_progress', conclusion=None))
        self.assertEqual(self.index.coverage('TauCeti', collect.iso(self.until)), '2026-10-15T00:01:00Z')
        self.assertEqual(self.index.coverage('TauCeti', '2026-10-16T00:00:00Z'), '2026-10-16T00:00:00Z')

    def test_completed_futures_are_saved_even_when_another_hits_the_budget(self):
        self.inventory(run(1)); self.inventory(run(2))
        def record(gh, repo, source, *args, **kwargs):
            if source['id'] == 2:
                raise inc.CollectionStopped('injected limit')
            value = inc.inventory_record(repo, source)
            value['jobs_fetched'] = True
            return value
        gh = Mock()
        with patch.object(collect, 'run_record', side_effect=record), self.assertRaises(inc.CollectionStopped):
            inc.enrich(gh, self.index, 'jobs', self.checkpoint, build=True)
        self.assertEqual([r['run_id'] for r in self.read('jobs')], [1])
        self.assertEqual([r[1]['id'] for r in self.index.pending(build=True)], [2])

    def test_failed_run_does_not_starve_the_rest_of_the_job_queue(self):
        self.inventory(run(1)); self.inventory(run(2))
        def record(gh, repo, source, *args, **kwargs):
            if source['id'] == 1:
                raise RuntimeError('unavailable jobs')
            value = inc.inventory_record(repo, source); value['jobs_fetched'] = True
            return value
        with patch.object(collect, 'run_record', side_effect=record), contextlib.redirect_stdout(io.StringIO()):
            inc.enrich(Mock(), self.index, 'jobs', self.checkpoint, build=True)
        self.assertEqual([r['run_id'] for r in self.read('jobs')], [2])
        self.assertEqual(self.index.coverage('TauCeti', collect.iso(self.until)), '2026-10-15T00:01:00Z')

    def test_completed_job_batches_avoid_a_git_object_for_every_run(self):
        for n in range(32):
            self.inventory(run(n))
        def record(gh, repo, source, *args, **kwargs):
            value = inc.inventory_record(repo, source); value['jobs_fetched'] = True
            return value
        with patch.object(collect, 'run_record', side_effect=record):
            inc.enrich(Mock(), self.index, 'batched', self.checkpoint, build=True)
        self.assertEqual(len(self.read('jobs')), 32)
        self.assertEqual(len(list((self.records/'jobs').rglob('*.gz'))), 2)

    def test_main_history_retains_finished_metadata_when_a_later_page_stops(self):
        def commits(*args, **kwargs):
            yield {'sha': 'a'*40, 'parents': [], 'commit': {'message': 'Example (#42)',
                   'committer': {'date': collect.iso(self.since)}}}
            raise inc.CollectionStopped('budget')
        gh = Mock(); gh.paginate.side_effect = commits
        with self.assertRaises(inc.CollectionStopped):
            collect.collect_main(gh, 'TauCeti', self.since, self.until, 'main', False)
        self.assertEqual(self.read('main')[0]['pr'], 42)

    def test_atomic_batches_do_not_rewrite_a_previous_checkpoint(self):
        self.inventory(run(1))
        path = next((self.records/'inventory').rglob('*.ndjson.gz'))
        original = path.read_bytes()
        self.inventory(run(2))
        self.assertEqual(path.read_bytes(), original)
        self.assertEqual(len(self.read('inventory')), 2)

    def test_recent_metadata_stays_fresh_while_historical_inventory_is_still_incomplete(self):
        old = '2026-10-12T11:02:42Z'
        inc.save_state('cursor.json', {'TauCeti': old, 'Other': collect.iso(self.since)})
        items = [run(4, workflow='.github/workflows/label.yml', stamp=old), run(8)]
        api = CollectionAPI(items, budget=80)
        enricher = Mock()
        enricher.tested.return_value = {}; enricher.telemetry.return_value = None
        enricher.failure.return_value = None
        with patch.object(inc, 'observations'), patch.object(collect, 'collect_main'), \
                patch.object(collect, 'snapshot_settings'), patch.object(collect, 'Enricher', return_value=enricher), \
                patch.object(collect.resources, 'observations', return_value={}), \
                contextlib.redirect_stdout(io.StringIO()):
            inc.run(api, self.index, ['TauCeti', 'Other'], self.until, 'bounded', self.checkpoint)
        cursor = inc.read_state('inventory-cursor.json')
        self.assertGreater(cursor['TauCeti'], old)
        self.assertLess(cursor['TauCeti'], collect.iso(self.until))
        self.assertIsNotNone(self.index.db.execute("SELECT id FROM runs WHERE repo='Other' AND id=8").fetchone())
        self.assertLessEqual(api.started, api.max_calls)
        self.assertLess(inc.read_state('cursor.json')['TauCeti'], collect.iso(self.until))
        self.assertTrue(self.read('jobs'))

    def test_observation_interruption_keeps_records_without_claiming_an_unfinished_day(self):
        old = '2026-10-14T23:58:00Z'
        inc.save_state('observations-cursor.json', {'TauCeti': old})
        values = [{'observed_at': collect.iso(self.since+dt.timedelta(minutes=n)), 'eligible': []}
                  for n in range(201)]
        def interrupted(*args, **kwargs):
            yield from values
            raise RuntimeError('later archive page failed')
        with patch.object(inc, 'read_days', side_effect=interrupted), self.assertRaises(RuntimeError):
            inc.observations(Mock(), 'partial', self.checkpoint, self.since+dt.timedelta(hours=5))
        self.assertEqual(len(self.read('observations')), 200)
        self.assertEqual(inc.read_state('observations-cursor.json')['TauCeti'], old)


class BudgetAndEnrichmentTests(unittest.TestCase):
    def budget(self, calls=2, seconds=10):
        with patch('collector.gh._token', return_value='test'):
            return inc.BoundedGitHub(calls, seconds)

    def test_limits_count_request_attempts_and_never_sleep_across_a_quota_reset(self):
        gh = self.budget()
        self.assertFalse(gh.wait_for_rate_limit)
        gh.before_request(); gh.before_request()
        with self.assertRaises(inc.CollectionStopped):
            gh.before_request()
        self.assertEqual(gh.started, 2)
        gh = self.budget(); gh.deadline = 0
        with self.assertRaises(inc.CollectionStopped):
            gh.check()

    def test_phase_time_limit_leaves_later_streams_time_to_work(self):
        gh = self.budget(seconds=100)
        gh.phase(2, seconds=5)
        gh.phase_deadline = 0
        with self.assertRaises(inc.CollectionStopped):
            gh.check()
        gh.phase(2)
        gh.check()
        gh.before_request()

    def test_phase_stop_does_not_spend_other_streams_reserve(self):
        gh = self.budget(calls=4)
        gh.phase(1); gh.before_request()
        with self.assertRaises(inc.CollectionStopped):
            gh.before_request()
        gh.phase(2); gh.before_request(); gh.before_request()
        self.assertEqual(gh.started, 3)

    def test_shared_artifact_listing_is_fetched_once_and_callers_cannot_mutate_cache(self):
        gh = self.budget()
        with patch.object(GitHub, 'get', return_value={'artifacts': [{'id': 1}]}) as get:
            first = gh.get('repos/test/actions/runs/1/artifacts')
            first['artifacts'].clear()
            self.assertEqual(gh.get('repos/test/actions/runs/1/artifacts')['artifacts'], [{'id': 1}])
            get.assert_called_once()

    def test_current_attempt_jobs_are_not_repeated_during_optional_enrichment(self):
        value = run(attempt=3)
        job = {'id': 10, 'name': 'sandboxed-build', 'conclusion': 'success',
               'steps': [{'number': 1, 'name': 'Build', 'conclusion': 'success'}]}
        gh = Mock(); gh.paginate.return_value = [job]
        jobs = collect.run_record(gh, 'TauCeti', value, None, 'all',
                                  current_attempt_only=True, keep_steps=True)
        self.assertIn('/attempts/3/jobs', gh.paginate.call_args.args[0])
        self.assertEqual(gh.paginate.call_count, 1)
        self.assertEqual(jobs['jobs'][0]['steps'][0]['name'], 'Build')
        gh.paginate.reset_mock()
        enricher = Mock()
        enricher.tested.return_value = {}; enricher.telemetry.return_value = None
        enricher.failure.return_value = None
        with patch('collector.collect.resources.observations', return_value={}):
            enriched = collect.run_record(gh, 'TauCeti', value, enricher, 'all', preloaded_record=jobs)
        gh.paginate.assert_not_called()
        self.assertEqual(enriched['jobs'], jobs['jobs'])


class DerivedDataTests(unittest.TestCase):
    def test_inventory_and_merge_members_survive_an_enrichment_backlog(self):
        raw = run(1)
        raw.update(event='repository_dispatch', display_title=f'bors branch=main batch=7 head={"b"*40} base={"c"*40}')
        inventory = inc.inventory_record('TauCeti', raw)
        full = copy.deepcopy(inventory)
        full['run_attempt'] = 0; full['conclusion'] = 'failure'
        full.pop('_listing')
        full['telemetry'] = {'phases': [], 'build': {}, 'meta': {}}
        observation = {'observed_at': '2026-10-15T00:20:00Z', 'eligible': [{'pr': 42}, {'pr': 42}],
                       'candidate_scan_complete': False, 'candidate_snapshot_at': None}
        data = {'inventory': [inventory], 'runs': [full], 'observations': [observation],
                'merge-members': [{'repo': 'TauCeti', 'batch_id': 7, 'head_sha': 'b'*40,
                                   'members': [{'pr': 42, 'head_sha': 'd'*40}]}],
                'inventory-coverage': [{'repo': 'TauCeti', 'created_at': raw['created_at'],
                                        'until': '2026-10-15T00:30:00Z'}]}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(build_db, 'ROOT', root), patch.object(build_db, 'RECORDS', root/'records'), \
                    patch.object(build_db, 'records', side_effect=lambda kind: iter(data.get(kind, []))):
                build_db.build(root/'ci.sqlite')
            with contextlib.closing(sqlite3.connect(root/'ci.sqlite')) as db:
                self.assertEqual(db.execute('SELECT run_attempt,conclusion FROM runs').fetchone(), (1, 'success'))
                self.assertEqual(db.execute('SELECT members_json FROM merge_builds').fetchone()[0],
                                 json.dumps(data['merge-members'][0]['members']))
                self.assertFalse(db.execute('SELECT * FROM telemetry').fetchall())
                self.assertEqual(db.execute('SELECT eligible_count,candidate_scan_complete,candidate_snapshot_at '
                                             'FROM queue_observation_metadata').fetchone(), (1, 0, None))
                self.assertEqual(db.execute('SELECT complete_to FROM stream_coverage').fetchone()[0],
                                 '2026-10-15T00:30:00Z')


if __name__ == '__main__':
    unittest.main()
