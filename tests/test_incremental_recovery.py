import contextlib
import copy
import io
import json
import sqlite3
import subprocess
import unittest
import urllib.error
from unittest.mock import Mock, patch

from collector import collect, incremental as inc
from collector.index import Index
import test_incremental as fixtures


class RecoveryTests(unittest.TestCase):
    setUp = fixtures.IncrementalTests.setUp
    inventory = fixtures.IncrementalTests.inventory
    read = fixtures.IncrementalTests.read

    def test_checkpoint_packing_preserves_committed_files_and_the_cache_manifest(self):
        self.inventory(fixtures.run(1))
        first=next((self.records/'inventory').rglob('*.gz'))
        original=first.read_bytes()
        for args in [('init','-b','main'),('config','user.name','test'),('config','user.email','test@example.invalid'),
                     ('add','records'),('commit','-m','published baseline')]:
            subprocess.run(['git',*args],cwd=self.root,check=True,capture_output=True)
        for number in (2,3,4):
            self.inventory(fixtures.run(number))
        inc.coalesce(self.index,'packed')
        self.assertEqual(first.read_bytes(),original)
        self.assertEqual(len(list((self.records/'inventory').rglob('*.gz'))),2)
        self.assertEqual({r['run_id'] for r in self.read('inventory')},{1,2,3,4})
        with patch('collector.index.gzip.open',side_effect=AssertionError('reread packed archive')):
            self.assertEqual(self.index.sync(),0)
        for path in (self.records/'inventory').rglob('*.gz'):
            if path != first:
                path.unlink()  # A failed push must not lend cached packed records.
        self.index.sync()
        self.assertEqual(self.index.db.execute('SELECT COUNT(*) FROM runs').fetchone()[0],1)

    def test_unpublished_cached_jobs_are_requeued_without_a_final_sync(self):
        value = self.inventory(fixtures.run())
        value['jobs_fetched'] = True
        inc.persist(self.index, 'jobs', 'TauCeti', [value], 'unpublished')
        for path in (self.records/'jobs').rglob('*.gz'):
            path.unlink()  # The push failed and the next checkout lacks this file.
        self.index.close()
        self.index = Index(self.root/'index.sqlite', self.records)
        self.addCleanup(self.index.close)
        self.index.sync()
        self.assertEqual(len(self.index.pending(build=True)), 1)

    def test_completed_detail_payload_is_not_retained_in_the_cache(self):
        value = self.inventory(fixtures.run())
        inc.persist(self.index, 'runs', 'TauCeti', [value], 'complete')
        self.assertEqual(self.index.db.execute('SELECT source,jobs FROM runs').fetchone(), (None,None))
        self.assertEqual(len(self.read('runs')), 1)

    def test_checkpoint_failure_retains_all_completed_futures_and_stops(self):
        for n in range(32):
            self.inventory(fixtures.run(n))
        def normalize(gh, repo, source, *args, **kwargs):
            return inc.inventory_record(repo, source)
        with patch.object(collect, 'run_record', side_effect=normalize):
            self.checkpoint.side_effect = inc.CheckpointFailed('push failed')
            with self.assertRaises(inc.CheckpointFailed):
                inc.enrich(Mock(), self.index, 'jobs', self.checkpoint, build=True)
        self.assertEqual(len(self.read('jobs')), 32)
        self.assertEqual(self.checkpoint.call_count, 1)

    def test_shared_api_outages_do_not_permanently_retire_runs(self):
        self.inventory(fixtures.run())
        for attempt in range(4):
            with patch('collector.incremental.time.time',return_value=10000+attempt*3601),                     patch.object(collect,'run_record',side_effect=RuntimeError('GitHub API failed repeatedly')),                     contextlib.redirect_stdout(io.StringIO()):
                inc.enrich(Mock(),self.index,'outage',self.checkpoint,build=True)
        self.assertFalse(self.read('jobs'))
        self.assertEqual([r['failures'] for r in self.read('collection-errors')],[0]*4)
        with patch('collector.index.time.time',return_value=30000):
            self.assertEqual(len(self.index.pending(build=True)),1)

    def test_future_backfill_endpoint_is_clamped_and_the_requested_range_is_retained(self):
        with patch.object(inc,'Index'),patch.object(inc,'BoundedGitHub'),                 patch.object(inc.signal,'signal'),patch.object(inc,'run') as run:
            inc.main(['--repos','TauCeti','--since','2026-10-01','--until','2999-01-01'])
        self.assertLessEqual(run.call_args.args[3],collect.dt.datetime.now(collect.UTC))
        self.assertEqual(run.call_args.kwargs['requested_until'].year,2999)

    def test_extending_a_current_day_backfill_resumes_without_touching_observations(self):
        requested=self.until+collect.dt.timedelta(days=1)
        inc.save_state('observations-cursor.json',{'TauCeti':'2026-10-15T00:05:00Z'})
        ranges=[]
        for end in (self.since+collect.dt.timedelta(minutes=15),self.until):
            api=fixtures.CollectionAPI([],budget=80)
            with patch.object(inc,'observations') as observations,patch.object(collect,'collect_main'),                     patch.object(collect,'snapshot_settings'),contextlib.redirect_stdout(io.StringIO()):
                inc.run(api,self.index,['TauCeti'],end,'growing',self.checkpoint,
                        since=self.since,requested_until=requested)
            observations.assert_not_called()
            ranges.extend(api.requests)
        self.assertEqual([r[0] for r in ranges],
                         ['2026-10-15T00:00:00Z..2026-10-15T00:14:59Z','2026-10-15T00:15:00Z..2026-10-15T00:29:59Z'])
        self.assertEqual(inc.read_state('observations-cursor.json')['TauCeti'],'2026-10-15T00:05:00Z')

    def test_deleted_jobs_leave_an_explicit_gap_instead_of_pinning_coverage(self):
        self.inventory(fixtures.run())
        error = urllib.error.HTTPError('https://api.github.com/run/jobs',404,'deleted',{},None)
        self.addCleanup(error.close)
        with patch.object(collect,'run_record',side_effect=error), contextlib.redirect_stdout(io.StringIO()):
            inc.enrich(Mock(), self.index, 'gap', self.checkpoint, build=True)
        self.assertIn('jobs_gap',self.read('jobs')[0])
        self.assertFalse(self.index.pending(build=True))
        self.assertEqual(self.index.coverage('TauCeti',collect.iso(self.until)),collect.iso(self.until))

    def test_repeated_errors_have_durable_backoff_then_an_explicit_gap(self):
        self.inventory(fixtures.run())
        for attempt in range(3):
            with patch('collector.incremental.time.time',return_value=10000+attempt*3601), \
                    patch.object(collect,'run_record',side_effect=ValueError('bad payload')), \
                    contextlib.redirect_stdout(io.StringIO()):
                inc.enrich(Mock(),self.index,'retry',self.checkpoint,build=True)
            if attempt == 0:
                self.index.close()
                (self.root/'index.sqlite').unlink()
                self.index = Index(self.root/'index.sqlite',self.records)
                self.addCleanup(self.index.close)
                self.index.sync()
                with patch('collector.index.time.time',return_value=10001):
                    self.assertFalse(self.index.pending(build=True))
        self.assertEqual(len(self.read('collection-errors')),2)
        self.assertEqual(self.read('jobs')[0]['jobs_gap']['failures'],3)

    def test_earlier_attempt_is_recovered_even_if_only_seen_active_before_the_retry(self):
        self.inventory(fixtures.run(attempt=1,status='in_progress',conclusion=None))
        self.inventory(fixtures.run(attempt=2))
        self.assertEqual([r[2] for r in self.index.attempts()],[1])
        gh = Mock()
        previous = fixtures.run(attempt=1,conclusion='failure')
        gh.get.return_value=previous
        inc.earlier_attempts(gh,self.index,'attempt',self.checkpoint)
        self.assertFalse(self.index.attempts())
        gh.get.assert_called_once_with('repos/TauCetiProject/TauCeti/actions/runs/1/attempts/1')
        self.assertEqual({r[1]['run_attempt'] for r in self.index.pending(build=True)},{1,2})

    def test_archived_earlier_jobs_are_not_fetched_again(self):
        value=inc.inventory_record('TauCeti',fixtures.run(attempt=2))
        value['jobs']=[{'run_attempt':1,'id':11}]
        inc.persist(self.index,'runs','TauCeti',[value],'legacy')
        self.assertFalse(self.index.attempts())

    def test_missing_attempt_metadata_is_explicitly_retired(self):
        value=self.inventory(fixtures.run(attempt=2))
        inc.persist(self.index,'jobs','TauCeti',[value],'latest')
        gh=Mock(); gh.get.side_effect=urllib.error.HTTPError('https://api.github.com/run/attempt',410,'expired',{},None)
        self.addCleanup(gh.get.side_effect.close)
        inc.earlier_attempts(gh,self.index,'gap',self.checkpoint)
        self.assertFalse(self.index.attempts())
        self.assertEqual(self.read('attempt-gaps')[0]['run_attempt'],1)
        self.assertEqual(self.index.coverage('TauCeti',collect.iso(self.until)),collect.iso(self.until))

    def test_backfill_resumes_inventory_without_relisting_the_prefix_twice(self):
        since=self.since-collect.dt.timedelta(days=1)
        requests=[]
        for _ in range(2):
            api=fixtures.CollectionAPI([],budget=80)
            with patch.object(inc,'observations'),patch.object(collect,'collect_main'), \
                    patch.object(collect,'snapshot_settings'),contextlib.redirect_stdout(io.StringIO()):
                inc.run(api,self.index,['TauCeti'],self.until,'backfill',self.checkpoint,since=since)
            requests.extend(api.requests)
        self.assertEqual(len(requests),49)  # 24.5 hours at 30-minute windows, exactly once.
        self.assertEqual(len(set(requests)),49)
        self.assertFalse(inc.read_state('cursor.json'))

    def test_main_history_uses_its_own_cursor_with_overlap(self):
        inc.save_state('cursor.json',{'TauCeti':collect.iso(self.since)})
        inc.save_state('main-cursor.json',{'TauCeti':'2026-10-14T18:00:00Z'})
        api=fixtures.CollectionAPI([],budget=80)
        with patch.object(inc,'observations'),patch.object(collect,'collect_main') as main, \
                patch.object(collect,'snapshot_settings'),contextlib.redirect_stdout(io.StringIO()):
            inc.run(api,self.index,['TauCeti'],self.until,'main',self.checkpoint)
        self.assertEqual(collect.iso(main.call_args.args[2]),'2026-10-14T12:00:00Z')
        self.assertEqual(inc.read_state('main-cursor.json')['TauCeti'],collect.iso(self.until))

    def test_skipped_inventory_is_not_missing_job_data(self):
        value=self.inventory(fixtures.run(conclusion='skipped'))
        self.assertNotIn('jobs_fetched',value)
        self.assertFalse(self.index.pending(build=True))


if __name__ == '__main__':
    unittest.main()
