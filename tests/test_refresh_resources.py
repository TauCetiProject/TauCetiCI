import datetime as dt
import gzip
import io
import json
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import Mock, patch

from collector import collect, refresh_resources as refresh
from collector.gh import RateLimitUnavailable


def run(number, event='repository_dispatch'):
    return {'id': number, 'run_attempt': 1, 'created_at': '2026-10-14T12:00:00Z',
            'status': 'completed', 'event': event, 'head_branch': 'main',
            'head_repository': {'full_name': 'TauCetiProject/TauCeti'},
            'head_sha': 'a'*40, 'path': '.github/workflows/pr-build.yml'}


class RefreshTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.patch = patch.object(collect, 'RECORDS', self.root / 'records')
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.now = dt.datetime(2026, 10, 14, 13, tzinfo=dt.timezone.utc)
        self.gh = Mock(calls=4, max_calls=150, started_requests=4)
        self.output = patch('builtins.print')
        self.output.start()
        self.addCleanup(self.output.stop)

    @staticmethod
    def record(gh, repo, value, *args, **kwargs):
        return {'repo': repo, 'run_id': value['id'], 'run_attempt': 1,
                'created_at': value['created_at'], 'observations': {
                    'resources': {'coverage': 'present', 'payload': {'samples': 2}}}}

    def receipt(self):
        return json.loads(next((self.root / 'records/resource-refresh').rglob('*.json')).read_text())

    def snapshots(self):
        return [json.loads(line) for p in (self.root / 'records/runs').rglob('*.gz')
                for line in gzip.open(p, 'rt')]

    def test_listing_limits_trust_seen_filter_and_receipt_do_not_advance_cursor(self):
        self.gh.get.side_effect = [
            {'total_count': 500, 'workflow_runs': [run(1), run(2), run(3, 'pull_request')]},
            {'total_count': 0, 'workflow_runs': []}]
        with patch.object(collect, 'seen_keys', return_value={(1, 1)}), \
                patch.object(collect, 'run_record', side_effect=self.record) as record:
            value = refresh.refresh(self.gh, now=self.now)
        self.assertEqual(record.call_count, 1)
        self.assertFalse(record.call_args.kwargs['batch_fallback'])
        self.assertEqual([r['run_id'] for r in self.snapshots()], [2])
        self.assertTrue(value['listings'][0]['truncated'])
        self.assertEqual(value['already_recorded'], 1)
        self.assertEqual(value['unverified_producers'], 1)
        self.assertEqual(value, self.receipt())
        self.assertEqual(self.snapshots()[0]['collection_scope']['mode'], 'bounded-recent-resources')
        self.assertFalse((self.root / 'state').exists())
        for call in self.gh.get.call_args_list:
            self.assertEqual(call.kwargs['per_page'], 100)
            self.assertEqual(call.kwargs['status'], 'completed')

    def test_run_cap_and_explicit_reverification_are_bounded(self):
        self.gh.get.side_effect = [run(1), run(2), run(3)]
        with patch.object(collect, 'run_record', side_effect=self.record):
            value = refresh.refresh(self.gh, [1, 2, 3], max_runs=2, now=self.now)
        self.assertEqual(value['recorded'], 2)
        self.assertEqual(value['deferred'], 'run limit reached')
        self.assertCountEqual([r['run_id'] for r in self.snapshots()], [3, 2])
        self.assertIsNone(self.snapshots()[0]['collection_scope']['since'])

    def test_later_budget_exhaustion_preserves_completed_records(self):
        self.gh.get.side_effect = [run(1), run(2)]
        self.gh.check_budget.side_effect = [None, refresh.RefreshBudgetExceeded('budget')]
        with patch.object(collect, 'run_record', side_effect=self.record):
            value = refresh.refresh(self.gh, [1, 2], now=self.now)
        self.assertEqual([r['run_id'] for r in self.snapshots()], [2])
        self.assertEqual(value['recorded'], 1)
        self.assertEqual(value['deferred'], 'budget')

    def test_missing_artifacts_and_quota_deferral_are_visible(self):
        self.gh.get.side_effect = [run(1)]
        with patch.object(collect, 'run_record', return_value={'observations': {
                'resources': {'coverage': 'unavailable'}}}):
            value = refresh.refresh(self.gh, [1], now=self.now)
        self.assertEqual(value['without_artifacts'], 1)
        self.assertEqual(value['gaps'], [{'run_id': 1, 'run_attempt': 1,
                                         'coverage': {'resources': 'unavailable'}}])
        self.assertEqual(self.snapshots(), [])
        self.gh.get.side_effect = RateLimitUnavailable('quota')
        value = refresh.refresh(self.gh, [1], now=self.now + dt.timedelta(seconds=1))
        self.assertEqual(value['deferred'], 'quota')

    def test_unexpected_failure_keeps_prior_snapshots_and_visible_failure_receipt(self):
        self.gh.get.side_effect = [run(1), run(2)]
        with patch.object(collect, 'run_record', side_effect=[
                self.record(None, 'TauCeti', run(2)), RuntimeError('network')]):
            with self.assertRaises(RuntimeError):
                refresh.refresh(self.gh, [1, 2], now=self.now)
        self.assertEqual(self.receipt()['error'], 'RuntimeError')
        self.assertEqual(self.receipt()['recorded'], 1)
        self.assertEqual([r['run_id'] for r in self.snapshots()], [2])


class RequestBudgetTests(unittest.TestCase):
    def client(self, **kwargs):
        with patch('collector.gh._token', return_value='test'):
            return refresh.RefreshGitHub(**kwargs)

    def test_api_budget_includes_retries_and_raw_downloads(self):
        gh = self.client(max_calls=1)
        error = urllib.error.HTTPError('https://api.github.com/test', 500, 'error', {}, io.BytesIO())
        self.addCleanup(error.close)
        with patch('urllib.request.urlopen', side_effect=error) as request, patch('time.sleep'):
            with self.assertRaises(refresh.RefreshBudgetExceeded):
                gh.get('test')
        self.assertEqual(request.call_count, 1)
        with self.assertRaises(refresh.RefreshBudgetExceeded):
            gh.raw('test')

    def test_time_and_quota_exhaustion_never_wait_across_reset(self):
        gh = self.client(max_seconds=-1)
        with self.assertRaises(refresh.RefreshBudgetExceeded):
            gh.get('test')
        gh = self.client()
        error = urllib.error.HTTPError('https://api.github.com/test', 403, 'quota',
                                      {'x-ratelimit-remaining': '0'}, io.BytesIO())
        self.addCleanup(error.close)
        with patch('urllib.request.urlopen', side_effect=error), patch('time.sleep') as sleep:
            with self.assertRaises(RateLimitUnavailable):
                gh.get('test')
            sleep.assert_not_called()

    def test_network_errors_also_spend_request_budget(self):
        gh = self.client(max_calls=1)
        with patch('urllib.request.urlopen', side_effect=urllib.error.URLError('network')) as request, \
                patch('time.sleep'):
            with self.assertRaises(refresh.RefreshBudgetExceeded):
                gh.get('test')
        self.assertEqual(request.call_count, 1)
        self.assertEqual(gh.started_requests, 1)


if __name__ == '__main__':
    unittest.main()
