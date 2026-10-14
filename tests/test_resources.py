import io
import json
import unittest
import zipfile
from unittest.mock import Mock

from analysis.report import Report
from collector.collect import job_record, run_record
from collector.resources import observations, cohort
import test_merge_reporting
from test_merge_reporting import run


def archive(payload, name='ci-host-resources.json'):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w') as z:
        z.writestr(name, json.dumps(payload))
    return buffer.getvalue()


class CollectionTests(unittest.TestCase):
    def setUp(self):
        self.gh = Mock()
        self.run = {'id': 4, 'run_attempt': 2}
        self.payload = {'schema': 'tauceti-ci.host-resources/v1', 'trust': 'host',
                        'run_id': '4', 'run_attempt': 2, 'samples': 2}
        self.gh.get.return_value = {'artifacts': [{'id': 7, 'name': 'ci-host-resources-2'}]}
        self.gh.raw.return_value = archive(self.payload)

    def test_matching_attempt_only_and_provenance(self):
        self.assertEqual(observations(self.gh, 'TauCeti', self.run)['resources']['coverage'], 'present')
        self.run['run_attempt'] = 3
        self.assertEqual(observations(self.gh, 'TauCeti', self.run)['resources']['coverage'], 'missing')
        self.gh.raw.assert_called_once()
        self.run['run_attempt'] = 2
        for field, value in [('trust', 'sandbox'), ('run_id', '5'), ('run_attempt', 1)]:
            self.gh.raw.return_value = archive({**self.payload, field: value})
            self.assertEqual(observations(self.gh, 'TauCeti', self.run)['resources']['coverage'], 'invalid-or-unavailable')

    def test_missing_expired_partial_invalid_and_api_failure_are_gaps(self):
        self.gh.get.return_value = {'artifacts': []}
        self.assertEqual(observations(self.gh, 'TauCeti', self.run)['resources']['coverage'], 'missing')
        self.gh.get.return_value = {'artifacts': [{'id': 7, 'name': 'ci-host-resources-2', 'expired': True}]}
        self.assertEqual(observations(self.gh, 'TauCeti', self.run)['resources']['coverage'], 'expired')
        self.gh.get.return_value['artifacts'][0]['expired'] = False
        self.gh.raw.return_value = b'partial zip'
        self.assertEqual(observations(self.gh, 'TauCeti', self.run)['resources']['coverage'], 'invalid-or-unavailable')
        self.gh.get.side_effect = RuntimeError('API failed')
        self.assertEqual(observations(self.gh, 'TauCeti', self.run)['resources']['coverage'], 'unavailable')

    def test_oversized_zip_payload_is_not_decompressed(self):
        self.gh.get.return_value['artifacts'][0]['size_in_bytes'] = 2**21
        self.assertEqual(observations(self.gh, 'TauCeti', self.run)['resources']['coverage'], 'invalid-or-unavailable')
        self.gh.raw.assert_not_called()

    def test_bad_counter_shapes_are_nonfatal(self):
        for value in (42, {'cpu': []}, {'cpu': {'some': -1}}, {'cpu': {'some': float('nan')}}):
            self.gh.raw.return_value = archive({**self.payload, 'host_pressure_stall_us': value})
            self.assertEqual(observations(self.gh, 'TauCeti', self.run)['resources']['coverage'], 'invalid-or-unavailable')

    def test_cohorts_never_claim_warm_cache_and_truncated_diff_is_unknown(self):
        self.assertEqual(cohort({'tested': {'diff': {'paths': ['lean-toolchain']}}}), 'dependency-bump (warmth unknown)')
        self.assertEqual(cohort({'tested': {'diff': {'paths': ['TauCeti/A.lean']}}}), 'ordinary')
        self.assertEqual(cohort({'tested': {'diff': {'paths': [], 'files_truncated': True}}}), 'unknown')
        self.assertEqual(cohort({}), 'unknown')

    def test_jobs_are_bound_to_attempt_api_paths(self):
        gh = Mock()
        gh.paginate.side_effect = [[{'id': 1, 'name': 'build', 'status': 'completed'}],
                                   [{'id': 2, 'name': 'build', 'status': 'completed'}]]
        value = {'id': 7, 'run_attempt': 2, 'path': '.github/workflows/ci.yml',
                 'conclusion': 'success', 'event': 'push', 'head_sha': 'a'*40,
                 'created_at': '2026-10-13T01:00:00Z'}
        record = run_record(gh, 'TauCeti', value, None, 'all')
        self.assertEqual([job['run_attempt'] for job in record['jobs']], [1, 2])
        self.assertTrue(gh.paginate.call_args_list[0].args[0].endswith('/attempts/1/jobs'))
        self.assertTrue(gh.paginate.call_args_list[1].args[0].endswith('/attempts/2/jobs'))

    def test_api_job_state_preserved(self):
        for state in ('queued', 'in_progress', 'completed'):
            self.assertEqual(job_record({'id': 1, 'name': 'build', 'status': state}, True)['status'], state)


class ResourceReportTests(unittest.TestCase):
    database = test_merge_reporting.ReportingTests.database
    def test_failure_cancel_partial_rerun_and_publication_separate_from_landing(self):
        runs = [run(1, 'b'*40), run(1, 'b'*40, attempt=2), run(2, 'c'*40), run(3, 'd'*40)]
        for value, conclusion in zip(runs, ('failure', 'success', 'cancelled', 'success')):
            value['conclusion'] = conclusion
            value['jobs'][0].update(conclusion=conclusion, status='completed')
        runs[1]['observations'] = {
            'resources': {'coverage': 'present', 'artifact_id': 7,
                'payload': {'dependency_bump': '1', 'samples': 2, 'command_cpu_user_s': 3,
                            'command_cpu_system_s': 1, 'command_max_process_rss_bytes': 2**20}},
            'publication': {'coverage': 'present', 'payload': {'complete': True, 'staged_objects': 3,
                             'staged_bytes': 40, 'uploaded': True, 'confirmed': False}}}
        runs[-1]['jobs'][0].update(status='queued', created_at='2026-10-04T01:00:00Z')
        db = self.database(runs)
        self.assertEqual(db.execute('SELECT COUNT(*) FROM run_attempts').fetchone()[0], 4)
        self.assertEqual(db.execute("SELECT run_s FROM jobs WHERE api_status='queued'").fetchone()[0], None)
        report = Report(db)
        report.resources()
        result = '\n'.join(report.parts)
        self.assertIn('dependency-bump (warmth unknown)', result)
        self.assertIn('cancelled', result)
        self.assertIn('0 public-map confirmations', result)
        self.assertIn('40 bytes and 3 objects', result)
        self.assertIn('historical-missing', result)
        self.assertIn('retry execution: 1.0 minutes', result)
        self.assertIn('Speculation waste is unknown', result)
        self.assertEqual(db.execute('SELECT COUNT(*) FROM main_landings').fetchone()[0], 0)

    def test_prior_jobs_without_an_attempt_snapshot_have_explicit_gaps(self):
        value = run(1, 'b'*40, attempt=2)
        value['jobs'].append({'id': 11, 'name': 'build', 'run_attempt': 1,
                              'conclusion': 'failure'})
        db = self.database([value])
        self.assertEqual(db.execute('SELECT conclusion,cohort FROM run_attempts WHERE run_attempt=1').fetchone(), (None, 'unknown'))
        self.assertEqual(db.execute("SELECT coverage FROM resource_observations WHERE run_attempt=1 AND kind='resources'").fetchone()[0], 'attempt-not-collected')
        self.assertIsNone(db.execute('SELECT workflow_sha FROM run_attempts WHERE run_attempt=2').fetchone()[0])

    def test_resource_values_never_override_ci_or_landing(self):
        value = run(1, 'b'*40)
        value['conclusion'] = 'failure'
        value['observations'] = {'resources': {'coverage': 'present', 'payload': {
            'command_cpu_user_s': 0, 'command_cpu_system_s': 0, 'ci_success': True, 'landed': True}}}
        db = self.database([value])
        self.assertEqual(db.execute('SELECT conclusion FROM runs').fetchone()[0], 'failure')
        self.assertEqual(db.execute('SELECT landed FROM merge_builds').fetchone()[0], 0)


if __name__ == '__main__':
    unittest.main()
