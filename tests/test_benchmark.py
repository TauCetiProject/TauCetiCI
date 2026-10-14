import copy
import json
from pathlib import Path
import tempfile
import unittest

from benchmarks.experiment import authorize, inventory, plan, slots, summarize


class BenchmarkTests(unittest.TestCase):
    def test_budget_and_unique_cells(self):
        p = plan()
        self.assertEqual(len(slots(p)), 18)
        self.assertEqual(len({s['slot'] for s in slots(p)}), 18)
        for w in p['workloads']:
            for shape in p['shapes']:
                self.assertEqual(sum(s['workload'] == w and s['shape'] == shape for s in slots(p)), 2)

    def test_cap_rejects_rerun_later_dispatch_and_branch(self):
        runs = [{'id': 12}, {'id': 11}]
        authorize(runs, 11, 1, 'refs/heads/main')
        for args in [(runs, 12, 1, 'refs/heads/main'), (runs, 11, 2, 'refs/heads/main'),
                     (runs, 11, 1, 'refs/heads/experiment'), ([], 11, 1, 'refs/heads/main')]:
            with self.assertRaises(ValueError):
                authorize(*args)

    def test_cache_inventory_detects_byte_and_mapping_drift(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            with self.assertRaises(ValueError):
                inventory(root)
            (root / 'artifacts').mkdir()
            (root / 'outputs/TauCeti').mkdir(parents=True)
            mapping = root / 'outputs/TauCeti/aaaaaaaaaaaaaaaa.json'
            mapping.write_text('{"data":"bbbbbbbbbbbbbbbb.ltar"}')
            f = root / 'artifacts/bbbbbbbbbbbbbbbb.ltar'
            f.write_bytes(b'a')
            one = inventory(root)
            f.with_suffix('.ltar.hash').write_text('ignored sidecar')
            self.assertEqual(one, inventory(root))
            f.write_bytes(b'b')
            self.assertNotEqual(one, inventory(root))
            (root / 'artifacts/unrelated.ltar').write_text('unrelated dependency build')
            (root / 'outputs/Mathlib').mkdir()
            (root / 'outputs/Mathlib/tool.json').write_text('{}')
            before = inventory(root)
            (root / 'artifacts/unrelated.ltar').write_text('different local tool build')
            self.assertEqual(inventory(root), before)
            self.assertEqual(inventory(root)[1], 2)

    def records(self):
        return [{**s, 'status': 'success', 'elapsed_s': 60, 'cache_identity': s['workload']} for s in slots(plan())]

    def test_missing_and_failed_stay_in_denominator(self):
        rows = self.records()
        rows.pop()
        rows[0]['status'] = 'failed'
        report = summarize(plan(), rows)
        self.assertFalse(report['complete'])
        self.assertEqual(len(report['slots']), 18)
        self.assertEqual(sum(r['status'] == 'missing' for r in report['slots']), 1)
        self.assertEqual(sum(g['successful'] for g in report['groups']), 16)

    def test_drift_invalidates_entire_workload(self):
        rows = self.records()
        rows[0]['cache_identity'] = 'changed'
        report = summarize(plan(), rows)
        self.assertFalse(report['complete'])
        self.assertEqual(sum(r['status'] == 'incomparable-cache' for r in report['slots']), 6)

    def test_cost_and_duplicate_accounting(self):
        rows = self.records()
        report = summarize(plan(), rows)
        self.assertTrue(report['complete'])
        self.assertEqual([g['median_unit_minutes'] for g in report['groups'][:3]], [8, 16, 16])
        with self.assertRaises(ValueError):
            summarize(plan(), rows + [rows[0]])
        with self.assertRaises(ValueError):
            summarize(plan(), [{'slot': 'unknown'}])

    def test_workflow_has_no_production_writers_and_both_attempt_guards(self):
        workflow = Path('.github/workflows/runner-benchmark.yml').read_text()
        self.assertNotIn('write', workflow.replace('cache-save or publication authority.', ''))
        self.assertNotIn('secrets.', workflow)
        self.assertNotIn('actions/cache', workflow)
        self.assertGreaterEqual(workflow.count('github.run_attempt == 1'), 3)
        script = Path('benchmarks/run.sh').read_text()
        self.assertIn('--setenv STAGE_LAKE_CACHE 0', script)
        self.assertIn('LAKE_CACHE_MAX_REVS=1', script)
        self.assertNotIn('cache put', script)
        self.assertIn('sandbox-build.sh', script)


if __name__ == '__main__':
    unittest.main()

class WholeJobTests(unittest.TestCase):
    def test_job_state_cost_and_queue_failures(self):
        p = plan()
        rows = BenchmarkTests().records()
        jobs = [dict(name='benchmark (' + s['slot'] + ')', id=i, status='completed', conclusion='success',
                     started_at='2026-10-14T00:50:00Z', completed_at='2026-10-14T01:02:00Z',
                     runner_name='runner', steps=[dict(started_at='2026-10-14T01:00:00Z')]) for i, s in enumerate(slots(p))]
        report = summarize(p, rows, jobs)
        self.assertEqual(report['groups'][0]['median_unit_minutes'], 16)
        self.assertEqual(report['groups'][1]['median_unit_minutes'], 32)
        self.assertEqual(report['groups'][0]['median_job_elapsed_s'], 120)
        report = summarize(p, [], jobs)
        self.assertFalse(report['complete'])
        self.assertEqual(report['slots'][0]['job']['conclusion'], 'success')
        self.assertEqual(report['slots'][0]['status'], 'missing')

    def test_recommendation_requires_complete_pair_and_cost_threshold(self):
        rows = BenchmarkTests().records()
        for row in rows:
            if row['shape'] == '16x32':
                row['elapsed_s'] = 30
        report = summarize(plan(), rows)
        self.assertTrue(all(r['shape'] == '16x32' for r in report['recommendations']))
        rows[2]['status'] = 'failed'
        report = summarize(plan(), rows)
        self.assertEqual(report['recommendations'][0]['shape'], '8x16')

class FailureEvidenceTests(unittest.TestCase):
    def test_truncated_result_is_accounted_without_losing_report(self):
        from benchmarks.experiment import read_results
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            cell = root / ('benchmark-' + slots(plan())[0]['slot'])
            cell.mkdir()
            (cell / 'result.json').write_text('{')
            report = summarize(plan(), read_results(root, plan()))
            self.assertEqual(report['slots'][0]['status'], 'corrupt-result')
            self.assertEqual(sum(r['status'] == 'missing' for r in report['slots']), 17)

    def test_job_failure_cannot_look_successful(self):
        rows = BenchmarkTests().records()
        jobs = [dict(name=rows[0]['slot'], status='completed', conclusion='failure')]
        report = summarize(plan(), rows, jobs)
        self.assertEqual(report['slots'][0]['status'], 'failed-job')
        self.assertFalse(report['complete'])

    def test_worker_failure_writes_result_and_filters_credentials(self):
        import hashlib
        import os
        from unittest.mock import patch
        from benchmarks import worker
        p = copy.deepcopy(plan())
        mem = int(next(l.split()[1] for l in Path('/proc/meminfo').read_text().splitlines() if l.startswith('MemTotal:'))) * 1024
        p['shapes']['8x16']['memory_gb'] = mem / 10**9
        p['manifest_sha256'] = hashlib.sha256(b'{}').hexdigest()
        env = dict(SLOT=slots(p)[0]['slot'], GITHUB_RUN_ID='1', GITHUB_RUN_ATTEMPT='1',
                   HARNESS_SHA='a' * 40, GH_TOKEN='test-token', ACTIONS_RUNTIME_TOKEN='test-runtime')
        previous = Path.cwd()
        with tempfile.TemporaryDirectory() as d:
            try:
                os.chdir(d)
                Path('pr').mkdir()
                Path('pr/lake-manifest.json').write_text('{}')
                Path('pr/lean-toolchain').write_text(p['toolchain'])
                with patch.dict(os.environ, env), patch.object(worker, 'plan', return_value=p), \
                        patch.object(worker.os, 'sched_getaffinity', return_value=set(range(8))), \
                        patch.object(worker.subprocess, 'call', return_value=7) as call:
                    self.assertEqual(worker.main(), 1)
                sent = call.call_args.kwargs['env']
                self.assertNotIn('GH_TOKEN', sent)
                self.assertNotIn('ACTIONS_RUNTIME_TOKEN', sent)
                result = json.loads(Path('results/result.json').read_text())
                self.assertEqual(result['status'], 'failed')
                self.assertEqual(result['exit_code'], 7)
                self.assertGreaterEqual(result['estimated_unit_minutes'], 0)
            finally:
                os.chdir(previous)

class MapBindingTests(unittest.TestCase):
    def test_downloaded_root_mappings_must_equal_pinned_map(self):
        from benchmarks.experiment import verify_mappings
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            output = root / 'outputs/TauCeti'
            output.mkdir(parents=True)
            map_file = root / 'published.jsonl'
            map_file.write_text('"2026-03-17"\n["aaaaaaaaaaaaaaaa","bbbbbbbbbbbbbbbb.ltar"]\n')
            with self.assertRaises(ValueError):
                verify_mappings(root, map_file)
            f = output / 'aaaaaaaaaaaaaaaa.json'
            f.write_text('{"data":"bbbbbbbbbbbbbbbb.ltar"}')
            verify_mappings(root, map_file)
            f.write_text('{"data":"cccccccccccccccc.ltar"}')
            with self.assertRaises(ValueError):
                verify_mappings(root, map_file)

class QueueCostTests(unittest.TestCase):
    def test_cancelled_unallocated_job_is_not_billed_as_execution(self):
        p = plan()
        slot = slots(p)[0]['slot']
        job = dict(name=slot, status='completed', conclusion='cancelled', runner_name=None,
                   started_at='2026-10-14T01:00:00Z', completed_at='2026-10-14T01:20:00Z', steps=[])
        row = summarize(p, [], [job])['slots'][0]
        self.assertNotIn('job_estimated_unit_minutes', row)
        self.assertEqual(row['job_timing_coverage'], 'unallocated-or-timing-unavailable')
        self.assertEqual(row['status'], 'missing')
