import json
import sqlite3
import unittest
from dataclasses import replace

from analysis.replay import Head, Model, calibration, capture, compare, duration, form, replay
from collector.build_db import SCHEMA


def head(n, **kwargs):
    return Head(n, f"{n:040x}", 0, 0, 10, **kwargs)


class ReplayTests(unittest.TestCase):
    def test_deterministic_across_input_order_and_complete_accounting(self):
        heads = [head(i) for i in range(70)]
        a = replay(heads, Model(), cap=16, policy="work-risk", proof_rate=.2)
        b = replay(list(reversed(heads)), Model(), cap=16, policy="work-risk", proof_rate=.2)
        self.assertEqual(a, b)
        self.assertEqual(a["landed"] + a["rejected"] + a["expired"] + a["deferred_infrastructure"], len(heads))
        self.assertGreater(a["costs"]["isolation_s"], 0)
        self.assertTrue(all(len(t["members"]) <= 16 for t in a["trace"]))

    def test_infrastructure_exhaustion_never_proof_quarantines(self):
        r = replay([head(i) for i in range(12)], Model(), cap=8, infra_rate=1, proof_rate=0)
        self.assertEqual(r["rejected"], 0)
        self.assertEqual(r["deferred_infrastructure"], 12)
        self.assertEqual(r["attempts"], 6)
        self.assertEqual(r["trace"][0]["members"], r["trace"][1]["members"])
        self.assertEqual(r["trace"][1]["members"], r["trace"][2]["members"])

    def test_atomic_oversize_and_proof_bisection_never_split_bundle(self):
        heads = [head(i, bundle="atomic") for i in range(9)] + [head(20)]
        r = replay(heads, Model(), cap=8, proof_rate=1, infra_rate=0)
        self.assertEqual(r["rejected"], 10)
        self.assertEqual(len(r["trace"][0]["members"]), 9)
        self.assertEqual(len(r["trace"][0]["units"]), 1)
        for t in r["trace"]:
            if heads[0].key in t["members"]:
                self.assertTrue(all(h.key in t["members"] for h in heads[:9]))

    def test_retry_is_frozen_while_new_heads_arrive(self):
        heads = [head(1), replace(head(2), lo=1, hi=1)]
        r = replay(heads, Model(fixed_s=10), proof_rate=1, infra_rate=0)
        self.assertEqual(r["trace"][0]["members"], [heads[0].key])
        r = replay(heads, Model(fixed_s=10), proof_rate=0, infra_rate=1)
        self.assertTrue(all(t["members"] == [heads[0].key] for t in r["trace"][:3]))

    def test_stale_head_and_bundle_expiry(self):
        a = head(1, stale_at=5, bundle="a")
        b = head(2, bundle="a")
        c = replace(head(3), hi=1, lo=1)
        r = replay([a, b, c], Model(fixed_s=10), proof_rate=0, infra_rate=0)
        self.assertEqual(r["expired"], 2)
        self.assertEqual(r["landed"], 1)
        self.assertGreater(r["costs"]["stale_s"], 0)

    def test_fairness_oldest_expensive_unit_anchors_and_overdue_bypass(self):
        old = replace(head(1, behind=500), work=10000)
        new = [replace(head(i), lo=10, hi=10) for i in range(2, 10)]
        units = [[old]] + [[h] for h in new]
        selected = form(units, 100, 8, "work-risk", "hi", Model(), 50, .5, 1)
        self.assertEqual(selected[0], [old])
        self.assertEqual(sum(map(len, selected)), 8)
        r = replay([old] + new, Model(), cap=1, policy="work-risk", proof_rate=0, infra_rate=0)
        self.assertEqual(r["trace"][0]["members"], [old.key])
        self.assertEqual(r["landed"], 9)

    def test_overlap_dependency_and_stale_cost_sensitivity(self):
        a = head(1, modules=("A.lean",))
        b = head(2, modules=("A.lean",))
        model = Model()
        self.assertLess(duration([[a], [b]], model, 1), duration([[a], [b]], model, 0))
        self.assertGreater(duration([[replace(a, behind=500)]], model), duration([[a]], model))
        self.assertEqual(duration([[replace(a, dependency=True)]], model), duration([[a]], model) * 3)

    def test_bundle_waits_for_all_members_and_sensitivity_changes_cost(self):
        a = head(1, bundle="a")
        b = replace(head(2, bundle="a"), lo=100, hi=200)
        r = replay([a, b], Model(), proof_rate=0, infra_rate=0)
        self.assertEqual(r["trace"][0]["start_s"], 200)
        fast = replay([a, b], Model(), proof_rate=0, infra_rate=0, work_scale=.5)
        slow = replay([a, b], Model(), proof_rate=0, infra_rate=0, work_scale=2)
        self.assertGreater(slow["runner_minutes_per_landed"], fast["runner_minutes_per_landed"])

    def test_scenario_matrix_and_disabled_policy(self):
        result = compare([head(i) for i in range(12)], Model(), {})
        self.assertFalse(result["adaptive_scheduling_enabled"])
        self.assertEqual(len(result["results"]), 11 * 2 * 7)
        self.assertEqual({r["cap"] for r in result["results"]}, {8, 16, 32, 128})
        self.assertEqual(result, compare([head(i) for i in range(12)], Model(), {}))
        self.assertNotIn('NaN', json.dumps(result, allow_nan=False))

    def test_empty_workload(self):
        r = replay([], Model())
        self.assertEqual(r["landed"], 0)
        self.assertIsNone(r["runner_minutes_per_landed"])


class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.executescript(SCHEMA)

    def tearDown(self):
        self.db.close()

    def obs(self, at, heads):
        self.db.execute("INSERT INTO merge_observations(observed_at,eligible_json) VALUES (?,?)",
                        (f"2026-10-14T{at}Z", json.dumps(heads)))

    def feature(self, available, pr, sha, paths, built=5):
        self.db.execute("INSERT INTO replay_features VALUES ('TauCeti',1,1,?,?,?,?)",
                        (pr, sha, f"2026-10-14T{available}Z", json.dumps({"diff": {"paths": paths},
                         "behind_by": 100, "build": {"jobs_by_verb": {"Built": built}}})))

    def test_bounds_backlog_missing_and_no_future_feature_leak(self):
        a = {"pr": 1, "head_sha": "a", "ready": True}
        b = {"pr": 2, "head_sha": "b", "ready": True}
        self.obs("00:00:00", [a])
        self.obs("00:10:00", [a, b])
        # Available in the uncertain interval: excluded from BOTH bound runs.
        self.feature("00:05:00", 2, "b", ["lake-manifest.json"])
        heads, model, c = capture(self.db)
        self.assertEqual(c["initial_left_censored_backlog"], 1)
        self.assertEqual(c["sample_gaps_over_120s"], 1)
        self.assertEqual(c["missing_features"], 2)
        self.assertEqual((heads[1].lo, heads[1].hi), (0, 600))
        self.assertIsNone(heads[1].dependency)
        self.assertTrue(c["calibration"]["fallback"])

    def test_exact_head_change_staleness_and_dependency_features(self):
        self.feature("00:00:00", 1, "a", ["lake-manifest.json", "A.lean"])
        self.obs("00:01:00", [{"pr": 1, "head_sha": "a"}])
        self.obs("00:02:00", [{"pr": 1, "head_sha": "b"}])
        heads, _, c = capture(self.db)
        self.assertEqual(heads[0].stale_at, 60)
        self.assertTrue(heads[0].dependency)
        self.assertEqual(heads[0].modules, ("A.lean",))
        self.assertIsNone(heads[1].behind)
        self.assertEqual(c["unknown_dependency"], 1)

    def test_no_observations_nonfatal(self):
        heads, _, coverage = capture(self.db)
        self.assertFalse(heads)
        self.assertIn("unavailable", coverage)

    def test_training_excludes_future_failures_unknown_and_dependency_cohorts(self):
        for n, cohort, conclusion, date in [
                (1, "ordinary", "success", "2026-10-12"),
                (2, "ordinary", "success", "2026-10-12"),
                (3, "ordinary", "success", "2026-10-12"),
                (4, "dependency-bump", "success", "2026-10-12"),
                (5, "ordinary", "cancelled", "2026-10-12"),
                (6, "unknown", "success", "2026-10-12"),
                (7, "ordinary", "failure", "2026-10-12"),
                (8, "ordinary", "success", "2026-10-15")]:
            at = date + "T00:00:00Z"
            self.db.execute("INSERT INTO runs(repo,run_id,run_attempt,conclusion,updated_at) VALUES ('TauCeti',?,1,?,?)", (n, conclusion, at))
            self.db.execute("INSERT INTO run_attempts VALUES ('TauCeti',?,1,?,?,NULL)", (n, conclusion, cohort))
            self.db.execute("INSERT INTO jobs(repo,run_id,job_id,run_attempt,name,run_s) VALUES ('TauCeti',?,?,1,'sandboxed-build',?)", (n, n, 100 + 10*n))
            self.db.execute("INSERT INTO merge_builds(repo,run_id,engine,members_json) VALUES ('TauCeti',?,'bors','[{\"pr\":1}]')", (n,))
            self.db.execute("INSERT INTO telemetry(repo,run_id,build_json) VALUES ('TauCeti',?,?)", (n, json.dumps({"jobs_by_verb": {"Built": n}})))
        model, evidence = calibration(self.db, "2026-10-14T00:00:00Z")
        self.assertEqual(evidence["run_ids"], [1, 2, 3])
        self.assertFalse(evidence["fallback"])
        self.assertAlmostEqual(model.fixed_s, 100)
        self.assertAlmostEqual(model.per_work_s, 10)
        self.assertEqual(evidence["conclusions"]["infrastructure"], 1)


if __name__ == "__main__":
    unittest.main()
