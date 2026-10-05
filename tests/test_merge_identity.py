import copy
import json
import unittest
import pathlib
import sqlite3
import tempfile
from unittest.mock import patch
from collector import build_db
from analysis.report import Report
from collector.merge_identity import apply_telemetry, identity, apply_dispatch_title
from collector.build_db import seconds


class IdentityTests(unittest.TestCase):
    def test_dispatch_workflow_source_sha_does_not_become_tested_sha(self):
        r = {"event": "repository_dispatch", "trigger": "repository_dispatch", "head_sha": "a" * 40}
        t = {"meta": {"merge_engine": "bors", "head_sha": "b" * 40, "base_sha": "c" * 40,
                      "batch_id": "7", "batch_members": json.dumps([{"pr": 42, "head_sha": "d" * 40}])}}
        apply_telemetry(r, t)
        self.assertEqual(r["head_sha"], "a" * 40)
        self.assertEqual(identity(r), ("bors", "b" * 40, "c" * 40, 7, [{"pr": 42, "head_sha": "d" * 40}]))

    def test_variable_cannot_relabel_a_queue_or_pr_run(self):
        for event, trigger in (("merge_group", "merge_queue"), ("pull_request_target", "pr")):
            r = {"event": event, "trigger": trigger, "head_sha": "a" * 40}
            before = copy.deepcopy(r)
            apply_telemetry(r, {"meta": {"merge_engine": "bors"}})
            self.assertEqual(r, before)

    def test_skipped_job_timestamp_order_never_creates_negative_cost(self):
        self.assertEqual(seconds("2026-10-04T01:00:01Z", "2026-10-04T01:00:00Z"), 0)
        self.assertEqual(seconds("2026-10-04T01:00:00Z", "2026-10-04T01:01:00Z"), 60)

    def test_derived_cost_counts_failures_and_deduplicates_actually_landed_members(self):
        source, tested = "a" * 40, "b" * 40
        runs = []
        for run_id, conclusion, head in ((1, "failure", "c" * 40), (2, "success", tested), (3, "success", tested)):
            runs.append({"repo": "TauCeti", "run_id": run_id, "run_attempt": 1,
                "event": "repository_dispatch", "trigger": "bors_staging", "head_sha": source,
                "workflow": ".github/workflows/pr-build.yml", "conclusion": conclusion,
                "created_at": "2026-10-04T01:00:00Z", "updated_at": "2026-10-04T01:01:00Z",
                "tested": {"head_sha": head, "base_sha": source, "batch_id": run_id,
                           "batch_members": [{"pr": 42, "head_sha": "d" * 40}]},
                "jobs": [{"id": run_id, "name": "publish-status", "conclusion": conclusion,
                          "started_at": "2026-10-04T01:00:00Z", "completed_at": "2026-10-04T01:01:00Z"}]})
        data = {"runs": runs, "main": [{"sha": h, "committed_at": "2026-10-04T01:02:00Z"} for h in (source, tested)]}
        with tempfile.TemporaryDirectory() as name:
            root = pathlib.Path(name)
            with patch.object(build_db, "ROOT", root), patch.object(build_db, "RECORDS", root / "records"), \
                    patch.object(build_db, "records", side_effect=lambda kind: iter(data.get(kind, []))):
                build_db.build(root / "ci.sqlite")
            db = sqlite3.connect(root / "ci.sqlite")
            self.assertEqual(db.execute("SELECT landed FROM merge_builds WHERE run_id=1").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT head_sha FROM merge_builds WHERE run_id=2").fetchone()[0], tested)
            report = Report(db)
            report.merge_backends()
            self.assertIn("| bors | 3 | 1 | 3.0 | 0.0 | 3.0 | 3.0 | 1 | 0 |", "\n".join(report.parts))
            db.close()

    def test_title_preserves_main_cost_identity_without_an_artifact_and_excludes_pilots(self):
        r = {"event": "repository_dispatch", "workflow": ".github/workflows/pr-build.yml", "head_sha": "a" * 40}
        apply_dispatch_title(r, f"bors branch=main batch=7 head={'b' * 40} base={'c' * 40}")
        self.assertEqual(identity(r), ("bors", "b" * 40, "c" * 40, 7, []))
        apply_dispatch_title(r, f"bors branch=pilot batch=7 head={'b' * 40} base={'c' * 40}")
        self.assertIsNone(identity(r))
        self.assertEqual(r["trigger"], "bors_pilot")
        apply_dispatch_title(r, "pr-build")
        self.assertIsNone(identity(r))
        self.assertEqual(r["trigger"], "bors_unknown")

    def test_missing_dispatch_identity_stays_unknown(self):
        r = {"event": "repository_dispatch", "trigger": "repository_dispatch", "head_sha": "a" * 40}
        apply_telemetry(r, {"meta": {"merge_engine": "bors", "head_sha": "bad"}})
        self.assertIsNone(identity(r))


if __name__ == "__main__":
    unittest.main()
