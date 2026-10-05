import json
import pathlib
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from analysis.report import Report
from collector import build_db


def run(number, head, trigger="bors_staging", attempt=1, start="01:01", end="01:02"):
    value = {"repo": "TauCeti", "run_id": number, "run_attempt": attempt,
             "event": "repository_dispatch", "trigger": trigger,
             "head_sha": "a" * 40, "workflow": ".github/workflows/pr-build.yml",
             "conclusion": "success", "created_at": f"2026-10-04T{start}:00Z",
             "updated_at": f"2026-10-04T{end}:00Z",
             "tested": {"head_sha": head, "base_sha": "a" * 40, "batch_id": number,
                        "batch_members": [{"pr": 42, "head_sha": "d" * 40}]},
             "jobs": [{"id": number * 10 + attempt, "run_attempt": attempt, "name": "build",
                       "started_at": f"2026-10-04T{start}:00Z",
                       "completed_at": f"2026-10-04T{end}:00Z"}]}
    if trigger == "main":
        value.update(event="push", head_sha=head, workflow=".github/workflows/ci.yml", tested={})
    return value


class ReportingTests(unittest.TestCase):
    def database(self, runs, main=(), observations=()):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = pathlib.Path(directory.name)
        data = {"runs": runs, "main": main, "observations": observations}
        with patch.object(build_db, "ROOT", root), patch.object(build_db, "RECORDS", root / "records"), \
                patch.object(build_db, "records", side_effect=lambda kind: iter(data.get(kind, []))):
            build_db.build(root / "ci.sqlite")
        db = sqlite3.connect(root / "ci.sqlite")
        self.addCleanup(db.close)
        return db

    def report(self, db):
        report = Report(db)
        report.merge_backends()
        return "\n".join(report.parts)

    def test_push_cost_is_counted_once_despite_duplicate_validation_and_all_rerun_jobs(self):
        head = "b" * 40
        pushes = [run(3, head, "main", start="01:15", end="01:18"),
                  run(3, head, "main", attempt=2, start="02:15", end="02:17")]
        pushes[0]["conclusion"] = "failure"
        # GitHub keeps the original run creation time on rerun.
        pushes[1]["created_at"] = pushes[0]["created_at"]
        db = self.database([run(1, head), run(2, head), *pushes])
        self.assertEqual(db.execute("SELECT COUNT(*) FROM post_merge_runs").fetchone()[0], 1)
        self.assertEqual(db.execute("SELECT landed_at FROM main_landings WHERE head_sha=?", (head,))
                         .fetchone()[0], "2026-10-04T01:15:00Z")
        self.assertIn("| bors | 2 | 1 | 2.0 | 5.0 | 7.0 | 7.0 |", self.report(db))

    def test_commit_creation_is_never_used_as_landing_time_and_cancelled_push_is_evidence(self):
        head = "b" * 40
        push = run(2, head, "main", start="01:15", end="01:15")
        push.update(conclusion="cancelled", jobs=[])
        observations = [{"observed_at": "2026-10-04T01:00:00Z", "backend": "bors",
                         "updated_at": "2026-10-04T01:00:00Z", "github_count": 0, "bors_count": 1,
                         "eligible": [{"pr": 42, "head_sha": "d" * 40}]},
                        {"observed_at": "2026-10-04T01:20:00Z", "backend": "queue",
                         "updated_at": "2026-10-04T01:20:00Z", "github_count": 0, "bors_count": 0}]
        main = [{"sha": head, "committed_at": "2026-10-03T12:00:00Z"}]
        db = self.database([run(1, head), push], main, observations)
        report = self.report(db)
        self.assertIn("| bors | 1 | 15.0 | 15.0 |", report)
        self.assertIn("upper bound", report)

    def test_missing_push_evidence_retains_merge_but_has_no_invented_latency(self):
        head = "b" * 40
        db = self.database([run(1, head)], [{"sha": head, "committed_at": "2026-10-04T01:00:00Z"}])
        self.assertEqual(db.execute("SELECT landed FROM merge_builds").fetchone()[0], 1)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM main_landings").fetchone()[0], 0)

    def test_coalesced_push_proves_tested_prefix_landed_and_handles_cyclic_bad_history(self):
        first, second = "b" * 40, "c" * 40
        main = [{"sha": first, "committed_at": "2026-10-04T01:00:00Z", "parents": [second]},
                {"sha": second, "committed_at": "2026-10-04T01:00:00Z", "parents": [first]}]
        db = self.database([run(1, first), run(2, second), run(3, second, "main", start="01:15")], main)
        self.assertEqual(db.execute("SELECT landed_at,source FROM main_landings WHERE head_sha=?",
                                    (first,)).fetchone(),
                         ("2026-10-04T01:15:00Z", "main-push-ancestor-bound"))

    def test_unattributed_and_ambiguous_main_push_cost_stays_separate(self):
        head = "b" * 40
        queue = run(2, head)
        queue.update(event="merge_group", trigger="merge_queue", head_sha=head,
                     tested={"queue_pr": 42, "base_sha": "a" * 40})
        db = self.database([run(1, head), queue, run(3, head, "main"),
                            run(4, "e" * 40, "main")])
        self.assertEqual(db.execute("SELECT COUNT(*) FROM post_merge_runs WHERE engine IS NULL")
                         .fetchone()[0], 2)
        self.assertIn("Unattributed or ambiguous main-push cost: 2.0", self.report(db))

    def test_dispatch_main_workflow_source_is_not_landing_evidence_for_tested_head(self):
        db = self.database([run(1, "b" * 40), run(2, "a" * 40, "main")])
        self.assertEqual(db.execute("SELECT landed FROM merge_builds").fetchone()[0], 0)

    def test_experiment_latest_snapshot_and_boundary_costs(self):
        plan = {"id": "daily", "phase": "queue", "requested_at": "2026-10-04T01:00:00Z",
                "bors_started_at": "2026-10-04T01:02:00Z",
                "bors_ended_at": "2026-10-04T01:03:00Z",
                "queue_requested_at": "2026-10-04T01:03:00Z",
                "queue_started_at": "2026-10-04T01:04:00Z"}
        obs = [{"observed_at": at, "backend": "queue", "pending": pending,
                "eligible": [{"pr": 42, "head_sha": "d" * 40}], "experiment": experiment}
               for at, pending, experiment in [
                   ("2026-10-04T01:00:00Z", 5, {"id": "daily", "phase": "draining_to_bors"}),
                   ("2026-10-04T01:02:00Z", 3, plan),
                   ("2026-10-04T01:06:00Z", 1, plan)]]
        db = self.database([run(1, "b"*40, start="01:01", end="01:04"),
                            run(2, "b"*40, "main", start="01:03", end="01:05")], observations=obs)
        self.assertEqual(db.execute("SELECT observed_at FROM merge_experiments").fetchone()[0],
                         "2026-10-04T01:06:00Z")
        result = self.report(db)
        self.assertIn("| draining to bors | closed | bors | 0.03 | 0 | 0.00 | 1.0 | 0.0 | 1.0 |", result)
        self.assertIn("| bors | closed | bors | 0.02 | 0 | 0.00 | 1.0 | 0.0 | 1.0 |", result)
        self.assertIn("| draining to queue | closed | bors | 0.02 | 1 | 60.00 | 1.0 | 1.0 | 2.0 |", result)
        self.assertIn("| queue | open | bors | 0.03 | 0 | 0.00 | 0.0 | 1.0 | 1.0 |", result)
        self.assertIn("pending heads at first/last sample 3/3", result)

    def test_other_repository_push_never_changes_tauceti_landing_or_cost(self):
        push = run(2, "b" * 40, "main")
        push["repo"] = "bors-ng"
        db = self.database([run(1, "b" * 40), push])
        self.assertEqual(db.execute("SELECT COUNT(*) FROM main_landings").fetchone()[0], 0)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM post_merge_runs").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
