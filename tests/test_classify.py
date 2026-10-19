import copy
from contextlib import closing
import pathlib
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from collector import build_db, classify


BUILD = "Build exact candidate under bwrap with workflow-pinned audits, offline"
SUCCESS_LINES = [
    "✔ [7737/7835] Unpacked TauCeti.Algebra.AlgebraicGroup.KilledByRank",
    "✔ [11519/11633] Built TauCeti.Geometry.Convex.Cone.ErrorBound (2.8s)",
    "✔ [20038/20151] Unpacked TauCeti.AlgebraicGeometry.GroupScheme.KilledByRank",
]


class ClassifierTests(unittest.TestCase):
    def test_source_option_check_is_policy_even_without_logs(self):
        step = "Reject option overrides and heartbeat instrumentation"
        for lines in (None, [], ["##[group]Run set -euo pipefail"]):
            with self.subTest(lines=lines):
                self.assertEqual(classify.classify("failure", step, lines), "policy")

    def test_successful_module_names_do_not_override_actual_failure(self):
        for expected, diagnostic in [
            ("lean-error", "error: TauCeti/A.lean:95:11: Unknown constant `example`"),
            ("lint-env", "LINT-ENV: FAIL — 1 new violation(s)"),
            ("lint-style", "error: found 1 new style error(s)! Try `lake exe lint-style --fix`"),
        ]:
            with self.subTest(expected=expected):
                self.assertEqual(classify.classify("failure", BUILD, [*SUCCESS_LINES, diagnostic]),
                                 expected)

    def test_success_and_shell_trace_lines_do_not_exhaust_excerpt_budget(self):
        log = "\n".join([
            *["2026-10-07T10:00:00Z " + line for line in SUCCESS_LINES * 20],
            "2026-10-07T10:00:00Z + tee --output-error=warn lake-build.log",
            "2026-10-07T10:00:00Z \x1b[36;1mecho '::error::not executed'\x1b[0m",
            "2026-10-07T10:00:00Z LINT-ENV: FAIL — 1 new violation(s)",
        ])
        self.assertEqual(classify.excerpt(log, limit=1), ["LINT-ENV: FAIL — 1 new violation(s)"])

    def test_watchdog_path_is_not_timeout_evidence(self):
        lines = ["/tmp/lean-watchdog-toolchain/bin/lean: cannot execute: No such file"]
        self.assertEqual(classify.classify("failure", BUILD, lines), "lean-error")

    def test_sigkill_is_not_proof_of_deadline_expiry(self):
        for kill in [
            '/tmp/lean-watchdog-toolchain/bin/lean: line 46: 114865 Killed timeout --signal=TERM',
            "error: Lean exited with code 137",
        ]:
            with self.subTest(kill=kill):
                lines = [kill, "error: Lean process for A.lean exceeded the 3000s wall-clock limit"]
                self.assertEqual(classify.classify("failure", BUILD, lines), "process-killed")

    def test_confirmed_timeouts_are_retained(self):
        for diagnostic in ["error: operation timed out", "error: exceeded the build deadline",
                           "error: Lean process for A.lean exceeded the 3000s wall-clock limit"]:
            with self.subTest(diagnostic=diagnostic):
                self.assertEqual(classify.classify("failure", BUILD, [diagnostic]), "timeout")
        self.assertEqual(classify.classify("timed_out", BUILD, SUCCESS_LINES), "timeout")

    def test_heartbeat_exhaustion_remains_a_lean_error(self):
        lines = [*SUCCESS_LINES, "error: TauCeti/A.lean:95:0: (deterministic) timeout at `isDefEq`, "
                 "maximum number of heartbeats (200000) has been reached"]
        self.assertEqual(classify.classify("failure", BUILD, lines), "lean-error")

    def test_cancelled_and_missing_logs_keep_their_meaning(self):
        self.assertEqual(classify.classify("cancelled", BUILD, SUCCESS_LINES), "cancelled")
        self.assertEqual(classify.classify("failure", BUILD, None), "build-unknown")
        self.assertIsNone(classify.classify("success", BUILD, SUCCESS_LINES))

    def test_database_reclassifies_archived_evidence_without_mutating_records(self):
        job = {"id": 10, "name": "sandboxed-build", "conclusion": "failure", "failure": {
            "class": "timeout", "classifier": 1, "failed_step": BUILD,
            "excerpt": [*SUCCESS_LINES, "LINT-ENV: FAIL — 1 new violation(s)"],
        }}
        missing = {**job, "id": 11, "failure": {**job["failure"], "excerpt": None}}
        record = {"repo": "TauCeti", "run_id": 1, "run_attempt": 1,
                  "event": "pull_request_target", "trigger": "pr", "head_sha": "a" * 40,
                  "workflow": ".github/workflows/pr-build.yml", "conclusion": "failure",
                  "created_at": "2026-10-07T10:00:00Z", "jobs": [job, missing]}
        for job_id, lines in [(12, None), (13, []),
                              (14, ["##[group]Run set -euo pipefail"])]:
            record["jobs"].append({**job, "id": job_id, "failure": {
                "class": "other", "classifier": 2,
                "failed_step": "Reject option overrides and heartbeat instrumentation",
                "excerpt": lines,
            }})
        original = copy.deepcopy(record)
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            with patch.object(build_db, "ROOT", root), \
                    patch.object(build_db, "RECORDS", root / "records"), \
                    patch.object(build_db, "records", side_effect=lambda kind: iter(
                        [record] if kind == "runs" else [])):
                build_db.build(root / "ci.sqlite")
            with closing(sqlite3.connect(root / "ci.sqlite")) as db:
                self.assertEqual(db.execute("SELECT job_id,failure_class FROM jobs ORDER BY job_id")
                                 .fetchall(), [(10, "lint-env"), (11, "timeout"),
                                              (12, "policy"), (13, "policy"), (14, "policy")])
                self.assertEqual(db.execute("SELECT excerpt FROM jobs WHERE job_id=10").fetchone()[0],
                                 "\n".join(job["failure"]["excerpt"]))
        self.assertEqual(record, original)
