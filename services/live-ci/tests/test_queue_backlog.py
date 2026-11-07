import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("queue_backlog", Path(__file__).resolve().parents[1] / "scripts/queue_backlog.py")
backlog = importlib.util.module_from_spec(spec)
spec.loader.exec_module(backlog)


def fixture():
    def event(identity, at, kind, **detail):
        return {"id": identity, "at": at, "kind": kind, "detail": detail}
    return {
        "captured_at": 300,
        "journal_bounds": {"first": 1, "last": 7, "count": 7},
        "journal": [
            event(1, 0, "setting", key="publication_enabled", value=True),
            event(2, 10, "approved", input_id=1, number=11),
            event(3, 50, "approved", input_id=2, number=22),
            event(4, 100, "approved", input_id=3, number=11),
            event(5, 130, "withdrawn", number=11),
            event(6, 180, "landed", members=[22]),
            event(7, 200, "approved", input_id=4, number=33),
        ],
        "inputs": [{"id": i, "repository": backlog.REPO, "number": pr, "state": state,
                    "approved_at": -10000} for i, pr, state in
                   [(1, 11, "cancelled"), (2, 22, "landed"), (3, 11, "cancelled"), (4, 33, "waiting")]],
        "batches": [{"id": 1, "repository": backlog.REPO, "state": "landed"}],
        "members": [{"number": 22, "batch_id": 1, "removed": False, "withdrawn": False}],
    }


class QueueBacklogTests(unittest.TestCase):
    def test_replay_counts_prs_not_heads_and_uses_recorded_admission_times(self):
        result = backlog.reconstruct(fixture(), 240)
        self.assertEqual([row["eligible_count"] for row in result["observations"]], [0, 2, 2, 0, 1])
        self.assertTrue(all(row["reconstructed"] for row in result["observations"]))
        self.assertEqual(result["replayed_current_count"], 1)

    def test_clock_inversion_omits_ambiguous_minute_instead_of_reordering_state(self):
        source = fixture()
        source["journal"][3]["at"] = 130
        source["journal"][4]["at"] = 110
        result = backlog.reconstruct(source, 240)
        row = result["observations"][2]
        self.assertFalse(row["candidate_scan_complete"])
        self.assertIsNone(row["candidate_snapshot_at"])
        self.assertEqual(result["replayed_current_count"], 1)

    def test_missing_state_transition_fails_snapshot_reconciliation(self):
        source = fixture()
        source["journal"] = [r for r in source["journal"] if r["kind"] != "withdrawn"]
        with self.assertRaisesRegex(ValueError, "does not match"):
            backlog.reconstruct(source, 240)

    def test_missing_approval_or_retired_journal_is_rejected(self):
        for change in ("approval", "journal"):
            source = fixture()
            if change == "approval":
                source["journal"] = [r for r in source["journal"] if r["id"] != 2]
            else:
                source["journal_bounds"]["first"] = 2
            with self.assertRaises(ValueError):
                backlog.reconstruct(source, 240)

    def test_scope_and_production_bounds_are_required(self):
        source = fixture()
        source["inputs"][0]["repository"] = "private/repo"
        with self.assertRaisesRegex(ValueError, "Mixed repository"):
            backlog.reconstruct(source, 240)
        source = fixture()
        source["journal"][0]["detail"]["value"] = False
        with self.assertRaisesRegex(ValueError, "production window"):
            backlog.reconstruct(source, 240)
        with self.assertRaisesRegex(ValueError, "production window"):
            backlog.reconstruct(fixture(), 301)


if __name__ == "__main__":
    unittest.main()
