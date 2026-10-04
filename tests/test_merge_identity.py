import copy
import json
import unittest
from collector.merge_identity import apply_telemetry, identity


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

    def test_missing_dispatch_identity_stays_unknown(self):
        r = {"event": "repository_dispatch", "trigger": "repository_dispatch", "head_sha": "a" * 40}
        apply_telemetry(r, {"meta": {"merge_engine": "bors", "head_sha": "bad"}})
        self.assertIsNone(identity(r))


if __name__ == "__main__":
    unittest.main()
