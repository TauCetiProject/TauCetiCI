import importlib.util
import json
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SERVICE = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("setup", SERVICE / "scripts/setup.py")
setup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(setup)


class SetupTests(unittest.TestCase):
    def test_config_is_parseable_and_resources_use_the_tauceti_account(self):
        config = json.loads((SERVICE / "wrangler.json").read_text())
        self.assertEqual(config["account_id"], setup.ACCOUNT)
        self.assertEqual(config["vars"]["PUBLICATION_MODE"], "shadow")
        self.assertEqual(config["vars"]["GITHUB_HOURLY_BUDGET"], "2000")

    def test_prepare_preserves_credentials_and_uses_private_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            with patch.object(setup, "DIRECTORY", path):
                setup.prepare()
                original = (path / "admin-token").read_text()
                setup.prepare()
            self.assertEqual((path / "admin-token").read_text(), original)
            for file in path.iterdir():
                self.assertEqual(stat.S_IMODE(file.stat().st_mode), 0o600)

    def test_operations_scope_rejects_private_inventory_fields_and_unknown_repositories(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = {"vars": {"ALLOWED_REPOSITORIES": json.dumps(["TauCetiProject/TauCeti"])}}
            (root / "operations-repositories.json").write_text(json.dumps(["Private/NotSelected"]))
            with patch.object(setup, "DIRECTORY", root), self.assertRaises(SystemExit): setup.operations_config(config)
            (root / "operations-repositories.json").write_text(json.dumps(["TauCetiProject/TauCeti"]))
            host = {"machine_id": "a" * 32, "alias": "yael", "target": "private-ssh"}
            (root / "operations-hosts.json").write_text(json.dumps([host]))
            with patch.object(setup, "DIRECTORY", root), self.assertRaises(SystemExit): setup.operations_config(config)
            del host["target"]
            (root / "operations-hosts.json").write_text(json.dumps([host]))
            with patch.object(setup, "DIRECTORY", root): setup.operations_config(config)
            self.assertEqual(json.loads(config["vars"]["OPERATIONS_PUBLIC_HOSTS"]), [host])


if __name__ == "__main__":
    unittest.main()
