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
        self.assertEqual(config["vars"]["GITHUB_HOURLY_BUDGET"], "500")

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


if __name__ == "__main__":
    unittest.main()
