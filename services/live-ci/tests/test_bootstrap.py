import contextlib
import importlib.util
import io
import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

SERVICE = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("bootstrap", SERVICE / "scripts/bootstrap.py")
bootstrap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bootstrap)


class BootstrapTests(unittest.TestCase):
    def test_parallel_batches_resume_and_complete_only_after_acknowledgement(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sqlite3.connect(root / "source.sqlite").close()
            (root / "token").write_text("test-token")
            argv = [
                "bootstrap", "--db", str(root / "source.sqlite"),
                "--receipts", str(root / "receipts.sqlite"),
                "--token-file", str(root / "token"), "--concurrency", "2",
            ]
            barrier = threading.Barrier(2)
            lock = threading.Lock()
            completed = set()
            batches = []

            def upstream(request, **kwargs):
                records = json.loads(request.data)
                if "bootstrap" in records[0]:
                    self.assertEqual(len(completed), 105)
                else:
                    with lock:
                        index = len(batches)
                        batches.append(records)
                    if index < 2:
                        barrier.wait(timeout=5)
                    with lock:
                        completed.update(e["sequence"] for e in records)
                return contextlib.closing(io.BytesIO(b"{}"))

            def records(*args):
                return iter({"sequence": i, "historical": True} for i in range(105))

            with patch("sys.argv", argv), patch.object(bootstrap, "events", records), \
                 patch.object(bootstrap.urllib.request, "urlopen", upstream), \
                 contextlib.redirect_stdout(io.StringIO()):
                bootstrap.main()
                self.assertEqual(len(batches), 3)
                bootstrap.main()
                self.assertEqual(len(batches), 3)
            with sqlite3.connect(root / "receipts.sqlite") as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM imported").fetchone()[0], 106)


if __name__ == "__main__":
    unittest.main()
