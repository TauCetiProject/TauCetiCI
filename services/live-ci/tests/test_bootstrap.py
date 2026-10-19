import contextlib
import importlib.util
import io
import json
import sqlite3
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

SERVICE = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "bootstrap", SERVICE / "scripts/bootstrap.py"
)
bootstrap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bootstrap)
sys.path.insert(0, str(SERVICE.parents[1]))
from collector.build_db import SCHEMA


class BootstrapTests(unittest.TestCase):
    def test_day_interleaving_preserves_every_historical_event(self):
        with contextlib.closing(sqlite3.connect(":memory:")) as db:
            db.executescript(SCHEMA)
            for day in range(1, 9):
                for offset in range(2):
                    identity = day * 10 + offset
                    created = f"2026-10-{day:02}T12:{offset:02}:00Z"
                    db.execute(
                        """INSERT INTO runs(repo,run_id,run_attempt,workflow,conclusion,
                        created_at,updated_at,jobs_fetched) VALUES ('TauCeti',?,1,'aux','success',?,?,1)""",
                        (identity, created, created),
                    )
                    db.execute(
                        """INSERT INTO jobs(repo,run_id,job_id,run_attempt,name,conclusion,
                        created_at,started_at,completed_at,run_s,runner_kind,labels)
                        VALUES ('TauCeti',?,?,1,'aux','success',?,?,?,0,'github','[]')""",
                        (identity, identity, created, created, created),
                    )
            serial = list(bootstrap.events(db, "2026-10-01", 1))
            interleaved = list(bootstrap.events(db, "2026-10-01", 4))
            self.assertEqual(len(interleaved), 16)
            self.assertEqual(
                len({r["run"]["created_at"][:10] for r in interleaved[:4]}), 4
            )
            canonical = lambda rows: sorted(json.dumps(r, sort_keys=True) for r in rows)
            self.assertEqual(canonical(serial), canonical(interleaved))

    def test_parallel_batches_resume_and_complete_only_after_acknowledgement(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sqlite3.connect(root / "source.sqlite").close()
            (root / "token").write_text("test-token")
            argv = [
                "bootstrap",
                "--db",
                str(root / "source.sqlite"),
                "--receipts",
                str(root / "receipts.sqlite"),
                "--token-file",
                str(root / "token"),
                "--concurrency",
                "2",
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

            with patch("sys.argv", argv), patch.object(
                bootstrap, "events", records
            ), patch.object(
                bootstrap.urllib.request, "urlopen", upstream
            ), contextlib.redirect_stdout(
                io.StringIO()
            ):
                bootstrap.main()
                self.assertEqual(len(batches), 3)
                bootstrap.main()
                self.assertEqual(len(batches), 3)
            with sqlite3.connect(root / "receipts.sqlite") as db:
                self.assertEqual(
                    db.execute("SELECT COUNT(*) FROM imported").fetchone()[0], 106
                )


if __name__ == "__main__":
    unittest.main()
