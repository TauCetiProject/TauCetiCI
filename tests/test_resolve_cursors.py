import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from collector.resolve_cursors import merge, resolve


class CursorTests(unittest.TestCase):
    def test_union_preserves_earlier_shared_watermarks_with_timezone_offsets(self):
        self.assertEqual(merge({'TauCeti': '2026-10-14T02:00:00Z', 'other': '2026-10-14T01:00:00Z'},
                               {'TauCeti': '2026-10-14T03:30:00+02:00'}),
                         {'TauCeti': '2026-10-14T03:30:00+02:00', 'other': '2026-10-14T01:00:00Z'})

    def test_malformed_cursors_are_not_guessed(self):
        for invalid in ([], {'TauCeti': 12}, {'TauCeti': 'today'}, {'TauCeti': '2026-10-14'}):
            with self.assertRaises((TypeError, ValueError)):
                merge({}, invalid)

    def test_only_cursor_conflicts_are_auto_resolved(self):
        for paths in (b'', b'records/runs/a.ndjson.gz\0', b'state/cursor.json\0README.md\0'):
            with patch('subprocess.check_output', return_value=paths), patch('subprocess.run') as run:
                with self.assertRaises(ValueError):
                    resolve()
                run.assert_not_called()

    def test_real_rebase_keeps_both_record_files_and_conservative_cursor(self):
        with tempfile.TemporaryDirectory() as directory:
            def git(*args, check=True):
                return subprocess.run(['git', '-C', directory, *args], check=check, capture_output=True)
            root = Path(directory)
            git('init', '-b', 'main')
            git('config', 'user.name', 'test')
            git('config', 'user.email', 'test@example.invalid')
            (root/'state').mkdir(); (root/'records').mkdir()
            (root/'records/base.json').write_text('{}')
            cursor = root/'state/observations-cursor.json'
            cursor.write_text(json.dumps({'TauCeti': '2026-10-14T00:00:00Z'}))
            git('add', '.'); git('commit', '-m', 'base'); git('branch', 'other')
            cursor.write_text(json.dumps({'TauCeti': '2026-10-14T02:00:00Z'}))
            (root/'records/left.json').write_text('{}')
            git('add', '.'); git('commit', '-m', 'left'); git('switch', 'other')
            cursor.write_text(json.dumps({'TauCeti': '2026-10-14T01:00:00Z'}))
            (root/'records/right.json').write_text('{}')
            git('add', '.'); git('commit', '-m', 'right')
            self.assertNotEqual(git('rebase', 'main', check=False).returncode, 0)
            script = Path(__file__).resolve().parent.parent/'collector/resolve_cursors.py'
            subprocess.run([sys.executable, str(script)], cwd=directory, check=True)
            git('-c', 'core.editor=true', 'rebase', '--continue')
            self.assertEqual(json.loads(cursor.read_text())['TauCeti'], '2026-10-14T01:00:00Z')
            self.assertTrue((root/'records/left.json').exists())
            self.assertTrue((root/'records/right.json').exists())


if __name__ == '__main__':
    unittest.main()
