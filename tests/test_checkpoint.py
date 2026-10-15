"""Exercise publication against a real local remote; no network or credentials."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.remote = self.root/'remote.git'
        self.checkout = self.root/'checkout'
        self.git('init', '--bare', '-b', 'main', str(self.remote))
        self.git('clone', str(self.remote), str(self.checkout))
        self.git('config', 'user.name', 'test', cwd=self.checkout)
        self.git('config', 'user.email', 'test@example.invalid', cwd=self.checkout)
        for name in ('records', 'state'):
            (self.checkout/name).mkdir()
            (self.checkout/name/'base.json').write_text('{}')
        (self.checkout/'source.py').write_text('base\n')
        self.git('add', '.', cwd=self.checkout)
        self.git('commit', '-m', 'base', cwd=self.checkout)
        self.git('push', 'origin', 'main', cwd=self.checkout)

    def git(self, *args, cwd=None):
        return subprocess.run(['git', *args], cwd=cwd, capture_output=True, text=True, check=True)

    def checkpoint(self, success=True):
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parent.parent))
        result = subprocess.run([sys.executable, '-m', 'collector.checkpoint'], cwd=self.checkout,
                                env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode == 0, success, result.stdout+result.stderr)
        return result

    def test_publish_and_retry_an_already_committed_unpushed_batch(self):
        (self.checkout/'records/first.json').write_text('{"first":1}')
        self.checkpoint()
        self.assertEqual(self.git('--git-dir', str(self.remote), 'show', 'main:records/first.json').stdout,
                         '{"first":1}')
        (self.checkout/'records/second.json').write_text('{"second":2}')
        self.git('add', '.', cwd=self.checkout)
        self.git('commit', '-m', 'batch committed before a lost push', cwd=self.checkout)
        self.checkpoint()
        self.assertEqual(self.git('--git-dir', str(self.remote), 'show', 'main:records/second.json').stdout,
                         '{"second":2}')
        self.checkpoint()  # An empty checkpoint makes no extra commit.
        self.assertEqual(self.git('rev-list', '--count', 'HEAD', cwd=self.checkout).stdout.strip(), '3')

    def test_reject_committed_source_and_unrelated_staged_changes(self):
        before = self.git('--git-dir', str(self.remote), 'rev-parse', 'main').stdout
        (self.checkout/'source.py').write_text('unreviewed\n')
        self.git('add', '.', cwd=self.checkout)
        self.assertIn('only records/ and state/', self.checkpoint(success=False).stderr)
        self.git('commit', '-m', 'unreviewed source', cwd=self.checkout)
        self.assertIn('only records/ and state/', self.checkpoint(success=False).stderr)
        self.assertEqual(self.git('--git-dir', str(self.remote), 'rev-parse', 'main').stdout, before)

    def test_preserve_concurrent_append_only_writer(self):
        other = self.root/'other'
        self.git('clone', str(self.remote), str(other))
        self.git('config', 'user.name', 'test', cwd=other)
        self.git('config', 'user.email', 'test@example.invalid', cwd=other)
        (other/'records/other.json').write_text('{}')
        self.git('add', '.', cwd=other)
        self.git('commit', '-m', 'another writer', cwd=other)
        self.git('push', 'origin', 'main', cwd=other)
        (self.checkout/'records/own.json').write_text('{}')
        self.checkpoint()
        for name in ('own', 'other'):
            self.assertEqual(self.git('--git-dir', str(self.remote), 'show', f'main:records/{name}.json').stdout, '{}')


if __name__ == '__main__':
    unittest.main()
