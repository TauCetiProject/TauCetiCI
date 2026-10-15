import datetime as dt
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from collector import collect, incremental as inc, observations


class ObservationPagesTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        for key, value in [('ROOT', root), ('RECORDS', root/'records')]:
            p = patch.object(collect, key, value)
            p.start(); self.addCleanup(p.stop)
        self.since = collect.parse_time('2026-10-15T01:00:00Z')
        self.values = [{'observed_at': collect.iso(self.since+dt.timedelta(minutes=n))} for n in range(130)]
        self.requests = []
        inc.save_state('observations-cursor.json', {'TauCeti': '2026-10-15T01:02:00Z'})

    def page(self, url, timeout):
        query = parse_qs(urlparse(url).query)
        cursor = query.get('cursor', ['0'])[0]
        self.requests.append(cursor)
        offset = int(cursor)
        truncated = offset+100 < len(self.values)
        return {'observations': self.values[offset:offset+100], 'truncated': truncated,
                'cursor': str(offset+100) if truncated else None}

    def collect(self, until):
        with patch.object(observations, 'read_json', side_effect=self.page):
            inc.observations(type('Clock', (), {'check': lambda _: None})(), 'test', lambda **kw: None, until)

    def test_replay_only_last_page_and_read_new_rows(self):
        until = self.since+dt.timedelta(minutes=125)
        self.collect(until)
        self.assertEqual(self.requests, ['0', '100'])
        self.assertEqual(inc.read_state('observations-pages.json'), {'day': '2026-10-15', 'cursor': '100'})
        self.requests.clear()
        self.values.extend({'observed_at': collect.iso(self.since+dt.timedelta(minutes=n))} for n in range(130, 151))
        self.collect(self.since+dt.timedelta(minutes=150))
        self.assertEqual(self.requests, ['100'])
        self.assertEqual(inc.read_state('observations-cursor.json')['TauCeti'], '2026-10-15T03:30:00Z')
        from gzip import open as gzopen
        rows = [json.loads(line) for p in collect.RECORDS.rglob('*.gz') for line in gzopen(p, 'rt')]
        self.assertIn('2026-10-15T03:30:00Z', {r['observed_at'] for r in rows})

    def test_pages_newer_than_the_snapshot_boundary_remain_replayable(self):
        self.values.extend({'observed_at': collect.iso(self.since+dt.timedelta(minutes=n))} for n in range(130, 350))
        self.collect(self.since+dt.timedelta(minutes=125))
        self.assertEqual(inc.read_state('observations-pages.json')['cursor'], '100')
        self.requests.clear()
        self.collect(self.since+dt.timedelta(minutes=349))
        self.assertEqual(self.requests, ['100', '200', '300'])

    def test_interrupted_later_page_resumes_after_durable_prefix(self):
        def failing(url, timeout):
            if 'cursor=' in url:
                raise RuntimeError('interruption')
            return self.page(url, timeout)
        with patch.object(observations, 'read_json', side_effect=failing), self.assertRaises(RuntimeError):
            inc.observations(type('Clock', (), {'check': lambda _: None})(), 'test', lambda **kw: None,
                             self.since+dt.timedelta(hours=3))
        self.assertEqual(inc.read_state('observations-pages.json')['cursor'], '100')
        self.assertEqual(inc.read_state('observations-cursor.json'), {'TauCeti': '2026-10-15T01:02:00Z'})
        self.requests.clear()
        self.collect(self.since+dt.timedelta(hours=3))
        self.assertEqual(self.requests, ['100'])


if __name__ == '__main__':
    unittest.main()
