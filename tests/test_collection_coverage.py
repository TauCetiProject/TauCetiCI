import contextlib
import io
import json
import zipfile
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, Mock

from collector import build_db, collect, incremental as inc
import test_incremental as fixtures


class CoverageTests(unittest.TestCase):
    def test_published_coverage_excludes_archived_repositories_and_exposes_details_and_gaps(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); (root/'state').mkdir()
            (root/'state/cursor.json').write_text(json.dumps({'TauCeti':'2026-10-15T01:00:00Z','Archived':'2026-10-01'}))
            (root/'state/repositories.json').write_text(json.dumps({'active':['TauCeti']}))
            (root/'state/details-cursor.json').write_text(json.dumps({'TauCeti':'2026-10-15T00:30:00Z'}))
            value=inc.inventory_record('TauCeti',fixtures.run())
            value['collection_stage']='jobs'
            value['jobs_gap']={'reason':'deleted'}
            data={'jobs':[value],'attempt-gaps':[{'repo':'TauCeti','run_id':1,'run_attempt':0,
                       'created_at':value['created_at'],'reason':'attempt unavailable'}]}
            with patch.object(build_db,'ROOT',root),patch.object(build_db,'RECORDS',root/'records'), \
                    patch.object(build_db,'records',side_effect=lambda kind: iter(data.get(kind,[]))):
                build_db.build(root/'db.sqlite')
            with contextlib.closing(sqlite3.connect(root/'db.sqlite')) as db:
                self.assertEqual(db.execute('SELECT repo FROM coverage').fetchall(),[('TauCeti',)])
                self.assertEqual(db.execute("SELECT complete_to FROM stream_coverage WHERE stream='details'").fetchone()[0],
                                 '2026-10-15T00:30:00Z')
                self.assertEqual(db.execute('SELECT COUNT(*) FROM collection_gaps').fetchone()[0],2)
                self.assertEqual(db.execute('SELECT DISTINCT coverage FROM resource_observations WHERE run_attempt=1').fetchall(),
                                 [('pending-enrichment',)])
                self.assertEqual(db.execute('SELECT DISTINCT coverage FROM resource_observations WHERE run_attempt=0').fetchall(),
                                 [('attempt-unavailable',)])

    def test_fixed_name_telemetry_cannot_be_borrowed_from_another_attempt(self):
        buffer=io.BytesIO()
        with zipfile.ZipFile(buffer,'w') as archive:
            archive.writestr('telemetry.json',json.dumps({'schema':'tauceti-ci.telemetry/v1'}))
        gh=Mock()
        gh.get.return_value={'artifacts':[
            {'id':1,'created_at':'2026-10-15T00:02:00Z'},
            {'id':2,'created_at':'2026-10-15T00:12:00Z'}]}
        gh.raw.return_value=buffer.getvalue()
        source=fixtures.run(attempt=2)
        source.update(run_started_at='2026-10-15T00:10:00Z',updated_at='2026-10-15T00:15:00Z')
        record=collect.Enricher(gh,'TauCeti').telemetry(source)
        self.assertIsNotNone(record)
        gh.raw.assert_called_once_with('repos/TauCetiProject/TauCeti/actions/artifacts/2/zip')

    def test_actual_remaining_quota_caps_this_invocation_and_preserves_a_reserve(self):
        with patch('collector.gh._token',return_value='test'):
            gh=inc.BoundedGitHub(4500,100)
        def quota(path):
            gh.before_request()
            return {'resources':{'core':{'remaining':300}}}
        with patch.object(gh,'get',side_effect=quota):
            gh.fit_quota()
        self.assertEqual(gh.max_calls,101)  # Quota lookup plus 100 requests, 200 reserved.


class ActiveRunTests(unittest.TestCase):
    setUp=fixtures.IncrementalTests.setUp
    inventory=fixtures.IncrementalTests.inventory
    read=fixtures.IncrementalTests.read

    def test_long_running_workflow_is_refreshed_after_the_normal_overlap(self):
        value=fixtures.run(status='in_progress',conclusion=None,stamp='2026-10-14T01:00:00Z')
        self.inventory(value)
        gh=Mock(); gh.get.return_value={**value,'status':'completed','conclusion':'failure','updated_at':'2026-10-15T00:20:00Z'}
        inc.refresh_active(gh,self.index,'active',self.checkpoint,self.until)
        self.assertEqual(len(self.index.pending(build=True)),1)
        self.assertTrue(any(r['conclusion']=='failure' for r in self.read('inventory')))
        gh.get.reset_mock()
        inc.refresh_active(gh,self.index,'again',self.checkpoint,self.until)
        gh.get.assert_not_called()

    def test_sampled_job_work_can_be_enabled_after_a_build_only_inventory(self):
        value=fixtures.run(4,workflow='.github/workflows/label.yml')
        inc.persist(self.index,'inventory','TauCeti',[inc.inventory_record('TauCeti',value,'build')],'build-only')
        self.assertFalse(self.index.pending(build=False))
        inc.scan(fixtures.ListingAPI([value]),self.index,'TauCeti',self.since,self.until,'all',self.checkpoint)
        self.assertEqual(len(self.index.pending(build=False)),1)


if __name__ == '__main__':
    unittest.main()
