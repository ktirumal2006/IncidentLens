"""Resume and diagnostic schedule checks without starting the benchmark stack."""
import copy
import fcntl
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

import stream_hardening as runner


class ResumeTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.output=Path(self.temp.name)
        self.provenance={
            'smoke':False,'probe_sustained':False,'candidate_tag':'candidate2',
            'declaration_sha256':'declaration','baseline_manifest_sha256':'frozen',
            'source_manifest':{'files':{'source':'same'}},
            'binary':{'path':'/bench','sha256':'binary'},
            'lag_binary':{'path':'/lag','sha256':'lag'},
            'images':{'candidate':{'worker':{'id':'candidate-id'}},
                      'baseline':{'worker':{'id':'baseline-id'}}},
        }
        self.record={**copy.deepcopy(self.provenance),'status':'interrupted','cases':[]}

    def add_case(self,index,status='completed'):
        name,rate,duration,warmup,rep,variant,label=runner.schedule(False)[index]
        directory=self.output/label
        directory.mkdir()
        entry={'label':label,'variant':variant,'path':'streaming',
               'repetition':rep,'project':'incidentlens-retro-test-'+str(index),
               'stage':runner.case_stage(name,rate,duration,warmup,directory,False),
               'status':status}
        if status=='completed':
            (directory/'analysis-exact.json').write_text('{"status":"observed"}')
            entry['retained_volumes']={'before':['volume-'+str(index)],
                                       'after':['volume-'+str(index)]}
        self.record['cases'].append(entry)
        return directory

    def test_full_schedule_reverses_second_repetition_and_probe_is_one_pair(self):
        full=runner.schedule(False)
        self.assertEqual(len(full),16)
        self.assertEqual([row[5] for row in full[:2]],['baseline','candidate'])
        self.assertEqual([row[5] for row in full[8:10]],['candidate','baseline'])
        probe=runner.schedule(False,True)
        self.assertEqual([(row[0],row[1],row[2],row[3],row[5]) for row in probe],
                         [('sustained-8000',8000,120,10,'baseline'),
                          ('sustained-8000',8000,120,10,'candidate')])

    def test_query_index_is_candidate_only_and_resume_rejects_schema_drift(self):
        overlay=runner.ROOT/'deploy/query/trace-index.yaml'
        self.assertNotIn(overlay,runner.case_overlays('baseline',True))
        self.assertNotIn(overlay,runner.case_overlays('candidate',False))
        self.assertIn(overlay,runner.case_overlays('candidate',True))
        changed={**self.provenance,'candidate_query_index':True}
        with self.assertRaisesRegex(ValueError,'candidate_query_index'):
            runner.resume_position(self.output,self.record,changed)
        self.record['candidate_query_index']=True
        self.assertEqual(runner.resume_position(self.output,self.record,changed),0)
        index=[{'name':'trace_id_bloom','expr':'trace_id','type':'bloom_filter','granularity':1}]
        runner.verify_query_indexes([],False)
        runner.verify_query_indexes(index,True)
        for actual,expected in [(index,False),([],True),([{**index[0],'expr':'span_id'}],True)]:
            with self.assertRaisesRegex(RuntimeError,'unexpected query index schema'):
                runner.verify_query_indexes(actual,expected)

    def test_partial_pair_is_archived_and_completed_pair_remains_canonical(self):
        first=self.add_case(0)
        second=self.add_case(1)
        partial=self.add_case(2)
        failing=self.add_case(3,'interrupted')
        (partial/'raw.txt').write_text('preserved baseline')
        (failing/'raw.txt').write_text('preserved candidate')
        completed=runner.resume_position(self.output,self.record,self.provenance)
        self.assertEqual(completed,2)
        def save():pass
        runner.archive_incomplete_pair(self.output,self.record,completed,save)
        self.assertTrue(first.is_dir())
        self.assertTrue(second.is_dir())
        self.assertFalse(partial.exists())
        self.assertFalse(failing.exists())
        archive=self.output/'attempts/resume-001'
        self.assertEqual((archive/partial.name/'raw.txt').read_text(),'preserved baseline')
        self.assertEqual((archive/failing.name/'raw.txt').read_text(),'preserved candidate')
        self.assertEqual(len(self.record['cases']),2)
        self.assertEqual(len(self.record['abandoned_attempts'][0]['cases']),2)
        self.assertEqual(self.record['status'],'running')

    def test_resume_journal_survives_interrupted_archive_move(self):
        self.add_case(0)
        self.add_case(1)
        partial=self.add_case(2,'measuring')
        completed=runner.resume_position(self.output,self.record,self.provenance)
        calls=[]
        def interrupt():
            calls.append(1)
            raise RuntimeError('process stopped after journal save')
        with self.assertRaises(RuntimeError):
            runner.archive_incomplete_pair(self.output,self.record,completed,interrupt)
        archive=self.output/'attempts/resume-001'
        archive.mkdir(parents=True)
        partial.rename(archive/partial.name)
        self.record['status']='running'
        with self.assertRaisesRegex(ValueError,'exclusive lock'):
            runner.resume_position(self.output,self.record,self.provenance)
        self.assertEqual(runner.resume_position(self.output,self.record,self.provenance,locked=True),2)
        runner.archive_incomplete_pair(self.output,self.record,completed,lambda:None)
        self.assertEqual(len(self.record['abandoned_attempts']),1)
        self.assertNotIn('resume_plan',self.record)

    def test_reject_changed_controls_provenance_or_incomplete_evidence(self):
        self.add_case(0)
        self.add_case(1)
        self.add_case(2,'interrupted')
        for key,value in [('candidate_tag','candidate3'),
                          ('binary',{'path':'/bench','sha256':'other'}),
                          ('source_manifest',{'files':{'source':'other'}}),
                          ('images',{'candidate':{},'baseline':{}}),
                          ('declaration_sha256','other'),('probe_sustained',True)]:
            changed=copy.deepcopy(self.provenance)
            changed[key]=value
            with self.subTest(key=key),self.assertRaisesRegex(ValueError,'provenance changed'):
                runner.resume_position(self.output,self.record,changed)
        self.record['cases'][2]['stage']['workload']['rate']=123
        with self.assertRaisesRegex(ValueError,'controls or order'):
            runner.resume_position(self.output,self.record,self.provenance)
        self.record['cases'][2]['stage']['workload']['rate']=8000
        (self.output/self.record['cases'][0]['label']/'analysis-exact.json').unlink()
        with self.assertRaisesRegex(ValueError,'lacks observed analysis'):
            runner.resume_position(self.output,self.record,self.provenance)

    def test_reject_running_campaign_and_cases_after_partial_pair(self):
        self.add_case(0,'measuring')
        self.add_case(1,'completed')
        self.add_case(2,'completed')
        with self.assertRaisesRegex(ValueError,'cases exist after'):
            runner.resume_position(self.output,self.record,self.provenance)
        self.record['status']='running'
        with self.assertRaisesRegex(ValueError,'exclusive lock'):
            runner.resume_position(self.output,self.record,self.provenance)

    def test_cleanup_abandoned_preserves_volumes_and_releases_network(self):
        self.add_case(0)
        self.add_case(1)
        self.add_case(2,'measuring')
        completed=runner.resume_position(self.output,self.record,self.provenance)
        runner.archive_incomplete_pair(self.output,self.record,completed,lambda:None)
        commands=[]
        with mock.patch.object(runner.environment,'configure_stack'), \
             mock.patch.object(runner.environment,'compose_args',return_value=['compose']), \
             mock.patch.object(runner,'volume_inventory',return_value=['kept-volume']):
            runner.cleanup_abandoned(self.record,lambda args,timeout:commands.append(args),lambda:None)
        self.assertEqual([args[-3:] for args in commands],
                         [['stop','-t','15'],['down','--timeout','15']])
        self.assertNotIn('-v',sum(commands,[]))
        self.assertTrue(self.record['abandoned_attempts'][0]['cleanup_complete'])

    def test_live_runner_blocks_explicit_resume_before_any_campaign_read(self):
        lock_path=self.output/'.runner.lock'
        with lock_path.open('a+') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            arguments=['stream_hardening.py','--resume','--output',str(self.output),
                       '--binary','/missing-binary','--lag-binary','/missing-lag-binary']
            with mock.patch.object(sys,'argv',arguments),self.assertRaisesRegex(RuntimeError,'already holds'):
                runner.main()


if __name__=='__main__':unittest.main()
