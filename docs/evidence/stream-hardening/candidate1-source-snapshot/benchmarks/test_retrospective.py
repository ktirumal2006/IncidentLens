import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

import environment
import retrospective
import run


class RetrospectiveTests(unittest.TestCase):
    def tearDown(self):
        environment.configure_stack('direct')

    def test_isolated_project_and_overlay_do_not_change_default(self):
        environment.configure_stack('streaming',project='incidentlens-retro-test',overlays=['deploy/benchmark/streaming.yaml'])
        args=environment.compose_args()
        self.assertIn('incidentlens-retro-test',args)
        self.assertIn(str(Path('deploy/benchmark/streaming.yaml').resolve()),args)
        environment.configure_stack('direct')
        self.assertEqual(environment.compose_args(),['docker','compose','-f',str(environment.COMPOSE)])
        with self.assertRaises(ValueError):environment.configure_stack('direct',project='incidentlens')

    def test_workloads_retain_fixed_limits_and_fit_caps(self):
        for name,rate,duration,warmup in retrospective.cases():
            s=retrospective.stage(name,rate,duration,warmup)
            run.validate_plan({'version':1,'declaration':'benchmarks/WORKLOAD_RETROSPECTIVE.md','stages':[s]})
            self.assertEqual(s['workload']['batch-traces'],32)
            self.assertEqual(s['workload']['queue'],64)
            self.assertEqual(s['workload']['concurrency'],16)
        self.assertEqual(len(retrospective.cases()),7)

    def test_failed_cpu_throttle_still_restores_original_quota(self):
        class Records:
            def __init__(self):self.rows=[]
            def write(self,row):self.rows.append(row)
        calls=[]
        def command(args,timeout=15):
            calls.append(args)
            if 'ps' in args:return {'returncode':0,'stdout':'owned-id\n'}
            if 'update' in args:return {'returncode':1 if '0.25' in args else 0,'stdout':''}
            return {'returncode':0,'stdout':'2000000000\n'}
        records=Records();result={};active=threading.Event();stop=threading.Event()
        with patch.object(run,'read_load_start',return_value=('2026-01-01T00:00:00Z',0)),patch.object(environment,'command',side_effect=command):
            retrospective.slowdown(stop,records,result,active,Path('/unused'))
        self.assertEqual(result['status'],'failed')
        self.assertIn(['docker','update','--cpus','2','owned-id'],calls)
        self.assertFalse(active.is_set())

    def test_continuation_preserves_completed_prefix_and_rejects_measured_retry(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary)
            label='sustained-500-r1-direct'
            case=directory/label;case.mkdir()
            (case/'environment.json').write_text(json.dumps({'binary':{'sha256':'unchanged'}}))
            (case/'analysis-exact.json').write_text(json.dumps({'status':'observed'}))
            parent={'status':'failed','smoke':False,'restore_direct':{'returncode':0},
                    'declaration_sha256':'unchanged','lag_binary':{'sha256':'unchanged'},
                    'cases':[{'label':label,'stage':retrospective.stage('sustained-500',500,120,10),'status':'completed'},
                             {'label':'sustained-500-r1-streaming','stage':retrospective.stage('sustained-500',500,120,10),'status':'starting'}]}
            source=directory/'campaign.json'
            source.write_text(json.dumps(parent))
            with patch.object(environment,'digest',return_value='unchanged'):
                result=retrospective.continuation(directory,Path('/binary'),Path('/lag'))
                self.assertEqual(result['completed_labels'],[label])
                parent['cases'][-1]['result']={'status':'failed'}
                source.write_text(json.dumps(parent))
                with self.assertRaisesRegex(ValueError,'measured or incomplete'):
                    retrospective.continuation(directory,Path('/binary'),Path('/lag'))
                del parent['cases'][-1]['result']
                parent['cases'][0]['stage']['workload']['rate']=2000
                source.write_text(json.dumps(parent))
                with self.assertRaisesRegex(ValueError,'order or workload'):
                    retrospective.continuation(directory,Path('/binary'),Path('/lag'))


if __name__=='__main__':unittest.main()
