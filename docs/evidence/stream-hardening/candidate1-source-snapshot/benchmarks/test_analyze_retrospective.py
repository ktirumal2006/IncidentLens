import json
import hashlib
import tempfile
import unittest
from pathlib import Path

import analyze_retrospective as retro


class ExactAnalysisTests(unittest.TestCase):
    def test_rational_rates_and_integer_percentiles(self):
        self.assertEqual(retro.ratio(2, 3), '2/3')
        self.assertEqual(retro.ratio(6, 3), '2')
        self.assertIsNone(retro.ratio(1, 0))
        self.assertEqual(retro.quantiles(list(range(1, 101)))['p99_ns'], 100)
        self.assertEqual(retro.quantiles([])['p95_ns'], None)

    def test_operation_trailer_and_dropped_records_fail_closed(self):
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/'producer.jsonl'
            event={'kind':'kafka_produce','started_ns':1,'finished_ns':4,'duration_ns':3,'rows':6,'success':True}
            path.write_text(json.dumps(event)+'\n')
            issues=[]
            self.assertEqual(len(retro.operation_file(path,issues)),1)
            self.assertTrue(any('trailer' in item for item in issues))
            path.write_text(json.dumps(event)+'\n'+json.dumps({'kind':'recorder_end','events':1,'dropped':0,'write_failed':False})+'\n')
            issues=[]
            self.assertFalse(issues)
            self.assertEqual(len(retro.operation_file(path,issues)),1)
            self.assertFalse(issues)
            path.write_text(json.dumps(event)+'\n'+json.dumps({'kind':'recorder_end','events':1,'dropped':1,'write_failed':False})+'\n')
            issues=[];retro.operation_file(path,issues)
            self.assertTrue(issues)

    def test_operation_throughput_uses_successful_rows_and_exact_window(self):
        rows=[{'kind':'clickhouse_write','started_ns':10,'finished_ns':20,'duration_ns':10,'rows':6,'success':True,'prepare_ns':2,'append_ns':3,'send_ns':4},
              {'kind':'clickhouse_write','started_ns':11,'finished_ns':21,'duration_ns':10,'rows':6,'success':False},
              {'kind':'clickhouse_write','started_ns':30,'finished_ns':40,'duration_ns':10,'rows':9,'success':True}]
        result=retro.operation_metrics(rows,'clickhouse_write',10,30)
        self.assertEqual(result['started_window_successful_rows'],6)
        self.assertEqual(result['started_window_failed_operations'],1)
        self.assertEqual(result['completed_window_successful_rows'],6)
        self.assertEqual(result['completed_rows_per_second'],'300000000')
        self.assertEqual(result['latency']['prepare']['p95_ns'],2)

    def test_completed_rows_can_cross_measurement_boundary(self):
        rows=[{'kind':'clickhouse_write','started_ns':9,'finished_ns':12,'duration_ns':3,'rows':6,'success':True},
              {'kind':'clickhouse_write','started_ns':19,'finished_ns':22,'duration_ns':3,'rows':9,'success':True}]
        result=retro.operation_metrics(rows,'clickhouse_write',10,20)
        self.assertEqual(result['started_window_successful_rows'],9)
        self.assertEqual(result['completed_window_successful_rows'],6)
        self.assertEqual(result['completed_rows_per_second'],'600000000')

    def test_stable_two_zero_lag_bounds_and_missing_trailer(self):
        rows=[{'kind':'lag','started_ns':99,'finished_ns':100,'committed':1,'end':4,'lag_records':3},
              {'kind':'lag','started_ns':110,'finished_ns':112,'committed':4,'end':4,'lag_records':0},
              {'kind':'lag','started_ns':120,'finished_ns':123,'committed':4,'end':4,'lag_records':0},
              {'kind':'observer_end','samples':3,'finished_ns':124}]
        issues=[];result=retro.lag_analysis(rows,100,issues)
        self.assertFalse(issues)
        self.assertEqual(result['drain']['lower_bound_ns'],0)
        self.assertEqual(result['drain']['upper_bound_ns'],23)
        self.assertEqual(result['max_observed_lag_records'],3)
        rows.insert(1,{'kind':'lag','started_ns':104,'finished_ns':107,'committed':2,'end':4,'lag_records':2})
        rows[-1]['samples']=4
        result=retro.lag_analysis(rows,100,[])
        self.assertEqual(result['drain']['lower_bound_ns'],4)
        rows[-2]['end']=5;rows[-2]['lag_records']=1
        result=retro.lag_analysis(rows,100,[])
        self.assertEqual(result['drain']['status'],'not_observed')
        issues=[];retro.lag_analysis(rows[:-1],100,issues)
        self.assertTrue(any('trailer' in item for item in issues))

    def test_lag_zeros_before_final_end_do_not_claim_drain(self):
        rows=[{'kind':'lag','started_ns':101,'finished_ns':102,'committed':2,'end':2,'lag_records':0},
              {'kind':'lag','started_ns':103,'finished_ns':104,'committed':2,'end':2,'lag_records':0},
              {'kind':'lag','started_ns':105,'finished_ns':106,'committed':2,'end':3,'lag_records':1},
              {'kind':'observer_end','samples':3,'finished_ns':107}]
        result=retro.lag_analysis(rows,100,[])
        self.assertEqual(result['drain']['status'],'not_observed')

    def test_zero_time_unacknowledged_completion_is_not_bucketed(self):
        events=[{'kind':'batch_complete','status':'terminal_error','traces':2,'ack':'0001-01-01T00:00:00Z'},
                {'kind':'batch_complete','status':'acknowledged','traces':1,'ack':'2026-01-01T00:00:00Z'}]
        buckets=retro.event_buckets(events,[])
        self.assertEqual(sum(row['acked_spans'] for row in buckets.values()),6)

    def test_query_schedule_separates_drops_skips_and_late_starts(self):
        clock=lambda n:f'2026-01-01T00:00:0{n}Z'
        events=[{'kind':'query','route':'services','scheduled':clock(1),'started':clock(1)},
                {'kind':'query','route':'services','scheduled':clock(2),'started':clock(4)},
                {'kind':'query_drop','route':'traces','scheduled':clock(1)},
                {'kind':'query_skipped','route':'detail','scheduled':clock(2)}]
        start=retro.analyze.time_ns(clock(0));end=retro.analyze.time_ns(clock(3))
        result=retro.query_schedule(events,start,end,[])
        self.assertEqual(result['services']['scheduled_measured'],2)
        self.assertEqual(result['services']['started_in_window'],1)
        self.assertEqual(result['services']['late_start'],1)
        self.assertEqual(result['traces']['queue_dropped'],1)
        self.assertEqual(result['detail']['skipped_no_acknowledged_trace'],1)

    def test_visibility_probe_traffic_is_distinct_from_paced_queries(self):
        events=[{'kind':'visibility_probe','phase':'measured','http_status':404,'response_bytes':3,'latency_ns':7},
                {'kind':'visibility_probe','phase':'measured','http_status':200,'response_bytes':5,'latency_ns':9},
                {'kind':'query','phase':'measured','http_status':200},
                {'kind':'visibility_probe','phase':'warmup','http_status':503}]
        raw={'visibility':{'sample_count':1,'censored_count':1,'first_send_to_observation':{'count':0}}}
        result=retro.visibility_traffic(events,raw)
        self.assertEqual(result['probe_http_requests_measured_phase'],2)
        self.assertEqual(result['probe_http_failures'],1)
        self.assertEqual(result['probe_response_bytes'],8)

    def test_server_finish_stats_exclude_start_records_and_keep_source_ms(self):
        rows=[{'type':'QueryStart','query_duration_ms':0,'written_rows':0},
              {'type':'QueryFinish','query_duration_ms':11,'written_rows':192,'written_bytes':100,
               'exception_code':0,'ProfileEvents':{'InsertedRows':192,'DiskWriteElapsedMicroseconds':17}},
              {'type':'QueryFinish','query_duration_ms':13,'written_rows':6,'written_bytes':4,
               'exception_code':0,'ProfileEvents':{'InsertedRows':6}}]
        result=retro.server_insert_summary(rows)
        self.assertEqual(result['query_finish_count'],2)
        self.assertEqual(result['query_duration_ms']['p95_ms'],13)
        self.assertEqual(result['written_rows'],198)
        self.assertEqual(result['profile_event_totals']['InsertedRows'],198)

    def test_continuation_keeps_failed_parent_attempt_and_combines_28_unique_labels(self):
        with tempfile.TemporaryDirectory() as root:
            parent=Path(root)/'parent';later=Path(root)/'continuation'
            parent.mkdir();later.mkdir()
            expected=retro.expected_cases()
            def entry(spec,status):
                label,path,repetition,*_=spec
                return {'label':label,'path':path,'repetition':repetition,'status':status}
            earlier=[entry(spec,'completed') for spec in expected[:26]]+[entry(expected[26],'starting')]
            first={'status':'failed','smoke':False,'declaration_sha256':'declaration','lag_binary':{'sha256':'lag'},'cases':earlier,'error':'network pool exhausted'}
            raw=json.dumps(first).encode();(parent/'campaign.json').write_bytes(raw)
            (parent/earlier[0]['label']).mkdir()
            (parent/earlier[0]['label']/'environment.json').write_text(json.dumps({'binary':{'sha256':'bench'}}))
            second={'status':'completed','smoke':False,'declaration_sha256':'declaration','lag_binary':{'sha256':'lag'},
                    'cases':[entry(spec,'completed') for spec in expected[26:]],
                    'continuation_of':{'path':str(parent),'sha256':hashlib.sha256(raw).hexdigest(),
                                       'completed_labels':[x['label'] for x in earlier[:26]]}}
            (later/'campaign.json').write_text(json.dumps(second))
            (later/second['cases'][0]['label']).mkdir()
            (later/second['cases'][0]['label']/'environment.json').write_text(json.dumps({'binary':{'sha256':'bench'}}))
            issues=[];_,cases,attempts=retro.campaign_cases(later,issues)
            self.assertFalse(issues)
            self.assertEqual(len(cases),28)
            self.assertEqual(attempts[0]['status'],'failed')
            self.assertEqual(attempts[0]['unmeasured_startup_failure'],expected[26][0])
            self.assertEqual(attempts[1]['status'],'completed')
            second['continuation_of']['sha256']='bad'
            (later/'campaign.json').write_text(json.dumps(second))
            issues=[];retro.campaign_cases(later,issues)
            self.assertTrue(any('digest mismatch' in item for item in issues))

    def test_lag_load_gap_invalid_but_startup_error_can_be_excluded(self):
        rows=[{'kind':'lag','started_ns':1,'error':'group not started'},
              {'kind':'lag','started_ns':10_000_000_000,'finished_ns':10_000_000_001,'committed':1,'end':2,'lag_records':1},
              {'kind':'lag','started_ns':15_000_000_000,'finished_ns':15_000_000_001,'committed':2,'end':2,'lag_records':0},
              {'kind':'observer_end','samples':3,'finished_ns':16_000_000_000}]
        issues=[];retro.lag_analysis(rows,15_000_000_000,issues,10_000_000_000,15_000_000_001)
        self.assertTrue(any('gap' in item for item in issues))
        self.assertFalse(any('startup' in item for item in issues))


if __name__=='__main__':unittest.main()
