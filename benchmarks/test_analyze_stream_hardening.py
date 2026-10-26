"""Boundary checks for the declared paired streaming comparison."""
import copy
import unittest

import analyze_stream_hardening as hardening


def sample(admitted=100, ack=200_000_000, rows='100', drain=100,
           censored=2, query_fail=0, query_404=0, drops=0):
    return {
        'counts': {'acked_measured_spans': admitted,
                   'unresolved_acked_at_original_deadline': 0},
        'ack_latency': {'p95_ns': ack, 'p99_ns': ack},
        'operations': {'worker': {'clickhouse_write':
                      {'completed_rows_per_second': rows}}},
        'lag': {'drain': {'status': 'observed_bounded',
                          'upper_bound_ns': drain}},
        'visibility': {'sample_count': 10, 'censored_count': censored,
                       'first_send_to_observation': {'p95_ns': 4_000_000_000}},
        'queries': {route: {'statuses': {'200': 20-query_fail-query_404,
                     '500': query_fail, '404': query_404}}
                    for route in hardening.ROUTES},
        'query_schedule': {route: {'scheduled_measured': 20,
                            'queue_dropped': drops,
                            'skipped_no_acknowledged_trace': 0,
                            'late_start': 0} for route in hardening.ROUTES},
        'supplemental_drain': None,
    }


def statuses(name, baseline, candidate):
    return {row['name'].split('/')[-1]: row['status']
            for row in hardening.pair_gates(name, 1, baseline, candidate)}


class PairGateTests(unittest.TestCase):
    def test_hardware_comparison_ignores_observation_times(self):
        from pathlib import Path
        import json
        path=Path(__file__).resolve().parent/'results/20261001-retrospective/sustained-2000-r1-streaming/environment.json'
        original=json.loads(path.read_text())
        changed=copy.deepcopy(original)
        changed['hardware']['hw.model']['at']='later'
        changed['hardware']['hw.model']['elapsed_seconds']=999
        self.assertEqual(hardening.stable_hardware(original),hardening.stable_hardware(changed))
        changed['hardware']['hw.model']['stdout']='different\n'
        self.assertNotEqual(hardening.stable_hardware(original),hardening.stable_hardware(changed))

    def test_exact_sustained_8000_boundaries(self):
        baseline=sample()
        candidate=sample(admitted=95, ack=250_000_000, rows='125',
                         drain=50, censored=1)
        self.assertEqual(set(statuses('sustained-8000',baseline,candidate).values()), {'pass'})
        candidate['operations']['worker']['clickhouse_write']['completed_rows_per_second']='124999/1000'
        self.assertEqual(statuses('sustained-8000',baseline,candidate)['completed_rows_per_second_1_25x'],'fail')
        candidate['operations']['worker']['clickhouse_write']['completed_rows_per_second']='125'
        candidate['ack_latency']['p99_ns']=250_000_001
        self.assertEqual(statuses('sustained-8000',baseline,candidate)['ack_p99_ns'],'fail')

    def test_zero_baseline_censoring_requires_zero_candidate(self):
        baseline=sample(censored=0)
        candidate=sample(censored=1)
        self.assertEqual(statuses('sustained-8000',baseline,candidate)['visibility_censored_fraction_half'],'fail')
        candidate['visibility']['censored_count']=0
        self.assertEqual(statuses('sustained-8000',baseline,candidate)['visibility_censored_fraction_half'],'pass')

    def test_query_404_separate_and_exact_absolute_increase(self):
        baseline=sample(query_fail=1,query_404=1)
        candidate=sample(query_fail=2,query_404=1,drops=1)
        result=statuses('sustained-2000',baseline,candidate)
        self.assertEqual(result['non404_query_failure_fraction_increase_at_most_1_20'],'pass')
        self.assertEqual(result['query_schedule_drop_fraction_increase_at_most_1_20'],'pass')
        candidate['queries']['services']['statuses']['500']=4
        self.assertEqual(statuses('sustained-2000',baseline,candidate)['non404_query_failure_fraction_increase_at_most_1_20'],'fail')

    def test_missing_metrics_do_not_pass_and_recovery_is_finite(self):
        baseline=sample()
        candidate=sample(admitted=95)
        candidate['counts']['unresolved_acked_at_original_deadline']=1
        self.assertEqual(statuses('burst-32000',baseline,candidate)['admitted_recovery_within_bound'],'fail')
        candidate['supplemental_drain']={'complete':True,'observations':[{'snapshot_verified':True,'acked_missing':0}]}
        self.assertEqual(statuses('burst-32000',baseline,candidate)['admitted_recovery_within_bound'],'pass')
        candidate['queries']['services']['statuses']={}
        for route in hardening.ROUTES[1:]: candidate['queries'][route]['statuses']={}
        self.assertEqual(statuses('burst-32000',baseline,candidate)['non404_query_failure_fraction_increase_at_most_1_20'],'unavailable')

    def test_invalid_raw_fractions_are_unavailable(self):
        baseline=sample()
        candidate=sample()
        candidate['visibility']['censored_count']=11
        self.assertEqual(statuses('sustained-8000',baseline,candidate)['visibility_censored_fraction_half'],'unavailable')
        candidate=sample()
        candidate['queries']['services']['statuses']['500']=True
        self.assertEqual(statuses('sustained-8000',baseline,candidate)['non404_query_failure_fraction_increase_at_most_1_20'],'unavailable')
        candidate=sample()
        candidate['query_schedule']['services']['queue_dropped']=81
        self.assertEqual(statuses('sustained-8000',baseline,candidate)['query_schedule_drop_fraction_increase_at_most_1_20'],'unavailable')

    def test_schedule_reversal_and_control_drift(self):
        cases=[]
        for label,name,variant,rep,rate,warmup,duration in hardening.expected_cases():
            cases.append({'label':label,'variant':variant,'path':'streaming','repetition':rep,
                          'stage':{'name':name,'timeout_seconds':480,
                                   'workload':{'rate':rate,'warmup':f'{warmup}s','duration':f'{duration}s',
                                               'concurrency':16,'batch-traces':32,'queue':64,'attempts':3,
                                               'export-timeout':'8s','drain':'120s','settle':'35s',
                                               'attribute-bytes':256,'query-rate':4,'query-concurrency':4,
                                               'query-mix':'services,traces,detail,incidents','baseline-traces':128,
                                               'visibility-every':10,'visibility-timeout':'10s',
                                               'max-event-bytes':268435456,
                                               'setup-lag-file':f'/campaign/{label}/lag.jsonl'}}})
        issues=[]
        hardening.same_controls(cases,issues)
        self.assertEqual(issues,[])
        changed=copy.deepcopy(cases)
        changed[9]['stage']['workload']['query-rate']=0
        hardening.same_controls(changed,issues)
        self.assertTrue(any('workload differs' in issue for issue in issues))
        changed=copy.deepcopy(cases)
        del changed[0]['stage']['workload']['setup-lag-file']
        issues=[]
        hardening.same_controls(changed,issues)
        self.assertTrue(any('barrier path missing' in issue for issue in issues))


if __name__=='__main__': unittest.main()
