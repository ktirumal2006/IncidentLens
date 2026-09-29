import hashlib
import unittest
import analyze

class AnalysisTests(unittest.TestCase):
    def test_counter_reset_is_unknown_not_negative_io(self):
        self.assertEqual(analyze.counter_delta([100,150,180]),80)
        self.assertIsNone(analyze.counter_delta([100,10,30]))
        self.assertIsNone(analyze.counter_delta([100]))

    def test_nanosecond_timestamp_and_offset(self):
        self.assertEqual(analyze.time_ns('2026-01-01T01:00:00.123456789+01:00'), analyze.time_ns('2026-01-01T00:00:00.123456789Z'))
        self.assertEqual(analyze.time_ns('2026-01-01T00:00:00.000000001Z') - analyze.time_ns('2026-01-01T00:00:00Z'),1)

    def test_percentiles_and_no_samples(self):
        self.assertIsNone(analyze.percentiles([])['p99_ns'])
        p=analyze.percentiles([9,1,3,2])
        self.assertEqual((p['p50_ns'],p['p99_ns']),(3,9))

    def test_partial_offering_uses_planned_denominator(self):
        c={'span_kinds_per_trace':{'SERVER':3,'CLIENT':2,'INTERNAL':1}, 'rate_spans_per_second':6,'warmup_ns':0,'duration_ns':2_000_000_000,'batch_traces':1,'load_traces':2}
        events=[{'kind':'scheduled','batch':0,'first_trace':0,'traces':1,'scheduled':'2026-01-01T00:00:00Z'}]
        issues=[]; start=analyze.time_ns('2026-01-01T00:00:00Z')
        m=analyze.analyze_events(events,c,start,start+2_000_000_000,issues)
        self.assertEqual(m['measured_counts_spans']['planned'],12)
        self.assertEqual(m['measured_counts_spans']['scheduled'],6)
        self.assertEqual(analyze.assess_targets(m)['visibility']['uncensored'],'unknown')

    def test_identity_ledger_missing_duplicate_and_loss(self):
        config={'load_traces':1,'baseline_traces':0,'seed':'test'}
        identity=hashlib.sha256(b'test:0000000000000000').hexdigest()[:32]
        row={'kind':'expected_trace','index':0,'trace_id':identity,'expected_mask':63,'stored_mask':31,'acked':True,'emitted':True}
        events=[{'kind':'export_attempt','first_trace':0,'traces':1},{'kind':'batch_complete','first_trace':0,'traces':1,'ack':'2026-01-01T00:00:00Z'}]
        issues=[]; r=analyze.analyze_reconciliation([row],config,events,issues)
        self.assertEqual(r['counts']['acked_missing'],1)
        self.assertEqual(r['zero_identity_loss'],'fail')
        issues=[];r=analyze.analyze_reconciliation([row,row],config,events,issues)
        self.assertEqual(r['status'],'unknown')
        self.assertTrue(issues)
        self.assertEqual(analyze.analyze_reconciliation([],config,events,[])['status'],'unknown')

if __name__=='__main__': unittest.main()
