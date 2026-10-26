"""Summarize a completed paired diagnostic; never change its original evidence."""
import argparse, collections, json, pathlib, sys
ROOT=pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'benchmarks'))
import analyze, analyze_stream_hardening as gates
p=argparse.ArgumentParser();p.add_argument('campaign',type=pathlib.Path);p.add_argument('output',type=pathlib.Path);p.add_argument('--declaration',type=pathlib.Path)
a=p.parse_args();root=a.campaign.resolve()
if a.declaration:gates.DECLARATION=a.declaration.resolve()
campaign=json.loads((root/'campaign.json').read_text())
assert campaign['status']=='completed' and campaign.get('probe_sustained') and len(campaign['cases'])==2
assert not a.output.exists(),'preserve existing summary'
issues=[];provenance=gates.check_provenance(root,campaign,campaign['cases'],issues)
assert issues==['diagnostic sustained probe is not a full measured comparison'],issues
out={'scope':'diagnostic only; full repeated campaign and operational acceptance still required','campaign':str(root),'provenance':provenance,'provenance_issues':issues,'cases':{}}
rows=[]
for variant in ['baseline','candidate']:
 case=root/('sustained-8000-r1-'+variant)
 row=json.loads((case/'analysis-exact.json').read_text());rows.append(row)
 assert row['status']=='observed' and not row['issues']
 paced=collections.Counter();visibility=collections.Counter()
 for line in (case/'measurement/harness/events.jsonl').open():
  event=json.loads(line);kind=event.get('kind');code=event.get('http_status',0);cause=event.get('query_failure','unclassified')
  if kind=='query' and row['window']['from_ns']<=analyze.time_ns(event['started'])<row['window']['to_ns']:
   paced[(event['route'],code,cause)]+=1
  elif kind=='visibility_probe' and event.get('phase')=='measured':visibility[(code,cause)]+=1
 assert sum(paced.values())==gates.non404_fraction(row)[2][1]
 out['cases'][variant]={
  'acked_measured_spans':row['counts']['acked_measured_spans'],
  'acked_within_window_spans':row['counts']['acked_within_window_spans'],
  'measurement_duration_ns':row['window']['duration_ns'],
  'counts':row['counts'],
  'final_recovered_stored_unique_spans':row.get('supplemental_final_stored_unique_spans') or row['counts']['stored_unique_final_spans'],
  'recovered':gates.recovered(row),'ack_latency':row['ack_latency'],
  'write_completed_rows_per_second':row['operations']['worker']['clickhouse_write']['completed_rows_per_second'],
  'commit_completed_rows_per_second':row['operations']['worker']['consumer_commit']['completed_rows_per_second'],
  'peak_kafka_lag_records':row['lag']['max_observed_lag_records'],'drain':row['lag']['drain'],
  'visibility':row['visibility'],'non404_counts':gates.non404_fraction(row)[2],
  'query_drop_counts':gates.query_drop_fraction(row)[1],
  'paced_query_started_window': [{'route':r,'http_status':c,'cause':f,'count':n} for (r,c,f),n in sorted(paced.items())],
  'visibility_probe_measured_phase':[{'http_status':c,'cause':f,'count':n} for (c,f),n in sorted(visibility.items())]}
out['gates']=gates.pair_gates('sustained-8000',1,*rows)
with a.output.open('x') as f:json.dump(out,f,indent=2);f.write('\n')
print(json.dumps({'output':str(a.output),'gates':[(g['name'],g['status']) for g in out['gates']]}))
