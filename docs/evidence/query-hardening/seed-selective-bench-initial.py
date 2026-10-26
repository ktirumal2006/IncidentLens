"""Retained-corpus exact selection comparison; no writes or ingestion changes."""
import argparse,base64,json,pathlib,time,urllib.request,urllib.parse,urllib.error,uuid,re
p=argparse.ArgumentParser();p.add_argument('--output',type=pathlib.Path,required=True);a=p.parse_args();a.output.mkdir(exist_ok=False)
source=pathlib.Path(__file__).resolve().parent/'query2-seed-sql.json'
old=json.loads(source.read_text())['query']
cols='service_namespace,service_name,span_name,trace_id,span_id,start_time,duration_ns,status_code'
pre=old.split(' FROM spans FINAL PREWHERE ',1)[1].split(' WHERE span_kind=2 AND (',1)[0]
groups=re.findall(r"\(service_namespace='[^']*' AND service_name='[^']*' AND span_name='[^']*'\)",old)
assert len(groups)==3
order='status_code=2 DESC,duration_ns DESC,start_time ASC,trace_id ASC,span_id ASC'
queries={'ordered_grouped':[old],'per_operation_distinct':[f'SELECT {cols} FROM spans FINAL PREWHERE {pre} AND {g} WHERE span_kind=2 ORDER BY {order} LIMIT 1 BY trace_id LIMIT 5' for g in groups], 'per_operation_top5':[f'SELECT {cols} FROM spans FINAL PREWHERE {pre} AND {g} WHERE span_kind=2' for g in groups]}
(a.output/'queries.json').write_text(json.dumps(queries,indent=2)+'\n')
def request(sql,user='query',qid=None):
 params={'database':'incidentlens'}
 if qid:params['query_id']=qid
 req=urllib.request.Request('http://127.0.0.1:18123/?'+urllib.parse.urlencode(params),data=sql.encode());req.add_header('Authorization','Basic '+base64.b64encode((user+':local-'+('admin' if user=='default' else 'query')).encode()).decode())
 with urllib.request.urlopen(req,timeout=7) as response:raw=response.read().decode()
 rows=[json.loads(l) for l in raw.splitlines()]
 if any('exception' in r for r in rows):raise RuntimeError('in-band exception: '+str(rows[-1]))
 return rows
results=[]
for rep in range(3):
 variants=list(queries) if rep%2==0 else list(reversed(queries))
 for variant in variants:
  row={'variant':variant,'repetition':rep+1,'queries':[],'output':[]};start=time.monotonic()
  try:
   for sql in queries[variant]:
    seen=set()
    for iteration in range(5 if variant=='per_operation_top5' else 1):
     actual=sql
     if variant=='per_operation_top5':
      if seen:actual+=' AND trace_id NOT IN ('+','.join("'"+x+"'" for x in sorted(seen))+')'
      actual+=' ORDER BY '+order+' LIMIT 5'
     qid='seed-selective-'+uuid.uuid4().hex;row['queries'].append({'id':qid,'sql':actual})
     data=request(actual+' SETTINGS max_execution_time=5 FORMAT JSONEachRow',qid=qid)
     for item in data:
      if item['trace_id'] not in seen and len(seen)<5:row['output'].append(item);seen.add(item['trace_id'])
     if variant!='per_operation_top5' or len(seen)==5 or len(data)<5:break
    if variant=='ordered_grouped':row['output']=data
   row['complete']=True
  except Exception as e:row.update(complete=False,error=repr(e),server_error=e.read().decode()[:1000] if isinstance(e,urllib.error.HTTPError) else '')
  row['elapsed_seconds']=time.monotonic()-start;results.append(row);(a.output/'results.json').write_text(json.dumps(results,indent=2)+'\n')
request('SYSTEM FLUSH LOGS','default')
for row in results:
 for q in row['queries']:q['server']=request("SELECT type,query_duration_ms,read_rows,read_bytes,memory_usage,exception_code FROM system.query_log WHERE query_id='"+q['id']+"' AND type!='QueryStart' FORMAT JSONEachRow",'default')
 row['complete']=row['complete'] and all(q['server'] and all(r['type']=='QueryFinish' and r['exception_code']==0 for r in q['server']) for q in row['queries'])
oracle=next((r for r in results if r['variant']=='ordered_grouped' and r['complete']),None)
canonical=lambda row:sorted(json.dumps(r,sort_keys=True) for r in row['output'])
for row in results:row['exact_rows_match']=canonical(row)==canonical(oracle) if row['complete'] and oracle else None
(a.output/'results.json').write_text(json.dumps(results,indent=2)+'\n')
print(json.dumps([{k:v for k,v in r.items() if k not in ['queries','output']}|{'rows':len(r['output']),'queries':len(r['queries']),'max_memory':max((s['memory_usage'] for q in r['queries'] for s in q['server']),default=0)} for r in results],indent=2))
