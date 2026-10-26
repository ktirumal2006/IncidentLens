"""Measure selective pre-FINAL filtering under the existing immutable-retry contract."""
import argparse,base64,json,pathlib,time,urllib.request,urllib.parse,urllib.error,uuid
p=argparse.ArgumentParser();p.add_argument('--output',type=pathlib.Path,required=True);a=p.parse_args();a.output.mkdir(exist_ok=False)
source=pathlib.Path(__file__).resolve().parent/'query3-read-sql.json';raw=json.loads(source.read_text())
plans={'seeds':{'current':raw['queries'],'kind_prewhere':[q.replace(' WHERE span_kind=2 AND span_name=', ' AND span_kind=2 WHERE span_name=') for q in raw['queries']]},'statistics':{'current':[raw['statistics']],'kind_prewhere':[raw['statistics'].replace(' WHERE span_kind=2 GROUP BY',' AND span_kind=2 GROUP BY')]},'services':{'current':[raw['services']],'kind_prewhere':[raw['services'].replace(' FROM spans FINAL WHERE ',' FROM spans FINAL PREWHERE span_kind=2 WHERE ').replace(' AND span_kind = 2','')]},'search':{'current':[raw['search']],'key_prewhere':[raw['search'].replace(' WHERE ',' AND ',1)]}}
assert all(v['current']!=v[next(k for k in v if k!='current')] for v in plans.values())
(a.output/'plans.json').write_text(json.dumps(plans,indent=2)+'\n')
def request(sql,user='query',qid=None,json_rows=True):
 params={'database':'incidentlens'}
 if qid:params['query_id']=qid
 req=urllib.request.Request('http://127.0.0.1:18123/?'+urllib.parse.urlencode(params),data=sql.encode());req.add_header('Authorization','Basic '+base64.b64encode((user+':local-'+('admin' if user=='default' else 'query')).encode()).decode())
 with urllib.request.urlopen(req,timeout=10) as response:data=response.read().decode()
 if not json_rows:return data
 rows=[json.loads(l) for l in data.splitlines()]
 if any('exception' in r for r in rows):raise RuntimeError('in-band exception: '+str(rows[-1]))
 return rows
results=[]
for rep in range(3):
 for route,variants in plans.items():
  order=list(variants) if rep%2==0 else list(reversed(variants))
  for variant in order:
   row={'route':route,'variant':variant,'repetition':rep+1,'queries':[],'output':[]};start=time.monotonic()
   try:
    for index,sql in enumerate(variants[variant]):
     qid='kind-selective-'+uuid.uuid4().hex;row['queries'].append({'id':qid,'index':index})
     row['output'].append(request(sql+' SETTINGS max_execution_time=5 FORMAT JSONEachRow',qid=qid))
    row['complete']=True
   except Exception as error:row.update(complete=False,error=repr(error),server_error=error.read().decode()[:1000] if isinstance(error,urllib.error.HTTPError) else '')
   row['elapsed_seconds']=time.monotonic()-start;results.append(row);(a.output/'results.json').write_text(json.dumps(results,indent=2)+'\n')
request('SYSTEM FLUSH LOGS','default')
for row in results:
 for q in row['queries']:q['server']=request("SELECT type,query_duration_ms,read_rows,read_bytes,memory_usage,exception_code FROM system.query_log WHERE query_id='"+q['id']+"' AND type!='QueryStart' FORMAT JSONEachRow",'default')
 row['complete']=row['complete'] and all(q['server'] and all(s['type']=='QueryFinish' and s['exception_code']==0 for s in q['server']) for q in row['queries'])
for route in plans:
 oracle=next((r for r in results if r['route']==route and r['variant']=='current' and r['complete']),None)
 for row in results:
  if row['route']==route:row['exact_output_match']=row['output']==oracle['output'] if oracle and row['complete'] else None
(a.output/'results.json').write_text(json.dumps(results,indent=2)+'\n')
explains={}
for route,variants in plans.items():
 explains[route]={}
 for variant,sqls in variants.items():
  try:explains[route][variant]=request('EXPLAIN PIPELINE '+sqls[0],json_rows=False)
  except Exception as error:explains[route][variant]=repr(error)
(a.output/'pipelines.json').write_text(json.dumps(explains,indent=2)+'\n')
print(json.dumps([{k:v for k,v in r.items() if k not in ['output','queries']}|{'memory_peak':max((s['memory_usage'] for q in r['queries'] for s in q['server']),default=0),'read_bytes':sum(s['read_bytes'] for q in r['queries'] for s in q['server'])} for r in results],indent=2))
