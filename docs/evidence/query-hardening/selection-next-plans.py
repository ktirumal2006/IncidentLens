"""Compare exact per-operation selection plans on a retained synthetic corpus."""
import argparse,base64,json,pathlib,time,urllib.request,urllib.parse,urllib.error,uuid,re
p=argparse.ArgumentParser();p.add_argument('--query',type=pathlib.Path,required=True);p.add_argument('--output',type=pathlib.Path,required=True);a=p.parse_args();a.output.mkdir(exist_ok=False)
seeds=json.loads(a.query.read_text())['queries'];assert len(seeds)==3
cols='service_namespace,service_name,span_name,trace_id,span_id,start_time,duration_ns,status_code'
current=[q.replace('SELECT trace_id,span_id,start_time,duration_ns,status_code', 'SELECT '+cols,1) for q in seeds]
union=' UNION ALL '.join('SELECT * FROM ('+q+')' for q in current)
aggregates=[]
for q in seeds:
 body=q.split(' FROM spans FINAL ',1)[1].split(' ORDER BY ',1)[0]
 identity=[re.search(key+"=('(?:[^']|\\')*?')",body).group(1) for key in ['service_namespace','service_name','span_name']]
 priority='tuple(status_code!=2,bitNot(duration_ns),start_time,trace_id,span_id)';value='tuple(status_code!=2,bitNot(duration_ns),start_time,trace_id,span_id,status_code)'
 aggregates.append('SELECT '+identity[0]+' AS selected_namespace,'+identity[1]+' AS selected_service,'+identity[2]+' AS selected_operation,chosen.4 AS trace_id,chosen.5 AS span_id,chosen.3 AS start_time,bitNot(chosen.2) AS duration_ns,chosen.6 AS status_code FROM (SELECT arrayJoin(groupArraySorted(5)(seed)) AS chosen FROM (SELECT trace_id,argMin('+value+','+priority+') AS seed FROM spans FINAL '+body+' GROUP BY trace_id)) ORDER BY chosen')
plans={'sequential_distinct':current,'union_distinct':[union],'sequential_aggregate':aggregates}
(a.output/'plans.json').write_text(json.dumps(plans,indent=2)+'\n')
def request(sql,user='query',qid=None,raw=False):
 params={'database':'incidentlens'}
 if qid:params['query_id']=qid
 req=urllib.request.Request('http://127.0.0.1:18123/?'+urllib.parse.urlencode(params),data=sql.encode());req.add_header('Authorization','Basic '+base64.b64encode((user+':local-'+('admin' if user=='default' else 'query')).encode()).decode())
 with urllib.request.urlopen(req,timeout=10) as response:data=response.read().decode()
 if raw:return data
 rows=[json.loads(line) for line in data.splitlines()]
 if any('exception' in row for row in rows):raise RuntimeError('in-band exception: '+str(rows[-1]))
 for row in rows:
  for old,new in [('selected_namespace','service_namespace'),('selected_service','service_name'),('selected_operation','span_name')]:
   if old in row:row[new]=row.pop(old)
 return rows
results=[]
for rep in range(3):
 for variant in list(plans) if rep%2==0 else list(reversed(plans)):
  row={'variant':variant,'repetition':rep+1,'queries':[],'output':[]};start=time.monotonic()
  try:
   for sql in plans[variant]:
    qid='next-selection-'+uuid.uuid4().hex;row['queries'].append({'id':qid});row['output']+=request(sql+' SETTINGS max_execution_time=5 FORMAT JSONEachRow',qid=qid)
   row['complete']=True
  except Exception as error:row.update(complete=False,error=repr(error),server_error=error.read().decode()[:1000] if isinstance(error,urllib.error.HTTPError) else '')
  row['elapsed_seconds']=time.monotonic()-start;results.append(row);(a.output/'results.json').write_text(json.dumps(results,indent=2)+'\n')
request('SYSTEM FLUSH LOGS','default')
for row in results:
 for q in row['queries']:q['server']=request("SELECT type,query_duration_ms,read_rows,read_bytes,memory_usage,exception_code,ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu_us FROM system.query_log WHERE query_id='"+q['id']+"' AND type!='QueryStart' FORMAT JSONEachRow",'default')
 row['complete']=row['complete'] and all(q['server'] and all(s['type']=='QueryFinish' and s['exception_code']==0 for s in q['server']) for q in row['queries'])
oracle=next((r for r in results if r['variant']=='sequential_distinct' and r['complete']),None)
canonical=lambda row:sorted(json.dumps(x,sort_keys=True) for x in row['output'])
for row in results:row['exact_output_match']=canonical(row)==canonical(oracle) if oracle and row['complete'] else None
(a.output/'results.json').write_text(json.dumps(results,indent=2)+'\n')
explains={}
for variant,sqls in plans.items():
 try:explains[variant]=request('EXPLAIN PIPELINE '+sqls[0],raw=True)
 except Exception as error:explains[variant]=repr(error)
(a.output/'pipelines.json').write_text(json.dumps(explains,indent=2)+'\n')
print(json.dumps([{k:v for k,v in r.items() if k not in ['queries','output']}|{'memory_peak':max((s['memory_usage'] for q in r['queries'] for s in q['server']),default=0),'cpu_us':sum(s['cpu_us'] for q in r['queries'] for s in q['server'])} for r in results],indent=2))
