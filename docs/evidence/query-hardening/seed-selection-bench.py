"""Compare exact evidence selection plans on the retained synthetic query1 corpus."""
import argparse, base64, json, pathlib, time, urllib.request, urllib.error, uuid
parser=argparse.ArgumentParser()
parser.add_argument("--output",type=pathlib.Path,required=True)
args=parser.parse_args()
args.output.mkdir(parents=True,exist_ok=False)
ROOT=pathlib.Path(__file__).resolve().parent

def request(sql,user='query',query_id=None):
    from urllib.parse import urlencode
    params={'database':'incidentlens'}
    if query_id: params['query_id']=query_id
    req=urllib.request.Request('http://127.0.0.1:18123/?'+urlencode(params),data=sql.encode())
    req.add_header('Authorization','Basic '+base64.b64encode((user+':local-'+('admin' if user=='default' else 'query')).encode()).decode())
    with urllib.request.urlopen(req,timeout=12) as response:return response.read().decode()

old=json.loads((ROOT/'seed-selection-queries.json').read_text())['window']
body=old.split(' FROM spans FINAL ',1)[1].split(') WHERE trace_rank=1',1)[0]
keys='service_namespace,service_name,span_name'
key='tuple(status_code!=2,bitNot(duration_ns),start_time,trace_id,span_id)'
seed='tuple(status_code!=2,bitNot(duration_ns),start_time,trace_id,span_id,status_code)'
topk=f'SELECT {keys},chosen.4 AS trace_id,chosen.5 AS span_id,chosen.3 AS start_time,bitNot(chosen.2) AS duration_ns,chosen.6 AS status_code FROM (SELECT {keys},arrayJoin(groupArraySorted(5)(seed)) AS chosen FROM (SELECT {keys},trace_id,argMin({seed},{key}) AS seed FROM spans FINAL {body} GROUP BY {keys},trace_id) GROUP BY {keys}) ORDER BY {keys},chosen'
order=f'{keys},status_code=2 DESC,duration_ns DESC,start_time ASC,trace_id ASC,span_id ASC'
cols=f'{keys},trace_id,span_id,start_time,duration_ns,status_code'
limitby=f'SELECT {cols} FROM (SELECT {cols} FROM spans FINAL {body} ORDER BY {order} LIMIT 1 BY {keys},trace_id) ORDER BY {order} LIMIT 5 BY {keys}'
queries={'window':old,'aggregate_top5':topk,'ordered_distinct':limitby}
(args.output/'queries.json').write_text(json.dumps(queries,indent=2)+'\n')
results=[]
for rep in range(3):
    order=['window','aggregate_top5','ordered_distinct'] if rep%2==0 else ['ordered_distinct','aggregate_top5','window']
    for variant in order:
        qid='seed-plan-'+uuid.uuid4().hex;started=time.monotonic();row={'variant':variant,'repetition':rep+1,'query_id':qid}
        try:
            raw=request(queries[variant]+' SETTINGS max_execution_time=5 FORMAT JSONEachRow',query_id=qid)
            rows=[json.loads(line) for line in raw.splitlines()]
            row.update(rows=len(rows),output=rows,stream_exception=any("exception" in r for r in rows))
        except Exception as error:row.update(error=repr(error),server_error=error.read().decode()[:1000] if isinstance(error,urllib.error.HTTPError) else '')
        row['elapsed_seconds']=time.monotonic()-started;results.append(row)
        (args.output/'results.json').write_text(json.dumps(results,indent=2)+'\n')
request('SYSTEM FLUSH LOGS','default')
for row in results:
    raw=request("SELECT type,query_duration_ms,read_rows,read_bytes,memory_usage,exception_code FROM system.query_log WHERE query_id='"+row['query_id']+"' AND type!='QueryStart' FORMAT JSONEachRow",'default')
    row['server']=[json.loads(line) for line in raw.splitlines()]
valid=[r for r in results if r.get('server') and all(s['type']=='QueryFinish' and s['exception_code']==0 for s in r['server']) and not r.get('stream_exception') and 'error' not in r]
oracle=next((r for r in valid if r['variant']=='window'),None)
canonical=lambda r:sorted(json.dumps(x,sort_keys=True) for x in r['output'])
for row in results:
    row['complete']=row in valid
    row['exact_rows_match']=canonical(row)==canonical(oracle) if row in valid and oracle else None
(args.output/'results.json').write_text(json.dumps(results,indent=2)+'\n')
print(json.dumps([{k:v for k,v in row.items() if k!='output'} for row in results],indent=2))
