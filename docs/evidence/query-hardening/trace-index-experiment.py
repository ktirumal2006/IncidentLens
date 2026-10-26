"""Isolated retained-corpus trace-index experiment; never run during a campaign."""
import argparse, base64, json, pathlib, time, urllib.request, urllib.parse, uuid

p = argparse.ArgumentParser()
p.add_argument('--output', type=pathlib.Path, required=True)
p.add_argument('--reuse-index', action='store_true')
p.add_argument('--disable-condition-cache', action='store_true')
p.add_argument('--worst-read', action='store_true')
a = p.parse_args()
a.output.mkdir(exist_ok=False)

def request(sql, user='query', qid=None, raw=False, timeout=15):
    params = {'database': 'incidentlens'}
    if qid:
        params['query_id'] = qid
    req = urllib.request.Request('http://127.0.0.1:18123/?'+urllib.parse.urlencode(params), data=sql.encode())
    req.add_header('Authorization', 'Basic '+base64.b64encode((user+':local-'+('admin' if user == 'default' else 'query')).encode()).decode())
    with urllib.request.urlopen(req, timeout=timeout) as response:
        data = response.read().decode()
    if raw:
        return data
    rows = [json.loads(line) for line in data.splitlines()]
    if any('exception' in row for row in rows):
        raise RuntimeError('in-band exception: '+str(rows[-1]))
    return rows

metadata = request("SELECT name,value FROM system.settings WHERE name IN ('max_threads','max_memory_usage','max_rows_to_read','max_bytes_to_read','use_skip_indexes_if_final','use_skip_indexes_if_final_exact_mode','use_query_condition_cache') FORMAT JSONEachRow")
plans = {}
for name, pattern in [('detail', 'SELECT span_id,parent_span_id%FROM spans FINAL PREWHERE trace_id=%'), ('context', 'SELECT trace_id,span_id,parent_span_id%FROM spans FINAL PREWHERE ingested_at%')]:
    order = 'read_rows DESC,event_time_microseconds DESC' if a.worst_read else 'event_time_microseconds DESC'
    found = request("SELECT query FROM system.query_log WHERE type='QueryFinish' AND user='query' AND query LIKE '"+pattern+"' AND query NOT LIKE '%FORMAT%' ORDER BY "+order+" LIMIT 1 FORMAT JSONEachRow", 'default')
    if not found:
        raise RuntimeError('No original query for '+name)
    plans[name] = found[0]['query']
(a.output/'metadata.json').write_text(json.dumps(metadata, indent=2)+'\n')
(a.output/'plans.json').write_text(json.dumps(plans, indent=2)+'\n')
before = request('SHOW CREATE TABLE spans FORMAT JSONEachRow', 'default')
(a.output/'schema-before.json').write_text(json.dumps(before, indent=2)+'\n')
if 'trace_id_bloom_trial' in str(before) and not a.reuse_index:
    raise RuntimeError('Trial index already exists')
ddl = ['ALTER TABLE spans ADD INDEX trace_id_bloom_trial trace_id TYPE bloom_filter(0.001) GRANULARITY 1', 'ALTER TABLE spans MATERIALIZE INDEX trace_id_bloom_trial SETTINGS mutations_sync=1']
(a.output/'ddl.json').write_text(json.dumps(ddl, indent=2)+'\n')
if not a.reuse_index:
    for sql in ddl:
        request(sql, 'default', raw=True, timeout=60)
results = []
for rep in range(3):
    for enabled in ([0, 1] if rep % 2 == 0 else [1, 0]):
        for name, sql in plans.items():
            qid = 'trace-index-'+uuid.uuid4().hex
            row = dict(name=name, index_enabled=enabled, repetition=rep+1, query_id=qid)
            start = time.monotonic()
            try:
                settings = 'max_execution_time=5,use_skip_indexes='+str(enabled)
                if a.disable_condition_cache:
                    settings += ',use_query_condition_cache=0'
                row['settings'] = settings
                row['output'] = request(sql+' SETTINGS '+settings+' FORMAT JSONEachRow', qid=qid)
                row['complete'] = True
            except Exception as error:
                row.update(complete=False, error=repr(error))
            row['elapsed_seconds'] = time.monotonic()-start
            results.append(row)
            (a.output/'results.json').write_text(json.dumps(results, indent=2)+'\n')
request('SYSTEM FLUSH LOGS', 'default', raw=True)
for row in results:
    row['server'] = request("SELECT type,query_duration_ms,read_rows,read_bytes,memory_usage,exception_code,ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu_us FROM system.query_log WHERE query_id='"+row['query_id']+"' AND type!='QueryStart' FORMAT JSONEachRow", 'default')
    row['complete'] = row['complete'] and bool(row['server']) and all(s['type']=='QueryFinish' and s['exception_code']==0 for s in row['server'])
    oracle = next((r for r in results if r['name']==row['name'] and r['index_enabled']==0 and r['complete']), None)
    row['exact_output_match'] = row['output']==oracle['output'] if row['complete'] and oracle else None
(a.output/'results.json').write_text(json.dumps(results, indent=2)+'\n')
explains = {name: request('EXPLAIN indexes=1 '+sql, raw=True) for name,sql in plans.items()}
(a.output/'explain.json').write_text(json.dumps(explains, indent=2)+'\n')
sizes = request("SELECT name,type,data_compressed_bytes,data_uncompressed_bytes,marks_bytes FROM system.data_skipping_indices WHERE database='incidentlens' AND table='spans' FORMAT JSONEachRow", 'default')
(a.output/'index-size.json').write_text(json.dumps(sizes, indent=2)+'\n')
print(json.dumps([{k:v for k,v in r.items() if k!='output'} for r in results], indent=2))
