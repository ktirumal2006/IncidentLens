#!/usr/bin/env python3
"""Controlled frozen-baseline/candidate streaming comparison; preserves volumes."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid
import threading
import urllib.request
import urllib.error

import environment
import run
import analyze_retrospective
from retrospective import stage, slowdown, wait_ready

ROOT = environment.ROOT
DECLARATION = ROOT / 'benchmarks/WORKLOAD_STREAM_HARDENING.md'


def cases():
    return [('sustained-2000',2000,120,10),('sustained-8000',8000,120,10),
            ('burst-32000',32000,10,0),('slowdown-2000',2000,120,10)]


def volume_inventory(project):
    probe=environment.command(['docker','volume','ls','--filter','label=com.docker.compose.project='+project,'--format','{{.Name}}'])
    if probe.get('returncode')!=0:raise RuntimeError('volume inventory failed')
    return sorted(probe['stdout'].split())


def image_inventory():
    frozen=json.loads((ROOT/'benchmarks/baselines/phase5-streaming-20261003/manifest.json').read_text())
    result={}
    for variant in ('baseline','candidate'):
        result[variant]={}
        for service,component in [('stream-ingest','producer'),('stream-worker','worker'),('api','api')]:
            reference=('incidentlens-phase5-baseline-'+component+':20261003' if variant=='baseline' else 'incidentlens-hardening-'+component+':candidate1')
            probe=environment.command(['docker','image','inspect',reference,'--format','{{.Id}}'])
            if probe.get('returncode')!=0:raise RuntimeError('missing built image '+reference)
            identity=probe['stdout'].strip()
            if variant=='baseline' and identity!=frozen['images'][component]['id']:raise RuntimeError('frozen baseline image changed')
            result[variant][service]={'reference':reference,'id':identity}
    return result


def verify_images(provenance,expected):
    for service,image in expected.items():
        actual=[c for c in provenance['containers'] if c['name'].endswith('-'+service+'-1')]
        if len(actual)!=1 or actual[0]['image_id']!=image['id']:raise RuntimeError('unexpected image for '+service)


class StatusObserver:
    """Identical extra status request load in both variants; baseline may be 404."""
    def __init__(self,path):
        self.file=path.open('x');self.stop=threading.Event();self.thread=threading.Thread(target=self.observe)
    def start(self):self.thread.start()
    def close(self):
        self.stop.set();self.thread.join(timeout=4)
        if self.thread.is_alive():raise RuntimeError('status observer did not stop')
        self.file.close()
    def observe(self):
        count=0
        while not self.stop.is_set():
            row={'kind':'ingestion_status','started_ns':time.time_ns()}
            try:
                try:response=urllib.request.urlopen('http://127.0.0.1:18081/api/v1/ingestion-status',timeout=1.5)
                except urllib.error.HTTPError as error:response=error
                with response:
                    row['http_status']=response.status
                    data=response.read(65537)
                    if len(data)>65536:row['error']='oversized_response'
                    else:
                        try:row['body']=json.loads(data)
                        except ValueError:row['error']='non_json_response'
            except (OSError,TimeoutError):row['error']='request_failed'
            row['finished_ns']=time.time_ns()
            self.file.write(json.dumps(row)+'\n');self.file.flush();count+=1
            self.stop.wait(2)
        self.file.write(json.dumps({'kind':'observer_end','samples':count,'finished_ns':time.time_ns()})+'\n');self.file.flush()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--lag-binary', type=Path, required=True)
    parser.add_argument('--smoke', action='store_true', help='unmeasured paired 120-load-span verification with setup traces')
    args = parser.parse_args()
    output=args.output.resolve(); output.mkdir(parents=True,exist_ok=False)
    binary=args.binary.resolve(); lag_binary=args.lag_binary.resolve()
    if not all(p.is_file() and os.access(p,os.X_OK) for p in [binary,lag_binary]):
        raise ValueError('build both benchmark executables before running')
    record={'started_at':environment.utc(), 'status':'running', 'smoke':args.smoke,
            'declaration_sha256':environment.digest(DECLARATION),
            'lag_binary':{'path':str(lag_binary),'sha256':environment.digest(lag_binary)},
            'cases':[], 'commands':[]}
    def save(): run.save(output/'campaign.json',record)
    def command(arguments, timeout=180):
        result=environment.command(arguments,timeout=timeout)
        record['commands'].append(result);save()
        if result.get('returncode') != 0: raise RuntimeError('command failed: '+str(arguments))
        return result
    record['source_manifest']=environment.manifest()
    record['images']=image_inventory()
    record['baseline_manifest_sha256']=environment.digest(ROOT/'benchmarks/baselines/phase5-streaming-20261003/manifest.json')
    record['binary']={'path':str(binary),'sha256':environment.digest(binary)}
    save()
    original=['docker','compose','-f',str(environment.COMPOSE)]
    prefix='incidentlens-retro-'+uuid.uuid4().hex[:8]
    active_compose=None; lag=None; lag_log=None; status_observer=None
    try:
        command(original+['stop','collector','ingest','api','clickhouse'])
        specifications=[('smoke',24,5,0)] if args.smoke else cases()
        for repetition in range(1,2 if args.smoke else 3):
            for name,rate,duration,warmup in specifications:
                for variant in (['baseline','candidate'] if repetition==1 else ['candidate','baseline']):
                    path='streaming'
                    label=name+'-r'+str(repetition)+'-'+variant
                    directory=output/label;directory.mkdir()
                    ops=directory/'operations';ops.mkdir(mode=0o777);ops.chmod(0o777)
                    project=prefix+'-'+str(len(record['cases']))
                    os.environ['RETRO_PROJECT']=project
                    os.environ['BENCH_EVIDENCE_DIR']=str(ops)
                    overlays=[ROOT/'deploy/benchmark/streaming.yaml']
                    if variant=='candidate':overlays.append(ROOT/'deploy/streaming/hardening.yaml')
                    overlays.append(ROOT/'deploy/benchmark'/('hardening-'+variant+'.yaml'))
                    version='v2' if variant=='candidate' else 'v1'
                    topic='incidentlens-spans-'+version;group='incidentlens-stream-worker-'+version
                    environment.configure_stack(path,project=project,overlays=overlays,stream_topic=topic,stream_group=group)
                    active_compose=environment.compose_args()
                    entry={'label':label,'project':project,'path':path,'repetition':repetition,
                           'variant':variant,'stage':stage(name,rate,duration,warmup),'status':'starting'}
                    if args.smoke:
                        entry['stage']['workload'].update({'baseline-traces':128,'query-rate':0,'batch-traces':4,'settle':'2s'})
                    entry['stage']['workload']['setup-lag-file']=str(directory/'lag.jsonl')
                    record['cases'].append(entry);save()
                    command(active_compose+['up','-d','--no-build'],240)
                    wait_ready()
                    provenance=environment.environment(binary)
                    run.save(directory/'environment.json',provenance)
                    if len(provenance['containers']) != len(environment.SERVICES): raise RuntimeError('incomplete provenance')
                    verify_images(provenance, record['images'][variant])
                    if provenance['source_manifest'] != record['source_manifest']:raise RuntimeError('source changed during campaign')
                    initial=environment.storage_snapshot();run.save(directory/'initial-storage.json',initial)
                    host_before=time.time_ns()
                    vm_clock=environment.command(active_compose+['exec','-T','clickhouse','date','+%s%N'])
                    host_after=time.time_ns()
                    clocks={'host_before_ns':host_before,'host_after_ns':host_after,'vm_observation':vm_clock}
                    try:
                        vm_ns=int(vm_clock['stdout'].strip())
                        clocks['vm_minus_host_offset_bounds_ns']=[vm_ns-host_after,vm_ns-host_before]
                    except (KeyError,ValueError):clocks['offset_bounds_unknown']=True
                    run.save(directory/'clock-check.json',clocks)
                    parts=json.loads(initial['clickhouse_parts']['stdout'])['data']
                    if sum(int(p['physical_rows']) for p in parts)!=0: raise RuntimeError('experiment volume is not empty')
                    if path=='streaming':
                        lag_log=(directory/'lag-stderr.txt').open('x')
                        lag=subprocess.Popen([str(lag_binary),'--output',str(directory/'lag.jsonl'),'--topic',topic,'--group',group,'--partitions','4' if variant=='candidate' else '1'],stderr=lag_log)
                        entry['lag_pid']=lag.pid;save()
                    status_observer=StatusObserver(directory/'ingestion-status.jsonl');status_observer.start()
                    entry['status']='measuring';save()
                    result=run.run_stage(binary,entry['stage'],uuid.uuid4().hex,directory/'measurement',2,
                                         extra_fault=slowdown if name.startswith('slowdown') else None)
                    entry['result']=result;save()
                    # Preserve raw server-side INSERT diagnostics after load only.
                    flushed=environment.command(active_compose+['exec','-T','clickhouse','clickhouse-client','--password','local-admin','--query','SYSTEM FLUSH LOGS'])
                    sql="SELECT type,event_time_microseconds,query_start_time_microseconds,query_duration_ms,written_rows,written_bytes,memory_usage,exception_code,ProfileEvents FROM system.query_log WHERE initial_user='ingest' AND query_kind='Insert' ORDER BY event_time_microseconds FORMAT JSONEachRow"
                    diagnostic=environment.command(active_compose+['exec','-T','clickhouse','clickhouse-client','--password','local-admin','--query',sql])
                    run.save(directory/'server-inserts.json',{'flush':flushed,'observation':diagnostic})
                    # Two additional observations after producer work/settling/drain.
                    if lag is not None:
                        time.sleep(3)
                        lag.send_signal(signal.SIGTERM);code=lag.wait(timeout=5);lag_log.close();lag=None;lag_log=None
                        if code!=0: raise RuntimeError('lag observer failed')
                    status_observer.close();status_observer=None
                    command(active_compose+['stop','-t','15'],120)
                    before=volume_inventory(project)
                    command(active_compose+['down','--timeout','15'],120)
                    after=volume_inventory(project)
                    entry['retained_volumes']={'before':before,'after':after}
                    if not before or before!=after:raise RuntimeError('experiment volume preservation failed')
                    active_compose=None
                    expected=['ingest.jsonl'] if path=='direct' else ['producer.jsonl','worker.jsonl']
                    for filename in expected:
                        lines=(ops/filename).read_text().splitlines()
                        last=json.loads(lines[-1]) if lines else {}
                        if last.get('kind')!='recorder_end' or last.get('dropped')!=0 or last.get('write_failed') or last.get('events')!=len(lines)-1:
                            raise RuntimeError('incomplete operation evidence: '+filename)
                    if result['status']!='completed': raise RuntimeError('measurement failed: '+result['status'])
                    reconciliation=result['summary']['reconciliation']
                    if reconciliation.get('acked_missing',0)>0 and not result.get('supplemental_drain',{}).get('complete'):
                        raise RuntimeError('unresolved acknowledged backlog; no next load')
                    entry['status']='completed';save()
                    analyzed=analyze_retrospective.analyze_case(directory,entry)
                    run.save(directory/'analysis-exact.json',analyzed)
                    if analyzed['status']!='observed':
                        entry['status']='evidence_incomplete';save()
                        raise RuntimeError('case evidence incomplete: '+repr(analyzed['issues']))
        record['status']='completed'
    except BaseException as exc:
        record.update(status='failed',error=repr(exc))
    finally:
        if status_observer is not None:status_observer.close()
        if lag is not None:
            if lag.poll() is None: lag.send_signal(signal.SIGTERM)
            try:lag.wait(timeout=5)
            except subprocess.TimeoutExpired:lag.kill();lag.wait()
            if lag_log:lag_log.close()
        if active_compose is not None:
            # Stop only this explicitly created experiment; never remove volumes.
            cleanup=environment.command(active_compose+['stop','-t','15'],timeout=120)
            record['cleanup']=cleanup
        environment.configure_stack('direct')
        restored=environment.command(original+['up','-d','ingest','api','collector'],timeout=180)
        record['restore_direct']=restored
        if restored.get('returncode') != 0:record.update(status='failed',restore_error='direct compose restoration failed')
        try:wait_ready()
        except Exception as exc:record.update(status='failed',restore_error=repr(exc))
        record['ended_at']=environment.utc();save()
    return 0 if record['status']=='completed' else 1


if __name__=='__main__':raise SystemExit(main())
