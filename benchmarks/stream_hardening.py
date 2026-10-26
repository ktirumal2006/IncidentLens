#!/usr/bin/env python3
"""Controlled frozen-baseline/candidate streaming comparison; preserves volumes."""
import argparse
import fcntl
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


def case_overlays(variant, candidate_query_index=False):
    overlays=[ROOT/'deploy/benchmark/streaming.yaml']
    if variant=='candidate':
        overlays.append(ROOT/'deploy/streaming/hardening.yaml')
        if candidate_query_index:
            overlays.append(ROOT/'deploy/query/trace-index.yaml')
    overlays.append(ROOT/'deploy/benchmark'/('hardening-'+variant+'.yaml'))
    return overlays


def verify_query_indexes(indexes, expected):
    wanted=[{'name':'trace_id_bloom','expr':'trace_id','type':'bloom_filter','granularity':1}] if expected else []
    if indexes != wanted:
        raise RuntimeError('unexpected query index schema: '+str(indexes))


def schedule(smoke,probe_sustained=False):
    specs=([('smoke',24,5,0)] if smoke else
           [('sustained-8000',8000,120,10)] if probe_sustained else cases())
    return [(name,rate,duration,warmup,repetition,variant,
             name+'-r'+str(repetition)+'-'+variant)
            for repetition in range(1,2 if smoke or probe_sustained else 3)
            for name,rate,duration,warmup in specs
            for variant in (('baseline','candidate') if repetition==1 else ('candidate','baseline'))]


def case_stage(name,rate,duration,warmup,directory,smoke):
    result=stage(name,rate,duration,warmup)
    if smoke:
        result['workload'].update({'baseline-traces':128,'query-rate':0,'batch-traces':4,'settle':'2s'})
    result['workload']['setup-lag-file']=str(directory/'lag.jsonl')
    return result


def resume_position(output,record,provenance,locked=False):
    """Reject drift and return the first whole pair requiring a fresh attempt."""
    if record.get('status') not in ('failed','interrupted') and not (record.get('status')=='running' and locked):
        raise ValueError('resume requires a failed/interrupted campaign or an exclusive lock on a stale running campaign')
    if record.get('candidate_query_index',False) != provenance.get('candidate_query_index',False):
        raise ValueError('resume provenance changed: candidate_query_index')
    for key in ('smoke','probe_sustained','candidate_tag','declaration_sha256',
                'baseline_manifest_sha256','source_manifest','images'):
        if record.get(key) != provenance[key]:
            raise ValueError('resume provenance changed: '+key)
    for key in ('binary','lag_binary'):
        if (record.get(key) or {}).get('sha256') != provenance[key]['sha256']:
            raise ValueError('resume provenance changed: '+key)
    actual=record.get('cases')
    wanted=schedule(record['smoke'],record['probe_sustained'])
    if not isinstance(actual,list) or len(actual)>len(wanted):
        raise ValueError('invalid saved case schedule')
    projects=set()
    for index,entry in enumerate(actual):
        name,rate,duration,warmup,repetition,variant,label=wanted[index]
        expected_stage=case_stage(name,rate,duration,warmup,output/label,record['smoke'])
        if (entry.get('label'),entry.get('variant'),entry.get('path'),entry.get('repetition'),entry.get('stage')) != \
                (label,variant,'streaming',repetition,expected_stage):
            raise ValueError('saved case controls or order changed: '+label)
        if not isinstance(entry.get('project'),str) or not entry['project'].startswith('incidentlens-retro-'):
            raise ValueError('saved experiment project invalid: '+label)
        if entry['project'] in projects:raise ValueError('experiment project reused: '+entry['project'])
        projects.add(entry['project'])
    completed=0
    for index in range(0,len(wanted),2):
        pair=actual[index:index+2]
        if len(pair)==2 and all(entry.get('status')=='completed' for entry in pair):
            for entry in pair:
                directory=output/entry['label']
                exact=directory/'analysis-exact.json'
                if not exact.is_file() or json.loads(exact.read_text()).get('status')!='observed':
                    raise ValueError('completed case lacks observed analysis: '+entry['label'])
                volumes=entry.get('retained_volumes') or {}
                if not volumes.get('before') or volumes.get('before')!=volumes.get('after'):
                    raise ValueError('completed case lacks retained volumes: '+entry['label'])
            completed=index+2
            continue
        if len(actual)>index+2:
            raise ValueError('cases exist after an incomplete matched pair')
        break
    for index in range(completed):
        if not (output/wanted[index][6]).is_dir():
            raise ValueError('completed case directory missing: '+wanted[index][6])
    for index in range(completed+2,len(wanted)):
        if (output/wanted[index][6]).exists():
            raise ValueError('unexpected future case directory: '+wanted[index][6])
    return completed


def archive_incomplete_pair(output,record,completed,save):
    """Journal moves so an interruption during archiving can be resumed safely."""
    plan=record.get('resume_plan')
    if plan is None:
        pair=schedule(record['smoke'],record['probe_sustained'])[completed:completed+2]
        number=len(record.get('abandoned_attempts',[]))+1
        archive='attempts/resume-'+str(number).zfill(3)
        if (output/archive).exists():
            raise ValueError('attempt archive already exists: '+archive)
        plan={'completed':completed,'archive':archive,
              'labels':[item[6] for item in pair],
              'cases':record['cases'][completed:],
              'prior_status':record['status'],'prior_error':record.get('error'),
              'at':environment.utc()}
        record['resume_plan']=plan;save()
    elif plan.get('completed')!=completed or plan.get('labels')!=[item[6] for item in schedule(record['smoke'],record['probe_sustained'])[completed:completed+2]]:
        raise ValueError('saved resume journal does not match incomplete pair')
    archive_dir=output/plan['archive']
    archive_dir.mkdir(parents=True,exist_ok=True)
    for label in plan['labels']:
        source=output/label;destination=archive_dir/label
        if source.exists() and destination.exists():
            raise ValueError('case exists in both canonical and archive paths: '+label)
        if source.exists():source.rename(destination)
    record.setdefault('abandoned_attempts',[]).append(plan)
    record['cases']=record['cases'][:completed]
    record.pop('resume_plan',None)
    record.pop('error',None)
    record.pop('ended_at',None)
    record['status']='running'
    save()


def cleanup_abandoned(record,command,save):
    """Stop abandoned projects and release networks, retaining their volumes."""
    for attempt in record.get('abandoned_attempts',[]):
        if attempt.get('cleanup_complete'):continue
        for entry in attempt['cases']:
            project=entry['project']
            if any(item.get('project')==project for item in attempt.get('cleanup',[])):
                continue
            overlays=case_overlays(entry['variant'],record.get('candidate_query_index',False))
            version='v2' if entry['variant']=='candidate' else 'v1'
            environment.configure_stack('streaming',project=project,overlays=overlays,
                                        stream_topic='incidentlens-spans-'+version,
                                        stream_group='incidentlens-stream-worker-'+version)
            compose=environment.compose_args()
            before=volume_inventory(project)
            command(compose+['stop','-t','15'],120)
            command(compose+['down','--timeout','15'],120)
            after=volume_inventory(project)
            if before!=after:raise RuntimeError('abandoned experiment volume preservation failed: '+project)
            attempt.setdefault('cleanup',[]).append({'project':project,'retained_volumes':after})
            save()
        attempt['cleanup_complete']=True;save()


def volume_inventory(project):
    probe=environment.command(['docker','volume','ls','--filter','label=com.docker.compose.project='+project,'--format','{{.Name}}'])
    if probe.get('returncode')!=0:raise RuntimeError('volume inventory failed')
    return sorted(probe['stdout'].split())


def image_inventory(candidate_tag='candidate1'):
    frozen=json.loads((ROOT/'benchmarks/baselines/phase5-streaming-20261003/manifest.json').read_text())
    result={}
    for variant in ('baseline','candidate'):
        result[variant]={}
        for service,component in [('stream-ingest','producer'),('stream-worker','worker'),('api','api')]:
            reference=('incidentlens-phase5-baseline-'+component+':20261003' if variant=='baseline' else 'incidentlens-hardening-'+component+':'+candidate_tag)
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
    mode=parser.add_mutually_exclusive_group()
    mode.add_argument('--smoke', action='store_true', help='unmeasured paired 120-load-span verification with setup traces')
    mode.add_argument('--probe-sustained', action='store_true', help='diagnostic 8000 spans/s pair only; never a full acceptance campaign')
    parser.add_argument('--resume', action='store_true', help='resume a failed campaign with unchanged controls and provenance')
    parser.add_argument('--candidate-tag', default='candidate1', help='prebuilt candidate image tag')
    parser.add_argument('--candidate-query-index', action='store_true', help='apply the declared trace-ID index only to the candidate schema')
    args = parser.parse_args()
    if not args.candidate_tag or any(c not in 'abcdefghijklmnopqrstuvwxyz0123456789.-_' for c in args.candidate_tag):
        raise ValueError('candidate tag must use lowercase Docker tag characters')
    output=args.output.resolve()
    if args.resume:
        if not output.is_dir():raise ValueError('resume output directory does not exist')
    else:output.mkdir(parents=True,exist_ok=False)
    with (output/'.runner.lock').open('a+') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError as error:raise RuntimeError('campaign runner already holds the output lock') from error
        return run_campaign(args,output)


def run_campaign(args,output):
    os.environ['STREAM_CANDIDATE_TAG']=args.candidate_tag
    binary=args.binary.resolve(); lag_binary=args.lag_binary.resolve()
    if not all(p.is_file() and os.access(p,os.X_OK) for p in [binary,lag_binary]):
        raise ValueError('build both benchmark executables before running')
    provenance={'smoke':args.smoke,'probe_sustained':args.probe_sustained,'candidate_tag':args.candidate_tag,
            'candidate_query_index':args.candidate_query_index,
            'declaration_sha256':environment.digest(DECLARATION),
            'baseline_manifest_sha256':environment.digest(ROOT/'benchmarks/baselines/phase5-streaming-20261003/manifest.json'),
            'source_manifest':environment.manifest(),
            'images':image_inventory(args.candidate_tag),
            'binary':{'path':str(binary),'sha256':environment.digest(binary)},
            'lag_binary':{'path':str(lag_binary),'sha256':environment.digest(lag_binary)},
            }
    if args.resume:
        record=json.loads((output/'campaign.json').read_text())
        completed=resume_position(output,record,provenance,locked=True)
    else:
        record={'started_at':environment.utc(), 'status':'running',
                'cases':[], 'commands':[], **provenance}
        completed=0
    def save(): run.save(output/'campaign.json',record)
    def command(arguments, timeout=180):
        result=environment.command(arguments,timeout=timeout)
        record['commands'].append(result);save()
        if result.get('returncode') != 0: raise RuntimeError('command failed: '+str(arguments))
        return result
    if args.resume and completed<len(schedule(args.smoke,args.probe_sustained)):
        archive_incomplete_pair(output,record,completed,save)
    elif args.resume:
        record.setdefault('resume_finalization',[]).append({'at':environment.utc(),
                                                             'prior_status':record['status'],
                                                             'prior_error':record.get('error')})
        record.pop('error',None);record['status']='running';save()
    else:save()
    original=['docker','compose','-f',str(environment.COMPOSE)]
    used_projects={entry['project'] for entry in record['cases']}
    for attempt in record.get('abandoned_attempts',[]):
        used_projects.update(entry['project'] for entry in attempt['cases'])
    while True:
        prefix='incidentlens-retro-'+uuid.uuid4().hex[:8]
        if all(prefix+'-'+str(index) not in used_projects for index in range(len(schedule(args.smoke,args.probe_sustained)))):
            break
    active_compose=None; lag=None; lag_log=None; status_observer=None
    try:
        command(original+['stop','collector','ingest','api','clickhouse'])
        if args.resume:cleanup_abandoned(record,command,save)
        for name,rate,duration,warmup,repetition,variant,label in schedule(args.smoke,args.probe_sustained)[completed:]:
            path='streaming'
            directory=output/label;directory.mkdir()
            ops=directory/'operations';ops.mkdir(mode=0o777);ops.chmod(0o777)
            project=prefix+'-'+str(len(record['cases']))
            os.environ['RETRO_PROJECT']=project
            os.environ['BENCH_EVIDENCE_DIR']=str(ops)
            overlays=case_overlays(variant,args.candidate_query_index)
            version='v2' if variant=='candidate' else 'v1'
            topic='incidentlens-spans-'+version;group='incidentlens-stream-worker-'+version
            environment.configure_stack(path,project=project,overlays=overlays,stream_topic=topic,stream_group=group)
            active_compose=environment.compose_args()
            entry={'label':label,'project':project,'path':path,'repetition':repetition,
                   'variant':variant,'stage':case_stage(name,rate,duration,warmup,directory,args.smoke),'status':'starting'}
            record['cases'].append(entry);save()
            command(active_compose+['up','-d','--no-build'],240)
            wait_ready()
            provenance=environment.environment(binary)
            run.save(directory/'environment.json',provenance)
            if len(provenance['containers']) != len(environment.SERVICES): raise RuntimeError('incomplete provenance')
            verify_images(provenance, record['images'][variant])
            if provenance['source_manifest'] != record['source_manifest']:raise RuntimeError('source changed during campaign')
            initial=environment.storage_snapshot();run.save(directory/'initial-storage.json',initial)
            index_sql="SELECT name,expr,type,granularity FROM system.data_skipping_indices WHERE database='incidentlens' AND table='spans' ORDER BY name FORMAT JSON"
            indexes=command(active_compose+['exec','-T','clickhouse','clickhouse-client','--password','local-admin','--query',index_sql])
            run.save(directory/'initial-query-indexes.json',indexes)
            verify_query_indexes(json.loads(indexes['stdout'])['data'],variant=='candidate' and args.candidate_query_index)
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
            # Fresh case volumes contain only this case's query log. Capture
            # bounded post-load diagnostics, never change query traffic/budgets.
            query_sql="SELECT type,event_time_microseconds,query_start_time_microseconds,query_duration_ms,read_rows,result_rows,result_bytes,memory_usage,exception_code FROM system.query_log WHERE initial_user='query' AND query_kind='Select' ORDER BY query_start_time_microseconds,event_time_microseconds LIMIT 50001 SETTINGS max_execution_time=5 FORMAT JSONEachRow"
            queries=environment.command(active_compose+['exec','-T','clickhouse','clickhouse-client','--password','local-admin','--query',query_sql])
            run.save(directory/'server-queries.json',{'observation':queries,
                     'row_limit':50001,'possibly_capped':len(queries.get('stdout','').splitlines())>=50001,
                     'scope':'post-load diagnostics only; no per-request HTTP cause attribution'})
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
            # Completion is published only after the raw case has been analyzed.
            # A crash here leaves an incomplete pair that resume archives and
            # reruns together, rather than a falsely completed case.
            entry['status']='analyzing';save()
            analyzed=analyze_retrospective.analyze_case(directory,{**entry,'status':'completed'})
            run.save(directory/'analysis-exact.json',analyzed)
            if analyzed['status']!='observed':
                entry['status']='evidence_incomplete';save()
                raise RuntimeError('case evidence incomplete: '+repr(analyzed['issues']))
            entry['status']='completed';save()
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
