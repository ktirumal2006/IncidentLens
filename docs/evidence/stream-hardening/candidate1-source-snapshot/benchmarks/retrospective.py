#!/usr/bin/env python3
"""Serial, isolated paired retrospective; retains data and restores the direct stack."""
import argparse
import datetime
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid

import environment
import run
import analyze_retrospective

ROOT = environment.ROOT
DECLARATION = ROOT / 'benchmarks/WORKLOAD_RETROSPECTIVE.md'


def cases():
    return ([('sustained-'+str(rate), rate, 120, 10) for rate in [500, 2000, 8000]] +
            [('burst-'+str(rate), rate, 10, 0) for rate in [2000, 8000, 32000]] +
            [('slowdown-2000', 2000, 120, 10)])


def stage(name, rate, duration, warmup):
    return {'name': name, 'timeout_seconds': 480, 'workload': {
        'rate': rate, 'duration': str(duration)+'s', 'warmup': str(warmup)+'s',
        'concurrency': 16, 'batch-traces': 32, 'queue': 64, 'attempts': 3,
        'export-timeout': '8s', 'drain': '120s', 'settle': '35s',
        'query-rate': 4, 'query-concurrency': 4,
        'query-mix': 'services,traces,detail,incidents', 'baseline-traces': 128,
        'attribute-bytes': 256, 'visibility-every': 10, 'visibility-timeout': '10s',
        'max-event-bytes': 268435456}}


def slowdown(stop, records, result, active, events):
    while not stop.is_set():
        load = run.read_load_start(events)
        if load is not None:
            break
        stop.wait(.1)
    else:
        result.update(status='not_started')
        return
    if stop.wait(max(0, load[1]+30-time.time())):
        result.update(status='not_started')
        return
    ids = environment.command(environment.compose_args()+['ps', '-q', 'clickhouse'])
    identity = ids.get('stdout', '').split()
    if ids.get('returncode') != 0 or len(identity) != 1:
        result.update(status='failed', error='ClickHouse identity unavailable')
        return
    active.set()
    result.update(status='running', load_start=load[0], started_at=environment.utc(),
                  actual_offset_seconds=time.time()-load[1], container_id=identity[0])
    try:
        result['throttle'] = environment.command(['docker', 'update', '--cpus', '0.25', identity[0]], timeout=15)
        result['throttled_quota'] = environment.command(['docker', 'inspect', '--format', '{{.HostConfig.NanoCpus}}', identity[0]])
        records.write({'kind': 'slowdown_start', 'at': environment.utc(), **result})
        if result['throttle']['returncode'] != 0 or result['throttled_quota'].get('stdout','').strip() != '250000000':
            raise RuntimeError('throttle not verified')
        started = time.monotonic_ns()
        early = stop.wait(30)
        result.update(throttled_ns=time.monotonic_ns()-started,
                      status='shortened' if early else 'completed')
    except Exception as exc:
        result.update(status='failed', error=repr(exc))
    finally:
        result['restore'] = environment.command(['docker', 'update', '--cpus', '2', identity[0]], timeout=15)
        result['restored_quota'] = environment.command(['docker', 'inspect', '--format', '{{.HostConfig.NanoCpus}}', identity[0]])
        if result['restore']['returncode'] != 0 or result['restored_quota'].get('stdout','').strip() != '2000000000':
            result['status'] = 'restore_failed'
        result['restored_at'] = environment.utc()
        active.clear()
        records.write({'kind': 'slowdown_restore', 'at': environment.utc(), **result})


def wait_ready(timeout=120):
    deadline = time.monotonic()+timeout
    while time.monotonic() < deadline:
        if run.readiness(timeout=2) == environment.expected_readiness():
            return
        time.sleep(1)
    raise RuntimeError('dependencies not ready')


def continuation(directory, binary, lag_binary):
    """Continue only after a pre-load startup failure; keep the original immutable."""
    source=directory.resolve()/'campaign.json'
    previous=json.loads(source.read_text())
    if (previous.get('status')!='failed' or previous.get('smoke')
            or previous.get('continuation_of') or previous.get('restore_error')
            or previous.get('restore_direct',{}).get('returncode')!=0):
        raise ValueError('continuation requires a failed, restored full campaign')
    if previous.get('declaration_sha256')!=environment.digest(DECLARATION):
        raise ValueError('workload declaration changed')
    if previous.get('lag_binary',{}).get('sha256')!=environment.digest(lag_binary):
        raise ValueError('lag observer binary changed')
    expected=[(name+'-r'+str(rep)+'-'+path,stage(name,rate,duration,warmup))
              for rep in (1,2) for name,rate,duration,warmup in cases()
              for path in (['direct','streaming'] if rep==1 else ['streaming','direct'])]
    entries=previous.get('cases',[])
    if not entries or len(entries)>len(expected):
        raise ValueError('invalid parent case inventory')
    completed=[]
    for index,entry in enumerate(entries):
        label,specification=expected[index]
        if entry.get('label')!=label or entry.get('stage')!=specification:
            raise ValueError('parent case order or workload changed')
        if entry.get('status')=='completed':
            provenance=json.loads((directory/label/'environment.json').read_text())
            analysis=json.loads((directory/label/'analysis-exact.json').read_text())
            if provenance['binary']['sha256']!=environment.digest(binary) or analysis.get('status')!='observed':
                raise ValueError('parent binary changed or completed evidence invalid')
            completed.append(label)
        elif index!=len(entries)-1 or entry.get('status')!='starting' or entry.get('result'):
            raise ValueError('cannot silently retry a measured or incomplete case')
    if len(completed)!=len(entries)-1:
        raise ValueError('parent must end in one unmeasured startup failure')
    return {'path':str(directory.resolve()),'sha256':environment.digest(source),
            'completed_labels':completed,'startup_failure':previous.get('error')}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--lag-binary', type=Path, required=True)
    parser.add_argument('--smoke', action='store_true', help='unmeasured paired 120-span instrumentation verification')
    parser.add_argument('--continue-from',type=Path,help='retain and skip verified cases after a pre-load startup failure')
    args = parser.parse_args()
    output=args.output.resolve(); output.mkdir(parents=True,exist_ok=False)
    binary=args.binary.resolve(); lag_binary=args.lag_binary.resolve()
    if not all(p.is_file() and os.access(p,os.X_OK) for p in [binary,lag_binary]):
        raise ValueError('build both benchmark executables before running')
    if args.smoke and args.continue_from:raise ValueError('cannot continue a smoke campaign')
    parent=continuation(args.continue_from,binary,lag_binary) if args.continue_from else None
    record={'started_at':environment.utc(), 'status':'running', 'smoke':args.smoke,
            'declaration_sha256':environment.digest(DECLARATION),
            'lag_binary':{'path':str(lag_binary),'sha256':environment.digest(lag_binary)},
            'cases':[], 'commands':[]}
    if parent:record['continuation_of']=parent
    def save(): run.save(output/'campaign.json',record)
    def command(arguments, timeout=180):
        result=environment.command(arguments,timeout=timeout)
        record['commands'].append(result);save()
        if result.get('returncode') != 0: raise RuntimeError('command failed: '+str(arguments))
        return result
    save()
    original=['docker','compose','-f',str(environment.COMPOSE)]
    prefix='incidentlens-retro-'+uuid.uuid4().hex[:8]
    active_compose=None; lag=None; lag_log=None
    try:
        command(original+['stop','collector','ingest','api','clickhouse'])
        specifications=[('smoke',24,5,0)] if args.smoke else cases()
        for repetition in range(1,2 if args.smoke else 3):
            for name,rate,duration,warmup in specifications:
                for path in (['direct','streaming'] if repetition==1 else ['streaming','direct']):
                    label=name+'-r'+str(repetition)+'-'+path
                    if parent and label in parent['completed_labels']:continue
                    directory=output/label;directory.mkdir()
                    ops=directory/'operations';ops.mkdir(mode=0o777);ops.chmod(0o777)
                    project=prefix+'-'+str(len(record['cases']))
                    os.environ['RETRO_PROJECT']=project
                    os.environ['BENCH_EVIDENCE_DIR']=str(ops)
                    overlay=ROOT/'deploy/benchmark'/('direct.yaml' if path=='direct' else 'streaming.yaml')
                    environment.configure_stack(path,project=project,overlays=[overlay])
                    active_compose=environment.compose_args()
                    entry={'label':label,'project':project,'path':path,'repetition':repetition,
                           'stage':stage(name,rate,duration,warmup),'status':'starting'}
                    if args.smoke:
                        entry['stage']['workload'].update({'baseline-traces':0,'query-rate':0,'batch-traces':4,'settle':'2s'})
                    record['cases'].append(entry);save()
                    command(active_compose+['up','-d','--no-build'],240)
                    wait_ready()
                    provenance=environment.environment(binary)
                    run.save(directory/'environment.json',provenance)
                    if len(provenance['containers']) != len(environment.SERVICES): raise RuntimeError('incomplete provenance')
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
                        lag=subprocess.Popen([str(lag_binary),'--output',str(directory/'lag.jsonl')],stderr=lag_log)
                        entry['lag_pid']=lag.pid;save()
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
                    command(active_compose+['stop','-t','15'],120)
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
