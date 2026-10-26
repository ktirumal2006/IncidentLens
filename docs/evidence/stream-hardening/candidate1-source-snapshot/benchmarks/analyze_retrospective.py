#!/usr/bin/env python3
"""Exact, fail-closed analysis of the isolated direct/streaming experiment."""
import argparse
import hashlib
import json
from collections import Counter, defaultdict
from fractions import Fraction
from pathlib import Path

import analyze

OP_LIMIT = 64 << 20
CASE_SPECS = ([('sustained-'+str(rate),rate,120,10) for rate in (500,2000,8000)] +
              [('burst-'+str(rate),rate,10,0) for rate in (2000,8000,32000)] +
              [('slowdown-2000',2000,120,10)])


def expected_cases():
    result=[]
    for repetition in (1,2):
        for name,rate,duration,warmup in CASE_SPECS:
            for path in (('direct','streaming') if repetition==1 else ('streaming','direct')):
                result.append((f'{name}-r{repetition}-{path}',path,repetition,rate,duration,warmup))
    return result


def validate_schedule(cases, issues):
    expected=expected_cases()
    if len(cases)!=len(expected):issues.append(f'expected 28 completed paired cases, found {len(cases)}')
    if len({case['label'] for _,case in cases})!=len(cases):issues.append('duplicate case label')
    for index,(directory,case) in enumerate(cases):
        if index>=len(expected):break
        label,path,repetition,rate,duration,warmup=expected[index]
        work=case.get('stage',{}).get('workload',{})
        controls={'rate':rate,'duration':f'{duration}s','warmup':f'{warmup}s','concurrency':16,'batch-traces':32,
                  'queue':64,'attempts':3,'export-timeout':'8s','drain':'120s','settle':'35s','query-rate':4,
                  'query-concurrency':4,'query-mix':'services,traces,detail,incidents','baseline-traces':128,
                  'attribute-bytes':256,'visibility-every':10,'visibility-timeout':'10s','max-event-bytes':268435456}
        if (case.get('label'),case.get('path'),case.get('repetition'))!=(label,path,repetition) or case.get('stage',{}).get('timeout_seconds')!=480 or work!=controls:
            issues.append(f'case {index} label/path/repetition/workload differs from predeclared schedule')


def campaign_cases(root, issues):
    campaign=load_json(root/'campaign.json',issues)
    attempts=[{'path':str(root),'status':campaign.get('status'),'error':campaign.get('error')}]
    completed=[(root/case['label'],case) for case in campaign.get('cases',[]) if case.get('status')=='completed']
    continuation=campaign.get('continuation_of')
    if continuation:
        parent=Path(continuation.get('path',''))
        if not parent.is_absolute() or parent.resolve()==root.resolve():
            issues.append('invalid continuation parent path')
            return campaign,completed,attempts
        raw=parent/'campaign.json'
        try: digest=hashlib.sha256(raw.read_bytes()).hexdigest()
        except OSError as error:
            issues.append(f'continuation parent unavailable: {error}')
            return campaign,completed,attempts
        if digest!=continuation.get('sha256'):issues.append('continuation parent campaign digest mismatch')
        prior=load_json(raw,issues)
        if prior.get('status')!='failed' or prior.get('continuation_of') or prior.get('smoke'):
            issues.append('continuation parent is not a failed full original campaign')
        prior_completed=[case for case in prior.get('cases',[]) if case.get('status')=='completed']
        if [case['label'] for case in prior_completed]!=continuation.get('completed_labels'):
            issues.append('continuation completed-label inventory differs from parent')
        if len(prior_completed)!=26 or len(prior.get('cases',[]))!=27 or prior['cases'][-1].get('status')!='starting':
            issues.append('parent startup-failure boundary unexpected')
        if prior.get('declaration_sha256')!=campaign.get('declaration_sha256') or prior.get('lag_binary',{}).get('sha256')!=campaign.get('lag_binary',{}).get('sha256'):
            issues.append('continuation declaration/lag binary provenance differs')
        parent_env=load_json(parent/prior_completed[0]['label']/'environment.json',issues) if prior_completed else {}
        current_env=load_json(root/campaign['cases'][0]['label']/'environment.json',issues) if campaign.get('cases') else {}
        if parent_env.get('binary',{}).get('sha256') is None or parent_env.get('binary',{}).get('sha256')!=current_env.get('binary',{}).get('sha256'):
            issues.append('continuation benchmark binary hash differs or unavailable')
        attempts.insert(0,{'path':str(parent),'status':prior.get('status'),'error':prior.get('error'),
                           'unmeasured_startup_failure':prior['cases'][-1]['label'] if prior.get('cases') else None})
        completed=[(parent/case['label'],case) for case in prior_completed]+completed
    return campaign,completed,attempts


def ratio(numerator, denominator):
    return None if denominator <= 0 else str(Fraction(numerator, denominator))


def quantiles(values):
    values = sorted(values)
    if not values:
        return {"count": 0, "p50_ns": None, "p95_ns": None, "p99_ns": None}
    return {"count": len(values), **{f"p{p}_ns": values[min(len(values)-1, len(values)*p//100)] for p in (50, 95, 99)}}


def load_json(path, issues):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError) as error:
        issues.append(f"{path}: {error}")
        return {}


def load_lines(path, issues, limit=None):
    if limit is not None and path.exists() and path.stat().st_size > limit:
        issues.append(f"{path}: exceeds {limit} byte limit")
        return []
    return analyze.read_jsonl(path, issues)


def operation_file(path, issues):
    rows = load_lines(path, issues, OP_LIMIT)
    if not rows:
        issues.append(f"{path}: no operation records")
        return []
    trailer = rows[-1]
    if trailer.get("kind") != "recorder_end" or trailer.get("events") != len(rows)-1 or trailer.get("dropped") != 0 or trailer.get("write_failed") is not False:
        issues.append(f"{path}: missing/incomplete recorder trailer")
    result = rows[:-1] if trailer.get("kind") == "recorder_end" else rows
    for row in result:
        if (not isinstance(row.get("started_ns"), int) or not isinstance(row.get("finished_ns"), int)
                or row["finished_ns"] < row["started_ns"] or not isinstance(row.get("duration_ns"), int) or row["duration_ns"] < 0
                or not isinstance(row.get("rows"), int) or row["rows"] < 0 or not isinstance(row.get("success"), bool)):
            issues.append(f"{path}: invalid operation event")
            break
    return result


def operation_metrics(rows, kind, start, end):
    selected = [r for r in rows if r.get("kind") == kind and start <= r.get("started_ns", -1) < end]
    successes = [r for r in selected if r.get("success") is True]
    completed = [r for r in rows if r.get("kind") == kind and start <= r.get("finished_ns", -1) < end]
    completed_successes = [r for r in completed if r.get("success") is True]
    failures = len(selected)-len(successes)
    fields = {"total": "duration_ns"}
    if kind == "clickhouse_write":
        fields.update(prepare="prepare_ns", append="append_ns", send="send_ns")
    return {"started_window_attempts":len(selected), "started_window_successful_operations":len(successes), "started_window_failed_operations":failures,
            "started_window_successful_rows":sum(r["rows"] for r in successes), "started_window_attempted_rows":sum(r["rows"] for r in selected),
            "completed_window_successful_operations":len(completed_successes),
            "completed_window_successful_rows":sum(r["rows"] for r in completed_successes),
            "completed_window_failed_operations":len(completed)-len(completed_successes),
            "completed_rows_per_second":ratio(sum(r["rows"] for r in completed_successes)*1_000_000_000,end-start),
            "latency":{name:quantiles([r[field] for r in successes if isinstance(r.get(field),int)]) for name,field in fields.items()},
            "all_successful_rows_include_replays":True}


def bucket_add(buckets, timestamp_ns, key, count):
    if isinstance(timestamp_ns, int):
        buckets[timestamp_ns//1_000_000_000][key] += count


def event_buckets(events, issues):
    buckets=defaultdict(Counter)
    for event in events:
        kind=event.get('kind'); count=event.get('traces',0)*6
        field={'generated':'started','export_attempt':'started','batch_complete':'ack'}.get(kind)
        if field and (kind!='export_attempt' or event.get('attempt')==1) and (kind!='batch_complete' or event.get('status')=='acknowledged'):
            try: bucket_add(buckets,analyze.time_ns(event[field]),{'generated':'generated_spans','export_attempt':'emitted_spans','batch_complete':'acked_spans'}[kind],count)
            except (ValueError,TypeError,KeyError): issues.append(f'{kind}: invalid bucket clock')
    return buckets


def query_schedule(events, start, end, issues):
    """Count scheduled query outcomes separately from actual start-window requests."""
    routes=defaultdict(Counter)
    seen=set()
    for event in events:
        if event.get('kind') not in ('query','query_drop','query_skipped'):
            continue
        try:
            scheduled=analyze.time_ns(event['scheduled'])
        except (KeyError,TypeError,ValueError):
            issues.append('query outcome lacks scheduled clock')
            continue
        if not start <= scheduled < end:
            continue
        route=event.get('route','unknown'); key=(route,scheduled)
        if key in seen:
            issues.append('duplicate scheduled query outcome')
            continue
        seen.add(key)
        row=routes[route];row['scheduled_measured']+=1
        kind=event['kind']
        if kind=='query':
            row['issued_measured']+=1
            try:
                begun=analyze.time_ns(event['started'])
                row['started_in_window' if start <= begun < end else 'late_start']+=1
            except (KeyError,TypeError,ValueError):
                issues.append('issued query lacks start clock')
        elif kind=='query_drop':row['queue_dropped']+=1
        else:row['skipped_no_acknowledged_trace']+=1
    return {route:{name:values[name] for name in ('scheduled_measured','issued_measured','queue_dropped','skipped_no_acknowledged_trace','started_in_window','late_start')} for route,values in sorted(routes.items())}


def visibility_traffic(events, raw):
    probes=[r for r in events if r.get('kind')=='visibility_probe' and r.get('phase')=='measured']
    failed=sum(bool(r.get('error')) or not 200<=r.get('http_status',0)<300 for r in probes)
    observed=raw.get('visibility',{})
    return {'sample_count':observed.get('sample_count'), 'censored_count':observed.get('censored_count'),
            'observed_count':observed.get('first_send_to_observation',{}).get('count'),
            'first_send_to_observation':observed.get('first_send_to_observation'),
            'probe_http_requests_measured_phase':len(probes),'probe_http_failures':failed,
            'probe_http_statuses':dict(Counter(str(r.get('http_status',0)) for r in probes)),
            'probe_response_bytes':sum(r.get('response_bytes',0) for r in probes),
            'probe_request_latency':quantiles([r['latency_ns'] for r in probes if isinstance(r.get('latency_ns'),int)]),
            'note':'Probe HTTP traffic is separate from the four-route paced query mix.'}


def server_insert_summary(records):
    finished=[r for r in records if r.get('type')=='QueryFinish']
    fields=('InsertedRows','InsertedBytes','DiskWriteElapsedMicroseconds','NetworkReceiveElapsedMicroseconds','MergeTreeDataWriterRows','InsertedCompactParts')
    profiles=Counter()
    for row in finished:
        for field in fields:
            value=row.get('ProfileEvents',{}).get(field)
            if isinstance(value,int):profiles[field]+=value
    durations=[r['query_duration_ms'] for r in finished if isinstance(r.get('query_duration_ms'),int)]
    latency=quantiles(durations)
    return {'scope':'all_case_QueryFinish_including_setup_and_drain','query_finish_count':len(finished),
            'query_duration_ms':{k.replace('_ns','_ms'):v for k,v in latency.items()},
            'written_rows':sum(r.get('written_rows',0) for r in finished),
            'written_bytes':sum(r.get('written_bytes',0) for r in finished),
            'nonzero_exception_codes':sum(r.get('exception_code',0)!=0 for r in finished),
            'profile_event_totals':dict(profiles)}


def lag_analysis(rows, offering_finished, issues, load_start=0, run_end=None):
    if not rows or rows[-1].get("kind") != "observer_end" or rows[-1].get("samples") != len(rows)-1:
        issues.append("lag observer trailer absent/inconsistent")
    values=[]
    for row in rows:
        if row.get("kind") != "lag":
            continue
        if row.get("error") or row.get("uninitialized_or_inconsistent") or not all(isinstance(row.get(k),int) for k in ("started_ns","finished_ns","committed","end","lag_records")):
            if row.get('started_ns',0)>=load_start:
                issues.append("lag sample unavailable or inconsistent during load/settling")
            continue
        if row["finished_ns"] < row["started_ns"] or row["end"]-row["committed"] != row["lag_records"] or row["lag_records"] < 0:
            issues.append("lag sample invalid positions")
            continue
        values.append({k:row[k] for k in ("started_ns","finished_ns","committed","end","lag_records")})
    after=[r for r in values if r["started_ns"] >= offering_finished]
    active=[r for r in values if load_start <= r['started_ns'] <= (run_end or offering_finished)]
    if not active or active[0]['started_ns']-load_start > 3_000_000_000 or ((run_end or offering_finished)-active[-1]['finished_ns'] > 3_000_000_000):
        issues.append('lag observer lacks load/settling boundary coverage')
    if any(b['started_ns']-a['finished_ns']>3_000_000_000 for a,b in zip(active,active[1:])):
        issues.append('lag observer has >3s gap during load/settling')
    drain=None
    final_end=values[-1]['end'] if values else None
    for previous,current in zip(after,after[1:]):
        if previous["lag_records"] == current["lag_records"] == 0 and previous["end"] == current["end"] == final_end:
            prior_nonzero=[r["started_ns"] for r in after if r["started_ns"] < previous["started_ns"] and r["lag_records"] > 0]
            drain={"status":"observed_bounded", "lower_bound_ns":max(0,max(prior_nonzero)-offering_finished) if prior_nonzero else 0,
                   "upper_bound_ns":current["finished_ns"]-offering_finished,
                   "first_zero_started_ns":previous["started_ns"],"second_zero_finished_ns":current["finished_ns"],"stable_end":current["end"]}
            break
    if drain is None:
        drain={"status":"not_observed", "last_observation_ns":after[-1]["finished_ns"] if after else None}
    return {"samples":values,"sample_count":len(values),"post_offering_sample_count":len(after),
            "max_observed_lag_records":max((r["lag_records"] for r in values),default=None),
            "first_observed_lag_records":values[0]["lag_records"] if values else None,
            "last_observed_lag_records":values[-1]["lag_records"] if values else None,
            "net_observed_lag_change_records":values[-1]["lag_records"]-values[0]["lag_records"] if values else None,
            "growth_from_first_to_max_records":max(r["lag_records"] for r in values)-values[0]["lag_records"] if values else None,
            "drain":drain,"note":"Sequential committed/end reads are not an atomic snapshot; lag is records, not spans."}


def analyze_case(directory, case):
    issues=[]
    measurement=directory/'measurement'
    run=load_json(measurement/'run.json',issues)
    summary=load_json(measurement/'harness/summary.json',issues)
    config=load_json(measurement/'harness/config.json',issues)
    events=load_lines(measurement/'harness/events.jsonl',issues)
    ledger=load_lines(measurement/'harness/reconciliation.jsonl',issues)
    resources=load_lines(measurement/'resources.jsonl',issues)
    operations={}
    for name in (['ingest'] if case.get('path')=='direct' else ['producer','worker']):
        operations[name]=operation_file(directory/'operations'/f'{name}.jsonl',issues)
    if case.get('path')=='streaming':
        lag_rows=load_lines(directory/'lag.jsonl',issues)
    else:
        lag_rows=[]
    if run.get('status')!='completed' or case.get('status')!='completed':
        issues.append('case or measurement did not complete')
    if str(case.get('label','')).startswith('slowdown-'):
        fault=run.get('slowdown',{})
        if fault.get('status')!='completed' or fault.get('throttled_quota',{}).get('stdout','').strip()!='250000000' or fault.get('restored_quota',{}).get('stdout','').strip()!='2000000000':
            issues.append('declared CPU slowdown or quota restoration unverified')
    reconciliation=summary.get('reconciliation',{})
    if not all(reconciliation.get(k) is True for k in ('verified','snapshot_verified','settled')):
        issues.append('finite reconciliation not verified/settled')
    if summary.get('error') or summary.get('drain_deadline_expired'):
        issues.append('harness error or caller drain expired')
    raw_counts=Counter(f"{r.get('kind')}:{r.get('phase','')}" for r in events)
    if not summary.get('event_counts'):
        issues.append('event count inventory missing')
    for key,count in summary.get('event_counts',{}).items():
        if raw_counts[key] != count:
            issues.append(f'event count mismatch: {key}')
    try:
        start=analyze.time_ns(summary['measurement_start']); end=analyze.time_ns(summary['measurement_end'])
        offering_finished=analyze.time_ns(summary['offering_finished'])
        if end<=start: raise ValueError('nonpositive measurement interval')
    except (KeyError,ValueError,TypeError) as error:
        issues.append(f'measurement clocks unavailable: {error}')
        start=end=offering_finished=0
    raw=analyze.analyze_events(events,config,start,end,issues) if end>start else {}
    rec=analyze.analyze_reconciliation(ledger,config,events,issues)
    if rec.get('status')!='verified_ledger': issues.append('raw identity ledger invalid')
    observed=rec.get('counts',{})
    for key in ('planned_spans','expected_stored','acked_missing','emitted_missing'):
        if observed.get(key)!=reconciliation.get(key):
            issues.append(f'reconciliation mismatch: {key}')
    buckets=event_buckets(events,issues)
    op_metrics={}
    mapping={'ingest':['clickhouse_write'],'producer':['kafka_produce'],'worker':['clickhouse_write','consumer_commit']}
    for file,rows in operations.items():
        op_metrics[file]={kind:operation_metrics(rows,kind,start,end) if end>start else None for kind in mapping[file]}
        for kind in mapping[file]:
            if not any(row.get('kind')==kind for row in rows):
                issues.append(f'{file}: no {kind} operation records')
        for row in rows:
            key={'clickhouse_write':'successful_write_rows','kafka_produce':'broker_produced_rows','consumer_commit':'consumer_committed_rows'}.get(row.get('kind'))
            if key and row.get('success') is True:
                bucket_add(buckets,row.get('finished_ns'),key,row.get('rows',0))
    writer=operations.get('ingest',operations.get('worker',[]))
    successful_writes=[r['finished_ns'] for r in writer if r.get('kind')=='clickhouse_write' and r.get('success') is True]
    last_write=max(successful_writes,default=None)
    try:
        load_start=analyze.time_ns(summary['load_start'])
        run_end=analyze.time_ns(summary['run_end'])
    except (KeyError,TypeError,ValueError):
        issues.append('load/run end clocks unavailable for lag coverage')
        load_start=0;run_end=offering_finished
    lag=lag_analysis(lag_rows,offering_finished,issues,load_start,run_end) if case.get('path')=='streaming' else None
    supplemental=None
    final={}
    supplemental_path=measurement/'supplemental-drain.json'
    if supplemental_path.exists():
        supplemental=load_json(supplemental_path,issues)
        final=supplemental.get('observations',[])[-1] if supplemental.get('observations') else {}
        if (supplemental.get('complete') is not True or final.get('snapshot_verified') is not True
                or final.get('emitted_missing')!=0 or final.get('acked_missing')!=0):
            issues.append('supplemental drain evidence incomplete')
    server_issues=[]
    server=load_json(directory/'server-inserts.json',server_issues)
    server_records=[]
    observation=server.get('observation',{})
    if server.get('flush',{}).get('returncode')!=0 or observation.get('returncode')!=0:
        server_issues.append('server insert diagnostics unavailable')
    else:
        for line in observation.get('stdout','').splitlines():
            try: server_records.append(json.loads(line))
            except ValueError: server_issues.append('malformed server insert diagnostic')
    query={}
    for route,values in raw.get('queries',{}).items():
        successful=values['successful']; total=values['successful_requests']+values['failed_requests']
        query[route]={"success_count":values['successful_requests'],"failure_count":values['failed_requests'],
                      "error_fraction":ratio(values['failed_requests'],total),"successful_p95_ns":successful['p95_ns'],
                      "successful_p50_ns":successful['p50_ns'],"successful_p99_ns":successful['p99_ns'],"statuses":values['statuses']}
    full_generated=sum(r.get('traces',0)*6 for r in events if r.get('kind')=='generated')
    scheduled_queries=query_schedule(events,start,end,issues) if end>start else {}
    visibility=visibility_traffic(events,raw)
    counts={"planned_spans":config.get('load_spans',0)+config.get('setup_spans',0),
            "generated_full_run_spans":full_generated,
            "emitted_full_run_spans":reconciliation.get('emitted_spans'),
            "acked_full_run_spans":reconciliation.get('acked_spans'),
            "generated_measured_spans":raw.get('measured_counts_spans',{}).get('generated'),
            "emitted_measured_spans":raw.get('measured_counts_spans',{}).get('attempted'),
            "acked_measured_spans":raw.get('measured_counts_spans',{}).get('acknowledged'),
            "acked_within_window_spans":raw.get('measured_counts_spans',{}).get('acknowledged_within_window'),
            "stored_unique_final_spans":observed.get('expected_stored'),
            "stored_unique_at_original_snapshot_spans":observed.get('expected_stored'),
            "physical_rows_at_snapshot":reconciliation.get('raw_rows'),
            "never_emitted_driver_spans":observed.get('planned_spans',0)-reconciliation.get('emitted_spans',0) if reconciliation else None,
            "unresolved_acked_at_original_deadline":reconciliation.get('acked_missing'),
            "unresolved_emitted_at_original_deadline":reconciliation.get('emitted_missing')}
    return {"label":case.get('label',directory.name),"path":case.get('path'),"status":"incomplete" if issues else "observed",
            "issues":issues,"window":{"from_ns":start,"to_ns":end,"duration_ns":end-start},
            "counts":counts,"ack_latency":raw.get('acknowledgment_from_first_send'),"queries":query,
            "query_schedule":scheduled_queries,"visibility":visibility,
            "operations":op_metrics,"lag":lag,
            "last_successful_write_finished_ns":last_write,
            "last_successful_write_relative_to_offering_finished_ns":last_write-offering_finished if last_write is not None else None,
            "slowdown":run.get('slowdown'),"supplemental_drain":supplemental,
            "supplemental_final_stored_unique_spans":final.get('expected_stored') if supplemental else None,
            "one_second_buckets":[{"epoch_second":s,**dict(v)} for s,v in sorted(buckets.items())],
            "server_inserts":{"status":"observed" if not server_issues else "unknown", "issues":server_issues,
                              "query_finish_summary":server_insert_summary(server_records) if not server_issues else None,
                              "records":server_records},
            "resource_sample_count":sum(r.get('kind')=='resource_sample' for r in resources),
            "clock_note":"Per-process Unix wall clocks align buckets; durations use local monotonic clocks. Host/VM skew and sequential offset reads bound causal timing claims."}


def markdown(report):
    lines=['# Controlled retrospective analysis','', 'Counts and durations are exact integers; rates/error fractions are reduced rational strings. A missing recorder/lag trailer or incomplete identities invalidates the affected case. Direct ACK is storage completion; streaming ACK is broker admission.','',
           '| Case | Path | Status | Generated measured | ACKed measured | FINAL unique | ACK p50/p95/p99 ns | ACKed missing at deadline |',
           '| --- | --- | --- | ---: | ---: | ---: | --- | ---: |']
    for c in report['cases']:
        a=c.get('ack_latency') or {}
        lines.append(f"| {c['label']} | {c['path']} | {c['status']} | {c['counts']['generated_measured_spans']} | {c['counts']['acked_measured_spans']} | {c['counts']['stored_unique_final_spans']} | {a.get('p50_ns')}/{a.get('p95_ns')}/{a.get('p99_ns')} | {c['counts']['unresolved_acked_at_original_deadline']} |")
    lines+=['','## Hypothesis evidence map','',
            '- Burst absorption: compare exact ACK counts and latency during each ten-second pulse with one-second broker production, writer/commit buckets, lag growth and later drain bounds.',
            '- Ingestion/storage decoupling: compare streaming broker ACK buckets with ClickHouse write/commit buckets and FINAL identity counts; do not equate broker acceptance with visibility.',
            '- Backpressure isolation: compare scheduling drops, failed exports and producer failures with direct write failures at matched rates; distinguish caller/Collector queues from product capacity.',
            '- Downstream slowdown recovery: compare the paired 0.25-CPU intervention records, write/commit progress, lag and post-restore drain. A case without a completed intervention cannot support this claim.',
            '', '## Per-case evidence','']
    for c in report['cases']:
        lines.append(f"### {c['label']}")
        lines.append(f"Path {c['path']}; status {c['status']}; planned {c['counts']['planned_spans']}, never emitted {c['counts']['never_emitted_driver_spans']}, unresolved emitted {c['counts']['unresolved_emitted_at_original_deadline']}. ")
        for filename,kinds in c['operations'].items():
            for kind,m in kinds.items():
                if m:
                    lines.append(f"- {filename}/{kind}: {m['completed_window_successful_rows']} completed physical rows; {m['completed_window_failed_operations']} completed failures; {m['completed_rows_per_second']} completed rows/s. Start-window latency cohort: {m['started_window_successful_operations']} successes, {m['started_window_failed_operations']} failures; total p50/p95/p99 ns {m['latency']['total']['p50_ns']}/{m['latency']['total']['p95_ns']}/{m['latency']['total']['p99_ns']}.")
                    if kind=='clickhouse_write':
                        for phase in ('prepare','append','send'):
                            q=m['latency'][phase]
                            lines.append(f"  - {phase} p50/p95/p99 ns {q['p50_ns']}/{q['p95_ns']}/{q['p99_ns']} (n={q['count']}).")
        lines.append(f"- Last successful ClickHouse write finished {c['last_successful_write_relative_to_offering_finished_ns']} ns relative to end of offering; positive values are post-pulse writes.")
        if c['lag']:
            l=c['lag'];lines.append(f"- Kafka lag records: first {l['first_observed_lag_records']}, max sampled {l['max_observed_lag_records']}, last {l['last_observed_lag_records']}; post-offering drain {l['drain']}.")
        if c['supplemental_drain']:
            lines.append(f"- Separate supplemental drain: complete={c['supplemental_drain'].get('complete')}; original-deadline missing remains {c['counts']['unresolved_acked_at_original_deadline']} ACKed spans.")
        if c['slowdown']:
            lines.append(f"- Controlled slowdown status {c['slowdown'].get('status')}; observed quota {c['slowdown'].get('throttled_quota',{}).get('stdout','').strip()} then restored {c['slowdown'].get('restored_quota',{}).get('stdout','').strip()} nanocpus.")
        for route,q in c['queries'].items():
            lines.append(f"- {route} query: p95 {q['successful_p95_ns']} ns; failures {q['failure_count']}/{q['success_count']+q['failure_count']} ({q['error_fraction']}).")
        for route,schedule in c['query_schedule'].items():
            lines.append(f"- {route} scheduled query outcomes: {schedule}.")
        lines.append(f"- Visibility probes, separate from paced queries: {c['visibility']['probe_http_requests_measured_phase']} HTTP requests, {c['visibility']['probe_http_failures']} HTTP failures; {c['visibility']['sample_count']} sampled batches, {c['visibility']['censored_count']} censored.")
        if c['server_inserts']['query_finish_summary']:
            s=c['server_inserts']['query_finish_summary']
            lines.append(f"- ClickHouse server QueryFinish (all case): {s['query_finish_count']} records, {s['written_rows']} written rows, source-ms duration p50/p95/p99 {s['query_duration_ms']['p50_ms']}/{s['query_duration_ms']['p95_ms']}/{s['query_duration_ms']['p99_ms']}; selected ProfileEvents totals {s['profile_event_totals']}.")
        if c['supplemental_final_stored_unique_spans'] is not None:
            lines.append(f"- Supplemental FINAL unique stored: {c['supplemental_final_stored_unique_spans']}; original snapshot stored {c['counts']['stored_unique_at_original_snapshot_spans']}.")
        if c['issues']: lines.extend(f"- Evidence gap: {i}" for i in c['issues'])
        lines.append('')
    return '\n'.join(lines)+'\n'


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaign',type=Path)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    issues=[]
    campaign, retained, attempts=campaign_cases(args.campaign,issues)
    validate_schedule(retained,issues)
    cases=[]
    for directory,case in retained:
        cases.append(analyze_case(directory,case))
    if not cases:issues.append('no retained cases')
    if campaign.get('status')!='completed':issues.append('campaign did not complete')
    report={'schema_version':1,'campaign_status':campaign.get('status','unknown'),'campaign_attempts':attempts,
            'issues':issues,'cases':cases,
            'method':'Exact raw event/operation/identity analysis; no rounded rates. Cases remain paired but separate; no causal gain inferred from ACK alone.'}
    args.output.mkdir(parents=True,exist_ok=True)
    (args.output/'analysis.json').write_text(json.dumps(report,indent=2,sort_keys=True)+'\n')
    (args.output/'tables.md').write_text(markdown(report))
    return int(bool(issues) or any(c['status']=='incomplete' for c in cases))

if __name__=='__main__':raise SystemExit(main())
