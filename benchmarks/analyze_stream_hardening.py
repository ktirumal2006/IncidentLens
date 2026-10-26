#!/usr/bin/env python3
"""Exact paired analysis for the predeclared Phase 5 hardening campaign."""
import argparse
import hashlib
import json
from fractions import Fraction
from pathlib import Path

import analyze_retrospective as retro

ROOT = Path(__file__).resolve().parent
DECLARATION = ROOT / 'WORKLOAD_STREAM_HARDENING.md'
BASELINE_MANIFEST = ROOT / 'baselines/phase5-streaming-20261003/manifest.json'
SPECS = [('sustained-2000', 2000, 10, 120), ('sustained-8000', 8000, 10, 120),
         ('burst-32000', 32000, 0, 10), ('slowdown-2000', 2000, 10, 120)]
ROUTES = ('services', 'traces', 'detail', 'incidents')


def expected_cases():
    return [(f'{name}-r{rep}-{variant}', name, variant, rep, rate, warmup, duration)
            for rep in (1, 2) for name, rate, warmup, duration in SPECS
            for variant in (('baseline', 'candidate') if rep == 1 else ('candidate', 'baseline'))]


def frac(value):
    if isinstance(value, bool): raise ValueError('boolean is not a measurement')
    if isinstance(value, int): return Fraction(value)
    if isinstance(value, str): return Fraction(value)
    raise ValueError('missing or non-exact measurement')


def valid_count(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError('invalid count')
    return value


def gate(name, observed, predicate):
    try:
        passed = bool(predicate())
        status = 'pass' if passed else 'fail'
    except (KeyError, ValueError, TypeError, ZeroDivisionError, IndexError):
        status = 'unavailable'
    return {'name': name, 'status': status, 'observed': observed}


def fraction_increase_within(base, candidate):
    if base is None or candidate is None:
        raise ValueError('missing paired fraction')
    return candidate-base <= Fraction(1,20)


def censored_fraction(case):
    visible = case['visibility']
    samples = valid_count(visible['sample_count'])
    censored = valid_count(visible['censored_count'])
    if samples == 0 or censored > samples:
        raise ValueError('invalid visibility fraction')
    return Fraction(censored, samples)


def same_controls(cases, issues):
    expected = expected_cases()
    if len(cases) != len(expected): issues.append(f'expected 16 cases, found {len(cases)}')
    if len({c.get('label') for c in cases}) != len(cases): issues.append('duplicate case label')
    for i, (case, wanted) in enumerate(zip(cases, expected)):
        label, name, variant, rep, rate, warmup, duration = wanted
        work = case.get('stage', {}).get('workload', {})
        required = {'rate':rate, 'warmup':f'{warmup}s', 'duration':f'{duration}s',
                    'concurrency':16, 'batch-traces':32, 'queue':64, 'attempts':3,
                    'export-timeout':'8s', 'drain':'120s', 'settle':'35s',
                    'attribute-bytes':256, 'query-rate':4, 'query-concurrency':4,
                    'query-mix':'services,traces,detail,incidents', 'baseline-traces':128,
                    'visibility-every':10, 'visibility-timeout':'10s', 'max-event-bytes':268435456}
        lag_file = work.get('setup-lag-file')
        if not isinstance(lag_file, str) or Path(lag_file).parts[-2:] != (label, 'lag.jsonl'):
            issues.append(f'{label}: setup lag barrier path missing or incorrect')
        required['setup-lag-file'] = lag_file
        if (case.get('label'), case.get('variant'), case.get('path'), case.get('repetition')) != (label, variant, 'streaming', rep):
            issues.append(f'case {i} label/variant/path/repetition differs from schedule')
        if work != required or case.get('stage', {}).get('timeout_seconds') != 480:
            issues.append(f'{label}: workload differs from predeclared controls')
        if case.get('stage', {}).get('name') != name:
            issues.append(f'{label}: stage name mismatch')


def loaded(path, issues):
    return retro.load_json(path, issues)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def image_for(env, service):
    matches = [c for c in env.get('containers', []) if str(c.get('name','')).endswith('-'+service+'-1')]
    return matches[0].get('image_id') if len(matches) == 1 else None


def stable_hardware(env):
    """Compare sysctl results, excluding per-command observation metadata."""
    hardware = env.get('hardware') or {}
    keys = ('hw.model', 'machdep.cpu.brand_string', 'hw.physicalcpu',
            'hw.logicalcpu', 'hw.memsize')
    result = {}
    for key in keys:
        row = hardware.get(key)
        if not isinstance(row, dict) or row.get('returncode') != 0 or not isinstance(row.get('stdout'), str) or not row['stdout'].strip():
            raise ValueError(f'hardware observation unavailable: {key}')
        result[key] = row['stdout'].strip()
    return result


def check_provenance(root, campaign, cases, issues):
    if campaign.get('declaration_sha256') != digest(DECLARATION):
        issues.append('campaign declaration hash differs from predeclared workload')
    frozen = loaded(BASELINE_MANIFEST, issues)
    if campaign.get('baseline_manifest_sha256') != digest(BASELINE_MANIFEST):
        issues.append('campaign frozen baseline manifest hash differs')
    if campaign.get('smoke') is not False:
        issues.append('smoke campaign is not a measured comparison')
    if campaign.get('probe_sustained',False) is not False:
        issues.append('diagnostic sustained probe is not a full measured comparison')
    tag=campaign.get('candidate_tag','candidate1')
    if not isinstance(tag,str) or not tag or any(c not in 'abcdefghijklmnopqrstuvwxyz0123456789.-_' for c in tag):
        issues.append('candidate image tag invalid')
    for variant in ('baseline','candidate'):
        for service,component in (('stream-ingest','producer'),('stream-worker','worker'),('api','api')):
            image=(((campaign.get('images') or {}).get(variant) or {}).get(service) or {})
            expected=('incidentlens-phase5-baseline-'+component+':20261003' if variant=='baseline'
                      else 'incidentlens-hardening-'+component+':'+str(tag))
            if image.get('reference')!=expected or not image.get('id'):
                issues.append(f'{variant} {service} image reference or identity differs from declared tag')
    for field in ('binary','lag_binary'):
        if not (campaign.get(field) or {}).get('sha256'):
            issues.append(f'campaign {field} hash missing')
    if not frozen.get('archive_sha256') or not frozen.get('images'):
        issues.append('frozen baseline manifest unavailable')
    environments = []
    for case in cases:
        label = case.get('label','unknown')
        env = loaded(root/label/'environment.json', issues)
        summary = loaded(root/label/'measurement/harness/summary.json', issues)
        if summary.get('event_counts', {}).get('setup_offsets_ready:') != 1 or summary.get('config', {}).get('setup_lag_file') != str(root/label/'lag.jsonl'):
            issues.append(f'{label}: setup offset barrier completion unverified')
        environments.append(env)
        if env.get('source_manifest') != campaign.get('source_manifest') or (env.get('binary') or {}).get('sha256') != (campaign.get('binary') or {}).get('sha256'):
            issues.append(f'{label}: campaign source or binary differs from case')
        images=(campaign.get('images') or {}).get(case.get('variant')) or {}
        for service in ('stream-ingest','stream-worker','api'):
            if image_for(env,service) != (images.get(service) or {}).get('id'):
                issues.append(f'{label}: image differs from declared {case.get("variant")} image')
        volumes=case.get('retained_volumes') or {}
        if not volumes.get('before') or volumes.get('before') != volumes.get('after'):
            issues.append(f'{label}: retained volume identity missing or changed')
        initial=loaded(root/label/'initial-storage.json',issues)
        try:
            parts=json.loads(initial['clickhouse_parts']['stdout'])['data']
            if sum(int(part['physical_rows']) for part in parts)!=0:
                issues.append(f'{label}: initial ClickHouse volume was not empty')
        except (KeyError,TypeError,ValueError):
            issues.append(f'{label}: initial ClickHouse row inventory unavailable')
        if env.get('stack') != 'streaming' or not env.get('binary',{}).get('sha256') or not env.get('source_manifest',{}).get('files'):
            issues.append(f'{label}: environment/binary/source manifest missing')
        for service, limit in {'clickhouse':(2_000_000_000, 2<<30), 'api':(1_000_000_000,512<<20),
                               'collector':(1_000_000_000,256<<20), 'kafka':(2_000_000_000,1<<30),
                               'stream-ingest':(1_000_000_000,512<<20), 'stream-worker':(1_000_000_000,512<<20)}.items():
            matches=[c for c in env.get('containers',[]) if str(c.get('name','')).endswith('-'+service+'-1')]
            if len(matches)!=1 or (matches[0].get('limits',{}).get('NanoCpus'), matches[0].get('limits',{}).get('Memory')) != limit:
                issues.append(f'{label}: {service} resource quota unavailable or changed')
        if case.get('variant') == 'baseline':
            for key, service in [('producer','stream-ingest'),('worker','stream-worker'),('api','api')]:
                if image_for(env,service) != frozen.get('images',{}).get(key,{}).get('id'):
                    issues.append(f'{label}: frozen baseline {key} image differs')
    if environments:
        reference = environments[0]
        for label, env in zip((c.get('label') for c in cases), environments):
            for field in ('hardware','docker_vm','binary','source_manifest'):
                left = reference.get(field); right = env.get(field)
                if field == 'hardware':
                    try: left = stable_hardware(reference); right = stable_hardware(env)
                    except ValueError as error:
                        issues.append(f'{label}: {error}')
                        continue
                if field == 'binary':
                    left = (left or {}).get('sha256'); right = (right or {}).get('sha256')
                if field == 'source_manifest':
                    left = (left or {}).get('files'); right = (right or {}).get('files')
                if left != right: issues.append(f'{label}: {field} differs across paired campaign')
            for service in ('clickhouse','collector','kafka'):
                if image_for(env,service) != image_for(reference,service):
                    issues.append(f'{label}: {service} image differs across cases')
    return {'frozen_baseline_manifest':str(BASELINE_MANIFEST), 'frozen_baseline_archive_sha256':frozen.get('archive_sha256'),
            'declaration_sha256':campaign.get('declaration_sha256'), 'campaign_case_count':len(cases)}


def non404_fraction(case):
    numerator=denominator=notfound=0
    for route in ROUTES:
        statuses=case['queries'][route]['statuses']
        for status,count in statuses.items():
            code=int(status)
            count_value=valid_count(count)
            denominator+=count_value
            if code==404: notfound+=count_value
            elif code<200 or code>=300: numerator+=count_value
    if denominator<=0: raise ValueError('no query outcomes')
    return Fraction(numerator,denominator), Fraction(notfound,denominator), (numerator,denominator,notfound)


def query_drop_fraction(case):
    scheduled=dropped=skipped=late=0
    for route in ROUTES:
        row=case['query_schedule'][route]
        route_scheduled=valid_count(row['scheduled_measured'])
        route_dropped=valid_count(row['queue_dropped'])
        route_skipped=valid_count(row['skipped_no_acknowledged_trace'])
        route_late=valid_count(row['late_start'])
        if route_dropped+route_skipped>route_scheduled or route_late>route_scheduled-route_dropped-route_skipped:
            raise ValueError('inconsistent query schedule')
        scheduled+=route_scheduled; dropped+=route_dropped
        skipped+=route_skipped; late+=route_late
    if scheduled<=0: raise ValueError('no scheduled queries')
    return Fraction(dropped,scheduled), (dropped,scheduled,skipped,late)


def recovered(case):
    missing=case['counts']['unresolved_acked_at_original_deadline']
    valid_count(missing)
    if missing==0: return True
    supplemental=case.get('supplemental_drain') or {}
    final=(supplemental.get('observations') or [{}])[-1]
    return supplemental.get('complete') is True and final.get('snapshot_verified') is True and valid_count(final.get('acked_missing'))==0


def pair_gates(name, rep, baseline, candidate):
    label=f'{name}-r{rep}'
    b,c=baseline,candidate
    gates=[]
    admitted={'baseline':b['counts'].get('acked_measured_spans'),'candidate':c['counts'].get('acked_measured_spans')}
    if name in ('sustained-8000','burst-32000','slowdown-2000'):
        gates.append(gate(label+'/admission_at_least_95_percent', admitted,
                          lambda: frac(admitted['candidate']) >= Fraction(95,100)*frac(admitted['baseline']) and frac(admitted['baseline'])>0))
    if name=='sustained-8000':
        for quantile in ('p95_ns','p99_ns'):
            values={'baseline':b['ack_latency'].get(quantile),'candidate':c['ack_latency'].get(quantile)}
            gates.append(gate(label+'/ack_'+quantile,values,
                              lambda values=values: frac(values['candidate']) <= max(Fraction(5,4)*frac(values['baseline']),Fraction(250_000_000))))
        rows={'baseline':b['operations']['worker']['clickhouse_write']['completed_rows_per_second'],
              'candidate':c['operations']['worker']['clickhouse_write']['completed_rows_per_second']}
        gates.append(gate(label+'/completed_rows_per_second_1_25x',rows,
                          lambda: frac(rows['candidate']) >= Fraction(5,4)*frac(rows['baseline']) and frac(rows['baseline'])>0))
        drain={'baseline':b['lag']['drain'],'candidate':c['lag']['drain']}
        gates.append(gate(label+'/drain_upper_bound_half',drain,
                          lambda: drain['baseline']['status']=='observed_bounded' and drain['candidate']['status']=='observed_bounded' and
                          frac(drain['candidate']['upper_bound_ns']) <= frac(drain['baseline']['upper_bound_ns'])/2))
        samples={'baseline':(b['visibility']['censored_count'],b['visibility']['sample_count']),
                 'candidate':(c['visibility']['censored_count'],c['visibility']['sample_count'])}
        gates.append(gate(label+'/visibility_censored_fraction_half',samples,
                          lambda: censored_fraction(c) <= censored_fraction(b)/2))
    if name=='sustained-2000':
        v=c['visibility']
        observed={'censored':v.get('censored_count'),'samples':v.get('sample_count'),
                  'observed_p95_ns':(v.get('first_send_to_observation') or {}).get('p95_ns')}
        gates.append(gate(label+'/visibility_no_censor_and_p95_at_most_5s',observed,
                          lambda: frac(observed['samples'])>0 and frac(observed['censored'])==0 and
                          frac(observed['observed_p95_ns'])<=5_000_000_000))
    if name in ('burst-32000','slowdown-2000'):
        observed={'baseline':recovery_view(b),'candidate':recovery_view(c)}
        gates.append(gate(label+'/admitted_recovery_within_bound',observed,lambda: recovered(b) and recovered(c)))
    for metric, getter in [('non404_query_failure_fraction',non404_fraction),('query_schedule_drop_fraction',query_drop_fraction)]:
        bv=cv=None
        try:
            bv,braw=getter(b)[0],getter(b)[-1]
            cv,craw=getter(c)[0],getter(c)[-1]
            observed={'baseline':str(bv),'candidate':str(cv),'baseline_counts':braw,'candidate_counts':craw}
        except (KeyError,ValueError,TypeError,IndexError):
            observed={'baseline':None,'candidate':None}
        gates.append(gate(label+'/'+metric+'_increase_at_most_1_20',observed,
                          lambda bv=bv,cv=cv: fraction_increase_within(bv,cv)))
    return gates


def recovery_view(case):
    return {'original_deadline_acked_missing':case.get('counts',{}).get('unresolved_acked_at_original_deadline'),
            'supplemental_complete':(case.get('supplemental_drain') or {}).get('complete')}


def report(campaign_dir):
    campaign_dir=Path(campaign_dir)
    issues=[]
    campaign=loaded(campaign_dir/'campaign.json',issues)
    cases=campaign.get('cases',[])
    if not isinstance(cases,list): cases=[]; issues.append('campaign cases invalid')
    same_controls(cases,issues)
    provenance=check_provenance(campaign_dir,campaign,cases,issues)
    if campaign.get('status')!='completed': issues.append('campaign did not complete')
    analyses=[]
    for case in cases:
        label=case.get('label','unknown')
        if case.get('status')!='completed': issues.append(f'{label}: run status {case.get("status")}')
        try:
            exact=retro.analyze_case(campaign_dir/label,case)
        except (KeyError,ValueError,TypeError,IndexError,OSError) as error:
            issues.append(f'{label}: raw analysis failed: {type(error).__name__}: {error}')
            exact={'label':label,'path':case.get('path'),'status':'incomplete','issues':['raw analysis failed']}
        analyses.append({'variant':case.get('variant'),'repetition':case.get('repetition'),**exact})
        previous=loaded(campaign_dir/label/'analysis-exact.json',issues)
        if previous and previous!=exact: issues.append(f'{label}: saved exact analysis differs from raw replay')
        if exact.get('status')!='observed': issues.append(f'{label}: incomplete exact analysis')
    lookup={(c['label'],c['variant']):c for c in analyses}
    gates=[]
    for rep in (1,2):
        for name,*_ in SPECS:
            b=lookup.get((f'{name}-r{rep}-baseline','baseline'))
            c=lookup.get((f'{name}-r{rep}-candidate','candidate'))
            if b is None or c is None:
                gates.append({'name':f'{name}-r{rep}/paired_evidence','status':'unavailable','observed':None})
            else:
                try:
                    gates.extend(pair_gates(name,rep,b,c))
                except (KeyError,ValueError,TypeError,IndexError) as error:
                    issues.append(f'{name}-r{rep}: paired metrics incomplete: {error}')
                    gates.append({'name':f'{name}-r{rep}/paired_evidence','status':'unavailable','observed':None})
    verdict='incomplete' if issues or any(g['status']=='unavailable' for g in gates) else ('pass' if all(g['status']=='pass' for g in gates) else 'fail')
    return {'schema_version':1,'performance_verdict':verdict,'promotion_verdict':'pending_correctness_and_operational_evidence',
            'correctness_verdict':'pending_external_evidence','issues':issues,'provenance':provenance,'gates':gates,'cases':analyses,
            'method':'Exact Fraction comparisons; baseline/candidate paired by case and repetition. Producer ACK is Kafka admission, not query visibility.'}


def markdown(result):
    lines=['# Phase 5 streaming hardening comparison','',
           f"Performance verdict: **{result['performance_verdict']}**. Promotion: **{result['promotion_verdict']}**.",
           'All rates and fractions use exact rational arithmetic. Admission ACK means Kafka acceptance; completed rows are physical ClickHouse writes including replays. Polling bounds and censored visibility remain separate.','',
           '| Case | Variant | Status | Generated | Emitted | Admitted | Original unique | ACK p95/p99 ns | Completed rows/s | Peak lag records | Drain upper ns | Visibility censored/sampled | Visibility observed p95/p99 ns | Non-404 failures | 404s | Scheduled drops |',
           '| --- | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | --- | --- | --- | --- | --- |']
    for c in result['cases']:
        counts=c.get('counts') or {}; ack=c.get('ack_latency') or {}; lag=c.get('lag') or {}; vis=c.get('visibility') or {}; op=c.get('operations') or {}
        rows=(op.get('worker') or {}).get('clickhouse_write') or {}
        drain=(lag.get('drain') or {}).get('upper_bound_ns')
        try: query=non404_fraction(c)[-1]; drops=query_drop_fraction(c)[-1]
        except (KeyError,ValueError,TypeError): query=drops=None
        tail=vis.get('first_send_to_observation') or {}
        lines.append(f"| {c.get('label')} | {c.get('variant')} | {c.get('status')} | {counts.get('generated_measured_spans')} | {counts.get('emitted_measured_spans')} | {counts.get('acked_measured_spans')} | {counts.get('stored_unique_at_original_snapshot_spans')} | {ack.get('p95_ns')}/{ack.get('p99_ns')} | {rows.get('completed_rows_per_second')} | {lag.get('max_observed_lag_records')} | {drain} | {vis.get('censored_count')}/{vis.get('sample_count')} | {tail.get('p95_ns')}/{tail.get('p99_ns')} | {query[:2] if query else None} | {query[2] if query else None} | {drops[:2] if drops else None} |")
    lines+=['','| Case | Route | Status counts | Successful p95/p99 ns | Scheduled | Issued | Queue drops | Skipped (no ACK) | Late starts |',
            '| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |']
    for c in result['cases']:
        for route in ROUTES:
            query=(c.get('queries') or {}).get(route) or {}
            schedule=(c.get('query_schedule') or {}).get(route) or {}
            lines.append(f"| {c.get('label')} | {route} | {json.dumps(query.get('statuses'),sort_keys=True)} | {query.get('successful_p95_ns')}/{query.get('successful_p99_ns')} | {schedule.get('scheduled_measured')} | {schedule.get('issued_measured')} | {schedule.get('queue_dropped')} | {schedule.get('skipped_no_acknowledged_trace')} | {schedule.get('late_start')} |")
    lines+=['','## Predeclared performance gates','']
    for gate_row in result['gates']:
        lines.append(f"- {gate_row['status']}: {gate_row['name']} — {gate_row['observed']}")
    if result['issues']:
        lines+=['','## Evidence gaps','']+[f'- {issue}' for issue in result['issues']]
    lines+=['','The full JSON retains per-route statuses and p50/p95/p99, scheduling drops/skips/late starts, conditional visibility tails, write/commit phase timings, per-second producer/consumer activity, resource sample counts, broker lag samples and finite identity ledgers. Correctness/UI/startup/rollback evidence is assessed separately.','']
    return '\n'.join(lines)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaign',type=Path)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    result=report(args.campaign)
    args.output.mkdir(parents=True,exist_ok=True)
    (args.output/'analysis.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
    (args.output/'tables.md').write_text(markdown(result))
    return 0 if result['performance_verdict']=='pass' else 1


if __name__=='__main__': raise SystemExit(main())
