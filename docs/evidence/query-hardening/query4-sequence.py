"""Run the declared query4 smoke/probe/full sequence, stopping at any failed gate."""
import json, os, pathlib, subprocess, sys

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT/'benchmarks'))
import environment, analyze_stream_hardening as gates

EVIDENCE = ROOT/'docs/evidence/query-hardening'
STATE = EVIDENCE/'query4-campaign-sequence.json'
FREEZE = json.loads((EVIDENCE/'query4-freeze.json').read_text())
assert not STATE.exists(), 'preserve an existing sequence; inspect it before any retry'
state = dict(pid=os.getpid(), started_at=environment.utc(), status='running', stages=[])
def save():
    tmp = STATE.with_suffix('.tmp')
    tmp.write_text(json.dumps(state, indent=2)+'\n')
    tmp.replace(STATE)
def stage(name, option):
    assert environment.manifest()==FREEZE['source_manifest'], 'source changed since freeze'
    output = ROOT/('benchmarks/results/20261012-candidate3-query4-'+name)
    command = [sys.executable, '-u', str(ROOT/'benchmarks/stream_hardening.py'),
        '--output', str(output), '--binary', FREEZE['binary']['path'],
        '--lag-binary', str(pathlib.Path.home()/'Library/Caches/IncidentLens/bin/bench-lag'),
        '--candidate-tag', 'candidate3-query4', '--candidate-query-index']+option
    entry = dict(name=name, command=command, output=str(output), started_at=environment.utc())
    state['stages'].append(entry)
    save()
    with (EVIDENCE/('query4-'+name+'-runner.txt')).open('x') as log:
        process = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        entry['pid'] = process.pid
        save()
        entry['returncode'] = process.wait()
    entry['ended_at'] = environment.utc()
    entry['status'] = 'completed' if entry['returncode']==0 else 'failed'
    save()
    assert entry['returncode']==0, name+' runner failed'
    campaign = json.loads((output/'campaign.json').read_text())
    assert campaign['status']=='completed', name+' campaign not completed'
    assert campaign['source_manifest']==FREEZE['source_manifest'], 'campaign source drift'
    assert campaign['candidate_query_index'] is True
    for component, service in [('producer','stream-ingest'),('worker','stream-worker'),('api','api')]:
        assert campaign['images']['candidate'][service]['id']==FREEZE['images'][component]['id']
    return output, campaign

try:
    save()
    output, campaign = stage('smoke', ['--smoke'])
    assert len(campaign['cases'])==2
    for case in campaign['cases']:
        row=json.loads((output/case['label']/'analysis-exact.json').read_text())
        assert row['status']=='observed' and not row['issues'] and gates.recovered(row)
        assert row['counts']['acked_full_run_spans']==888
    output, campaign = stage('probe', ['--probe-sustained'])
    summary=EVIDENCE/'query4-probe-summary.json'
    subprocess.run([sys.executable, str(EVIDENCE/'summarize-probe.py'), str(output), str(summary)], check=True, cwd=ROOT)
    observed=json.loads(summary.read_text())
    assert all(case['recovered'] for case in observed['cases'].values())
    assert len(observed['gates'])==8
    failed=[g['name'] for g in observed['gates'] if g['status']!='pass']
    if failed:
        state.update(status='diagnostic_failed', failed_gates=failed)
    else:
        output, campaign=stage('full', [])
        result=gates.report(output)
        (EVIDENCE/'query4-full-analysis.json').write_text(json.dumps(result, indent=2)+'\n')
        state.update(status='performance_'+result['performance_verdict'])
except Exception as error:
    state.update(status='failed', error=repr(error))
finally:
    state['ended_at']=environment.utc()
    save()
    print(json.dumps(state))
