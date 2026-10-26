"""Run the predeclared query3 smoke/probe, then full campaign only after exact gates pass."""
import datetime,json,os,pathlib,subprocess,sys
ROOT=pathlib.Path(__file__).resolve().parents[3]
EVIDENCE=ROOT/'docs/evidence/query-hardening'
sys.path.insert(0,str(ROOT/'benchmarks'))
import environment
import analyze_stream_hardening as analysis
freeze=json.loads((EVIDENCE/'query3-freeze.json').read_text())
state={'pid':os.getpid(),'started_at':environment.utc(),'status':'running','stages':[]}
STATE=EVIDENCE/'query3-campaign-sequence.json'
def save():STATE.write_text(json.dumps(state,indent=2)+'\n')
def run(name,flags):
    assert environment.manifest()==freeze['source_manifest'],'source drift'
    output=ROOT/('benchmarks/results/20261011-candidate3-query3-'+name)
    assert not output.exists(),str(output)+' already exists; no automatic overwrite/resume'
    cmd=[sys.executable,'-u',str(ROOT/'benchmarks/stream_hardening.py'),'--output',str(output),'--binary','/Users/kousil/Library/Caches/IncidentLens/bin/bench-query-diagnostics','--lag-binary','/Users/kousil/Library/Caches/IncidentLens/bin/bench-lag','--candidate-tag','candidate3-query3',*flags]
    entry={'name':name,'command':cmd,'output':str(output),'started_at':environment.utc()};state['stages'].append(entry);save()
    with (EVIDENCE/('query3-'+name+'-launch.txt')).open('x') as log:
        child=subprocess.Popen(cmd,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
        entry['pid']=child.pid;save();entry['returncode']=child.wait()
    entry['ended_at']=environment.utc();save()
    campaign=json.loads((output/'campaign.json').read_text())
    assert entry['returncode']==0 and campaign['status']=='completed','campaign did not complete: '+name+' '+str(campaign.get('error'))
    entry['status']='completed';save()
    return output,campaign
try:
    save()
    root,smoke=run('smoke',['--smoke'])
    assert len(smoke['cases'])==2
    for case in smoke['cases']:
        row=json.loads((root/case['label']/'analysis-exact.json').read_text())
        assert row['status']=='observed' and row['counts']['acked_full_run_spans']==888 and row['counts']['stored_unique_final_spans']==888
    root,probe=run('probe',['--probe-sustained'])
    assert len(probe['cases'])==2
    rows=[json.loads((root/('sustained-8000-r1-'+v)/'analysis-exact.json').read_text()) for v in ['baseline','candidate']]
    issues=[];analysis.check_provenance(root,probe,probe['cases'],issues)
    assert issues==['diagnostic sustained probe is not a full measured comparison'],issues
    assert all(r['status']=='observed' and not r['issues'] and analysis.recovered(r) for r in rows)
    gates=analysis.pair_gates('sustained-8000',1,*rows)
    (EVIDENCE/'query3-probe-gates.json').write_text(json.dumps({'scope':'diagnostic only; no default promotion','gates':gates},indent=2)+'\n')
    if not all(g['status']=='pass' for g in gates):
        state.update(status='diagnostic_failed',failed_gates=[g['name'] for g in gates if g['status']!='pass'])
    else:
        root,full=run('full',[])
        state.update(status='campaigns_completed',full_campaign=str(root),note='Full analysis, operational acceptance and promotion audit still required.')
except BaseException as error:
    state.update(status='stopped',error=repr(error))
finally:
    state['ended_at']=environment.utc();save();print(json.dumps(state))
