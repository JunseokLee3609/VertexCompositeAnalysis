from pathlib import Path
import json,subprocess,os,argparse
p=argparse.ArgumentParser();p.add_argument('--submit',action='store_true');p.add_argument('--replace',action='store_true');o=p.parse_args()
R=Path('/home/jun502s/DstarAna/DStarAnalysis/Fit/PbPb/Test/D0mesonspintest');K=R/'output/campaign_26Nov12_d0_trackmatched_mc/study/gen_dca_friend_20261117';P=R/'condor/d0_gen_dca_friend_26Nov17'
a=json.loads((K/'application_manifest.json').read_text());assert a['complete']and a['ready']==5114 and len(a['rows'])==5114
for j in a['rows']:
 f=K/'supplements'/f'{j["manifest_index"]:05d}.root'
 if not f.exists()or f.stat().st_size!=j['supplement_bytes']:raise ValueError(('Missing supplement',j['manifest_index']))
 if not Path(j['target_ntuple']).is_file():raise ValueError(('Missing target ntuple',j['target_ntuple']))
P.mkdir(exist_ok=True);(P/'logs').mkdir(exist_ok=True);wrapper=P/'worker.sh';wrapper.write_text('#!/bin/bash\nset -euo pipefail\nsource /software/ROOT/ROOT-v6.36/root-6.36-install/bin/thisroot.sh\npython3 -B '+str(K/'code/apply_one.py')+' "$1" '+('replace'if o.replace else'align')+'\n');wrapper.chmod(0o755)
sub=P/'submit.sub';sub.write_text(f'''universe = vanilla
executable = {wrapper}
arguments = $(Process)
should_transfer_files = NO
getenv = True
request_cpus = 1
request_memory = 2500MB
notification = Never
on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)
+JobBatchName = "D0Dstar_GenDca_Friend_Apply"
output = {P}/logs/$(Process).out
error = {P}/logs/$(Process).err
log = {P}/logs/events.log
queue 5114
''')
if o.submit:
 r=subprocess.run(['/software/condor/common/bin/condor_submit',str(sub)],env={**os.environ,'CONDOR_CONFIG':'/software/condor/kunpl/etc/condor_config'},capture_output=True,text=True);(P/'submission.json').write_text(json.dumps(dict(replace=o.replace,stdout=r.stdout,stderr=r.stderr,returncode=r.returncode),indent=2)+'\n');print(r.stdout,r.stderr);r.check_returncode()
else:print('Prepared 5114 jobs; replacements='+str(o.replace)+'; submit file '+str(sub))
