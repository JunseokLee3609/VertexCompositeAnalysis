from pathlib import Path
import json,subprocess,time,re
b=Path('/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113')
ready=json.loads((b/'configuration/production_ready.json').read_text())
assert ready['status']=='ready' and ready['input_files']==ready['reference_files']==5114
p=b/'configuration/submission.json'
state=json.loads(p.read_text()) if p.exists() else dict(status='submitting',test_submissions=0,tasks=[])
tasks=json.loads((b/'configuration/tasks.json').read_text())
jobs=json.loads((b/'configuration/all_jobs.json').read_text())
refs=json.loads((b/'configuration/an_reference_manifest.json').read_text())
by={r['input_lfn']:r for r in refs}
groups={t['group']:[] for t in tasks}
for j in jobs:groups[j['group']].append(by[j['miniaod_lfn']])
eos=Path('/eos/user/j/junseok/GenQ/Reference_20261113/an')
accepted={r['group'] for r in state['tasks'] if r['status']=='submitted'}
while len(accepted)<len(tasks):
 progress={}
 for t in tasks:
  if t['group'] in accepted:continue
  rows=groups[t['group']]
  count=0
  for ref in rows:
   try:ok=(eos/(str(ref['file_index'])+'.root')).stat().st_size==ref['bytes']
   except FileNotFoundError:ok=False
   if ok:count+=1
  progress[t['group']]=dict(ready=count,total=len(rows))
  if count!=len(rows):continue
  log=b/'configuration'/('submit_'+t['group']+'.log')
  start=time.time()
  print('SUBMITTING_COMPLETE_SAMPLE',t['group'],len(rows),flush=True)
  with log.open('w') as f:r=subprocess.run(['crab','submit','-c',t['config']],stdout=f,stderr=subprocess.STDOUT)
  output=log.read_text(errors='replace')
  row=dict(group=t['group'],files=t['files'],request_name=t['request_name'],project=t['project'],returncode=r.returncode,seconds=time.time()-start,log=str(log))
  match=re.search(r'Task name:\s*(\S+)',output)
  if match:row['task_name']=match.group(1)
  row['status']='submitted' if r.returncode==0 else 'failed'
  state['tasks'].append(row);state['status']='submitting' if r.returncode==0 else 'failed'
  p.write_text(json.dumps(state,indent=2)+'\n')
  print('CRAB_SUBMISSION',json.dumps(row),flush=True)
  if r.returncode:raise SystemExit(r.returncode)
  accepted.add(t['group'])
 (b/'configuration/reference_progress.json').write_text(json.dumps(progress,indent=2)+'\n')
 if len(accepted)<len(tasks):
  print('WAITING_REMAINING_REFERENCES',json.dumps(progress),flush=True)
  time.sleep(30)
state['status']='submitted';state['files']=sum(t['files'] for t in tasks);state['time']=time.time()
p.write_text(json.dumps(state,indent=2)+'\n')
print('ALL_SIX_CRAB_TASKS_SUBMITTED',state['files'],flush=True)
