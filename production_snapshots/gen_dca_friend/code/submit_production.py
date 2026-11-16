from pathlib import Path
import os,json,time,traceback
P=Path('/afs/cern.ch/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117');E=Path('/eos/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117');os.chdir(E/'logs/crab')
from CRABAPI.RawCommand import crabCommand
ready=json.loads((P/'configuration/production_ready.json').read_text());assert ready['status']=='ready'and ready['inputs']==5114 and ready['tag']['pushed']
statefile=P/'configuration/submission.json';state=json.loads(statefile.read_text())if statefile.exists()else dict(status='submitting',tasks=[])
accepted={r['group']for r in state['tasks']if r['status']=='submitted'}
for t in json.loads((P/'configuration/tasks.json').read_text()):
 if t['group']in accepted:continue
 print('SUBMITTING',t['group'],t['files'],flush=True);begin=time.time()
 try:
  result=crabCommand('submit',config=t['configuration']);unique=result.get('uniquerequestname');assert unique,('No CRAB task receipt',result)
  row=dict(t,status='submitted',task_name=unique,receipt=result,seconds=time.time()-begin)
 except BaseException as error:
  row=dict(t,status='failed',error=str(error),seconds=time.time()-begin);state['tasks'].append(row);state['status']='failed';statefile.write_text(json.dumps(state,indent=2,default=str)+'\n');raise
 state['tasks'].append(row);statefile.write_text(json.dumps(state,indent=2,default=str)+'\n');print('ACCEPTED',json.dumps(row,default=str),flush=True)
state.update(status='submitted',files=5114);statefile.write_text(json.dumps(state,indent=2,default=str)+'\n');print('ALL_SIX_CRAB_TASKS_SUBMITTED',flush=True)
