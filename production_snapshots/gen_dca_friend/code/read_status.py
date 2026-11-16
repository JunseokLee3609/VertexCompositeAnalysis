from pathlib import Path
import os,json,collections
P=Path('/afs/cern.ch/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117');E=Path('/eos/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117');os.chdir(E/'logs/crab')
from CRABAPI.RawCommand import crabCommand
rows=[]
for t in json.loads((P/'configuration/submission.json').read_text())['tasks']:
 if t['status']!='submitted':continue
 try:
  s=crabCommand('status',dir=t['project']);row=dict(group=t['group'],task=t['task_name'],status=s.get('status'),jobs_per_status=s.get('jobsPerStatus'),db_status=s.get('dbStatus'),raw=s)
 except Exception as error:row=dict(group=t['group'],query_error=str(error))
 rows.append(row);(E/'logs/status.json').write_text(json.dumps(rows,indent=2,default=str)+'\n')
print(json.dumps([{k:v for k,v in x.items()if k!='raw'}for x in rows],indent=2,default=str))
