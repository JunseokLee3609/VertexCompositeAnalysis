import json,subprocess,sys
from pathlib import Path
b=Path('/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113')
state=json.loads((b/'configuration/submission.json').read_text())
rows=[]
for t in state['tasks']:
 if t['status']!='submitted':continue
 out=b/'configuration'/('status_'+t['group']+'.json')
 with (b/'configuration'/('status_'+t['group']+'.log')).open('w') as log:
  r=subprocess.run([sys.executable,str(b/'code/status_one.py'),t['project'],str(out)],stdout=log,stderr=subprocess.STDOUT)
 row=dict(group=t['group'],files=t['files'],query_returncode=r.returncode)
 if out.exists():row.update(json.loads(out.read_text()))
 rows.append(row)
(b/'configuration/status.json').write_text(json.dumps(rows,indent=2)+'\n')
print('CRAB_STATUS_SUMMARY',json.dumps(rows),flush=True)
