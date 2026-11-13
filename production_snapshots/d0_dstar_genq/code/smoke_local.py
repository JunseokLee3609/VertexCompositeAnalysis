import json,subprocess
from pathlib import Path
b=Path('/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113')
jobs=json.loads((b/'configuration/all_jobs.json').read_text())
row=next(j for j in jobs if j['group']=='OfficialPromptPthat2Pt4' and j['original_job']==1)
p=b/'local_validation/smoke';p.mkdir(exist_ok=True)
cmd=['cmsRun','-j','FrameworkJobReport.xml',str(b/'code/genq_cfg.py'),'inputFiles=root://cms-xrd-global.cern.ch/'+row['miniaod_lfn'],'manifest='+str(b/'configuration/mapping_OfficialPromptPthat2Pt4.json'),'maxEvents=200','allowPartial=True']
(p/'command.json').write_text(json.dumps(cmd,indent=2)+'\n')
with (p/'cmsRun.log').open('w') as log:r=subprocess.run(cmd,cwd=p,stdout=log,stderr=subprocess.STDOUT)
print('SMOKE_RETURN',r.returncode)
if r.returncode:print((p/'cmsRun.log').read_text()[-12000:]);raise SystemExit(r.returncode)
r=subprocess.run(['python3',str(b/'code/validate_local.py'),str(p)])
raise SystemExit(r.returncode)
