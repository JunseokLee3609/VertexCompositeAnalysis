import json,subprocess,concurrent.futures,time,os,sys
from pathlib import Path
b=Path('/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113')
v=b/'local_validation/final';v.mkdir(exist_ok=True)
jobs=json.loads((b/'configuration/all_jobs.json').read_text())
target={'OfficialNonpromptPthat10Pt20':507,'OfficialNonpromptPthat2Pt4':99,'OfficialNonpromptPthat5Pt10':704,'OfficialPromptPthat10Pt20':1,'OfficialPromptPthat2Pt4':309,'OfficialPromptPthat5Pt10':290}
cases=[]
for group,job in target.items():
 row=next(r for r in jobs if r['group']==group and r['original_job']==job)
 cases.append(dict(name=group,row=row,partial=False,event_ranges=[]))
row=next(r for r in jobs if r['group']=='OfficialPromptPthat2Pt4' and r['original_job']==1)
cases.append(dict(name='ANClusterEdgeFull',row=row,partial=False,event_ranges=[]))
edgefile=b/'local_validation/edge_cases.json'
if edgefile.exists():cases+=json.loads(edgefile.read_text())
(v/'cases.json').write_text(json.dumps(cases,indent=2)+'\n')
def one(case):
 p=v/case['name'];p.mkdir(exist_ok=True)
 row=case['row'];mapping=b/'configuration'/('mapping_'+row['group']+'.json')
 (p/mapping.name).write_text(mapping.read_text())
 (p/'genq_cfg.py').write_text((b/'code/genq_cfg.py').read_text())
 cfg='genq_cfg.py'
 if case['event_ranges']:
  cfg='edge_cfg.py'
  (p/cfg).write_text('import FWCore.ParameterSet.Config as cms\nfrom genq_cfg import process\nprocess.source.eventsToProcess=cms.untracked.VEventRange('+repr(case['event_ranges'])+')\n')
 opts=['inputFiles=root://cms-xrd-global.cern.ch/'+row['miniaod_lfn'],'manifest='+mapping.name,'maxEvents=-1','allowPartial='+str(case['partial'])]
 start=time.time();receipt=dict(case=case['name'],miniaod_lfn=row['miniaod_lfn'],partial=case['partial'],started=start)
 with (p/'config.log').open('w') as err,(p/'PSet.py').open('w') as out:
  r=subprocess.run(['python3','-c',"import runpy,sys; cfg=sys.argv[1]; sys.argv=[cfg]+sys.argv[2:]; ns=runpy.run_path(cfg); print(ns['process'].dumpPython())",cfg]+opts,cwd=p,stdout=out,stderr=err)
 if r.returncode:receipt.update(status='failed',stage='serialize_PSet',returncode=r.returncode)
 else:
  # Full-file checks use exactly the production scriptExe and serialized PSet.
  cmd=['bash',str(b/'code/run_crab.sh')] if not case['partial'] else ['cmsRun','-j','FrameworkJobReport.xml','PSet.py']
  (p/'command.json').write_text(json.dumps(cmd,indent=2)+'\n')
  with (p/'cmsRun.log').open('w') as log:r=subprocess.run(cmd,cwd=p,stdout=log,stderr=subprocess.STDOUT)
  if r.returncode:receipt.update(status='failed',stage='cmsRun',returncode=r.returncode)
  else:
   with (p/'readback.log').open('w') as log:r=subprocess.run(['python3',str(b/'code/validate_local.py'),str(p)],cwd=p,stdout=log,stderr=subprocess.STDOUT)
   receipt.update(status='passed' if r.returncode==0 else 'failed',stage='readback',returncode=r.returncode)
 receipt['seconds']=time.time()-start
 (p/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
 print(json.dumps(receipt),flush=True)
 return receipt
rows=[]
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
 for r in pool.map(one,cases):rows.append(r)
result=dict(status='passed' if all(r['status']=='passed' for r in rows) else 'failed',cases=rows,crab_test_submissions=0)
(v/'result.json').write_text(json.dumps(result,indent=2)+'\n')
print('LOCAL_VALIDATION',result['status'],len(rows),flush=True)
sys.exit(0 if result['status']=='passed' else 1)

