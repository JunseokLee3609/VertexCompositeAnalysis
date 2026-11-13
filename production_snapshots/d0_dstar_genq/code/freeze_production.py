import json,os,subprocess,hashlib,shutil,time,runpy,sys
from pathlib import Path
b=Path('/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113')
v=json.loads((b/'local_validation/final/result.json').read_text())
assert v['status']=='passed' and len(v['cases'])==15
r=json.loads((b/'local_validation/final_wrapper/readback.json').read_text())
assert r['status']=='passed' and not r['partial']
jobs=json.loads((b/'configuration/all_jobs.json').read_text())
refs=json.loads((b/'configuration/an_reference_manifest.json').read_text())
assert len(jobs)==len(refs)==5114
by={r['input_lfn']:r for r in refs}
eos=Path('/eos/user/j/junseok/GenQ/Reference_20261113/an')
for j in jobs:
 ref=by[j['miniaod_lfn']]
 assert ref['bytes']>0
# Each task is gated on its own complete reference set immediately before submission.
# Freeze the wrapper that passed the full-file worker-style check.
shutil.copyfile(b/'code/run_crab_staged.sh',b/'code/run_crab.sh')
(b/'code/run_crab.sh').chmod(0o755)
tasks=json.loads((b/'configuration/tasks.json').read_text())
for t in tasks:
 p=Path(t['config']);s=p.read_text()
 marker="config.JobType.inputFiles="
 i=s.index(marker);end=s.index('\n',i)
 paths=[str(b/'configuration'/('mapping_'+t['group']+'.json')),str(b/'code/stage_reference.py'),str(b/'code/finalize_output.py'),str(b/'credentials/an_reference.token')]
 s=s[:i]+marker+repr(paths)+s[end:];p.write_text(s)
 cfg=runpy.run_path(str(p))['config']
 assert cfg.Data.unitsPerJob==1 and len(cfg.Data.userInputFiles)==t['files']
 assert cfg.Site.storageSite=='T3_CH_CERNBOX' and not cfg.JobType.pyCfgParams[-1].endswith('False')
 (b/'crab_projects').mkdir(exist_ok=True)
snapshot=b/'production_snapshot';snapshot.mkdir(exist_ok=True)
for sub in ['code','configuration']:
 dst=snapshot/sub;dst.mkdir(exist_ok=True)
 for p in (b/sub).iterdir():
  if p.is_file() and p.suffix in ['.cc','.h','.xml','.py','.sh','.json'] and p.name!='run_crab_staged.sh':
   shutil.copyfile(p,dst/p.name)
for name,src in [('local_validation.json',b/'local_validation/final/result.json'),('wrapper_validation.json',b/'local_validation/final_wrapper/readback.json')]:
 shutil.copyfile(src,snapshot/name)
files=list(snapshot.rglob('*'))
fingerprint={str(p.relative_to(snapshot)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files if p.is_file()}
fingerprint['compiled_plugin']=hashlib.sha256((b/'cmssw/CMSSW_13_2_11/lib/el8_amd64_gcc11/pluginD0DstarGenQPlugins.so').read_bytes()).hexdigest()
(snapshot/'fingerprint.json').write_text(json.dumps(fingerprint,indent=2)+'\n')
receipt=dict(status='ready',local_cases=len(v['cases']),worker_wrapper='passed',input_files=len(jobs),reference_files=len(refs),reference_bytes=sum(r['bytes'] for r in refs),production_test_submissions=0,prepared=time.time())
(b/'configuration/production_ready.json').write_text(json.dumps(receipt,indent=2)+'\n')
print('PRODUCTION_READY',json.dumps(receipt),flush=True)
