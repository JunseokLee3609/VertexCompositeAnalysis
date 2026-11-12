from pathlib import Path
import json,datetime
r=Path(__file__).resolve().parent.parent
base0=json.loads((r/'configuration_v2/scope.json').read_text())
pilot=json.loads((r/'configuration_v3pilot/scope.json').read_text())
all_jobs=json.loads((r/'configuration/all_jobs.json').read_text())
pilot_lfns={x['miniaod_lfn']for rows in pilot['selection'].values()for x in rows}
assert len(pilot_lfns)==12
target=r/'configuration_v3';target.mkdir(exist_ok=True)
base='/store/user/junseok/D0Rematching/Official6_PythiaGEN_CRAB_20261112_v3/production'
groups={};requests={}
for group in sorted(base0['groups']):
 rows=[x for x in all_jobs if x['group']==group and x['miniaod_lfn'] not in pilot_lfns]
 mapping=target/('mapping_'+group+'.json');mapping.write_text(json.dumps(rows,indent=2)+'\n')
 request='D0Friend_PythiaGEN_12Nov26_'+group+'_v3'
 groups[group]=len(rows);requests[group]=request
 replacements={
 'config.General.requestName':request,
 'config.General.workArea':str(r/'crab_projects_v3'),
 'config.JobType.psetName':str(r/'code_v3/friend_cfg.py'),
 'config.JobType.pyCfgParams':['manifest='+mapping.name,'signalOnlyGenMatching=True'],
 'config.JobType.inputFiles':[str(mapping)],
 'config.JobType.scriptExe':str(r/'code_v3/run_crab.sh'),
 'config.Data.userInputFiles':[x['miniaod_lfn']for x in rows],
 'config.Data.outputDatasetTag':request,
 'config.Data.outLFNDirBase':base+'/'+group}
 old=(r/'configuration_v2'/('crab_'+group+'.py')).read_text().splitlines()
 lines=[]
 for line in old:
  key=line.split('=',1)[0].strip()
  lines.append(key+'='+repr(replacements[key]) if key in replacements else line)
 (target/('crab_'+group+'.py')).write_text('\n'.join(lines)+'\n')
scope=dict(total=sum(groups.values()),groups=groups,requests=requests,output_base=base,datasets=base0['datasets'],matching_reference_commit=base0['matching_reference_commit'],native_gen_commits={k:v for k,v in json.loads((r/'native_commits.json').read_text()).items() if k in ['an','refit-diag']},signalOnlyGenMatching=True,identity_policy='exact pT error bits, charge, hit count and both DCA significances within 2 float32 ULP; require unique identity',earlier_v3_files=12,combined_files=len(all_jobs),pilot_result_required_before_submission=False,user_instruction='Submit remaining files immediately without waiting for Grid pilot results',local_validation=json.loads((r/'validation_v3/local_reproduction_progress.json').read_text()),created_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
assert scope['total']==5102 and scope['combined_files']==5114
(target/'scope.json').write_text(json.dumps(scope,indent=2)+'\n')
print(json.dumps({k:v for k,v in scope.items()if k in ['total','groups','requests','earlier_v3_files','combined_files','output_base']}),flush=True)
