from pathlib import Path
import json,datetime
r=Path(__file__).resolve().parent.parent
failed=json.loads((r/'recovery_v4/failed_inputs.json').read_text())
classification=json.loads((r/'recovery_v4/failure_classification.json').read_text())
assert not failed['errors'] and not classification['unresolved']
assert all(x['failure_text'] for x in failed['rows'])
base0=json.loads((r/'configuration_v2/scope.json').read_text())
target=r/'configuration_v4recovery';target.mkdir(exist_ok=True)
base='/store/user/junseok/D0Rematching/Official6_PythiaGEN_CRAB_20261112_v4recovery'
groups={};requests={}
for group in sorted({x['group']for x in failed['rows']}):
 rows=[x for x in failed['rows']if x['group']==group]
 mapping=target/('mapping_'+group+'.json');mapping.write_text(json.dumps(rows,indent=2)+'\n')
 request='D0Friend_PythiaGEN_12Nov26_'+group+'_v4recovery'
 groups[group]=len(rows);requests[group]=request
 replacements={
 'config.General.requestName':request,
 'config.General.workArea':str(r/'crab_projects_v4recovery'),
 'config.JobType.psetName':str(r/'code_v4/friend_cfg.py'),
 'config.JobType.pyCfgParams':['manifest='+mapping.name,'signalOnlyGenMatching=True'],
 'config.JobType.inputFiles':[str(mapping)],
 'config.JobType.scriptExe':str(r/'code_v4/run_crab.sh'),
 'config.Data.userInputFiles':[x['miniaod_lfn']for x in rows],
 'config.Data.outputDatasetTag':request,
 'config.Data.outLFNDirBase':base+'/'+group}
 old=(r/'configuration_v2'/('crab_'+group+'.py')).read_text().splitlines()
 lines=[]
 for line in old:
  key=line.split('=',1)[0].strip()
  lines.append(key+'='+repr(replacements[key]) if key in replacements else line)
 (target/('crab_'+group+'.py')).write_text('\n'.join(lines)+'\n')
native=json.loads((r/'native_commits.json').read_text())
scope=dict(total=sum(groups.values()),groups=groups,requests=requests,output_base=base,datasets=base0['datasets'],matching_reference_commit=base0['matching_reference_commit'],native_gen_commits={k:native[k]for k in ['an','refit-diag']},signalOnlyGenMatching=True,identity_policy='Exact charge, float32 pT error and hit count. Collapse identical charge/pt/eta/phi matching inputs. Use both DCA significances within two float32 ULP only when distinct momentum choices remain. Require one distinct momentum.',failed_inputs_only=True,source_v3_tasks_preserved=True,failure_classification=classification,created_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
assert scope['total']==failed['total']==74
(target/'scope.json').write_text(json.dumps(scope,indent=2)+'\n')
print(json.dumps({k:v for k,v in scope.items()if k in ['total','groups','requests','output_base']}),flush=True)
