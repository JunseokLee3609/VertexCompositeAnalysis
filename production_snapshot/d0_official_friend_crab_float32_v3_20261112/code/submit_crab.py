from pathlib import Path
import subprocess,json,datetime,re
root=Path('/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_official_crab_rematching_20261112')
receipts=root/'submission_v3'
receipts.mkdir(exist_ok=True)
scope=json.loads((root/'configuration_v3/scope.json').read_text())
for group,n in scope['groups'].items():
 target=receipts/(group+'.json')
 if target.exists():
  record=json.loads(target.read_text())
  if record.get('returncode')==0 and record.get('task'):
   print('PRESERVED_SUBMISSION',record['task'],flush=True)
   continue
 request=scope['requests'][group]
 project=root/'crab_projects_v3'/('crab_'+request)
 if (project/'.requestcache').exists():
  raise RuntimeError('Existing request cache requires inspection: '+str(project))
 command=['crab','submit','-c',str(root/'configuration_v3'/('crab_'+group+'.py'))]
 print('SUBMITTING',group,n,flush=True)
 log=receipts/(group+'.log')
 with log.open('w') as output:
  p=subprocess.run(command,stdout=output,stderr=subprocess.STDOUT,stdin=subprocess.DEVNULL,text=True)
 output=log.read_text()
 match=re.search(r'Task name:\s*(\S+)',output)
 task=match.group(1) if match else None
 record=dict(group=group,input_files=n,command=command,returncode=p.returncode,task=task,project=str(project),signalOnlyGenMatching=True,time_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),log=str(log))
 target.write_text(json.dumps(record,indent=2)+'\n')
 print('RECEIPT',json.dumps(record),flush=True)
 if p.returncode or not task:
  print(output[-7000:],flush=True)
  raise RuntimeError('Submission requires correction: '+group)
all_records=[json.loads((receipts/(g+'.json')).read_text())for g in scope['groups']]
(receipts/'tasks.json').write_text(json.dumps(all_records,indent=2)+'\n')
print('ALL_SUBMITTED',len(all_records),sum(x['input_files']for x in all_records),flush=True)
