from pathlib import Path
import subprocess,json,datetime
r=Path('/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_official_crab_rematching_20261112')
tasks=json.loads((r/'submission_v3/tasks.json').read_text())
records=[]
for t in tasks:
 p=subprocess.run(['crab','status','-d',t['project']],text=True,capture_output=True,stdin=subprocess.DEVNULL)
 (r/'submission_v3'/(t['group']+'_status.log')).write_text(p.stdout+p.stderr)
 lines=[s for s in (p.stdout+p.stderr).splitlines() if any(k in s for k in ['Task name:','Status on the CRAB server:','Status on the scheduler:','Jobs status:','finished','running','idle','held','failed','SUBMITTED','SUBMITFAILED','QUEUED','NEW','Error'])]
 rec=dict(group=t['group'],task=t['task'],input_files=t['input_files'],returncode=p.returncode,lines=lines)
 records.append(rec)
 print(json.dumps(rec),flush=True)
(r/'submission/status.json').write_text(json.dumps(dict(time_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),records=records),indent=2)+'\n')
