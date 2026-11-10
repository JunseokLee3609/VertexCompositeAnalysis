from pathlib import Path
import subprocess,json,time,collections
S=Path(__file__).resolve().parents[1];CLUSTERS={13149508,13149509,13149510};start=time.monotonic();last=None
CACHE=S/'configuration/spooled_log_sources.json';sources=json.loads(CACHE.read_text()) if CACHE.exists() else {}
(S/'results/job_metadata').mkdir(exist_ok=True)
def retrieve(cluster,jobs):
 global sources
 if not jobs:return
 key=str(cluster)
 if cluster not in {13149508,13149509,13149510}:
  condition='ClusterId == '+key+' && JobStatus == 4 && ('+' || '.join('ProcId == '+str(x['ProcId']) for x in jobs)+')'
  r=subprocess.run(['condor_transfer_data','-constraint',condition],capture_output=True,text=True)
  print('RETRIEVE_RETRY_LOGS',cluster,len(jobs),r.returncode,r.stderr[-1000:],flush=True)
  if r.returncode==0:
   r=subprocess.run(['condor_rm','-constraint',condition],capture_output=True,text=True)
   print('RETIRE_COMPLETED_RETRIES',cluster,len(jobs),r.returncode,r.stderr[-500:],flush=True)
  return
 if key not in sources:
  normal=next((x for x in jobs if '_condor_stdout=' in x.get('TransferOutputRemaps','')),None)
  if normal is None:print('LOG_SOURCE_PENDING',cluster,flush=True);return
  pairs=dict(x.split('=',1) for x in normal['TransferOutputRemaps'].split(';'));sources[key]=dict(out=pairs['_condor_stdout'],err=pairs['_condor_stderr']);CACHE.write_text(json.dumps(sources,indent=2)+'\n')
 src=sources[key];condition='ClusterId == '+key+' && JobStatus == 4 && ('+' || '.join('ProcId == '+str(x['ProcId']) for x in jobs)+')'
 expression='strcat('+json.dumps(src['out']+'=logs/job_')+',string(ClusterId),"_",string(ProcId),"_",Args,".out;",'+json.dumps(src['err']+'=logs/job_')+',string(ClusterId),"_",string(ProcId),"_",Args,".err")'
 for attribute,value in [('Out',json.dumps(src['out'])),('Err',json.dumps(src['err'])),('TransferOutputRemaps',expression)]:
  r=subprocess.run(['condor_qedit','-constraint',condition,attribute,value],capture_output=True,text=True)
  if r.returncode:print('LOG_REMAP_FAILED',attribute,r.stderr[-700:],flush=True);return
 r=subprocess.run(['condor_transfer_data','-constraint',condition],capture_output=True,text=True)
 print('RETRIEVE_LOGS',cluster,len(jobs),r.returncode,r.stdout[-500:],r.stderr[-1000:],flush=True)
 if r.returncode==0:
  removed=subprocess.run(['condor_rm','-constraint',condition],capture_output=True,text=True)
  print('RETIRE_COMPLETED',cluster,len(jobs),removed.returncode,removed.stdout[-500:],removed.stderr[-500:],flush=True)
while True:
 retry_config=S/'configuration/retry_clusters.json'
 if retry_config.exists():
  try:CLUSTERS.update(json.loads(retry_config.read_text()))
  except json.JSONDecodeError:pass
 r=subprocess.run(['condor_q','-json','-attributes','ClusterId,ProcId,JobStatus,Args,ExitCode,HoldReason,RemoteWallClockTime,RemoteUserCpu,RemoteSysCpu,NumJobStarts,TransferOutputRemaps,Out,Err'],capture_output=True,text=True)
 if r.returncode:print('QUERY_RETRY',r.stderr[-800:],flush=True);time.sleep(30);continue
 a=[x for x in json.loads(r.stdout or '[]') if x.get('ClusterId') in CLUSTERS];counts=collections.Counter(str(x['JobStatus']) for x in a);failed=[x for x in a if x['JobStatus']==5 or x['JobStatus']==4 and x.get('ExitCode',0)!=0];out=list(Path('/eos/user/j/junseok/analysis/matching_studies/dstar_official_upstream_full_20261108/results').glob('file_*.json.gz'))
 ready_ids={int(p.name[5:11]) for p in out}
 historical_failed={int(json.loads(p.read_text())['Args'].split()[0]) for p in (S/'results').glob('failed_*.json')}
 state=dict(clusters=sorted(CLUSTERS),status_counts=dict(counts),results=len(out),failed_or_held=failed,unresolved_failed_file_ids=sorted(historical_failed-ready_ids),seconds=time.monotonic()-start)
 (S/'configuration/batch_progress.json').write_text(json.dumps(state,indent=2)+'\n')
 marker=(tuple(sorted(counts.items())),len(out),len(failed))
 if marker!=last:print('BATCH',json.dumps(state),flush=True);last=marker
 completed=[x for x in a if x['JobStatus']==4]
 for x in completed:
  (S/'results/job_metadata'/(str(x['ClusterId'])+'.'+str(x['ProcId'])+'.json')).write_text(json.dumps(x,indent=2)+'\n')
  if x.get('ExitCode',0)!=0:(S/'results'/('failed_'+str(x['ClusterId'])+'.'+str(x['ProcId'])+'.json')).write_text(json.dumps(x,indent=2)+'\n')
 for cluster in sorted(CLUSTERS):retrieve(cluster,[x for x in completed if x['ClusterId']==cluster])
 if len(out)==5114:print('ALL_MATCHING_RESULTS_COMPLETE',flush=True);break
 time.sleep(30)
