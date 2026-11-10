from pathlib import Path
from datetime import datetime, timezone
import concurrent.futures, gzip, json, os, re, subprocess
S=Path(__file__).resolve().parents[1]
EOS=Path('/eos/user/j/junseok/analysis/matching_studies/dstar_official_upstream_full_20261108')
R=S/'configuration/retries';R.mkdir(exist_ok=True)
registry=S/'configuration/catalogue_retry_submissions.json'
prior=json.loads(registry.read_text()) if registry.exists() else []
submitted={fid for rec in prior for fid in rec['file_ids']}
failed={int(json.loads(p.read_text())['Args'].split()[0]) for p in (S/'results').glob('failed_*.json')}
ids=sorted(fid for fid in failed-submitted if not (EOS/'results'/f'file_{fid:06d}.json.gz').exists())
if not ids:
    print('NO_NEW_TRANSFER_RETRIES',flush=True);raise SystemExit(0)
def query(fid):
    p=json.load(gzip.open(EOS/'payloads'/f'file_{fid:06d}.json.gz','rt'));lfn=p['input_LFN']
    r=subprocess.run(['/cvmfs/cms.cern.ch/common/dasgoclient','-query','site file='+lfn,'-json'],capture_output=True,text=True,timeout=120)
    if r.returncode:raise RuntimeError('DAS failed '+str(fid)+': '+r.stderr[-2000:])
    replicas={};size=None
    for record in json.loads(r.stdout):
        for site in record.get('site',[]):
            size=site.get('bytes',site.get('size',size))
            for url,meta in site.get('pfns',{}).items():
                if meta.get('type')=='DISK' and url.startswith(('davs://','https://')):
                    http='https://'+url.split('://',1)[1];replicas[http]=dict(url=http,priority=meta.get('priority',99),rse=meta.get('rse'))
    if not replicas or size is None:raise RuntimeError('No authoritative disk replicas '+str(fid))
    result=dict(file_index=fid,bytes=int(size),replicas=sorted(replicas.values(),key=lambda x:x['priority']))
    print('CATALOGUE_READY',fid,len(replicas),size,flush=True)
    return lfn,result
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    catalogue=dict(pool.map(query,ids))
sequence=len(prior)+1;tag=f'{sequence:04d}'
catfile=R/f'catalogue_{tag}.json';catfile.write_text(json.dumps(catalogue,indent=2)+chr(10))
idsfile=R/f'ids_{tag}.txt';idsfile.write_text(''.join(f'{fid:06d}'+chr(10) for fid in ids))
wrapper=S/'code/worker_staged_catalogue_retry.sh'
text=(S/'code/worker_staged.sh').read_text().replace('python3 stage_miniaod.py input.json.gz input_miniaod.root','python3 stage_miniaod_catalogue_retry.py input.json.gz input_miniaod.root "$2"')
wrapper.write_text(text);wrapper.chmod(0o755)
base=(S/'configuration/full.sub').read_text().splitlines();lines=[]
for line in base:
    if line.startswith(('max_materialize','max_idle')):continue
    if line.startswith('executable ='):line='executable = code/worker_staged_catalogue_retry.sh'
    elif line.startswith('arguments ='):line='arguments = $(fileid) '+catfile.name
    elif line.startswith('transfer_input_files ='):line=line.replace('code/stage_miniaod.py','code/stage_miniaod_catalogue_retry.py')+','+str(catfile.relative_to(S))
    elif line.startswith('request_memory ='):line='request_memory = 3000MB'
    elif line.startswith('queue fileid from'):line='queue fileid from '+str(idsfile.relative_to(S))
    lines.append(line)
sub=R/f'retry_{tag}.sub';sub.write_text(chr(10).join(lines)+chr(10))
env=os.environ.copy();env['_CONDOR_SEC_CREDENTIAL_PRODUCER']=''
r=subprocess.run(['condor_submit','-spool',str(sub.relative_to(S))],cwd=S,env=env,capture_output=True,text=True,timeout=180)
print(r.stdout,r.stderr,flush=True);r.check_returncode()
m=re.search(r'cluster (\d+)',r.stdout);assert m;cluster=int(m.group(1))
rec=dict(cluster=cluster,file_ids=ids,catalogue=str(catfile),submit=str(sub),utc=datetime.now(timezone.utc).isoformat())
prior.append(rec);registry.write_text(json.dumps(prior,indent=2)+chr(10))
(S/'configuration/retry_clusters.json').write_text(json.dumps([x['cluster'] for x in prior])+chr(10))
print('RETRY_SUBMITTED',json.dumps(rec),flush=True)
