import os,sys,json,gzip,subprocess
from pathlib import Path
p=json.load(gzip.open(sys.argv[1],'rt'));lfn=p['input_LFN'];dest=Path(sys.argv[2]);fail=[]
for endpoint in ['root://cms-xrd-global.cern.ch/','root://cmsxrootd.fnal.gov/','root://xrootd-cms.infn.it/']:
 print('TRY_MINIAOD',endpoint+lfn,flush=True)
 r=subprocess.run(['xrdcp','--silent','--streams','4','-f',endpoint+lfn,str(dest)],capture_output=True,text=True)
 if r.returncode==0:print('MINIAOD_READY',dest.stat().st_size,flush=True);sys.exit(0)
 fail.append(dict(endpoint=endpoint,returncode=r.returncode,error=r.stderr[-1500:]));print('INPUT_RETRY',r.returncode,r.stderr[-600:],flush=True)
# Ask the authoritative replica catalogue only when redirector access fails.
r=subprocess.run(['/cvmfs/cms.cern.ch/common/dasgoclient','-query','site file='+lfn,'-json'],capture_output=True,text=True,timeout=120)
if r.returncode==0:
 urls={}
 for record in json.loads(r.stdout):
  for site in record.get('site',[]):
   for url,meta in site.get('pfns',{}).items():
    if meta.get('type')=='DISK' and url.startswith(('davs://','https://')):urls[url]=(meta.get('priority',99),site.get('bytes',site.get('size')))
 proxy=os.environ.get('X509_USER_PROXY','/tmp/x509up_u'+str(os.getuid()));ca=os.environ.get('X509_CERT_DIR','/etc/grid-security/certificates')
 for url,(priority,size) in sorted(urls.items(),key=lambda x:x[1][0]):
  http='https://'+url.split('://',1)[1];print('TRY_DIRECT_DISK',http,flush=True)
  rr=subprocess.run(['curl','--fail','--location','--silent','--show-error','--connect-timeout','30','--max-time','2400','--capath',ca,'--cert',proxy,'--key',proxy,'--output',str(dest),http],capture_output=True,text=True)
  if rr.returncode==0 and (size is None or dest.stat().st_size==int(size)):print('MINIAOD_READY',dest.stat().st_size,flush=True);sys.exit(0)
  fail.append(dict(endpoint=http,returncode=rr.returncode,error=rr.stderr[-1500:]));print('DIRECT_DISK_FAILED',rr.returncode,rr.stderr[-600:],flush=True)
raise RuntimeError('MiniAOD unavailable: '+json.dumps(fail))
