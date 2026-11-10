import gzip, json, os, subprocess, sys
from pathlib import Path
payload=json.load(gzip.open(sys.argv[1],'rt'));lfn=payload['input_LFN'];dest=Path(sys.argv[2])
catalogue=json.loads(Path(sys.argv[3]).read_text());record=catalogue[lfn]
proxy=os.environ.get('X509_USER_PROXY','/tmp/x509up_u'+str(os.getuid()))
ca=os.environ.get('X509_CERT_DIR') or ('/cvmfs/grid.cern.ch/etc/grid-security/certificates' if Path('/cvmfs/grid.cern.ch/etc/grid-security/certificates').exists() else '/etc/grid-security/certificates')
fail=[]
for item in record['replicas']:
    url=item['url'];print('TRY_CATALOGUE_DISK',url,flush=True)
    r=subprocess.run(['curl','--fail','--location','--silent','--show-error','--connect-timeout','30','--max-time','1800','--capath',ca,'--cert',proxy,'--key',proxy,'--output',str(dest),url],capture_output=True,text=True)
    if r.returncode==0 and dest.stat().st_size==record['bytes']:
        print('MINIAOD_READY',dest.stat().st_size,flush=True);sys.exit(0)
    fail.append(dict(endpoint=url,returncode=r.returncode,bytes=dest.stat().st_size if dest.exists() else None,error=r.stderr[-2500:]))
    print('DIRECT_DISK_FAILED',r.returncode,r.stderr[-1000:],flush=True)
raise RuntimeError('Authoritative disk replicas unavailable: '+json.dumps(fail))
