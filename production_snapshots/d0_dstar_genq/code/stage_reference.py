import json,os,subprocess,sys
from pathlib import Path
from PSet import process
if len(process.source.fileNames)!=1:
 raise RuntimeError('Exactly one MiniAOD file per job is required')
if process.genq.allowPartial.value():
 raise RuntimeError('Partial mode is forbidden in production')
lfn=str(process.source.fileNames[0]);lfn=lfn[lfn.index('/store/'):].split('?')[0]
rows=json.loads(Path(process.genq.manifest.value()).read_text())
row=next(r for r in rows if r['miniaod_lfn']==lfn)
url=row['an_raw_url']
token=Path('an_reference.token').read_text().strip()
if not token.startswith('zteos64:'):raise RuntimeError('Invalid reference read-token format')
env=os.environ.copy()
env['XrdSecPROTOCOL']='unix'
env['KRB5CCNAME']='FILE:/tmp/genq_absent_kerberos'
env.pop('X509_USER_PROXY',None)
r=subprocess.run(['xrdcp','--nopbar','--force',url+'?authz='+token,'an_reference.root'],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
if r.returncode:
 sys.stderr.write((r.stdout+r.stderr).replace(token,'[REDACTED_REFERENCE_TOKEN]'))
 raise RuntimeError('Reference staging failed: '+str(r.returncode))
row['an_archive_url']=url
row['an_raw_url']='an_reference.root'
Path('runtime_mapping.json').write_text(json.dumps([row])+'\n')
Path('run_cfg.py').write_text("""import FWCore.ParameterSet.Config as cms
from PSet import process
process.genq.inputFile=cms.string(process.source.fileNames[0])
process.genq.manifest=cms.string('runtime_mapping.json')
""")
print('REFERENCE_STAGED bytes='+str(Path('an_reference.root').stat().st_size),flush=True)
