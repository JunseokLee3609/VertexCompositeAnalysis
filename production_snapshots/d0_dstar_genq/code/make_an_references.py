import json,uproot,concurrent.futures,os,sys,time
from pathlib import Path
base=Path('/home/jun502s/DstarAna/DStarAnalysis/Fit/PbPb/Test/SpinAlignment/CommentAction/studies/d0_dstar_genq_reference_20261113')
base.mkdir(parents=True,exist_ok=True)
manifest=json.load(open('/tmp/dstar_genq_an_input_manifest_20261113.json'))
rows=manifest if isinstance(manifest,list) else manifest['files']
event=['RunNb','LSNb','EventNb']
plane=event+['bestvtxX','bestvtxY','bestvtxZ']+[k+s for s in ['','_forw','_afterw'] for k in ['all_trkQx','all_trkQy','all_trkW','trkQx','trkQy']]
star=event+['candSize','mass','centrality','pTerrGrandD1','pTerrGrandD2','pTerrD2','NHitGrandD1','NHitGrandD2','NHitD2','HighPurityGrandDaugther1','HighPurityGrandDaugther2','HighPurityDaugther2','zDCASignificanceGrandDaugther1','zDCASignificanceGrandDaugther2','zDCASignificanceDaugther2','xyDCASignificanceGrandDaugther1','xyDCASignificanceGrandDaugther2','xyDCASignificanceDaugther2','EtaGrandD1','EtaGrandD2','EtaD2','PhiGrandD1','PhiGrandD2','PhiD2']+['ephf'+s+k for s in ['','p','m'] for k in ['sumCosRaw','sumSinRaw','sumPtOrEt']]
def one(r):
 out=base/(str(r['file_index'])+'.root')
 with uproot.open(r['local']) as f,uproot.recreate(out,compression=uproot.ZSTD(4)) as g:
  sizes=[]
  for path,fields in [('d0ana_newreduced/PATCompositeNtuple',event+['candSize']),('dStarana_mc/PATCompositeNtuple',star),('eventplane/EventPlane',plane)]:
   t=f[path];a=t.arrays(fields,library='ak',how=dict);g.mktree(path,a);sizes.append(t.num_entries)
  assert len(set(sizes))==1
 return dict(file_index=r['file_index'],input_lfn=r['input_LFN'],source=r['local'],reference=str(out),entries=sizes[0],bytes=out.stat().st_size)
def cached(r):
 out=base/(str(r['file_index'])+'.root')
 if out.exists():
  try:
   with uproot.open(out) as f:
    sizes=[f[t].num_entries for t in ['d0ana_newreduced/PATCompositeNtuple','dStarana_mc/PATCompositeNtuple','eventplane/EventPlane']]
    assert len(set(sizes))==1
   return dict(file_index=r['file_index'],input_lfn=r['input_LFN'],source=r['local'],reference=str(out),entries=sizes[0],bytes=out.stat().st_size)
  except Exception as e:print('REBUILD_INCOMPLETE_REFERENCE',r['file_index'],str(e)[:150],flush=True)
 return one(r)

if __name__=='__main__':
 chosen=rows if len(sys.argv)<2 or sys.argv[1]=='all' else [next(r for r in rows if r['original_job']==1 and r['sample']=='prompt_2_4')]
 result=[]
 with concurrent.futures.ProcessPoolExecutor(max_workers=8) as pool:
  for x in pool.map(cached,chosen):
   result.append(x)
   if len(result)%100==0:print('REFERENCES',len(result),'bytes',sum(z['bytes'] for z in result),flush=True)
 (base/('manifest_all.json' if len(chosen)>1 else 'manifest_probe.json')).write_text(json.dumps(result,indent=2)+'\n')
 print(json.dumps(dict(files=len(result),bytes=sum(x['bytes'] for x in result),example=result[:1])),flush=True)
