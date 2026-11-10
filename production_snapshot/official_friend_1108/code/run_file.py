from pathlib import Path
import os
for k in ['OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS']:os.environ[k]='1'
import ROOT,json,gzip,sys,time
from DataFormats.FWLite import Events,Handle
ROOT.gSystem.Load('libFWCoreFWLite');ROOT.FWLiteEnabler.enable()
ROOT.gSystem.Load('libDataFormatsPatCandidates');ROOT.gSystem.Load('libDataFormatsTrackReco')
if ROOT.IsImplicitMTEnabled():ROOT.DisableImplicitMT()
ROOT.gInterpreter.AddIncludePath(os.environ['CMSSW_RELEASE_BASE']+'/src')
ROOT.gInterpreter.AddIncludePath(str(Path(__file__).parent))
ROOT.gInterpreter.Declare((Path(__file__).parent/'IndependentMatcher.hxx').read_text())
vd=ROOT.std.vector('double');vi=ROOT.std.vector('int')
COLUMNS=['central_entry','source_entry','candidate_index','matchGEN','matchHYDJET','isSwap','matchGen_pruned_index','matchGen_index',
 'missing_identity','ambiguous_identity','reason','metadata_index','raw_track_index_0','raw_track_index_1','raw_track_index_2',
 'raw_track_deltaR_0','raw_track_deltaR_1','raw_track_deltaR_2','track_GEN_collection_0','track_GEN_collection_1','track_GEN_collection_2',
 'track_GEN_index_0','track_GEN_index_1','track_GEN_index_2']
THREAD=dict(ROOT_implicit_MT=bool(ROOT.IsImplicitMTEnabled()),ROOT_thread_pool=int(ROOT.GetThreadPoolSize()),
 environment={k:os.environ[k] for k in ['OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS']})
assert not THREAD['ROOT_implicit_MT'] and THREAD['ROOT_thread_pool']==0
def run_file(input_path,output_path):
 start=time.monotonic();p=json.load(gzip.open(input_path,'rt'));needed={(e['run'],e['lumi'],e['event']):e for e in p['events']}
 wanted=len(needed);assert wanted==len(p['events']);records=[];metadata=[];nfound=0;scanned=0;unmapped=0;duplicate=0;unique_gen=0
 hs=[Handle('std::vector<pat::PackedCandidate>') for _ in range(3)]
 hv=Handle('std::vector<reco::Vertex>');hb=Handle('reco::BeamSpot')
 hp=Handle('std::vector<pat::PackedGenParticle>');hg=Handle('std::vector<reco::GenParticle>')
 lfn=p['input_LFN'];url=os.environ.get('DSTAR_LOCAL_INPUT') or ('file:/eos/cms'+lfn if os.path.exists('/eos/cms'+lfn) else 'root://cms-xrd-global.cern.ch/'+lfn)
 print('START',p['file_index'],'events',wanted,'thread',json.dumps(THREAD),flush=True)
 for ev in (Events([url]) if needed else []):
  scanned+=1;a=ev.eventAuxiliary();key=(int(a.run()),int(a.luminosityBlock()),int(a.event()))
  e=needed.pop(key,None)
  if e is None:continue
  for label,h in zip(['packedPFCandidates','lostTracks',('lostTracks','eleTracks')],hs):
   ev.getByLabel(label,h)
   if not h.isValid():raise RuntimeError('missing '+str(label))
  ev.getByLabel('offlineSlimmedPrimaryVertices',hv);ev.getByLabel('offlineBeamSpot',hb)
  ev.getByLabel('packedGenParticles',hp);ev.getByLabel('prunedGenParticles',hg)
  if not all(h.isValid() for h in [hv,hb,hp,hg]):raise RuntimeError('missing GEN/vertex/beamspot product')
  r=e['reco'];g=e['gen']
  result=ROOT.independentMatch(*[h.product() for h in hs],hv.product(),hb.product(),hp.product(),hg.product(),
    vd(r['error']),vi(r['charge']),vi(r['hits']),vd(r['dzsig']),vd(r['dxysig']),vi(r['kaon_pos']))
  offset=len(metadata);eventmatched=[]
  for truth in result.truth:metadata.append([float(v) for v in truth])
  for c,z in enumerate(result.rows):
   mid=offset+int(z.metadata) if z.metadata>=0 else -1;stored=-1
   if z.signal and mid>=0:
    vals=metadata[mid]
    matches=[j for j in range(len(g['index'])) if abs(g['parent_pt'][j]-vals[0])<1e-4 and abs(g['parent_eta'][j]-vals[1])<1e-5
      and abs(__import__('math').remainder(g['parent_phi'][j]-vals[2],2*__import__('math').pi))<1e-5]
    if len(matches)>1:raise RuntimeError('Ambiguous original stored GEN index')
    if matches:stored=int(g['index'][matches[0]])
    else:unmapped+=1
   records.append([int(r['central_entry'][c]),int(e['source_entry']),int(r['candidate_index'][c]),
    int(z.signal),int(z.hydjet),int(z.swap),int(z.gen),stored,int(z.missing),int(z.ambiguous),int(z.reason),mid,
    *[int(v) for v in z.raw],*[float(v) for v in z.distance],*[int(v) for v in z.selectedCollection],*[int(v) for v in z.selectedIndex]])
   if z.signal and not z.swap:eventmatched.append(int(z.gen))
  unique_gen+=len(set(eventmatched));duplicate+=len(eventmatched)-len(set(eventmatched));nfound+=1
  if nfound%200==0:print('PROGRESS',nfound,'/',wanted,round(time.monotonic()-start,1),flush=True)
  if not needed:break
 if needed:raise RuntimeError('Missing events '+str(list(needed)[:5]))
 if len(records)!=p['all_candidates']:raise RuntimeError('row count mismatch')
 if sorted(int(r[0]) for r in records)!=list(range(p['all_candidates'])):raise RuntimeError('Missing or repeated upstream candidate')
 summary=dict(status='complete',file_index=p['file_index'],source=p['source'],all_events=p['all_events'],all_candidates=p['all_candidates'],events=wanted,events_found=nfound,scanned=scanned,rows=len(records),
  matched=sum(r[3] for r in records),hydjet_matched=sum(r[4] for r in records),swapped=sum(r[5] for r in records),
  missing_identity=sum(bool(r[8]) for r in records),ambiguous_identity=sum(bool(r[9]) for r in records),
  unmatched_stored_GEN_indices=unmapped,unique_matched_GEN_Dstars=unique_gen,duplicate_candidates=duplicate,
  thread=THREAD,seconds=time.monotonic()-start)
 out=dict(summary=summary,columns=COLUMNS,records=records,metadata=metadata)
 target=Path(output_path);temporary=target.with_suffix('.writing')
 with gzip.open(temporary,'wt',compresslevel=3) as f:json.dump(out,f,separators=(',',':'))
 temporary.replace(target);print('SUMMARY',json.dumps(summary),flush=True)
 return out
if __name__=='__main__':run_file(sys.argv[1],sys.argv[2])
