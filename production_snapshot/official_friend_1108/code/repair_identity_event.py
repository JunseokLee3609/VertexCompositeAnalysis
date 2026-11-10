from pathlib import Path
import os
for k in ['OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS']:os.environ[k]='1'
import ROOT,json,gzip,sys,time,math
from DataFormats.FWLite import Events,Handle
S=Path(__file__).resolve().parents[1];EOS=Path('/eos/user/j/junseok/analysis/matching_studies/dstar_official_upstream_full_20261108');fid=int(sys.argv[1]);started=time.monotonic()
ROOT.gSystem.Load('libFWCoreFWLite');ROOT.FWLiteEnabler.enable();ROOT.gSystem.Load('libDataFormatsPatCandidates');ROOT.gSystem.Load('libDataFormatsTrackReco')
if ROOT.IsImplicitMTEnabled():ROOT.DisableImplicitMT()
assert not ROOT.IsImplicitMTEnabled() and ROOT.GetThreadPoolSize()==0
ROOT.gInterpreter.AddIncludePath(os.environ['CMSSW_RELEASE_BASE']+'/src');ROOT.gInterpreter.AddIncludePath(str(S/'code'))
code=(S/'code/IndependentMatcher.hxx').read_text().replace('#include "full_rank.hxx"','#include "full_rank_equivalent.hxx"');ROOT.gInterpreter.Declare(code)
vd=ROOT.std.vector('double');vi=ROOT.std.vector('int')
p=json.load(gzip.open(EOS/'payloads'/f'file_{fid:06d}.json.gz','rt'));dest=EOS/'results'/f'file_{fid:06d}.json.gz';a=json.load(gzip.open(dest,'rt'));cols={k:i for i,k in enumerate(a['columns'])}
bad=[r for r in a['records'] if r[cols['ambiguous_identity']]];needed={int(r[cols['source_entry']]) for r in bad};events={int(e['source_entry']):e for e in p['events'] if int(e['source_entry']) in needed};targets={(e['run'],e['lumi'],e['event']):e for e in events.values()};assert targets
bykey={(int(r[cols['source_entry']]),int(r[cols['candidate_index']])):r for r in a['records']};proof=[];event_changes=[]
labels=['packedPFCandidates','lostTracks',('lostTracks','eleTracks'),'offlineSlimmedPrimaryVertices','offlineBeamSpot','packedGenParticles','prunedGenParticles'];types=['std::vector<pat::PackedCandidate>']*3+['std::vector<reco::Vertex>','reco::BeamSpot','std::vector<pat::PackedGenParticle>','std::vector<reco::GenParticle>']
print('REPAIR_EVENTS',fid,len(targets),len(bad),flush=True)
for ev in Events(['root://cms-xrd-global.cern.ch/'+p['input_LFN']]):
    x=ev.eventAuxiliary();key=(int(x.run()),int(x.luminosityBlock()),int(x.event()));e=targets.pop(key,None)
    if e is None:continue
    handles=[]
    for label,kind in zip(labels,types):
        h=Handle(kind);ev.getByLabel(label,h);assert h.isValid();handles.append(h)
    r=e['reco'];g=e['gen'];result=ROOT.independentMatch(*[h.product() for h in handles],vd(r['error']),vi(r['charge']),vi(r['hits']),vd(r['dzsig']),vd(r['dxysig']),vi(r['kaon_pos']))
    old_event=[bykey[(int(e['source_entry']),i)] for i in range(len(r['candidate_index']))]
    old_groups=[int(row[cols['matchGen_pruned_index']]) for row in old_event if row[cols['matchGEN']] and not row[cols['isSwap']]]
    before_unmapped=sum(bool(row[cols['matchGEN']]) and row[cols['matchGen_index']]<0 for row in old_event)
    offset=len(a['metadata']);a['metadata'].extend([[float(v) for v in truth] for truth in result.truth])
    changed=[]
    for i,z in enumerate(result.rows):
        old=old_event[i]
        if not old[cols['ambiguous_identity']]:
            assert [int(z.signal),int(z.hydjet),int(z.swap),int(z.gen)]==[int(old[cols[k]]) for k in ['matchGEN','matchHYDJET','isSwap','matchGen_pruned_index']]
            continue
        assert not z.missing and not z.ambiguous
        stored=-1;mid=offset+int(z.metadata) if z.metadata>=0 else -1
        if z.signal and mid>=0:
            vals=a['metadata'][mid];matches=[j for j in range(len(g['index'])) if abs(g['parent_pt'][j]-vals[0])<1e-4 and abs(g['parent_eta'][j]-vals[1])<1e-5 and abs(math.remainder(g['parent_phi'][j]-vals[2],2*math.pi))<1e-5];assert len(matches)<=1
            if matches:stored=int(g['index'][matches[0]])
        new=[int(r['central_entry'][i]),int(e['source_entry']),int(r['candidate_index'][i]),int(z.signal),int(z.hydjet),int(z.swap),int(z.gen),stored,int(z.missing),int(z.ambiguous),int(z.reason),mid,*[int(v) for v in z.raw],*[float(v) for v in z.distance],*[int(v) for v in z.selectedCollection],*[int(v) for v in z.selectedIndex]]
        assert len(new)==len(a['columns']);proof.append(dict(source_entry=int(e['source_entry']),candidate_index=i,flat_candidate=int(old[0]),old=old.copy(),new=new.copy(),criterion='all reconstructed-track state components equal exactly; no new numerical tolerance'))
        old[:]=new;changed.append(i)
    new_groups=[int(row[cols['matchGen_pruned_index']]) for row in old_event if row[cols['matchGEN']] and not row[cols['isSwap']]]
    after_unmapped=sum(bool(row[cols['matchGEN']]) and row[cols['matchGen_index']]<0 for row in old_event)
    a['summary']['unique_matched_GEN_Dstars']+=len(set(new_groups))-len(set(old_groups));a['summary']['duplicate_candidates']+=(len(new_groups)-len(set(new_groups)))-(len(old_groups)-len(set(old_groups)));a['summary']['unmatched_stored_GEN_indices']+=after_unmapped-before_unmapped
    event_changes.append(dict(run=key[0],lumi=key[1],event=key[2],source_entry=int(e['source_entry']),changed_candidates=changed));print('EVENT_REPAIRED',event_changes[-1],flush=True)
    if not targets:break
assert not targets and len(proof)==len(bad)
for target,key in [('matched','matchGEN'),('hydjet_matched','matchHYDJET'),('swapped','isSwap'),('missing_identity','missing_identity'),('ambiguous_identity','ambiguous_identity')]:a['summary'][target]=sum(bool(r[cols[key]]) for r in a['records'])
a['summary']['identity_equivalence_repair']=dict(events=event_changes,candidates=len(proof),seconds=time.monotonic()-started,thread=1,criterion='exact equality of pt, eta, phi, ptError, charge, hits and both impact-parameter significances')
assert a['summary']['ambiguous_identity']==0
(S/'results'/f'identity_repair_{fid:06d}.json').write_text(json.dumps(dict(summary=a['summary'],proof=proof),indent=2)+chr(10))
tmp=dest.with_name(dest.name+'.identity-repair-writing')
with gzip.open(tmp,'wt',compresslevel=3) as f:json.dump(a,f,separators=(',',':'))
tmp.replace(dest);print('PUBLISHED_REPAIRED_RESULT',json.dumps(a['summary']),flush=True)
