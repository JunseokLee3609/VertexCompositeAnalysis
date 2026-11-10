from pathlib import Path
import os
for k in ['OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS']:os.environ[k]='1'
import ROOT, json, struct, time
from DataFormats.FWLite import Events,Handle
S=Path(__file__).resolve().parents[1]
ROOT.gSystem.Load('libFWCoreFWLite');ROOT.FWLiteEnabler.enable();ROOT.gSystem.Load('libDataFormatsPatCandidates');ROOT.gSystem.Load('libDataFormatsTrackReco')
if ROOT.IsImplicitMTEnabled():ROOT.DisableImplicitMT()
ROOT.gInterpreter.AddIncludePath(os.environ['CMSSW_RELEASE_BASE']+'/src')
text=(S/'code/full_rank.hxx').read_text();ROOT.gInterpreter.Declare(text)
prefix=text[text.index(' std::vector<reco::Track>raw;'):text.index(' std::vector<std::tuple<double,int,int>>pairs;')]
helper="std::vector<std::vector<double>> inspectOriginalTracks(const std::vector<pat::PackedCandidate>&pf,const std::vector<pat::PackedCandidate>&lost,const std::vector<pat::PackedCandidate>&ele,const std::vector<reco::Vertex>&vertices,const reco::BeamSpot&bs) { using namespace fullmatch;"+prefix+"std::vector<std::vector<double>> result;for(unsigned i=0;i<tracks.size();++i){const auto&t=tracks[i];result.push_back({double(i),t.pt,t.eta,t.phi,t.err,double(t.charge),double(t.hits),t.dzsig,t.dxysig});}return result;}"
ROOT.gInterpreter.Declare(helper)
cases=json.loads((S/'configuration/unresolved_identity_cases.json').read_text());groups={}
for case in cases:groups.setdefault((case['file_index'],case['event']['run'],case['event']['lumi'],case['event']['event']),[]).append(case)
labels=['packedPFCandidates','lostTracks',('lostTracks','eleTracks'),'offlineSlimmedPrimaryVertices','offlineBeamSpot','packedGenParticles','prunedGenParticles']
types=['std::vector<pat::PackedCandidate>']*3+['std::vector<reco::Vertex>','reco::BeamSpot','std::vector<pat::PackedGenParticle>','std::vector<reco::GenParticle>']
def f32(x):return struct.unpack('f',struct.pack('f',x))[0]
out=[]
for key,items in groups.items():
    c=items[0];wanted=key[1:];print('READ_SINGLE_EVENT',key,c['lfn'],flush=True);found=False
    for ev in Events(['root://cms-xrd-global.cern.ch/'+c['lfn']]):
        a=ev.eventAuxiliary()
        if (int(a.run()),int(a.luminosityBlock()),int(a.event()))!=wanted:continue
        found=True;handles=[]
        for label,kind in zip(labels,types):
            h=Handle(kind);ev.getByLabel(label,h);assert h.isValid();handles.append(h)
        tracks=[[float(v) for v in row] for row in ROOT.inspectOriginalTracks(*[h.product() for h in handles[:5]])]
        for c in items:
            sig=c['signature'];row=c['row'];diagnostics=[]
            for leg in range(3):
                if not (int(row['missing_identity'])|int(row['ambiguous_identity']))&(1<<leg):continue
                candidates=[t for t in tracks if t[5]==sig['charge'][leg] and t[6]==sig['hits'][leg] and f32(t[4])==sig['error'][leg]]
                exact=[t for t in candidates if f32(t[7])==sig['dzsig'][leg] and f32(t[8])==sig['dxysig'][leg]]
                xy=[t for t in candidates if f32(t[8])==sig['dxysig'][leg]]
                nearest=sorted([t for t in tracks if t[5]==sig['charge'][leg] and t[6]==sig['hits'][leg]],key=lambda t:abs(f32(t[4])-sig['error'][leg]))[:8]
                diagnostics.append(dict(leg=leg,error_hits_charge_candidates=candidates,exact_DCA_candidates=exact,exact_xy_candidates=xy,nearest_error_same_charge_hits=nearest))
            gen=[]
            for leg in range(3):
                coll=int(row[f'track_GEN_collection_{leg}']);idx=int(row[f'track_GEN_index_{leg}'])
                if coll<0:continue
                particle=handles[6 if coll==0 else 5].product()[idx];chain=[];cursor=particle
                for n in range(7):
                    chain.append(int(cursor.pdgId()))
                    if cursor.numberOfMothers()==0:break
                    cursor=cursor.mother(0)
                    if not cursor:break
                gen.append(dict(leg=leg,collection=coll,index=idx,ancestry_pdgIds=chain))
            record=dict(file_index=c['file_index'],event=c['event'],flat_candidate=int(row['central_entry']),candidate_index=int(row['candidate_index']),track_columns=['raw_index','pt','eta','phi','ptError','charge','valid_hits','dz_significance','dxy_significance'],diagnostics=diagnostics,known_GEN_ancestry=gen)
            out.append(record);print('IDENTITY_DIAGNOSTIC',json.dumps(record),flush=True)
        break
    assert found,('event not found',wanted)
(S/'results/identity_event_diagnostics.json').write_text(json.dumps(out,indent=2)+chr(10));print('DIAGNOSIS_COMPLETE',len(out),flush=True)
