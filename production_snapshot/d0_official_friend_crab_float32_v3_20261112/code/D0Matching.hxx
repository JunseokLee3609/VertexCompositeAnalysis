#pragma once
// Matching semantics frozen from PATCompositeTreeProducer6 at 75af18a65a5a1c24aaaf722ab9d7fb1d5f6c8592.
#include <vector>
#include <map>
#include <set>
#include <array>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <string>
#include <stdexcept>
#include <algorithm>
#include <cmath>
#include <cstring>
#include <cstdint>
#include <sstream>
#include <iomanip>
#include "TFile.h"
#include "TTree.h"
#include "TTreeReader.h"
#include "TTreeReaderArray.h"
#include "TTreeReaderValue.h"
#include "TObjString.h"
#include "TVector3.h"
#include "DataFormats/PatCandidates/interface/PackedCandidate.h"
#include "DataFormats/PatCandidates/interface/PackedGenParticle.h"
#include "DataFormats/HepMCCandidate/interface/GenParticle.h"
#include "DataFormats/TrackReco/interface/Track.h"
#include "DataFormats/VertexReco/interface/Vertex.h"
#include "DataFormats/BeamSpot/interface/BeamSpot.h"
namespace d0friend {
using Key=std::tuple<unsigned,unsigned,unsigned>;
inline uint32_t bits(float v){uint32_t b;std::memcpy(&b,&v,4);return b;}
inline uint64_t trackKey(int ch,double err){return (uint64_t(bits(float(err)))<<32)|uint32_t(ch+8);}
inline uint64_t floatUlpDistance(float a,float b){
 if(a==b)return 0;
 if(!std::isfinite(a)||!std::isfinite(b))return UINT64_MAX;
 auto ordered=[](float v){auto u=bits(v);return (u&0x80000000U)?~u:(u|0x80000000U);};
 uint64_t aa=ordered(a),bb=ordered(b);return aa>bb?aa-bb:bb-aa;
}
inline const reco::Candidate* mother(const reco::Candidate*p){return p&&p->numberOfMothers()?p->mother(0):nullptr;}
inline bool hadron(int id){id=std::abs(id);return id==211||id==321||id==2212;}
struct Summary {int nonGamma=0,gamma=0;bool kaon=false,pion=false;};
inline Summary topology(const reco::Candidate*p){Summary s;for(unsigned i=0;i<p->numberOfDaughters();++i){auto d=p->daughter(i);if(!d)continue;int id=std::abs(d->pdgId());if(id==22)++s.gamma;else{++s.nonGamma;s.kaon|=id==321;s.pion|=id==211;}}return s;}
inline std::pair<int,int> ancestor(const reco::Candidate*p){int id=0,flavor=0;for(int depth=0;p&&depth<50;p=mother(p),++depth){if(depth>0){id=p->pdgId();auto text=std::to_string(std::abs(id));flavor=text[0]-'0';if(text[0]=='5'||std::abs(id)<=40)break;}}return {id,flavor};}
inline const reco::Candidate* differentMother(const reco::Candidate*p){if(!p)return nullptr;int id=p->pdgId();const auto*q=p;while(q&&q->pdgId()==id&&q->numberOfMothers())q=mother(q);return q&&q->pdgId()!=id?q:nullptr;}
struct Track {double pt,eta,phi,err,dz,xy;int charge,hits;};
struct Signature {float err,dz,xy;int charge,hits;};
using SignatureKey=std::tuple<uint32_t,uint32_t,uint32_t,int,int>;
struct Candidate {int positive,negative;bool kaonPositive,oldMatch,oldSwap;float eta,phi;};
struct InputEvent {unsigned run,lumi,event;Long64_t entry;int oldGen;std::vector<Signature>signatures;std::vector<Candidate>candidates;};
class Input {
 std::unique_ptr<TFile>file;TTree*tree;std::unique_ptr<TTreeReader>reader;
 std::unique_ptr<TTreeReaderValue<unsigned>>run,lumi,event;
 std::unique_ptr<TTreeReaderValue<int>>count,oldGen;
 std::map<std::string,std::unique_ptr<TTreeReaderArray<float>>>floats;
 std::unique_ptr<TTreeReaderArray<bool>>oldMatch,oldSwap;
 std::map<Key,Long64_t>index;
 public:Long64_t entries=0;int maximum=1;
 Input(const std::string&path){
  file.reset(TFile::Open(path.c_str()));if(!file||file->IsZombie())throw std::runtime_error("Cannot open raw ntuple");
  tree=dynamic_cast<TTree*>(file->Get("d0ana_newreduced/PATCompositeNtuple"));if(!tree)throw std::runtime_error("Missing D0 tree");
  entries=tree->GetEntries();
  {TTreeReader r(tree);TTreeReaderValue<unsigned>a(r,"RunNb"),b(r,"LSNb"),c(r,"EventNb");TTreeReaderValue<int>n(r,"candSize");Long64_t entry=0;while(r.Next()){if(!index.emplace(Key{*a,*b,*c},entry++).second)throw std::runtime_error("Nonunique stored 32-bit event key");maximum=std::max(maximum,*n);}if(entry!=entries)throw std::runtime_error("Incomplete raw event index");}
  reader.reset(new TTreeReader(tree));run.reset(new TTreeReaderValue<unsigned>(*reader,"RunNb"));lumi.reset(new TTreeReaderValue<unsigned>(*reader,"LSNb"));event.reset(new TTreeReaderValue<unsigned>(*reader,"EventNb"));count.reset(new TTreeReaderValue<int>(*reader,"candSize"));oldGen.reset(new TTreeReaderValue<int>(*reader,"candSize_gen"));
  for(const char*name:{"pTerrD1","pTerrD2","NHitD1","NHitD2","zDCASignificanceDaugther1","zDCASignificanceDaugther2","xyDCASignificanceDaugther1","xyDCASignificanceDaugther2","flavor","eta","phi"})floats[name].reset(new TTreeReaderArray<float>(*reader,name));
  oldMatch.reset(new TTreeReaderArray<bool>(*reader,"matchGEN"));oldSwap.reset(new TTreeReaderArray<bool>(*reader,"isSwap"));
 }
 Long64_t find(unsigned r,unsigned l,ULong64_t e)const{auto i=index.find({r,l,unsigned(e&0xffffffffULL)});return i==index.end()?-1:i->second;}
 InputEvent read(Long64_t entry){
  if(reader->SetEntry(entry)!=TTreeReader::kEntryValid)throw std::runtime_error("Cannot read raw source entry");
  InputEvent out{**run,**lumi,**event,entry,**oldGen,{},{}};
  if(**count<0||**count>maximum)throw std::runtime_error("Invalid candSize");
  for(const auto&f:floats)if(int(f.second->GetSize())!=**count)throw std::runtime_error("Candidate-array length mismatch: "+f.first);
  std::map<SignatureKey,int>unique;
  auto add=[&](unsigned c,int leg){std::string l=std::to_string(leg);Signature s{(*floats.at("pTerrD"+l))[c],(*floats.at("zDCASignificanceDaugther"+l))[c],(*floats.at("xyDCASignificanceDaugther"+l))[c],leg==1?1:-1,0};float nh=(*floats.at("NHitD"+l))[c];if(!std::isfinite(nh)||nh<0)throw std::runtime_error("Invalid stored track hit count");s.hits=int(nh);SignatureKey k{bits(s.err),bits(s.dz),bits(s.xy),s.charge,s.hits};auto q=unique.find(k);if(q!=unique.end())return q->second;int ix=out.signatures.size();unique[k]=ix;out.signatures.push_back(s);return ix;};
  for(int c=0;c<**count;++c){float fl=(*floats.at("flavor"))[c];if(fl!=1&&fl!=-1)throw std::runtime_error("Unexpected D0 flavor");out.candidates.push_back({add(c,1),add(c,2),fl<0,(*oldMatch)[c],(*oldSwap)[c],(*floats.at("eta"))[c],(*floats.at("phi"))[c]});}
  return out;
 }
};
inline std::vector<Track> originalTracks(const std::vector<pat::PackedCandidate>&pf,const std::vector<pat::PackedCandidate>&lost,const std::vector<pat::PackedCandidate>&ele,const std::vector<reco::Vertex>&vertices,const reco::BeamSpot&bs){
 std::vector<reco::Track>raw;unsigned added=0;
 auto add=[&](const auto&objects){for(const auto&p:objects){
  if(p.hasTrackDetails()){const auto&t=p.pseudoTrack();raw.emplace_back(t.chi2(),t.ndof()!=0?t.ndof():.1,t.referencePoint(),t.momentum(),t.charge(),t.covariance());const_cast<reco::HitPattern&>(raw.back().hitPattern())=t.hitPattern();}
  else if(p.covarianceVersion()>0){math::RhoEtaPhiVector p3(p.ptTrk(),p.etaAtVtx(),p.phiAtVtx());raw.emplace_back(.1,.1,p.vertex(),math::XYZVector(p3.x(),p3.y(),p3.z()),p.charge(),reco::TrackBase::CovarianceMatrix(ROOT::Math::SMatrixIdentity())*1.E12);if(p.lostInnerHits()==pat::PackedCandidate::validHitInFirstPixelBarrelLayer)raw.back().appendTrackerHitPattern(PixelSubdetector::PixelBarrel,1,0,TrackingRecHit::valid);}
  else continue;
  if(!vertices.empty()&&p.vertexRef().isNonnull()&&p.vertexRef().key()==0&&p.pvAssociationQuality()>pat::PackedCandidate::UsedInFitLoose)++added;
 }};add(pf);add(lost);add(ele);
 bool usePV=!vertices.empty()&&!(vertices[0].isFake()&&added==0)&&vertices[0].tracksSize()+added>=2;
 math::XYZPoint point(usePV?vertices[0].x():bs.position().x(),usePV?vertices[0].y():bs.position().y(),usePV?vertices[0].z():0.);
 double ze=usePV?vertices[0].zError():0.;double xe=usePV?vertices[0].xError():bs.BeamWidthX(),ye=usePV?vertices[0].yError():bs.BeamWidthY();
 std::vector<Track>out;out.reserve(raw.size());for(const auto&t:raw)out.push_back({t.pt(),t.eta(),t.phi(),t.ptError(),t.dz(point)/std::sqrt(t.dzError()*t.dzError()+ze*ze),t.dxy(point)/std::sqrt(t.dxyError()*t.dxyError()+xe*ye),int(t.charge()),int(t.numberOfValidHits())});return out;
}
struct GenTrack {const reco::Candidate*p;};
class Writer {
 std::unique_ptr<TFile>ownedFile;TFile*file=nullptr;TTree*tree;
 unsigned run=0,lumi=0,event32=0;ULong64_t event64=0;Long64_t sourceEntry=0;int n=0,ng=0,oldNg=0;
 std::map<std::string,std::vector<float>>rf,gf;
 std::map<std::string,std::vector<int>>ri,gi;
 std::map<std::string,std::unique_ptr<bool[]>>rb,gb;
 bool signalOnlyGenMatching;
 int maxReco;static constexpr int maxGen=50000;
 void reset(){for(auto&x:rf)std::fill_n(x.second.begin(),n,-99.f);for(auto&x:ri)std::fill_n(x.second.begin(),n,-99);for(auto&x:rb)std::fill_n(x.second.get(),n,false);for(auto&x:gf)std::fill_n(x.second.begin(),ng,-99.f);for(auto&x:gi)std::fill_n(x.second.begin(),ng,-99);for(auto&x:gb)std::fill_n(x.second.get(),ng,false);for(const char*k:{"matchGen_index","matchGen_pruned_index","matchGen_collisionId"})std::fill_n(ri.at(k).begin(),n,-1);std::fill_n(ri.at("matchGen_reason").begin(),n,0);}
 void particle(std::map<std::string,std::vector<float>>&f,std::map<std::string,std::vector<int>>&i,const std::string&prefix,int slot,const reco::Candidate*p,bool underscore){std::string s=underscore?"_":"";f.at(prefix+s+"pT")[slot]=p->pt();f.at(prefix+s+"eta")[slot]=p->eta();f.at(prefix+s+"phi")[slot]=p->phi();f.at(prefix+s+"mass")[slot]=p->mass();f.at(prefix+s+"y")[slot]=p->rapidity();i.at(prefix+s+"charge")[slot]=p->charge();i.at(prefix+s+"pdgId")[slot]=p->pdgId();}
 public:
 std::map<std::string,ULong64_t>stats;
 Writer(const std::string&path,int maximum,const std::string&provenance,TFile*externalFile=nullptr,bool signalOnly=true):signalOnlyGenMatching(signalOnly),maxReco(std::max(1,maximum)){
  if(externalFile)file=externalFile;else{ownedFile.reset(TFile::Open(path.c_str(),"RECREATE","D0 gen-matching friend",106));file=ownedFile.get();}if(!file||file->IsZombie())throw std::runtime_error("Cannot create friend");file->mkdir("d0ana_newreduced")->cd();tree=new TTree("PATCompositeNtuple","Entry-aligned D0 RECO and GEN replacement branches");tree->SetAutoFlush(-16000000);
  tree->Branch("RunNb",&run,"RunNb/i");tree->Branch("LSNb",&lumi,"LSNb/i");tree->Branch("EventNb",&event32,"EventNb/i");tree->Branch("EventNb64",&event64,"EventNb64/l");tree->Branch("source_entry",&sourceEntry,"source_entry/L");tree->Branch("candSize",&n,"candSize/I");tree->Branch("candSize_gen",&ng,"candSize_gen/I");tree->Branch("old_candSize_gen",&oldNg,"old_candSize_gen/I");
  auto floats=[&](auto&fields,const std::string&name,bool gen){auto&v=fields[name];v.resize(gen?maxGen:maxReco);tree->Branch(name.c_str(),v.data(),(name+"["+(gen?"candSize_gen":"candSize")+"]/F").c_str());};
  auto ints=[&](auto&fields,const std::string&name,bool gen){auto&v=fields[name];v.resize(gen?maxGen:maxReco);tree->Branch(name.c_str(),v.data(),(name+"["+(gen?"candSize_gen":"candSize")+"]/I").c_str());};
  auto bools=[&](auto&fields,const std::string&name,bool gen){fields[name].reset(new bool[gen?maxGen:maxReco]);tree->Branch(name.c_str(),fields[name].get(),(name+"["+(gen?"candSize_gen":"candSize")+"]/O").c_str());};
  for(const char*k:{"matchGEN","matchHYDJET","isSwap","matchGen_validD0chain"})bools(rb,k,false);
  for(const char*k:{"matchGen_index","matchGen_pruned_index","matchGen_collisionId","matchGen_reason","idmom_reco","idBAnc_reco","matchGen_D1ancestorId_","matchGen_D1ancestorFlavor_","matchGen_D0Dau1_motherPdgId","matchGen_D0Dau1_motherNDau","matchGen_D0Dau2_motherPdgId","matchGen_D0Dau2_motherNDau"})ints(ri,k,false);
  for(const char*k:{"matchGen_deltaR","matchGen3DPointingAngle","matchGen2DPointingAngle","matchGen3DDecayLength","matchGen2DDecayLength"})floats(rf,k,false);
  for(std::string p:{"matchGen_D0","matchGen_D0Dau1_","matchGen_D0Dau2_"}){for(std::string k:{"pT","eta","phi","mass","y"})floats(rf,p+k,false);for(std::string k:{"charge","pdgId"})ints(ri,p+k,false);}
  for(const char*k:{"gen_mass","gen_pT","gen_eta","gen_phi","gen_y"})floats(gf,k,true);
  for(const char*k:{"gen_status","gen_pdgId","gen_charge","gen_MotherID","gen_DauID1","gen_DauID2","gen_D0ancestorId_","gen_D0ancestorFlavor_","gen_pruned_index","gen_collisionId"})ints(gi,k,true);
  bools(gb,"gen_validD0chain",true);
  for(std::string p:{"gen_D0","gen_D0Dau1_","gen_D0Dau2_"}){for(std::string k:{"pT","eta","phi","mass","y"})floats(gf,p+k,true);for(std::string k:{"charge","pdgId"})ints(gi,p+k,true);}
  file->cd();TObjString(provenance.c_str()).Write("provenance");
  for(const char*k:{"events","candidates","old_matchGEN","new_matchGEN","new_matchHYDJET","old_signal_nonswap","new_signal_nonswap","gained_signal_nonswap","lost_signal_nonswap","new_swaps","gen_candidates","old_gen_candidates","identity_tracks","identity_xy_disambiguations","identity_roundoff_recoveries","identity_max_dz_ulp","identity_max_xy_ulp","audited_track_matches"})stats[k]=0;
 }
 void process(const InputEvent&input,ULong64_t eid,const std::vector<pat::PackedCandidate>&pf,const std::vector<pat::PackedCandidate>&lost,const std::vector<pat::PackedCandidate>&ele,const std::vector<reco::Vertex>&vertices,const reco::BeamSpot&bs,const std::vector<pat::PackedGenParticle>&packed,const std::vector<reco::GenParticle>&pruned,bool audit=false){
  if(input.entry!=Long64_t(stats.at("events")))throw std::runtime_error("MiniAOD source order does not match ntuple order");
  run=input.run;lumi=input.lumi;event32=input.event;event64=eid;sourceEntry=input.entry;n=input.candidates.size();oldNg=input.oldGen;
  std::vector<int>genRefs;std::unordered_map<const reco::Candidate*,int>prunedIndex;std::unordered_map<const reco::Candidate*,int>genIndex;
  for(unsigned j=0;j<pruned.size();++j){const auto&p=pruned[j];prunedIndex[&p]=j;if(std::abs(p.pdgId())!=421||(signalOnlyGenMatching&&p.collisionId()!=0))continue;auto s=topology(&p);if(s.nonGamma!=2||!s.kaon||!s.pion||s.gamma>1||p.numberOfDaughters()!=unsigned(2+s.gamma))continue;genIndex[&p]=genRefs.size();genRefs.push_back(j);}
  if(genRefs.size()>maxGen)throw std::runtime_error("GEN selection exceeds native maximum");ng=genRefs.size();reset();
  for(int j=0;j<ng;++j){const auto&p=pruned[genRefs[j]];particle(gf,gi,"gen",j,&p,true);particle(gf,gi,"gen_D0",j,&p,false);gi.at("gen_status")[j]=p.status();gi.at("gen_MotherID")[j]=mother(&p)?mother(&p)->pdgId():-99;gi.at("gen_DauID1")[j]=p.numberOfDaughters()>0&&p.daughter(0)?p.daughter(0)->pdgId():-99;gi.at("gen_DauID2")[j]=p.numberOfDaughters()>1&&p.daughter(1)?p.daughter(1)->pdgId():-99;auto a=ancestor(&p);gi.at("gen_D0ancestorId_")[j]=a.first;gi.at("gen_D0ancestorFlavor_")[j]=a.second;gi.at("gen_pruned_index")[j]=genRefs[j];gi.at("gen_collisionId")[j]=p.collisionId();bool k=false,pi=false;for(unsigned d=0;d<p.numberOfDaughters();++d){auto q=p.daughter(d);if(!q)continue;int id=std::abs(q->pdgId());if(id==321&&!k){particle(gf,gi,"gen_D0Dau1",j,q,true);k=true;}if(id==211&&!pi){particle(gf,gi,"gen_D0Dau2",j,q,true);pi=true;}}gb.at("gen_validD0chain")[j]=true;}
  auto raw=originalTracks(pf,lost,ele,vertices,bs);std::unordered_map<uint64_t,std::vector<int>>lookup;for(unsigned j=0;j<raw.size();++j)lookup[trackKey(raw[j].charge,raw[j].err)].push_back(j);
  std::vector<int>identity;identity.reserve(input.signatures.size());
  for(const auto&s:input.signatures){
   auto q=lookup.find(trackKey(s.charge,s.err));std::vector<int>same;
   if(q!=lookup.end())for(int j:q->second)if(raw[j].hits==s.hits&&floatUlpDistance(float(raw[j].dz),s.dz)<=2&&floatUlpDistance(float(raw[j].xy),s.xy)<=2)same.push_back(j);
   if(same.size()!=1){
    std::ostringstream msg;msg<<std::setprecision(17)<<"Original-track identity not unique at source entry "<<input.entry<<", signature "<<identity.size()<<", matches "<<same.size()<<" STORED charge="<<s.charge<<" hits="<<s.hits<<" err="<<s.err<<" dz="<<s.dz<<" xy="<<s.xy;
    if(q!=lookup.end())for(int j:q->second){const auto&t=raw[j];msg<<" RAW["<<j<<"] hits="<<t.hits<<" err="<<float(t.err)<<" dz="<<float(t.dz)<<" xy="<<float(t.xy)<<" pt="<<t.pt<<" eta="<<t.eta<<" phi="<<t.phi;}
    throw std::runtime_error(msg.str());
   }
   int found=same[0];auto du=floatUlpDistance(float(raw[found].dz),s.dz),xu=floatUlpDistance(float(raw[found].xy),s.xy);
   stats["identity_roundoff_recoveries"]+=du!=0||xu!=0;
   stats["identity_max_dz_ulp"]=std::max<ULong64_t>(stats["identity_max_dz_ulp"],du);
   stats["identity_max_xy_ulp"]=std::max<ULong64_t>(stats["identity_max_xy_ulp"],xu);
   identity.push_back(found);
  }
  std::vector<GenTrack>pool;std::unordered_set<unsigned>represented;
  for(const auto&p:packed){if(p.charge()==0)continue;auto ref=p.lastPrunedRef();if(ref.isNonnull()&&ref.isAvailable()&&ref->status()==1){represented.insert(ref.key());if(hadron(ref->pdgId()))pool.push_back({ref.get()});}else if(hadron(p.pdgId()))pool.push_back({&p});}
  for(unsigned j=0;j<pruned.size();++j){const auto&p=pruned[j];if(p.status()==1&&p.charge()!=0&&hadron(p.pdgId())&&!represented.count(j))pool.push_back({&p});}
  constexpr int nPhi=180;const double twoPi=2*std::acos(-1.);std::map<std::pair<int,int>,std::vector<unsigned>>cells;
  auto phiCell=[&](double v){double p=std::fmod(v,twoPi);if(p<0)p+=twoPi;return std::min(nPhi-1,int(p/twoPi*nPhi));};
  for(unsigned j=0;j<pool.size();++j){auto p=pool[j].p;if(std::isfinite(p->eta())&&std::isfinite(p->phi()))cells[{int(std::floor(p->eta()/.04)),phiCell(p->phi())}].push_back(j);}
  auto distance=[&](const Track&t,const reco::Candidate*p){double dp=std::remainder(p->phi()-t.phi,twoPi),de=p->eta()-t.eta;return dp*dp+de*de;};
  std::unordered_map<int,int>selected;
  for(int rid:identity){if(selected.count(rid))continue;const auto&t=raw[rid];int best=-1;double dmax=.03*.03;std::vector<unsigned>nearby;if(std::isfinite(t.eta)&&std::isfinite(t.phi)&&t.pt>0){int e=int(std::floor(t.eta/.04)),p=phiCell(t.phi);for(int de=-1;de<=1;++de)for(int dp=-1;dp<=1;++dp){auto f=cells.find({e+de,(p+dp+nPhi)%nPhi});if(f!=cells.end())nearby.insert(nearby.end(),f->second.begin(),f->second.end());}std::sort(nearby.begin(),nearby.end());for(unsigned j:nearby){auto p=pool[j].p;if(p->charge()!=t.charge||p->pt()<=0||std::abs(t.pt-p->pt())/t.pt>=.5)continue;double d=distance(t,p);if(d<dmax){dmax=d;best=j;}}}
   if(audit&&stats["audited_track_matches"]<64){int reference=-1;double d=.03*.03;for(unsigned j=0;j<pool.size();++j){auto p=pool[j].p;if(t.pt<=0||p->charge()!=t.charge||p->pt()<=0||std::abs(t.pt-p->pt())/t.pt>=.5)continue;double dd=distance(t,p);if(dd<d){reference=j;d=dd;}}if(reference!=best)throw std::runtime_error("Grid nearest match disagrees with full GEN scan");++stats["audited_track_matches"];}
   selected[rid]=best;
  }
  for(int c=0;c<n;++c){const auto&cnd=input.candidates[c];auto finish=[&](){bool old=cnd.oldMatch&&!cnd.oldSwap,newSignal=rb.at("matchGEN")[c]&&!rb.at("isSwap")[c];stats["old_matchGEN"]+=cnd.oldMatch;stats["new_matchGEN"]+=rb.at("matchGEN")[c];stats["new_matchHYDJET"]+=rb.at("matchHYDJET")[c];stats["old_signal_nonswap"]+=old;stats["new_signal_nonswap"]+=newSignal;stats["gained_signal_nonswap"]+=newSignal&&!old;stats["lost_signal_nonswap"]+=old&&!newSignal;stats["new_swaps"]+=rb.at("isSwap")[c];};
   int p=selected.at(identity[cnd.positive]),m=selected.at(identity[cnd.negative]);if(p<0||m<0){ri.at("matchGen_reason")[c]=2;finish();continue;}auto a=pool[p].p,b=pool[m].p;if(a==b){ri.at("matchGen_reason")[c]=3;finish();continue;}int i=std::abs(a->pdgId()),j=std::abs(b->pdgId());if(!((i==321&&j==211)||(i==211&&j==321))){ri.at("matchGen_reason")[c]=4;finish();continue;}auto d0=mother(a);if(!d0||d0!=mother(b)||std::abs(d0->pdgId())!=421){ri.at("matchGen_reason")[c]=5;finish();continue;}
   auto d=dynamic_cast<const reco::GenParticle*>(d0);if(!d||!prunedIndex.count(d0))throw std::runtime_error("Matched D0 is absent from pruned GEN");auto kaon=i==321?a:b,pion=i==211?a:b;auto s=topology(d0);bool valid=s.nonGamma==2&&s.kaon&&s.pion&&s.gamma<=1;
   rb.at("matchGEN")[c]=valid&&d->collisionId()==0;rb.at("matchHYDJET")[c]=valid&&d->collisionId()!=0;rb.at("isSwap")[c]=std::abs((cnd.kaonPositive?a:b)->pdgId())!=321;rb.at("matchGen_validD0chain")[c]=valid&&s.gamma==0;
   ri.at("matchGen_index")[c]=genIndex.count(d0)?genIndex.at(d0):-1;ri.at("matchGen_pruned_index")[c]=prunedIndex.at(d0);ri.at("matchGen_collisionId")[c]=d->collisionId();ri.at("matchGen_reason")[c]=d->collisionId()==0?10:11;
   rf.at("matchGen_deltaR")[c]=std::sqrt(std::pow(d0->eta()-cnd.eta,2)+std::pow(std::remainder(d0->phi()-cnd.phi,twoPi),2));particle(rf,ri,"matchGen_D0",c,d0,false);particle(rf,ri,"matchGen_D0Dau1",c,kaon,true);particle(rf,ri,"matchGen_D0Dau2",c,pion,true);
   auto anc=ancestor(d0);ri.at("matchGen_D1ancestorId_")[c]=anc.first;ri.at("matchGen_D1ancestorFlavor_")[c]=anc.second;
   auto mom=differentMother(d0);if(mom)ri.at("idmom_reco")[c]=mom->pdgId();auto ancMom=mom;for(int depth=0;ancMom&&depth<50;++depth){ancMom=differentMother(ancMom);if(ancMom&&(std::abs(ancMom->pdgId())%1000)/100==5){ri.at("idBAnc_reco")[c]=ancMom->pdgId();break;}}
   for(int leg=1;leg<=2;++leg){auto leaf=leg==1?kaon:pion,mm=mother(leaf);std::string pre="matchGen_D0Dau"+std::to_string(leg);ri.at(pre+"_motherPdgId")[c]=mm?mm->pdgId():-99;ri.at(pre+"_motherNDau")[c]=mm?mm->numberOfDaughters():-99;}
   if(d0->numberOfDaughters()&&d0->daughter(0)){auto v=d0->daughter(0)->vertex(),o=d0->vertex();TVector3 delta(v.x()-o.x(),v.y()-o.y(),v.z()-o.z()),momentum(d0->px(),d0->py(),d0->pz());rf.at("matchGen3DPointingAngle")[c]=momentum.Angle(delta);rf.at("matchGen3DDecayLength")[c]=delta.Mag();delta.SetZ(0);momentum.SetZ(0);rf.at("matchGen2DPointingAngle")[c]=momentum.Angle(delta);rf.at("matchGen2DDecayLength")[c]=delta.Mag();}
   if(valid&&(!signalOnlyGenMatching||d->collisionId()==0)&&ri.at("matchGen_index")[c]<0)throw std::runtime_error("Matched valid D0 absent from regenerated GEN denominator");finish();
  }
  if(tree->Fill()<0)throw std::runtime_error("ROOT friend Fill failed");++stats["events"];stats["candidates"]+=n;stats["gen_candidates"]+=ng;stats["old_gen_candidates"]+=oldNg;stats["identity_tracks"]+=identity.size();
 }
 void close(Long64_t expected,const std::string&summary){if(Long64_t(stats.at("events"))!=expected)throw std::runtime_error("Missing source events");file->cd("d0ana_newreduced");tree->Write();file->cd();TObjString(summary.c_str()).Write("summary");file->Flush();if(ownedFile)file->Close();}
};
}
