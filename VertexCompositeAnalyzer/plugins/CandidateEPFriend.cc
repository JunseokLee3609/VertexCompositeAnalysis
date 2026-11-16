// Candidate-specific tracker EP contributions; additive friend for existing ntuples.
// GEN DCA semantics: VertexCompositeAnalysis baebad240f9764d2f3a3cbd27da909342e327c2a.
#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "FWCore/Framework/interface/one/EDAnalyzer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/Utilities/interface/Exception.h"
#include "CommonTools/UtilAlgos/interface/TFileService.h"
#include "DataFormats/TrackReco/interface/Track.h"
#include "DataFormats/VertexReco/interface/Vertex.h"
#include "DataFormats/BeamSpot/interface/BeamSpot.h"
#include "DataFormats/PatCandidates/interface/PackedGenParticle.h"
#include "DataFormats/HepMCCandidate/interface/GenParticle.h"
#include "DataFormats/Math/interface/Point3D.h"
#include "TFile.h"
#include "TTree.h"
#include "TLeaf.h"
#include "TObjString.h"
#include "TNamed.h"
#include "TVector3.h"

namespace candidateep {
constexpr unsigned NS=9;
using Q=std::array<double,NS*3>;
using EventKey=std::tuple<unsigned,unsigned,unsigned>;
uint32_t bits(float v){uint32_t b;std::memcpy(&b,&v,4);return b;}
uint64_t ulp(float a,float b){
 if(a==b)return 0;if(!std::isfinite(a)||!std::isfinite(b))return UINT64_MAX;
 auto ordered=[](float v){auto b=bits(v);return b&0x80000000U?~b:b|0x80000000U;};
 uint64_t x=ordered(a),y=ordered(b);return x>y?x-y:y-x;
}
const reco::Candidate* d0Of(const reco::Candidate* p){
 if(!p)return nullptr;if(std::abs(p->pdgId())==421)return p;
 for(unsigned i=0;i<p->numberOfDaughters();++i)if(std::abs(p->daughter(i)->pdgId())==421)return p->daughter(i);
 return nullptr;
}
std::vector<const reco::Candidate*> leavesOf(const reco::Candidate* p){
 std::vector<const reco::Candidate*> out;
 const auto*d=d0Of(p);if(!d)return out;
 for(unsigned i=0;i<d->numberOfDaughters();++i){
  auto*c=d->daughter(i);int id=std::abs(c->pdgId());if(id==321||id==211)out.push_back(c);
 }
 if(std::abs(p->pdgId())==413)for(unsigned i=0;i<p->numberOfDaughters();++i)
  if(std::abs(p->daughter(i)->pdgId())==211)out.push_back(p->daughter(i));
 return out;
}
struct Input {
 TTree*t=nullptr;std::map<EventKey,Long64_t> index;std::set<Long64_t> seen;
 std::map<std::string,TLeaf*> leaf;bool star;
 Input(TFile*f,const std::string&path,bool s):star(s){
  t=dynamic_cast<TTree*>(f->Get(path.c_str()));if(!t)throw cms::Exception("FriendInput")<<"Missing "<<path;
  t->SetBranchStatus("*",0);
  for(auto*b:*t->GetListOfBranches()){
   auto*br=static_cast<TBranch*>(b);std::string n=br->GetName();
   bool keep=n=="RunNb"||n=="LSNb"||n=="EventNb"||n=="candSize"||n=="candSize_gen"||n=="flavor"||
    n.find("pTerr")!=std::string::npos||n.find("NHit")!=std::string::npos||
    n.find("HighPurity")!=std::string::npos||n.find("DCASignificance")!=std::string::npos||
    n.rfind("gen_",0)==0||n.rfind("matchGen_",0)==0||n=="matchGEN"||n=="matchHYDJET";
   if(keep){t->SetBranchStatus(n.c_str(),1);leaf[n]=static_cast<TLeaf*>(br->GetListOfLeaves()->At(0));}
  }
  for(Long64_t e=0;e<t->GetEntries();++e){
   t->GetEntry(e);EventKey k{unsigned(v("RunNb")),unsigned(v("LSNb")),unsigned(v("EventNb"))};
   if(!index.emplace(k,e).second)throw cms::Exception("FriendInput")<<"Duplicate stored event key "<<path;
  }
 }
 bool has(const std::string&n)const{return leaf.count(n);}
 double v(const std::string&n,int i=0)const{
  auto p=leaf.find(n);if(p==leaf.end())throw cms::Exception("FriendInput")<<"Missing branch "<<n;
  return p->second->GetValue(i);
 }
 Long64_t load(EventKey k){
  auto p=index.find(k);if(p==index.end())return -1;
  if(!seen.insert(p->second).second)throw cms::Exception("FriendInput")<<"Repeated source entry";
  t->GetEntry(p->second);return p->second;
 }
};
struct Output {
 TTree*t;unsigned run=0,ls=0;ULong64_t event=0;Long64_t entry=0;
 int n=0,ng=0;std::vector<float> dx,dy,dw,gdx,gdy,gdw,dca,gdca;
 std::vector<int> valid,gvalid,gindex,matchedD0,removed,gremoved,status,gstatus,dcaValid,gdcaValid;
 Output(TFileDirectory d){
  t=d.make<TTree>("CandidateEPFriend","Candidate-specific EP deltas and baebad2 GEN D0 DCA");
  t->Branch("RunNb",&run);t->Branch("LSNb",&ls);t->Branch("EventNb64",&event);t->Branch("source_entry",&entry);
  t->Branch("candSize",&n);t->Branch("candSize_gen",&ng);
  t->Branch("epDeltaQx",&dx);t->Branch("epDeltaQy",&dy);t->Branch("epDeltaW",&dw);
  t->Branch("epValid",&valid);t->Branch("epIdentityStatus",&status);t->Branch("epNRemoved",&removed);
  t->Branch("gen_epDeltaQx",&gdx);t->Branch("gen_epDeltaQy",&gdy);t->Branch("gen_epDeltaW",&gdw);
  t->Branch("gen_epValid",&gvalid);t->Branch("gen_epAssociationStatus",&gstatus);t->Branch("gen_epNRemoved",&gremoved);
  t->Branch("gen_pruned_index",&gindex);t->Branch("matchedD0_pruned_index",&matchedD0);
  t->Branch("genDca3D",&dca);t->Branch("gen_D0dca3D",&gdca);
  t->Branch("genDca3DValid",&dcaValid);t->Branch("gen_D0dca3DValid",&gdcaValid);
 }
 void reset(int nr,int ngr){
  n=nr;ng=ngr;dx.assign(n*NS,NAN);dy=dw=dx;gdx.assign(ng*NS,NAN);gdy=gdw=gdx;
  valid.assign(n,0);status.assign(n,0);removed.assign(n,0);
  gvalid.assign(ng,0);gstatus.assign(ng,0);gremoved.assign(ng,0);
  dca.assign(n,NAN);gdca.assign(ng,NAN);dcaValid.assign(n,0);gdcaValid.assign(ng,0);gindex.assign(ng,-1);matchedD0.assign(n,-1);
 }
 void put(int i,const Q&q,bool gen){
  auto&x=gen?gdx:dx;auto&y=gen?gdy:dy;auto&w=gen?gdw:dw;
  for(unsigned s=0;s<NS;++s){x[i*NS+s]=q[3*s];y[i*NS+s]=q[3*s+1];w[i*NS+s]=q[3*s+2];}
 }
};
struct GenMatcher {
 std::vector<const reco::Candidate*> pool;
 std::map<std::pair<int,int>,std::vector<unsigned>> cells;
 static int phiCell(double phi){double x=std::fmod(phi,2*M_PI);if(x<0)x+=2*M_PI;return std::min(179,int(x/(2*M_PI)*180));}
 GenMatcher(const reco::GenParticleCollection&g,const pat::PackedGenParticleCollection&p){
  std::unordered_set<unsigned>represented;
  auto had=[](int x){x=std::abs(x);return x==211||x==321||x==2212;};
  for(const auto&v:p){
   if(!v.charge())continue;auto r=v.lastPrunedRef();
   if(r.isNonnull()&&r.isAvailable()&&r->status()==1){represented.insert(r.key());if(had(r->pdgId()))pool.push_back(r.get());}
   else if(had(v.pdgId()))pool.push_back(&v);
  }
  for(unsigned i=0;i<g.size();++i)if(g[i].status()==1&&g[i].charge()&&had(g[i].pdgId())&&!represented.count(i))pool.push_back(&g[i]);
  for(unsigned i=0;i<pool.size();++i)if(std::isfinite(pool[i]->eta())&&std::isfinite(pool[i]->phi()))
   cells[{int(std::floor(pool[i]->eta()/.04)),phiCell(pool[i]->phi())}].push_back(i);
 }
 const reco::Candidate* find(const reco::Track&t)const{
  if(!(t.pt()>0)||!std::isfinite(t.eta())||!std::isfinite(t.phi()))return nullptr;
  int eta=int(std::floor(t.eta()/.04)),phi=phiCell(t.phi());std::vector<unsigned>near;
  for(int de=-1;de<=1;++de)for(int dp=-1;dp<=1;++dp){
   auto p=cells.find({eta+de,(phi+dp+180)%180});if(p!=cells.end())near.insert(near.end(),p->second.begin(),p->second.end());
  }
  std::sort(near.begin(),near.end());double best=.03*.03;const reco::Candidate* result=nullptr;
  for(unsigned i:near){
   auto*p=pool[i];if(p->charge()!=t.charge()||p->pt()<=0||std::abs(t.pt()-p->pt())/t.pt()>=.5)continue;
   double de=p->eta()-t.eta(),dp=std::remainder(p->phi()-t.phi(),2*M_PI),d=de*de+dp*dp;
   if(d<best){best=d;result=p;}
  }
  return result;
 }
};
} // namespace candidateep

class CandidateEPFriend:public edm::one::EDAnalyzer<edm::one::SharedResources>{
 using Q=candidateep::Q;using Input=candidateep::Input;using Output=candidateep::Output;
 edm::EDGetTokenT<reco::TrackCollection> tracks_;
 edm::EDGetTokenT<reco::VertexCollection> vertices_;
 edm::EDGetTokenT<reco::BeamSpot> beam_;
 edm::EDGetTokenT<reco::GenParticleCollection> gen_;
 edm::EDGetTokenT<pat::PackedGenParticleCollection> packed_;
 edm::EDGetTokenT<math::XYZPointF> origin_;
 std::unique_ptr<TFile> input_;std::array<std::unique_ptr<Input>,2> readers_;
 std::array<std::unique_ptr<Output>,2> writers_;
 TTree*events_;unsigned run_,ls_;ULong64_t event_;bool signalPVValid_;math::XYZPointF signalPV_;
 std::array<float,27>all_,allNew_;float original_[27];Long64_t sourceEntry_;
 bool complete_,direct_;std::string report_;unsigned long long processed_=0,invalid_=0,genInvalid_=0,directTests_=0;
 double maxClosure_=0,maxStoredQ_=0;unsigned long long storedBad_=0;
 unsigned long long matchedReco_=0,validRecoDca_=0,storedGen_=0,validGenDca_=0;
 bool genDca(const reco::Candidate&,float&)const;
 Q contribution(const reco::Track&,const reco::Vertex&,bool)const;
 public:
 explicit CandidateEPFriend(const edm::ParameterSet&c):
 tracks_(consumes<reco::TrackCollection>(c.getParameter<edm::InputTag>("tracks"))),
 vertices_(consumes<reco::VertexCollection>(c.getParameter<edm::InputTag>("vertices"))),
 beam_(consumes<reco::BeamSpot>(c.getParameter<edm::InputTag>("beamspot"))),
 gen_(consumes<reco::GenParticleCollection>(c.getParameter<edm::InputTag>("gen"))),
 packed_(consumes<pat::PackedGenParticleCollection>(c.getParameter<edm::InputTag>("packedGen"))),
 origin_(consumes<math::XYZPointF>(c.getParameter<edm::InputTag>("genOrigin"))),
 complete_(c.getParameter<bool>("requireComplete")),direct_(c.getParameter<bool>("directCheck")),
 report_(c.getParameter<std::string>("report")){
  usesResource(TFileService::kSharedResource);
  input_.reset(TFile::Open(c.getParameter<std::string>("ntuple").c_str()));
  if(!input_||input_->IsZombie())throw cms::Exception("FriendInput")<<"Cannot open source ntuple";
  readers_[0]=std::make_unique<Input>(input_.get(),"d0ana_newreduced/PATCompositeNtuple",false);
  readers_[1]=std::make_unique<Input>(input_.get(),"dStarana_mc/PATCompositeNtuple",true);
  edm::Service<TFileService>fs;
  fs->make<TNamed>("provenance",c.getParameter<std::string>("provenance").c_str());
  writers_[0]=std::make_unique<Output>(fs->mkdir("d0"));writers_[1]=std::make_unique<Output>(fs->mkdir("dstar"));
  events_=fs->make<TTree>("EventQ","All-track Q sums; indices full,+eta,-eta for abs eta 2.4,1.6,0.8");
  events_->Branch("RunNb",&run_);events_->Branch("LSNb",&ls_);events_->Branch("EventNb64",&event_);
  events_->Branch("source_entry",&sourceEntry_);
  events_->Branch("allQ",all_.data(),"allQ[27]/F");
  events_->Branch("allQ_currentDxy_diagnosticOnly",allNew_.data(),"allQ_currentDxy[27]/F");
  events_->Branch("originalAllQ",original_,"originalAllQ[27]/F");
  events_->Branch("genSignalPrimaryVertexValid",&signalPVValid_);
 }
 void analyze(const edm::Event&,const edm::EventSetup&)override;
 void endJob()override;
};

candidateep::Q CandidateEPFriend::contribution(const reco::Track&t,const reco::Vertex&v,bool corrected)const{
 Q q{};
 const float ex=v.xError(),ey=v.yError(),ez=v.zError();
 const math::XYZPoint point(float(v.x()),float(v.y()),float(v.z()));
 const float dz=t.dz(point),dxy=t.dxy(point);
 const float dzerr=std::sqrt(t.dzError()*t.dzError()+ez*ez);
 const float xyerr=corrected?t.dxyError(point,v.covariance()):std::sqrt(t.dxyError()*t.dxyError()+ex*ey);
 if(!t.quality(reco::TrackBase::highPurity)||std::abs(t.ptError())/t.pt()>.10||
    std::abs(dz/dzerr)>3||std::abs(dxy/xyerr)>3||t.pt()<=.3||t.pt()>=3||std::abs(t.eta())>=2.4)return q;
 const float pt=t.pt(),phi=t.phi(),eta=t.eta();
 const double x=pt*std::cos(2*phi),y=pt*std::sin(2*phi);
 for(unsigned k=0;k<3;++k){
  const double limit=k==0?2.4:k==1?1.6:.8;if(std::abs(eta)>=limit)continue;
  for(unsigned h=0;h<3;++h){
   if(h==1&&!(eta>.05&&eta<limit))continue;
   if(h==2&&!(eta<-.05&&eta>-limit))continue;
   unsigned j=3*(3*k+h);q[j]=x;q[j+1]=y;q[j+2]=pt;
  }
 }
 return q;
}
bool CandidateEPFriend::genDca(const reco::Candidate& d0, float& value) const {
  value = std::numeric_limits<float>::quiet_NaN();
  if (std::abs(d0.pdgId()) != 421) return false;
  const auto* genD0 = dynamic_cast<const reco::GenParticle*>(&d0);
  if (!genD0 || genD0->collisionId() < 0) return false;

  math::XYZPoint origin;
  if (genD0->collisionId() == 0) {
    if (!signalPVValid_) return false;
    origin = math::XYZPoint(signalPV_.x(), signalPV_.y(), signalPV_.z());
  } else {
    // HYDJET and PYTHIA vertices can be stored in different coordinate origins.
    // A prompt GEN ancestor provides the HYDJET origin without removing B flight.
    std::vector<const reco::Candidate*> pending{&d0};
    std::unordered_set<const reco::Candidate*> seen;
    bool found = false;
    constexpr double vertexToleranceCm = 1.e-5;
    while (!pending.empty()) {
      const auto* candidate = pending.back();
      pending.pop_back();
      if (!candidate || !seen.insert(candidate).second) continue;
      if (seen.size() > 1000) return false;
      const auto* particle = dynamic_cast<const reco::GenParticle*>(candidate);
      if (!particle || particle->collisionId() <= 0) return false;
      if (particle->statusFlags().isPrompt()) {
        if (!std::isfinite(particle->vx()) || !std::isfinite(particle->vy()) ||
            !std::isfinite(particle->vz())) return false;
        const math::XYZPoint vertex(particle->vx(), particle->vy(), particle->vz());
        if (found && (vertex - origin).R() > vertexToleranceCm) return false;
        origin = vertex;
        found = true;
        continue;
      }
      if (particle->numberOfMothers() == 0) return false;
      for (size_t i = 0; i < particle->numberOfMothers(); ++i) {
        const auto& mother = particle->motherRef(i);
        if (mother.isNull() || !mother.isAvailable()) return false;
        pending.push_back(mother.get());
      }
    }
    if (!found) return false;
  }
  if (!std::isfinite(d0.vx()) || !std::isfinite(d0.vy()) || !std::isfinite(d0.vz()) ||
      !std::isfinite(d0.px()) || !std::isfinite(d0.py()) || !std::isfinite(d0.pz())) return false;

  // Neutral D0 trajectory to its GEN collision vertex, in cm, also for D* daughters.
  const TVector3 displacement(d0.vx() - origin.x(), d0.vy() - origin.y(), d0.vz() - origin.z());
  const TVector3 momentum(d0.px(), d0.py(), d0.pz());
  const double p = momentum.Mag();
  if (!(p > 0.) || !std::isfinite(p)) return false;
  const double dca = displacement.Cross(momentum * (1. / p)).Mag();
  if (!std::isfinite(dca) || dca > std::numeric_limits<float>::max()) return false;
  value = static_cast<float>(dca);
  return true;
}


void CandidateEPFriend::analyze(const edm::Event&e,const edm::EventSetup&){
 using namespace candidateep;
 run_=e.id().run();ls_=e.luminosityBlock();event_=e.id().event();
 EventKey key{run_,ls_,unsigned(event_)};
 std::array<Long64_t,2>entries{{readers_[0]->load(key),readers_[1]->load(key)}};
 if(entries[0]<0&&entries[1]<0)return;
 if(entries[0]<0||entries[1]<0)throw cms::Exception("FriendAlignment")<<"D0/Dstar event lists differ";
 sourceEntry_=entries[0];
 if(entries[0]!=Long64_t(processed_)||entries[1]!=Long64_t(processed_))
  throw cms::Exception("FriendAlignment")<<"Source entry order differs from MiniAOD order";

 const auto&tracks=e.get(tracks_);const auto&vertices=e.get(vertices_);const auto&bs=e.get(beam_);
 reco::Vertex epPV;
 if(!vertices.empty()&&!vertices[0].isFake()&&vertices[0].tracksSize()>=2)epPV=vertices[0];
 else epPV=reco::Vertex(bs.position(),bs.covariance3D());
 const bool usePV=!vertices.empty()&&!vertices[0].isFake()&&vertices[0].tracksSize()>=2;
 const math::XYZPoint candPoint(usePV?vertices[0].x():bs.x0(),usePV?vertices[0].y():bs.y0(),usePV?vertices[0].z():0.);
 const double cx=usePV?vertices[0].xError():bs.BeamWidthX(),cy=usePV?vertices[0].yError():bs.BeamWidthY(),cz=usePV?vertices[0].zError():0.;
 const auto&gen=e.get(gen_);const auto&packed=e.get(packed_);
 auto origin=e.getHandle(origin_);signalPVValid_=origin.isValid()&&std::isfinite(origin->x())&&std::isfinite(origin->y())&&std::isfinite(origin->z());
 if(signalPVValid_)signalPV_=*origin;
 std::vector<Q>contrib;contrib.reserve(tracks.size());Q sum{};
 all_.fill(0);allNew_.fill(0);
 std::map<std::tuple<int,uint32_t,int>,std::vector<int>>lookup;
 std::vector<std::pair<float,float>>sig;
 GenMatcher matcher(gen,packed);
 std::unordered_map<const reco::Candidate*,std::vector<int>>truthTracks;
 for(unsigned i=0;i<tracks.size();++i){
  const auto&t=tracks[i];auto q=contribution(t,epPV,false),qn=contribution(t,epPV,true);contrib.push_back(q);
  for(unsigned j=0;j<27;++j){sum[j]+=q[j];all_[j]+=q[j];allNew_[j]+=qn[j];}
  lookup[{int(t.charge()),bits(float(t.ptError())),int(t.numberOfValidHits())}].push_back(i);
  sig.emplace_back(float(t.dz(candPoint)/std::sqrt(t.dzError()*t.dzError()+cz*cz)),
                   float(t.dxy(candPoint)/std::sqrt(t.dxyError()*t.dxyError()+cx*cy)));
  if(q[2]>0){auto*g=matcher.find(t);if(g)truthTracks[g].push_back(i);}
 }
 std::fill_n(original_,27,NAN);
 auto*et=dynamic_cast<TTree*>(input_->Get("eventplane/EventPlane"));
 if(et){
  et->GetEntry(sourceEntry_);
  const std::array<std::string,9>suffix{{"","_forw","_afterw","_eta16","_forw_eta16","_afterw_eta16","_eta08","_forw_eta08","_afterw_eta08"}};
  bool bad=false;
  for(unsigned s=0;s<9;++s){
   auto value=[&](const std::string&name){auto*b=et->GetBranch(name.c_str());if(!b)throw cms::Exception("FriendInput")<<name;return static_cast<TLeaf*>(b->GetListOfLeaves()->At(0))->GetValue();};
   double w=value("all_trkW"+suffix[s]),x=value("all_trkQx"+suffix[s]),y=value("all_trkQy"+suffix[s]);
   original_[3*s]=x*w;original_[3*s+1]=y*w;original_[3*s+2]=w;
   if(w>0&&all_[3*s+2]>0){
    double d=std::max(std::abs(x-all_[3*s]/all_[3*s+2]),std::abs(y-all_[3*s+1]/all_[3*s+2]));
    maxStoredQ_=std::max(maxStoredQ_,d);if(d>2e-6||std::abs(w-all_[3*s+2])>1e-3)bad=true;
   }else if(w!=all_[3*s+2])bad=true;
  }
  if(bad)++storedBad_;
 }
 // Exact float32 GEN kinematics are used only to locate the already-stored GEN particle.
 auto locate=[&](const Input&r,int i,const std::string&prefix,int pid)->int{
  const std::string pt=prefix+"pT",eta=prefix+"eta",phi=prefix+"phi";
  if(!r.has(pt)||!r.has(eta)||!r.has(phi))return -1;
  std::vector<int>found;
  for(unsigned j=0;j<gen.size();++j){
   const auto&p=gen[j];if(std::abs(p.pdgId())!=pid)continue;
   if(ulp(float(p.pt()),float(r.v(pt,i)))<=2&&ulp(float(p.eta()),float(r.v(eta,i)))<=2&&
      ulp(float(p.phi()),float(r.v(phi,i)))<=2)found.push_back(j);
  }
  return found.size()==1?found[0]:-1;
 };
 unsigned checksLeft=0;
 auto collect=[&](const std::set<int>&ids)->Q{
  Q q{};for(int id:ids)for(unsigned j=0;j<27;++j)q[j]+=contrib[id][j];
  if(direct_&&checksLeft){
   --checksLeft;
   Q direct{};for(unsigned i=0;i<contrib.size();++i)if(!ids.count(i))for(unsigned j=0;j<27;++j)direct[j]+=contrib[i][j];
   for(unsigned j=0;j<27;++j)maxClosure_=std::max(maxClosure_,std::abs((sum[j]-q[j])-direct[j]));
   ++directTests_;
  }
  return q;
 };
 for(unsigned type=0;type<2;++type){
  auto&r=*readers_[type];auto&o=*writers_[type];
  o.run=run_;o.ls=ls_;o.event=event_;o.entry=entries[type];o.reset(int(r.v("candSize")),int(r.v("candSize_gen")));
  checksLeft=2;
  for(int i=0;i<o.n;++i){
   std::set<int>ids;bool valid=true;
   for(unsigned leg=0;leg<(type?3u:2u);++leg){
    std::string pe,nh,dz,xy,hp;int charge;
    if(type&&leg<2){
     auto s=std::to_string(leg+1);pe="pTerrGrandD"+s;nh="NHitGrandD"+s;dz="zDCASignificanceGrandDaugther"+s;
     xy="xyDCASignificanceGrandDaugther"+s;hp="HighPurityGrandDaugther"+s;charge=leg==0?1:-1;
    }else{
     auto s=std::to_string(type?2:leg+1);pe="pTerrD"+s;nh="NHitD"+s;dz="zDCASignificanceDaugther"+s;
     xy="xyDCASignificanceDaugther"+s;hp="HighPurityDaugther"+s;charge=type?int(r.v("flavor",i)):(leg==0?1:-1);
    }
    auto it=lookup.find({charge,bits(float(r.v(pe,i))),int(r.v(nh,i))});
    std::vector<int>bucket;
    if(it!=lookup.end())for(int id:it->second)if(bool(tracks[id].quality(reco::TrackBase::highPurity))==bool(r.v(hp,i)))bucket.push_back(id);
    auto equivalent=[&](const std::vector<int>&v){
     if(v.empty())return false;for(int id:v)if(contrib[id]!=contrib[v[0]]||tracks[id].pt()!=tracks[v[0]].pt()||tracks[id].eta()!=tracks[v[0]].eta()||tracks[id].phi()!=tracks[v[0]].phi())return false;return true;
    };
    if(!equivalent(bucket)){
     std::vector<int>precise;for(int id:bucket)if(ulp(sig[id].first,float(r.v(dz,i)))<=2&&ulp(sig[id].second,float(r.v(xy,i)))<=2)precise.push_back(id);bucket.swap(precise);
    }
    if(!equivalent(bucket)){valid=false;break;}
    ids.insert(bucket.front());
   }
   if(valid){o.put(i,collect(ids),false);o.valid[i]=1;o.status[i]=1;for(int id:ids)if(contrib[id][2]>0)++o.removed[i];}
   else {++invalid_;o.status[i]=-1;}
   bool matched=r.has("matchGEN")&&r.v("matchGEN",i);if(r.has("matchHYDJET"))matched|=bool(r.v("matchHYDJET",i));
   if(matched){++matchedReco_;int g=locate(r,i,"matchGen_D0",421);o.matchedD0[i]=g;if(g>=0)o.dcaValid[i]=genDca(gen[g],o.dca[i]);validRecoDca_+=o.dcaValid[i];}
  }
  checksLeft=2;
  for(int i=0;i<o.ng;++i){
   ++storedGen_;int g=locate(r,i,"gen_",type?413:421);o.gindex[i]=g;
   if(g<0){++genInvalid_;o.gstatus[i]=-1;continue;}
   auto*d=d0Of(&gen[g]);if(d)o.gdcaValid[i]=genDca(*d,o.gdca[i]);validGenDca_+=o.gdcaValid[i];
   auto leaves=leavesOf(&gen[g]);if(leaves.size()!=(type?3u:2u)){++genInvalid_;o.gstatus[i]=-2;continue;}
   std::set<int>ids;
   for(auto*leaf:leaves){auto it=truthTracks.find(leaf);if(it!=truthTracks.end())ids.insert(it->second.begin(),it->second.end());}
   o.put(i,collect(ids),true);o.gvalid[i]=1;o.gstatus[i]=1;o.gremoved[i]=ids.size();
  }
  o.t->Fill();
 }
 events_->Fill();++processed_;
}
void CandidateEPFriend::endJob(){
 std::ofstream f(report_);
 f<<"{\"processed_events\":"<<processed_<<",\"d0_source_events\":"<<readers_[0]->index.size()
  <<",\"dstar_source_events\":"<<readers_[1]->index.size()<<",\"invalid_reco_identity\":"<<invalid_
  <<",\"invalid_gen_identity\":"<<genInvalid_<<",\"direct_subtraction_tests\":"<<directTests_
  <<",\"max_direct_Q_difference\":"<<maxClosure_<<",\"stored_Q_mismatch_events\":"<<storedBad_
  <<",\"max_stored_normalized_Q_difference\":"<<maxStoredQ_
  <<",\"matched_reco\":"<<matchedReco_<<",\"valid_reco_DCA\":"<<validRecoDca_
  <<",\"stored_gen\":"<<storedGen_<<",\"valid_gen_DCA\":"<<validGenDca_<<"}\n";f.close();
 if(maxClosure_>1e-8||storedBad_||invalid_||genInvalid_||
    (complete_&&(readers_[0]->seen.size()!=readers_[0]->index.size()||readers_[1]->seen.size()!=readers_[1]->index.size())))
  throw cms::Exception("FriendValidation")<<"Validation failed: see "<<report_;
}
DEFINE_FWK_MODULE(CandidateEPFriend);
