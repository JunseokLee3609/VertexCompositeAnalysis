#include "OriginalNtuple.h"
#include "FrozenNativeGenSelection.h"
#include <fstream>
#include <chrono>
#include <cstring>
#include <algorithm>
#include <unordered_map>
#include <sstream>
#include <limits>
#include "FWCore/Framework/interface/one/EDAnalyzer.h"
#include "FWCore/Framework/interface/one/EDProducer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/Run.h"
#include "DataFormats/Common/interface/TriggerResults.h"
#include "FWCore/Common/interface/TriggerNames.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/Utilities/interface/Exception.h"
#include "CommonTools/UtilAlgos/interface/TFileService.h"
#include "DataFormats/TrackReco/interface/Track.h"
#include "DataFormats/VertexReco/interface/Vertex.h"
#include "DataFormats/BeamSpot/interface/BeamSpot.h"
#include "DataFormats/HepMCCandidate/interface/GenParticle.h"
#include "DataFormats/HeavyIonEvent/interface/EvtPlane.h"
#include "SimDataFormats/HiGenData/interface/GenHIEvent.h"
#include "SimDataFormats/GeneratorProducts/interface/GenEventInfoProduct.h"
#include "SimDataFormats/GeneratorProducts/interface/GenRunInfoProduct.h"
#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"
#include "TObjString.h"
namespace {
uint32_t fbits(float x){uint32_t b;std::memcpy(&b,&x,4);return b;}
const reco::Candidate* mom(const reco::Candidate*p){return p&&p->numberOfMothers()?p->mother(0):nullptr;}
std::pair<int,int> ancestry(const reco::Candidate*p){
 int id=0,fl=0;for(int n=0;p&&n<50;p=mom(p),++n)if(n){id=p->pdgId();auto s=std::to_string(std::abs(id));fl=s[0]-'0';if(s[0]=='5'||std::abs(id)<=40)break;}return {id,fl};
}
struct Daughters {const reco::Candidate *k=nullptr,*pi=nullptr,*d0=nullptr;int gamma=0,nonGamma=0;};
Daughters daughters(const reco::Candidate*p){Daughters o;if(!p)return o;for(unsigned i=0;i<p->numberOfDaughters();++i){auto*d=p->daughter(i);if(!d)continue;int id=std::abs(d->pdgId());if(id==22){++o.gamma;continue;}++o.nonGamma;if(id==321&&!o.k)o.k=d;if(id==211&&!o.pi)o.pi=d;if(id==421&&!o.d0)o.d0=d;}return o;}
bool goodD0(const reco::Candidate*p,const Daughters& d){return p&&d.k&&d.pi&&d.nonGamma==2&&d.gamma<=1&&p->numberOfDaughters()==unsigned(2+d.gamma);}
void four(float* x,const reco::Candidate*p){x[0]=p->pt();x[1]=p->eta();x[2]=p->phi();x[3]=p->mass();for(int i=0;i<4;++i)if(!std::isfinite(x[i]))throw cms::Exception("GenQ")<<"Nonfinite GEN four-vector";}
float norm(float x,float w){return w>0?x/w:std::numeric_limits<float>::quiet_NaN();}
}
class GenQInputIndex:public edm::one::EDProducer<edm::one::SharedResources>{
 unsigned long long index_=0;double sumW_=0,sumW2_=0;
 edm::EDGetTokenT<GenEventInfoProduct> generator_;
 public:explicit GenQInputIndex(const edm::ParameterSet&):generator_(consumes<GenEventInfoProduct>(edm::InputTag("generator"))){usesResource(TFileService::kSharedResource);produces<unsigned long long>();}
 void produce(edm::Event&e,const edm::EventSetup&)override{
  const auto&g=e.get(generator_);sumW_+=g.weight();sumW2_+=g.weight()*g.weight();e.put(std::make_unique<unsigned long long>(index_++));
 }
 void endJob()override{
  edm::Service<TFileService>fs;fs->file().cd();auto*t=new TTree("InputSummary","All input events before every event filter");
  t->Branch("events",&index_,"events/l");t->Branch("sumGeneratorWeight",&sumW_,"sumGeneratorWeight/D");t->Branch("sumGeneratorWeight2",&sumW2_,"sumGeneratorWeight2/D");t->Fill();
 }
};
DEFINE_FWK_MODULE(GenQInputIndex);
class D0DstarGenQ:public edm::one::EDAnalyzer<edm::one::SharedResources,edm::one::WatchRuns>{
 struct TrackerResult {float all[3][3]{},veto[3][3]{}; bool allValid[3]{},vetoValid[3]{}; unsigned nAll[3]{},nRemoved[3]{}; float replay=0;};
 struct Mapping{std::string raw,group,anRaw;int originalJob=0,manifestIndex=0,sourceIndex=0;};
 std::map<std::string,Mapping> mappings_;std::string input_,manifest_,summary_;
 std::unique_ptr<genq::OriginalNtuple> original_,anOriginal_;Mapping mapping_;
 bool partial_,signalOnly_;std::set<Long64_t> found_,anFound_;
 edm::EDGetTokenT<reco::TrackCollection> tracks_;
 edm::EDGetTokenT<reco::VertexCollection> vertices_;
 edm::EDGetTokenT<reco::BeamSpot> beam_;
 edm::EDGetTokenT<int> cent_;
 edm::EDGetTokenT<reco::EvtPlaneCollection> planes_;
 edm::EDGetTokenT<reco::GenParticleCollection> gen_;
 edm::EDGetTokenT<edm::GenHIEvent> truth_;
 edm::EDGetTokenT<GenEventInfoProduct> generator_;
 edm::EDGetTokenT<unsigned long long> inputIndex_;
 edm::EDGetTokenT<GenRunInfoProduct> runInfo_;
 edm::EDGetTokenT<edm::TriggerResults> decisions_;
 TTree *events_=nullptr,*d0s_=nullptr,*stars_=nullptr,*meta_=nullptr,*runs_=nullptr;
 unsigned metadataRun_=0;double sigmaPb_=-1,internalSigmaPb_=-1,filterEfficiency_=-1;bool validRunInfo_=false;
 unsigned run_=0,lumi_=0;ULong64_t event_=0,eventRow_=0,sourceEntry_=0;
 Long64_t originalEntry_=-1,anOriginalEntry_=-1;
 int hiBin_=-1,sourceIndex_=0,originalJob_=0,manifestIndex_=0,nD0_=0,nDstar_=0;
 float pv_[3]{},pthat_=0,truePsi_=0;
 double generatorWeight_=1;bool passCount_=false,anPassCount_=false;
 int anND0_=0,anNDstar_=0;
 bool passHLT_=false,passPV_=false,passCluster_=false,passHF_=false,passBasic_=false;
 TrackerResult d0Q_,anQ_;
 float hf_[3][3]{};bool hfValid_[3]{};
 ULong64_t genEvent_=0,genRow_=0,d0Link_=0;
 int prunedIndex_=0,pdg_=0,collision_=0,mother_=0,ancestorId_=0,ancestorFlavor_=0,ndau_=0,ngamma_=0;
 float p4_[4]{},k4_[4]{},pi4_[4]{},slow4_[4]{};
 int kaonPid_=0,pionPid_=0;
 ULong64_t nEvents_=0,nPass_=0,nFail_=0,nGenD0_=0,nGenDstar_=0,nReplay_=0,nLeaves_=0;
 ULong64_t anNPass_=0,anNReplay_=0;double anMaxReplay_=0;
 double maxReplay_=0;bool complete_=false;std::chrono::steady_clock::time_point start_;
 void initialize(){
  if(original_)return;
  auto at=input_.find("/store/");if(at==std::string::npos)throw cms::Exception("GenQ")<<"Input must identify one MiniAOD LFN";
  input_=input_.substr(at);auto q=input_.find('?');if(q!=std::string::npos)input_.resize(q);
  auto it=mappings_.find(input_);if(it==mappings_.end())throw cms::Exception("GenQ")<<"Unmapped input "<<input_;
  mapping_=it->second;sourceIndex_=mapping_.sourceIndex;originalJob_=mapping_.originalJob;manifestIndex_=mapping_.manifestIndex;
  original_=std::make_unique<genq::OriginalNtuple>(mapping_.raw);
  anOriginal_=std::make_unique<genq::OriginalNtuple>(mapping_.anRaw);
  edm::Service<TFileService> fs;fs->file().cd();
  std::ostringstream p;p<<"{\"schema\":\"D0DstarGenQ_2\",\"gen_reference_commit\":\"e23d40aea8141705123999bd1f6f238a0fb3e5b1\",\"input_lfn\":\""<<input_<<"\",\"raw_url\":\""<<mapping_.raw<<"\",\"group\":\""<<mapping_.group<<"\",\"signalOnlyGenMatching\":"<<(signalOnly_?"true":"false")<<",\"link_scope\":\"manifestIndex plus table-local rowId/eventRow\",\"q_sides\":[\"full\",\"positive_eta\",\"negative_eta\"],\"tracker_definition\":\"normalized sum pT cos(2phi),sin(2phi),sum_pT; original saved Q retained for count-passing events\",\"hf_definition\":\"raw sumCos(0),sumSin(0),sumPtOrEt; harmonic 2\",\"gen_p4_order\":[\"pt\",\"eta\",\"phi\",\"mass\"],\"no_parent_kinematic_cut\":true}";
  TObjString(p.str().c_str()).Write("provenance");
  TObjString(mapping_.anRaw.c_str()).Write("an_reference_url");
  TObjString("trk: October D0 dxyError^2+vxError*vyError; anTrk: September AN track.dxyError(point,vertex.covariance); both retain original saved normalized Q when present").Write("tracker_definitions");
 }
 void book(){
  edm::Service<TFileService> fs;fs->file().cd();
  events_=new TTree("Event","All MiniAOD events with event-filter decisions and both historical Q definitions");
  events_->Branch("run",&run_,"run/i");events_->Branch("lumi",&lumi_,"lumi/i");events_->Branch("event",&event_,"event/l");
  events_->Branch("eventRow",&eventRow_,"eventRow/l");events_->Branch("sourceEntry",&sourceEntry_,"sourceEntry/l");events_->Branch("originalEntry",&originalEntry_,"originalEntry/L");
  events_->Branch("sourceIndex",&sourceIndex_,"sourceIndex/I");events_->Branch("originalJob",&originalJob_,"originalJob/I");events_->Branch("manifestIndex",&manifestIndex_,"manifestIndex/I");
  events_->Branch("hiBin",&hiBin_,"hiBin/I");events_->Branch("pv",pv_,"pv[3]/F");events_->Branch("pthat",&pthat_,"pthat/F");events_->Branch("generatorWeight",&generatorWeight_,"generatorWeight/D");events_->Branch("truePsi",&truePsi_,"truePsi/F");
  events_->Branch("passD0Count",&passCount_,"passD0Count/O");events_->Branch("nD0",&nD0_,"nD0/I");events_->Branch("nDstar",&nDstar_,"nDstar/I");
  events_->Branch("trkAll",d0Q_.all,"trkAll[3][3]/F");events_->Branch("trkVeto",d0Q_.veto,"trkVeto[3][3]/F");events_->Branch("hf",hf_,"hf[3][3]/F");
  events_->Branch("trkAllValid",d0Q_.allValid,"trkAllValid[3]/O");events_->Branch("trkVetoValid",d0Q_.vetoValid,"trkVetoValid[3]/O");events_->Branch("hfValid",hfValid_,"hfValid[3]/O");
  events_->Branch("nEPTracks",d0Q_.nAll,"nEPTracks[3]/i");events_->Branch("nRemovedTracks",d0Q_.nRemoved,"nRemovedTracks[3]/i");
  events_->Branch("trackerReplayDelta",&d0Q_.replay,"trackerReplayDelta/F");
  events_->Branch("anOriginalEntry",&anOriginalEntry_,"anOriginalEntry/L");
  events_->Branch("anPassD0Count",&anPassCount_,"anPassD0Count/O");
  events_->Branch("anND0",&anND0_,"anND0/I");events_->Branch("anNDstar",&anNDstar_,"anNDstar/I");
  events_->Branch("anTrkAll",anQ_.all,"anTrkAll[3][3]/F");events_->Branch("anTrkVeto",anQ_.veto,"anTrkVeto[3][3]/F");
  events_->Branch("anTrkAllValid",anQ_.allValid,"anTrkAllValid[3]/O");events_->Branch("anTrkVetoValid",anQ_.vetoValid,"anTrkVetoValid[3]/O");
  events_->Branch("anNEPTracks",anQ_.nAll,"anNEPTracks[3]/i");events_->Branch("anNRemovedTracks",anQ_.nRemoved,"anNRemovedTracks[3]/i");
  events_->Branch("anTrackerReplayDelta",&anQ_.replay,"anTrackerReplayDelta/F");
  events_->Branch("passHLT",&passHLT_,"passHLT/O");events_->Branch("passPV",&passPV_,"passPV/O");
  events_->Branch("passCluster",&passCluster_,"passCluster/O");events_->Branch("passHF",&passHF_,"passHF/O");
  events_->Branch("passBasic",&passBasic_,"passBasic/O");

  d0s_=new TTree("GenD0","PYTHIA D0 to Kpi(gamma), all parent kinematics");
  stars_=new TTree("GenDstar","PYTHIA Dstar to D0pi, linked D0 table");
  for(auto*t:{d0s_,stars_}){
   t->Branch("manifestIndex",&manifestIndex_,"manifestIndex/I");t->Branch("rowId",&genRow_,"rowId/l");t->Branch("eventRow",&genEvent_,"eventRow/l");t->Branch("prunedIndex",&prunedIndex_,"prunedIndex/I");t->Branch("pdgId",&pdg_,"pdgId/I");t->Branch("collisionId",&collision_,"collisionId/I");
   t->Branch("motherId",&mother_,"motherId/I");t->Branch("ancestorId",&ancestorId_,"ancestorId/I");t->Branch("ancestorFlavor",&ancestorFlavor_,"ancestorFlavor/I");
   t->Branch("nDaughters",&ndau_,"nDaughters/I");t->Branch("nGamma",&ngamma_,"nGamma/I");t->Branch("p4",p4_,"p4[4]/F");t->SetAutoFlush(-4000000);
  }
  d0s_->Branch("kaon",k4_,"kaon[4]/F");d0s_->Branch("pion",pi4_,"pion[4]/F");d0s_->Branch("kaonPdgId",&kaonPid_,"kaonPdgId/I");d0s_->Branch("pionPdgId",&pionPid_,"pionPdgId/I");
  stars_->Branch("d0Row",&d0Link_,"d0Row/l");stars_->Branch("slowPion",slow4_,"slowPion[4]/F");
  events_->SetAutoFlush(-4000000);
  meta_=new TTree("Summary","Production completeness and validation counters");
  meta_->Branch("complete",&complete_,"complete/O");meta_->Branch("partial",&partial_,"partial/O");
  meta_->Branch("events",&nEvents_,"events/l");meta_->Branch("countPassed",&nPass_,"countPassed/l");meta_->Branch("countFailed",&nFail_,"countFailed/l");
  meta_->Branch("genD0",&nGenD0_,"genD0/l");meta_->Branch("genDstar",&nGenDstar_,"genDstar/l");
  meta_->Branch("trackerReplayDifferences",&nReplay_,"trackerReplayDifferences/l");meta_->Branch("maxTrackerReplayDelta",&maxReplay_,"maxTrackerReplayDelta/D");
 }
 TrackerResult tracker(const reco::TrackCollection& tracks,const reco::Vertex& vertex,const genq::OriginalEvent& old,bool present,bool fullCov){
  TrackerResult r;float pv[3]={float(vertex.x()),float(vertex.y()),float(vertex.z())};
  if(present)std::copy(old.vertex,old.vertex+3,pv);
  math::XYZPoint point(pv[0],pv[1],pv[2]);float ex=vertex.xError(),ey=vertex.yError(),ez=vertex.zError();
  std::vector<std::array<float,2>> ip;std::map<std::pair<int,uint32_t>,std::vector<unsigned>> buckets;
  for(unsigned i=0;i<tracks.size();++i){const auto&t=tracks[i];float z=t.dz(point),x=t.dxy(point),ze=sqrt(t.dzError()*t.dzError()+ez*ez),xe=fullCov?t.dxyError(point,vertex.covariance()):sqrt(t.dxyError()*t.dxyError()+ex*ey);ip.push_back({{z/ze,x/xe}});if(t.quality(reco::TrackBase::highPurity))buckets[{int(t.numberOfValidHits()),fbits(float(t.ptError()))}].push_back(i);}
  std::set<unsigned> removed;
  for(const auto&sig:old.leaves){
   auto it=buckets.find({sig.hits,fbits(sig.error)});std::vector<unsigned> candidates;if(it!=buckets.end())candidates=it->second;
   if(candidates.size()>1){
    std::vector<unsigned> distinct;
    for(auto j:candidates){bool same=false;for(auto k:distinct)if(tracks[j].charge()==tracks[k].charge()&&tracks[j].pt()==tracks[k].pt()&&tracks[j].eta()==tracks[k].eta()&&tracks[j].phi()==tracks[k].phi()){same=true;break;}if(!same)distinct.push_back(j);}
    candidates=distinct;
   }
   if(candidates.size()>1){std::vector<unsigned> next;for(auto j:candidates)if(std::abs(ip[j][0]-sig.dz)<1e-3&&std::abs(ip[j][1]-sig.xy)<1e-3)next.push_back(j);candidates=next;}
   if(candidates.size()!=1)throw cms::Exception("GenQTrackIdentity")<<"Original Dstar daughter identity is not unique at run/lumi/event "<<run_<<"/"<<lumi_<<"/"<<event_<<" candidates="<<candidates.size()<<" hits="<<sig.hits<<" ptError="<<sig.error;
   removed.insert(candidates[0]);++nLeaves_;
  }
  float sums[2][3][3]{};std::fill(r.nAll,r.nAll+3,0);std::fill(r.nRemoved,r.nRemoved+3,0);
  for(unsigned i=0;i<tracks.size();++i){const auto&t=tracks[i];
   if(!t.quality(reco::TrackBase::highPurity)||fabs(t.ptError())/t.pt()>.10||fabs(ip[i][0])>3||fabs(ip[i][1])>3||t.pt()<=.3||t.pt()>=3||fabs(t.eta())>=2.4)continue;
   float pt=t.pt(),phi=t.phi(),eta=t.eta();bool sides[3]={true,eta>.05&&eta<2.4,eta> -2.4&&eta< -.05};bool gone=removed.count(i);
   for(int s=0;s<3;++s)if(sides[s]){
    ++r.nAll[s];if(gone)++r.nRemoved[s];
    for(int v=0;v<2;++v){if(v&&gone)continue;sums[v][s][0]+=pt*cos(2*phi);sums[v][s][1]+=pt*sin(2*phi);sums[v][s][2]+=pt;}
   }
  }
  r.replay=0;
  for(int s=0;s<3;++s){
   for(int k=0;k<2;++k){r.all[s][k]=norm(sums[0][s][k],sums[0][s][2]);r.veto[s][k]=norm(sums[1][s][k],sums[1][s][2]);}
   r.all[s][2]=sums[0][s][2];r.veto[s][2]=sums[1][s][2];
   if(present){
    for(int k=0;k<2;++k){
     if(std::isfinite(r.all[s][k])&&std::isfinite(old.all[s][k])&&fabs(old.all[s][k])<=1)r.replay=std::max(r.replay,float(fabs(r.all[s][k]-old.all[s][k])));
     r.all[s][k]=old.all[s][k];r.veto[s][k]=old.veto[s][k];
    }
    r.all[s][2]=old.all[s][2];
    r.veto[s][2]=old.all[s][2]-(sums[0][s][2]-sums[1][s][2]);
    if(r.veto[s][2]< -1e-3)throw cms::Exception("GenQ")<<"Negative excluded track sumW";
   }
   r.allValid[s]=r.all[s][2]>0&&std::isfinite(r.all[s][0])&&std::isfinite(r.all[s][1])&&fabs(r.all[s][0])<=1&&fabs(r.all[s][1])<=1;
   r.vetoValid[s]=r.veto[s][2]>0&&std::isfinite(r.veto[s][0])&&std::isfinite(r.veto[s][1])&&fabs(r.veto[s][0])<=1&&fabs(r.veto[s][1])<=1;
  }
  return r;
 }
 void genealogy(const reco::GenParticle&p,int index){
  prunedIndex_=index;pdg_=p.pdgId();collision_=p.collisionId();mother_=mom(&p)?mom(&p)->pdgId():0;
  auto a=ancestry(&p);ancestorId_=a.first;ancestorFlavor_=a.second;ndau_=p.numberOfDaughters();ngamma_=daughters(&p).gamma;four(p4_,&p);
 }
 void fillGen(const edm::Handle<reco::GenParticleCollection>& handle){
  const auto&g=*handle;
  const auto d0Refs=FrozenNativeGenSelection(421,signalOnly_).select(handle);
  const auto starRefs=FrozenNativeGenSelection(413,signalOnly_).select(handle);
  std::unordered_map<const reco::Candidate*,ULong64_t> links;
  genEvent_=nEvents_;
  for(const auto&ref:d0Refs){
   unsigned i=ref.key();const auto&p=g[i];
   auto d=daughters(&p);if(!goodD0(&p,d))continue;
   genealogy(p,i);four(k4_,d.k);four(pi4_,d.pi);kaonPid_=d.k->pdgId();pionPid_=d.pi->pdgId();
   links[&p]=nGenD0_;genRow_=nGenD0_;d0s_->Fill();++nGenD0_;
  }
  for(const auto&ref:starRefs){
   unsigned i=ref.key();const auto&p=g[i];
   auto d=daughters(&p);if(d.gamma||d.nonGamma!=2||!d.d0||!d.pi)continue;
   auto dd=daughters(d.d0);if(!goodD0(d.d0,dd)||dd.k->charge()*d.pi->charge()>=0)continue;
   auto link=links.find(d.d0);if(link==links.end())throw cms::Exception("GenQ")<<"Native selected Dstar has no PYTHIA D0 link";
   genealogy(p,i);d0Link_=link->second;genRow_=nGenDstar_;four(slow4_,d.pi);stars_->Fill();++nGenDstar_;
  }
 }
 public:
 explicit D0DstarGenQ(const edm::ParameterSet& c):
 input_(c.getParameter<std::string>("inputFile")),manifest_(c.getParameter<std::string>("manifest")),summary_(c.getParameter<std::string>("summary")),
 partial_(c.getParameter<bool>("allowPartial")),signalOnly_(c.getUntrackedParameter<bool>("signalOnlyGenMatching",true)),
 tracks_(consumes<reco::TrackCollection>(edm::InputTag("unpackedTracksAndVertices"))),
 vertices_(consumes<reco::VertexCollection>(edm::InputTag("unpackedTracksAndVertices"))),
 beam_(consumes<reco::BeamSpot>(edm::InputTag("offlineBeamSpot"))),
 cent_(consumes<int>(edm::InputTag("centralityBin","HFtowers"))),
 planes_(consumes<reco::EvtPlaneCollection>(edm::InputTag("hiEvtPlaneRecalc"))),
 gen_(consumes<reco::GenParticleCollection>(edm::InputTag("prunedGenParticles"))),
 truth_(consumes<edm::GenHIEvent>(edm::InputTag("heavyIon"))),
 generator_(consumes<GenEventInfoProduct>(edm::InputTag("generator"))),
 inputIndex_(consumes<unsigned long long>(edm::InputTag("inputIndex"))),
 runInfo_(consumes<GenRunInfoProduct,edm::InRun>(edm::InputTag("generator"))),
 decisions_(consumes<edm::TriggerResults>(edm::InputTag("TriggerResults","","GENQ"))){
  usesResource(TFileService::kSharedResource);
  boost::property_tree::ptree data;boost::property_tree::read_json(manifest_,data);
  int index=0;for(auto&x:data){auto&r=x.second;Mapping m{r.get<std::string>("raw_url"),r.get<std::string>("group"),r.get<std::string>("an_raw_url"),r.get<int>("original_job"),r.get<int>("manifest_index",index++),r.get<int>("source_index")};auto lfn=r.get<std::string>("miniaod_lfn");if(!mappings_.emplace(lfn,m).second)throw cms::Exception("GenQ")<<"Duplicate input LFN";}
 }
 void beginJob() override {
  start_=std::chrono::steady_clock::now();book();edm::Service<TFileService>fs;fs->file().cd();
  runs_=new TTree("RunInfo","Generator cross-section metadata, recorded without applying normalization");
  runs_->Branch("run",&metadataRun_,"run/i");runs_->Branch("valid",&validRunInfo_,"valid/O");
  runs_->Branch("crossSectionPb",&sigmaPb_,"crossSectionPb/D");runs_->Branch("internalCrossSectionPb",&internalSigmaPb_,"internalCrossSectionPb/D");runs_->Branch("filterEfficiency",&filterEfficiency_,"filterEfficiency/D");
 }
 void beginRun(const edm::Run&,const edm::EventSetup&)override{}
 void endRun(const edm::Run&r,const edm::EventSetup&)override{
  metadataRun_=r.run();const auto g=r.getHandle(runInfo_);validRunInfo_=g.isValid();
  sigmaPb_=g.isValid()?g->crossSection():-1;internalSigmaPb_=g.isValid()?g->internalXSec().value():-1;filterEfficiency_=g.isValid()?g->filterEfficiency():-1;runs_->Fill();
 }
 void analyze(const edm::Event&e,const edm::EventSetup&) override {
  initialize();run_=e.id().run();lumi_=e.luminosityBlock();event_=e.id().event();sourceEntry_=e.get(inputIndex_);eventRow_=nEvents_;
  const auto& decisions=e.get(decisions_);const auto& names=e.triggerNames(decisions);
  auto passed=[&](const char* name){auto i=names.triggerIndex(name);if(i>=decisions.size()||decisions.error(i)||!decisions.wasrun(i))throw cms::Exception("GenQ")<<"Event decision not evaluated: "<<name;return decisions.accept(i);};
  passHLT_=passed("HLTSelection");passPV_=passed("PVSelection");passCluster_=passed("ClusterSelection");passHF_=passed("HFSelection");
  passBasic_=passHLT_&&passPV_&&passCluster_&&passHF_;
  anOriginalEntry_=anOriginal_->find(run_,lumi_,event_);anPassCount_=anOriginalEntry_>=0;anND0_=anNDstar_=0;
  genq::OriginalEvent anOld;
  if(anPassCount_){if(!anFound_.insert(anOriginalEntry_).second)throw cms::Exception("GenQ")<<"Repeated AN original event";anOld=anOriginal_->read(anOriginalEntry_);anND0_=anOld.nD0;anNDstar_=anOld.nDstar;++anNPass_;}
  originalEntry_=original_->find(run_,lumi_,event_);passCount_=originalEntry_>=0;nD0_=nDstar_=0;
  genq::OriginalEvent old;
  if(passCount_){if(!found_.insert(originalEntry_).second)throw cms::Exception("GenQ")<<"Repeated original event";old=original_->read(originalEntry_);nD0_=old.nD0;nDstar_=old.nDstar;++nPass_;}else ++nFail_;
  const auto&tracks=e.get(tracks_);const auto&vertices=e.get(vertices_);const auto&bs=e.get(beam_);hiBin_=e.get(cent_);
  if(anPassCount_&&hiBin_!=anOld.hiBin)throw cms::Exception("GenQ")<<"AN original centrality mismatch";
  if(passCount_&&hiBin_!=old.hiBin)throw cms::Exception("GenQ")<<"Original centrality mismatch";
  reco::Vertex vertex;if(!vertices.empty()&&!vertices[0].isFake()&&vertices[0].tracksSize()>=2)vertex=vertices[0];else vertex=reco::Vertex(bs.position(),bs.covariance3D());
  for(int i=0;i<3;++i)pv_[i]=i==0?vertex.x():i==1?vertex.y():vertex.z();
  if(passCount_)std::copy(old.vertex,old.vertex+3,pv_);
  d0Q_=tracker(tracks,vertex,old,passCount_,false);
  anQ_=tracker(tracks,vertex,anOld,anPassCount_,true);
  if(d0Q_.replay>2e-6)++nReplay_;
  maxReplay_=std::max(maxReplay_,double(d0Q_.replay));
  if(anQ_.replay>2e-6)++anNReplay_;
  anMaxReplay_=std::max(anMaxReplay_,double(anQ_.replay));
  const auto&planes=e.get(planes_);const int hfIndex[3]={2,1,0};if(planes.size()<3)throw cms::Exception("GenQ")<<"HF event planes missing";
  for(int s=0;s<3;++s){
   const auto&p=planes[hfIndex[s]];hf_[s][0]=p.sumCos(0);hf_[s][1]=p.sumSin(0);hf_[s][2]=p.sumPtOrEt();
   if(passCount_||anPassCount_){
    const auto& reference=passCount_?old:anOld;
    for(int k=0;k<2;++k)if(reference.hf[s][2]>0&&hf_[s][2]>0&&fabs(hf_[s][k]/hf_[s][2]-reference.hf[s][k]/reference.hf[s][2])>2e-6)throw cms::Exception("GenQ")<<"HF Q reproduction failed";
    std::copy(reference.hf[s],reference.hf[s]+3,hf_[s]);
    if(passCount_&&anPassCount_)for(int k=0;k<2;++k)if(old.hf[s][2]>0&&anOld.hf[s][2]>0&&fabs(old.hf[s][k]/old.hf[s][2]-anOld.hf[s][k]/anOld.hf[s][2])>2e-6)throw cms::Exception("GenQ")<<"D0/AN HF Q differs";
   }
   hfValid_[s]=hf_[s][2]>0&&std::isfinite(hf_[s][0])&&std::isfinite(hf_[s][1]);
  }
  const auto truth=e.getHandle(truth_);truePsi_=truth.isValid()?truth->evtPlane():std::numeric_limits<float>::quiet_NaN();
  const auto gi=e.getHandle(generator_);if(!gi.isValid())throw cms::Exception("GenQ")<<"GenEventInfoProduct missing";
  generatorWeight_=gi->weight();pthat_=gi->binningValues().empty()?gi->qScale():gi->binningValues().front();
  fillGen(e.getHandle(gen_));events_->Fill();++nEvents_;
 }
 void endJob()override{
  initialize();
  if(!partial_&&Long64_t(found_.size())!=original_->entries)throw cms::Exception("GenQIncomplete")<<"Original events found "<<found_.size()<<" expected "<<original_->entries;
  if(!partial_&&Long64_t(anFound_.size())!=anOriginal_->entries)throw cms::Exception("GenQIncomplete")<<"AN original events found "<<anFound_.size()<<" expected "<<anOriginal_->entries;
  complete_=true;meta_->Fill();
  std::ostringstream o;o<<"{\"status\":\"complete\",\"partial\":"<<(partial_?"true":"false")<<",\"input_lfn\":\""<<input_<<"\",\"group\":\""<<mapping_.group<<"\",\"events\":"<<nEvents_<<",\"count_passed\":"<<nPass_<<",\"count_failed\":"<<nFail_<<",\"original_events\":"<<original_->entries<<",\"an_count_passed\":"<<anNPass_<<",\"an_original_events\":"<<anOriginal_->entries<<",\"an_replay_differences\":"<<anNReplay_<<",\"an_max_replay_delta\":"<<anMaxReplay_<<",\"gen_d0\":"<<nGenD0_<<",\"gen_dstar\":"<<nGenDstar_<<",\"identity_leaves\":"<<nLeaves_<<",\"replay_differences\":"<<nReplay_<<",\"max_replay_delta\":"<<maxReplay_<<",\"seconds\":"<<std::chrono::duration<double>(std::chrono::steady_clock::now()-start_).count()<<"}";
  std::ofstream f(summary_);f<<o.str()<<"\n";edm::Service<TFileService> fs;fs->file().cd();TObjString(o.str().c_str()).Write("completion");
 }
};
DEFINE_FWK_MODULE(D0DstarGenQ);
