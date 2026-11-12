#include "FWCore/Framework/interface/one/EDAnalyzer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/EventSetup.h"
#include "FWCore/Framework/interface/FileBlock.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/Utilities/interface/Exception.h"
#include "CommonTools/UtilAlgos/interface/TFileService.h"
#include "boost/property_tree/ptree.hpp"
#include "boost/property_tree/json_parser.hpp"
#include "D0Matching.hxx"
#include <fstream>
#include <chrono>

class D0CrabFriend : public edm::one::EDAnalyzer<edm::one::SharedResources> {
 using Packed=std::vector<pat::PackedCandidate>;
 struct Mapping {std::string raw,group,lfn;};
 std::map<std::string,Mapping> mappings_;
 std::string current_,manifest_,summaryPath_,rawPath_,group_;
 bool allowPartial_,signalOnlyGenMatching_;
 std::unique_ptr<d0friend::Input> source_;
 std::unique_ptr<d0friend::Writer> writer_;
 std::set<Long64_t> seen_;
 std::chrono::steady_clock::time_point started_;
 edm::EDGetTokenT<Packed> pf_,lost_,ele_;
 edm::EDGetTokenT<std::vector<reco::Vertex>> vertices_;
 edm::EDGetTokenT<reco::BeamSpot> beam_;
 edm::EDGetTokenT<std::vector<pat::PackedGenParticle>> packed_;
 edm::EDGetTokenT<std::vector<reco::GenParticle>> pruned_;
 void initialize(){
  if(source_)return;
  auto pos=current_.find("/store/");if(pos==std::string::npos)throw cms::Exception("InputMapping")<<"Invalid MiniAOD input "<<current_;
  current_=current_.substr(pos);auto query=current_.find('?');if(query!=std::string::npos)current_.resize(query);
  auto found=mappings_.find(current_);
  if(found==mappings_.end())throw cms::Exception("InputMapping")<<"No original ntuple for "<<current_;
  rawPath_=found->second.raw;group_=found->second.group;
  source_=std::make_unique<d0friend::Input>(rawPath_);
  edm::Service<TFileService> fs;
  if(!fs.isAvailable())throw cms::Exception("Configuration")<<"TFileService required";
  std::ostringstream provenance;
  provenance<<"{\"matching_commit\":\"75af18a65a5a1c24aaaf722ab9d7fb1d5f6c8592\",\"identity_formula\":\"unpacked_tracks_unique_momentum_2ulp_disambiguation\",\"input_lfn\":\""<<current_<<"\",\"raw_url\":\""<<rawPath_<<"\",\"group\":\""<<group_<<"\"}";
  provenance.seekp(-1,std::ios_base::cur);
  provenance<<",\"signalOnlyGenMatching\":"<<(signalOnlyGenMatching_?"true":"false")<<"}";
  writer_=std::make_unique<d0friend::Writer>("",source_->maximum,provenance.str(),&fs->file(),signalOnlyGenMatching_);
 }
 public:
 explicit D0CrabFriend(const edm::ParameterSet& cfg):
 current_(cfg.getParameter<std::string>("inputFile")),
 manifest_(cfg.getParameter<std::string>("manifest")),
 summaryPath_(cfg.getParameter<std::string>("summary")),
 allowPartial_(cfg.getParameter<bool>("allowPartial")),
 signalOnlyGenMatching_(cfg.getUntrackedParameter<bool>("signalOnlyGenMatching",true)),
 pf_(consumes<Packed>(edm::InputTag("packedPFCandidates"))),
 lost_(consumes<Packed>(edm::InputTag("lostTracks"))),
 ele_(consumes<Packed>(edm::InputTag("lostTracks","eleTracks"))),
 vertices_(consumes<std::vector<reco::Vertex>>(edm::InputTag("offlineSlimmedPrimaryVertices"))),
 beam_(consumes<reco::BeamSpot>(edm::InputTag("offlineBeamSpot"))),
 packed_(consumes<std::vector<pat::PackedGenParticle>>(edm::InputTag("packedGenParticles"))),
 pruned_(consumes<std::vector<reco::GenParticle>>(edm::InputTag("prunedGenParticles"))){
  usesResource(TFileService::kSharedResource);
  boost::property_tree::ptree data;
  boost::property_tree::read_json(manifest_,data);
  for(auto const& row:data){
   auto lfn=row.second.get<std::string>("miniaod_lfn");
   Mapping m{row.second.get<std::string>("raw_url"),row.second.get<std::string>("group"),lfn};
   if(!mappings_.emplace(lfn,std::move(m)).second)throw cms::Exception("InputMapping")<<"Duplicate MiniAOD "<<lfn;
  }
 }
 void beginJob() override {started_=std::chrono::steady_clock::now();}
 void analyze(edm::Event const& ev,edm::EventSetup const&) override {
  initialize();
  auto entry=source_->find(ev.id().run(),ev.luminosityBlock(),ev.id().event());
  if(entry<0)return;
  if(!seen_.insert(entry).second)throw cms::Exception("InputMapping")<<"Repeated original entry "<<entry;
  auto input=source_->read(entry);
  auto pf=ev.getHandle(pf_),lost=ev.getHandle(lost_),ele=ev.getHandle(ele_);
  auto vertices=ev.getHandle(vertices_);auto beam=ev.getHandle(beam_);
  auto packed=ev.getHandle(packed_);auto pruned=ev.getHandle(pruned_);
  if(!pf.isValid()||!lost.isValid()||!ele.isValid()||!vertices.isValid()||!beam.isValid()||!packed.isValid()||!pruned.isValid())
   throw cms::Exception("ProductNotFound")<<"Missing matching input product";
  writer_->process(input,ev.id().event(),*pf,*lost,*ele,*vertices,*beam,*packed,*pruned);
 }
 void endJob() override {
  initialize();
  auto count=Long64_t(writer_->stats.at("events"));
  if(!allowPartial_&&count!=source_->entries)throw cms::Exception("IncompleteFriend")<<"Processed "<<count<<" of "<<source_->entries;
  std::ostringstream out;
  out<<"{\"status\":\"complete\",\"partial\":"<<(allowPartial_?"true":"false")<<",\"group\":\""<<group_<<"\",\"miniaod_lfn\":\""<<current_<<"\",\"raw_url\":\""<<rawPath_<<"\",\"source_events\":"<<source_->entries<<",\"seconds\":"<<std::chrono::duration<double>(std::chrono::steady_clock::now()-started_).count();
  out<<",\"signalOnlyGenMatching\":"<<(signalOnlyGenMatching_?"true":"false");
  for(auto const& item:writer_->stats)out<<",\""<<item.first<<"\":"<<item.second;
  out<<"}";
  writer_->close(allowPartial_?count:source_->entries,out.str());
  std::ofstream output(summaryPath_);output<<out.str()<<"\n";
 }
};
DEFINE_FWK_MODULE(D0CrabFriend);
