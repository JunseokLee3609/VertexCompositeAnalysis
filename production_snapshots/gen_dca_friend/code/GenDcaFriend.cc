// GEN-only additive D0/Dstar DCA supplement. Physics helper copied from baebad240f9764d2f3a3cbd27da909342e327c2a.
#include <cmath>
#include <limits>
#include <memory>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <fstream>
#include "FWCore/Framework/interface/one/EDAnalyzer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/Utilities/interface/Exception.h"
#include "CommonTools/UtilAlgos/interface/TFileService.h"
#include "DataFormats/HepMCCandidate/interface/GenParticle.h"
#include "DataFormats/Math/interface/Point3D.h"
#include "TTree.h"
#include "TNamed.h"
#include "TVector3.h"
class GenDcaFriend: public edm::one::EDAnalyzer<edm::one::SharedResources>{
 edm::EDGetTokenT<reco::GenParticleCollection> particles_;
 edm::EDGetTokenT<math::XYZPointF> origin_;
 math::XYZPointF genPrimaryVertex_;bool genPrimaryVertexValid_=false;
 unsigned run_=0,lumi_=0;unsigned long long event_=0,sourceEntry_=0,events_=0,rows_=0,invalid_=0;
 std::vector<int> index_,pdg_,charge_,collision_,d0index_;
 std::vector<float> pt_,eta_,phi_,mass_,dca_;
 std::vector<unsigned char> valid_;
 std::string input_,report_;TTree* tree_;
 bool computeGenD0Dca3D(const reco::Candidate&,float&)const;
 public:
 explicit GenDcaFriend(const edm::ParameterSet& c):particles_(consumes<reco::GenParticleCollection>(c.getParameter<edm::InputTag>("particles"))),origin_(consumes<math::XYZPointF>(c.getParameter<edm::InputTag>("origin"))),input_(c.getParameter<std::string>("inputLFN")),report_(c.getParameter<std::string>("report")){
  usesResource(TFileService::kSharedResource);edm::Service<TFileService> fs;
  fs->make<TNamed>("inputLFN",input_.c_str());fs->make<TNamed>("schema","GenDcaSupplement_v1");
  fs->make<TNamed>("physicsBase","baebad240f9764d2f3a3cbd27da909342e327c2a; D0 GEN trajectory DCA to its own collision origin, cm; no event or candidate filter");
  tree_=fs->make<TTree>("Events","All-input-event GEN D0 and Dstar DCA supplement");
  tree_->Branch("run",&run_,"run/i");tree_->Branch("lumi",&lumi_,"lumi/i");tree_->Branch("event",&event_,"event/l");tree_->Branch("sourceEntry",&sourceEntry_,"sourceEntry/l");
  tree_->Branch("signalPVValid",&genPrimaryVertexValid_,"signalPVValid/O");
  tree_->Branch("prunedIndex",&index_);tree_->Branch("pdgId",&pdg_);tree_->Branch("charge",&charge_);tree_->Branch("collisionId",&collision_);tree_->Branch("d0Index",&d0index_);
  tree_->Branch("pT",&pt_);tree_->Branch("eta",&eta_);tree_->Branch("phi",&phi_);tree_->Branch("mass",&mass_);tree_->Branch("dca3D",&dca_);tree_->Branch("valid",&valid_);
 }
 void analyze(const edm::Event& e,const edm::EventSetup&)override{
  const auto& gen=e.get(particles_);const auto h=e.getHandle(origin_);genPrimaryVertexValid_=h.isValid();
  if(genPrimaryVertexValid_){genPrimaryVertex_=*h;genPrimaryVertexValid_=std::isfinite(h->x())&&std::isfinite(h->y())&&std::isfinite(h->z());}
  run_=e.id().run();lumi_=e.luminosityBlock();event_=e.id().event();sourceEntry_=events_;
  index_.clear();pdg_.clear();charge_.clear();collision_.clear();d0index_.clear();pt_.clear();eta_.clear();phi_.clear();mass_.clear();dca_.clear();valid_.clear();
  std::unordered_map<const reco::Candidate*,int> keys;for(unsigned i=0;i<gen.size();++i)keys[&gen[i]]=i;
  for(unsigned i=0;i<gen.size();++i){const auto& p=gen[i];const int id=std::abs(p.pdgId());if(id!=421&&id!=413)continue;
   const reco::Candidate* d0=nullptr;if(id==421)d0=&p;else for(unsigned d=0;d<p.numberOfDaughters();++d)if(p.daughter(d)&&std::abs(p.daughter(d)->pdgId())==421){if(d0)throw cms::Exception("GenDca")<<"Ambiguous Dstar D0 daughter";d0=p.daughter(d);}
   float value=-99.f;bool good=d0&&computeGenD0Dca3D(*d0,value);auto it=keys.find(d0);
   index_.push_back(i);pdg_.push_back(p.pdgId());charge_.push_back(p.charge());collision_.push_back(p.collisionId());d0index_.push_back(it==keys.end()?-1:it->second);
   pt_.push_back(p.pt());eta_.push_back(p.eta());phi_.push_back(p.phi());mass_.push_back(p.mass());dca_.push_back(value);valid_.push_back(good);++rows_;if(!good)++invalid_;
  }
  tree_->Fill();++events_;
 }
 void endJob()override{std::ofstream f(report_);f<<"{\"status\":\"complete\",\"events\":"<<events_<<",\"rows\":"<<rows_<<",\"invalid_rows\":"<<invalid_<<"}\n";}
};

bool GenDcaFriend::computeGenD0Dca3D(const reco::Candidate& d0, float& value) const {
  value = -99.f;
  if (std::abs(d0.pdgId()) != 421) return false;
  const auto* genD0 = dynamic_cast<const reco::GenParticle*>(&d0);
  if (!genD0 || genD0->collisionId() < 0) return false;

  math::XYZPoint origin;
  if (genD0->collisionId() == 0) {
    if (!genPrimaryVertexValid_) return false;
    origin = math::XYZPoint(genPrimaryVertex_.x(), genPrimaryVertex_.y(), genPrimaryVertex_.z());
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

DEFINE_FWK_MODULE(GenDcaFriend);
