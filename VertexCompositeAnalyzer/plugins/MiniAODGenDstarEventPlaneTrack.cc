// system include files
#include <array>
#include <cmath>
#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include <TH1.h>
#include <TTree.h>
#include <TVector3.h>

// user include files
#include "FWCore/Framework/interface/Frameworkfwd.h"
#include "FWCore/Framework/interface/one/EDAnalyzer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/Run.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Utilities/interface/InputTag.h"
#include "FWCore/Utilities/interface/Exception.h"
#include "FWCore/ServiceRegistry/interface/Service.h"

#include "CommonTools/UtilAlgos/interface/TFileService.h"

#include "DataFormats/Candidate/interface/Candidate.h"
#include "DataFormats/HeavyIonEvent/interface/Centrality.h"
#include "DataFormats/HepMCCandidate/interface/GenParticle.h"
#include "DataFormats/PatCandidates/interface/PackedGenParticle.h"

namespace {
  constexpr double kInvalid = -999.0;

  struct QVectorResult {
    double qx = kInvalid;
    double qy = kInvalid;
    double sumW = 0.0;
    int n = 0;
  };

  struct SimpleP4 {
    double px = 0.0;
    double py = 0.0;
    double pz = 0.0;
    double e = 0.0;
  };

  SimpleP4 makeP4(const reco::Candidate& cand) {
    return SimpleP4{cand.px(), cand.py(), cand.pz(), cand.energy()};
  }

  TVector3 boostToRestFrame(const SimpleP4& p4, const SimpleP4& parent) {
    TVector3 momentum(p4.px, p4.py, p4.pz);
    if (parent.e <= 0.0)
      return momentum;

    const TVector3 beta(parent.px / parent.e, parent.py / parent.e, parent.pz / parent.e);
    const double beta2 = beta.Mag2();
    if (beta2 <= 0.0 || beta2 >= 1.0)
      return momentum;

    const double gamma = 1.0 / std::sqrt(1.0 - beta2);
    const double bp = beta.Dot(momentum);
    const double factor = ((gamma - 1.0) * bp / beta2) - gamma * p4.e;
    return momentum + factor * beta;
  }

  const reco::GenParticle* lastCopy(const reco::GenParticle& p) {
    const reco::GenParticle* current = &p;
    bool advanced = true;
    while (advanced) {
      advanced = false;
      for (size_t i = 0; i < current->numberOfDaughters(); ++i) {
        const auto* dau = dynamic_cast<const reco::GenParticle*>(current->daughter(i));
        if (!dau)
          continue;
        if (dau->pdgId() == current->pdgId()) {
          current = dau;
          advanced = true;
          break;
        }
      }
    }
    return current;
  }

  bool twoBodyDecay(const reco::GenParticle& parent,
                    int pdg1,
                    int pdg2,
                    const reco::Candidate*& out1,
                    const reco::Candidate*& out2) {
    const reco::GenParticle* p = lastCopy(parent);
    if (p->numberOfDaughters() != 2)
      return false;

    const reco::Candidate* d0 = p->daughter(0);
    const reco::Candidate* d1 = p->daughter(1);
    if (!d0 || !d1)
      return false;

    const int id0 = d0->pdgId();
    const int id1 = d1->pdgId();
    if ((id0 == pdg1 && id1 == pdg2) || (id0 == pdg2 && id1 == pdg1)) {
      out1 = d0;
      out2 = d1;
      return true;
    }
    return false;
  }

  bool hasAncestor(const reco::Candidate& cand, const reco::Candidate* ancestor, int maxDepth = 50) {
    if (!ancestor)
      return false;
    if (&cand == ancestor)
      return true;

    std::vector<const reco::Candidate*> stack;
    stack.reserve(cand.numberOfMothers());
    for (size_t i = 0; i < cand.numberOfMothers(); ++i) {
      const auto* mom = cand.mother(i);
      if (mom)
        stack.push_back(mom);
    }

    int depth = 0;
    while (!stack.empty() && depth < maxDepth) {
      const reco::Candidate* node = stack.back();
      stack.pop_back();
      if (!node)
        continue;
      if (node == ancestor)
        return true;
      for (size_t i = 0; i < node->numberOfMothers(); ++i) {
        const auto* mom = node->mother(i);
        if (mom)
          stack.push_back(mom);
      }
      ++depth;
    }
    return false;
  }

  const pat::PackedGenParticle* findPackedWithAncestor(const pat::PackedGenParticleCollection& packed,
                                                       int pdgId,
                                                       const reco::Candidate* ancestor) {
    for (const auto& p : packed) {
      if (p.pdgId() != pdgId)
        continue;
      if (p.charge() == 0)
        continue;
      if (!hasAncestor(p, ancestor))
        continue;
      return &p;
    }
    return nullptr;
  }

  QVectorResult computeQVector(const pat::PackedGenParticleCollection& packed,
                               int harmonic,
                               const std::unordered_set<const pat::PackedGenParticle*>& excluded,
                               double ptMin,
                               double ptMax,
                               double absEtaMax,
                               double etaMin,
                               double etaMax) {
    QVectorResult res;
    double qxRaw = 0.0;
    double qyRaw = 0.0;
    double sumW = 0.0;
    int n = 0;

    for (const auto& gp : packed) {
      if (gp.charge() == 0)
        continue;
      if (!excluded.empty() && excluded.count(&gp))
        continue;

      const double pt = gp.pt();
      const double eta = gp.eta();
      if (pt < ptMin || pt > ptMax)
        continue;
      if (std::abs(eta) > absEtaMax)
        continue;
      if (!(eta >= etaMin && eta <= etaMax))
        continue;

      const double phi = gp.phi();
      qxRaw += pt * std::cos(harmonic * phi);
      qyRaw += pt * std::sin(harmonic * phi);
      sumW += pt;
      ++n;
    }

    res.sumW = sumW;
    res.n = n;
    if (sumW > 0.0) {
      res.qx = qxRaw / sumW;
      res.qy = qyRaw / sumW;
    }
    return res;
  }

  double computePsi(double qx, double qy, int harmonic) {
    if (qx == kInvalid || qy == kInvalid || harmonic <= 0)
      return kInvalid;
    return std::atan2(qy, qx) / harmonic;
  }

  const reco::Candidate* findDaughterByAbsPdg(const reco::Candidate& parent, int absPdgId) {
    for (size_t i = 0; i < parent.numberOfDaughters(); ++i) {
      const auto* dau = parent.daughter(i);
      if (dau && std::abs(dau->pdgId()) == absPdgId)
        return dau;
    }
    return nullptr;
  }

  void getAncestorId(const reco::Candidate& cand, int& ancestorId, int& ancestorFlavor) {
    ancestorId = 0;
    ancestorFlavor = 0;
    const reco::Candidate* mom = &cand;
    int depth = 0;
    constexpr int kMaxDepth = 50;

    while (mom != nullptr && depth < kMaxDepth) {
      if (depth > 0) {
        const int currentId = mom->pdgId();
        const int absId = std::abs(currentId);
        ancestorId = currentId;
        const std::string idstr = std::to_string(absId);
        if (!idstr.empty())
          ancestorFlavor = std::stoi(std::string{idstr.begin(), idstr.begin() + 1});
        if (!idstr.empty() && idstr[0] == '5')
          break;
        if (absId <= 40)
          break;
      }
      mom = mom->mother();
      ++depth;
    }
  }

  double computeHelicityCosTheta(const reco::Candidate& dstar, const reco::Candidate& daughter) {
    // Use the D* flight direction in the lab as the helicity axis and evaluate
    // the daughter direction after boosting it into the D* rest frame.
    const TVector3 helicityAxisLab(dstar.px(), dstar.py(), dstar.pz());
    const TVector3 daughterInDstarRest = boostToRestFrame(makeP4(daughter), makeP4(dstar));
    if (helicityAxisLab.Mag2() <= 0.0 || daughterInDstarRest.Mag2() <= 0.0)
      return kInvalid;
    return daughterInDstarRest.Unit().Dot(helicityAxisLab.Unit());
  }
}  // namespace

class MiniAODGenDstarEventPlaneTrack : public edm::one::EDAnalyzer<edm::one::WatchRuns> {
public:
  explicit MiniAODGenDstarEventPlaneTrack(const edm::ParameterSet&);
  ~MiniAODGenDstarEventPlaneTrack() override = default;

private:
  void beginJob() override;
  void beginRun(const edm::Run&, const edm::EventSetup&) override {}
  void endRun(const edm::Run&, const edm::EventSetup&) override {}
  void analyze(const edm::Event&, const edm::EventSetup&) override;
  void endJob() override {}

  void initTree();
  void initHistogram();

  edm::Service<TFileService> fs_;

  TTree* eventTree_ = nullptr;
  TH1D* hNGenTrkSel_ = nullptr;

  bool saveTree_ = true;
  bool saveHistogram_ = false;

  bool isCentrality_ = false;

  double trackPtMin_ = 0.3;
  double trackPtMax_ = 3.0;
  double trackAbsEtaMax_ = 2.4;
  double subEventAbsEtaMin_ = 0.5;

  int harmonic2_ = 2;
  int harmonic3_ = 3;

  edm::EDGetTokenT<reco::GenParticleCollection> tok_pruned_;
  edm::EDGetTokenT<pat::PackedGenParticleCollection> tok_packed_;
  edm::EDGetTokenT<int> tok_centBinLabel_;
  edm::EDGetTokenT<reco::Centrality> tok_centSrc_;

  // tree variables (one entry per gen D*)
  uint32_t runNb_ = 0;
  uint32_t eventNb_ = 0;
  uint32_t lsNb_ = 0;
  short centrality_ = -1;
  int Ntrkoffline_ = -1;
  int nGenDstar_ = 0;
  int nGenDstarDauTrkExcluded_ = 0;

  int nGenTrkSelAllIncl_ = 0;
  int nGenTrkSelForwIncl_ = 0;
  int nGenTrkSelBackIncl_ = 0;
  int nGenTrkSelAll_ = 0;
  int nGenTrkSelForw_ = 0;
  int nGenTrkSelBack_ = 0;

  // Inclusive (no exclusion) event-plane
  double genQx2AllIncl_ = kInvalid, genQy2AllIncl_ = kInvalid, genPsi2AllIncl_ = kInvalid, genSumPt2AllIncl_ = 0;
  double genQx2AllForwIncl_ = kInvalid, genQy2AllForwIncl_ = kInvalid, genPsi2AllForwIncl_ = kInvalid, genSumPt2AllForwIncl_ = 0;
  double genQx2AllBackIncl_ = kInvalid, genQy2AllBackIncl_ = kInvalid, genPsi2AllBackIncl_ = kInvalid, genSumPt2AllBackIncl_ = 0;
  double genQx3AllIncl_ = kInvalid, genQy3AllIncl_ = kInvalid, genPsi3AllIncl_ = kInvalid, genSumPt3AllIncl_ = 0;
  double genQx3AllForwIncl_ = kInvalid, genQy3AllForwIncl_ = kInvalid, genPsi3AllForwIncl_ = kInvalid, genSumPt3AllForwIncl_ = 0;
  double genQx3AllBackIncl_ = kInvalid, genQy3AllBackIncl_ = kInvalid, genPsi3AllBackIncl_ = kInvalid, genSumPt3AllBackIncl_ = 0;

  // Excluding all D*→Kππ daughter tracks in the event (recommended for D* v_n to avoid autocorrelation)
  double genQx2All_ = kInvalid, genQy2All_ = kInvalid, genPsi2All_ = kInvalid, genSumPt2All_ = 0;
  double genQx2AllForw_ = kInvalid, genQy2AllForw_ = kInvalid, genPsi2AllForw_ = kInvalid, genSumPt2AllForw_ = 0;
  double genQx2AllBack_ = kInvalid, genQy2AllBack_ = kInvalid, genPsi2AllBack_ = kInvalid, genSumPt2AllBack_ = 0;
  double genQx3All_ = kInvalid, genQy3All_ = kInvalid, genPsi3All_ = kInvalid, genSumPt3All_ = 0;
  double genQx3AllForw_ = kInvalid, genQy3AllForw_ = kInvalid, genPsi3AllForw_ = kInvalid, genSumPt3AllForw_ = 0;
  double genQx3AllBack_ = kInvalid, genQy3AllBack_ = kInvalid, genPsi3AllBack_ = kInvalid, genSumPt3AllBack_ = 0;

  std::vector<float> genDstarPt_;
  std::vector<float> genDstarY_;
  std::vector<float> genD0Pt_;
  std::vector<float> genD0Y_;
  std::vector<int> genD0PdgId_;
  std::vector<int> genD0AncestorId_;
  std::vector<int> genD0AncestorFlavor_;
  std::vector<int> genD0IsPrompt_;
  std::vector<float> genD0CosThetaHX_;
};

MiniAODGenDstarEventPlaneTrack::MiniAODGenDstarEventPlaneTrack(const edm::ParameterSet& iConfig) {
  saveTree_ = iConfig.getUntrackedParameter<bool>("saveTree", true);
  saveHistogram_ = iConfig.getUntrackedParameter<bool>("saveHistogram", false);

  trackPtMin_ = iConfig.getUntrackedParameter<double>("trackPtMin", 0.3);
  trackPtMax_ = iConfig.getUntrackedParameter<double>("trackPtMax", 3.0);
  trackAbsEtaMax_ = iConfig.getUntrackedParameter<double>("trackAbsEtaMax", 2.4);
  subEventAbsEtaMin_ = iConfig.getUntrackedParameter<double>("subEventAbsEtaMin", 0.5);

  harmonic2_ = iConfig.getUntrackedParameter<int>("harmonic2", 2);
  harmonic3_ = iConfig.getUntrackedParameter<int>("harmonic3", 3);

  tok_pruned_ = consumes<reco::GenParticleCollection>(
      edm::InputTag(iConfig.getUntrackedParameter<edm::InputTag>("prunedGenParticles")));
  tok_packed_ = consumes<pat::PackedGenParticleCollection>(
      edm::InputTag(iConfig.getUntrackedParameter<edm::InputTag>("packedGenParticles")));

  isCentrality_ = (iConfig.exists("isCentrality") ? iConfig.getParameter<bool>("isCentrality") : false);
  if (isCentrality_) {
    tok_centBinLabel_ = consumes<int>(iConfig.getParameter<edm::InputTag>("centralityBinLabel"));
    tok_centSrc_ = consumes<reco::Centrality>(iConfig.getParameter<edm::InputTag>("centralitySrc"));
  }
}

void MiniAODGenDstarEventPlaneTrack::beginJob() {
  TH1D::SetDefaultSumw2();
  if (saveTree_)
    initTree();
  if (saveHistogram_)
    initHistogram();
}

void MiniAODGenDstarEventPlaneTrack::initTree() {
  eventTree_ = fs_->make<TTree>("GenEventPlane", "GenEventPlane");

  eventTree_->Branch("RunNb", &runNb_, "RunNb/i");
  eventTree_->Branch("LSNb", &lsNb_, "LSNb/i");
  eventTree_->Branch("EventNb", &eventNb_, "EventNb/i");
  if (isCentrality_) {
    eventTree_->Branch("centrality", &centrality_, "centrality/S");
    eventTree_->Branch("Ntrkoffline", &Ntrkoffline_, "Ntrkoffline/I");
  }
  eventTree_->Branch("nGenDstar", &nGenDstar_, "nGenDstar/I");
  eventTree_->Branch("nGenDstarDauTrkExcluded", &nGenDstarDauTrkExcluded_, "nGenDstarDauTrkExcluded/I");
  eventTree_->Branch("nGenTrkSelAllIncl", &nGenTrkSelAllIncl_, "nGenTrkSelAllIncl/I");
  eventTree_->Branch("nGenTrkSelForwIncl", &nGenTrkSelForwIncl_, "nGenTrkSelForwIncl/I");
  eventTree_->Branch("nGenTrkSelBackIncl", &nGenTrkSelBackIncl_, "nGenTrkSelBackIncl/I");
  eventTree_->Branch("nGenTrkSelAll", &nGenTrkSelAll_, "nGenTrkSelAll/I");
  eventTree_->Branch("nGenTrkSelForw", &nGenTrkSelForw_, "nGenTrkSelForw/I");
  eventTree_->Branch("nGenTrkSelBack", &nGenTrkSelBack_, "nGenTrkSelBack/I");

  eventTree_->Branch("genQx2AllIncl", &genQx2AllIncl_, "genQx2AllIncl/D");
  eventTree_->Branch("genQy2AllIncl", &genQy2AllIncl_, "genQy2AllIncl/D");
  eventTree_->Branch("genPsi2AllIncl", &genPsi2AllIncl_, "genPsi2AllIncl/D");
  eventTree_->Branch("genSumPt2AllIncl", &genSumPt2AllIncl_, "genSumPt2AllIncl/D");
  eventTree_->Branch("genQx2AllForwIncl", &genQx2AllForwIncl_, "genQx2AllForwIncl/D");
  eventTree_->Branch("genQy2AllForwIncl", &genQy2AllForwIncl_, "genQy2AllForwIncl/D");
  eventTree_->Branch("genPsi2AllForwIncl", &genPsi2AllForwIncl_, "genPsi2AllForwIncl/D");
  eventTree_->Branch("genSumPt2AllForwIncl", &genSumPt2AllForwIncl_, "genSumPt2AllForwIncl/D");
  eventTree_->Branch("genQx2AllBackIncl", &genQx2AllBackIncl_, "genQx2AllBackIncl/D");
  eventTree_->Branch("genQy2AllBackIncl", &genQy2AllBackIncl_, "genQy2AllBackIncl/D");
  eventTree_->Branch("genPsi2AllBackIncl", &genPsi2AllBackIncl_, "genPsi2AllBackIncl/D");
  eventTree_->Branch("genSumPt2AllBackIncl", &genSumPt2AllBackIncl_, "genSumPt2AllBackIncl/D");

  eventTree_->Branch("genQx2All", &genQx2All_, "genQx2All/D");
  eventTree_->Branch("genQy2All", &genQy2All_, "genQy2All/D");
  eventTree_->Branch("genPsi2All", &genPsi2All_, "genPsi2All/D");
  eventTree_->Branch("genSumPt2All", &genSumPt2All_, "genSumPt2All/D");
  eventTree_->Branch("genQx2AllForw", &genQx2AllForw_, "genQx2AllForw/D");
  eventTree_->Branch("genQy2AllForw", &genQy2AllForw_, "genQy2AllForw/D");
  eventTree_->Branch("genPsi2AllForw", &genPsi2AllForw_, "genPsi2AllForw/D");
  eventTree_->Branch("genSumPt2AllForw", &genSumPt2AllForw_, "genSumPt2AllForw/D");
  eventTree_->Branch("genQx2AllBack", &genQx2AllBack_, "genQx2AllBack/D");
  eventTree_->Branch("genQy2AllBack", &genQy2AllBack_, "genQy2AllBack/D");
  eventTree_->Branch("genPsi2AllBack", &genPsi2AllBack_, "genPsi2AllBack/D");
  eventTree_->Branch("genSumPt2AllBack", &genSumPt2AllBack_, "genSumPt2AllBack/D");

  eventTree_->Branch("genQx3AllIncl", &genQx3AllIncl_, "genQx3AllIncl/D");
  eventTree_->Branch("genQy3AllIncl", &genQy3AllIncl_, "genQy3AllIncl/D");
  eventTree_->Branch("genPsi3AllIncl", &genPsi3AllIncl_, "genPsi3AllIncl/D");
  eventTree_->Branch("genSumPt3AllIncl", &genSumPt3AllIncl_, "genSumPt3AllIncl/D");
  eventTree_->Branch("genQx3AllForwIncl", &genQx3AllForwIncl_, "genQx3AllForwIncl/D");
  eventTree_->Branch("genQy3AllForwIncl", &genQy3AllForwIncl_, "genQy3AllForwIncl/D");
  eventTree_->Branch("genPsi3AllForwIncl", &genPsi3AllForwIncl_, "genPsi3AllForwIncl/D");
  eventTree_->Branch("genSumPt3AllForwIncl", &genSumPt3AllForwIncl_, "genSumPt3AllForwIncl/D");
  eventTree_->Branch("genQx3AllBackIncl", &genQx3AllBackIncl_, "genQx3AllBackIncl/D");
  eventTree_->Branch("genQy3AllBackIncl", &genQy3AllBackIncl_, "genQy3AllBackIncl/D");
  eventTree_->Branch("genPsi3AllBackIncl", &genPsi3AllBackIncl_, "genPsi3AllBackIncl/D");
  eventTree_->Branch("genSumPt3AllBackIncl", &genSumPt3AllBackIncl_, "genSumPt3AllBackIncl/D");

  eventTree_->Branch("genQx3All", &genQx3All_, "genQx3All/D");
  eventTree_->Branch("genQy3All", &genQy3All_, "genQy3All/D");
  eventTree_->Branch("genPsi3All", &genPsi3All_, "genPsi3All/D");
  eventTree_->Branch("genSumPt3All", &genSumPt3All_, "genSumPt3All/D");
  eventTree_->Branch("genQx3AllForw", &genQx3AllForw_, "genQx3AllForw/D");
  eventTree_->Branch("genQy3AllForw", &genQy3AllForw_, "genQy3AllForw/D");
  eventTree_->Branch("genPsi3AllForw", &genPsi3AllForw_, "genPsi3AllForw/D");
  eventTree_->Branch("genSumPt3AllForw", &genSumPt3AllForw_, "genSumPt3AllForw/D");
  eventTree_->Branch("genQx3AllBack", &genQx3AllBack_, "genQx3AllBack/D");
  eventTree_->Branch("genQy3AllBack", &genQy3AllBack_, "genQy3AllBack/D");
  eventTree_->Branch("genPsi3AllBack", &genPsi3AllBack_, "genPsi3AllBack/D");
  eventTree_->Branch("genSumPt3AllBack", &genSumPt3AllBack_, "genSumPt3AllBack/D");

  eventTree_->Branch("genDstarPt", &genDstarPt_);
  eventTree_->Branch("genDstarY", &genDstarY_);
  eventTree_->Branch("genD0Pt", &genD0Pt_);
  eventTree_->Branch("genD0Y", &genD0Y_);
  eventTree_->Branch("genD0PdgId", &genD0PdgId_);
  eventTree_->Branch("genD0AncestorId", &genD0AncestorId_);
  eventTree_->Branch("genD0AncestorFlavor", &genD0AncestorFlavor_);
  eventTree_->Branch("genD0IsPrompt", &genD0IsPrompt_);
  eventTree_->Branch("genD0CosThetaHX", &genD0CosThetaHX_);
}

void MiniAODGenDstarEventPlaneTrack::initHistogram() {
  hNGenTrkSel_ = fs_->make<TH1D>("hNGenTrkSel", ";N(packed charged gen tracks selected)", 2000, 0, 2000);
}

void MiniAODGenDstarEventPlaneTrack::analyze(const edm::Event& iEvent, const edm::EventSetup&) {
  edm::Handle<reco::GenParticleCollection> pruned;
  iEvent.getByToken(tok_pruned_, pruned);
  if (!pruned.isValid())
    throw cms::Exception("MiniAODGenDstarEventPlaneTrack") << "prunedGenParticles collection not found!";

  edm::Handle<pat::PackedGenParticleCollection> packed;
  iEvent.getByToken(tok_packed_, packed);
  if (!packed.isValid())
    throw cms::Exception("MiniAODGenDstarEventPlaneTrack") << "packedGenParticles collection not found!";

  runNb_ = iEvent.id().run();
  eventNb_ = iEvent.id().event();
  lsNb_ = iEvent.luminosityBlock();

  centrality_ = -1;
  Ntrkoffline_ = -1;
  if (isCentrality_) {
    const auto& cent = iEvent.getHandle(tok_centSrc_);
    Ntrkoffline_ = (cent.isValid() ? cent->Ntracks() : -1);
    edm::Handle<int> cbin;
    iEvent.getByToken(tok_centBinLabel_, cbin);
    centrality_ = (cbin.isValid() ? *cbin : -1);
  }

  genDstarPt_.clear();
  genDstarY_.clear();
  genD0Pt_.clear();
  genD0Y_.clear();
  genD0PdgId_.clear();
  genD0AncestorId_.clear();
  genD0AncestorFlavor_.clear();
  genD0IsPrompt_.clear();
  genD0CosThetaHX_.clear();

  const std::unordered_set<const pat::PackedGenParticle*> noExcluded;
  const auto q2InclFull = computeQVector(
      *packed, harmonic2_, noExcluded, trackPtMin_, trackPtMax_, trackAbsEtaMax_, -trackAbsEtaMax_, trackAbsEtaMax_);
  const auto q2InclForw = computeQVector(
      *packed, harmonic2_, noExcluded, trackPtMin_, trackPtMax_, trackAbsEtaMax_, subEventAbsEtaMin_, trackAbsEtaMax_);
  const auto q2InclBack = computeQVector(
      *packed, harmonic2_, noExcluded, trackPtMin_, trackPtMax_, trackAbsEtaMax_, -trackAbsEtaMax_, -subEventAbsEtaMin_);

  const auto q3InclFull = computeQVector(
      *packed, harmonic3_, noExcluded, trackPtMin_, trackPtMax_, trackAbsEtaMax_, -trackAbsEtaMax_, trackAbsEtaMax_);
  const auto q3InclForw = computeQVector(
      *packed, harmonic3_, noExcluded, trackPtMin_, trackPtMax_, trackAbsEtaMax_, subEventAbsEtaMin_, trackAbsEtaMax_);
  const auto q3InclBack = computeQVector(
      *packed, harmonic3_, noExcluded, trackPtMin_, trackPtMax_, trackAbsEtaMax_, -trackAbsEtaMax_, -subEventAbsEtaMin_);

  nGenTrkSelAllIncl_ = q2InclFull.n;
  nGenTrkSelForwIncl_ = q2InclForw.n;
  nGenTrkSelBackIncl_ = q2InclBack.n;

  genQx2AllIncl_ = q2InclFull.qx;
  genQy2AllIncl_ = q2InclFull.qy;
  genPsi2AllIncl_ = computePsi(genQx2AllIncl_, genQy2AllIncl_, harmonic2_);
  genSumPt2AllIncl_ = q2InclFull.sumW;
  genQx2AllForwIncl_ = q2InclForw.qx;
  genQy2AllForwIncl_ = q2InclForw.qy;
  genPsi2AllForwIncl_ = computePsi(genQx2AllForwIncl_, genQy2AllForwIncl_, harmonic2_);
  genSumPt2AllForwIncl_ = q2InclForw.sumW;
  genQx2AllBackIncl_ = q2InclBack.qx;
  genQy2AllBackIncl_ = q2InclBack.qy;
  genPsi2AllBackIncl_ = computePsi(genQx2AllBackIncl_, genQy2AllBackIncl_, harmonic2_);
  genSumPt2AllBackIncl_ = q2InclBack.sumW;

  genQx3AllIncl_ = q3InclFull.qx;
  genQy3AllIncl_ = q3InclFull.qy;
  genPsi3AllIncl_ = computePsi(genQx3AllIncl_, genQy3AllIncl_, harmonic3_);
  genSumPt3AllIncl_ = q3InclFull.sumW;
  genQx3AllForwIncl_ = q3InclForw.qx;
  genQy3AllForwIncl_ = q3InclForw.qy;
  genPsi3AllForwIncl_ = computePsi(genQx3AllForwIncl_, genQy3AllForwIncl_, harmonic3_);
  genSumPt3AllForwIncl_ = q3InclForw.sumW;
  genQx3AllBackIncl_ = q3InclBack.qx;
  genQy3AllBackIncl_ = q3InclBack.qy;
  genPsi3AllBackIncl_ = computePsi(genQx3AllBackIncl_, genQy3AllBackIncl_, harmonic3_);
  genSumPt3AllBackIncl_ = q3InclBack.sumW;

  // Build the union of D* daughter packed tracks to exclude from EP (autocorrelation removal).
  std::unordered_set<const pat::PackedGenParticle*> dstarDaughterPacked;
  nGenDstar_ = 0;
  for (const auto& gp : *pruned) {
    if (std::abs(gp.pdgId()) != 413)
      continue;

    const int sign = (gp.pdgId() > 0 ? 1 : -1);

    const reco::Candidate* d0Cand = nullptr;
    const reco::Candidate* softPiCand = nullptr;
    if (!twoBodyDecay(gp, sign * 421, sign * 211, d0Cand, softPiCand))
      continue;
    (void)softPiCand;

    const auto* d0Gen = dynamic_cast<const reco::GenParticle*>(d0Cand);
    if (!d0Gen)
      continue;

    const reco::GenParticle* dstarLast = lastCopy(gp);
    const reco::GenParticle* d0Last = lastCopy(*d0Gen);
    const reco::Candidate* kaonCand = findDaughterByAbsPdg(*d0Last, 321);
    if (!kaonCand)
      continue;

    const pat::PackedGenParticle* softPiPacked = findPackedWithAncestor(*packed, sign * 211, dstarLast);
    const pat::PackedGenParticle* kPacked = findPackedWithAncestor(*packed, -sign * 321, d0Last);
    const pat::PackedGenParticle* piFromD0Packed = findPackedWithAncestor(*packed, sign * 211, d0Last);
    if (!softPiPacked || !kPacked || !piFromD0Packed)
      continue;

    ++nGenDstar_;
    int ancestorId = 0;
    int ancestorFlavor = 0;
    getAncestorId(*d0Last, ancestorId, ancestorFlavor);

    genDstarPt_.push_back(dstarLast->pt());
    genDstarY_.push_back(dstarLast->rapidity());
    genD0Pt_.push_back(d0Last->pt());
    genD0Y_.push_back(d0Last->rapidity());
    genD0PdgId_.push_back(d0Last->pdgId());
    genD0AncestorId_.push_back(ancestorId);
    genD0AncestorFlavor_.push_back(ancestorFlavor);
    genD0IsPrompt_.push_back(ancestorFlavor == 5 ? 0 : 1);
    genD0CosThetaHX_.push_back(computeHelicityCosTheta(*dstarLast, *d0Last));

    dstarDaughterPacked.insert(softPiPacked);
    dstarDaughterPacked.insert(kPacked);
    dstarDaughterPacked.insert(piFromD0Packed);
  }
  nGenDstarDauTrkExcluded_ = static_cast<int>(dstarDaughterPacked.size());

  const auto q2ExclFull = computeQVector(
      *packed, harmonic2_, dstarDaughterPacked, trackPtMin_, trackPtMax_, trackAbsEtaMax_, -trackAbsEtaMax_, trackAbsEtaMax_);
  const auto q2ExclForw = computeQVector(
      *packed, harmonic2_, dstarDaughterPacked, trackPtMin_, trackPtMax_, trackAbsEtaMax_, subEventAbsEtaMin_, trackAbsEtaMax_);
  const auto q2ExclBack = computeQVector(
      *packed, harmonic2_, dstarDaughterPacked, trackPtMin_, trackPtMax_, trackAbsEtaMax_, -trackAbsEtaMax_, -subEventAbsEtaMin_);
  const auto q3ExclFull = computeQVector(
      *packed, harmonic3_, dstarDaughterPacked, trackPtMin_, trackPtMax_, trackAbsEtaMax_, -trackAbsEtaMax_, trackAbsEtaMax_);
  const auto q3ExclForw = computeQVector(
      *packed, harmonic3_, dstarDaughterPacked, trackPtMin_, trackPtMax_, trackAbsEtaMax_, subEventAbsEtaMin_, trackAbsEtaMax_);
  const auto q3ExclBack = computeQVector(
      *packed, harmonic3_, dstarDaughterPacked, trackPtMin_, trackPtMax_, trackAbsEtaMax_, -trackAbsEtaMax_, -subEventAbsEtaMin_);

  nGenTrkSelAll_ = q2ExclFull.n;
  nGenTrkSelForw_ = q2ExclForw.n;
  nGenTrkSelBack_ = q2ExclBack.n;
  if (saveHistogram_ && hNGenTrkSel_)
    hNGenTrkSel_->Fill(nGenTrkSelAll_);

  genQx2All_ = q2ExclFull.qx;
  genQy2All_ = q2ExclFull.qy;
  genPsi2All_ = computePsi(genQx2All_, genQy2All_, harmonic2_);
  genSumPt2All_ = q2ExclFull.sumW;
  genQx2AllForw_ = q2ExclForw.qx;
  genQy2AllForw_ = q2ExclForw.qy;
  genPsi2AllForw_ = computePsi(genQx2AllForw_, genQy2AllForw_, harmonic2_);
  genSumPt2AllForw_ = q2ExclForw.sumW;
  genQx2AllBack_ = q2ExclBack.qx;
  genQy2AllBack_ = q2ExclBack.qy;
  genPsi2AllBack_ = computePsi(genQx2AllBack_, genQy2AllBack_, harmonic2_);
  genSumPt2AllBack_ = q2ExclBack.sumW;

  genQx3All_ = q3ExclFull.qx;
  genQy3All_ = q3ExclFull.qy;
  genPsi3All_ = computePsi(genQx3All_, genQy3All_, harmonic3_);
  genSumPt3All_ = q3ExclFull.sumW;
  genQx3AllForw_ = q3ExclForw.qx;
  genQy3AllForw_ = q3ExclForw.qy;
  genPsi3AllForw_ = computePsi(genQx3AllForw_, genQy3AllForw_, harmonic3_);
  genSumPt3AllForw_ = q3ExclForw.sumW;
  genQx3AllBack_ = q3ExclBack.qx;
  genQy3AllBack_ = q3ExclBack.qy;
  genPsi3AllBack_ = computePsi(genQx3AllBack_, genQy3AllBack_, harmonic3_);
  genSumPt3AllBack_ = q3ExclBack.sumW;

  if (saveTree_ && eventTree_)
    eventTree_->Fill();
}

DEFINE_FWK_MODULE(MiniAODGenDstarEventPlaneTrack);
