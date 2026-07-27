// system include files
#include <array>
#include <cmath>
#include <cstdint>
#include <vector>

#include <TH1.h>
#include <TTree.h>

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
#include "DataFormats/HepMCCandidate/interface/GenParticle.h"
#include "DataFormats/HeavyIonEvent/interface/Centrality.h"

namespace {
  constexpr double kInvalid = -999.0;

  struct QVectorResult {
    double qx = kInvalid;
    double qy = kInvalid;
    double sumW = 0.0;
    int n = 0;
  };

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

  bool matchesDstarToKPiPi(const reco::GenParticle& dstar,
                           std::array<const reco::Candidate*, 3>& daughters) {
    const int dstarId = dstar.pdgId();
    if (std::abs(dstarId) != 413)
      return false;
    const int sign = (dstarId > 0 ? 1 : -1);

    const reco::Candidate* d0Cand = nullptr;
    const reco::Candidate* softPiCand = nullptr;
    if (!twoBodyDecay(dstar, sign * 421, sign * 211, d0Cand, softPiCand))
      return false;

    const auto* d0Gen = dynamic_cast<const reco::GenParticle*>(d0Cand);
    if (!d0Gen)
      return false;

    const reco::Candidate* kCand = nullptr;
    const reco::Candidate* piFromD0Cand = nullptr;
    if (!twoBodyDecay(*d0Gen, -sign * 321, sign * 211, kCand, piFromD0Cand))
      return false;

    daughters = {{kCand, piFromD0Cand, softPiCand}};

    for (const auto* dau : daughters) {
      if (!dau)
        return false;
      if (dau->charge() == 0)
        return false;
    }

    return true;
  }

  bool isExcluded(const reco::Candidate& cand, const std::array<const reco::Candidate*, 3>& excluded) {
    const reco::Candidate* ptr = &cand;
    for (const auto* ex : excluded) {
      if (ex == ptr)
        return true;
    }
    return false;
  }

  QVectorResult computeQVector(const reco::GenParticleCollection& genParticles,
                               int harmonic,
                               const std::array<const reco::Candidate*, 3>& excluded,
                               bool applyExclusion,
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

    for (const auto& gp : genParticles) {
      if (gp.status() != 1)
        continue;
      if (gp.charge() == 0)
        continue;
      if (applyExclusion && isExcluded(gp, excluded))
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
}  // namespace

class GenDstarEventPlaneTrack : public edm::one::EDAnalyzer<edm::one::WatchRuns> {
public:
  explicit GenDstarEventPlaneTrack(const edm::ParameterSet&);
  ~GenDstarEventPlaneTrack() override = default;

private:
  void beginJob() override;
  void beginRun(const edm::Run&, const edm::EventSetup&) override {}
  void endRun(const edm::Run&, const edm::EventSetup&) override {}
  void analyze(const edm::Event&, const edm::EventSetup&) override;
  void endJob() override {}

  void initTree();
  void initHistogram();

  edm::Service<TFileService> fs_;

  TTree* tree_ = nullptr;
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

  edm::EDGetTokenT<reco::GenParticleCollection> tok_genParticles_;
  edm::EDGetTokenT<int> tok_centBinLabel_;
  edm::EDGetTokenT<reco::Centrality> tok_centSrc_;

  // tree variables (one entry per gen D*)
  uint32_t runNb_ = 0;
  uint32_t eventNb_ = 0;
  uint32_t lsNb_ = 0;
  short centrality_ = -1;
  int Ntrkoffline_ = -1;
  int nGenDstar_ = 0;

  int dstarPdgId_ = 0;
  float dstarPt_ = 0;
  float dstarEta_ = 0;
  float dstarPhi_ = 0;
  float dstarMass_ = 0;

  float kPt_ = 0, kEta_ = 0, kPhi_ = 0;
  float piFromD0Pt_ = 0, piFromD0Eta_ = 0, piFromD0Phi_ = 0;
  float softPiPt_ = 0, softPiEta_ = 0, softPiPhi_ = 0;

  int nGenTrkSelAll_ = 0;
  int nGenTrkSelForw_ = 0;
  int nGenTrkSelBack_ = 0;

  double genQx2_ = kInvalid, genQy2_ = kInvalid, genPsi2_ = kInvalid, genSumPt2_ = 0;
  double genQx2All_ = kInvalid, genQy2All_ = kInvalid, genPsi2All_ = kInvalid, genSumPt2All_ = 0;
  double genQx2AllForw_ = kInvalid, genQy2AllForw_ = kInvalid, genPsi2AllForw_ = kInvalid, genSumPt2AllForw_ = 0;
  double genQx2AllBack_ = kInvalid, genQy2AllBack_ = kInvalid, genPsi2AllBack_ = kInvalid, genSumPt2AllBack_ = 0;
  double genQx2Forw_ = kInvalid, genQy2Forw_ = kInvalid, genPsi2Forw_ = kInvalid, genSumPt2Forw_ = 0;
  double genQx2Back_ = kInvalid, genQy2Back_ = kInvalid, genPsi2Back_ = kInvalid, genSumPt2Back_ = 0;

  double genQx3_ = kInvalid, genQy3_ = kInvalid, genPsi3_ = kInvalid, genSumPt3_ = 0;
  double genQx3All_ = kInvalid, genQy3All_ = kInvalid, genPsi3All_ = kInvalid, genSumPt3All_ = 0;
  double genQx3AllForw_ = kInvalid, genQy3AllForw_ = kInvalid, genPsi3AllForw_ = kInvalid, genSumPt3AllForw_ = 0;
  double genQx3AllBack_ = kInvalid, genQy3AllBack_ = kInvalid, genPsi3AllBack_ = kInvalid, genSumPt3AllBack_ = 0;
  double genQx3Forw_ = kInvalid, genQy3Forw_ = kInvalid, genPsi3Forw_ = kInvalid, genSumPt3Forw_ = 0;
  double genQx3Back_ = kInvalid, genQy3Back_ = kInvalid, genPsi3Back_ = kInvalid, genSumPt3Back_ = 0;
};

GenDstarEventPlaneTrack::GenDstarEventPlaneTrack(const edm::ParameterSet& iConfig) {
  saveTree_ = iConfig.getUntrackedParameter<bool>("saveTree", true);
  saveHistogram_ = iConfig.getUntrackedParameter<bool>("saveHistogram", false);

  trackPtMin_ = iConfig.getUntrackedParameter<double>("trackPtMin", 0.3);
  trackPtMax_ = iConfig.getUntrackedParameter<double>("trackPtMax", 3.0);
  trackAbsEtaMax_ = iConfig.getUntrackedParameter<double>("trackAbsEtaMax", 2.4);
  subEventAbsEtaMin_ = iConfig.getUntrackedParameter<double>("subEventAbsEtaMin", 0.5);

  harmonic2_ = iConfig.getUntrackedParameter<int>("harmonic2", 2);
  harmonic3_ = iConfig.getUntrackedParameter<int>("harmonic3", 3);

  tok_genParticles_ = consumes<reco::GenParticleCollection>(
      edm::InputTag(iConfig.getUntrackedParameter<edm::InputTag>("GenParticleCollection")));

  isCentrality_ = (iConfig.exists("isCentrality") ? iConfig.getParameter<bool>("isCentrality") : false);
  if (isCentrality_) {
    tok_centBinLabel_ = consumes<int>(iConfig.getParameter<edm::InputTag>("centralityBinLabel"));
    tok_centSrc_ = consumes<reco::Centrality>(iConfig.getParameter<edm::InputTag>("centralitySrc"));
  }
}

void GenDstarEventPlaneTrack::beginJob() {
  TH1D::SetDefaultSumw2();
  if (saveTree_)
    initTree();
  if (saveHistogram_)
    initHistogram();
}

void GenDstarEventPlaneTrack::initTree() {
  tree_ = fs_->make<TTree>("GenDstarEventPlane", "GenDstarEventPlane");
  eventTree_ = fs_->make<TTree>("GenEventPlane", "GenEventPlane");

  tree_->Branch("RunNb", &runNb_, "RunNb/i");
  tree_->Branch("LSNb", &lsNb_, "LSNb/i");
  tree_->Branch("EventNb", &eventNb_, "EventNb/i");
  if (isCentrality_) {
    tree_->Branch("centrality", &centrality_, "centrality/S");
    tree_->Branch("Ntrkoffline", &Ntrkoffline_, "Ntrkoffline/I");
  }

  tree_->Branch("dstarPdgId", &dstarPdgId_, "dstarPdgId/I");
  tree_->Branch("dstarPt", &dstarPt_, "dstarPt/F");
  tree_->Branch("dstarEta", &dstarEta_, "dstarEta/F");
  tree_->Branch("dstarPhi", &dstarPhi_, "dstarPhi/F");
  tree_->Branch("dstarMass", &dstarMass_, "dstarMass/F");

  tree_->Branch("kPt", &kPt_, "kPt/F");
  tree_->Branch("kEta", &kEta_, "kEta/F");
  tree_->Branch("kPhi", &kPhi_, "kPhi/F");
  tree_->Branch("piFromD0Pt", &piFromD0Pt_, "piFromD0Pt/F");
  tree_->Branch("piFromD0Eta", &piFromD0Eta_, "piFromD0Eta/F");
  tree_->Branch("piFromD0Phi", &piFromD0Phi_, "piFromD0Phi/F");
  tree_->Branch("softPiPt", &softPiPt_, "softPiPt/F");
  tree_->Branch("softPiEta", &softPiEta_, "softPiEta/F");
  tree_->Branch("softPiPhi", &softPiPhi_, "softPiPhi/F");

  tree_->Branch("nGenTrkSelAll", &nGenTrkSelAll_, "nGenTrkSelAll/I");
  tree_->Branch("nGenTrkSelForw", &nGenTrkSelForw_, "nGenTrkSelForw/I");
  tree_->Branch("nGenTrkSelBack", &nGenTrkSelBack_, "nGenTrkSelBack/I");

  tree_->Branch("genQx2", &genQx2_, "genQx2/D");
  tree_->Branch("genQy2", &genQy2_, "genQy2/D");
  tree_->Branch("genPsi2", &genPsi2_, "genPsi2/D");
  tree_->Branch("genSumPt2", &genSumPt2_, "genSumPt2/D");
  tree_->Branch("genQx2All", &genQx2All_, "genQx2All/D");
  tree_->Branch("genQy2All", &genQy2All_, "genQy2All/D");
  tree_->Branch("genPsi2All", &genPsi2All_, "genPsi2All/D");
  tree_->Branch("genSumPt2All", &genSumPt2All_, "genSumPt2All/D");
  tree_->Branch("genQx2Forw", &genQx2Forw_, "genQx2Forw/D");
  tree_->Branch("genQy2Forw", &genQy2Forw_, "genQy2Forw/D");
  tree_->Branch("genPsi2Forw", &genPsi2Forw_, "genPsi2Forw/D");
  tree_->Branch("genSumPt2Forw", &genSumPt2Forw_, "genSumPt2Forw/D");
  tree_->Branch("genQx2Back", &genQx2Back_, "genQx2Back/D");
  tree_->Branch("genQy2Back", &genQy2Back_, "genQy2Back/D");
  tree_->Branch("genPsi2Back", &genPsi2Back_, "genPsi2Back/D");
  tree_->Branch("genSumPt2Back", &genSumPt2Back_, "genSumPt2Back/D");

  tree_->Branch("genQx3", &genQx3_, "genQx3/D");
  tree_->Branch("genQy3", &genQy3_, "genQy3/D");
  tree_->Branch("genPsi3", &genPsi3_, "genPsi3/D");
  tree_->Branch("genSumPt3", &genSumPt3_, "genSumPt3/D");
  tree_->Branch("genQx3All", &genQx3All_, "genQx3All/D");
  tree_->Branch("genQy3All", &genQy3All_, "genQy3All/D");
  tree_->Branch("genPsi3All", &genPsi3All_, "genPsi3All/D");
  tree_->Branch("genSumPt3All", &genSumPt3All_, "genSumPt3All/D");
  tree_->Branch("genQx3Forw", &genQx3Forw_, "genQx3Forw/D");
  tree_->Branch("genQy3Forw", &genQy3Forw_, "genQy3Forw/D");
  tree_->Branch("genPsi3Forw", &genPsi3Forw_, "genPsi3Forw/D");
  tree_->Branch("genSumPt3Forw", &genSumPt3Forw_, "genSumPt3Forw/D");
  tree_->Branch("genQx3Back", &genQx3Back_, "genQx3Back/D");
  tree_->Branch("genQy3Back", &genQy3Back_, "genQy3Back/D");
  tree_->Branch("genPsi3Back", &genPsi3Back_, "genPsi3Back/D");
  tree_->Branch("genSumPt3Back", &genSumPt3Back_, "genSumPt3Back/D");

  eventTree_->Branch("RunNb", &runNb_, "RunNb/i");
  eventTree_->Branch("LSNb", &lsNb_, "LSNb/i");
  eventTree_->Branch("EventNb", &eventNb_, "EventNb/i");
  if (isCentrality_) {
    eventTree_->Branch("centrality", &centrality_, "centrality/S");
    eventTree_->Branch("Ntrkoffline", &Ntrkoffline_, "Ntrkoffline/I");
  }
  eventTree_->Branch("nGenDstar", &nGenDstar_, "nGenDstar/I");
  eventTree_->Branch("nGenTrkSelAll", &nGenTrkSelAll_, "nGenTrkSelAll/I");
  eventTree_->Branch("nGenTrkSelForw", &nGenTrkSelForw_, "nGenTrkSelForw/I");
  eventTree_->Branch("nGenTrkSelBack", &nGenTrkSelBack_, "nGenTrkSelBack/I");

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
}

void GenDstarEventPlaneTrack::initHistogram() {
  hNGenTrkSel_ = fs_->make<TH1D>("hNGenTrkSel", ";N(gen charged tracks selected)", 2000, 0, 2000);
}

void GenDstarEventPlaneTrack::analyze(const edm::Event& iEvent, const edm::EventSetup&) {
  edm::Handle<reco::GenParticleCollection> genParticles;
  iEvent.getByToken(tok_genParticles_, genParticles);
  if (!genParticles.isValid())
    throw cms::Exception("GenDstarEventPlaneTrack") << "GenParticle collection not found!";

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

  // Pre-compute "all tracks" multiplicities (no exclusion).
  const std::array<const reco::Candidate*, 3> emptyExcluded{{nullptr, nullptr, nullptr}};
  const auto q2AllFull = computeQVector(*genParticles,
                                        harmonic2_,
                                        emptyExcluded,
                                        false,
                                        trackPtMin_,
                                        trackPtMax_,
                                        trackAbsEtaMax_,
                                        -trackAbsEtaMax_,
                                        trackAbsEtaMax_);
  const auto q2AllForw = computeQVector(*genParticles,
                                        harmonic2_,
                                        emptyExcluded,
                                        false,
                                        trackPtMin_,
                                        trackPtMax_,
                                        trackAbsEtaMax_,
                                        subEventAbsEtaMin_,
                                        trackAbsEtaMax_);
  const auto q2AllBack = computeQVector(*genParticles,
                                        harmonic2_,
                                        emptyExcluded,
                                        false,
                                        trackPtMin_,
                                        trackPtMax_,
                                        trackAbsEtaMax_,
                                        -trackAbsEtaMax_,
                                        -subEventAbsEtaMin_);
  const auto q3AllFull = computeQVector(*genParticles,
                                        harmonic3_,
                                        emptyExcluded,
                                        false,
                                        trackPtMin_,
                                        trackPtMax_,
                                        trackAbsEtaMax_,
                                        -trackAbsEtaMax_,
                                        trackAbsEtaMax_);
  const auto q3AllForw = computeQVector(*genParticles,
                                        harmonic3_,
                                        emptyExcluded,
                                        false,
                                        trackPtMin_,
                                        trackPtMax_,
                                        trackAbsEtaMax_,
                                        subEventAbsEtaMin_,
                                        trackAbsEtaMax_);
  const auto q3AllBack = computeQVector(*genParticles,
                                        harmonic3_,
                                        emptyExcluded,
                                        false,
                                        trackPtMin_,
                                        trackPtMax_,
                                        trackAbsEtaMax_,
                                        -trackAbsEtaMax_,
                                        -subEventAbsEtaMin_);
  nGenTrkSelAll_ = q2AllFull.n;
  nGenTrkSelForw_ = q2AllForw.n;
  nGenTrkSelBack_ = q2AllBack.n;
  if (saveHistogram_ && hNGenTrkSel_)
    hNGenTrkSel_->Fill(nGenTrkSelAll_);

  genQx2All_ = q2AllFull.qx;
  genQy2All_ = q2AllFull.qy;
  genPsi2All_ = computePsi(genQx2All_, genQy2All_, harmonic2_);
  genSumPt2All_ = q2AllFull.sumW;

  genQx2AllForw_ = q2AllForw.qx;
  genQy2AllForw_ = q2AllForw.qy;
  genPsi2AllForw_ = computePsi(genQx2AllForw_, genQy2AllForw_, harmonic2_);
  genSumPt2AllForw_ = q2AllForw.sumW;

  genQx2AllBack_ = q2AllBack.qx;
  genQy2AllBack_ = q2AllBack.qy;
  genPsi2AllBack_ = computePsi(genQx2AllBack_, genQy2AllBack_, harmonic2_);
  genSumPt2AllBack_ = q2AllBack.sumW;

  genQx3All_ = q3AllFull.qx;
  genQy3All_ = q3AllFull.qy;
  genPsi3All_ = computePsi(genQx3All_, genQy3All_, harmonic3_);
  genSumPt3All_ = q3AllFull.sumW;

  genQx3AllForw_ = q3AllForw.qx;
  genQy3AllForw_ = q3AllForw.qy;
  genPsi3AllForw_ = computePsi(genQx3AllForw_, genQy3AllForw_, harmonic3_);
  genSumPt3AllForw_ = q3AllForw.sumW;

  genQx3AllBack_ = q3AllBack.qx;
  genQy3AllBack_ = q3AllBack.qy;
  genPsi3AllBack_ = computePsi(genQx3AllBack_, genQy3AllBack_, harmonic3_);
  genSumPt3AllBack_ = q3AllBack.sumW;

  // Per-event tree: count matched gen D* and fill exactly once per event.
  nGenDstar_ = 0;
  for (const auto& gp : *genParticles) {
    if (std::abs(gp.pdgId()) != 413)
      continue;
    std::array<const reco::Candidate*, 3> excluded;
    if (matchesDstarToKPiPi(gp, excluded))
      ++nGenDstar_;
  }
  if (saveTree_ && eventTree_)
    eventTree_->Fill();

  // Loop over gen D* and fill one tree entry per D* that matches D* -> D0(pi_s), D0 -> K pi.
  for (const auto& gp : *genParticles) {
    if (std::abs(gp.pdgId()) != 413)
      continue;

    std::array<const reco::Candidate*, 3> excluded;
    if (!matchesDstarToKPiPi(gp, excluded))
      continue;

    dstarPdgId_ = gp.pdgId();
    dstarPt_ = gp.pt();
    dstarEta_ = gp.eta();
    dstarPhi_ = gp.phi();
    dstarMass_ = gp.mass();

    kPt_ = excluded[0]->pt();
    kEta_ = excluded[0]->eta();
    kPhi_ = excluded[0]->phi();
    piFromD0Pt_ = excluded[1]->pt();
    piFromD0Eta_ = excluded[1]->eta();
    piFromD0Phi_ = excluded[1]->phi();
    softPiPt_ = excluded[2]->pt();
    softPiEta_ = excluded[2]->eta();
    softPiPhi_ = excluded[2]->phi();

    const auto q2ExclFull = computeQVector(*genParticles,
                                           harmonic2_,
                                           excluded,
                                           true,
                                           trackPtMin_,
                                           trackPtMax_,
                                           trackAbsEtaMax_,
                                           -trackAbsEtaMax_,
                                           trackAbsEtaMax_);
    const auto q2ExclForw = computeQVector(*genParticles,
                                           harmonic2_,
                                           excluded,
                                           true,
                                           trackPtMin_,
                                           trackPtMax_,
                                           trackAbsEtaMax_,
                                           subEventAbsEtaMin_,
                                           trackAbsEtaMax_);
    const auto q2ExclBack = computeQVector(*genParticles,
                                           harmonic2_,
                                           excluded,
                                           true,
                                           trackPtMin_,
                                           trackPtMax_,
                                           trackAbsEtaMax_,
                                           -trackAbsEtaMax_,
                                           -subEventAbsEtaMin_);

    const auto q3ExclFull = computeQVector(*genParticles,
                                           harmonic3_,
                                           excluded,
                                           true,
                                           trackPtMin_,
                                           trackPtMax_,
                                           trackAbsEtaMax_,
                                           -trackAbsEtaMax_,
                                           trackAbsEtaMax_);
    const auto q3ExclForw = computeQVector(*genParticles,
                                           harmonic3_,
                                           excluded,
                                           true,
                                           trackPtMin_,
                                           trackPtMax_,
                                           trackAbsEtaMax_,
                                           subEventAbsEtaMin_,
                                           trackAbsEtaMax_);
    const auto q3ExclBack = computeQVector(*genParticles,
                                           harmonic3_,
                                           excluded,
                                           true,
                                           trackPtMin_,
                                           trackPtMax_,
                                           trackAbsEtaMax_,
                                           -trackAbsEtaMax_,
                                           -subEventAbsEtaMin_);

    genQx2_ = q2ExclFull.qx;
    genQy2_ = q2ExclFull.qy;
    genPsi2_ = computePsi(genQx2_, genQy2_, harmonic2_);
    genSumPt2_ = q2ExclFull.sumW;
    genQx2Forw_ = q2ExclForw.qx;
    genQy2Forw_ = q2ExclForw.qy;
    genPsi2Forw_ = computePsi(genQx2Forw_, genQy2Forw_, harmonic2_);
    genSumPt2Forw_ = q2ExclForw.sumW;
    genQx2Back_ = q2ExclBack.qx;
    genQy2Back_ = q2ExclBack.qy;
    genPsi2Back_ = computePsi(genQx2Back_, genQy2Back_, harmonic2_);
    genSumPt2Back_ = q2ExclBack.sumW;

    genQx3_ = q3ExclFull.qx;
    genQy3_ = q3ExclFull.qy;
    genPsi3_ = computePsi(genQx3_, genQy3_, harmonic3_);
    genSumPt3_ = q3ExclFull.sumW;
    genQx3Forw_ = q3ExclForw.qx;
    genQy3Forw_ = q3ExclForw.qy;
    genPsi3Forw_ = computePsi(genQx3Forw_, genQy3Forw_, harmonic3_);
    genSumPt3Forw_ = q3ExclForw.sumW;
    genQx3Back_ = q3ExclBack.qx;
    genQy3Back_ = q3ExclBack.qy;
    genPsi3Back_ = computePsi(genQx3Back_, genQy3Back_, harmonic3_);
    genSumPt3Back_ = q3ExclBack.sumW;

    if (saveTree_ && tree_)
      tree_->Fill();
  }
}

DEFINE_FWK_MODULE(GenDstarEventPlaneTrack);
