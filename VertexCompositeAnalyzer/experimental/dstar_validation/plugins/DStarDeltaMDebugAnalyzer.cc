#include "FWCore/Framework/interface/one/EDAnalyzer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/ServiceRegistry/interface/Service.h"
#include "CommonTools/UtilAlgos/interface/TFileService.h"
#include "DataFormats/PatCandidates/interface/CompositeCandidate.h"
#include "DataFormats/RecoCandidate/interface/RecoCandidate.h"
#include "DataFormats/TrackReco/interface/Track.h"
#include "DataFormats/TrackReco/interface/TrackFwd.h"
#include "DataFormats/Math/interface/LorentzVector.h"

#include "TH1D.h"
#include "TH2D.h"

#include <array>
#include <cmath>
#include <limits>
#include <string>
#include <vector>

class DStarDeltaMDebugAnalyzer : public edm::one::EDAnalyzer<edm::one::SharedResources> {
public:
  explicit DStarDeltaMDebugAnalyzer(const edm::ParameterSet&);
  void analyze(const edm::Event&, const edm::EventSetup&) override;
  void endJob() override;

private:
  enum Category { kA = 0, kB = 1, kC = 2, kD = 3, kNCategory = 4 };
  enum CutStep {
    kAllKPiPairs = 0,
    kBeforeD0MassWindow,
    kAfterD0MassWindow,
    kAfterD0Topology,
    kAfterSlowPionAttach,
    kAfterDeltaMCalculated,
    kDeltaMLt0500,
    kDeltaMLt0300,
    kDeltaMLt0250,
    kDeltaMLt0200,
    kDeltaMLt0180,
    kDeltaMLt0165,
    kDeltaMLt0160,
    kAfterCosBinning,
    kNCutStep
  };

  struct CatData {
    std::array<unsigned long long, kNCutStep> cutflow{};
    double minDeltaM = std::numeric_limits<double>::infinity();
    double maxDeltaM = -std::numeric_limits<double>::infinity();
    unsigned long long nDeltaMLtPionMass = 0;
    unsigned long long nInvalidMass = 0;
    unsigned long long nDuplicateTrack = 0;
    std::array<unsigned long long, 5> nSwapVetoWindow{};
  };

  static constexpr double kPionMass = 0.13957018;
  static constexpr double kKaonMass = 0.493677;
  static constexpr double kD0Mass = 1.86484;
  static constexpr std::array<double, 5> kSwapWindows{{0.008, 0.012, 0.016, 0.020, 0.025}};

  int categoryIndex(int qK, int qPiD0, int qPiS) const;
  bool passSlowPionTrack(const reco::Track&) const;
  double massWithHypothesis(const reco::Candidate&, const reco::Candidate&, double, double) const;
  void fillCutflow(Category, CutStep);

  edm::EDGetTokenT<pat::CompositeCandidateCollection> d0RSToken_;
  edm::EDGetTokenT<pat::CompositeCandidateCollection> d0SSToken_;
  edm::EDGetTokenT<reco::TrackCollection> trackToken_;

  double tkChi2Cut_;
  int tkNhitsCut_;
  double tkPtCut_;
  double tkPtErrCut_;
  double tkEtaCut_;
  bool rejectDuplicateTrack_;
  std::vector<std::string> trackQualities_;

  std::array<CatData, kNCategory> data_;
  std::array<TH1D*, kNCategory> hDeltaM0500_{};
  std::array<TH1D*, kNCategory> hDeltaM0300_{};
  std::array<TH1D*, kNCategory> hDeltaM0200_{};
  std::array<TH1D*, kNCategory> hDeltaM0170_{};
  std::array<TH1D*, kNCategory> hQ0100_{};
  std::array<TH1D*, kNCategory> hQ0050_{};
  std::array<TH1D*, kNCategory> hQ0025_{};
  std::array<TH2D*, kNCategory> hDstarVsD0Mass_{};
  std::array<TH2D*, kNCategory> hDeltaMVsSlowPiPt_{};
  std::array<TH2D*, kNCategory> hDeltaMVsD0Pt_{};
  std::array<TH2D*, kNCategory> hDeltaMVsOpeningAngle_{};
  std::array<TH2D*, kNCategory> hQVsOpeningAngle_{};
  TH1D* hCatBNormalMass_ = nullptr;
  TH1D* hCatBSwapMass_ = nullptr;
  TH1D* hCatBDeltaMBeforeSwapVeto_ = nullptr;
  std::array<TH1D*, 5> hCatBDeltaMAfterSwapVeto_{};
  std::array<TH1D*, 5> hCatADeltaMAfterSwapVeto_{};
};

DStarDeltaMDebugAnalyzer::DStarDeltaMDebugAnalyzer(const edm::ParameterSet& iConfig)
    : d0RSToken_(consumes<pat::CompositeCandidateCollection>(iConfig.getParameter<edm::InputTag>("d0RS"))),
      d0SSToken_(consumes<pat::CompositeCandidateCollection>(iConfig.getParameter<edm::InputTag>("d0SS"))),
      trackToken_(consumes<reco::TrackCollection>(iConfig.getParameter<edm::InputTag>("tracks"))),
      tkChi2Cut_(iConfig.getParameter<double>("tkChi2Cut")),
      tkNhitsCut_(iConfig.getParameter<int>("tkNhitsCut")),
      tkPtCut_(iConfig.getParameter<double>("tkPtCut")),
      tkPtErrCut_(iConfig.getParameter<double>("tkPtErrCut")),
      tkEtaCut_(iConfig.getParameter<double>("tkEtaCut")),
      rejectDuplicateTrack_(iConfig.exists("rejectDuplicateTrack") && iConfig.getParameter<bool>("rejectDuplicateTrack")),
      trackQualities_(iConfig.getParameter<std::vector<std::string>>("trackQualities")) {
  usesResource("TFileService");
  edm::Service<TFileService> fs;
  const std::array<std::string, kNCategory> names{{"A", "B", "C", "D"}};
  for (int i = 0; i < kNCategory; ++i) {
    auto dir = fs->mkdir(("cat" + names[i]).c_str());
    hDeltaM0500_[i] = dir.make<TH1D>("dM_0139_0500", ";#DeltaM (GeV);candidates", 361, 0.139, 0.500);
    hDeltaM0300_[i] = dir.make<TH1D>("dM_0139_0300", ";#DeltaM (GeV);candidates", 322, 0.139, 0.300);
    hDeltaM0200_[i] = dir.make<TH1D>("dM_0139_0200", ";#DeltaM (GeV);candidates", 122, 0.139, 0.200);
    hDeltaM0170_[i] = dir.make<TH1D>("dM_0140_0170", ";#DeltaM (GeV);candidates", 120, 0.140, 0.170);
    hQ0100_[i] = dir.make<TH1D>("Q_000_0100", ";Q = #DeltaM - m_{#pi} (GeV);candidates", 200, 0.0, 0.100);
    hQ0050_[i] = dir.make<TH1D>("Q_000_0050", ";Q = #DeltaM - m_{#pi} (GeV);candidates", 200, 0.0, 0.050);
    hQ0025_[i] = dir.make<TH1D>("Q_000_0025", ";Q = #DeltaM - m_{#pi} (GeV);candidates", 200, 0.0, 0.025);
    hDstarVsD0Mass_[i] = dir.make<TH2D>("M_Dstar_vs_M_D0", ";M(K#pi) (GeV);M(K#pi#pi_{s}) (GeV)", 160, 1.70, 2.02, 250, 1.80, 2.30);
    hDeltaMVsSlowPiPt_[i] = dir.make<TH2D>("dM_vs_slowPiPt", ";p_{T}(#pi_{s}) (GeV);#DeltaM (GeV)", 150, 0.0, 15.0, 361, 0.139, 0.500);
    hDeltaMVsD0Pt_[i] = dir.make<TH2D>("dM_vs_D0Pt", ";p_{T}(D0) (GeV);#DeltaM (GeV)", 200, 0.0, 100.0, 361, 0.139, 0.500);
    hDeltaMVsOpeningAngle_[i] = dir.make<TH2D>("dM_vs_openingAngle", ";opening angle(D0,#pi_{s});#DeltaM (GeV)", 160, 0.0, 3.2, 361, 0.139, 0.500);
    hQVsOpeningAngle_[i] = dir.make<TH2D>("Q_vs_openingAngle", ";opening angle(D0,#pi_{s});Q (GeV)", 160, 0.0, 3.2, 200, 0.0, 0.100);
  }
  auto swapDir = fs->mkdir("catB_swap");
  hCatBNormalMass_ = swapDir.make<TH1D>("M_normal", ";M_{normal}(K#pi) (GeV);candidates", 160, 1.70, 2.02);
  hCatBSwapMass_ = swapDir.make<TH1D>("M_swap", ";M_{swap}(K#pi) (GeV);candidates", 160, 1.70, 2.02);
  hCatBDeltaMBeforeSwapVeto_ = swapDir.make<TH1D>("dM_before_swap_veto", ";#DeltaM (GeV);candidates", 361, 0.139, 0.500);
  const std::array<std::string, 5> vetoNames{{"008", "012", "016", "020", "025"}};
  for (int i = 0; i < 5; ++i) {
    hCatBDeltaMAfterSwapVeto_[i] = swapDir.make<TH1D>(("catB_dM_after_swap_veto_" + vetoNames[i]).c_str(), ";#DeltaM (GeV);candidates", 361, 0.139, 0.500);
    hCatADeltaMAfterSwapVeto_[i] = swapDir.make<TH1D>(("catA_dM_after_swap_veto_" + vetoNames[i]).c_str(), ";#DeltaM (GeV);candidates", 361, 0.139, 0.500);
  }
}

bool DStarDeltaMDebugAnalyzer::passSlowPionTrack(const reco::Track& trk) const {
  bool qualityOk = trackQualities_.empty();
  for (const auto& quality : trackQualities_) {
    if (trk.quality(reco::TrackBase::qualityByName(quality))) {
      qualityOk = true;
      break;
    }
  }
  if (!qualityOk) return false;
  if (trk.normalizedChi2() >= tkChi2Cut_) return false;
  if (trk.numberOfValidHits() < tkNhitsCut_) return false;
  if (trk.pt() <= tkPtCut_) return false;
  if (std::abs(trk.eta()) >= tkEtaCut_) return false;
  if (trk.ptError() / trk.pt() >= tkPtErrCut_) return false;
  return true;
}

int DStarDeltaMDebugAnalyzer::categoryIndex(int qK, int qPiD0, int qPiS) const {
  const int qKPi = qK * qPiD0;
  const int qKPiS = qK * qPiS;
  if (qKPi == -1 && qKPiS == -1) return kA;
  if (qKPi == -1 && qKPiS == 1) return kB;
  if (qKPi == 1 && qKPiS == -1) return kC;
  if (qKPi == 1 && qKPiS == 1) return kD;
  return -1;
}

double DStarDeltaMDebugAnalyzer::massWithHypothesis(const reco::Candidate& a, const reco::Candidate& b, double massA, double massB) const {
  const double eA = std::sqrt(a.px() * a.px() + a.py() * a.py() + a.pz() * a.pz() + massA * massA);
  const double eB = std::sqrt(b.px() * b.px() + b.py() * b.py() + b.pz() * b.pz() + massB * massB);
  const double px = a.px() + b.px();
  const double py = a.py() + b.py();
  const double pz = a.pz() + b.pz();
  const double m2 = (eA + eB) * (eA + eB) - px * px - py * py - pz * pz;
  return m2 > 0.0 ? std::sqrt(m2) : std::numeric_limits<double>::quiet_NaN();
}

void DStarDeltaMDebugAnalyzer::fillCutflow(Category cat, CutStep step) { data_[cat].cutflow[step]++; }

void DStarDeltaMDebugAnalyzer::analyze(const edm::Event& iEvent, const edm::EventSetup&) {
  edm::Handle<pat::CompositeCandidateCollection> d0RS;
  edm::Handle<pat::CompositeCandidateCollection> d0SS;
  edm::Handle<reco::TrackCollection> tracks;
  iEvent.getByToken(d0RSToken_, d0RS);
  iEvent.getByToken(d0SSToken_, d0SS);
  iEvent.getByToken(trackToken_, tracks);
  if (!d0RS.isValid() || !d0SS.isValid() || !tracks.isValid()) return;

  std::vector<reco::TrackRef> slowPions;
  slowPions.reserve(tracks->size());
  for (unsigned int i = 0; i < tracks->size(); ++i) {
    reco::TrackRef ref(tracks, i);
    if (passSlowPionTrack(*ref)) slowPions.push_back(ref);
  }

  auto processD0Collection = [&](const pat::CompositeCandidateCollection& d0s) {
    for (const auto& d0 : d0s) {
      if (d0.numberOfDaughters() < 2) continue;
      const auto* dau0 = d0.daughter(0);
      const auto* dau1 = d0.daughter(1);
      if (!dau0 || !dau1) continue;
      const auto* kaon = dau0->mass() > dau1->mass() ? dau0 : dau1;
      const auto* pion = dau0->mass() > dau1->mass() ? dau1 : dau0;
      const int qK = static_cast<int>(kaon->charge());
      const int qPiD0 = static_cast<int>(pion->charge());
      const auto kaonTrack = kaon->get<reco::TrackRef>();
      const auto pionTrack = pion->get<reco::TrackRef>();
      const double mNormal = d0.mass();
      const double mSwap = massWithHypothesis(*kaon, *pion, kPionMass, kKaonMass);

      for (const auto& slowPi : slowPions) {
        const int catIndex = categoryIndex(qK, qPiD0, slowPi->charge());
        if (catIndex < 0) continue;
        const auto cat = static_cast<Category>(catIndex);
        fillCutflow(cat, kAllKPiPairs);
        fillCutflow(cat, kBeforeD0MassWindow);
        fillCutflow(cat, kAfterD0MassWindow);
        fillCutflow(cat, kAfterD0Topology);
        fillCutflow(cat, kAfterSlowPionAttach);

        const bool duplicate = (kaonTrack.isNonnull() && kaonTrack == slowPi) || (pionTrack.isNonnull() && pionTrack == slowPi);
        if (duplicate) data_[cat].nDuplicateTrack++;
        if (rejectDuplicateTrack_ && duplicate) continue;

        math::PtEtaPhiMLorentzVector pPi(slowPi->pt(), slowPi->eta(), slowPi->phi(), kPionMass);
        const double mDstar = (d0.p4() + pPi).M();
        const double dM = mDstar - d0.mass();
        if (!std::isfinite(mDstar) || !std::isfinite(dM)) {
          data_[cat].nInvalidMass++;
          continue;
        }
        fillCutflow(cat, kAfterDeltaMCalculated);
        data_[cat].minDeltaM = std::min(data_[cat].minDeltaM, dM);
        data_[cat].maxDeltaM = std::max(data_[cat].maxDeltaM, dM);
        if (dM < kPionMass) data_[cat].nDeltaMLtPionMass++;
        if (dM < 0.500) fillCutflow(cat, kDeltaMLt0500);
        if (dM < 0.300) fillCutflow(cat, kDeltaMLt0300);
        if (dM < 0.250) fillCutflow(cat, kDeltaMLt0250);
        if (dM < 0.200) fillCutflow(cat, kDeltaMLt0200);
        if (dM < 0.180) fillCutflow(cat, kDeltaMLt0180);
        if (dM < 0.165) fillCutflow(cat, kDeltaMLt0165);
        if (dM < 0.160) fillCutflow(cat, kDeltaMLt0160);
        fillCutflow(cat, kAfterCosBinning);

        const double qValue = dM - kPionMass;
        const double dot = d0.px() * slowPi->px() + d0.py() * slowPi->py() + d0.pz() * slowPi->pz();
        const double mag = d0.p() * slowPi->p();
        const double openingAngle = mag > 0.0 ? std::acos(std::max(-1.0, std::min(1.0, dot / mag))) : std::numeric_limits<double>::quiet_NaN();

        hDeltaM0500_[cat]->Fill(dM);
        hDeltaM0300_[cat]->Fill(dM);
        hDeltaM0200_[cat]->Fill(dM);
        hDeltaM0170_[cat]->Fill(dM);
        hQ0100_[cat]->Fill(qValue);
        hQ0050_[cat]->Fill(qValue);
        hQ0025_[cat]->Fill(qValue);
        hDstarVsD0Mass_[cat]->Fill(d0.mass(), mDstar);
        hDeltaMVsSlowPiPt_[cat]->Fill(slowPi->pt(), dM);
        hDeltaMVsD0Pt_[cat]->Fill(d0.pt(), dM);
        if (std::isfinite(openingAngle)) {
          hDeltaMVsOpeningAngle_[cat]->Fill(openingAngle, dM);
          hQVsOpeningAngle_[cat]->Fill(openingAngle, qValue);
        }

        if (cat == kB) {
          hCatBNormalMass_->Fill(mNormal);
          hCatBSwapMass_->Fill(mSwap);
          hCatBDeltaMBeforeSwapVeto_->Fill(dM);
        }
        for (int iveto = 0; iveto < 5; ++iveto) {
          const bool passVeto = std::isfinite(mSwap) && std::abs(mSwap - kD0Mass) >= kSwapWindows[iveto];
          if (passVeto) {
            if (cat == kB) hCatBDeltaMAfterSwapVeto_[iveto]->Fill(dM);
            if (cat == kA) hCatADeltaMAfterSwapVeto_[iveto]->Fill(dM);
          } else {
            if (cat == kB) data_[cat].nSwapVetoWindow[iveto]++;
          }
        }
      }
    }
  };

  processD0Collection(*d0RS);
  processD0Collection(*d0SS);
}

void DStarDeltaMDebugAnalyzer::endJob() {
  const std::array<std::string, kNCategory> names{{"A qK*qPiD0=-1 qK*qPiS=-1",
                                                   "B qK*qPiD0=-1 qK*qPiS=+1",
                                                   "C qK*qPiD0=+1 qK*qPiS=-1",
                                                   "D qK*qPiD0=+1 qK*qPiS=+1"}};
  const std::array<std::string, kNCutStep> steps{{"all Kpi pairs",
                                                  "D0 mass window before",
                                                  "D0 mass window after",
                                                  "D0 topology after",
                                                  "slow pion attach after",
                                                  "dM calculated",
                                                  "dM < 0.500",
                                                  "dM < 0.300",
                                                  "dM < 0.250",
                                                  "dM < 0.200",
                                                  "dM < 0.180",
                                                  "dM < 0.165",
                                                  "dM < 0.160",
                                                  "after cos binning"}};
  for (int icat = 0; icat < kNCategory; ++icat) {
    edm::LogPrint("DStarDeltaMDebug") << "Category " << names[icat];
    unsigned long long prev = 0;
    for (int istep = 0; istep < kNCutStep; ++istep) {
      const auto n = data_[icat].cutflow[istep];
      const double survival = (istep == 0 || prev == 0) ? 1.0 : static_cast<double>(n) / static_cast<double>(prev);
      edm::LogPrint("DStarDeltaMDebug") << steps[istep] << ": N = " << n << ", survival = " << survival;
      prev = n;
    }
    edm::LogPrint("DStarDeltaMDebug") << "minDeltaM = " << data_[icat].minDeltaM
                                      << ", maxDeltaM = " << data_[icat].maxDeltaM
                                      << ", nDeltaM_lt_mPi = " << data_[icat].nDeltaMLtPionMass
                                      << ", nInvalidMass = " << data_[icat].nInvalidMass
                                      << ", nDuplicateTrack = " << data_[icat].nDuplicateTrack
                                      << ", nSwapVetoAbs008/012/016/020/025 = "
                                      << data_[icat].nSwapVetoWindow[0] << "/" << data_[icat].nSwapVetoWindow[1] << "/"
                                      << data_[icat].nSwapVetoWindow[2] << "/" << data_[icat].nSwapVetoWindow[3] << "/"
                                      << data_[icat].nSwapVetoWindow[4];
  }
}

DEFINE_FWK_MODULE(DStarDeltaMDebugAnalyzer);
