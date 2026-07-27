#include <algorithm>
#include <cmath>
#include <iostream>
#include <string>
#include <vector>

#include <TH1D.h>
#include <TTree.h>

#include "FWCore/Framework/interface/Frameworkfwd.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/Framework/interface/one/EDAnalyzer.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/ServiceRegistry/interface/Service.h"

#include "CommonTools/UtilAlgos/interface/TFileService.h"
#include "DataFormats/HeavyIonEvent/interface/EvtPlane.h"
#include "RecoHI/HiEvtPlaneAlgos/interface/HiEvtPlaneList.h"

class EvtPlaneComparator : public edm::one::EDAnalyzer<> {
public:
  explicit EvtPlaneComparator(const edm::ParameterSet& iConfig);
  ~EvtPlaneComparator() override = default;

  void analyze(const edm::Event& iEvent, const edm::EventSetup& iSetup) override;
  void beginJob() override;
  void endJob() override;

private:
  enum class PlaneGroup { HF, Tracker, Other };

  static double deltaPhiPeriodic(double a, double b, int order) {
    const double period = (order > 0 ? 2.0 * M_PI / static_cast<double>(order) : 2.0 * M_PI);
    double d = a - b;
    while (d > period / 2.0)
      d -= period;
    while (d <= -period / 2.0)
      d += period;
    return d;
  }
  static PlaneGroup planeGroup(int idx) {
    if (idx >= 0 && idx < hi::NumEPNames) {
      if (hi::EPDet[idx] == hi::HF)
        return PlaneGroup::HF;
      if (hi::EPDet[idx] == hi::Tracker)
        return PlaneGroup::Tracker;
    }
    return PlaneGroup::Other;
  }

  const edm::EDGetTokenT<reco::EvtPlaneCollection> tokStored_;
  const edm::EDGetTokenT<reco::EvtPlaneCollection> tokRecomputed_;

  const double angleTolerance_;
  const double qTolerance_;
  const double sumTolerance_;
  const bool failOnMissing_;
  const bool debugLog_;
  const int maxDebugPrints_;
  const double zeroSpikeWindow_;
  const double mismatchPrintThreshold_;

  edm::Service<TFileService> fs_;
  TTree* tree_ = nullptr;
  TH1D* hDeltaPsi2_ = nullptr;
  TH1D* hDeltaQ2_ = nullptr;
  TH1D* hDeltaSumW_ = nullptr;
  TH1D* hCollectionSizeDiff_ = nullptr;
  TH1D* hDeltaPsi2HF_ = nullptr;
  TH1D* hDeltaPsi2Tracker_ = nullptr;
  TH1D* hDeltaPsi2HFValid_ = nullptr;
  TH1D* hDeltaPsi2TrackerValid_ = nullptr;

  ULong64_t run_ = 0;
  ULong64_t lumi_ = 0;
  ULong64_t event_ = 0;
  int nStored_ = 0;
  int nReco_ = 0;
  int nCompared_ = 0;
  int nMatch_ = 0;
  int nMismatch_ = 0;
  int nComparedHF_ = 0;
  int nComparedTracker_ = 0;
  int nMismatchHF_ = 0;
  int nMismatchTracker_ = 0;
  int firstMismatchIndex_ = -1;
  float firstMismatchDeltaPsi2_ = 0.0f;
  float firstMismatchDeltaQ2_ = 0.0f;
  float firstMismatchDeltaSumW_ = 0.0f;

  unsigned long long totalEvents_ = 0;
  unsigned long long missingStored_ = 0;
  unsigned long long missingReco_ = 0;
  unsigned long long sizeMismatchEvents_ = 0;
  unsigned long long perfectMatchEvents_ = 0;
  unsigned long long zeroSpikePairs_ = 0;
  unsigned long long sentinelPairs_ = 0;
  unsigned long long comparedHFPairs_ = 0;
  unsigned long long comparedTrackerPairs_ = 0;
  unsigned long long mismatchHFPairs_ = 0;
  unsigned long long mismatchTrackerPairs_ = 0;
  int debugPrinted_ = 0;
};

EvtPlaneComparator::EvtPlaneComparator(const edm::ParameterSet& iConfig)
    : tokStored_(consumes<reco::EvtPlaneCollection>(iConfig.getParameter<edm::InputTag>("stored"))),
      tokRecomputed_(consumes<reco::EvtPlaneCollection>(iConfig.getParameter<edm::InputTag>("recomputed"))),
      angleTolerance_(iConfig.getUntrackedParameter<double>("angleTolerance", 1e-6)),
      qTolerance_(iConfig.getUntrackedParameter<double>("qTolerance", 1e-6)),
      sumTolerance_(iConfig.getUntrackedParameter<double>("sumTolerance", 1e-6)),
      failOnMissing_(iConfig.getUntrackedParameter<bool>("failOnMissing", true)),
      debugLog_(iConfig.getUntrackedParameter<bool>("debugLog", false)),
      maxDebugPrints_(iConfig.getUntrackedParameter<int>("maxDebugPrints", 200)),
      zeroSpikeWindow_(iConfig.getUntrackedParameter<double>("zeroSpikeWindow", 1e-12)),
      mismatchPrintThreshold_(iConfig.getUntrackedParameter<double>("mismatchPrintThreshold", 0.02)) {}

void EvtPlaneComparator::beginJob() {
  if (!fs_.isAvailable()) {
    throw cms::Exception("EvtPlaneComparator") << "TFileService is required.";
  }

  tree_ = fs_->make<TTree>("EvtPlaneCompare", "EvtPlaneCompare");
  tree_->Branch("run", &run_, "run/l");
  tree_->Branch("lumi", &lumi_, "lumi/l");
  tree_->Branch("event", &event_, "event/l");
  tree_->Branch("nStored", &nStored_, "nStored/I");
  tree_->Branch("nReco", &nReco_, "nReco/I");
  tree_->Branch("nCompared", &nCompared_, "nCompared/I");
  tree_->Branch("nMatch", &nMatch_, "nMatch/I");
  tree_->Branch("nMismatch", &nMismatch_, "nMismatch/I");
  tree_->Branch("nComparedHF", &nComparedHF_, "nComparedHF/I");
  tree_->Branch("nComparedTracker", &nComparedTracker_, "nComparedTracker/I");
  tree_->Branch("nMismatchHF", &nMismatchHF_, "nMismatchHF/I");
  tree_->Branch("nMismatchTracker", &nMismatchTracker_, "nMismatchTracker/I");
  tree_->Branch("firstMismatchIndex", &firstMismatchIndex_, "firstMismatchIndex/I");
  tree_->Branch("firstMismatchDeltaPsi2", &firstMismatchDeltaPsi2_, "firstMismatchDeltaPsi2/F");
  tree_->Branch("firstMismatchDeltaQ2", &firstMismatchDeltaQ2_, "firstMismatchDeltaQ2/F");
  tree_->Branch("firstMismatchDeltaSumW", &firstMismatchDeltaSumW_, "firstMismatchDeltaSumW/F");

  hDeltaPsi2_ = fs_->make<TH1D>("hDeltaPsi2", "#Delta#Psi_{2} (stored - recomputed);#Delta#Psi_{2};Entries", 400, -0.2, 0.2);
  hDeltaQ2_ = fs_->make<TH1D>("hDeltaQ2", "#Deltaq_{2} (stored - recomputed);#Deltaq_{2};Entries", 400, -0.2, 0.2);
  hDeltaSumW_ = fs_->make<TH1D>("hDeltaSumW", "#Deltasumw (stored - recomputed);#Deltasumw;Entries", 400, -10.0, 10.0);
  hCollectionSizeDiff_ = fs_->make<TH1D>("hCollectionSizeDiff", "Collection size difference;N_{stored} - N_{recomputed};Events", 101, -50.5, 50.5);
  hDeltaPsi2HF_ = fs_->make<TH1D>("hDeltaPsi2HF", "HF #Delta#Psi_{2} (stored - recomputed);#Delta#Psi_{2};Entries", 400, -0.2, 0.2);
  hDeltaPsi2Tracker_ = fs_->make<TH1D>("hDeltaPsi2Tracker",
                                       "Tracker #Delta#Psi_{2} (stored - recomputed);#Delta#Psi_{2};Entries",
                                       400,
                                       -0.2,
                                       0.2);
  hDeltaPsi2HFValid_ =
      fs_->make<TH1D>("hDeltaPsi2HFValid", "HF #Delta#Psi_{2} (valid only);#Delta#Psi_{2};Entries", 400, -0.2, 0.2);
  hDeltaPsi2TrackerValid_ = fs_->make<TH1D>(
      "hDeltaPsi2TrackerValid", "Tracker #Delta#Psi_{2} (valid only);#Delta#Psi_{2};Entries", 400, -0.2, 0.2);
}

void EvtPlaneComparator::analyze(const edm::Event& iEvent, const edm::EventSetup&) {
  ++totalEvents_;

  run_ = iEvent.id().run();
  lumi_ = iEvent.luminosityBlock();
  event_ = iEvent.id().event();

  nStored_ = 0;
  nReco_ = 0;
  nCompared_ = 0;
  nMatch_ = 0;
  nMismatch_ = 0;
  nComparedHF_ = 0;
  nComparedTracker_ = 0;
  nMismatchHF_ = 0;
  nMismatchTracker_ = 0;
  firstMismatchIndex_ = -1;
  firstMismatchDeltaPsi2_ = 0.0f;
  firstMismatchDeltaQ2_ = 0.0f;
  firstMismatchDeltaSumW_ = 0.0f;

  edm::Handle<reco::EvtPlaneCollection> stored;
  edm::Handle<reco::EvtPlaneCollection> reco;
  iEvent.getByToken(tokStored_, stored);
  iEvent.getByToken(tokRecomputed_, reco);

  if (!stored.isValid()) {
    ++missingStored_;
    if (failOnMissing_) {
      throw cms::Exception("EvtPlaneComparator") << "Stored EvtPlaneCollection is missing.";
    }
    tree_->Fill();
    return;
  }

  if (!reco.isValid()) {
    ++missingReco_;
    if (failOnMissing_) {
      throw cms::Exception("EvtPlaneComparator") << "Recomputed EvtPlaneCollection is missing.";
    }
    tree_->Fill();
    return;
  }

  nStored_ = static_cast<int>(stored->size());
  nReco_ = static_cast<int>(reco->size());
  hCollectionSizeDiff_->Fill(nStored_ - nReco_);
  if (nStored_ != nReco_) {
    ++sizeMismatchEvents_;
  }

  const int n = std::min(nStored_, nReco_);
  nCompared_ = n;

  for (int i = 0; i < n; ++i) {
    const auto& a = (*stored)[i];
    const auto& b = (*reco)[i];

    const double dPsi2 = deltaPhiPeriodic(a.angle(2), b.angle(2), 2);
    const double dQ2 = a.q(2) - b.q(2);
    const double dSumW = a.sumw() - b.sumw();
    const bool isSentinel = (a.angle(2) < -9.0 || b.angle(2) < -9.0);
    const bool isZeroSpike = (std::abs(dPsi2) <= zeroSpikeWindow_);
    const bool isValidAngle = (!isSentinel && a.sumw() > 0.f && b.sumw() > 0.f);
    const PlaneGroup group = planeGroup(i);

    if (isSentinel)
      ++sentinelPairs_;
    if (isZeroSpike)
      ++zeroSpikePairs_;

    hDeltaPsi2_->Fill(dPsi2);
    hDeltaQ2_->Fill(dQ2);
    hDeltaSumW_->Fill(dSumW);
    if (group == PlaneGroup::HF) {
      ++nComparedHF_;
      ++comparedHFPairs_;
      hDeltaPsi2HF_->Fill(dPsi2);
      if (isValidAngle)
        hDeltaPsi2HFValid_->Fill(dPsi2);
    } else if (group == PlaneGroup::Tracker) {
      ++nComparedTracker_;
      ++comparedTrackerPairs_;
      hDeltaPsi2Tracker_->Fill(dPsi2);
      if (isValidAngle)
        hDeltaPsi2TrackerValid_->Fill(dPsi2);
    }

    const bool match = (std::abs(dPsi2) <= angleTolerance_) && (std::abs(dQ2) <= qTolerance_) &&
                       (std::abs(dSumW) <= sumTolerance_);

    if (match) {
      ++nMatch_;
    } else {
      ++nMismatch_;
      if (group == PlaneGroup::HF) {
        ++nMismatchHF_;
        ++mismatchHFPairs_;
      } else if (group == PlaneGroup::Tracker) {
        ++nMismatchTracker_;
        ++mismatchTrackerPairs_;
      }
      if (firstMismatchIndex_ < 0) {
        firstMismatchIndex_ = i;
        firstMismatchDeltaPsi2_ = static_cast<float>(dPsi2);
        firstMismatchDeltaQ2_ = static_cast<float>(dQ2);
        firstMismatchDeltaSumW_ = static_cast<float>(dSumW);
      }
    }

    if (debugLog_ && debugPrinted_ < maxDebugPrints_) {
      const bool printThis = isSentinel || isZeroSpike || std::abs(dPsi2) >= mismatchPrintThreshold_;
      if (printThis) {
        edm::LogPrint("EvtPlaneComparatorDebug")
            << "[EvtPlaneComparatorDebug] run=" << run_
            << " lumi=" << lumi_
            << " event=" << event_
            << " idx=" << i
            << " planeName=" << (i >= 0 && i < hi::NumEPNames ? hi::EPNames[i] : std::string("Unknown"))
            << " planeGroup=" << (group == PlaneGroup::HF ? "HF" : (group == PlaneGroup::Tracker ? "Tracker" : "Other"))
            << " storedAngle2=" << a.angle(2)
            << " recoAngle2=" << b.angle(2)
            << " dPsi2=" << dPsi2
            << " storedQ2=" << a.q(2)
            << " recoQ2=" << b.q(2)
            << " dQ2=" << dQ2
            << " storedSumW=" << a.sumw()
            << " recoSumW=" << b.sumw()
            << " dSumW=" << dSumW
            << " sentinel=" << (isSentinel ? 1 : 0)
            << " validAngle=" << (isValidAngle ? 1 : 0)
            << " zeroSpike=" << (isZeroSpike ? 1 : 0)
            << " match=" << (match ? 1 : 0);
        ++debugPrinted_;
      }
    }
  }

  if (nMismatch_ == 0 && nStored_ == nReco_) {
    ++perfectMatchEvents_;
  }

  tree_->Fill();
}

void EvtPlaneComparator::endJob() {
  std::cout << "[EvtPlaneComparator] events=" << totalEvents_
            << " perfect_match_events=" << perfectMatchEvents_
            << " size_mismatch_events=" << sizeMismatchEvents_
            << " missing_stored=" << missingStored_
            << " missing_recomputed=" << missingReco_
            << " zero_spike_pairs=" << zeroSpikePairs_
            << " sentinel_pairs=" << sentinelPairs_
            << " compared_hf_pairs=" << comparedHFPairs_
            << " compared_tracker_pairs=" << comparedTrackerPairs_
            << " mismatch_hf_pairs=" << mismatchHFPairs_
            << " mismatch_tracker_pairs=" << mismatchTrackerPairs_
            << " debug_printed=" << debugPrinted_
            << " angleTol=" << angleTolerance_
            << " qTol=" << qTolerance_
            << " sumTol=" << sumTolerance_
            << std::endl;
}

DEFINE_FWK_MODULE(EvtPlaneComparator);
