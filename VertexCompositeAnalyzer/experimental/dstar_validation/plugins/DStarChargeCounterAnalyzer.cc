// -*- C++ -*-
#include <algorithm>
#include <cstdint>

#include "DataFormats/HepMCCandidate/interface/GenParticle.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/Frameworkfwd.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/Framework/interface/one/EDAnalyzer.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"

namespace {
bool hasMotherWithPdgId(const reco::Candidate* cand, const int pdgId) {
  if (cand == nullptr) return false;
  for (std::size_t i = 0; i < cand->numberOfMothers(); ++i) {
    const reco::Candidate* mom = cand->mother(i);
    if (mom != nullptr && mom->pdgId() == pdgId) return true;
  }
  return false;
}

bool hasKPiDaughtersRecursive(const reco::Candidate* cand,
                              const int selfPdgId,
                              const int kaonPdgId,
                              const int pionPdgId,
                              const int depth) {
  if (cand == nullptr || depth > 20) return false;

  bool hasKaon = false;
  bool hasPion = false;
  for (std::size_t i = 0; i < cand->numberOfDaughters(); ++i) {
    const reco::Candidate* dau = cand->daughter(i);
    if (dau == nullptr) continue;

    const int pdgId = dau->pdgId();
    if (pdgId == selfPdgId) {
      if (hasKPiDaughtersRecursive(dau, selfPdgId, kaonPdgId, pionPdgId, depth + 1)) return true;
      continue;
    }
    if (pdgId == kaonPdgId) hasKaon = true;
    if (pdgId == pionPdgId) hasPion = true;
  }
  return hasKaon && hasPion;
}
}  // namespace

class DStarChargeCounterAnalyzer : public edm::one::EDAnalyzer<> {
public:
  explicit DStarChargeCounterAnalyzer(const edm::ParameterSet& iConfig)
      : genToken_(consumes<reco::GenParticleCollection>(
            iConfig.getUntrackedParameter<edm::InputTag>("genCollection", edm::InputTag("prunedGenParticles")))),
        targetAbsPdgId_(std::abs(iConfig.getUntrackedParameter<int>("absPdgId", 413))),
        statusFilter_(iConfig.getUntrackedParameter<int>("status", -1)),
        printPerEvent_(iConfig.getUntrackedParameter<bool>("printPerEvent", true)),
        maxPrint_(iConfig.getUntrackedParameter<int>("maxPrint", 50)) {}

  void analyze(const edm::Event& iEvent, const edm::EventSetup&) override {
    ++eventsSeen_;

    edm::Handle<reco::GenParticleCollection> gens;
    iEvent.getByToken(genToken_, gens);
    if (!gens.isValid()) {
      ++eventsMissingCollection_;
      edm::LogWarning("DStarChargeCounter")
          << "Missing gen collection in run:lumi:event=" << iEvent.id().run() << ":" << iEvent.id().luminosityBlock()
          << ":" << iEvent.id().event();
      return;
    }

    int nPlus = 0;
    int nMinus = 0;
    int nD0ToKMinusPiPlus = 0;
    int nAntiD0ToKPlusPiMinus = 0;
    bool hasD0ToKMinusPiPlus = false;
    bool hasAntiD0ToKPlusPiMinus = false;
    for (const auto& gp : *gens) {
      if (std::abs(gp.pdgId()) != targetAbsPdgId_) continue;
      if (statusFilter_ >= 0 && gp.status() != statusFilter_) continue;
      if (gp.pdgId() > 0) ++nPlus;
      else if (gp.pdgId() < 0) ++nMinus;
    }
    for (const auto& gp : *gens) {
      if (gp.pdgId() == 421 && !hasMotherWithPdgId(&gp, 421)) {
        const bool isD0ToKpi = hasKPiDaughtersRecursive(&gp, 421, -321, 211, 0);
        if (isD0ToKpi) ++nD0ToKMinusPiPlus;
        if (!hasD0ToKMinusPiPlus && isD0ToKpi) hasD0ToKMinusPiPlus = true;
      }
      if (gp.pdgId() == -421 && !hasMotherWithPdgId(&gp, -421)) {
        const bool isAntiD0ToKpi = hasKPiDaughtersRecursive(&gp, -421, 321, -211, 0);
        if (isAntiD0ToKpi) ++nAntiD0ToKPlusPiMinus;
        if (!hasAntiD0ToKPlusPiMinus && isAntiD0ToKpi) hasAntiD0ToKPlusPiMinus = true;
      }
      if (hasD0ToKMinusPiPlus && hasAntiD0ToKPlusPiMinus) break;
    }
    totalD0ToKMinusPiPlusDecaysAllEvents_ += static_cast<std::uint64_t>(nD0ToKMinusPiPlus);
    totalAntiD0ToKPlusPiMinusDecaysAllEvents_ += static_cast<std::uint64_t>(nAntiD0ToKPlusPiMinus);

    totalPlus_ += static_cast<std::uint64_t>(nPlus);
    totalMinus_ += static_cast<std::uint64_t>(nMinus);
    if (nPlus + nMinus > 0) {
      ++eventsWithAnyDstar_;
      if (hasD0ToKMinusPiPlus) ++eventsWithAnyDstarAndD0ToKMinusPiPlus_;
      if (hasAntiD0ToKPlusPiMinus) ++eventsWithAnyDstarAndAntiD0ToKPlusPiMinus_;
      if (hasD0ToKMinusPiPlus || hasAntiD0ToKPlusPiMinus) ++eventsWithAnyDstarAndAnyD0KPi_;
    }

    if (printPerEvent_ && (maxPrint_ < 0 || printedEvents_ < maxPrint_)) {
      edm::LogInfo("DStarChargeCounter")
          << "[Event] run:lumi:event=" << iEvent.id().run() << ":" << iEvent.id().luminosityBlock() << ":"
          << iEvent.id().event() << " D*+ count=" << nPlus << " D*- count=" << nMinus
          << " total=" << (nPlus + nMinus) << " hasD0(421)->K-(321)pi+(211)=" << hasD0ToKMinusPiPlus
          << " hasD0bar(-421)->K+(321)pi-(211)=" << hasAntiD0ToKPlusPiMinus
          << " nD0(421)->K-pi+=" << nD0ToKMinusPiPlus << " nD0bar(-421)->K+pi-=" << nAntiD0ToKPlusPiMinus;
      ++printedEvents_;
    }
  }

  void endJob() override {
    const auto eventsUsed = (eventsSeen_ >= eventsMissingCollection_) ? (eventsSeen_ - eventsMissingCollection_) : 0ULL;
    const double denom = (eventsWithAnyDstar_ > 0) ? static_cast<double>(eventsWithAnyDstar_) : 1.0;
    const double d0TotalForRatio =
        static_cast<double>(totalD0ToKMinusPiPlusDecaysAllEvents_ + totalAntiD0ToKPlusPiMinusDecaysAllEvents_);
    const double fracD0 =
        (d0TotalForRatio > 0.0) ? (static_cast<double>(totalD0ToKMinusPiPlusDecaysAllEvents_) / d0TotalForRatio) : 0.0;
    const double fracAntiD0 =
        (d0TotalForRatio > 0.0) ? (static_cast<double>(totalAntiD0ToKPlusPiMinusDecaysAllEvents_) / d0TotalForRatio)
                               : 0.0;
    edm::LogInfo("DStarChargeCounter")
        << "[FinalSummary] eventsSeen=" << eventsSeen_ << " eventsMissingCollection=" << eventsMissingCollection_
        << " eventsUsed=" << eventsUsed << " eventsWithAnyDstar=" << eventsWithAnyDstar_ << " totalDStarPlus="
        << totalPlus_ << " totalDStarMinus=" << totalMinus_ << " totalDStarAll=" << (totalPlus_ + totalMinus_)
        << " eventsWithDstarAndD0ToKMinusPiPlus=" << eventsWithAnyDstarAndD0ToKMinusPiPlus_
        << " fracDstarEventsWithD0ToKMinusPiPlus=" << (eventsWithAnyDstarAndD0ToKMinusPiPlus_ / denom)
        << " eventsWithDstarAndAntiD0ToKPlusPiMinus=" << eventsWithAnyDstarAndAntiD0ToKPlusPiMinus_
        << " fracDstarEventsWithAntiD0ToKPlusPiMinus=" << (eventsWithAnyDstarAndAntiD0ToKPlusPiMinus_ / denom)
        << " eventsWithDstarAndAnyD0KPi=" << eventsWithAnyDstarAndAnyD0KPi_
        << " fracDstarEventsWithAnyD0KPi=" << (eventsWithAnyDstarAndAnyD0KPi_ / denom)
        << " totalN_D0ToKMinusPiPlus_AllEvents=" << totalD0ToKMinusPiPlusDecaysAllEvents_
        << " totalN_AntiD0ToKPlusPiMinus_AllEvents=" << totalAntiD0ToKPlusPiMinusDecaysAllEvents_
        << " fracD0AmongD0AndAntiD0_AllEvents=" << fracD0
        << " fracAntiD0AmongD0AndAntiD0_AllEvents=" << fracAntiD0;
  }

private:
  edm::EDGetTokenT<reco::GenParticleCollection> genToken_;
  int targetAbsPdgId_;
  int statusFilter_;
  bool printPerEvent_;
  int maxPrint_;

  std::uint64_t eventsSeen_ = 0;
  std::uint64_t eventsMissingCollection_ = 0;
  std::uint64_t eventsWithAnyDstar_ = 0;
  std::uint64_t eventsWithAnyDstarAndD0ToKMinusPiPlus_ = 0;
  std::uint64_t eventsWithAnyDstarAndAntiD0ToKPlusPiMinus_ = 0;
  std::uint64_t eventsWithAnyDstarAndAnyD0KPi_ = 0;
  std::uint64_t totalD0ToKMinusPiPlusDecaysAllEvents_ = 0;
  std::uint64_t totalAntiD0ToKPlusPiMinusDecaysAllEvents_ = 0;
  std::uint64_t totalPlus_ = 0;
  std::uint64_t totalMinus_ = 0;
  int printedEvents_ = 0;
};

DEFINE_FWK_MODULE(DStarChargeCounterAnalyzer);
