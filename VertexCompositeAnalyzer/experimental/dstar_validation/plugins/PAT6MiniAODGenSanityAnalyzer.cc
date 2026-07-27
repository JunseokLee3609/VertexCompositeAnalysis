// -*- C++ -*-
#include <array>
#include <cmath>
#include <unordered_map>

#include "DataFormats/HepMCCandidate/interface/GenParticle.h"
#include "DataFormats/PatCandidates/interface/PackedGenParticle.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/Frameworkfwd.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/Framework/interface/one/EDAnalyzer.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"

namespace {
static constexpr int kPdgGamma = 22;
static constexpr int kPdgKaon = 321;
static constexpr int kPdgPion = 211;
static constexpr int kPdgD0 = 421;
static constexpr int kPdgDStar = 413;
}  // namespace

class PAT6MiniAODGenSanityAnalyzer : public edm::one::EDAnalyzer<> {
public:
  explicit PAT6MiniAODGenSanityAnalyzer(const edm::ParameterSet& iConfig)
      : prunedToken_(consumes<reco::GenParticleCollection>(
            iConfig.getUntrackedParameter<edm::InputTag>("prunedGenCollection", edm::InputTag("prunedGenParticles")))),
        packedToken_(consumes<pat::PackedGenParticleCollection>(
            iConfig.getUntrackedParameter<edm::InputTag>("packedGenCollection", edm::InputTag("packedGenParticles")))),
        debug_(iConfig.getUntrackedParameter<bool>("debug", true)),
        maxPrint_(iConfig.getUntrackedParameter<unsigned int>("maxPrint", 5U)) {
    d0PrunedNDauAll_.fill(0);
    dsPrunedNDauAll_.fill(0);
  }

  void analyze(const edm::Event& iEvent, const edm::EventSetup&) override {
    ++nEventsSeen_;

    edm::Handle<reco::GenParticleCollection> prunedH;
    iEvent.getByToken(prunedToken_, prunedH);
    edm::Handle<pat::PackedGenParticleCollection> packedH;
    iEvent.getByToken(packedToken_, packedH);

    if (!prunedH.isValid() || !packedH.isValid()) {
      ++nEventsMissingHandles_;
      edm::LogInfo("PAT6GenSanity")
          << "[MiniAODSanity] run:lumi:event=" << iEvent.id().run() << ":" << iEvent.id().luminosityBlock() << ":"
          << iEvent.id().event() << " missing handles: prunedValid=" << prunedH.isValid()
          << " packedValid=" << packedH.isValid();
      return;
    }

    std::array<unsigned long long, 5> d0PrunedNDau = {{0, 0, 0, 0, 0}};
    std::array<unsigned long long, 5> dsPrunedNDau = {{0, 0, 0, 0, 0}};
    std::unordered_map<unsigned int, unsigned int> d0PackedK;
    std::unordered_map<unsigned int, unsigned int> d0PackedPi;
    std::unordered_map<unsigned int, unsigned int> d0PackedGamma;
    std::unordered_map<unsigned int, unsigned int> dsPackedPi;

    const auto prunedID = prunedH.id();
    for (const auto& pg : *packedH) {
      reco::GenParticleRef mom = pg.motherRef();
      if (mom.isNull() || !mom.isAvailable()) continue;
      if (mom.id() != prunedID) continue;

      const int momId = std::abs(mom->pdgId());
      const int pid = std::abs(pg.pdgId());
      if (momId == kPdgD0) {
        if (pid == kPdgKaon) d0PackedK[mom.key()]++;
        else if (pid == kPdgPion) d0PackedPi[mom.key()]++;
        else if (pid == kPdgGamma) d0PackedGamma[mom.key()]++;
      } else if (momId == kPdgDStar) {
        if (pid == kPdgPion) dsPackedPi[mom.key()]++;
      }
    }

    unsigned int printedD0 = 0;
    unsigned int printedDs = 0;
    for (unsigned int i = 0; i < prunedH->size(); ++i) {
      const reco::GenParticle& gp = (*prunedH)[i];
      const int absId = std::abs(gp.pdgId());
      const unsigned int nDau = gp.numberOfDaughters();
      const unsigned int bin = (nDau >= 4U ? 4U : nDau);

      if (absId == kPdgD0) {
        d0PrunedNDau[bin]++;
        d0PrunedNDauAll_[bin]++;
        ++nD0Total_;

        const auto kIt = d0PackedK.find(i);
        const auto piIt = d0PackedPi.find(i);
        const auto gIt = d0PackedGamma.find(i);
        const unsigned int nK = (kIt == d0PackedK.end() ? 0U : kIt->second);
        const unsigned int nPi = (piIt == d0PackedPi.end() ? 0U : piIt->second);
        const unsigned int nGamma = (gIt == d0PackedGamma.end() ? 0U : gIt->second);

        if (nK == 1U && nPi == 1U) ++nD0Packed11_;
        else if (nK == 1U && nPi == 0U) ++nD0Packed10_;
        else if (nK == 0U && nPi == 1U) ++nD0Packed01_;
        else if (nK == 0U && nPi == 0U) ++nD0Packed00_;
        else ++nD0PackedOther_;
        if (nGamma > 0U) ++nD0PackedGammaAny_;

        if (debug_ && printedD0 < maxPrint_) {
          edm::LogInfo("PAT6GenSanity")
              << "[D0] idx=" << i << " prunedNDau=" << nDau << " packedDirect(K,pi,gamma)=(" << nK << "," << nPi
              << "," << nGamma << ")";
          ++printedD0;
        }
      } else if (absId == kPdgDStar) {
        dsPrunedNDau[bin]++;
        dsPrunedNDauAll_[bin]++;
        ++nDStarTotal_;

        const auto piIt = dsPackedPi.find(i);
        const unsigned int nSlowPi = (piIt == dsPackedPi.end() ? 0U : piIt->second);
        if (nSlowPi == 0U) ++nDStarPackedSlowPi0_;
        else if (nSlowPi == 1U) ++nDStarPackedSlowPi1_;
        else ++nDStarPackedSlowPiGt1_;

        if (debug_ && printedDs < maxPrint_) {
          edm::LogInfo("PAT6GenSanity")
              << "[D*] idx=" << i << " prunedNDau=" << nDau << " packedDirect(slowPi)=" << nSlowPi;
          ++printedDs;
        }
      }
    }

    if (debug_) {
      edm::LogInfo("PAT6GenSanity")
          << "[Summary] run:lumi:event=" << iEvent.id().run() << ":" << iEvent.id().luminosityBlock() << ":"
          << iEvent.id().event() << " D0(421) prunedNDau bins [0]=" << d0PrunedNDau[0] << " [1]=" << d0PrunedNDau[1]
          << " [2]=" << d0PrunedNDau[2] << " [3]=" << d0PrunedNDau[3] << " [>=4]=" << d0PrunedNDau[4];
      edm::LogInfo("PAT6GenSanity")
          << "[Summary] run:lumi:event=" << iEvent.id().run() << ":" << iEvent.id().luminosityBlock() << ":"
          << iEvent.id().event() << " D*(413) prunedNDau bins [0]=" << dsPrunedNDau[0] << " [1]=" << dsPrunedNDau[1]
          << " [2]=" << dsPrunedNDau[2] << " [3]=" << dsPrunedNDau[3] << " [>=4]=" << dsPrunedNDau[4];
      edm::LogInfo("PAT6GenSanity")
          << "[MiniAODSanity] NOTE: pruned can miss status=1 daughters while packed keeps stable particles and maps "
             "motherRef to pruned.";
    }
  }

  void endJob() override {
    auto pct = [](unsigned long long num, unsigned long long den) -> double {
      return den ? (100.0 * static_cast<double>(num) / static_cast<double>(den)) : 0.0;
    };

    edm::LogInfo("PAT6GenSanity") << "[FinalSummary] eventsSeen=" << nEventsSeen_
                                  << " eventsMissingHandles=" << nEventsMissingHandles_
                                  << " eventsUsed=" << (nEventsSeen_ - nEventsMissingHandles_);

    edm::LogInfo("PAT6GenSanity")
        << "[FinalSummary] D0(421) total=" << nD0Total_ << " prunedNDau bins [0]=" << d0PrunedNDauAll_[0]
        << " [1]=" << d0PrunedNDauAll_[1] << " [2]=" << d0PrunedNDauAll_[2] << " [3]=" << d0PrunedNDauAll_[3]
        << " [>=4]=" << d0PrunedNDauAll_[4];
    edm::LogInfo("PAT6GenSanity")
        << "[FinalSummary] D0 packedDirect(K,pi) exact categories: (1,1)=" << nD0Packed11_ << " ("
        << pct(nD0Packed11_, nD0Total_) << "%), (1,0)=" << nD0Packed10_ << " (" << pct(nD0Packed10_, nD0Total_)
        << "%), (0,1)=" << nD0Packed01_ << " (" << pct(nD0Packed01_, nD0Total_) << "%), (0,0)=" << nD0Packed00_
        << " (" << pct(nD0Packed00_, nD0Total_) << "%), other=" << nD0PackedOther_ << " ("
        << pct(nD0PackedOther_, nD0Total_) << "%), gammaAny=" << nD0PackedGammaAny_ << " ("
        << pct(nD0PackedGammaAny_, nD0Total_) << "%)";

    edm::LogInfo("PAT6GenSanity")
        << "[FinalSummary] D*(413) total=" << nDStarTotal_ << " prunedNDau bins [0]=" << dsPrunedNDauAll_[0]
        << " [1]=" << dsPrunedNDauAll_[1] << " [2]=" << dsPrunedNDauAll_[2] << " [3]=" << dsPrunedNDauAll_[3]
        << " [>=4]=" << dsPrunedNDauAll_[4];
    edm::LogInfo("PAT6GenSanity")
        << "[FinalSummary] D* packedDirect(slowPi): 0=" << nDStarPackedSlowPi0_ << " ("
        << pct(nDStarPackedSlowPi0_, nDStarTotal_) << "%), 1=" << nDStarPackedSlowPi1_ << " ("
        << pct(nDStarPackedSlowPi1_, nDStarTotal_) << "%), >=2=" << nDStarPackedSlowPiGt1_ << " ("
        << pct(nDStarPackedSlowPiGt1_, nDStarTotal_) << "%)";
  }

private:
  edm::EDGetTokenT<reco::GenParticleCollection> prunedToken_;
  edm::EDGetTokenT<pat::PackedGenParticleCollection> packedToken_;
  bool debug_;
  unsigned int maxPrint_;
  unsigned long long nEventsSeen_ = 0;
  unsigned long long nEventsMissingHandles_ = 0;

  std::array<unsigned long long, 5> d0PrunedNDauAll_;
  std::array<unsigned long long, 5> dsPrunedNDauAll_;
  unsigned long long nD0Total_ = 0;
  unsigned long long nDStarTotal_ = 0;

  unsigned long long nD0Packed11_ = 0;
  unsigned long long nD0Packed10_ = 0;
  unsigned long long nD0Packed01_ = 0;
  unsigned long long nD0Packed00_ = 0;
  unsigned long long nD0PackedOther_ = 0;
  unsigned long long nD0PackedGammaAny_ = 0;

  unsigned long long nDStarPackedSlowPi0_ = 0;
  unsigned long long nDStarPackedSlowPi1_ = 0;
  unsigned long long nDStarPackedSlowPiGt1_ = 0;
};

DEFINE_FWK_MODULE(PAT6MiniAODGenSanityAnalyzer);
