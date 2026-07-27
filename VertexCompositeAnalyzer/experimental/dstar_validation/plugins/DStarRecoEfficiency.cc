// Simple D* reconstruction efficiency analyzer using PATCompositeTreeProducer5 matching definition
#include "FWCore/Framework/interface/one/EDAnalyzer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/Utilities/interface/InputTag.h"

#include "CommonTools/UtilAlgos/interface/TFileService.h"
#include "DataFormats/Math/interface/deltaR.h"
#include "DataFormats/HepMCCandidate/interface/GenParticle.h"
#include "DataFormats/PatCandidates/interface/CompositeCandidate.h"

#include "TEfficiency.h"
#include "TH1D.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <numeric>
#include <string>
#include <unordered_set>
#include <vector>

namespace {

struct TrackMatchResult {
  const reco::GenParticle* particle = nullptr;
  double deltaR = std::numeric_limits<double>::max();
  int index = -1;
};

TrackMatchResult findBestTrackMatch(const reco::Candidate& recoCand,
                                    const std::vector<const reco::GenParticle*>& genTracks,
                                    const std::vector<bool>& usedFlags,
                                    double maxDr) {
  TrackMatchResult best;
  for (size_t idx = 0; idx < genTracks.size(); ++idx) {
    if (idx < usedFlags.size() && usedFlags[idx]) {
      continue;
    }
    const auto* gen = genTracks[idx];
    if (!gen) {
      continue;
    }
    if (recoCand.charge() != 0 && gen->charge() != 0 && recoCand.charge() != gen->charge()) {
      continue;
    }
    const double dr = reco::deltaR(recoCand.eta(), recoCand.phi(), gen->eta(), gen->phi());
    if (dr > maxDr) {
      continue;
    }
    if (!best.particle || dr < best.deltaR) {
      best.particle = gen;
      best.deltaR = dr;
      best.index = static_cast<int>(idx);
    }
  }
  return best;
}

}  // namespace

class DStarRecoEfficiency : public edm::one::EDAnalyzer<edm::one::SharedResources> {
public:
  explicit DStarRecoEfficiency(const edm::ParameterSet& iConfig);
  ~DStarRecoEfficiency() override = default;

  void beginJob() override;
  void analyze(const edm::Event& iEvent, const edm::EventSetup& iSetup) override;
  void endJob() override;

private:
  struct D0DaughterSummary {
    int gammaCount = 0;
    int nonGammaCount = 0;
    bool hasKaon = false;
    bool hasPion = false;
  };

  D0DaughterSummary summarizeD0Daughters(const reco::Candidate* d0) const;
  const reco::GenParticle* findAncestor(const reco::GenParticle* particle, int absPdgId) const;
  bool matchD0WithFSR(const reco::Candidate* recoDau1,
                      const reco::Candidate* recoDau2,
                      const reco::GenParticle* genD0,
                      double maxDr) const;
  std::vector<unsigned int> findDaughterPermutation(const reco::GenParticle& particle, bool twoLayerDecay) const;
  bool isValidDStarDecayChain(const reco::Candidate* d1, const reco::Candidate* d2) const;
  std::vector<reco::GenParticleRef> processGenMatching(const edm::Handle<reco::GenParticleCollection>& genpars) const;
  const reco::GenParticle* matchRecoCandidate(const pat::CompositeCandidate& trk,
                                              const std::vector<const reco::GenParticle*>& genTrackPool) const;
  std::vector<const reco::GenParticle*> buildGenTrackPool(
      const std::vector<reco::GenParticleRef>& genRefs,
      const edm::Handle<reco::GenParticleCollection>& genpars) const;

  edm::EDGetTokenT<pat::CompositeCandidateCollection> tok_dstar_;
  edm::EDGetTokenT<reco::GenParticleCollection> tok_gen_;

  std::vector<double> ptBins_;
  double deltaR_;
  bool decayInGen_;
  bool twoLayerDecay_;
  bool genOnly_;
  int PID_;
  int PID_dau1_;
  int PID_dau2_;

  int d0PdgId_;
  int pionPdgId_;
  int kaonPdgId_;

  TEfficiency* effPt_;
  TH1D* hDenPt_;
  TH1D* hNumPt_;
};

DStarRecoEfficiency::DStarRecoEfficiency(const edm::ParameterSet& iConfig)
    : effPt_(nullptr), hDenPt_(nullptr), hNumPt_(nullptr) {
  usesResource("TFileService");

  tok_dstar_ = consumes<pat::CompositeCandidateCollection>(
      iConfig.getUntrackedParameter<edm::InputTag>("DStarCollection"));
  tok_gen_ = consumes<reco::GenParticleCollection>(
      iConfig.getUntrackedParameter<edm::InputTag>("GenParticleCollection"));

  ptBins_ = iConfig.getUntrackedParameter<std::vector<double> >("ptBins");
  deltaR_ = iConfig.getUntrackedParameter<double>("deltaR", 0.03);
  decayInGen_ = iConfig.getUntrackedParameter<bool>("decayInGen", true);
  twoLayerDecay_ = iConfig.getUntrackedParameter<bool>("twoLayerDecay", true);
  genOnly_ = iConfig.getUntrackedParameter<bool>("genOnly", false);

  PID_ = iConfig.getUntrackedParameter<int>("PID", 413);
  PID_dau1_ = iConfig.getUntrackedParameter<int>("PID_dau1", 421);
  PID_dau2_ = iConfig.getUntrackedParameter<int>("PID_dau2", 211);

  d0PdgId_ = iConfig.getUntrackedParameter<int>("D0PdgId", 421);
  pionPdgId_ = iConfig.getUntrackedParameter<int>("PionPdgId", 211);
  kaonPdgId_ = iConfig.getUntrackedParameter<int>("KaonPdgId", 321);
}

void DStarRecoEfficiency::beginJob() {
  edm::Service<TFileService> fs;
  if (!fs.isAvailable()) {
    throw cms::Exception("DStarRecoEfficiency") << "TFileService not available";
  }

  if (ptBins_.size() < 2) {
    throw cms::Exception("DStarRecoEfficiency") << "ptBins must have at least 2 entries";
  }

  hDenPt_ = fs->make<TH1D>("hGenDStarPt", "GEN D*;p_{T} (GeV);counts",
                           static_cast<int>(ptBins_.size() - 1), ptBins_.data());
  hNumPt_ = fs->make<TH1D>("hRecoMatchedDStarPt", "RECO matched D*;p_{T} (GeV);counts",
                           static_cast<int>(ptBins_.size() - 1), ptBins_.data());

  effPt_ = fs->make<TEfficiency>("effDStarRecoPt", "D* reco efficiency;GEN p_{T} (GeV);efficiency",
                                 static_cast<int>(ptBins_.size() - 1), ptBins_.data());
}

DStarRecoEfficiency::D0DaughterSummary
DStarRecoEfficiency::summarizeD0Daughters(const reco::Candidate* d0) const {
  D0DaughterSummary out;
  if (!d0) return out;
  for (size_t idau = 0; idau < d0->numberOfDaughters(); ++idau) {
    const auto* dau = d0->daughter(idau);
    if (!dau) continue;
    const int absId = std::abs(dau->pdgId());
    if (absId == 22) {
      ++out.gammaCount;
      continue;
    }
    ++out.nonGammaCount;
    if (absId == kaonPdgId_) out.hasKaon = true;
    if (absId == pionPdgId_) out.hasPion = true;
  }
  return out;
}

const reco::GenParticle*
DStarRecoEfficiency::findAncestor(const reco::GenParticle* particle, int absPdgId) const {
  const reco::GenParticle* current = particle;
  while (current) {
    const reco::Candidate* mother = current->mother();
    current = dynamic_cast<const reco::GenParticle*>(mother);
    if (!current) break;
    if (std::abs(current->pdgId()) == absPdgId) return current;
  }
  return nullptr;
}

bool DStarRecoEfficiency::matchD0WithFSR(const reco::Candidate* recoDau1,
                                         const reco::Candidate* recoDau2,
                                         const reco::GenParticle* genD0,
                                         double maxDr) const {
  if (!genD0 || !recoDau1 || !recoDau2) return false;

  bool invalidDau = false;
  bool hasKMatch = false;
  bool hasPiMatch = false;
  bool usedReco1 = false;
  bool usedReco2 = false;

  auto tryMatch = [&](const reco::Candidate* recoDau, bool& usedFlag,
                      const reco::Candidate* genDau) -> bool {
    if (usedFlag || !recoDau || !genDau) return false;
    if (reco::deltaR(*genDau, *recoDau) < maxDr) {
      usedFlag = true;
      return true;
    }
    return false;
  };

  for (size_t idau = 0; idau < genD0->numberOfDaughters(); ++idau) {
    const auto* genDau = genD0->daughter(idau);
    if (!genDau) continue;
    const int absId = std::abs(genDau->pdgId());
    if (absId == 22) continue;
    if (absId != kaonPdgId_ && absId != pionPdgId_) {
      invalidDau = true;
      break;
    }

    bool matchedThis = tryMatch(recoDau1, usedReco1, genDau);
    if (!matchedThis) matchedThis = tryMatch(recoDau2, usedReco2, genDau);

    if (matchedThis) {
      if (absId == kaonPdgId_) hasKMatch = true;
      else if (absId == pionPdgId_) hasPiMatch = true;
    }
  }

  return (!invalidDau && hasKMatch && hasPiMatch);
}

std::vector<unsigned int>
DStarRecoEfficiency::findDaughterPermutation(const reco::GenParticle& particle,
                                             bool twoLayerDecay) const {
  std::vector<unsigned int> idxs;

  std::vector<unsigned int> nonGammaDauIndices;
  for (size_t i = 0; i < particle.numberOfDaughters(); ++i) {
    if (std::abs(particle.daughter(i)->pdgId()) != 22) {
      nonGammaDauIndices.push_back(i);
    }
  }

  if (static_cast<int>(nonGammaDauIndices.size()) != 2) return idxs;

  std::vector<unsigned int> permutations(2);
  std::iota(permutations.begin(), permutations.end(), 0);
  std::sort(permutations.begin(), permutations.end());

  do {
    auto d1 = particle.daughter(nonGammaDauIndices[permutations.at(0)]);
    auto d2 = particle.daughter(nonGammaDauIndices[permutations.at(1)]);
    if (std::abs(d1->pdgId()) == PID_dau1_ && std::abs(d2->pdgId()) == PID_dau2_) {
      if (twoLayerDecay) {
        if (isValidDStarDecayChain(d1, d2)) {
          idxs = {nonGammaDauIndices[permutations.at(0)], nonGammaDauIndices[permutations.at(1)]};
          break;
        }
      } else {
        idxs = {nonGammaDauIndices[permutations.at(0)], nonGammaDauIndices[permutations.at(1)]};
        break;
      }
    }
  } while (std::next_permutation(permutations.begin(), permutations.end()));

  return idxs;
}

bool DStarRecoEfficiency::isValidDStarDecayChain(const reco::Candidate* d1,
                                                 const reco::Candidate* d2) const {
  if (!d1 || !d2) return false;

  const reco::Candidate* d0 = nullptr;
  if (std::abs(d1->pdgId()) == d0PdgId_ && std::abs(d2->pdgId()) == pionPdgId_) {
    d0 = d1;
  } else if (std::abs(d1->pdgId()) == pionPdgId_ && std::abs(d2->pdgId()) == d0PdgId_) {
    d0 = d2;
  } else {
    return false;
  }

  const auto d0Summary = summarizeD0Daughters(d0);
  if (d0Summary.nonGammaCount != 2 || !d0Summary.hasKaon || !d0Summary.hasPion) {
    return false;
  }
  return true;
}

std::vector<reco::GenParticleRef>
DStarRecoEfficiency::processGenMatching(const edm::Handle<reco::GenParticleCollection>& genpars) const {
  std::vector<reco::GenParticleRef> genRefs;
  if (!genpars.isValid()) {
    return genRefs;
  }

  for (unsigned int it = 0; it < genpars->size(); ++it) {
    const reco::GenParticle& trk = (*genpars)[it];
    const int id = trk.pdgId();
    if (std::abs(id) != PID_) continue;

    bool hasDau1 = false;
    bool hasDau2 = false;
    int dstarGammaCount = 0;
    for (size_t i = 0; i < trk.numberOfDaughters(); ++i) {
      const int dauId = std::abs(trk.daughter(i)->pdgId());
      if (dauId == 22) {
        ++dstarGammaCount;
        continue;
      }
      if (dauId == std::abs(PID_dau1_)) hasDau1 = true;
      if (dauId == std::abs(PID_dau2_)) hasDau2 = true;
    }

    if (decayInGen_ && (!hasDau1 || !hasDau2)) continue;
    if (twoLayerDecay_ && decayInGen_ && dstarGammaCount > 0) continue;

    std::vector<unsigned int> idxs = findDaughterPermutation(trk, twoLayerDecay_);
    if (decayInGen_ && idxs.empty()) continue;

    genRefs.push_back(reco::GenParticleRef(genpars, it));
  }
  return genRefs;
}

std::vector<const reco::GenParticle*> DStarRecoEfficiency::buildGenTrackPool(
    const std::vector<reco::GenParticleRef>& genRefs,
    const edm::Handle<reco::GenParticleCollection>& genpars) const {
  std::vector<const reco::GenParticle*> genTrackPool;
  auto addStableTrack = [&](const reco::GenParticle* p) {
    if (!p) return;
    if (p->status() != 1) return;
    if (p->charge() == 0) return;
    const int absId = std::abs(p->pdgId());
    if (absId != pionPdgId_ && absId != kaonPdgId_) return;
    genTrackPool.push_back(p);
  };

  if (!genRefs.empty()) {
    for (const auto& genRef : genRefs) {
      const auto* gen = genRef.get();
      if (!gen) continue;

      const reco::GenParticle* d0 = nullptr;
      const reco::GenParticle* slowPi = nullptr;
      for (size_t i = 0; i < gen->numberOfDaughters(); ++i) {
        const auto* dau = gen->daughter(i);
        if (!dau) continue;
        const int absId = std::abs(dau->pdgId());
        if (absId == d0PdgId_) {
          d0 = dynamic_cast<const reco::GenParticle*>(dau);
        } else if (absId == pionPdgId_ && dau->charge() != 0) {
          slowPi = dynamic_cast<const reco::GenParticle*>(dau);
        } else {
          if (absId == 22) continue;
          addStableTrack(dynamic_cast<const reco::GenParticle*>(dau));
        }
      }

      if (d0) {
        for (size_t j = 0; j < d0->numberOfDaughters(); ++j) {
          const auto* gd = d0->daughter(j);
          if (!gd) continue;
          if (std::abs(gd->pdgId()) == 22) continue;
          addStableTrack(dynamic_cast<const reco::GenParticle*>(gd));
        }
      }
      addStableTrack(slowPi);
    }
  }

  if (genTrackPool.empty() && genpars.isValid()) {
    genTrackPool.reserve(genpars->size());
    for (const auto& gen : *genpars) {
      addStableTrack(&gen);
    }
  }
  return genTrackPool;
}

const reco::GenParticle* DStarRecoEfficiency::matchRecoCandidate(
    const pat::CompositeCandidate& trk,
    const std::vector<const reco::GenParticle*>& genTrackPool) const {
  if (genTrackPool.empty()) return nullptr;
  if (!twoLayerDecay_) return nullptr;

  const reco::Candidate* d1 = trk.daughter(0);
  const reco::Candidate* d2 = trk.daughter(1);

  const reco::Candidate* recoD0 = nullptr;
  const reco::Candidate* recoSlow = nullptr;
  if (d1 && std::abs(d1->pdgId()) == d0PdgId_) {
    recoD0 = d1;
    recoSlow = d2;
  } else if (d2 && std::abs(d2->pdgId()) == d0PdgId_) {
    recoD0 = d2;
    recoSlow = d1;
  } else {
    recoD0 = d1;
    recoSlow = d2;
  }

  bool validReco = recoD0 && recoSlow && recoD0->numberOfDaughters() >= 2;
  const reco::Candidate* recoKaonCand = validReco ? recoD0->daughter(0) : nullptr;
  const reco::Candidate* recoPionCand = validReco ? recoD0->daughter(1) : nullptr;
  validReco = validReco && recoKaonCand && recoPionCand;
  if (!validReco) return nullptr;

  std::vector<bool> used(genTrackPool.size(), false);
  auto kaonMatch = findBestTrackMatch(*recoKaonCand, genTrackPool, used, deltaR_);
  if (kaonMatch.index >= 0 && static_cast<size_t>(kaonMatch.index) < used.size()) {
    used[kaonMatch.index] = true;
  }

  auto pionMatch = findBestTrackMatch(*recoPionCand, genTrackPool, used, deltaR_);
  if (pionMatch.index >= 0 && static_cast<size_t>(pionMatch.index) < used.size()) {
    used[pionMatch.index] = true;
  }

  auto slowPionMatch = findBestTrackMatch(*recoSlow, genTrackPool, used, deltaR_);
  if (slowPionMatch.index >= 0 && static_cast<size_t>(slowPionMatch.index) < used.size()) {
    used[slowPionMatch.index] = true;
  }

  const bool allTracksMatched = kaonMatch.particle && pionMatch.particle && slowPionMatch.particle;
  if (!allTracksMatched) return nullptr;

  const reco::GenParticle* genD0 = nullptr;
  const reco::GenParticle* genFromFirst = findAncestor(kaonMatch.particle, d0PdgId_);
  const reco::GenParticle* genFromSecond = findAncestor(pionMatch.particle, d0PdgId_);
  if (genFromFirst && genFromSecond && genFromFirst == genFromSecond) {
    genD0 = genFromFirst;
  }

  const reco::GenParticle* genDStar = nullptr;
  if (genD0) {
    genDStar = findAncestor(genD0, std::abs(PID_));
  }
  if (!genDStar && slowPionMatch.particle) {
    genDStar = findAncestor(slowPionMatch.particle, std::abs(PID_));
  }

  const reco::GenParticle* genDStarFromD0 = genD0 ? findAncestor(genD0, std::abs(PID_)) : nullptr;
  bool validChain = false;
  bool radiativeChain = false;
  if (genDStarFromD0 && genD0) {
    const bool dstarHas2 = genDStarFromD0->numberOfDaughters() == 2;
    const auto d0Summary = summarizeD0Daughters(genD0);
    const bool d0Strict2 = (d0Summary.gammaCount == 0 && d0Summary.nonGammaCount == 2 &&
                            d0Summary.hasKaon && d0Summary.hasPion);
    const bool d0Radiative = (d0Summary.gammaCount >= 1 && d0Summary.nonGammaCount == 2 &&
                              d0Summary.hasKaon && d0Summary.hasPion);
    bool dstarPDGs = false;
    bool d0PDGsStrict = false;
    bool d0PDGsAny = false;
    bool directSlowPi = false;
    if (dstarHas2) {
      const auto* ds_d0 = genDStarFromD0->daughter(0);
      const auto* ds_pi = genDStarFromD0->daughter(1);
      const int a0 = std::abs(ds_d0->pdgId());
      const int a1 = std::abs(ds_pi->pdgId());
      dstarPDGs = ((a0 == d0PdgId_ && a1 == pionPdgId_) || (a1 == d0PdgId_ && a0 == pionPdgId_));
      dstarPDGs = dstarPDGs && (ds_d0 == genD0 || ds_pi == genD0);
      if (slowPionMatch.particle) {
        directSlowPi =
            ((ds_d0 == genD0 && ds_pi == slowPionMatch.particle) ||
             (ds_pi == genD0 && ds_d0 == slowPionMatch.particle));
      }
    }
    d0PDGsAny = matchD0WithFSR(recoKaonCand, recoPionCand, genD0, deltaR_);
    if (d0Strict2) {
      d0PDGsStrict = d0PDGsAny;
    }
    validChain = (dstarPDGs && d0PDGsStrict && directSlowPi);
    radiativeChain = (dstarPDGs && d0PDGsAny && directSlowPi && d0Radiative);
  }

  const bool matchGEN = (allTracksMatched && (validChain || radiativeChain));
  if (!matchGEN) return nullptr;

  return genDStar ? genDStar : genDStarFromD0;
}

void DStarRecoEfficiency::analyze(const edm::Event& iEvent, const edm::EventSetup&) {
  edm::Handle<reco::GenParticleCollection> genpars;
  iEvent.getByToken(tok_gen_, genpars);
  if (!genpars.isValid()) return;

  const auto genRefs = processGenMatching(genpars);
  const auto genTrackPool = buildGenTrackPool(genRefs, genpars);

  if (genOnly_) {
    for (const auto& genRef : genRefs) {
      const auto* gen = genRef.get();
      if (!gen) continue;
      hDenPt_->Fill(gen->pt());
    }
    return;
  }

  edm::Handle<pat::CompositeCandidateCollection> recos;
  iEvent.getByToken(tok_dstar_, recos);

  std::unordered_set<const reco::GenParticle*> matchedGen;
  if (recos.isValid()) {
    for (const auto& trk : *recos) {
      const auto* genMatch = matchRecoCandidate(trk, genTrackPool);
      if (genMatch) matchedGen.insert(genMatch);
    }
  }

  for (const auto& genRef : genRefs) {
    const auto* gen = genRef.get();
    if (!gen) continue;
    const double pt = gen->pt();
    const bool matched = matchedGen.find(gen) != matchedGen.end();
    hDenPt_->Fill(pt);
    if (matched) hNumPt_->Fill(pt);
    effPt_->Fill(matched, pt);
  }
}

void DStarRecoEfficiency::endJob() {}

DEFINE_FWK_MODULE(DStarRecoEfficiency);
