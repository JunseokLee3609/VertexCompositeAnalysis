#include "VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/DStarGrandDauGenMatchProducer.h"

#include <algorithm>
#include <cmath>

#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"

#include "DataFormats/Math/interface/deltaR.h"

DStarGrandDauGenMatchProducer::DStarGrandDauGenMatchProducer(const edm::ParameterSet& cfg)
    : dstarToken_(consumes<pat::CompositeCandidateCollection>(
          cfg.getParameter<edm::InputTag>("dStarCollection"))),
      genToken_(consumes<reco::GenParticleCollection>(
          cfg.getParameter<edm::InputTag>("genParticleCollection"))),
      maxDeltaR_(cfg.getUntrackedParameter<double>("maxDeltaR", 0.05)),
      keepChargeMismatch_(cfg.getUntrackedParameter<bool>("keepChargeMismatch", false)),
      strictDecayChain_(cfg.getUntrackedParameter<bool>("strictDecayChain", true)) {
  usesResource("TFileService");
}

void DStarGrandDauGenMatchProducer::beginJob() {
  edm::Service<TFileService> fs;
  tree_ = fs->make<TTree>("dstarGenMatch", "D* granddaughter track gen matching");

  tree_->Branch("nCand", &nCand_, "nCand/I");
  tree_->Branch("dstar_pt", &dstar_pt_);
  tree_->Branch("dstar_eta", &dstar_eta_);
  tree_->Branch("dstar_phi", &dstar_phi_);
  tree_->Branch("dstar_mass", &dstar_mass_);
  tree_->Branch("dstar_hasGenMatch", &dstar_hasGenMatch_);
  tree_->Branch("dstar_match_dr", &dstar_match_dr_);
  tree_->Branch("dstar_match_pdgId", &dstar_match_pdgId_);
  tree_->Branch("dstar_match_pt", &dstar_match_pt_);
  tree_->Branch("dstar_match_eta", &dstar_match_eta_);
  tree_->Branch("dstar_match_phi", &dstar_match_phi_);
  tree_->Branch("dstar_match_mass", &dstar_match_mass_);

  tree_->Branch("d0_hasGenMatch", &d0_hasGenMatch_);
  tree_->Branch("d0_match_dr", &d0_match_dr_);
  tree_->Branch("d0_match_pdgId", &d0_match_pdgId_);
  tree_->Branch("d0_match_pt", &d0_match_pt_);
  tree_->Branch("d0_match_eta", &d0_match_eta_);
  tree_->Branch("d0_match_phi", &d0_match_phi_);
  tree_->Branch("d0_match_mass", &d0_match_mass_);
  tree_->Branch("d0_reco_pt", &d0_reco_pt_);
  tree_->Branch("d0_reco_eta", &d0_reco_eta_);
  tree_->Branch("d0_reco_phi", &d0_reco_phi_);
  tree_->Branch("d0_reco_mass", &d0_reco_mass_);

  tree_->Branch("gdau1_reco_pdgId", &gdau1_reco_pdgId_);
  tree_->Branch("gdau1_reco_charge", &gdau1_reco_charge_);
  tree_->Branch("gdau1_reco_pt", &gdau1_reco_pt_);
  tree_->Branch("gdau1_reco_eta", &gdau1_reco_eta_);
  tree_->Branch("gdau1_reco_phi", &gdau1_reco_phi_);
  tree_->Branch("gdau1_reco_mass", &gdau1_reco_mass_);
  tree_->Branch("gdau1_hasMatch", &gdau1_hasMatch_);
  tree_->Branch("gdau1_match_dr", &gdau1_match_dr_);
  tree_->Branch("gdau1_match_pdgId", &gdau1_match_pdgId_);

  tree_->Branch("gdau2_reco_pdgId", &gdau2_reco_pdgId_);
  tree_->Branch("gdau2_reco_charge", &gdau2_reco_charge_);
  tree_->Branch("gdau2_reco_pt", &gdau2_reco_pt_);
  tree_->Branch("gdau2_reco_eta", &gdau2_reco_eta_);
  tree_->Branch("gdau2_reco_phi", &gdau2_reco_phi_);
  tree_->Branch("gdau2_reco_mass", &gdau2_reco_mass_);
  tree_->Branch("gdau2_hasMatch", &gdau2_hasMatch_);
  tree_->Branch("gdau2_match_dr", &gdau2_match_dr_);
  tree_->Branch("gdau2_match_pdgId", &gdau2_match_pdgId_);
  tree_->Branch("slow_reco_pdgId", &slow_reco_pdgId_);
  tree_->Branch("slow_reco_charge", &slow_reco_charge_);
  tree_->Branch("slow_reco_pt", &slow_reco_pt_);
  tree_->Branch("slow_reco_eta", &slow_reco_eta_);
  tree_->Branch("slow_reco_phi", &slow_reco_phi_);
  tree_->Branch("slow_reco_mass", &slow_reco_mass_);
  tree_->Branch("slow_hasMatch", &slow_hasMatch_);
  tree_->Branch("slow_match_dr", &slow_match_dr_);
  tree_->Branch("slow_match_pdgId", &slow_match_pdgId_);
  tree_->Branch("d0_decayMask", &d0_decayMask_);
  tree_->Branch("dstar_decayMask", &dstar_decayMask_);
}

void DStarGrandDauGenMatchProducer::resetBranches() {
  nCand_ = 0;

  dstar_pt_.clear();
  dstar_eta_.clear();
  dstar_phi_.clear();
  dstar_mass_.clear();
  dstar_hasGenMatch_.clear();
  dstar_match_dr_.clear();
  dstar_match_pdgId_.clear();
  dstar_match_pt_.clear();
  dstar_match_eta_.clear();
  dstar_match_phi_.clear();
  dstar_match_mass_.clear();

  d0_hasGenMatch_.clear();
  d0_match_dr_.clear();
  d0_match_pdgId_.clear();
  d0_match_pt_.clear();
  d0_match_eta_.clear();
  d0_match_phi_.clear();
  d0_match_mass_.clear();
  d0_reco_pt_.clear();
  d0_reco_eta_.clear();
  d0_reco_phi_.clear();
  d0_reco_mass_.clear();

  gdau1_reco_pdgId_.clear();
  gdau1_reco_charge_.clear();
  gdau1_reco_pt_.clear();
  gdau1_reco_eta_.clear();
  gdau1_reco_phi_.clear();
  gdau1_reco_mass_.clear();
  gdau1_hasMatch_.clear();
  gdau1_match_dr_.clear();
  gdau1_match_pdgId_.clear();

  gdau2_reco_pdgId_.clear();
  gdau2_reco_charge_.clear();
  gdau2_reco_pt_.clear();
  gdau2_reco_eta_.clear();
  gdau2_reco_phi_.clear();
  gdau2_reco_mass_.clear();
  gdau2_hasMatch_.clear();
  gdau2_match_dr_.clear();
  gdau2_match_pdgId_.clear();
  slow_reco_pdgId_.clear();
  slow_reco_charge_.clear();
  slow_reco_pt_.clear();
  slow_reco_eta_.clear();
  slow_reco_phi_.clear();
  slow_reco_mass_.clear();
  slow_hasMatch_.clear();
  slow_match_dr_.clear();
  slow_match_pdgId_.clear();
  d0_decayMask_.clear();
  dstar_decayMask_.clear();
}

DStarGrandDauGenMatchProducer::MatchResult DStarGrandDauGenMatchProducer::findBestMatch(
    const reco::Candidate& recoCand,
    const std::vector<const reco::GenParticle*>& genTracks,
    const std::vector<bool>& usedFlags) const {
  MatchResult best;
  for (size_t idx = 0; idx < genTracks.size(); ++idx) {
    if (idx < usedFlags.size() && usedFlags[idx]) {
      continue;
    }
    const auto* gen = genTracks[idx];
    if (!keepChargeMismatch_ && recoCand.charge() != 0 && gen->charge() != 0 &&
        recoCand.charge() != gen->charge()) {
      continue;
    }

    const double dr = reco::deltaR(recoCand.eta(), recoCand.phi(), gen->eta(), gen->phi());
    if (dr > maxDeltaR_) {
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

const reco::GenParticle* DStarGrandDauGenMatchProducer::findAncestor(
    const reco::GenParticle* particle, int absPdgId) const {
  const reco::GenParticle* current = particle;
  while (current) {
    const reco::Candidate* mother = current->mother();
    current = dynamic_cast<const reco::GenParticle*>(mother);
    if (!current) {
      break;
    }
    if (std::abs(current->pdgId()) == absPdgId) {
      return current;
    }
  }
  return nullptr;
}

bool DStarGrandDauGenMatchProducer::hasAcceptableSubdecay(const reco::GenParticle* particle,
                                                          unsigned int maxSubDaughters) const {
  if (!particle) {
    return false;
  }
  const unsigned int nDau = particle->numberOfDaughters();
  if (nDau == 0) {
    return true;
  }
  if (nDau > maxSubDaughters) {
    return false;
  }
  return true;
}

bool DStarGrandDauGenMatchProducer::isValidD0Decay(const reco::GenParticle* genD0,
                                                   unsigned int* maskOut) const {
  unsigned int mask = 0;
  if (genD0) {
    mask |= kD0ExistsBit;
    if (std::abs(genD0->pdgId()) == 421) {
      mask |= kD0PdgBit;
    }
    if (genD0->numberOfDaughters() == 2) {
      mask |= kD0TwoDaughtersBit;
    }

    const auto* dau0 = (genD0->numberOfDaughters() > 0)
                           ? dynamic_cast<const reco::GenParticle*>(genD0->daughter(0))
                           : nullptr;
    const auto* dau1 = (genD0->numberOfDaughters() > 1)
                           ? dynamic_cast<const reco::GenParticle*>(genD0->daughter(1))
                           : nullptr;
    if (dau0 && dau1) {
      mask |= kD0DaughterPtrBit;
      const auto fillDaughterBits = [&](const reco::GenParticle* dau) {
        if (!dau) {
          return;
        }
        const int absId = std::abs(dau->pdgId());
        if (absId == 321) {
          mask |= kD0HasKaonBit;
          if (hasAcceptableSubdecay(dau, 2)) {
            mask |= kD0KaonSubDecayBit;
          }
        } else if (absId == 211) {
          mask |= kD0HasPionBit;
          if (hasAcceptableSubdecay(dau, 2)) {
            mask |= kD0PionSubDecayBit;
          }
        }
      };
      fillDaughterBits(dau0);
      fillDaughterBits(dau1);
    }
  }

  if (maskOut) {
    *maskOut = mask;
  }
  return (mask & kD0RequiredMask) == kD0RequiredMask;
}

bool DStarGrandDauGenMatchProducer::isValidDStarDecayChain(const reco::GenParticle* genDStar,
                                                           const reco::GenParticle* genD0,
                                                           const reco::GenParticle* slowPion,
                                                           bool slowConsistent,
                                                           unsigned int* maskOut) const {
  unsigned int mask = 0;
  const reco::GenParticle* d0FromStar = nullptr;
  const reco::GenParticle* pionFromStar = nullptr;

  if (genDStar) {
    mask |= kDStarExistsBit;
    if (std::abs(genDStar->pdgId()) == 413) {
      mask |= kDStarPdgBit;
    }
    if (genDStar->numberOfDaughters() == 2) {
      mask |= kDStarTwoDaughtersBit;
    }

    const auto* first = (genDStar->numberOfDaughters() > 0)
                            ? dynamic_cast<const reco::GenParticle*>(genDStar->daughter(0))
                            : nullptr;
    const auto* second = (genDStar->numberOfDaughters() > 1)
                             ? dynamic_cast<const reco::GenParticle*>(genDStar->daughter(1))
                             : nullptr;
    if (first && second) {
      mask |= kDStarDaughterPtrBit;
      if (std::abs(first->pdgId()) == 421 && std::abs(second->pdgId()) == 211) {
        d0FromStar = first;
        pionFromStar = second;
      } else if (std::abs(first->pdgId()) == 211 && std::abs(second->pdgId()) == 421) {
        d0FromStar = second;
        pionFromStar = first;
      }
    }
  }

  if (d0FromStar && genD0 && d0FromStar == genD0) {
    mask |= kDStarHasD0Bit;
  }

  if (slowPion) {
    const bool directMatch = (pionFromStar && slowPion == pionFromStar);
    bool ancestorMatch = false;
    if (!directMatch && pionFromStar) {
      const reco::GenParticle* current = slowPion;
      while (current) {
        if (current == pionFromStar) {
          ancestorMatch = true;
          break;
        }
        current = dynamic_cast<const reco::GenParticle*>(current->mother());
      }
    }
    if (directMatch || ancestorMatch) {
      mask |= kDStarHasSlowPionBit;
    }
    if (hasAcceptableSubdecay(slowPion, 2)) {
      mask |= kDStarSlowSubDecayBit;
    }
  }

  if (slowConsistent) {
    mask |= kDStarSlowLineageBit;
  }

  if (maskOut) {
    *maskOut = mask;
  }

  const bool hasRequiredBits = (mask & kDStarRequiredMask) == kDStarRequiredMask;
  if (!genD0 || !genDStar || !slowPion) {
    return false;
  }
  if (!hasRequiredBits) {
    return false;
  }
  return true;
}

void DStarGrandDauGenMatchProducer::analyze(const edm::Event& event, const edm::EventSetup&) {
  resetBranches();

  edm::Handle<pat::CompositeCandidateCollection> dstarHandle;
  event.getByToken(dstarToken_, dstarHandle);
  if (!dstarHandle.isValid()) {
    edm::LogWarning("DStarGrandDauGenMatch") << "D* composite candidate collection is missing";
    tree_->Fill();
    return;
  }

  edm::Handle<reco::GenParticleCollection> genHandle;
  event.getByToken(genToken_, genHandle);

  std::vector<const reco::GenParticle*> selectedGenTracks;
  if (genHandle.isValid()) {
    selectedGenTracks.reserve(genHandle->size());
    for (const auto& gen : *genHandle) {
      if (gen.status() != 1) {
        continue;
      }
      if (gen.charge() == 0) {
        continue;
      }
      const int absId = std::abs(gen.pdgId());
      if (absId != 211 && absId != 321) {
        continue;
      }
      selectedGenTracks.push_back(&gen);
    }
  } else {
    edm::LogWarning("DStarGrandDauGenMatch") << "GenParticle collection is missing";
  }

  for (const auto& cand : *dstarHandle) {
    nCand_++;

    std::vector<bool> usedGen(selectedGenTracks.size(), false);

    dstar_pt_.push_back(cand.pt());
    dstar_eta_.push_back(cand.eta());
    dstar_phi_.push_back(cand.phi());
    dstar_mass_.push_back(cand.mass());
    dstar_hasGenMatch_.push_back(0);
    dstar_match_dr_.push_back(kInvalidFloat_);
    dstar_match_pdgId_.push_back(kInvalidInt_);
    dstar_match_pt_.push_back(kInvalidFloat_);
    dstar_match_eta_.push_back(kInvalidFloat_);
    dstar_match_phi_.push_back(kInvalidFloat_);
    dstar_match_mass_.push_back(kInvalidFloat_);

    d0_hasGenMatch_.push_back(0);
    d0_match_dr_.push_back(kInvalidFloat_);
    d0_match_pdgId_.push_back(kInvalidInt_);
    d0_match_pt_.push_back(kInvalidFloat_);
    d0_match_eta_.push_back(kInvalidFloat_);
    d0_match_phi_.push_back(kInvalidFloat_);
    d0_match_mass_.push_back(kInvalidFloat_);

    const reco::Candidate* d0 = nullptr;
    for (size_t i = 0; i < cand.numberOfDaughters(); ++i) {
      const reco::Candidate* dau = cand.daughter(i);
      if (!dau) {
        continue;
      }
    if (std::abs(dau->pdgId()) == 421) {
      d0 = dau;
      break;
    }
  }

    if (!d0) {
      d0_reco_pt_.push_back(kInvalidFloat_);
      d0_reco_eta_.push_back(kInvalidFloat_);
      d0_reco_phi_.push_back(kInvalidFloat_);
      d0_reco_mass_.push_back(kInvalidFloat_);
    } else {
      d0_reco_pt_.push_back(d0->pt());
      d0_reco_eta_.push_back(d0->eta());
      d0_reco_phi_.push_back(d0->phi());
      d0_reco_mass_.push_back(d0->mass());
    }

    const reco::Candidate* gd1 = nullptr;
    const reco::Candidate* gd2 = nullptr;
    if (d0 && d0->numberOfDaughters() >= 2) {
      gd1 = d0->daughter(0);
      gd2 = d0->daughter(1);
    }

    const reco::Candidate* slowPion = nullptr;
    for (size_t i = 0; i < cand.numberOfDaughters(); ++i) {
      const reco::Candidate* dau = cand.daughter(i);
      if (!dau || dau == d0) {
        continue;
      }
      slowPion = dau;
      break;
    }

    const size_t candIndex = dstar_hasGenMatch_.size() - 1;
    const reco::GenParticle* matchedGd1 = nullptr;
    const reco::GenParticle* matchedGd2 = nullptr;
    const reco::GenParticle* matchedSlow = nullptr;

    if (!gd1) {
      gdau1_reco_pdgId_.push_back(kInvalidInt_);
      gdau1_reco_charge_.push_back(0);
      gdau1_reco_pt_.push_back(kInvalidFloat_);
      gdau1_reco_eta_.push_back(kInvalidFloat_);
      gdau1_reco_phi_.push_back(kInvalidFloat_);
      gdau1_reco_mass_.push_back(kInvalidFloat_);
      gdau1_hasMatch_.push_back(0);
      gdau1_match_dr_.push_back(kInvalidFloat_);
      gdau1_match_pdgId_.push_back(kInvalidInt_);
    } else {
      gdau1_reco_pdgId_.push_back(gd1->pdgId());
      gdau1_reco_charge_.push_back(gd1->charge());
      gdau1_reco_pt_.push_back(gd1->pt());
      gdau1_reco_eta_.push_back(gd1->eta());
      gdau1_reco_phi_.push_back(gd1->phi());
      gdau1_reco_mass_.push_back(gd1->mass());

      MatchResult match = findBestMatch(*gd1, selectedGenTracks, usedGen);
      if (match.particle) {
        gdau1_hasMatch_.push_back(1);
        gdau1_match_dr_.push_back(match.deltaR);
        gdau1_match_pdgId_.push_back(match.particle->pdgId());
        matchedGd1 = match.particle;
        if (match.index >= 0 && static_cast<size_t>(match.index) < usedGen.size()) {
          usedGen[match.index] = true;
        }
      } else {
        gdau1_hasMatch_.push_back(0);
        gdau1_match_dr_.push_back(kInvalidFloat_);
        gdau1_match_pdgId_.push_back(kInvalidInt_);
      }
    }

    if (!gd2) {
      gdau2_reco_pdgId_.push_back(kInvalidInt_);
      gdau2_reco_charge_.push_back(0);
      gdau2_reco_pt_.push_back(kInvalidFloat_);
      gdau2_reco_eta_.push_back(kInvalidFloat_);
      gdau2_reco_phi_.push_back(kInvalidFloat_);
      gdau2_reco_mass_.push_back(kInvalidFloat_);
      gdau2_hasMatch_.push_back(0);
      gdau2_match_dr_.push_back(kInvalidFloat_);
      gdau2_match_pdgId_.push_back(kInvalidInt_);
    } else {
      gdau2_reco_pdgId_.push_back(gd2->pdgId());
      gdau2_reco_charge_.push_back(gd2->charge());
      gdau2_reco_pt_.push_back(gd2->pt());
      gdau2_reco_eta_.push_back(gd2->eta());
      gdau2_reco_phi_.push_back(gd2->phi());
      gdau2_reco_mass_.push_back(gd2->mass());

      MatchResult match = findBestMatch(*gd2, selectedGenTracks, usedGen);
      if (match.particle) {
        gdau2_hasMatch_.push_back(1);
        gdau2_match_dr_.push_back(match.deltaR);
        gdau2_match_pdgId_.push_back(match.particle->pdgId());
        matchedGd2 = match.particle;
        if (match.index >= 0 && static_cast<size_t>(match.index) < usedGen.size()) {
          usedGen[match.index] = true;
        }
      } else {
        gdau2_hasMatch_.push_back(0);
        gdau2_match_dr_.push_back(kInvalidFloat_);
        gdau2_match_pdgId_.push_back(kInvalidInt_);
      }
    }

    if (!slowPion) {
      slow_reco_pdgId_.push_back(kInvalidInt_);
      slow_reco_charge_.push_back(0);
      slow_reco_pt_.push_back(kInvalidFloat_);
      slow_reco_eta_.push_back(kInvalidFloat_);
      slow_reco_phi_.push_back(kInvalidFloat_);
      slow_reco_mass_.push_back(kInvalidFloat_);
      slow_hasMatch_.push_back(0);
      slow_match_dr_.push_back(kInvalidFloat_);
      slow_match_pdgId_.push_back(kInvalidInt_);
    } else {
      slow_reco_pdgId_.push_back(slowPion->pdgId());
      slow_reco_charge_.push_back(slowPion->charge());
      slow_reco_pt_.push_back(slowPion->pt());
      slow_reco_eta_.push_back(slowPion->eta());
      slow_reco_phi_.push_back(slowPion->phi());
      slow_reco_mass_.push_back(slowPion->mass());

      MatchResult match = findBestMatch(*slowPion, selectedGenTracks, usedGen);
      if (match.particle) {
        slow_hasMatch_.push_back(1);
        slow_match_dr_.push_back(match.deltaR);
        slow_match_pdgId_.push_back(match.particle->pdgId());
        matchedSlow = match.particle;
        if (match.index >= 0 && static_cast<size_t>(match.index) < usedGen.size()) {
          usedGen[match.index] = true;
        }
      } else {
        slow_hasMatch_.push_back(0);
        slow_match_dr_.push_back(kInvalidFloat_);
        slow_match_pdgId_.push_back(kInvalidInt_);
      }
    }

    const reco::GenParticle* kaonGen = nullptr;
    const reco::GenParticle* pionGen = nullptr;
    if (matchedGd1) {
      const int absId = std::abs(matchedGd1->pdgId());
      if (absId == 321 && !kaonGen) {
        kaonGen = matchedGd1;
      } else if (absId == 211 && !pionGen) {
        pionGen = matchedGd1;
      }
    }
    if (matchedGd2) {
      const int absId = std::abs(matchedGd2->pdgId());
      if (absId == 321 && !kaonGen) {
        kaonGen = matchedGd2;
      } else if (absId == 211 && !pionGen) {
        pionGen = matchedGd2;
      }
    }

    const reco::GenParticle* genD0 = nullptr;
    if (kaonGen && pionGen) {
      const reco::GenParticle* d0FromKaon = findAncestor(kaonGen, 421);
      const reco::GenParticle* d0FromPion = findAncestor(pionGen, 421);
      if (d0FromKaon && d0FromPion && d0FromKaon == d0FromPion) {
        genD0 = d0FromKaon;
      }
    }

    const reco::GenParticle* genDStar = nullptr;
    bool slowConsistent = false;
    if (genD0) {
      genDStar = findAncestor(genD0, 413);
    }
    if (genDStar && matchedSlow) {
      const reco::GenParticle* dstarFromSlow = findAncestor(matchedSlow, 413);
      slowConsistent = (dstarFromSlow && dstarFromSlow == genDStar);
    }

    unsigned int d0Mask = 0;
    const bool d0StrictOk = isValidD0Decay(genD0, &d0Mask);
    d0_decayMask_.push_back(d0Mask);

    unsigned int dstarMask = 0;
    const bool dstarStrictOk = isValidDStarDecayChain(genDStar, genD0, matchedSlow, slowConsistent, &dstarMask);
    dstar_decayMask_.push_back(dstarMask);

    bool validD0Chain = (genD0 != nullptr);
    if (strictDecayChain_) {
      validD0Chain = validD0Chain && d0StrictOk;
    }

    bool validDStarChain = validD0Chain && matchedSlow && genDStar && slowConsistent;
    if (strictDecayChain_) {
      validDStarChain = validDStarChain && dstarStrictOk;
    }

    if (validDStarChain) {
      dstar_hasGenMatch_[candIndex] = 1;
      dstar_match_dr_[candIndex] = reco::deltaR(cand.eta(), cand.phi(), genDStar->eta(), genDStar->phi());
      dstar_match_pdgId_[candIndex] = genDStar->pdgId();
      dstar_match_pt_[candIndex] = genDStar->pt();
      dstar_match_eta_[candIndex] = genDStar->eta();
      dstar_match_phi_[candIndex] = genDStar->phi();
      dstar_match_mass_[candIndex] = genDStar->mass();
    }

    if (validD0Chain) {
      d0_hasGenMatch_[candIndex] = 1;
      if (d0 && genD0) {
        d0_match_dr_[candIndex] = reco::deltaR(d0->eta(), d0->phi(), genD0->eta(), genD0->phi());
      }
      if (genD0) {
        d0_match_pdgId_[candIndex] = genD0->pdgId();
        d0_match_pt_[candIndex] = genD0->pt();
        d0_match_eta_[candIndex] = genD0->eta();
        d0_match_phi_[candIndex] = genD0->phi();
        d0_match_mass_[candIndex] = genD0->mass();
      }
    }
  }

  tree_->Fill();
}

DEFINE_FWK_MODULE(DStarGrandDauGenMatchProducer);
