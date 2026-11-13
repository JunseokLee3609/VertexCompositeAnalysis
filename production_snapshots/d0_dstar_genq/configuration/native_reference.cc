std::vector<reco::GenParticleRef> PATCompositeTreeProducer6::processGenMatching(const edm::Handle<reco::GenParticleCollection>& genpars) {
  std::vector<reco::GenParticleRef> genRefs;

  auto hasKPiFromDaughters = [&](const reco::Candidate* cand) {
    if (!cand || cand->numberOfDaughters() < 2) { return false; }
    std::vector<unsigned int> perm(2);
    std::iota(perm.begin(), perm.end(), 0);
    std::sort(perm.begin(), perm.end());
    do {
      const reco::Candidate* d1 = cand->daughter(perm[0]);
      const reco::Candidate* d2 = cand->daughter(perm[1]);
      if (!d1 || !d2) { continue;}
      if (std::abs(d1->pdgId()) == kPdgKaon && std::abs(d2->pdgId()) == kPdgPion) { return true; }
    } while (std::next_permutation(perm.begin(), perm.end()));
    return false;
  };

  auto hasKPiGammaFromDaughters = [&](const reco::Candidate* cand) {
    if (!cand || cand->numberOfDaughters() < 3) {
      return false;
    }
    std::vector<unsigned int> perm(3);
    std::iota(perm.begin(), perm.end(), 0);
    std::sort(perm.begin(), perm.end());
    do {
      const reco::Candidate* d1 = cand->daughter(perm[0]);
      const reco::Candidate* d2 = cand->daughter(perm[1]);
      const reco::Candidate* d3 = cand->daughter(perm[2]);
      if (!d1 || !d2 || !d3) {
        continue;
      }
      if (std::abs(d1->pdgId()) == kPdgKaon && std::abs(d2->pdgId()) == kPdgPion &&
          std::abs(d3->pdgId()) == kPdgGamma) {
        return true;
      }
    } while (std::next_permutation(perm.begin(), perm.end()));
    return false;
  };

  if (!doGenMatching_) {
    return genRefs;
  }

  if (!genpars.isValid()) {
    return genRefs;
  }

  for (unsigned int it = 0; it < genpars->size(); ++it) {
    const reco::GenParticle& trk = (*genpars)[it];
    const int id = trk.pdgId();

    if (std::abs(id) != std::abs(pid_)) {
      continue;
    }

    // Match the MiniAOD signal definition used by PackedGenParticleSignalProducer.
    // In an embedded event collisionId()==0 is the primary Pythia/EvtGen signal;
    // non-zero collision IDs belong to the mixed HYDJET background.
    if (signalOnlyGenMatching_ && trk.collisionId() != 0) {
      if (debugGenMatching_) {
        edm::LogVerbatim("PAT6GenMatching")
            << "reject gen candidate index=" << it
            << " pdgId=" << id
            << " collisionId=" << trk.collisionId()
            << " pt=" << trk.pt()
            << " reason=non-signal collision";
      }
      continue;
    }

    if (!decayInGen_) {
      genRefs.emplace_back(genpars, it);
      continue;
    }

    int dstarGammaCount = 0;
    std::vector<unsigned int> dstarNonGammaIdx;
    dstarNonGammaIdx.reserve(trk.numberOfDaughters());
    for (size_t idau = 0; idau < trk.numberOfDaughters(); ++idau) {
      const reco::Candidate* dau = trk.daughter(idau);
      if (!dau) {
        continue;
      }
      if (std::abs(dau->pdgId()) == kPdgGamma) {
        ++dstarGammaCount;
      } else {
        dstarNonGammaIdx.push_back(static_cast<unsigned int>(idau));
      }
    }

    if (twoLayerDecay_ && dstarGammaCount > 0) {
      continue;
    }

    if (dstarNonGammaIdx.size() != 2) {
      continue;
    }

    int idxLayer1 = -1;
    std::sort(dstarNonGammaIdx.begin(), dstarNonGammaIdx.end());
    do {
      const reco::Candidate* Dd1 = trk.daughter(dstarNonGammaIdx[0]);
      const reco::Candidate* Dd2 = trk.daughter(dstarNonGammaIdx[1]);
      if (!Dd1 || !Dd2) {
        continue;
      }
      if (std::abs(Dd1->pdgId()) == std::abs(pidDau1_) && std::abs(Dd2->pdgId()) == std::abs(pidDau2_)) {
        idxLayer1 = static_cast<int>(dstarNonGammaIdx[0]);
        break;
      }
    } while (std::next_permutation(dstarNonGammaIdx.begin(), dstarNonGammaIdx.end()));

    if (idxLayer1 < 0) {
      continue;
    }

    if (twoLayerDecay_) {
      const reco::Candidate* d0 = trk.daughter(static_cast<unsigned int>(idxLayer1));
      if (!d0) {
        continue;
      }

      int d0GammaCount = 0;
      int d0NonGammaCount = 0;
      const reco::Candidate* genKaon = nullptr;
      for (size_t idau = 0; idau < d0->numberOfDaughters(); ++idau) {
        const reco::Candidate* dau = d0->daughter(idau);
        if (!dau) {
          continue;
        }
        if (std::abs(dau->pdgId()) == kPdgGamma) {
          ++d0GammaCount;
        } else {
          ++d0NonGammaCount;
          if (std::abs(dau->pdgId()) == kPdgKaon) genKaon = dau;
        }
      }

      if (d0GammaCount > 0) {
        if (d0->numberOfDaughters() != 3 || d0NonGammaCount != 2 || d0GammaCount != 1) {
          continue;
        }
        if (!hasKPiGammaFromDaughters(d0)) {
          continue;
        }
      } else {
        if (d0->numberOfDaughters() != 2 || d0NonGammaCount != 2) {
          continue;
        }
        if (!hasKPiFromDaughters(d0)) {
          continue;
        }
      }

      if (std::abs(pid_) == 413) {
        const auto* genSlowPion = trk.daughter(dstarNonGammaIdx[1]);
        if (!genKaon || !genSlowPion || genKaon->charge() * genSlowPion->charge() >= 0.) {
          continue;
        }
      }

      genRefs.emplace_back(genpars, it);
    } else {
      const int singleLayerGammaCount = dstarGammaCount;
      const int singleLayerNonGammaCount = static_cast<int>(dstarNonGammaIdx.size());

      if (singleLayerGammaCount > 0) {
        if (trk.numberOfDaughters() != 3 || singleLayerNonGammaCount != 2 || singleLayerGammaCount != 1) {
          continue;
        }
        if (!hasKPiGammaFromDaughters(&trk)) {
          continue;
        }
      } else {
        if (trk.numberOfDaughters() != 2 || singleLayerNonGammaCount != 2) {
          continue;
        }
        if (!hasKPiFromDaughters(&trk)) {
          continue;
        }
      }

      genRefs.emplace_back(genpars, it);
    }
  }

  return genRefs;
}
