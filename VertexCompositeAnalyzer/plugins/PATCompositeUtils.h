#pragma once

#include <vector>
#include <limits>
#include <cmath>
#include "DataFormats/Candidate/interface/Candidate.h"
#include "DataFormats/HepMCCandidate/interface/GenParticle.h"
#include "DataFormats/Math/interface/deltaR.h"

namespace patcomp {

// Basic decay policy container to keep FSR/structure rules in one place
struct DecaySelectionConfig {
    bool decayInGen = false;       // require checking daughters at GEN
    bool twoLayerDecay = false;    // D* → D0 + π structure
    bool allowDstarGamma = false;  // allow extra gamma at D* level
    bool allowD0Gamma = true;      // allow FSR gamma at D0 level
    int pidDau1 = 0;
    int pidDau2 = 0;
};

struct DaughterScanResult {
    std::vector<unsigned int> nonGammaIndices;
    int gammaCount = 0;
};

inline DaughterScanResult scanDaughtersExcludingGamma(const reco::Candidate& particle) {
    DaughterScanResult res;
    const size_t nDau = particle.numberOfDaughters();
    res.nonGammaIndices.reserve(nDau);
    for (size_t i = 0; i < nDau; ++i) {
        const auto* dau = particle.daughter(i);
        if (!dau) continue;
        const int absId = std::abs(dau->pdgId());
        if (absId == 22) {
            ++res.gammaCount;
            continue;
        }
        res.nonGammaIndices.push_back(i);
    }
    return res;
}

struct TrackMatchResult {
    const reco::GenParticle* particle = nullptr;
    double deltaR = std::numeric_limits<double>::max();
    int index = -1;
};

inline TrackMatchResult findBestTrackMatch(const reco::Candidate& recoCand,
                                           const std::vector<const reco::GenParticle*>& genTracks,
                                           const std::vector<bool>& usedFlags,
                                           double maxDr) {
    TrackMatchResult best;
    for (size_t idx = 0; idx < genTracks.size(); ++idx) {
        if (idx < usedFlags.size() && usedFlags[idx]) continue;
        const auto* gen = genTracks[idx];
        if (!gen) continue;
        if (recoCand.charge() != 0 && gen->charge() != 0 && recoCand.charge() != gen->charge()) continue;
        const double dr = reco::deltaR(recoCand.eta(), recoCand.phi(), gen->eta(), gen->phi());
        if (dr > maxDr) continue;
        if (!best.particle || dr < best.deltaR) {
            best.particle = gen;
            best.deltaR = dr;
            best.index = static_cast<int>(idx);
        }
    }
    return best;
}

inline const reco::GenParticle* findAncestor(const reco::GenParticle* particle, int absPdgId) {
    const reco::GenParticle* current = particle;
    while (current) {
        const reco::Candidate* mother = current->mother();
        current = dynamic_cast<const reco::GenParticle*>(mother);
        if (!current) break;
        if (std::abs(current->pdgId()) == absPdgId) return current;
    }
    return nullptr;
}

inline bool matchD0WithFSR(const reco::Candidate* recoDau1,
                           const reco::Candidate* recoDau2,
                           const reco::GenParticle* genD0,
                           double maxDr) {
    if (!genD0 || !recoDau1 || !recoDau2) return false;

    bool invalidDau = false;
    bool hasKMatch = false;
    bool hasPiMatch = false;
    bool usedReco1 = false;
    bool usedReco2 = false;

    auto tryMatch = [&](const reco::Candidate* recoDau, bool& usedFlag, const reco::Candidate* genDau) -> bool {
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
        if (absId == 22) continue;  // allow FSR gamma
        if (absId != 321 && absId != 211) { invalidDau = true; break; }

        bool matchedThis = tryMatch(recoDau1, usedReco1, genDau);
        if (!matchedThis) matchedThis = tryMatch(recoDau2, usedReco2, genDau);

        if (matchedThis) {
            if (absId == 321) hasKMatch = true;
            else if (absId == 211) hasPiMatch = true;
        }
    }

    return (!invalidDau && hasKMatch && hasPiMatch);
}

}  // namespace patcomp

