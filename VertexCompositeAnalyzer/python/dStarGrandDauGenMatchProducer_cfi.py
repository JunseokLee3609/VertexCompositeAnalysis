import FWCore.ParameterSet.Config as cms

# Analyzer that performs generator matching for the D* decay chain and slow pion tracks.
dStarGrandDauGenMatchProducer = cms.EDAnalyzer(
    "DStarGrandDauGenMatchProducer",
    dStarCollection=cms.InputTag("generalDStarCandidatesNew", "DStar"),
    genParticleCollection=cms.InputTag("genParticles"),
    maxDeltaR=cms.untracked.double(0.05),
    keepChargeMismatch=cms.untracked.bool(False),
)
