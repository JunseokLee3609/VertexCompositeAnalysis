import FWCore.ParameterSet.Config as cms

dstarRecoEff = cms.EDAnalyzer(
    "DStarRecoEfficiency",
    DStarCollection=cms.untracked.InputTag("generalDStarCandidatesNew", "DStar"),
    GenParticleCollection=cms.untracked.InputTag("genParticles"),
    ptBins=cms.untracked.vdouble(5, 7, 10, 20, 30, 50),
    deltaR=cms.untracked.double(0.03),
    decayInGen=cms.untracked.bool(True),
    twoLayerDecay=cms.untracked.bool(True),
    genOnly=cms.untracked.bool(False),
    PID=cms.untracked.int32(413),
    PID_dau1=cms.untracked.int32(421),
    PID_dau2=cms.untracked.int32(211),
    D0PdgId=cms.untracked.int32(421),
    PionPdgId=cms.untracked.int32(211),
    KaonPdgId=cms.untracked.int32(321),
)
