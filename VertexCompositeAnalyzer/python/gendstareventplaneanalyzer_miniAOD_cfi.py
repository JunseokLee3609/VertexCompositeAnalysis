import FWCore.ParameterSet.Config as cms

genDstarEventPlaneMiniAOD = cms.EDAnalyzer(
    "MiniAODGenDstarEventPlaneTrack",
    saveTree=cms.untracked.bool(True),
    saveHistogram=cms.untracked.bool(False),
    prunedGenParticles=cms.untracked.InputTag("prunedGenParticles"),
    packedGenParticles=cms.untracked.InputTag("packedGenParticles"),
    trackPtMin=cms.untracked.double(0.3),
    trackPtMax=cms.untracked.double(3.0),
    trackAbsEtaMax=cms.untracked.double(2.4),
    subEventAbsEtaMin=cms.untracked.double(0.5),
    harmonic2=cms.untracked.int32(2),
    harmonic3=cms.untracked.int32(3),
    isCentrality=cms.bool(False),
    centralityBinLabel=cms.InputTag("centralityBin", "HFtowers"),
    centralitySrc=cms.InputTag("hiCentrality"),
)
