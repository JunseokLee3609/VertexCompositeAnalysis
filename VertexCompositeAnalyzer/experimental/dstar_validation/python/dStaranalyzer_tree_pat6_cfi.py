import FWCore.ParameterSet.Config as cms

# Minimal gen-only analyzer for D* chain validation.
# Target chain: D* -> D0 + slow pion, D0 -> K + pion
# D0 FSR gamma is allowed, D* level gamma can be rejected by config.
dStarana_pat6_gen = cms.EDAnalyzer(
    "PATCompositeTreeProducer6",
    GenParticleCollection=cms.untracked.InputTag("genParticles"),
    doGenMatching=cms.untracked.bool(True),
    doGenNtuple=cms.untracked.bool(True),
    PID=cms.untracked.int32(413),
    PID_dau1=cms.untracked.int32(421),
    PID_dau2=cms.untracked.int32(211),
    decayInGen=cms.untracked.bool(True),
    twoLayerDecay=cms.untracked.bool(True),
    debugGenMatching=cms.untracked.bool(True),
    verboseDebug=cms.untracked.bool(False),
    VertexCollection=cms.untracked.InputTag("offlinePrimaryVertices"),
    TrackCollection=cms.untracked.InputTag("generalTracks"),
    multMax=cms.untracked.double(-1),
    multMin=cms.untracked.double(-1),
    doRunInfo=cms.bool(True),
    isEventPlane=cms.untracked.bool(False),
    eventplaneSrc=cms.untracked.InputTag("hiEvtPlane"),
    eventplaneSrcRecalc=cms.untracked.InputTag(""),
)
