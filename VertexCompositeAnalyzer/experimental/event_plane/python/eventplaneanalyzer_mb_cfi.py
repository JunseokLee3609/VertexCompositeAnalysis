import FWCore.ParameterSet.Config as cms

eventplaneMB = cms.EDAnalyzer(
    "PATEventPlaneTrackMB",
    doRecoNtuple=cms.untracked.bool(True),
    saveTree=cms.untracked.bool(True),
    beamSpotSrc=cms.untracked.InputTag("offlineBeamSpot"),
    VertexCollection=cms.untracked.InputTag("offlinePrimaryVertices"),
    VertexCompositeCollection=cms.untracked.InputTag(""),
    TrackCollection=cms.untracked.InputTag("generalTracks"),
    eventplaneSrcRecalc=cms.untracked.InputTag("hiEvtPlaneFlatRecalc"),
    massMinForExclusion=cms.untracked.double(1.7),
    massMaxForExclusion=cms.untracked.double(2.1),
    isCentrality=cms.bool(True),
    centralityBinLabel=cms.InputTag("centralityBin", "HFtowers"),
    centralitySrc=cms.InputTag("hiCentrality"),
)
