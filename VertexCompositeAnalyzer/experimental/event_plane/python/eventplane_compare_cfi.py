import FWCore.ParameterSet.Config as cms

evtPlaneCompare = cms.EDAnalyzer(
    "EvtPlaneComparator",
    stored=cms.InputTag("hiEvtPlaneFlat"),
    recomputed=cms.InputTag("hiEvtPlaneFlatRecalc"),
    angleTolerance=cms.untracked.double(1e-6),
    qTolerance=cms.untracked.double(1e-6),
    sumTolerance=cms.untracked.double(1e-6),
    failOnMissing=cms.untracked.bool(True),
    debugLog=cms.untracked.bool(False),
    maxDebugPrints=cms.untracked.int32(200),
    zeroSpikeWindow=cms.untracked.double(1e-12),
    mismatchPrintThreshold=cms.untracked.double(0.02),
)
