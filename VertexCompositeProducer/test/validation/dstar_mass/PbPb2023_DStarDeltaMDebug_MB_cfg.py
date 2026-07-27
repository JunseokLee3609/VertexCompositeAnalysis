import os

import FWCore.ParameterSet.Config as cms

_cfg = os.path.join(
    os.environ["CMSSW_BASE"],
    "src",
    "VertexCompositeAnalysis",
    "VertexCompositeProducer",
    "test",
    "validation/dstar_mass/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA_eventplaneMBOnly.py",
)

with open(_cfg, "r") as _handle:
    exec(compile(_handle.read(), _cfg, "exec"))

process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(10000))
process.options.numberOfThreads = cms.untracked.uint32(8)
process.TFileService.fileName = cms.string(
    "VertexCompositeAnalysis/VertexCompositeProducer/test/dstar_delta_m_debug_abcd_10k.root"
)

process.generalD0CandidatesNewSS = process.generalD0CandidatesNew.clone(
    isWrongSign=cms.bool(True),
)

process.generalDStarCandidatesDebugA = process.generalDStarCandidatesNew.clone(
    d0Collection=cms.InputTag("generalD0CandidatesNew", "D0"),
    isWrongSign=cms.bool(False),
    debugCategoryCutflow=cms.bool(True),
    debugLabel=cms.string("DStarFitterDebug_A"),
)
process.generalDStarCandidatesDebugB = process.generalDStarCandidatesNew.clone(
    d0Collection=cms.InputTag("generalD0CandidatesNew", "D0"),
    isWrongSign=cms.bool(True),
    debugCategoryCutflow=cms.bool(True),
    debugLabel=cms.string("DStarFitterDebug_B"),
)
process.generalDStarCandidatesDebugC = process.generalDStarCandidatesNew.clone(
    d0Collection=cms.InputTag("generalD0CandidatesNewSS", "D0"),
    isWrongSign=cms.bool(False),
    debugCategoryCutflow=cms.bool(True),
    debugLabel=cms.string("DStarFitterDebug_C"),
)
process.generalDStarCandidatesDebugD = process.generalDStarCandidatesNew.clone(
    d0Collection=cms.InputTag("generalD0CandidatesNewSS", "D0"),
    isWrongSign=cms.bool(True),
    debugCategoryCutflow=cms.bool(True),
    debugLabel=cms.string("DStarFitterDebug_D"),
)

process.dStarDeltaMDebug = cms.EDAnalyzer(
    "DStarDeltaMDebugAnalyzer",
    d0RS=cms.InputTag("generalD0CandidatesNew", "D0"),
    d0SS=cms.InputTag("generalD0CandidatesNewSS", "D0"),
    tracks=cms.InputTag("unpackedTracksAndVertices"),
    tkChi2Cut=cms.double(5.0),
    tkNhitsCut=cms.int32(0),
    tkPtCut=cms.double(0.3),
    tkPtErrCut=cms.double(0.1),
    tkEtaCut=cms.double(999.0),
    trackQualities=cms.vstring("highPurity"),
)

process.dStarDeltaMDebug_step = cms.Path(
    process.eventFilter_HM
    * process.generalD0CandidatesNew
    * process.generalD0CandidatesNewSS
    * process.generalDStarCandidatesDebugA
    * process.generalDStarCandidatesDebugB
    * process.generalDStarCandidatesDebugC
    * process.generalDStarCandidatesDebugD
    * process.dStarDeltaMDebug
)

process.schedule = cms.Schedule(
    process.Flag_colEvtSel,
    process.Flag_primaryVertexFilter,
    process.c,
    process.eventFilter_HM_step,
    process.dStarDeltaMDebug_step,
)
