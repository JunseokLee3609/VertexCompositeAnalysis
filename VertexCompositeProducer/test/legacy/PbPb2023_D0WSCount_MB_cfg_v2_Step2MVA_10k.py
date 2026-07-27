import os

import FWCore.ParameterSet.Config as cms

_base_cfg = os.path.join(
    os.environ["CMSSW_BASE"],
    "src",
    "VertexCompositeAnalysis",
    "VertexCompositeProducer",
    "test",
    "validation/dstar_mass/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA_eventplaneMBOnly.py",
)

with open(_base_cfg, "r") as _handle:
    exec(compile(_handle.read(), _base_cfg, "exec"))

process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(10000))
process.options.numberOfThreads = cms.untracked.uint32(8)
process.TFileService.fileName = cms.string("d0_ws_count_10k.root")

process.d0anaWSCount = process.d0ana_newreduced.clone(
    doRecoNtuple=cms.untracked.bool(True),
    doGenNtuple=cms.untracked.bool(False),
    doGenMatching=cms.untracked.bool(False),
    saveHistogram=cms.untracked.bool(False),
    CompositeCollection=cms.untracked.InputTag("generalD0CandidatesNewWrongSign:D0"),
    MVACollection=cms.untracked.InputTag("generalD0CandidatesNewWrongSign:MVAValuesD0"),
    isEventPlane=cms.untracked.bool(False),
)

process.d0WSCount_step = cms.Path(
    process.eventFilter_HM
    * process.generalD0CandidatesNewWrongSign
    * process.d0anaWSCount
)

process.schedule = cms.Schedule(
    process.Flag_colEvtSel,
    process.Flag_primaryVertexFilter,
    process.c,
    process.eventFilter_HM_step,
    process.d0WSCount_step,
)
