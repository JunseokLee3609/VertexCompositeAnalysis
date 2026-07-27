import os

import FWCore.ParameterSet.Config as cms

_base_cfg = os.path.join(
    os.environ["CMSSW_BASE"],
    "src",
    "VertexCompositeAnalysis",
    "VertexCompositeProducer",
    "test",
    "legacy/PbPb2023_DStarWSCount_MB_cfg_v2_Step2MVA_10k.py",
)

with open(_base_cfg, "r") as _handle:
    exec(compile(_handle.read(), _base_cfg, "exec"))

process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(1000))
process.options.numberOfThreads = cms.untracked.uint32(8)
process.TFileService.fileName = cms.string("dstar_ws_debug_counters_1k.root")
process.generalDStarCandidatesNewWS.debugCounters = cms.bool(True)
