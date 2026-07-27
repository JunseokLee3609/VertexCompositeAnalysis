import os

import FWCore.ParameterSet.Config as cms

_cfg = os.path.join(
    os.environ["CMSSW_BASE"],
    "src",
    "VertexCompositeAnalysis",
    "VertexCompositeProducer",
    "test",
    "production/pbpb2023/mc/PbPb2023_D0BothAndDStar_MB_cfg_mc_v2_Step2MVA.py",
)

with open(_cfg, "r") as _handle:
    exec(compile(_handle.read(), _cfg, "exec"))

process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(2000))
process.options.numberOfThreads = cms.untracked.uint32(8)
process.TFileService.fileName = cms.string(
    "VertexCompositeAnalysis/VertexCompositeProducer/test/dstar_genmatch_tail_counter_2000evt.root"
)

process.MessageLogger.cerr.PAT6GenMatchTail = cms.untracked.PSet(
    limit=cms.untracked.int32(-1)
)
