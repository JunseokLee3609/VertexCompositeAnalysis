import os

import FWCore.ParameterSet.Config as cms

_cfg = os.path.join(
    os.environ["CMSSW_BASE"],
    "src",
    "VertexCompositeAnalysis",
    "VertexCompositeProducer",
    "test",
    "validation/gen_matching/PbPb2023_DStarGenMatchTailCounter_20k.py",
)

with open(_cfg, "r") as _handle:
    exec(compile(_handle.read(), _cfg, "exec"))

process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(50000))
process.options.numberOfThreads = cms.untracked.uint32(os.cpu_count() or 1)
process.options.numberOfStreams = cms.untracked.uint32(os.cpu_count() or 1)
process.TFileService.fileName = cms.string(
    "VertexCompositeAnalysis/VertexCompositeProducer/test/dstar_genmatch_tail_rawtrack_50k.root"
)
process.dStarana_mc.debugDstarTailMaxPrint = cms.untracked.int32(100)
