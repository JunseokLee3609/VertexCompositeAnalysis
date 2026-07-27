import os

import FWCore.ParameterSet.Config as cms

_cfg = os.path.join(
    os.environ["CMSSW_BASE"],
    "src",
    "VertexCompositeAnalysis",
    "VertexCompositeProducer",
    "test",
    "validation/dstar_mass/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA_eventplaneMBOnly_D0SS.py",
)

with open(_cfg, "r") as _handle:
    exec(compile(_handle.read(), _cfg, "exec"))

process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(10000))
process.options.numberOfThreads = cms.untracked.uint32(8)
process.TFileService.fileName = cms.string(
    "VertexCompositeAnalysis/VertexCompositeProducer/test/dstar_delta_mass_hist_step2_fiducial_mvacutm1_d0ss_rawkin_10kcheck.root"
)
