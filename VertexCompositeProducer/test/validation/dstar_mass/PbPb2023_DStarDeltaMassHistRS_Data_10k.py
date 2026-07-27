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
process.options.numberOfStreams = cms.untracked.uint32(8)
process.MessageLogger.cerr.FwkReport.reportEvery = 1000
process.TFileService.fileName = cms.string(
    "VertexCompositeAnalysis/VertexCompositeProducer/test/"
    "hist_cut_validation_data_20260712/"
    "dstar_delta_mass_hist_rs_data_10k.root"
)
