import os
import runpy

import FWCore.ParameterSet.Config as cms


test_dir = os.path.join(
    os.environ["CMSSW_BASE"],
    "src",
    "VertexCompositeAnalysis",
    "VertexCompositeProducer",
    "test",
)
base_cfg = os.path.join(
    test_dir,
    "production",
    "pbpb2023",
    "mc",
    "PbPb2023_D0BothAndDStar_MB_cfg_mc_v2_Step2MVA.py",
)
process = runpy.run_path(base_cfg)["process"]

process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(500))
process.MessageLogger.cerr.FwkReport.reportEvery = 50
process.TFileService.fileName = cms.string("dxy_error_definition_ab_test_mc.root")
