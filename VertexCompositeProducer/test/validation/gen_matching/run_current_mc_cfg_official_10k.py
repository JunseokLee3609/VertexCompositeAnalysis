import os
import runpy

import FWCore.ParameterSet.Config as cms


_test_dir = os.path.join(
    os.environ["CMSSW_BASE"],
    "src",
    "VertexCompositeAnalysis",
    "VertexCompositeProducer",
    "test",
)
process = runpy.run_path(
    os.path.join(
        _test_dir,
        "production",
        "pbpb2023",
        "mc",
        "PbPb2023_D0BothAndDStar_MB_cfg_mc_v2_Step2MVA.py",
    )
)["process"]

process.maxEvents.input = 10000
process.source.fileNames = cms.untracked.vstring(
    "root://cms-xrd-global.cern.ch//store/mc/HINPbPbSpring23MiniAOD/"
    "promptDStarToD0PiToKPiPi_pT-0_TuneCP5_5p36TeV_pythia8-evtgen/"
    "MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/120000/"
    "97e2747f-da54-4572-b1fc-186915252c24.root"
)
process.TFileService.fileName = cms.string("current_mc_cfg_official_10k.root")

process.d0ana_newreduced.saveHistogram = cms.untracked.bool(False)
process.dStarana_mc.saveHistogram = cms.untracked.bool(False)

process.generalDStarCandidatesNew.isWrongSign = cms.bool(False)
process.generalDStarCandidatesNew.useRawDStarKinematics = cms.bool(False)
process.generalDStarCandidatesNew.rejectDuplicateSlowPion = cms.bool(False)
process.generalDStarCandidatesNew.debugCategoryCutflow = cms.bool(False)
process.generalDStarCandidatesNew.debugSlowPionPtScan = cms.bool(False)

process.MessageLogger.cerr.FwkReport.reportEvery = 100
