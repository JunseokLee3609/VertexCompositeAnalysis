import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing
from Configuration.StandardSequences.Eras import eras

options = VarParsing("analysis")
options.setDefault("outputFile", "gen_dstar_helicity_miniAOD.root")
options.setDefault(
    "inputFiles",
    [
        "root://xrootd-cms.infn.it//store/user/junseok/DStarAnalysis/RECO_MINIAOD_DStarKpipiPU_Prompt_forcedDStar_T2Vandbilt_CMSSW_13_2_10_10Dec25_v1/DStarKpipi_Prompt_ForcedD0Decay/crab_RECO_MINIAOD_DStarKpipiPU_Prompt_forcedDStar_T2Vandbilt_CMSSW_13_2_10_10Dec25_v1/251210_062957/0001/step4_1230.root",
    ],
)
options.setDefault("maxEvents", 1000)
options.parseArguments()

process = cms.Process("GENDSTARHX", eras.Run3_2023)

process.load("FWCore.MessageService.MessageLogger_cfi")
process.MessageLogger.cerr.FwkReport.reportEvery = 100
process.options = cms.untracked.PSet(wantSummary=cms.untracked.bool(True))

process.source = cms.Source(
    "PoolSource",
    fileNames=cms.untracked.vstring(*options.inputFiles),
)

process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(options.maxEvents))

process.TFileService = cms.Service(
    "TFileService",
    fileName=cms.string(options.outputFile),
)

process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.gendstareventplaneanalyzer_miniAOD_cfi")
process.genDstarEventPlaneMiniAOD.saveTree = cms.untracked.bool(True)
process.genDstarEventPlaneMiniAOD.saveHistogram = cms.untracked.bool(False)
process.genDstarEventPlaneMiniAOD.isCentrality = cms.bool(False)

process.analysis = cms.Path(process.genDstarEventPlaneMiniAOD)
process.schedule = cms.Schedule(process.analysis)
