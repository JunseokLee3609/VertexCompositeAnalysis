import FWCore.ParameterSet.Config as cms
from Configuration.StandardSequences.Eras import eras
process = cms.Process('ANASKIM', eras.Run3_2023)

process.load('Configuration.StandardSequences.Services_cff')
process.load('Configuration.StandardSequences.GeometryRecoDB_cff')
process.load('Configuration.StandardSequences.MagneticField_cff')
process.load('Configuration.StandardSequences.Reconstruction_Data_cff')

# Limit the output messages
process.load('FWCore.MessageService.MessageLogger_cfi')
process.MessageLogger.cerr.FwkReport.reportEvery = 10
process.MessageLogger.cerr.threshold = "INFO"
process.options = cms.untracked.PSet(wantSummary = cms.untracked.bool(True))

# Define the input source
process.source = cms.Source("PoolSource",
    fileNames = cms.untracked.vstring("file:/eos/cms/store/group/phys_heavyions/dileptons/Data2023/MINIAOD/HIPhysicsRawPrime0/Run375064/7ed5766f-6b1d-415e-8916-e62825a6347f.root"),
    #fileNames = cms.untracked.vstring("file:step4.root"),
    #fileNames = cms.untracked.vstring("/store/user/junseok/Genproduction/RECO_MINIAOD_DStarKpipiPU_CMSSW_13_2_10_081924_v1/DStarKpipiPU/crab_RECO_MINIAOD_DStarKpipiPU_CMSSW_13_2_10_081924_v1/240819_054039/0001/step4_1619.root"),
)
process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(10000))

# Set the global tag
process.load('Configuration.StandardSequences.FrontierConditions_GlobalTag_cff')
process.GlobalTag.globaltag = cms.string('132X_dataRun3_Prompt_v4')
#process.GlobalTag.globaltag = cms.string('132X_mcRun3_2023_realistic_HI_v9')

from VertexCompositeAnalysis.VertexCompositeProducer.PATAlgos_cff import changeToMiniAOD

# changeToMiniAOD expects these to exist; keep them as no-ops for lite config.
process.eventFilter_HM = cms.Sequence()
process.eventFilter_HM_step = cms.Path(process.eventFilter_HM)

# Define the analysis steps

########## D0 candidate rereco ###############################################################
process.load("VertexCompositeAnalysis.VertexCompositeProducer.generalD0Candidates_cff")
process.generalD0CandidatesNew = process.generalD0Candidates.clone()
#process.generalD0CandidatesNew.trkPtSumCut = cms.double(1.6)
process.generalD0CandidatesNew.tkChi2Cut = cms.double(5)
process.generalD0CandidatesNew.tkNhitsCut = cms.int32(0)
process.generalD0CandidatesNew.tkPtErrCut = cms.double(0.1)
# process.generalD0CandidatesNew.tkPtCut = cms.double(0.8)
process.generalD0CandidatesNew.tkPtCut = cms.double(1.5)
process.generalD0CandidatesNew.tkEtaCut = cms.double(2.4)
#process.generalD0CandidatesNew.tkEtaCut = cms.double(1.8)
process.generalD0CandidatesNew.tkPtSumCut = cms.double(0.0)
process.generalD0CandidatesNew.tkEtaDiffCut = cms.double(1.0)
process.generalD0CandidatesNew.dauTransImpactSigCut = cms.double(0.)
process.generalD0CandidatesNew.dauLongImpactSigCut = cms.double(0.)
process.generalD0CandidatesNew.tkDCACut = cms.double(0.05)
process.generalD0CandidatesNew.vtxChi2Cut = cms.double(9999.0)
process.generalD0CandidatesNew.VtxChiProbCut = cms.double(0.00)
process.generalD0CandidatesNew.collinearityCut2D = cms.double(-2.0)
process.generalD0CandidatesNew.collinearityCut3D = cms.double(-2.0)
process.generalD0CandidatesNew.alphaCut = cms.double(1.0)
process.generalD0CandidatesNew.alpha2DCut = cms.double(999.0)
process.generalD0CandidatesNew.rVtxCut = cms.double(0.0)
process.generalD0CandidatesNew.lVtxCut = cms.double(0.0)
process.generalD0CandidatesNew.vtxSignificance2DCut = cms.double(0.0)
process.generalD0CandidatesNew.vtxSignificance3DCut = cms.double(3.0)
process.generalD0CandidatesNew.d0MassCut = cms.double(0.14)
process.generalD0CandidatesNew.d0AbsYCut = cms.double(1.6)
process.generalD0CandidatesNew.dPtCut = cms.double(0.0)

process.generalD0CandidatesNew.useAnyMVA = cms.bool(True)
process.generalD0CandidatesNew.mvaCut = cms.double(-1)
# process.generalD0CandidatesNew.GBRForestLabel = cms.string('D0InPbPbXGB')
# #process.generalD0CandidatesNew.GBRForestFileName = cms.string('GBRForestfile_XGBDT_PromptD0InPbPb_15Params_v1_08Mar.root')
# process.generalD0CandidatesNew.GBRForestFileName = cms.string('GBRForestfile_XGBDT_PromptD0InPbPb_pT_y_cBIN_19Params_v1_25Mar.root')
process.generalD0CandidatesNew.input_names = cms.vstring('input')
process.generalD0CandidatesNew.output_names = cms.vstring('probabilities')
# process.generalD0CandidatesNew.onnxModelFileName = cms.string("Xgboost_woCent_y_01Dec25.onnx")
process.generalD0CandidatesNew.onnxModelFileName = cms.string("XGBoost_Model_0428_0_OnlyPrompt.onnx")

process.generalD0CandidatesNew.mPiKCutMin = cms.double(1.70)
process.generalD0CandidatesNew.mPiKCutMax = cms.double(2.00)
#process.generalD0CandidatesNewWrongSign = process.generalD0CandidatesNew.clone(isWrongSign = cms.bool(True))

process.load("VertexCompositeAnalysis.VertexCompositeProducer.generalDStarCandidates_cff")
process.generalDStarCandidatesNew = process.generalDStarCandidates.clone()
process.generalDStarCandidatesNew.trkPtSumCut = cms.double(0.0)
process.generalDStarCandidatesNew.trkEtaDiffCut = cms.double(99.0)
process.generalDStarCandidatesNew.tkNhitsCut = cms.int32(0)
process.generalDStarCandidatesNew.tkPtErrCut = cms.double(0.1)
process.generalDStarCandidatesNew.tkPtCut = cms.double(0.3)
process.generalDStarCandidatesNew.tkChi2Cut = cms.double(5)
process.generalDStarCandidatesNew.VtxChiProbCut = cms.double(0.00)
#process.generalDStarCandidatesNew.vtxSignificance3DCut = cms.double(3)
#process.generalDStarCandidatesNew.alphaCut = cms.double(1)
#process.generalDStarCandidatesNew.alpha2DCut = cms.double(1)
process.generalDStarCandidatesNew.dauLongImpactSigCut = cms.double(0.0)
process.generalDStarCandidatesNew.dauTransImpactSigCut = cms.double(0.0)# it will be cut of by 3 in selector
process.generalDStarCandidatesNew.dPtCut = cms.double(0.0)
# process.generalDStarCandidatesNew.useAnyMVA=cms.bool(True)
# process.generalDStarCandidatesNew.GBRForestFileName=cms.string('GBRForestfile_XGBDT_PromptDstarInPbPb_default_MB_OnlyMC.root')


#process.d0rereco_wrongsign_step = cms.Path( process.eventFilter_HM * process.generalD0CandidatesNewWrongSign )



# produce D0 trees
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.dStaranalyzer_tree_onnxlite_cfi")

process.TFileService = cms.Service("TFileService",
    fileName =
    cms.string('d0ana_tree_step2_onnxlite.root')
    )

process.generalDStarCandidatesNew.d0Collection = cms.InputTag("generalD0CandidatesNew:D0")

process.dStarana_onnxlite.CompositeCollection = cms.untracked.InputTag("generalDStarCandidatesNew:DStar")
process.dStarana_onnxlite.VertexCollection = cms.untracked.InputTag("offlinePrimaryVertices")
process.dStarana_onnxlite.onnxModelFileName = process.generalD0CandidatesNew.onnxModelFileName
process.dStarana_onnxlite.input_names = process.generalD0CandidatesNew.input_names
process.dStarana_onnxlite.output_names = process.generalD0CandidatesNew.output_names
process.dStarana_onnxlite.vtxChi2Cut = cms.untracked.double(process.generalD0CandidatesNew.vtxChi2Cut.value())
process.dStarana_onnxlite.VtxChiProbCut = cms.untracked.double(process.generalD0CandidatesNew.VtxChiProbCut.value())
process.dStarana_onnxlite.collinearityCut2D = cms.untracked.double(process.generalD0CandidatesNew.collinearityCut2D.value())
process.dStarana_onnxlite.collinearityCut3D = cms.untracked.double(process.generalD0CandidatesNew.collinearityCut3D.value())
process.dStarana_onnxlite.alphaCut = cms.untracked.double(process.generalD0CandidatesNew.alphaCut.value())
process.dStarana_onnxlite.alpha2DCut = cms.untracked.double(process.generalD0CandidatesNew.alpha2DCut.value())
process.dStarana_onnxlite.rVtxCut = cms.untracked.double(process.generalD0CandidatesNew.rVtxCut.value())
process.dStarana_onnxlite.lVtxCut = cms.untracked.double(process.generalD0CandidatesNew.lVtxCut.value())
process.dStarana_onnxlite.vtxSignificance2DCut = cms.untracked.double(process.generalD0CandidatesNew.vtxSignificance2DCut.value())
process.dStarana_onnxlite.vtxSignificance3DCut = cms.untracked.double(process.generalD0CandidatesNew.vtxSignificance3DCut.value())
process.dStarana_onnxlite.applyCuts = cms.untracked.bool(True)
process.dStarana_onnxlite.printDiffThreshold = cms.untracked.double(0.001)
process.dStarana_onnxlite.saveHist = cms.untracked.bool(True)
process.dStarana_onnxlite.printAllCandidates = cms.untracked.bool(False)
process.dStarana_onnxlite.compareRecalc = cms.untracked.bool(False)
process.dStarana_onnxlite.compareThreshold = cms.untracked.double(1.0e-6)

process.dStarAna_step = cms.Path(
    process.generalD0CandidatesNew *
    process.generalDStarCandidatesNew *
    process.dStarana_onnxlite
)

process.schedule = cms.Schedule(
    process.eventFilter_HM_step,
    process.dStarAna_step
)

changeToMiniAOD(process)
process.options.numberOfThreads = 1

#process.output = cms.OutputModule("PoolOutputModule",
#    outputCommands = cms.untracked.vstring("keep *_*_*_ANASKIM"),
#    fileName = cms.untracked.string('output.root'),
#)
#
#process.outputPath = cms.EndPath(process.output)
#process.schedule.append(process.outputPath)
