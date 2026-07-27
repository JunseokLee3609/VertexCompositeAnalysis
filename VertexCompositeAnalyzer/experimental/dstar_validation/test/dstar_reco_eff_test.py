import FWCore.ParameterSet.Config as cms

process = cms.Process("DSTARRECOEFF")

process.load("FWCore.MessageLogger.MessageLogger_cfi")
process.load("Configuration.StandardSequences.MagneticField_cff")
process.load("Configuration.StandardSequences.GeometryRecoDB_cff")
process.load("Configuration.StandardSequences.FrontierConditions_GlobalTag_cff")
from Configuration.AlCa.GlobalTag import GlobalTag
process.GlobalTag = GlobalTag(process.GlobalTag, "132X_mcRun3_2023_realistic_HI_v9", "")
process.MessageLogger.cerr.FwkReport.reportEvery = cms.untracked.int32(10)
process.options = cms.untracked.PSet(wantSummary=cms.untracked.bool(True))

process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(-1))

process.source = cms.Source(
    "PoolSource",
    # fileNames=cms.untracked.vstring("file:/eos/home-j/junseok/analysis/D0study/CMSSW_13_2_10/src/step4.root"),
    # fileNames=cms.untracked.vstring("file:/eos/home-j/junseok/analysis/D0study/CMSSW_13_2_10/src/DStarKpipi_Prompt_pTHatMin10_py_GEN_SIM_PU.root"),
    # fileNames=cms.untracked.vstring("root://cluster142.knu.ac.kr//store/user/junseok/DStarMC/DStarKpipi_Prompt_pTHat4_dPt10/MINIAOD/step4_1.root","root://cluster142.knu.ac.kr//store/user/junseok/DStarMC/DStarKpipi_Prompt_pTHat4_dPt10/MINIAOD/step4_0.root"),
    fileNames=cms.untracked.vstring("root://xrootd-cms.infn.it//store/user/junseok/Genproduction/GEN_DStarKpipiPU_NonPrompt_forcedDstar_ptfilter4_pthatmin10_dpT20_y1p8_CMSSW_13_2_10_28Jan25_v1/DStarKpipi_Prompt_ForcedDstarDecay/crab_GEN_DStarKpipiPU_NonPrompt_forcedDstar_ptfilter4_pthatmin10_dpT20_y1p8_CMSSW_13_2_10_28Jan25_v1/260209_134231/0000/DStarKpipi_Prompt_pTHat10_dPt20_py_GEN_SIM_PU_910.root"),
)

process.load("VertexCompositeAnalysis.VertexCompositeProducer.unpackedTracksAndVertices_cfi")
process.load("VertexCompositeAnalysis.VertexCompositeProducer.generalD0Candidates_cff")
process.generalD0CandidatesNew = process.generalD0Candidates.clone()
process.generalD0CandidatesNew.trackRecoAlgorithm = cms.InputTag("unpackedTracksAndVertices")
process.generalD0CandidatesNew.vertexRecoAlgorithm = cms.InputTag("unpackedTracksAndVertices")
process.generalD0CandidatesNew.tkChi2Cut = cms.double(5)
process.generalD0CandidatesNew.tkNhitsCut = cms.int32(0)
process.generalD0CandidatesNew.tkPtErrCut = cms.double(0.1)
process.generalD0CandidatesNew.tkPtCut = cms.double(1.5)
process.generalD0CandidatesNew.tkEtaCut = cms.double(2.4)
process.generalD0CandidatesNew.tkPtSumCut = cms.double(0.0)
process.generalD0CandidatesNew.tkEtaDiffCut = cms.double(1.0)
process.generalD0CandidatesNew.dauTransImpactSigCut = cms.double(0.0)
process.generalD0CandidatesNew.dauLongImpactSigCut = cms.double(0.0)
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
process.generalD0CandidatesNew.d0AbsYCut = cms.double(1.8)
process.generalD0CandidatesNew.dPtCut = cms.double(0.0)
process.generalD0CandidatesNew.useAnyMVA = cms.bool(True)
process.generalD0CandidatesNew.mvaCut = cms.double(0.9)
process.generalD0CandidatesNew.input_names = cms.vstring("float_input")
process.generalD0CandidatesNew.output_names = cms.vstring("probabilities")
process.generalD0CandidatesNew.onnxModelFileName = cms.string("Xgboost_new_16Jan01.onnx")
process.generalD0CandidatesNew.mPiKCutMin = cms.double(1.70)
process.generalD0CandidatesNew.mPiKCutMax = cms.double(2.00)

process.load("VertexCompositeAnalysis.VertexCompositeProducer.generalDStarCandidates_cff")
process.generalDStarCandidatesNew = process.generalDStarCandidates.clone()
process.generalDStarCandidatesNew.d0Collection = cms.InputTag("generalD0CandidatesNew", "D0")
process.generalDStarCandidatesNew.trackRecoAlgorithm = cms.InputTag("unpackedTracksAndVertices")
process.generalDStarCandidatesNew.vertexRecoAlgorithm = cms.InputTag("unpackedTracksAndVertices")
process.generalDStarCandidatesNew.trkPtSumCut = cms.double(0.0)
process.generalDStarCandidatesNew.trkEtaDiffCut = cms.double(99.0)
process.generalDStarCandidatesNew.tkNhitsCut = cms.int32(0)
process.generalDStarCandidatesNew.tkPtErrCut = cms.double(0.1)
process.generalDStarCandidatesNew.tkPtCut = cms.double(0.3)
process.generalDStarCandidatesNew.tkChi2Cut = cms.double(5)
process.generalDStarCandidatesNew.VtxChiProbCut = cms.double(0.00)
process.generalDStarCandidatesNew.dauLongImpactSigCut = cms.double(0.0)
process.generalDStarCandidatesNew.dauTransImpactSigCut = cms.double(0.0)
process.generalDStarCandidatesNew.dPtCut = cms.double(4.5)

process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.dstar_reco_eff_cfi")
# process.dstarRecoEff.GenParticleCollection = cms.untracked.InputTag("genParticles")
process.dstarRecoEff.genOnly = cms.untracked.bool(True)
process.dstarRecoEff.GenParticleCollection = cms.untracked.InputTag("prunedGenParticles") if process.dstarRecoEff.genOnly == cms.untracked.bool(False) else cms.untracked.InputTag("genParticles")


process.TFileService = cms.Service(
    "TFileService",
    fileName=cms.string("dstar_reco_eff.root"),
)

useGenOnly = bool(process.dstarRecoEff.genOnly.value())
if useGenOnly:
    process.p = cms.Path(process.dstarRecoEff)
else:
    process.p = cms.Path(
        process.unpackedTracksAndVertices *
        process.generalD0CandidatesNew *
        process.generalDStarCandidatesNew *
        process.dstarRecoEff
    )
