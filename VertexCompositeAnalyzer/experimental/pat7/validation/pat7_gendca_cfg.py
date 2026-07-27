import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing


options = VarParsing("analysis")
options.outputFile = "pat7_gendca.root"
options.parseArguments()

process = cms.Process("PAT7TEST")

process.load("Configuration.StandardSequences.Services_cff")
process.load("Configuration.StandardSequences.GeometryRecoDB_cff")
process.load("Configuration.StandardSequences.MagneticField_cff")
process.load("Configuration.StandardSequences.FrontierConditions_GlobalTag_cff")
from Configuration.AlCa.GlobalTag import GlobalTag

process.GlobalTag = GlobalTag(process.GlobalTag, "132X_mcRun3_2023_realistic_HI_v9", "")

process.source = cms.Source(
    "PoolSource",
    fileNames=cms.untracked.vstring(options.inputFiles),
)
process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(options.maxEvents))
process.options = cms.untracked.PSet(
    numberOfThreads=cms.untracked.uint32(1),
    numberOfStreams=cms.untracked.uint32(0),
    wantSummary=cms.untracked.bool(True),
)

process.load("FWCore.MessageService.MessageLogger_cfi")
process.MessageLogger.cerr.FwkReport.reportEvery = 1

process.load("VertexCompositeAnalysis.VertexCompositeProducer.unpackedTracksAndVertices_cfi")
process.load("VertexCompositeAnalysis.VertexCompositeProducer.generalD0Candidates_cff")
process.load("VertexCompositeAnalysis.VertexCompositeProducer.generalDStarCandidates_cff")

# Keep the candidate cuts used by the PAT6 production configuration.  Only the
# ONNX inference is disabled because the validation targets the DCA branch.
process.generalD0CandidatesNew = process.generalD0Candidates.clone(
    trackRecoAlgorithm=cms.InputTag("unpackedTracksAndVertices"),
    vertexRecoAlgorithm=cms.InputTag("unpackedTracksAndVertices"),
    tkChi2Cut=cms.double(5),
    tkNhitsCut=cms.int32(0),
    tkPtErrCut=cms.double(0.1),
    tkPtCut=cms.double(1.5),
    tkEtaCut=cms.double(2.4),
    tkPtSumCut=cms.double(0.0),
    tkEtaDiffCut=cms.double(1.0),
    dauTransImpactSigCut=cms.double(0.0),
    dauLongImpactSigCut=cms.double(0.0),
    tkDCACut=cms.double(0.05),
    vtxChi2Cut=cms.double(9999.0),
    VtxChiProbCut=cms.double(0.0),
    collinearityCut2D=cms.double(-2.0),
    collinearityCut3D=cms.double(-2.0),
    alphaCut=cms.double(1.0),
    alpha2DCut=cms.double(999.0),
    rVtxCut=cms.double(0.0),
    lVtxCut=cms.double(0.0),
    vtxSignificance2DCut=cms.double(0.0),
    vtxSignificance3DCut=cms.double(3.0),
    d0MassCut=cms.double(0.14),
    d0AbsYCut=cms.double(1.8),
    dPtCut=cms.double(0.0),
    mPiKCutMin=cms.double(1.70),
    mPiKCutMax=cms.double(2.00),
    useAnyMVA=cms.bool(False),
)

process.generalDStarCandidatesNew = process.generalDStarCandidates.clone(
    d0Collection=cms.InputTag("generalD0CandidatesNew", "D0"),
    trackRecoAlgorithm=cms.InputTag("unpackedTracksAndVertices"),
    vertexRecoAlgorithm=cms.InputTag("unpackedTracksAndVertices"),
    trkPtSumCut=cms.double(0.0),
    trkEtaDiffCut=cms.double(99.0),
    tkNhitsCut=cms.int32(0),
    tkPtErrCut=cms.double(0.1),
    tkPtCut=cms.double(0.3),
    tkChi2Cut=cms.double(5),
    VtxChiProbCut=cms.double(0.0),
    dauLongImpactSigCut=cms.double(0.0),
    dauTransImpactSigCut=cms.double(0.0),
    dPtCut=cms.double(4.5),
    isWrongSign=cms.bool(False),
    useRawDStarKinematics=cms.bool(False),
    rejectDuplicateSlowPion=cms.bool(False),
    debugCategoryCutflow=cms.bool(False),
    debugSlowPionPtScan=cms.bool(False),
)

process.pat7 = cms.EDAnalyzer(
    "PATCompositeTreeProducer7",
    doRecoNtuple=cms.untracked.bool(True),
    doGenNtuple=cms.untracked.bool(False),
    doGenMatching=cms.untracked.bool(True),
    doGenMatchingTOF=cms.untracked.bool(False),
    hasSwap=cms.untracked.bool(False),
    decayInGen=cms.untracked.bool(True),
    twoLayerDecay=cms.untracked.bool(True),
    threeProngDecay=cms.untracked.bool(False),
    PID=cms.untracked.int32(413),
    PID_dau1=cms.untracked.int32(421),
    PID_dau2=cms.untracked.int32(211),
    deltaR=cms.untracked.double(0.03),
    VertexCollection=cms.untracked.InputTag("unpackedTracksAndVertices"),
    TrackCollection=cms.untracked.InputTag("unpackedTracksAndVertices"),
    CompositeCollection=cms.untracked.InputTag("generalDStarCandidatesNew", "DStar"),
    GenParticleCollection=cms.untracked.InputTag("prunedGenParticles"),
    GenPrimaryVertexCollection=cms.untracked.InputTag("genParticles", "xyz0"),
    MuonCollection=cms.untracked.InputTag("null"),
    doMuon=cms.untracked.bool(False),
    doMuonFull=cms.untracked.bool(False),
    saveTree=cms.untracked.bool(True),
    saveHistogram=cms.untracked.bool(False),
    saveAllHistogram=cms.untracked.bool(False),
    massHistPeak=cms.untracked.double(2.01),
    massHistWidth=cms.untracked.double(0.2),
    massHistBins=cms.untracked.int32(100),
    pTBins=cms.untracked.vdouble(0, 1.2, 1.5, 2.4, 3.0, 3.5, 4.2, 5.0, 6.0, 7.0, 8.0),
    yBins=cms.untracked.vdouble(-2.4, -1.0, 0.0, 1.0, 2.4),
    useAnyMVA=cms.bool(False),
    doRunInfo=cms.bool(True),
    isEventPlane=cms.bool(False),
    eventplaneSrc=cms.InputTag(""),
    eventplaneSrcRecalc=cms.InputTag(""),
    isSkimMVA=cms.untracked.bool(False),
    MVACollection=cms.InputTag("generalDStarCandidatesNew", "MVAValuesNewDStar"),
    isCentrality=cms.bool(False),
    centralityBinLabel=cms.InputTag("centralityBin", "HFtowers"),
    centralitySrc=cms.InputTag("hiCentrality"),
)

process.TFileService = cms.Service("TFileService", fileName=cms.string(options.outputFile))

process.analysis = cms.Path(
    process.unpackedTracksAndVertices
    * process.generalD0CandidatesNew
    * process.generalDStarCandidatesNew
    * process.pat7
)
