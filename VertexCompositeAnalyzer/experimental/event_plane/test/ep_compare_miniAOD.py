import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing

options = VarParsing("analysis")
# Built-in VarParsing("analysis") options:
#   inputFiles, maxEvents, outputFile
options.maxEvents = 1000
options.outputFile = "ep_compare.root"
options.register("storedTag", "hiEvtPlaneFlat", VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Stored EvtPlane collection label")
options.register("runCentralityBin", True, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                 "Run centralityBin producer from hiCentrality")
options.register("vertexTag", "offlineSlimmedPrimaryVertices", VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Vertex collection label for miniAOD")
options.register("centralityTag", "hiCentrality", VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Centrality collection label")
options.register("debugLog", False, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                 "Enable EvtPlaneComparator debug logs")
options.register("maxDebugPrints", 200, VarParsing.multiplicity.singleton, VarParsing.varType.int,
                 "Maximum number of debug lines")
options.register("usePpOnAAStyleTrackerCuts", True, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                 "Use pp_on_AA EP settings (cutEra=0, minpt=0.5, minet=0.01, dzdzerror_pix=40, caloCentRef=-1)")
options.register("globalTag", "132X_dataRun3_Prompt_v7", VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "GlobalTag to use for EP recomputation")
options.register("matchPSetDumpHF", False, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                 "Match HF-related hiEvtPlane settings from psetdump (80/5). Ignored when usePpOnAAStyleTrackerCuts=True.")
options.register("hfCaloTag", "particleFlow", VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Calo input for EP producer. towerMaker is not available in miniAOD.")
options.parseArguments()

process = cms.Process("EPCOMP")

process.load("Configuration.StandardSequences.Services_cff")
process.load("FWCore.MessageService.MessageLogger_cfi")
process.load("Configuration.StandardSequences.FrontierConditions_GlobalTag_cff")

from Configuration.AlCa.GlobalTag import GlobalTag
process.GlobalTag = GlobalTag(process.GlobalTag, options.globalTag, "")

process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(options.maxEvents))
process.source = cms.Source(
    "PoolSource",
    fileNames=cms.untracked.vstring(options.inputFiles),
)

process.TFileService = cms.Service("TFileService", fileName=cms.string(options.outputFile))

# Recompute event-plane on miniAOD-compatible inputs.
process.load("RecoHI.HiEvtPlaneAlgos.HiEvtPlane_cfi")
process.hiEvtPlaneRecalc = process.hiEvtPlane.clone(
    centralityVariable=cms.string("HFtowers"),
    centralityBinTag=cms.InputTag("centralityBin", "HFtowers"),
    vertexTag=cms.InputTag(options.vertexTag),
    trackTag=cms.InputTag("packedPFCandidates"),
    lostTag=cms.InputTag("lostTracks"),
    chi2MapTag=cms.InputTag("packedPFCandidateTrackChi2"),
    chi2MapLostTag=cms.InputTag("lostTrackChi2"),
    caloTag=cms.InputTag(options.hfCaloTag),
    loadDB=cms.bool(False),
)

if options.usePpOnAAStyleTrackerCuts:
    # Match the pp_on_AA modifier values from RecoHI/HiEvtPlaneAlgos/python/HiEvtPlane_cfi.py
    process.hiEvtPlaneRecalc.trackTag = cms.InputTag("packedPFCandidates")
    process.hiEvtPlaneRecalc.caloTag = cms.InputTag("particleFlow")
    process.hiEvtPlaneRecalc.minet = cms.double(0.01)
    process.hiEvtPlaneRecalc.minpt = cms.double(0.5)
    process.hiEvtPlaneRecalc.dzdzerror_pix = cms.double(40.0)
    process.hiEvtPlaneRecalc.caloCentRef = cms.double(-1.0)
    process.hiEvtPlaneRecalc.caloCentRefWidth = cms.double(-1.0)
    process.hiEvtPlaneRecalc.cutEra = cms.int32(0)
elif options.matchPSetDumpHF:
    process.hiEvtPlaneRecalc.caloCentRef = cms.double(80.0)
    process.hiEvtPlaneRecalc.caloCentRefWidth = cms.double(5.0)

process.load("RecoHI.HiEvtPlaneAlgos.hiEvtPlaneFlat_cfi")
process.hiEvtPlaneFlatRecalc = process.hiEvtPlaneFlat.clone(
    centralityVariable=cms.string("HFtowers"),
    centralityBinTag=cms.InputTag("centralityBin", "HFtowers"),
    centralityTag=cms.InputTag(options.centralityTag),
    vertexTag=cms.InputTag(options.vertexTag),
    inputPlanesTag=cms.InputTag("hiEvtPlaneRecalc"),
    trackTag=cms.InputTag("packedPFCandidates"),
)

process.load("RecoHI.HiCentralityAlgos.CentralityBin_cfi")
process.centralityBin.Centrality = cms.InputTag(options.centralityTag)
process.centralityBin.centralityVariable = cms.string("HFtowers")
process.centralityBin.nonDefaultGlauberModel = cms.string("")

process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.eventplane_compare_cfi")
process.evtPlaneCompare.stored = cms.InputTag(options.storedTag)
process.evtPlaneCompare.recomputed = cms.InputTag("hiEvtPlaneFlatRecalc")
process.evtPlaneCompare.debugLog = cms.untracked.bool(options.debugLog)
process.evtPlaneCompare.maxDebugPrints = cms.untracked.int32(options.maxDebugPrints)

if options.runCentralityBin:
    process.epSeq = cms.Sequence(process.centralityBin * process.hiEvtPlaneRecalc * process.hiEvtPlaneFlatRecalc * process.evtPlaneCompare)
else:
    process.epSeq = cms.Sequence(process.hiEvtPlaneRecalc * process.hiEvtPlaneFlatRecalc * process.evtPlaneCompare)

process.p = cms.Path(process.epSeq)

process.MessageLogger.cerr.FwkReport.reportEvery = 100
