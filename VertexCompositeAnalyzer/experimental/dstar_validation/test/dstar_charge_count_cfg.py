import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing

options = VarParsing("analysis")
options.register(
    "genLabel",
    "prunedGenParticles",
    VarParsing.multiplicity.singleton,
    VarParsing.varType.string,
    "GEN collection label to count D* charges from",
)
options.register(
    "status",
    -1,
    VarParsing.multiplicity.singleton,
    VarParsing.varType.int,
    "Status filter for GEN particle (-1: disable)",
)
options.register(
    "printPerEvent",
    True,
    VarParsing.multiplicity.singleton,
    VarParsing.varType.bool,
    "Print per-event D*+ / D*- counts",
)
options.register(
    "maxPrint",
    50,
    VarParsing.multiplicity.singleton,
    VarParsing.varType.int,
    "Maximum number of per-event log lines (-1: no limit)",
)
options.register(
    "fileList",
    "",
    VarParsing.multiplicity.singleton,
    VarParsing.varType.string,
    "Optional text file with input file paths (one per line)",
)
options.parseArguments()

process = cms.Process("DSTARCOUNT")
process.load("FWCore.MessageService.MessageLogger_cfi")
process.MessageLogger.cerr.threshold = "INFO"
process.MessageLogger.cerr.FwkReport.reportEvery = 100
process.MessageLogger.cerr.DStarChargeCounter = cms.untracked.PSet(limit=cms.untracked.int32(-1))

input_files = list(options.inputFiles)
if len(input_files) == 0 and options.fileList:
    with open(options.fileList, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            token = line.split()[0]
            if token.startswith("root://") or token.startswith("file:"):
                input_files.append(token)
            elif token.startswith("/store/"):
                input_files.append("root://cms-xrd-global.cern.ch/" + token)
            else:
                input_files.append(token)

if len(input_files) == 0:
    process.source = cms.Source("EmptySource")
    process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(0))
else:
    process.source = cms.Source("PoolSource", fileNames=cms.untracked.vstring(*input_files))
    process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(options.maxEvents))

process.dstarChargeCounter = cms.EDAnalyzer(
    "DStarChargeCounterAnalyzer",
    genCollection=cms.untracked.InputTag(options.genLabel),
    absPdgId=cms.untracked.int32(413),
    status=cms.untracked.int32(options.status),
    printPerEvent=cms.untracked.bool(options.printPerEvent),
    maxPrint=cms.untracked.int32(options.maxPrint),
)

process.options = cms.untracked.PSet(wantSummary=cms.untracked.bool(True))
process.p = cms.Path(process.dstarChargeCounter)
