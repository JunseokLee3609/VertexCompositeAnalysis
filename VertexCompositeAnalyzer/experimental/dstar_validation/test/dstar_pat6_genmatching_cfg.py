import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing

options = VarParsing("analysis")
options.register(
    "debug",
    True,
    VarParsing.multiplicity.singleton,
    VarParsing.varType.bool,
    "Enable per-candidate gen matching logs",
)
options.register(
    "verbose",
    False,
    VarParsing.multiplicity.singleton,
    VarParsing.varType.bool,
    "Also print pid-mismatch rejects",
)
options.register(
    "genLabel",
    "genParticles",
    VarParsing.multiplicity.singleton,
    VarParsing.varType.string,
    "Gen particle collection label (e.g. genParticles, prunedGenParticles)",
)
options.parseArguments()

process = cms.Process("PAT6GEN")

process.load("FWCore.MessageService.MessageLogger_cfi")
process.MessageLogger.cerr.threshold = "INFO"
process.MessageLogger.cerr.PAT6GenMatching = cms.untracked.PSet(limit=cms.untracked.int32(-1))

# If no input file is provided, run 0 events just for plugin/config load check.
if len(options.inputFiles) == 0:
    process.source = cms.Source("EmptySource")
    process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(0))
else:
    process.source = cms.Source(
        "PoolSource",
        fileNames=cms.untracked.vstring(*options.inputFiles),
    )
    process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(options.maxEvents))

from VertexCompositeAnalysis.VertexCompositeAnalyzer.dStaranalyzer_tree_pat6_cfi import dStarana_pat6_gen

process.pat6 = dStarana_pat6_gen.clone(
    debugGenMatching=cms.untracked.bool(options.debug),
    verboseDebug=cms.untracked.bool(options.verbose),
    GenParticleCollection=cms.untracked.InputTag(options.genLabel),
)

process.options = cms.untracked.PSet(wantSummary=cms.untracked.bool(True))
process.p = cms.Path(process.pat6)
