import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing
options=VarParsing('analysis')
options.parseArguments()
process=cms.Process('GENDCA')
process.load('FWCore.MessageService.MessageLogger_cfi')
process.MessageLogger.cerr.FwkReport.reportEvery=2000
process.options=cms.untracked.PSet(numberOfThreads=cms.untracked.uint32(1),numberOfStreams=cms.untracked.uint32(1),wantSummary=cms.untracked.bool(True))
process.maxEvents=cms.untracked.PSet(input=cms.untracked.int32(options.maxEvents))
process.source=cms.Source('PoolSource',fileNames=cms.untracked.vstring(options.inputFiles),duplicateCheckMode=cms.untracked.string('noDuplicateCheck'))
process.TFileService=cms.Service('TFileService',fileName=cms.string('gen_dca_friend.root'),closeFileFast=cms.untracked.bool(True))
process.friend=cms.EDAnalyzer('GenDcaFriend',particles=cms.InputTag('prunedGenParticles'),origin=cms.InputTag('genParticles','xyz0'),inputLFN=cms.string(options.inputFiles[0][options.inputFiles[0].index('/store/'):] if len(options.inputFiles)==1 and '/store/' in options.inputFiles[0] else ''),report=cms.string('summary.json'))
process.path=cms.Path(process.friend)
