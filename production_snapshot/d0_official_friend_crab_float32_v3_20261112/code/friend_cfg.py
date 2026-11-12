import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing
options=VarParsing('analysis')
options.register('manifest','mapping.json',VarParsing.multiplicity.singleton,VarParsing.varType.string,'Original ntuple mapping')
options.register('allowPartial',False,VarParsing.multiplicity.singleton,VarParsing.varType.bool,'Pilot only')
options.register('signalOnlyGenMatching',True,VarParsing.multiplicity.singleton,VarParsing.varType.bool,'Store only Pythia GEN entries')
options.parseArguments()
process=cms.Process('D0REMATCH')
process.load('FWCore.MessageService.MessageLogger_cfi')
process.MessageLogger.cerr.FwkReport.reportEvery=1000
process.options=cms.untracked.PSet(numberOfThreads=cms.untracked.uint32(1),numberOfStreams=cms.untracked.uint32(1),wantSummary=cms.untracked.bool(True))
process.maxEvents=cms.untracked.PSet(input=cms.untracked.int32(options.maxEvents))
process.source=cms.Source('PoolSource',fileNames=cms.untracked.vstring(options.inputFiles),
    inputCommands=cms.untracked.vstring('keep *'),
    duplicateCheckMode=cms.untracked.string('noDuplicateCheck'))
process.TFileService=cms.Service('TFileService',fileName=cms.string('d0_matching_friend.root'),closeFileFast=cms.untracked.bool(True))
process.rematch=cms.EDAnalyzer('D0CrabFriend',inputFile=cms.string(options.inputFiles[0] if len(options.inputFiles)==1 else ''),manifest=cms.string(options.manifest),summary=cms.string('summary.json'),allowPartial=cms.bool(options.allowPartial),signalOnlyGenMatching=cms.untracked.bool(options.signalOnlyGenMatching))
process.p=cms.Path(process.rematch)
