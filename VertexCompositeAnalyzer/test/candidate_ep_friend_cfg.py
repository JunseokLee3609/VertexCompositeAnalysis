import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing
options=VarParsing('analysis')
options.register('ntuple','input_slim.root',VarParsing.multiplicity.singleton,VarParsing.varType.string,'Existing D0/Dstar ntuple or slim; candidates unchanged')
options.register('report','validation.json',VarParsing.multiplicity.singleton,VarParsing.varType.string,'Validation JSON')
options.register('directCheck',False,VarParsing.multiplicity.singleton,VarParsing.varType.bool,'Direct track-loop checks on two RECO and two GEN candidates per species/event')
options.register('provenance','',VarParsing.multiplicity.singleton,VarParsing.varType.string,'Production metadata JSON')
options.parseArguments()
process=cms.Process('CANDIDATEEP')
process.load('FWCore.MessageService.MessageLogger_cfi')
process.MessageLogger.cerr.FwkReport.reportEvery=100
process.options=cms.untracked.PSet(numberOfThreads=cms.untracked.uint32(1),numberOfStreams=cms.untracked.uint32(1))
process.maxEvents=cms.untracked.PSet(input=cms.untracked.int32(options.maxEvents))
process.source=cms.Source('PoolSource',fileNames=cms.untracked.vstring(options.inputFiles),
    duplicateCheckMode=cms.untracked.string('noDuplicateCheck'))
process.TFileService=cms.Service('TFileService',fileName=cms.string(options.outputFile))
process.unpackedTracksAndVertices=cms.EDProducer('D0AuditTrackAndVertexUnpacker',
    packedCandidates=cms.VInputTag('packedPFCandidates','lostTracks',cms.InputTag('lostTracks','eleTracks')),
    packedCandidateNormChi2Map=cms.VInputTag('packedPFCandidateTrackChi2','lostTrackChi2',cms.InputTag('')),
    primaryVertices=cms.InputTag('offlineSlimmedPrimaryVertices'),
    secondaryVertices=cms.InputTag('slimmedSecondaryVertices'),recoverTracks=cms.bool(True))
process.friend=cms.EDAnalyzer('CandidateEPFriend',
    provenance=cms.string(options.provenance),
    ntuple=cms.string(options.ntuple),report=cms.string(options.report),
    requireComplete=cms.bool(options.maxEvents<0),directCheck=cms.bool(options.directCheck),
    tracks=cms.InputTag('unpackedTracksAndVertices'),vertices=cms.InputTag('unpackedTracksAndVertices'),
    beamspot=cms.InputTag('offlineBeamSpot'),gen=cms.InputTag('prunedGenParticles'),
    packedGen=cms.InputTag('packedGenParticles'),genOrigin=cms.InputTag('genParticles','xyz0'))
process.p=cms.Path(process.unpackedTracksAndVertices*process.friend)
