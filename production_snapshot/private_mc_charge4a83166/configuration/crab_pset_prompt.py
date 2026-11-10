from pathlib import Path
import runpy
import FWCore.ParameterSet.Config as cms

process = runpy.run_path(str(Path(__file__).with_name("an_mc_prompt_baseline.py")))["process"]
process.source.fileNames = cms.untracked.vstring('root://cms-xrd-global.cern.ch//store/user/rucio/junseok/DstarPromptPt4to15Y12_3M_20260925/DstarPromptPthat2Pt4to15Y12PrivateMC/FullChainPt4to15Y12CERNtoKISTI_20260925/260925_192735/0000/MINIAODSIM_56.root')
process.maxEvents.input = cms.untracked.int32(-1)
process.options.numberOfThreads = cms.untracked.uint32(1)
process.options.numberOfStreams = cms.untracked.uint32(1)
process.TFileService.fileName = cms.string("dstar_tree_mc.root")
process.dStarAna_step.remove(process.d0ana_newreduced)
del process.d0ana_newreduced
for analyzer in (process.dStarana_mc,):
    analyzer.deltaR = cms.untracked.double(0.03)
    analyzer.maxGenDeltaPtRel = cms.untracked.double(0.5)
process.MessageLogger.cerr.FwkReport.reportEvery = 1000
