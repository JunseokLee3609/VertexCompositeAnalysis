import FWCore.ParameterSet.Config as cms
from Configuration.StandardSequences.Eras import eras
process = cms.Process('ANASKIM', eras.Run3_2023)

process.load('Configuration.StandardSequences.Services_cff')
process.load('Configuration.StandardSequences.GeometryRecoDB_cff')
process.load('Configuration.StandardSequences.MagneticField_cff')
process.load('Configuration.StandardSequences.Reconstruction_Data_cff')

# Limit the output messages
process.load('FWCore.MessageService.MessageLogger_cfi')
process.MessageLogger.cerr.FwkReport.reportEvery = 1
process.options = cms.untracked.PSet(wantSummary = cms.untracked.bool(True))

# Define the input source
process.source = cms.Source("PoolSource",
    fileNames = cms.untracked.vstring("file:/eos/cms/store/group/phys_heavyions/dileptons/Data2023/MINIAOD/HIPhysicsRawPrime0/Run375064/7ed5766f-6b1d-415e-8916-e62825a6347f.root"),
)
process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(300))

# Set the global tag
process.load('Configuration.StandardSequences.FrontierConditions_GlobalTag_cff')
process.GlobalTag.globaltag = cms.string('132X_dataRun3_Prompt_v4')

## Set ZDC information
#process.es_pool = cms.ESSource("PoolDBESSource",
#    timetype = cms.string('runnumber'),
#    toGet = cms.VPSet(cms.PSet(record = cms.string("HcalElectronicsMapRcd"), tag = cms.string("HcalElectronicsMap_2021_v2.0_data"))),
#    connect = cms.string('frontier://FrontierProd/CMS_CONDITIONS'),
#    authenticationMethod = cms.untracked.uint32(1)
#)
#process.es_prefer = cms.ESPrefer('HcalTextCalibrations', 'es_ascii')
#process.es_ascii = cms.ESSource('HcalTextCalibrations',
#    input = cms.VPSet(cms.PSet(object = cms.string('ElectronicsMap'), file = cms.FileInPath("emap_2023_newZDC_v3.txt")))
#)

# Add PbPb centrality
process.load("RecoHI.HiCentralityAlgos.CentralityBin_cfi")
process.GlobalTag.snapshotTime = cms.string("9999-12-31 23:59:59.000")
process.GlobalTag.toGet.extend([
    cms.PSet(record = cms.string("HeavyIonRcd"),
        tag = cms.string("CentralityTable_HFtowers200_DataPbPb_periHYDJETshape_run3v1302x04_offline_374810"),
        connect = cms.string("sqlite_file:CentralityTable_HFtowers200_DataPbPb_periHYDJETshape_run3v1302x04_offline_374810.db"),
        label = cms.untracked.string("HFtowers")
        )
    ]
)
process.cent_seq = cms.Sequence(process.centralityBin)

# =============== Import Sequences =====================
#Trigger Selection
### Comment out for the timing being assuming running on secondary dataset with trigger bit selected already
# Add trigger selection
import HLTrigger.HLTfilters.hltHighLevel_cfi
process.hltFilter = HLTrigger.HLTfilters.hltHighLevel_cfi.hltHighLevel.clone()
process.hltFilter.andOr = cms.bool(True)
process.hltFilter.throw = cms.bool(False)
process.hltFilter.HLTPaths = [
    "HLT_HIMinimumBiasHF1AND_v*", #24
    "HLT_HIMinimumBiasHF1ANDZDC2nOR_v*", #25
    "HLT_HIMinimumBiasHF1ANDZDC1nOR_v*", #26
]

# Add PbPb collision event selection
process.load('VertexCompositeAnalysis.VertexCompositeProducer.collisionEventSelection_cff')
process.load('VertexCompositeAnalysis.VertexCompositeProducer.hfCoincFilter_cff')
process.load('VertexCompositeAnalysis.VertexCompositeProducer.hffilter_cfi')
process.colEvtSel = cms.Sequence()

# Define the event selection sequence
process.eventFilter_HM = cms.Sequence(
    process.hltFilter
)
process.eventFilter_HM_step = cms.Path( process.eventFilter_HM )

from VertexCompositeAnalysis.VertexCompositeProducer.PATAlgos_cff import changeToMiniAOD

# Define the analysis steps

########## D0 candidate rereco ###############################################################
process.load("VertexCompositeAnalysis.VertexCompositeProducer.generalD0Candidates_cff")
process.generalD0CandidatesNew = process.generalD0Candidates.clone()
process.generalD0CandidatesNew.trkPtSumCut = cms.double(1.6)
process.generalD0CandidatesNew.trkEtaDiffCut = cms.double(2.0)
process.generalD0CandidatesNew.tkNhitsCut = cms.int32(11)
process.generalD0CandidatesNew.tkPtErrCut = cms.double(0.1)
process.generalD0CandidatesNew.tkPtCut = cms.double(1.0)
process.generalD0CandidatesNew.alphaCut = cms.double(0.20)
process.generalD0CandidatesNew.alpha2DCut = cms.double(0.20)
process.generalD0CandidatesNew.dPtCut = cms.double(0.0)
process.generalD0CandidatesNew.VtxChiProbCut = cms.double(0.010)
process.generalD0CandidatesNew.mPiKCutMin = cms.double(1.74)
process.generalD0CandidatesNew.mPiKCutMax = cms.double(2.00)
process.generalD0CandidatesNewWrongSign = process.generalD0CandidatesNew.clone(isWrongSign = cms.bool(True))

process.load("VertexCompositeAnalysis.VertexCompositeProducer.generalDStarCandidates_cff")
process.generalDStarCandidatesNew = process.generalDStarCandidates.clone()
process.generalDStarCandidatesNew.trkPtSumCut = cms.double(0.0)
process.generalDStarCandidatesNew.trkEtaDiffCut = cms.double(0.0)
process.generalDStarCandidatesNew.tkNhitsCut = cms.int32(11)
process.generalDStarCandidatesNew.tkPtErrCut = cms.double(0.1)
process.generalDStarCandidatesNew.tkPtCut = cms.double(0.4)
process.generalDStarCandidatesNew.alphaCut = cms.double(0.4)
process.generalDStarCandidatesNew.alpha2DCut = cms.double(0.4)
process.generalDStarCandidatesNew.dPtCut = cms.double(0.0)


process.d0rereco_step = cms.Path( process.eventFilter_HM * process.generalD0CandidatesNew)
#process.d0rereco_wrongsign_step = cms.Path( process.eventFilter_HM * process.generalD0CandidatesNewWrongSign )



# produce D0 trees
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.d0selector_cff")
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.d0analyzer_tree_cff")
#process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.dStarselector_cff")
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.dStaranalyzer_tree_cff")
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.eventinfotree_cff")

process.TFileService = cms.Service("TFileService",
    fileName =
    cms.string('d0ana_tree.root')
    )

# set up selectors
process.d0selector = process.d0selectorBDTPreCut.clone()
# process.d0selector.GBRForestFileName = cms.string('GBRForestfile_BDT_PromptD0InpPb_default_HLT185_WS_Pt1p5MassPeak_v2.root')
process.d0selector.useAnyMVA = cms.bool(False)
process.d0selector.multMin = cms.untracked.double(0)
process.d0selector.multMax = cms.untracked.double(100000)
#process.d0selectorWS = process.d0selector.clone(
#    VertexCompositeCollection = cms.untracked.InputTag("generalD0CandidatesNewWrongSign:D0"),
#    MVACollection = cms.InputTag("generalD0CandidatesNewWrongSign:MVAValues")
#)

process.d0ana.useAnyMVA = cms.bool(False)
process.d0ana.multMin = cms.untracked.double(0)
process.d0ana.multMax = cms.untracked.double(100000)
process.d0ana.VertexCompositeCollection = cms.untracked.InputTag("d0selector:D0")
process.d0ana.MVACollection = cms.InputTag("d0selector:MVAValuesNewD0")
#process.d0ana_wrongsign.useAnyMVA = cms.bool(False)
#process.d0ana_wrongsign.multMin = cms.untracked.double(0)
#process.d0ana_wrongsign.multMax = cms.untracked.double(100000)
#process.d0ana_wrongsign.VertexCompositeCollection = cms.untracked.InputTag("d0selectorWS:D0")
#process.d0ana_wrongsign.MVACollection = cms.InputTag("d0selectorWS:MVAValuesNewD0")

process.d0selectorNewReduced = process.d0selector.clone()
# process.d0selectorNewReduced.GBRForestFileName = cms.string('GBRForestfile_BDT_PromptD0InpPb_default_HLT185_WS_Pt1p5MassPeak_NoPtErrNHitDLAngle2D_v3.root')
process.d0selectorNewReduced.DCAValCollection = cms.InputTag("generalD0CandidatesNew:DCAValuesD0")
process.d0selectorNewReduced.DCAErrCollection = cms.InputTag("generalD0CandidatesNew:DCAErrorsD0")
# process.d0selectorNewReduced.mvaMin = cms.untracked.double(0.3)
process.d0selectorNewReduced.cand3DDecayLengthSigMin = cms.untracked.double(3.)
process.d0selectorNewReduced.cand3DPointingAngleMax = cms.untracked.double(0.2)

process.generalDStarCandidatesNew.d0Collection = cms.InputTag("d0selectorNewReduced:D0")

process.d0selectorWSNewReduced = process.d0selectorWS.clone()
# process.d0selectorWSNewReduced.GBRForestFileName = cms.string('GBRForestfile_BDT_PromptD0InpPb_default_HLT185_WS_Pt1p5MassPeak_NoPtErrNHitDLAngle2D_v3.root')
process.d0selectorWSNewReduced.DCAValCollection = cms.InputTag("generalD0CandidatesNewWrongSign:DCAValuesD0")
process.d0selectorWSNewReduced.DCAErrCollection = cms.InputTag("generalD0CandidatesNewWrongSign:DCAErrorsD0")

process.d0ana_newreduced = process.d0ana.clone()
process.d0ana_newreduced.VertexCompositeCollection = cms.untracked.InputTag("d0selectorNewReduced:D0")
# process.d0ana_newreduced.MVACollection = cms.InputTag("d0selectorNewReduced:MVAValuesNewD0")
process.d0ana_newreduced.DCAValCollection = cms.InputTag("d0selectorNewReduced:DCAValuesNewD0")
process.d0ana_newreduced.DCAErrCollection = cms.InputTag("d0selectorNewReduced:DCAErrorsNewD0")

#process.d0ana_wrongsign_newreduced = process.d0ana_wrongsign.clone()
#process.d0ana_wrongsign_newreduced.VertexCompositeCollection = cms.untracked.InputTag("d0selectorWSNewReduced:D0")
#process.d0ana_wrongsign_newreduced.MVACollection = cms.InputTag("d0selectorWSNewReduced:MVAValuesNewD0")
#process.d0ana_wrongsign_newreduced.DCAValCollection = cms.InputTag("d0selectorWSNewReduced:DCAValuesNewD0")
#process.d0ana_wrongsign_newreduced.DCAErrCollection = cms.InputTag("d0selectorWSNewReduced:DCAErrorsNewD0")


process.d0ana_seq2 = cms.Sequence(process.eventFilter_HM * process.d0selectorNewReduced * process.d0ana_newreduced * process.generalDStarCandidatesNew * process.dStarana)
# process.d0ana_wrongsign_seq2 = cms.Sequence(process.eventFilter_HM * process.d0selectorWSNewReduced * process.d0ana_wrongsign_newreduced)

# eventinfoana must be in EndPath, and process.eventinfoana.selectEvents must be the name of eventFilter_HM Path
process.eventinfoana.selectEvents = cms.untracked.string('eventFilter_HM_step')
process.eventinfoana.triggerPathNames = cms.untracked.vstring(
    "HLT_HIMinimumBiasHF1AND_v*", #24
    "HLT_HIMinimumBiasHF1ANDZDC2nOR_v", #25
    "HLT_HIMinimumBiasHF1ANDZDC1nOR_v", #26
    )
process.eventinfoana.eventFilterNames = cms.untracked.vstring(
    'Flag_colEvtSel',
    'Flag_hfCoincFilter',
    'Flag_primaryVertexFilter', 
    )
process.eventinfoana.triggerFilterNames = cms.untracked.vstring()
process.eventinfoana.stageL1Trigger = cms.uint32(2)
process.pevt = cms.EndPath(process.eventinfoana)

process.p = cms.Path(process.d0ana_seq2)
# process.pws = cms.Path(process.d0ana_wrongsign_seq2)

# Add the Conversion tree

# Define the process schedule
process.schedule = cms.Schedule(
    process.eventFilter_HM_step,
    process.d0rereco_step,
#    process.d0rereco_wrongsign_step,
    process.p,
#    process.pws,
   process.pevt,
)

# Add the event selection filters
process.Flag_colEvtSel = cms.Path(process.eventFilter_HM * process.colEvtSel)
#process.Flag_hfCoincFilter = cms.Path(process.eventFilter_HM * process.hfCoincFilter2Th4)
process.Flag_primaryVertexFilter = cms.Path(process.eventFilter_HM * process.primaryVertexFilter * process.clusterCompatibilityFilter)
# follow the exactly same config of process.eventinfoana.eventFilterNames
#eventFilterPaths = [ process.Flag_colEvtSel , process.Flag_hfCoincFilter , process.Flag_primaryVertexFilter ]
eventFilterPaths = [ process.Flag_colEvtSel  , process.Flag_primaryVertexFilter ]
for P in eventFilterPaths:
    process.schedule.insert(0, P)

changeToMiniAOD(process)
process.options.numberOfThreads = 1

#process.output = cms.OutputModule("PoolOutputModule",
#    outputCommands = cms.untracked.vstring("keep *_*_*_ANASKIM"),
#    fileName = cms.untracked.string('output.root'),
#)
#
#process.outputPath = cms.EndPath(process.output)
#process.schedule.append(process.outputPath)
