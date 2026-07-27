import FWCore.ParameterSet.Config as cms
import FWCore.PythonUtilities.LumiList as LumiList
from Configuration.StandardSequences.Eras import eras
from Configuration.ProcessModifiers.pp_on_AA_cff import pp_on_AA
import sys, os
# Parse args robustly for cmsRun (argv[1]=cfg.py)
py_idx = next(i for i,a in enumerate(sys.argv) if a.endswith('.py'))
fileToRun = str(sys.argv[py_idx+1]) if len(sys.argv) > py_idx+1 else ''
outputSuffix = sys.argv[py_idx+2] if len(sys.argv) > py_idx+2 else 'default'
# sanitize suffix if a path was accidentally passed
outputSuffix = os.path.basename(outputSuffix).replace('.root','')
# optional args:
#   legacy call: <input> <idx> <list_subdir>
#   new call:    <input> <idx> <common_subdir> <list_subdir>
arg3 = sys.argv[py_idx+3] if len(sys.argv) > py_idx+3 else ''
arg4 = sys.argv[py_idx+4] if len(sys.argv) > py_idx+4 else ''

if arg4:
    commonSubDir = arg3
    listSubDir = arg4
else:
    commonSubDir = ''
    listSubDir = arg3

common_subdir_clean = str(commonSubDir).strip('/')
list_subdir_clean = str(listSubDir).strip('/')
if list_subdir_clean == '__NO_LIST_SUBDIR__':
    list_subdir_clean = ''
if not common_subdir_clean and not list_subdir_clean:
    list_subdir_clean = 'HIPhysicsRawPrime12'

legacy_out_base_dir = 'root://cluster142.knu.ac.kr//store/user/junseok/DstarAnalysis'
prefixed_out_base_dir = 'root://cluster142.knu.ac.kr//store/user/junseok'
if common_subdir_clean:
    out_dir_parts = [common_subdir_clean, list_subdir_clean]
    out_base_dir = prefixed_out_base_dir
else:
    out_dir_parts = [list_subdir_clean]
    out_base_dir = legacy_out_base_dir
is_remote_out = out_base_dir.startswith('root://')
outDir = '/'.join([out_base_dir] + [part for part in out_dir_parts if part])
if not is_remote_out:
    try:
        os.makedirs(outDir, exist_ok=True)
    except Exception as e:
        raise RuntimeError(f"Failed to create output directory: {outDir} ({e})")
outfile = f"{outDir}/d0ana_tree_stepMVA_{outputSuffix}.root"

process = cms.Process('ANASKIM', eras.Run3_pp_on_PbPb_2023)
# This cfg targets Run-3 PbPb conditions through the pp-on-PbPb era chain.
if not process.isUsingModifier(pp_on_AA):
    raise RuntimeError("Expected pp_on_AA modifier from Run3_pp_on_PbPb_2023 era.")

process.load('Configuration.StandardSequences.Services_cff')
process.load('Configuration.StandardSequences.GeometryRecoDB_cff')
process.load('Configuration.StandardSequences.MagneticField_cff')
process.load('Configuration.StandardSequences.Reconstruction_Data_cff')

# Limit the output messages
process.load('FWCore.MessageService.MessageLogger_cfi')
process.MessageLogger.cerr.FwkReport.reportEvery = 1000
# process.MessageLogger.cerr.threshold = "DEBUG"
# process.MessageLogger.debugModules=["*"]
#process.MessageLogger.cerr.threshold = cms.untracked.string('DEBUG')
process.MessageLogger.cerr.DStarDebug = cms.untracked.PSet(
      limit = cms.untracked.int32(-1)
  )
process.MessageLogger.cerr.D0DaughterOrder = cms.untracked.PSet(limit=cms.untracked.int32(-1))

process.MessageLogger.cerr.GenMatching = cms.untracked.PSet(
      limit = cms.untracked.int32(-1)
  )
process.MessageLogger.cerr.DStarDecayFilter = cms.untracked.PSet(
      limit = cms.untracked.int32(-1)
  )
process.MessageLogger.cerr.PATCompositeTreeProducer = cms.untracked.PSet(
      limit = cms.untracked.int32(-1)
  )
# Suppress CentralityDebug LogPrint messages.
process.MessageLogger.suppressInfo = cms.untracked.vstring('CentralityDebug')
process.MessageLogger.cerr.CentralityDebug = cms.untracked.PSet(limit=cms.untracked.int32(0))
#process.MessageLogger.cerr.CentralityDebug = cms.untracked.PSet(
#      limit = cms.untracked.int32(-1)
#  )
process.options = cms.untracked.PSet(wantSummary = cms.untracked.bool(True))
process.FastTimerService = cms.Service("FastTimerService",
                                       printEventSummary = cms.untracked.bool(True),
                                       printRunSummary = cms.untracked.bool(True),
                                       printJobSummary = cms.untracked.bool(True),
                                       enableDQM = cms.untracked.bool(False)
)


# Define the input source
process.source = cms.Source("PoolSource",
    fileNames = cms.untracked.vstring(
        str(fileToRun)
        #"file:/eos/cms/store/group/phys_heavyions/dileptons/Data2023/MINIAOD/HIPhysicsRawPrime0/Run375064/7ed5766f-6b1d-415e-8916-e62825a6347f.root",
    ),
)
process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(-1))
# Quick local timing estimate: set this to a small positive number (e.g. 200).

process.source.lumisToProcess = LumiList.LumiList(filename = '/eos/user/c/cmsdqm/www/CAF/certification/Collisions23HI/Cert_Collisions2023HI_374288_375823_Golden.json').getVLuminosityBlockRange()
# Set the global tag
process.load('Configuration.StandardSequences.FrontierConditions_GlobalTag_cff')
process.GlobalTag.globaltag = cms.string('132X_dataRun3_Prompt_v7')
#process.GlobalTag.globaltag = cms.string('132X_mcRun3_2023_realistic_HI_v9')

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

# Add PbPb centrality (read from conditions DB instead of local sqlite)
process.load("RecoHI.HiCentralityAlgos.CentralityBin_cfi")
isMC = False
process.GlobalTag.snapshotTime = cms.string("9999-12-31 23:59:59.000")
process.GlobalTag.toGet.extend([
    cms.PSet(
        record = cms.string("HeavyIonRcd"),
        tag = cms.string("CentralityTable_HFtowers200_HydjetDrum5F_Run3v1302x04_Official_MC") if isMC else cms.string("CentralityTable_HFtowers200_DataPbPb_periHYDJETshape_Run3v1302x04_Nominal_Offline"),
        connect = cms.string("frontier://FrontierProd/CMS_CONDITIONS"),
        label = cms.untracked.string("HFtowers")
    )
])
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
#process.generalD0CandidatesNew.trkPtSumCut = cms.double(1.6)
process.generalD0CandidatesNew.tkChi2Cut = cms.double(5)
process.generalD0CandidatesNew.tkNhitsCut = cms.int32(0)
process.generalD0CandidatesNew.tkPtErrCut = cms.double(0.1)
# process.generalD0CandidatesNew.tkPtCut = cms.double(0.8)
process.generalD0CandidatesNew.tkPtCut = cms.double(1.5)
process.generalD0CandidatesNew.tkEtaCut = cms.double(2.4)
#process.generalD0CandidatesNew.tkEtaCut = cms.double(1.8)
process.generalD0CandidatesNew.tkPtSumCut = cms.double(0.0)
process.generalD0CandidatesNew.tkEtaDiffCut = cms.double(1.0)
process.generalD0CandidatesNew.dauTransImpactSigCut = cms.double(0.)
process.generalD0CandidatesNew.dauLongImpactSigCut = cms.double(0.)
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
process.generalD0CandidatesNew.mvaCut = cms.double(-1)
# process.generalD0CandidatesNew.GBRForestLabel = cms.string('D0InPbPbXGB')
# #process.generalD0CandidatesNew.GBRForestFileName = cms.string('GBRForestfile_XGBDT_PromptD0InPbPb_15Params_v1_08Mar.root')
# process.generalD0CandidatesNew.GBRForestFileName = cms.string('GBRForestfile_XGBDT_PromptD0InPbPb_pT_y_cBIN_19Params_v1_25Mar.root')
process.generalD0CandidatesNew.input_names = cms.vstring('float_input')
process.generalD0CandidatesNew.output_names = cms.vstring('probabilities')
process.generalD0CandidatesNew.onnxModelFileName = cms.string("XGBoost_Model_OnlyNonPrompt_05Mar26_Centrality_pTerr_ptErr011_1.onnx")
process.generalD0CandidatesNew.onnxFeatureNames = cms.vstring(
    'pT',
    'y',
    'centrality',
    'VtxProb',
    '3DCosPointingAngle',
    '3DPointingAngle',
    '2DCosPointingAngle',
    '2DPointingAngle',
    '3DDecayLength',
    '3DDecayLengthSignificance',
    '2DDecayLength',
    '2DDecayLengthSignificance',
    'pTD1',
    'EtaD1',
    'pTerrD1',
    'pTD2',
    'EtaD2',
    'pTerrD2',
    'Trk3DDCA',
    'dEta_dau',
)

process.generalD0CandidatesNew.mPiKCutMin = cms.double(1.70)
process.generalD0CandidatesNew.mPiKCutMax = cms.double(2.00)
#process.generalD0CandidatesNewWrongSign = process.generalD0CandidatesNew.clone(isWrongSign = cms.bool(True))

process.load("VertexCompositeAnalysis.VertexCompositeProducer.generalDStarCandidates_cff")
process.generalDStarCandidatesNew = process.generalDStarCandidates.clone()
process.generalDStarCandidatesNew.trkPtSumCut = cms.double(0.0)
process.generalDStarCandidatesNew.trkEtaDiffCut = cms.double(99.0)
process.generalDStarCandidatesNew.tkNhitsCut = cms.int32(0)
process.generalDStarCandidatesNew.tkPtErrCut = cms.double(0.1)
process.generalDStarCandidatesNew.tkPtCut = cms.double(0.3)
process.generalDStarCandidatesNew.tkChi2Cut = cms.double(5)
process.generalDStarCandidatesNew.VtxChiProbCut = cms.double(0.00)
#process.generalDStarCandidatesNew.vtxSignificance3DCut = cms.double(3)
#process.generalDStarCandidatesNew.alphaCut = cms.double(1)
#process.generalDStarCandidatesNew.alpha2DCut = cms.double(1)
process.generalDStarCandidatesNew.dauLongImpactSigCut = cms.double(0.0)
process.generalDStarCandidatesNew.dauTransImpactSigCut = cms.double(0.0)# it will be cut of by 3 in selector
process.generalDStarCandidatesNew.dPtCut = cms.double(4.5)
# process.generalDStarCandidatesNew.useAnyMVA=cms.bool(True)
# process.generalDStarCandidatesNew.GBRForestFileName=cms.string('GBRForestfile_XGBDT_PromptDstarInPbPb_default_MB_OnlyMC.root')


#process.d0rereco_wrongsign_step = cms.Path( process.eventFilter_HM * process.generalD0CandidatesNewWrongSign )



# produce D0 trees
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.d0selector_cff")
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.d0analyzer_tree_cff")
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.dStarselector_cfi")
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.dStaranalyzer_tree_cff")
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.eventinfotree_cff")
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.eventplaneanalyzer_cfi")
process.load("RecoHI.HiEvtPlaneAlgos.HiEvtPlane_cfi")
process.load("RecoHI.HiEvtPlaneAlgos.hiEvtPlaneFlat_cfi")

# Keep the same config parameters from legacy modules, but run PAT6 plugin type.
process.d0ana = cms.EDAnalyzer("PATCompositeTreeProducer6", **process.d0ana.parameters_())
process.dStarana = cms.EDAnalyzer("PATCompositeTreeProducer6", **process.dStarana.parameters_())

process.TFileService = cms.Service("TFileService",
    fileName =
    cms.string(outfile)
    )

process.d0ana.multMin = cms.untracked.double(0)
process.d0ana.multMax = cms.untracked.double(100000)
process.d0ana.useAnyMVA = cms.untracked.bool(True)
process.d0ana.isCentrality = cms.untracked.bool(False)
process.d0ana.MVACollection = cms.untracked.InputTag("generalD0CandidatesNew:MVAValuesD0")
#process.d0ana_wrongsign.useAnyMVA = cms.bool(False)
#process.d0ana_wrongsign.multMin = cms.untracked.double(0)
#process.d0ana_wrongsign.multMax = cms.untracked.double(100000)
#process.d0ana_wrongsign.VertexCompositeCollection = cms.untracked.InputTag("d0selectorWS:D0")
#process.d0ana_wrongsign.MVACollection = cms.InputTag("d0selectorWS:MVAValuesNewD0")
process.generalDStarCandidatesNew.d0Collection = cms.InputTag("generalD0CandidatesNew:D0")

process.d0ana_newreduced = process.d0ana.clone()
process.d0ana_newreduced.CompositeCollection = cms.untracked.InputTag("generalD0CandidatesNew:D0")
process.dStarana.useAnyMVA = cms.untracked.bool(False)
process.dStarana.isEventPlane = cms.untracked.bool(True)
process.dStarana.isCentrality = cms.untracked.bool(True)
process.dStarana.centralityBinLabel = cms.untracked.InputTag("centralityBin","HFtowers")
process.dStarana.centralitySrc = cms.untracked.InputTag("hiCentrality")
process.dStarana.CompositeCollection = cms.untracked.InputTag("generalDStarCandidatesNew:DStar")
process.dStarana.MVACollection = cms.untracked.InputTag("generalDStarCandidatesNew:MVAValuesNewDStar")
# Use recalculated event planes for the main ntuple EP branches.
process.dStarana.eventplaneSrc = cms.untracked.InputTag("hiEvtPlaneFlatRecalc")
# Disable EP stored-vs-recalc comparison branches in final output.
# Leave empty so compareEventPlane_ stays false (debug-only path disabled for tree).
process.dStarana.eventplaneSrcRecalc = cms.untracked.InputTag("hiEvtPlaneFlatRecalc")
process.eventplane.VertexCompositeCollection= cms.untracked.InputTag("generalDStarCandidatesNew:DStar")
# Cover full D* fitter mass range (2.010 +/- 0.22) when removing candidate daughters.
process.eventplane.massMinForExclusion = cms.untracked.double(1.79)
process.eventplane.massMaxForExclusion = cms.untracked.double(2.25)

process.hiEvtPlaneRecalc = process.hiEvtPlane.clone(
    trackTag=cms.InputTag("packedPFCandidates"),
    lostTag=cms.InputTag("lostTracks"),
    chi2MapTag=cms.InputTag("packedPFCandidateTrackChi2"),
    chi2MapLostTag=cms.InputTag("lostTrackChi2"),
    caloTag=cms.InputTag("particleFlow"),
    vertexTag=cms.InputTag("offlineSlimmedPrimaryVertices"),
    centralityVariable=cms.string("HFtowers"),
    centralityBinTag=cms.InputTag("centralityBin", "HFtowers"),
    cutEra=cms.int32(0),
    minet=cms.double(0.01),
    minpt=cms.double(0.5),
    dzdzerror_pix=cms.double(40.0),
    caloCentRef=cms.double(-1.0),
    caloCentRefWidth=cms.double(-1.0),
)

process.hiEvtPlaneFlatRecalc = process.hiEvtPlaneFlat.clone(
    inputPlanesTag=cms.InputTag("hiEvtPlaneRecalc"),
    centralityTag=cms.InputTag("hiCentrality"),
    centralityVariable=cms.string("HFtowers"),
    centralityBinTag=cms.InputTag("centralityBin", "HFtowers"),
    trackTag=cms.InputTag("packedPFCandidates"),
    vertexTag=cms.InputTag("offlineSlimmedPrimaryVertices"),
    caloCentRef=cms.double(-1.0),
    caloCentRefWidth=cms.double(-1.0),
)
#process.d0ana_wrongsign_newreduced = process.d0ana_wrongsign.clone()
#process.d0ana_wrongsign_newreduced.VertexCompositeCollection = cms.untracked.InputTag("d0selectorWSNewReduced:D0")
#process.d0ana_wrongsign_newreduced.MVACollection = cms.InputTag("d0selectorWSNewReduced:MVAValuesNewD0")
#process.d0ana_wrongsign_newreduced.DCAValCollection = cms.InputTag("d0selectorWSNewReduced:DCAValuesNewD0")
#process.d0ana_wrongsign_newreduced.DCAErrCollection = cms.InputTag("d0selectorWSNewReduced:DCAErrorsNewD0")
process.d0candCountFilter = cms.EDFilter("CandViewCountFilter",
    src = cms.InputTag("generalD0CandidatesNew", "D0"),
    minNumber = cms.uint32(1),
)



process.dStarAna_step = cms.Path(
    process.eventFilter_HM
    * process.generalD0CandidatesNew
    * process.generalDStarCandidatesNew
    # * process.d0candCountFilter
    * process.hiEvtPlaneRecalc
    * process.hiEvtPlaneFlatRecalc
    * process.d0ana_newreduced
    * process.dStarana
    * process.eventplane
)
# process.dStarAna_step = cms.Path( process.eventFilter_HM * process.generalD0CandidatesNew* process.d0ana_newreduced * process.eventplane)
# process.dStarAna_step = cms.Path( process.eventFilter_HM * process.generalD0CandidatesNew* process.d0ana_newreduced)

# eventinfoana must be in EndPath, and process.eventinfoana.selectEvents must be the name of a Path
process.eventinfoana.selectEvents = cms.untracked.string('dStarAna_step')
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
process.eventinfoana.isEventPlane = cms.bool(False)
process.eventinfoana.eventplaneSrc = cms.InputTag("hiEvtPlaneFlatRecalc")
process.pevt = cms.EndPath(process.eventinfoana)

process.c = cms.Path(process.cent_seq)
# process.pws = cms.Path(process.d0ana_wrongsign_seq2)

# Add the Conversion tree

# Define the process schedule
process.schedule = cms.Schedule(
    process.c,
    process.eventFilter_HM_step,
    process.dStarAna_step,
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
