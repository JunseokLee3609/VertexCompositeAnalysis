import FWCore.ParameterSet.Config as cms
from Configuration.StandardSequences.Eras import eras
import sys, os

# Parse args robustly for cmsRun (argv[1]=cfg.py)
py_idx = next(i for i, a in enumerate(sys.argv) if a.endswith('.py'))
fileToRun = str(sys.argv[py_idx + 1]) if len(sys.argv) > py_idx + 1 else ''
outputSuffix = sys.argv[py_idx + 2] if len(sys.argv) > py_idx + 2 else 'default'
subDir = sys.argv[py_idx + 3] if len(sys.argv) > py_idx + 3 else ''

out_base_dir = '/eos/cms/store/group/phys_heavyions/junseok/DstarAnalysis'
out_dir = f"{out_base_dir}/{subDir.strip('/')}" if subDir else out_base_dir
out_suffix = os.path.basename(str(outputSuffix)).replace('.root', '') or 'default'
try:
    os.makedirs(out_dir, exist_ok=True)
except Exception as e:
    raise RuntimeError(f"Failed to create output directory: {out_dir} ({e})")
outfile = f"file:{out_dir}/d0ana_tree_step1_{out_suffix}.root"

process = cms.Process('ANASKIM', eras.Run3_2023)

process.load('Configuration.StandardSequences.Services_cff')
process.load('Configuration.StandardSequences.GeometryRecoDB_cff')
process.load('Configuration.StandardSequences.MagneticField_cff')
process.load('Configuration.StandardSequences.Reconstruction_Data_cff')

# Limit the output messages
process.load('FWCore.MessageService.MessageLogger_cfi')
process.MessageLogger.cerr.FwkReport.reportEvery = 10
process.options = cms.untracked.PSet(wantSummary=cms.untracked.bool(True))
process.FastTimerService = cms.Service(
    "FastTimerService",
    printEventSummary=cms.untracked.bool(True),
    printRunSummary=cms.untracked.bool(True),
    printJobSummary=cms.untracked.bool(True),
    enableDQM=cms.untracked.bool(False),
)

# Define the input source
_default_files = [
    "/store/mc/HINPbPbSpring23MiniAOD/promptD0ToKPi_PT-0_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/120000/9a2aa582-4960-4b1f-b853-b4fcfd5143ee.root",
    "/store/mc/HINPbPbSpring23MiniAOD/promptD0ToKPi_PT-0_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/120000/80d5a73b-06e3-42bb-b31a-0477dad1e436.root",
    "/store/mc/HINPbPbSpring23MiniAOD/promptD0ToKPi_PT-0_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/120000/ec044cd9-6811-4f78-9e73-c6d8ebf34fb3.root",
]
process.source = cms.Source(
    "PoolSource",
    fileNames=cms.untracked.vstring(str(fileToRun) if fileToRun else _default_files),
)
process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(2000))

# Set the global tag
process.load('Configuration.StandardSequences.FrontierConditions_GlobalTag_cff')
process.GlobalTag.globaltag = cms.string('132X_mcRun3_2023_realistic_HI_v9')

# Add PbPb centrality
process.load("RecoHI.HiCentralityAlgos.CentralityBin_cfi")
process.GlobalTag.snapshotTime = cms.string("9999-12-31 23:59:59.000")
process.GlobalTag.toGet.extend([
    cms.PSet(
        record=cms.string("HeavyIonRcd"),
        tag=cms.string("CentralityTable_HFtowers200_HydjetDrum5F_Run3v1302x04_Official_MC"),
        connect=cms.string("frontier://FrontierProd/CMS_CONDITIONS"),
        label=cms.untracked.string("HFtowers"),
    )
])
process.cent_seq = cms.Sequence(process.centralityBin)

# =============== Import Sequences =====================
# Trigger Selection
import HLTrigger.HLTfilters.hltHighLevel_cfi

process.hltFilter = HLTrigger.HLTfilters.hltHighLevel_cfi.hltHighLevel.clone()
process.hltFilter.andOr = cms.bool(True)
process.hltFilter.throw = cms.bool(False)
process.hltFilter.HLTPaths = [
    "HLT_HIMinimumBiasHF1AND_v*",  # 24
    "HLT_HIMinimumBiasHF1ANDZDC2nOR_v*",  # 25
    "HLT_HIMinimumBiasHF1ANDZDC1nOR_v*",  # 26
]

# Add PbPb collision event selection
process.load('VertexCompositeAnalysis.VertexCompositeProducer.collisionEventSelection_cff')
process.load('VertexCompositeAnalysis.VertexCompositeProducer.hfCoincFilter_cff')
process.load('VertexCompositeAnalysis.VertexCompositeProducer.hffilter_cfi')
process.colEvtSel = cms.Sequence()

# Define the event selection sequence
process.eventFilter_HM = cms.Sequence(process.hltFilter)
process.eventFilter_HM_step = cms.Path(process.eventFilter_HM)

from VertexCompositeAnalysis.VertexCompositeProducer.PATAlgos_cff import changeToMiniAOD

# Define the analysis steps

########## D0 candidate rereco ###############################################################
process.load("VertexCompositeAnalysis.VertexCompositeProducer.generalD0Candidates_cff")
process.generalD0CandidatesNew = process.generalD0Candidates.clone()
# process.generalD0CandidatesNew.trkPtSumCut = cms.double(1.6)
process.generalD0CandidatesNew.tkChi2Cut = cms.double(99)
process.generalD0CandidatesNew.tkNhitsCut = cms.int32(0)
process.generalD0CandidatesNew.tkPtErrCut = cms.double(0.1)
process.generalD0CandidatesNew.tkPtCut = cms.double(1.0)
process.generalD0CandidatesNew.tkEtaCut = cms.double(2.4)
process.generalD0CandidatesNew.tkPtSumCut = cms.double(0.0)
process.generalD0CandidatesNew.tkEtaDiffCut = cms.double(1.0)
process.generalD0CandidatesNew.dauTransImpactSigCut = cms.double(0.0)
process.generalD0CandidatesNew.dauLongImpactSigCut = cms.double(0.0)
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
process.generalD0CandidatesNew.vtxSignificance3DCut = cms.double(0.0)
process.generalD0CandidatesNew.d0MassCut = cms.double(0.14)
process.generalD0CandidatesNew.d0AbsYCut = cms.double(1.6)
process.generalD0CandidatesNew.dPtCut = cms.double(0.0)

process.generalD0CandidatesNew.mPiKCutMin = cms.double(1.70)
process.generalD0CandidatesNew.mPiKCutMax = cms.double(2.00)

process.load("VertexCompositeAnalysis.VertexCompositeProducer.generalDStarCandidates_cff")
process.generalDStarCandidatesNew = process.generalDStarCandidates.clone()
process.generalDStarCandidatesNew.tkChi2Cut = cms.double(999)
process.generalDStarCandidatesNew.tkNhitsCut = cms.int32(0)
process.generalDStarCandidatesNew.tkPtErrCut = cms.double(9999.0)
process.generalDStarCandidatesNew.tkPtCut = cms.double(0.3)
process.generalDStarCandidatesNew.tkEtaCut = cms.double(999.0)
process.generalDStarCandidatesNew.tkPtSumCut = cms.double(0.0)
process.generalDStarCandidatesNew.tkEtaDiffCut = cms.double(999.0)
process.generalDStarCandidatesNew.dauTransImpactSigCut = cms.double(0.0)
process.generalDStarCandidatesNew.dauLongImpactSigCut = cms.double(0.0)
process.generalDStarCandidatesNew.tkDCACut = cms.double(9999.0)
process.generalDStarCandidatesNew.vtxChi2Cut = cms.double(9999.0)
process.generalDStarCandidatesNew.VtxChiProbCut = cms.double(0.0001)
process.generalDStarCandidatesNew.collinearityCut2D = cms.double(-2.0)
process.generalDStarCandidatesNew.collinearityCut3D = cms.double(-2.0)
process.generalDStarCandidatesNew.alphaCut = cms.double(999.0)
process.generalDStarCandidatesNew.alpha2DCut = cms.double(999.0)
process.generalDStarCandidatesNew.rVtxCut = cms.double(0.0)
process.generalDStarCandidatesNew.lVtxCut = cms.double(0.0)
process.generalDStarCandidatesNew.vtxSignificance2DCut = cms.double(0.0)
process.generalDStarCandidatesNew.vtxSignificance3DCut = cms.double(0.0)
process.generalDStarCandidatesNew.dStarMassCut = cms.double(0.22)
process.generalDStarCandidatesNew.dPtCut = cms.double(0.0)

process.d0rereco_step = cms.Path(process.eventFilter_HM * process.generalD0CandidatesNew)

# produce D0 trees
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.d0selector_cff")
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.d0analyzer_tree_cff")
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.dStaranalyzer_tree_cff")
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.eventinfotree_cff")
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.eventplaneanalyzer_cfi")

process.TFileService = cms.Service(
    "TFileService",
    fileName=cms.string(outfile),
)

process.d0ana_mc.GenParticleCollection = cms.untracked.InputTag("prunedGenParticles")
process.d0ana_mc.useAnyMVA = cms.bool(False)
process.d0ana_mc.multMin = cms.untracked.double(0)
process.d0ana_mc.multMax = cms.untracked.double(100000)

process.d0ana_newreduced = process.d0ana_mc.clone()
process.d0ana_newreduced.CompositeCollection = cms.untracked.InputTag("generalD0CandidatesNew:D0")
process.d0ana_newreduced.VertexCompositeCollection = cms.untracked.InputTag("generalD0CandidatesNew:D0")

process.dStarana_mc.GenParticleCollection = cms.untracked.InputTag("prunedGenParticles")
process.dStarana_mc.useAnyMVA = cms.bool(False)
process.dStarana_mc.CompositeCollection = cms.untracked.InputTag("generalDStarCandidatesNew:DStar")
process.dStarana_mc.MVACollection = cms.InputTag("generalDStarCandidatesNew:MVAValuesNewDStar")
process.generalDStarCandidatesNew.d0Collection = cms.InputTag("generalD0CandidatesNew:D0")

process.dStarAna_step = cms.Path(
    process.eventFilter_HM
    * process.generalD0CandidatesNew
    * process.generalDStarCandidatesNew
    # * process.d0ana_newreduced
    * process.dStarana_mc
)

# eventinfoana must be in EndPath, and process.eventinfoana.selectEvents must be the name of eventFilter_HM Path
process.eventinfoana.selectEvents = cms.untracked.string('eventFilter_HM_step')
process.eventinfoana.triggerPathNames = cms.untracked.vstring(
    "HLT_HIMinimumBiasHF1AND_v*",  # 24
    "HLT_HIMinimumBiasHF1ANDZDC2nOR_v",  # 25
    "HLT_HIMinimumBiasHF1ANDZDC1nOR_v",  # 26
)
process.eventinfoana.eventFilterNames = cms.untracked.vstring(
    'Flag_colEvtSel',
    'Flag_hfCoincFilter',
    'Flag_primaryVertexFilter',
)

process.centralityBin.Centrality = cms.InputTag("hiCentrality")
process.centralityBin.centralityVariable = cms.string("HFtowers")
process.eventinfoana.triggerFilterNames = cms.untracked.vstring()
process.eventinfoana.stageL1Trigger = cms.uint32(2)
process.eventinfoana.centralityBinLabel = cms.InputTag("centralityBin", "HFtowers")
process.eventinfoana.centralitySrc = cms.InputTag("hiCentrality")
process.pevt = cms.EndPath(process.eventinfoana)
process.centralityPath = cms.Path(process.cent_seq)

# Define the process schedule
process.schedule = cms.Schedule(
    process.centralityPath,
    process.eventFilter_HM_step,
    process.dStarAna_step,
    process.pevt,
)

# Add the event selection filters
process.Flag_colEvtSel = cms.Path(process.eventFilter_HM * process.colEvtSel)
process.Flag_primaryVertexFilter = cms.Path(
    process.eventFilter_HM * process.primaryVertexFilter * process.clusterCompatibilityFilter
)
# follow the exactly same config of process.eventinfoana.eventFilterNames
eventFilterPaths = [process.Flag_colEvtSel, process.Flag_primaryVertexFilter]
for P in eventFilterPaths:
    process.schedule.insert(0, P)

changeToMiniAOD(process)
process.options.numberOfThreads = 1

process.output = cms.OutputModule(
    "PoolOutputModule",
    outputCommands=cms.untracked.vstring("keep *_*_*_ANASKIM"),
    fileName=cms.untracked.string('output.root'),
)

process.outputPath = cms.EndPath(process.output)
# process.schedule.append(process.outputPath)
