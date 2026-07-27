import FWCore.ParameterSet.Config as cms
import FWCore.PythonUtilities.LumiList as LumiList
from Configuration.StandardSequences.Eras import eras
from Configuration.ProcessModifiers.pp_on_AA_cff import pp_on_AA
import sys, os


def _is_bool_like(value):
    return str(value).lower() in ("0", "1", "false", "true", "no", "yes")


def _parse_bool(value):
    lowered = str(value).lower()
    if lowered in ("1", "true", "yes"):
        return True
    if lowered in ("0", "false", "no"):
        return False
    raise RuntimeError(
        "isMC must be one of: 1, 0, true, false, yes, no"
    )


# Usage:
# cmsRun cfg.py input.root outputSuffix [subDir] [isMC]
py_idx = next(i for i, a in enumerate(sys.argv) if a.endswith(".py"))
extra_args = sys.argv[py_idx + 1 :]

fileToRun = str(extra_args[0]) if len(extra_args) > 0 else ""
outputSuffix = extra_args[1] if len(extra_args) > 1 else "default"
isMC = False
subDir = ""

for extra_arg in extra_args[2:]:
    if _is_bool_like(extra_arg):
        isMC = _parse_bool(extra_arg)
    elif not subDir:
        subDir = extra_arg

outputSuffix = os.path.basename(outputSuffix).replace(".root", "") or "default"
if not subDir:
    subDir = "" if isMC else "HIPhysicsRawPrime12"

out_base_dir = "root://cluster142.knu.ac.kr//store/user/junseok/DstarAnalysis"
sub_dir_clean = str(subDir).strip("/")
is_remote_out = out_base_dir.startswith("root://")
outDir = f"{out_base_dir}/{sub_dir_clean}" if sub_dir_clean else out_base_dir
if not is_remote_out:
    try:
        os.makedirs(outDir, exist_ok=True)
    except Exception as e:
        raise RuntimeError(f"Failed to create output directory: {outDir} ({e})")
outfile = f"{outDir}/d0ana_tree_stepMVA_{outputSuffix}.root"

if isMC:
    process = cms.Process("ANASKIM", eras.Run3_2023)
else:
    process = cms.Process("ANASKIM", eras.Run3_pp_on_PbPb_2023)
    if not process.isUsingModifier(pp_on_AA):
        raise RuntimeError("Expected pp_on_AA modifier from Run3_pp_on_PbPb_2023 era.")

process.load("Configuration.StandardSequences.Services_cff")
process.load("Configuration.StandardSequences.GeometryRecoDB_cff")
process.load("Configuration.StandardSequences.MagneticField_cff")
process.load("Configuration.StandardSequences.Reconstruction_Data_cff")

process.load("FWCore.MessageService.MessageLogger_cfi")
if isMC:
    process.MessageLogger.cerr.threshold = "INFO"
    process.MessageLogger.debugModules = ["*"]
    process.MessageLogger.cerr.FwkReport.reportEvery = 1
else:
    process.MessageLogger.cerr.FwkReport.reportEvery = 1000
    process.MessageLogger.cerr.DStarDebug = cms.untracked.PSet(
        limit=cms.untracked.int32(-1)
    )
    process.MessageLogger.cerr.D0DaughterOrder = cms.untracked.PSet(
        limit=cms.untracked.int32(-1)
    )
    process.MessageLogger.cerr.GenMatching = cms.untracked.PSet(
        limit=cms.untracked.int32(-1)
    )
    process.MessageLogger.cerr.DStarDecayFilter = cms.untracked.PSet(
        limit=cms.untracked.int32(-1)
    )
    process.MessageLogger.cerr.PATCompositeTreeProducer = cms.untracked.PSet(
        limit=cms.untracked.int32(-1)
    )
    process.MessageLogger.suppressInfo = cms.untracked.vstring("CentralityDebug")
    process.MessageLogger.cerr.CentralityDebug = cms.untracked.PSet(
        limit=cms.untracked.int32(0)
    )

process.options = cms.untracked.PSet(wantSummary=cms.untracked.bool(True))
process.FastTimerService = cms.Service(
    "FastTimerService",
    printEventSummary=cms.untracked.bool(True),
    printRunSummary=cms.untracked.bool(True),
    printJobSummary=cms.untracked.bool(True),
    enableDQM=cms.untracked.bool(False),
)

process.source = cms.Source(
    "PoolSource",
    fileNames=cms.untracked.vstring(str(fileToRun)),
)
process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(-1))
if not isMC:
    process.source.lumisToProcess = LumiList.LumiList(
        filename="/eos/user/c/cmsdqm/www/CAF/certification/Collisions23HI/Cert_Collisions2023HI_374288_375823_Golden.json"
    ).getVLuminosityBlockRange()

process.load("Configuration.StandardSequences.FrontierConditions_GlobalTag_cff")
process.GlobalTag.globaltag = cms.string(
    "132X_mcRun3_2023_realistic_HI_v9"
    if isMC
    else "132X_dataRun3_Prompt_v7"
)

process.load("RecoHI.HiCentralityAlgos.CentralityBin_cfi")
process.GlobalTag.snapshotTime = cms.string("9999-12-31 23:59:59.000")
process.GlobalTag.toGet.extend(
    [
        cms.PSet(
            record=cms.string("HeavyIonRcd"),
            tag=cms.string(
                "CentralityTable_HFtowers200_HydjetDrum5F_Run3v1302x04_Official_MC"
                if isMC
                else "CentralityTable_HFtowers200_DataPbPb_periHYDJETshape_Run3v1302x04_Nominal_Offline"
            ),
            connect=cms.string("frontier://FrontierProd/CMS_CONDITIONS"),
            label=cms.untracked.string("HFtowers"),
        )
    ]
)
process.cent_seq = cms.Sequence(process.centralityBin)

import HLTrigger.HLTfilters.hltHighLevel_cfi

process.hltFilter = HLTrigger.HLTfilters.hltHighLevel_cfi.hltHighLevel.clone()
process.hltFilter.andOr = cms.bool(True)
process.hltFilter.throw = cms.bool(False)
process.hltFilter.HLTPaths = [
    "HLT_HIMinimumBiasHF1AND_v*",
    "HLT_HIMinimumBiasHF1ANDZDC2nOR_v*",
    "HLT_HIMinimumBiasHF1ANDZDC1nOR_v*",
]

process.load("VertexCompositeAnalysis.VertexCompositeProducer.collisionEventSelection_cff")
process.load("VertexCompositeAnalysis.VertexCompositeProducer.hfCoincFilter_cff")
process.load("VertexCompositeAnalysis.VertexCompositeProducer.hffilter_cfi")
process.colEvtSel = cms.Sequence()

process.eventFilter_HM = cms.Sequence(process.hltFilter)
process.eventFilter_HM_step = cms.Path(process.eventFilter_HM)

from VertexCompositeAnalysis.VertexCompositeProducer.PATAlgos_cff import changeToMiniAOD

process.load("VertexCompositeAnalysis.VertexCompositeProducer.generalD0Candidates_cff")
process.generalD0CandidatesNew = process.generalD0Candidates.clone()
process.generalD0CandidatesNew.tkChi2Cut = cms.double(5)
process.generalD0CandidatesNew.tkNhitsCut = cms.int32(0)
process.generalD0CandidatesNew.tkPtErrCut = cms.double(0.1)
process.generalD0CandidatesNew.tkPtCut = cms.double(1.5)
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
process.generalD0CandidatesNew.vtxSignificance3DCut = cms.double(3.0)
process.generalD0CandidatesNew.d0MassCut = cms.double(0.14)
process.generalD0CandidatesNew.d0AbsYCut = cms.double(1.8)
process.generalD0CandidatesNew.dPtCut = cms.double(0.0)
process.generalD0CandidatesNew.useAnyMVA = cms.bool(True)
process.generalD0CandidatesNew.mvaCut = cms.double(-1)
process.generalD0CandidatesNew.input_names = cms.vstring("float_input")
process.generalD0CandidatesNew.output_names = cms.vstring("probabilities")
if isMC:
    process.generalD0CandidatesNew.onnxModelFileName = cms.string(
        "XGBoost_Model_OnlyNonPrompt_05Mar26_Centrality_pTerr_ptErr011_1.onnx"
    )
    process.generalD0CandidatesNew.onnxFeatureNames = cms.vstring(
        "pT",
        "y",
        "centrality",
        "VtxProb",
        "3DCosPointingAngle",
        "3DPointingAngle",
        "2DCosPointingAngle",
        "2DPointingAngle",
        "3DDecayLength",
        "3DDecayLengthSignificance",
        "2DDecayLength",
        "2DDecayLengthSignificance",
        "pTD1",
        "EtaD1",
        "pTerrD1",
        "pTD2",
        "EtaD2",
        "pTerrD2",
        "Trk3DDCA",
        "dEta_dau",
    )
else:
    process.generalD0CandidatesNew.onnxModelFileName = cms.string(
        "Xgboost_new_16Jan01.onnx"
    )
process.generalD0CandidatesNew.mPiKCutMin = cms.double(1.70)
process.generalD0CandidatesNew.mPiKCutMax = cms.double(2.00)

process.load("VertexCompositeAnalysis.VertexCompositeProducer.generalDStarCandidates_cff")
process.generalDStarCandidatesNew = process.generalDStarCandidates.clone()
process.generalDStarCandidatesNew.trkPtSumCut = cms.double(0.0)
process.generalDStarCandidatesNew.trkEtaDiffCut = cms.double(99.0)
process.generalDStarCandidatesNew.tkNhitsCut = cms.int32(0)
process.generalDStarCandidatesNew.tkPtErrCut = cms.double(0.1)
process.generalDStarCandidatesNew.tkPtCut = cms.double(0.3)
process.generalDStarCandidatesNew.tkChi2Cut = cms.double(5)
process.generalDStarCandidatesNew.VtxChiProbCut = cms.double(0.00)
process.generalDStarCandidatesNew.dauLongImpactSigCut = cms.double(0.0)
process.generalDStarCandidatesNew.dauTransImpactSigCut = cms.double(0.0)
process.generalDStarCandidatesNew.dPtCut = cms.double(4.5)

process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.d0selector_cff")
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.d0analyzer_tree_cff")
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.dStaranalyzer_tree_cff")
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.eventinfotree_cff")
process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.eventplaneanalyzer_cfi")
process.load("RecoHI.HiEvtPlaneAlgos.HiEvtPlane_cfi")
process.load("RecoHI.HiEvtPlaneAlgos.hiEvtPlaneFlat_cfi")
if isMC:
    process.load(
        "VertexCompositeAnalysis.VertexCompositeAnalyzer.gendstareventplaneanalyzer_miniAOD_cfi"
    )
else:
    process.load("VertexCompositeAnalysis.VertexCompositeAnalyzer.dStarselector_cfi")

process.TFileService = cms.Service("TFileService", fileName=cms.string(outfile))

process.generalDStarCandidatesNew.d0Collection = cms.InputTag("generalD0CandidatesNew:D0")
process.eventplane.VertexCompositeCollection = cms.untracked.InputTag(
    "generalDStarCandidatesNew:DStar"
)
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

process.d0candCountFilter = cms.EDFilter(
    "CandViewCountFilter",
    src=cms.InputTag("generalD0CandidatesNew", "D0"),
    minNumber=cms.uint32(1),
)

if isMC:
    process.d0ana_mc.GenParticleCollection = cms.untracked.InputTag("prunedGenParticles")
    process.d0ana_mc.useAnyMVA = cms.bool(True)
    process.d0ana_mc.multMin = cms.untracked.double(0)
    process.d0ana_mc.multMax = cms.untracked.double(100000)
    process.d0ana_mc.MVACollection = cms.InputTag("generalD0CandidatesNew:MVAValuesD0")

    process.d0ana_newreduced = process.d0ana_mc.clone()
    process.d0ana_newreduced.CompositeCollection = cms.untracked.InputTag(
        "generalD0CandidatesNew:D0"
    )

    process.dStarana_mc.GenParticleCollection = cms.untracked.InputTag("prunedGenParticles")
    process.dStarana_mc.useAnyMVA = cms.bool(False)
    process.dStarana_mc.doRecoNtuple = cms.untracked.bool(True)
    process.dStarana_mc.CompositeCollection = cms.untracked.InputTag(
        "generalDStarCandidatesNew:DStar"
    )
    process.dStarana_mc.MVACollection = cms.InputTag(
        "generalDStarCandidatesNew:MVAValuesNewDStar"
    )
    process.dStarana_mc.debugGenMatching = cms.untracked.bool(True)

    process.dStarAna_step = cms.Path(
        process.eventFilter_HM
        * process.generalD0CandidatesNew
        * process.generalDStarCandidatesNew
        * process.d0candCountFilter
        * process.hiEvtPlaneRecalc
        * process.hiEvtPlaneFlatRecalc
        * process.dStarana_mc
        * process.eventplane
        * process.genDstarEventPlaneMiniAOD
    )
else:
    process.d0ana = cms.EDAnalyzer(
        "PATCompositeTreeProducer6", **process.d0ana.parameters_()
    )
    process.dStarana = cms.EDAnalyzer(
        "PATCompositeTreeProducer6", **process.dStarana.parameters_()
    )

    process.d0ana.multMin = cms.untracked.double(0)
    process.d0ana.multMax = cms.untracked.double(100000)
    process.d0ana.useAnyMVA = cms.untracked.bool(True)
    process.d0ana.isCentrality = cms.untracked.bool(False)
    process.d0ana.MVACollection = cms.untracked.InputTag(
        "generalD0CandidatesNew:MVAValuesD0"
    )

    process.d0ana_newreduced = process.d0ana.clone()
    process.d0ana_newreduced.CompositeCollection = cms.untracked.InputTag(
        "generalD0CandidatesNew:D0"
    )

    process.dStarana.useAnyMVA = cms.untracked.bool(False)
    process.dStarana.isEventPlane = cms.untracked.bool(True)
    process.dStarana.isCentrality = cms.untracked.bool(True)
    process.dStarana.centralityBinLabel = cms.untracked.InputTag(
        "centralityBin", "HFtowers"
    )
    process.dStarana.centralitySrc = cms.untracked.InputTag("hiCentrality")
    process.dStarana.CompositeCollection = cms.untracked.InputTag(
        "generalDStarCandidatesNew:DStar"
    )
    process.dStarana.MVACollection = cms.untracked.InputTag(
        "generalDStarCandidatesNew:MVAValuesNewDStar"
    )
    process.dStarana.eventplaneSrc = cms.untracked.InputTag("hiEvtPlaneFlatRecalc")
    process.dStarana.eventplaneSrcRecalc = cms.untracked.InputTag("hiEvtPlaneFlatRecalc")

    process.dStarAna_step = cms.Path(
        process.eventFilter_HM
        * process.generalD0CandidatesNew
        * process.generalDStarCandidatesNew
        * process.hiEvtPlaneRecalc
        * process.hiEvtPlaneFlatRecalc
        * process.d0ana_newreduced
        * process.dStarana
        * process.eventplane
    )

process.eventinfoana.selectEvents = cms.untracked.string("dStarAna_step")
process.eventinfoana.triggerPathNames = cms.untracked.vstring(
    "HLT_HIMinimumBiasHF1AND_v*",
    "HLT_HIMinimumBiasHF1ANDZDC2nOR_v",
    "HLT_HIMinimumBiasHF1ANDZDC1nOR_v",
)
process.eventinfoana.eventFilterNames = cms.untracked.vstring(
    "Flag_colEvtSel",
    "Flag_hfCoincFilter",
    "Flag_primaryVertexFilter",
)
process.eventinfoana.triggerFilterNames = cms.untracked.vstring()
process.eventinfoana.stageL1Trigger = cms.uint32(2)
process.eventinfoana.isEventPlane = cms.bool(False)
if not isMC:
    process.eventinfoana.eventplaneSrc = cms.InputTag("hiEvtPlaneFlatRecalc")
process.pevt = cms.EndPath(process.eventinfoana)

process.centralityPath = cms.Path(process.cent_seq)
process.schedule = cms.Schedule(
    process.centralityPath,
    process.eventFilter_HM_step,
    process.dStarAna_step,
    process.pevt,
)

process.Flag_colEvtSel = cms.Path(process.eventFilter_HM * process.colEvtSel)
process.Flag_primaryVertexFilter = cms.Path(
    process.eventFilter_HM
    * process.primaryVertexFilter
    * process.clusterCompatibilityFilter
)
eventFilterPaths = [process.Flag_colEvtSel, process.Flag_primaryVertexFilter]
for event_filter_path in eventFilterPaths:
    process.schedule.insert(0, event_filter_path)

changeToMiniAOD(process)
process.options.numberOfThreads = 1 if isMC else 8

if isMC:
    process.MessageLogger.cerr.DStarDebug = cms.untracked.PSet(
        limit=cms.untracked.int32(0)
    )
    process.MessageLogger.cerr.D0DaughterOrder = cms.untracked.PSet(
        limit=cms.untracked.int32(0)
    )
    process.MessageLogger.cerr.GenMatching = cms.untracked.PSet(
        limit=cms.untracked.int32(0)
    )
    process.MessageLogger.cerr.DStarDecayFilter = cms.untracked.PSet(
        limit=cms.untracked.int32(0)
    )
    process.MessageLogger.cerr.PATCompositeTreeProducer = cms.untracked.PSet(
        limit=cms.untracked.int32(0)
    )
