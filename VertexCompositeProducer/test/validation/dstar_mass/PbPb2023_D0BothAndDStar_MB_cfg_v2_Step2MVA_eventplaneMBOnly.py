import os

import FWCore.ParameterSet.Config as cms

_base_cfg = os.path.join(
    os.environ["CMSSW_BASE"],
    "src",
    "VertexCompositeAnalysis",
    "VertexCompositeProducer",
    "test",
    "production/pbpb2023/data/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py",
)

with open(_base_cfg, "r") as _handle:
    exec(compile(_handle.read(), _base_cfg, "exec"))

process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(-1))
process.options.numberOfThreads = cms.untracked.uint32(1)
process.TFileService.fileName = cms.string(
    "dstar_delta_mass_hist_step2_fiducial_mvacutm1_rs.root"
)

process.d0ana_newreduced.doRecoNtuple = cms.untracked.bool(False)
process.d0ana_newreduced.doGenNtuple = cms.untracked.bool(False)
process.d0ana_newreduced.saveHistogram = cms.untracked.bool(False)

process.generalD0CandidatesNew.mvaCut = cms.double(-1.0)

process.dStarana.doRecoNtuple = cms.untracked.bool(False)
process.dStarana.doGenNtuple = cms.untracked.bool(False)
process.dStarana.doGenMatching = cms.untracked.bool(False)
process.dStarana.saveHistogram = cms.untracked.bool(True)
process.dStarana.isEventPlane = cms.untracked.bool(False)
process.dStarana.massHistPeak = cms.untracked.double(0.147)
process.dStarana.massHistWidth = cms.untracked.double(0.008)
process.dStarana.massHistBins = cms.untracked.int32(160)
process.dStarana.dstarMassHistPtBins = cms.untracked.vdouble(5, 7, 10, 20, 30, 50)
process.dStarana.dstarMassHistYBins = cms.untracked.vdouble(0, 0.3, 0.8)
process.dStarana.dstarMassHistHiBins = cms.untracked.vdouble(0, 20, 60, 100)
process.dStarana.dstarMassHistDcaBins = cms.untracked.vdouble(
    0.0000,
    0.0008,
    0.0016,
    0.0024,
    0.0040,
    0.0060,
    0.0085,
    0.0120,
    0.0200,
    0.0800,
)
process.dStarana.dstarMassHistMvaCuts = cms.untracked.vdouble(
    0,
    0.1,
    0.3,
    0.5,
    0.7,
    0.8,
    0.9,
    0.91,
    0.92,
    0.93,
    0.94,
    0.95,
    0.96,
    0.97,
    0.98,
    0.99,
    0.991,
    0.992,
    0.993,
    0.994,
    0.995,
    0.996,
    0.997,
    0.998,
    0.999,
)

process.dStarMassHistRS_step = cms.Path(
    process.eventFilter_HM
    * process.generalD0CandidatesNew
    * process.d0candCountFilter
    * process.generalDStarCandidatesNew
    * process.dStarana
)

process.schedule = cms.Schedule(
    process.Flag_colEvtSel,
    process.Flag_primaryVertexFilter,
    process.c,
    process.eventFilter_HM_step,
    process.dStarMassHistRS_step,
)
