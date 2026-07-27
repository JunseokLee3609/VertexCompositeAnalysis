import os

import FWCore.ParameterSet.Config as cms

_base_cfg = os.path.join(
    os.environ["CMSSW_BASE"],
    "src",
    "VertexCompositeAnalysis",
    "VertexCompositeProducer",
    "test",
    "validation",
    "dstar_mass",
    "PbPb2023_DStarDeltaMDebug_MB_cfg.py",
)
exec(compile(open(_base_cfg).read(), _base_cfg, "exec"))

process.TFileService.fileName = cms.string(
    "VertexCompositeAnalysis/VertexCompositeProducer/test/dstar_delta_m_debug_abcd_overlapveto_10k.root"
)

process.generalDStarCandidatesDebugA.rejectDuplicateSlowPion = cms.bool(True)
process.generalDStarCandidatesDebugB.rejectDuplicateSlowPion = cms.bool(True)
process.generalDStarCandidatesDebugC.rejectDuplicateSlowPion = cms.bool(True)
process.generalDStarCandidatesDebugD.rejectDuplicateSlowPion = cms.bool(True)

process.generalDStarCandidatesDebugA.debugLabel = cms.string("DStarFitterDebug_A_overlapVeto")
process.generalDStarCandidatesDebugB.debugLabel = cms.string("DStarFitterDebug_B_overlapVeto")
process.generalDStarCandidatesDebugC.debugLabel = cms.string("DStarFitterDebug_C_overlapVeto")
process.generalDStarCandidatesDebugD.debugLabel = cms.string("DStarFitterDebug_D_overlapVeto")

process.dStarDeltaMDebug.rejectDuplicateTrack = cms.bool(True)
