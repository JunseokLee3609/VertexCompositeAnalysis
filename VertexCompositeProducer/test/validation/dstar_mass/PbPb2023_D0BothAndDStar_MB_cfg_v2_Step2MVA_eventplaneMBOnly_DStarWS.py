import os

import FWCore.ParameterSet.Config as cms

_cfg = os.path.join(
    os.environ["CMSSW_BASE"],
    "src",
    "VertexCompositeAnalysis",
    "VertexCompositeProducer",
    "test",
    "validation/dstar_mass/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA_eventplaneMBOnly.py",
)

with open(_cfg, "r") as _handle:
    exec(compile(_handle.read(), _cfg, "exec"))

process.TFileService.fileName = cms.string(
    "dstar_delta_mass_hist_step2_fiducial_mvacutm1_dstarws.root"
)

process.generalDStarCandidatesNewWS = process.generalDStarCandidatesNew.clone(
    isWrongSign=cms.bool(True),
)

process.dStaranaWS = process.dStarana.clone(
    CompositeCollection=cms.untracked.InputTag("generalDStarCandidatesNewWS:DStar"),
    MVACollection=cms.untracked.InputTag("generalDStarCandidatesNewWS:MVAValuesNewDStar"),
)

process.dStarMassHistWS_step = cms.Path(
    process.eventFilter_HM
    * process.generalD0CandidatesNew
    * process.d0candCountFilter
    * process.generalDStarCandidatesNewWS
    * process.dStaranaWS
)

process.schedule = cms.Schedule(
    process.Flag_colEvtSel,
    process.Flag_primaryVertexFilter,
    process.c,
    process.eventFilter_HM_step,
    process.dStarMassHistWS_step,
)
