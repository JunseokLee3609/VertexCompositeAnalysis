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
    "dstar_delta_mass_hist_step2_fiducial_mvacutm1_d0ss.root"
)

process.generalD0CandidatesNewSS = process.generalD0CandidatesNew.clone(
    isWrongSign=cms.bool(True),
)
process.d0candCountFilterSS = process.d0candCountFilter.clone(
    src=cms.InputTag("generalD0CandidatesNewSS", "D0"),
)
process.generalDStarCandidatesNewD0SS = process.generalDStarCandidatesNew.clone(
    d0Collection=cms.InputTag("generalD0CandidatesNewSS:D0"),
    isWrongSign=cms.bool(False),
    useRawDStarKinematics=cms.bool(True),
)

process.dStaranaD0SS = process.dStarana.clone(
    CompositeCollection=cms.untracked.InputTag("generalDStarCandidatesNewD0SS:DStar"),
    MVACollection=cms.untracked.InputTag("generalDStarCandidatesNewD0SS:MVAValuesNewDStar"),
)

process.dStarMassHistD0SS_step = cms.Path(
    process.eventFilter_HM
    * process.generalD0CandidatesNewSS
    * process.d0candCountFilterSS
    * process.generalDStarCandidatesNewD0SS
    * process.dStaranaD0SS
)

process.schedule = cms.Schedule(
    process.Flag_colEvtSel,
    process.Flag_primaryVertexFilter,
    process.c,
    process.eventFilter_HM_step,
    process.dStarMassHistD0SS_step,
)
