from pathlib import Path
import runpy
import FWCore.ParameterSet.Config as cms

nominal_cfg = Path(__file__).resolve().with_name("PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py")
process = runpy.run_path(str(nominal_cfg))["process"]
process.generalD0CandidatesNew.mvaCut = cms.double(0.8)
