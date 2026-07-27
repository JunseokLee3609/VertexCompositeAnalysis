import os
import sys

import FWCore.ParameterSet.Config as cms


def _arg(index, default=""):
    py_idx = next((i for i, arg in enumerate(sys.argv) if arg.endswith(".py")), 1)
    pos = py_idx + index
    return sys.argv[pos] if len(sys.argv) > pos else default


def _as_input_file(path):
    if path.startswith("root://") or path.startswith("file:"):
        return path
    if path.startswith("/store/"):
        return "root://xrootd-cms.infn.it//" + path.lstrip("/")
    return path


_input_file = _arg(1)
_job_idx = _arg(2, "default")
_output_dir = _arg(
    3,
    "root://cluster142.knu.ac.kr//store/user/junseok/DstarAnalysis/dstar_genmatch_tail_rawtrack_20260524",
).rstrip("/")
_max_events_arg = _arg(4, "all")
_max_events = -1 if _max_events_arg.lower() in ("all", "full", "unlimited") else int(_max_events_arg)
_threads = int(_arg(5, "1"))
_streams = int(_arg(6, str(_threads)))

if not _input_file:
    raise RuntimeError("Missing input file argument: cmsRun cfg.py <inputFile> <jobIdx> [outputDir] [maxEvents] [threads] [streams]")

_base_cfg = os.path.join(
    os.environ["CMSSW_BASE"],
    "src",
    "VertexCompositeAnalysis",
    "VertexCompositeProducer",
    "test",
    "production/pbpb2023/mc/PbPb2023_D0BothAndDStar_MB_cfg_mc_v2_Step2MVA.py",
)

with open(_base_cfg, "r") as _handle:
    exec(compile(_handle.read(), _base_cfg, "exec"))

if not _output_dir.startswith("root://"):
    os.makedirs(_output_dir, exist_ok=True)

process.source.fileNames = cms.untracked.vstring(_as_input_file(_input_file))
process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(_max_events))
process.options.numberOfThreads = cms.untracked.uint32(_threads)
process.options.numberOfStreams = cms.untracked.uint32(_streams)
process.TFileService.fileName = cms.string(
    f"{_output_dir}/dstar_genmatch_tail_rawtrack_{_job_idx}.root"
)

process.MessageLogger.cerr.PAT6GenMatchTail = cms.untracked.PSet(
    limit=cms.untracked.int32(-1)
)

# Keep logs compact for production. The ntuple branches carry the candidate-level diagnosis.
process.dStarana_mc.debugDstarTailAncestry = cms.untracked.bool(False)
process.dStarana_mc.debugDstarTailMaxPrint = cms.untracked.int32(0)
