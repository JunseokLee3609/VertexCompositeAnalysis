import os

import FWCore.ParameterSet.Config as cms

_cfg = os.path.join(
    os.environ["CMSSW_BASE"],
    "src",
    "VertexCompositeAnalysis",
    "VertexCompositeProducer",
    "test",
    "production/pbpb2023/mc/PbPb2023_D0BothAndDStar_MB_cfg_mc_v2_Step2MVA.py",
)

with open(_cfg, "r") as _handle:
    exec(compile(_handle.read(), _cfg, "exec"))

_file_list = os.path.join(
    os.environ["CMSSW_BASE"],
    "src",
    "VertexCompositeAnalysis",
    "VertexCompositeProducer",
    "test",
    "files_prompt_nov30",
)

with open(_file_list, "r") as _handle:
    _files = []
    for _line in _handle:
        _path = _line.split()[0]
        if not _path:
            continue
        _files.append("root://xrootd-cms.infn.it//" + _path.lstrip("/"))
        if len(_files) >= 30:
            break

process.source.fileNames = cms.untracked.vstring(*_files)
process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(20000))
process.options.numberOfThreads = cms.untracked.uint32(16)
process.options.numberOfStreams = cms.untracked.uint32(16)
process.TFileService.fileName = cms.string(
    "VertexCompositeAnalysis/VertexCompositeProducer/test/dstar_genmatch_tail_ancestry_20k.root"
)

process.MessageLogger.cerr.PAT6GenMatchTail = cms.untracked.PSet(
    limit=cms.untracked.int32(-1)
)
process.dStarana_mc.debugDstarTailAncestry = cms.untracked.bool(True)
