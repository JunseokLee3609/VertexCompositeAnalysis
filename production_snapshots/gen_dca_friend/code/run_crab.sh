#!/bin/bash
set -euo pipefail
cat > worker_cfg.py <<'PYCFG'
import FWCore.ParameterSet.Config as cms
from PSet import process
if len(process.source.fileNames)!=1: raise RuntimeError('One MiniAOD per GEN DCA friend job is required')
if process.maxEvents.input.value()!=-1: raise RuntimeError('Production must process the complete input file')
p=process.source.fileNames[0]
if '/store/' not in p: raise RuntimeError('Missing source LFN')
process.friend.inputLFN=cms.string(p[p.index('/store/'):])
PYCFG
cmsRun -j FrameworkJobReport.xml worker_cfg.py
python3 validate_worker.py gen_dca_friend.root FrameworkJobReport.xml
