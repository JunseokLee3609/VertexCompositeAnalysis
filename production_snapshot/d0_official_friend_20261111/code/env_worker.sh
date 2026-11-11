#!/bin/bash
set -euo pipefail
WORKDIR=$PWD
export SCRAM_ARCH=el8_amd64_gcc11
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd /cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src
eval "$(scram runtime -sh)"
cd "$WORKDIR"
python3 run_file.py "$@"
