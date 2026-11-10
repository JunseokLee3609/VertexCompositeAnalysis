#!/bin/bash
set -euo pipefail
STUDY_DIR=$PWD
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1
export SCRAM_ARCH=el8_amd64_gcc11
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd /cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src
eval "$(scram runtime -sh)"
cd "$STUDY_DIR"
python3 -u code/debug_identity_events.py
