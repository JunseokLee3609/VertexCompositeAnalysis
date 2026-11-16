#!/bin/bash
set -euo pipefail
source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=el8_amd64_gcc11
export TMPDIR=/eos/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117/runtime
base=/eos/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117
code=/afs/cern.ch/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117/code
cd "$base/cmssw/CMSSW_13_2_11/src"
eval "$(scram runtime -sh)"
python3 -B "$code/test_replace.py"
