#!/bin/bash
set -euo pipefail
source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=el8_amd64_gcc11
export TMPDIR=/eos/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117/runtime
work=/eos/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117
code=/afs/cern.ch/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117/code
mkdir -p "$work/cmssw"
cd "$work/cmssw"
if [ ! -d CMSSW_13_2_11 ]; then scram project CMSSW CMSSW_13_2_11; fi
cd CMSSW_13_2_11/src
mkdir -p GenDca/Friend/plugins
cp "$code/GenDcaFriend.cc" "$code/BuildFile.xml" GenDca/Friend/plugins/
eval "$(scram runtime -sh)"
scram b -j4
