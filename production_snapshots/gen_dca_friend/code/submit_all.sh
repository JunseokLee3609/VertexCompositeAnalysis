#!/bin/bash
set -euo pipefail
source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=el8_amd64_gcc11
export X509_USER_PROXY=/afs/cern.ch/user/j/junseok/private/cms-auth/x509up
export TMPDIR=/eos/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117/runtime
code=/afs/cern.ch/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117/code
cd /eos/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117/cmssw/CMSSW_13_2_11/src
eval "$(scram runtime -sh)"
set +u
source /cvmfs/cms.cern.ch/crab3/crab.sh
set -u
python3 -B "$code/freeze_tag.py"
python3 -B "$code/submit_production.py"
python3 -B "$code/read_status.py"
