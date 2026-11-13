#!/bin/bash
set -e
source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=el8_amd64_gcc11
export X509_USER_PROXY=/afs/cern.ch/user/j/junseok/private/cms-auth/x509up
cd /afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113/cmssw/CMSSW_13_2_11/src
eval "$(scram runtime -sh)"
source /cvmfs/cms.cern.ch/crab3/crab.sh
python3 /afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113/code/freeze_production.py
python3 /afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113/code/tag_production.py
python3 /afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113/code/submit_production.py
python3 /afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113/code/read_status.py
