#!/bin/bash
set -euo pipefail
source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=el8_amd64_gcc11
export X509_USER_PROXY=/afs/cern.ch/user/j/junseok/private/cms-auth/x509up
export TMPDIR=/eos/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117/runtime
export XRD_REQUESTTIMEOUT=120 XRD_CONNECTIONWINDOW=45
code=/afs/cern.ch/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117/code
base=/eos/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117
cd "$base/cmssw/CMSSW_13_2_11/src"
eval "$(scram runtime -sh)"
lfn=/store/mc/HINPbPbSpring23MiniAOD/nonpromptDStarToD0PiToKPiPi_pThat-2_pT-4_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/120000/9e00bb04-5ba0-497f-a86e-3572d013ca65.root
mkdir -p "$base/validation/native120"
cd "$base/validation/native120"
cmsRun -j FrameworkJobReport.xml "$code/gen_dca_friend_cfg.py" "inputFiles=root://cms-xrd-global.cern.ch/$lfn" maxEvents=120 > cmsRun.log 2>&1
python3 "$code/validate_worker.py" gen_dca_friend.root FrameworkJobReport.xml > readback.log 2>&1
python3 "$code/align_gen_dca.py" --ntuple /eos/user/j/junseok/analysis/production_studies/gen_dca_20261116/refit-diag/reco_check_numEvent120.root --supplement gen_dca_friend.root --aligned-output aligned.root --expected-lfn "$lfn" --trees d0/PATCompositeNtuple dstar/PATCompositeNtuple > align.log 2>&1
mkdir -p "$base/validation/full_worker"
cd "$base/validation/full_worker"
cp "$code/run_crab.sh" "$code/validate_worker.py" .
python3 - "$code/gen_dca_friend_cfg.py" "$lfn" <<'PY'
from pathlib import Path
import sys
Path('PSet.py').write_text('import sys\nsys.argv='+repr(['gen_dca_friend_cfg.py','inputFiles=root://cms-xrd-global.cern.ch/'+sys.argv[2],'maxEvents=-1'])+'\nexec(compile(open('+repr(sys.argv[1])+').read(),'+repr(sys.argv[1])+',"exec"))\n')
PY
bash run_crab.sh > worker.log 2>&1
printf 'LOCAL_VALIDATION_COMPLETE\n'
