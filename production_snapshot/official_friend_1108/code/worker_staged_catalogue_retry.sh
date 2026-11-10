#!/bin/bash
set -euo pipefail
FILEID=$1
EOSROOT=root://eosuser.cern.ch//eos/user/j/junseok/analysis/matching_studies/dstar_official_upstream_full_20261108
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1
export XRD_REQUESTTIMEOUT=120 XRD_CONNECTIONWINDOW=60
READY=0
for ATTEMPT in $(seq 1 20); do
 if xrdcp --silent -f "$EOSROOT/payloads/file_$FILEID.json.gz" input.json.gz; then READY=1; break; fi
 sleep 15
done
if [ "$READY" != 1 ]; then echo "INPUT_PAYLOAD_NOT_READY $FILEID" >&2; exit 10; fi
INPUT_LFN=$(python3 -c 'import gzip,json;print(json.load(gzip.open("input.json.gz","rt"))["input_LFN"])')
echo "STAGE_INPUT $INPUT_LFN"
python3 stage_miniaod_catalogue_retry.py input.json.gz input_miniaod.root "$2"
export DSTAR_LOCAL_INPUT="file:$PWD/input_miniaod.root"
/cvmfs/cms.cern.ch/common/cmssw-el8 --command-to-run "bash env_worker.sh input.json.gz result.json.gz"
xrdcp --silent -f result.json.gz "$EOSROOT/results/file_$FILEID.json.gz"
