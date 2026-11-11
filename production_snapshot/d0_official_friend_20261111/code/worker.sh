#!/bin/bash
set -euo pipefail
JOB=$1
shift
/cvmfs/cms.cern.ch/common/cmssw-el8 --command-to-run "bash env_worker.sh $JOB $*"
OUTPUT_ROOT=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["output_url"])' "$JOB")
xrdcp --force --retry 2 friend.root "$OUTPUT_ROOT"
xrdcp --force --retry 2 summary.json "${OUTPUT_ROOT%.root}.json"
