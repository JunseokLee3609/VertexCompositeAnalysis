#!/bin/bash
set -uo pipefail
python3 - <<'PY'
import json
from pathlib import Path
Path('summary.json').write_text(json.dumps({'status':'started'})+'\n')
PY
cat > run_cfg.py <<'PYCFG'
import FWCore.ParameterSet.Config as cms
from PSet import process
if len(process.source.fileNames)!=1:
    raise RuntimeError('Exactly one MiniAOD per friend job is required')
process.rematch.inputFile=cms.string(process.source.fileNames[0])
PYCFG
cmsRun -j FrameworkJobReport.xml run_cfg.py
result=$?
if [ "$result" -ne 0 ]; then
  python3 - "$result" <<'PY'
import json,sys
from pathlib import Path
Path('summary.json').write_text(json.dumps({'status':'failed','cmsRun_exit_code':int(sys.argv[1])})+'\n')
PY
fi
exit "$result"
