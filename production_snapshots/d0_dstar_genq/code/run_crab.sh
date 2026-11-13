#!/bin/bash
set -uo pipefail
python3 stage_reference.py
result=$?
if [ "$result" -eq 0 ]; then
  cmsRun -j FrameworkJobReport.xml run_cfg.py
  result=$?
fi
if [ "$result" -eq 0 ]; then
  python3 finalize_output.py
  result=$?
fi
if [ "$result" -ne 0 ]; then
  python3 - "$result" <<'PY'
import json,sys
from pathlib import Path
Path('summary.json').write_text(json.dumps({'status':'failed','wrapper_exit_code':int(sys.argv[1])})+'\n')
PY
fi
exit "$result"
