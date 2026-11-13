import sys,json
from pathlib import Path
from CRABAPI.RawCommand import crabCommand
r=crabCommand('status',dir=sys.argv[1])
keys=['status','dbStatus','dagStatus','jobsPerStatus','taskFailureMsg','taskWarningMsg']
out={k:r.get(k) for k in keys}
Path(sys.argv[2]).write_text(json.dumps(out,indent=2,default=str)+'\n')
