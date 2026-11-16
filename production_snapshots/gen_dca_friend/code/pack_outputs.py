from pathlib import Path
import json,sys,tarfile
P=Path('/eos/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117/application/manifest.json');a=json.loads(P.read_text());assert a['complete']and a['ready']==5114
start=int(sys.argv[1]);end=int(sys.argv[2]);rows=a['rows'][start:end]
with tarfile.open(fileobj=sys.stdout.buffer,mode='w|')as t:
 for row in rows:
  f=Path(row['supplement_eos']);assert f.stat().st_size==row['supplement_bytes'];t.add(f,arcname='supplements/%05d.root'%row['manifest_index'])
