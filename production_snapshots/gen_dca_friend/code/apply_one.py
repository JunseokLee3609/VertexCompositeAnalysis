from pathlib import Path
import json,sys
from align_gen_dca import align,replace
K=Path('/home/jun502s/DstarAna/DStarAnalysis/Fit/PbPb/Test/D0mesonspintest/output/campaign_26Nov12_d0_trackmatched_mc/study/gen_dca_friend_20261117')
a=json.loads((K/'application_manifest.json').read_text());assert a['complete']and a['ready']==5114
index=int(sys.argv[1]);j=a['rows'][index];idx=j['manifest_index'];supp=K/'supplements'/f'{idx:05d}.root';assert supp.stat().st_size==j['supplement_bytes']
out=K/'aligned'/j['group']/f'{idx:05d}.root';out.parent.mkdir(parents=True,exist_ok=True)
r=align(j['target_ntuple'],supp,out,j['miniaod_lfn'])
if len(sys.argv)>2 and sys.argv[2]=='replace':r['replacement']=replace(j['target_ntuple'],out)
receipt=K/'receipts'/f'{idx:05d}.json';receipt.parent.mkdir(exist_ok=True);receipt.write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r))
