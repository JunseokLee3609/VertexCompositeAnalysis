from pathlib import Path
import argparse,json,collections
P=Path('/afs/cern.ch/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117');E=Path('/eos/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117');OUT=Path('/eos/user/j/junseok/GenDcaFriend/Official6_20261117')
p=argparse.ArgumentParser();p.add_argument('--partial',action='store_true');o=p.parse_args()
jobs=json.loads((P/'configuration/all_jobs.json').read_text());by={x['miniaod_lfn']:x for x in jobs};assert len(by)==len(jobs)==5114
found={};counts=collections.Counter();total=0
for f in OUT.glob('**/summary_*.json'):
 s=json.loads(f.read_text())
 if s.get('validation')!='passed' or s.get('status')!='complete':raise ValueError(('Unvalidated output',str(f)))
 lfn=s['input_lfn'];j=by[lfn];suffix=f.stem[len('summary'):];root=f.with_name('gen_dca_friend'+suffix+'.root')
 if not root.exists()or root.stat().st_size!=s['bytes']:raise ValueError(('Incomplete ROOT output',str(root)))
 if lfn in found:raise ValueError(('Duplicate output LFN',lfn))
 row=dict(j);row.update(supplement_eos=str(root),supplement_bytes=s['bytes'],events=s['events'],target_ntuple=j['raw_path']);found[lfn]=row;counts[j['group']]+=1;total+=s['bytes']
rows=[found[x['miniaod_lfn']]for x in jobs if x['miniaod_lfn']in found]
state=dict(complete=len(rows)==len(jobs),expected=len(jobs),ready=len(rows),bytes=total,groups=dict(counts),rows=rows)
(E/'application').mkdir(exist_ok=True);dest=E/'application/manifest.json';dest.write_text(json.dumps(state,indent=2)+'\n')
print(json.dumps({k:v for k,v in state.items()if k!='rows'},indent=2))
if not state['complete']and not o.partial:raise SystemExit('Production is incomplete; replacement not enabled')
