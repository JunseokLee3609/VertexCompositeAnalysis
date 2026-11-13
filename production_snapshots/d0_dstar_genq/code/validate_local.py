import ROOT,json,math,sys
from pathlib import Path
ROOT.gROOT.SetBatch(True)
def check(folder):
 p=Path(folder);s=json.loads((p/'summary.json').read_text())
 f=ROOT.TFile.Open(str(p/'d0_dstar_genq.root'))
 assert f and not f.IsZombie() and not f.TestBit(ROOT.TFile.kRecovered),'Bad ROOT output'
 trees={k:f.Get(k) for k in ['Event','GenD0','GenDstar','Summary','InputSummary','RunInfo']}
 assert all(trees.values()),'Missing output tree'
 e,g0,gs=trees['Event'],trees['GenD0'],trees['GenDstar']
 assert int(e.GetEntries())==s['events'] and int(g0.GetEntries())==s['gen_d0'] and int(gs.GetEntries())==s['gen_dstar']
 trees['Summary'].GetEntry(0);assert bool(trees['Summary'].complete)
 trees['InputSummary'].GetEntry(0);nInput=int(trees['InputSummary'].events);assert nInput==e.GetEntries(),'All input events must be archived'
 rows=set();passed=failed=an_passed=empty_gen=0;mi=None
 for x in e:
  key=(int(x.manifestIndex),int(x.eventRow));assert key not in rows;rows.add(key)
  if mi is None:mi=int(x.manifestIndex)
  assert mi==int(x.manifestIndex)
  assert math.isfinite(float(x.pthat)) and math.isfinite(float(x.generatorWeight))
  if x.passD0Count:
   passed+=1;assert x.originalEntry>=0 and x.nD0>0
  else:
   failed+=1;assert x.originalEntry==-1 and x.nD0==0 and x.nDstar==0
  assert bool(x.passBasic)==all(bool(getattr(x,n)) for n in ['passHLT','passPV','passCluster','passHF'])
  if x.anPassD0Count:
   an_passed+=1;assert x.anOriginalEntry>=0 and x.anND0>0
  else:assert x.anOriginalEntry==-1 and x.anND0==0 and x.anNDstar==0
  for side in range(3):
   for name,valid in [('trkAll','trkAllValid'),('trkVeto','trkVetoValid'),('hf','hfValid'),('anTrkAll','anTrkAllValid'),('anTrkVeto','anTrkVetoValid')]:
    v=[x.GetLeaf(name).GetValue(side*3+k) for k in range(3)];flag=bool(getattr(x,valid)[side])
    if flag:assert all(math.isfinite(float(v[k])) for k in range(3)) and v[2]>0
   if not x.passD0Count:
    assert x.nRemovedTracks[side]==0
    for k in range(3):
     a,b=float(x.GetLeaf('trkAll').GetValue(side*3+k)),float(x.GetLeaf('trkVeto').GetValue(side*3+k));assert a==b or math.isnan(a) and math.isnan(b)
   if not x.anPassD0Count:
    assert x.anNRemovedTracks[side]==0
    for k in range(3):
     a,b=float(x.GetLeaf('anTrkAll').GetValue(side*3+k)),float(x.GetLeaf('anTrkVeto').GetValue(side*3+k));assert a==b or math.isnan(a) and math.isnan(b)
 assert an_passed==s['an_count_passed']
 if not s['partial']:assert an_passed==s['an_original_events']
 assert passed==s['count_passed'] and failed==s['count_failed']
 if not s['partial']:assert passed==s['original_events']
 d0s={};gen_events=set()
 def physical(v):
  a=[float(v[i]) for i in range(4)]
  assert all(math.isfinite(z) for z in a) and a[0]>=0 and a[3]>0
  return a
 for g in g0:
  key=(int(g.manifestIndex),int(g.eventRow));assert key in rows
  assert g.collisionId==0 and abs(g.pdgId)==421 and abs(g.kaonPdgId)==321 and abs(g.pionPdgId)==211
  physical(g.p4);physical(g.kaon);physical(g.pion)
  link=(int(g.manifestIndex),int(g.rowId));assert link not in d0s
  d0s[link]=(key,int(g.kaonPdgId));gen_events.add(key)
 for g in gs:
  key=(int(g.manifestIndex),int(g.eventRow));assert key in rows
  assert g.collisionId==0 and abs(g.pdgId)==413
  physical(g.p4);physical(g.slowPion)
  link=(int(g.manifestIndex),int(g.d0Row));assert link in d0s and d0s[link][0]==key
  gen_events.add(key)
 assert len(gen_events)<=len(rows)
 report=dict(status='passed',input_events=nInput,events=int(e.GetEntries()),count_passed=passed,count_failed=failed,an_count_passed=an_passed,an_tracker_replay_differences=s['an_replay_differences'],events_without_retained_GEN=len(rows)-len(gen_events),gen_d0=int(g0.GetEntries()),gen_dstar=int(gs.GetEntries()),output_bytes=(p/'d0_dstar_genq.root').stat().st_size,partial=s['partial'],tracker_replay_differences=s['replay_differences'])
 (p/'readback.json').write_text(json.dumps(report,indent=2)+'\n')
 f.Close();return report
if __name__=='__main__':print(json.dumps(check(sys.argv[1]),indent=2))
