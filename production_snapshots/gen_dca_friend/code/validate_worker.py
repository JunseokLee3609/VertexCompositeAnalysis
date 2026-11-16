from pathlib import Path
import json,sys,math,xml.etree.ElementTree as ET
import ROOT
ROOT.gROOT.SetBatch(True)
def integer(x):return ord(x)if isinstance(x,str)else int(x)
f=ROOT.TFile.Open(sys.argv[1]);assert f and not f.IsZombie(),'Cannot open GEN DCA output'
t=f.Get('friend/Events');assert t,'Missing GEN DCA Events tree'
assert f.Get('friend/schema').GetTitle()=='GenDcaSupplement_v1'
lfn=f.Get('friend/inputLFN').GetTitle();assert lfn.startswith('/store/'),'Missing source LFN'
rows=invalid=hydjet=0
for i in range(t.GetEntries()):
 t.GetEntry(i);assert int(t.sourceEntry)==i,'Non-sequential source entry'
 n=len(t.prunedIndex)
 for name in ['pdgId','charge','collisionId','d0Index','pT','eta','phi','mass','dca3D','valid']:assert len(getattr(t,name))==n,('Vector length mismatch',name,i)
 indices=list(t.prunedIndex);assert indices==sorted(set(indices)),('Duplicate or unsorted GEN index',i)
 for j in range(n):
  assert abs(int(t.pdgId[j]))in[421,413]
  if integer(t.valid[j]):assert math.isfinite(float(t.dca3D[j]))and float(t.dca3D[j])>=0,('Invalid accepted DCA',i,j)
  else:invalid+=1
  hydjet+=int(t.collisionId[j])>0
 rows+=n
n=t.GetEntries();f.Close()
if len(sys.argv)>2:
 report=ET.parse(sys.argv[2]).getroot();files=report.findall('InputFile');primary=[x for x in files if x.findtext('InputType')=='primaryFiles'];assert len(primary)==1,('Expected exactly one primary file',len(primary))
 read=int(primary[0].findtext('EventsRead'));assert read==n,('Missing input events',read,n)
s=json.loads(Path('summary.json').read_text());assert s['events']==n and s['rows']==rows and s['invalid_rows']==invalid
s.update(validation='passed',input_lfn=lfn,hydjet_rows=hydjet,bytes=Path(sys.argv[1]).stat().st_size)
Path('summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps(s))
