import ROOT,json,math
from array import array
from pathlib import Path
row=json.loads(Path('runtime_mapping.json').read_text())[0]
f=ROOT.TFile.Open('d0_dstar_genq.root','UPDATE')
if not f or f.IsZombie():raise RuntimeError('Cannot finalize reference provenance')
ROOT.TObjString(row['an_archive_url']).Write('an_reference_url',ROOT.TObject.kOverwrite)
ids={'manifestIndex':int(row['manifest_index']),'sourceIndex':int(row['source_index']),'originalJob':int(row['original_job'])}
for name in ['Summary','InputSummary','RunInfo']:
 t=f.Get(name)
 if not t:raise RuntimeError('Missing metadata tree '+name)
 buffers={k:array('i',[v]) for k,v in ids.items()}
 for k,v in buffers.items():
  if t.GetBranch(k):
   for i in range(t.GetEntries()):
    t.GetEntry(i)
    if int(getattr(t,k))!=ids[k]:raise RuntimeError('Metadata source identity mismatch')
  else:
   branch=t.Branch(k,v,k+'/I')
   for i in range(t.GetEntries()):branch.Fill()
 t.Write('',ROOT.TObject.kOverwrite)
t=f.Get('InputSummary');t.GetEntry(0)
exposure=dict(events=int(t.events),sumGeneratorWeight=float(t.sumGeneratorWeight),sumGeneratorWeight2=float(t.sumGeneratorWeight2))
f.Close()
p=Path('summary.json');s=json.loads(p.read_text())
s.update(ids);s['an_reference_url']=row['an_archive_url']
s['reference_bytes']=Path('an_reference.root').stat().st_size
s['input_exposure']=exposure
p.write_text(json.dumps(s)+'\n')
