"""Align a GEN-only supplement to existing ntuple entries; append four branches atomically on request."""
from pathlib import Path
from array import array
import argparse,json,math,os,shutil,struct
import ROOT
ROOT.gROOT.SetBatch(True)
TREEPATHS=['d0ana_newreduced/PATCompositeNtuple','dStarana_mc/PATCompositeNtuple']
def ulp(a,b):
 def key(x):
  v=struct.unpack('I',struct.pack('f',float(x)))[0]
  return (~v)&0xffffffff if v&0x80000000 else v|0x80000000
 if not math.isfinite(float(a))or not math.isfinite(float(b)):return 1<<64
 return abs(key(a)-key(b))
def integer(x):return ord(x)if isinstance(x,str)else int(x)
def leaf(t,name,required=True):
 b=t.GetBranch(name)
 if not b:
  if required:raise ValueError('Missing branch '+name)
  return None
 return b.GetListOfLeaves().At(0)
def align(ntuple,supplement,destination,lfn,trees=TREEPATHS):
 dst=Path(destination)
 if dst.exists():raise FileExistsError(dst)
 dst.parent.mkdir(parents=True,exist_ok=True)
 f=ROOT.TFile.Open(str(supplement));source=ROOT.TFile.Open(str(ntuple))
 if not f or f.IsZombie()or not source or source.IsZombie():raise ValueError('Cannot open input')
 meta=f.Get('friend/inputLFN');g=f.Get('friend/Events')
 if not meta or meta.GetTitle()!=lfn or not g:raise ValueError('Supplement LFN/schema mismatch')
 full={};low={}
 for e in range(g.GetEntries()):
  g.GetEntry(e);r,l,v=int(g.run),int(g.lumi),int(g.event)
  if(r,l,v)in full:raise ValueError('Duplicate full event identity')
  full[r,l,v]=e;k=(r,l,v&0xffffffff)
  if k in low:low[k]=-1
  else:low[k]=e
 part=Path(str(dst)+'.partial')
 if part.exists():raise FileExistsError(part)
 out=ROOT.TFile(str(part),'CREATE');ROOT.TNamed('source_ntuple',str(ntuple)).Write();ROOT.TNamed('inputLFN',lfn).Write();report={'ntuple':str(ntuple),'supplement':str(supplement),'input_lfn':lfn,'trees':{}}
 for path in trees:
  t=source.Get(path)
  if not t:raise ValueError('Missing tree '+path)
  names={b.GetName()for b in t.GetListOfBranches()};needed=['RunNb','LSNb','EventNb','candSize','candSize_gen','matchGEN','matchGen_D0pT','matchGen_D0eta','matchGen_D0phi','gen_pT','gen_eta','gen_phi']
  t.SetBranchStatus('*',0)
  for n in needed+['matchHYDJET','genDca3D','genDca3DValid','gen_D0dca3D','gen_D0dca3DValid','matchGen_D0pdgId','gen_pdgId','gen_collisionId']:
   if n in names:t.SetBranchStatus(n,1)
  leaves={n:leaf(t,n)for n in needed};leaves.update({n:leaf(t,n,False)for n in ['matchHYDJET','genDca3D','genDca3DValid','gen_D0dca3D','gen_D0dca3DValid','matchGen_D0pdgId','gen_pdgId','gen_collisionId']})
  def v(n,i=0):return leaves[n].GetValue(i)
  d=out.mkdir(path.rsplit('/',1)[0]);d.cd();w=ROOT.TTree('GenDcaFriend','GEN DCA aligned to original candidate and GEN order')
  run=array('I',[0]);lumi=array('I',[0]);event=array('Q',[0]);entry=array('q',[0]);n=array('i',[0]);ng=array('i',[0])
  for name,a,kind in [('RunNb',run,'i'),('LSNb',lumi,'i'),('EventNb64',event,'l'),('source_entry',entry,'L'),('candSize',n,'I'),('candSize_gen',ng,'I')]:w.Branch(name,a,name+'/'+kind)
  fields={'genDca3D':ROOT.std.vector('float')(),'genDca3DValid':ROOT.std.vector('int')(),'gen_D0dca3D':ROOT.std.vector('float')(),'gen_D0dca3DValid':ROOT.std.vector('int')(),'matchedD0_pruned_index':ROOT.std.vector('int')(),'gen_pruned_index':ROOT.std.vector('int')()}
  for name,a in fields.items():w.Branch(name,a)
  counts={'events':0,'reco':0,'gen':0,'matched_reco':0,'valid_reco':0,'valid_gen':0,'native_value_comparisons':0}
  use_full=leaves['EventNb'].GetTypeName()in['ULong64_t','Long64_t']
  for e in range(t.GetEntries()):
   t.GetEntry(e);r,l,ev=int(v('RunNb')),int(v('LSNb')),int(leaves['EventNb'].GetValueLong64());key=(r,l,ev if use_full else ev&0xffffffff);ge=(full if use_full else low).get(key)
   if ge is None or ge<0:raise ValueError(('Missing or ambiguous source event',path,e,key))
   g.GetEntry(ge);run[0]=r;lumi[0]=l;event[0]=int(g.event);entry[0]=e;n[0]=int(v('candSize'));ng[0]=int(v('candSize_gen'))
   for a in fields.values():a.clear()
   def locate(prefix,i,pid):
    candidates=[]
    for k in range(len(g.prunedIndex)):
     if abs(int(g.pdgId[k]))!=pid:continue
     if all(ulp(getattr(g,coord)[k],v(prefix+coord,i))<=2 for coord in ['pT','eta','phi']):candidates.append(k)
    optional=prefix+'pdgId'
    if leaves.get(optional):candidates=[k for k in candidates if int(g.pdgId[k])==int(v(optional,i))]
    if len(candidates)!=1:raise ValueError(('GEN identity is not unique',path,e,prefix,i,candidates))
    return candidates[0]
   for kind,count,prefix,pid in [('reco',n[0],'matchGen_D0',421),('gen',ng[0],'gen_',413 if 'dStar' in path or 'dstar' in path.lower() else 421)]:
    for i in range(count):
     matched=kind=='gen'or bool(v('matchGEN',i))or bool(leaves['matchHYDJET']and v('matchHYDJET',i))
     if matched:k=locate(prefix,i,pid);value=float(g.dca3D[k]);valid=integer(g.valid[k]);index=int(g.prunedIndex[k])
     else:value=-99.;valid=0;index=-1
     name='genDca3D'if kind=='reco'else'gen_D0dca3D';fields[name].push_back(value);fields[name+'Valid'].push_back(valid);fields['matchedD0_pruned_index'if kind=='reco'else'gen_pruned_index'].push_back(index)
     counts['valid_'+kind]+=valid
     if kind=='reco':counts['matched_reco']+=matched
     if leaves[name]and leaves[name+'Valid']:
      if bool(v(name+'Valid',i))!=bool(valid)or (valid and ulp(v(name,i),value)!=0):raise ValueError(('Existing production GEN DCA mismatch',path,e,i))
      counts['native_value_comparisons']+=1
    counts[kind]+=count
   for field in ['genDca3D','genDca3DValid','gen_D0dca3D','gen_D0dca3DValid']:
    if leaves.get(field):
     for i,x in enumerate(fields[field]):
      if float(x)!=float(v(field,i)):raise ValueError(('Native production DCA disagreement',path,e,field,i,float(x),v(field,i)))
      counts['native_value_comparisons']+=1
   w.Fill();counts['events']+=1
  w.Write();report['trees'][path]=counts
 out.cd();ROOT.TNamed('alignment',json.dumps(report)).Write();out.Close();source.Close();f.Close();os.replace(part,dst)
 Path(str(dst)+'.json').write_text(json.dumps(report,indent=2)+'\n');return report

def replace(ntuple,aligned,trees=TREEPATHS):
 path=Path(ntuple);stage=path.with_name(path.name+'.gendca_new.root')
 if stage.exists():raise FileExistsError(stage)
 shutil.copyfile(path,stage);f=ROOT.TFile.Open(str(stage),'UPDATE');a=ROOT.TFile.Open(str(aligned));expected={}
 for treepath in trees:
  t=f.Get(treepath);s=a.Get(treepath.rsplit('/',1)[0]+'/GenDcaFriend')
  if not t or not s or t.GetEntries()!=s.GetEntries():raise ValueError('Entry mismatch during append')
  new=['genDca3D','genDca3DValid','gen_D0dca3D','gen_D0dca3DValid']
  if any(t.GetBranch(n)for n in new):raise ValueError('GEN DCA branches already exist; refusing to overwrite silently')
  expected[treepath]=set(b.GetName()for b in t.GetListOfBranches())|set(new)
  maxr=max(1,int(t.GetMaximum('candSize')));maxg=max(1,int(t.GetMaximum('candSize_gen')))
  buffers={n:array('b'if n.endswith('Valid')else'f',[0])*(maxg if n.startswith('gen_')else maxr)for n in new}
  branches={n:t.Branch(n,buffers[n],n+'['+('candSize_gen'if n.startswith('gen_')else'candSize')+']/'+('O'if n.endswith('Valid')else'F'))for n in new}
  for e in range(t.GetEntries()):
   t.GetEntry(e);s.GetEntry(e)
   for n in new:
    vals=getattr(s,n)
    for i,x in enumerate(vals):buffers[n][i]=x
    branches[n].Fill()
  f.GetDirectory(treepath.rsplit('/',1)[0]).cd();t.Write('',ROOT.TObject.kOverwrite)
 f.cd();ROOT.TNamed('genDcaFriendProvenance',a.Get('alignment').GetTitle()).Write('',ROOT.TObject.kOverwrite);f.Close();a.Close()
 check=ROOT.TFile.Open(str(stage));ref=ROOT.TFile.Open(str(aligned))
 for treepath,names in expected.items():
  t=check.Get(treepath);s=ref.Get(treepath.rsplit('/',1)[0]+'/GenDcaFriend')
  if set(b.GetName()for b in t.GetListOfBranches())!=names:raise ValueError('Original branch set changed')
  for e in range(t.GetEntries()):
   t.GetEntry(e);s.GetEntry(e)
   for n in new:
    l=leaf(t,n);values=getattr(s,n)
    if l.GetNdata()!=len(values):raise ValueError('Appended branch count mismatch')
    if any(l.GetValue(i)!=float(x)for i,x in enumerate(values)):raise ValueError('Appended GEN DCA readback mismatch')
 check.Close();ref.Close()
 with stage.open('rb')as h:os.fsync(h.fileno())
 os.replace(stage,path)
 return {'status':'replaced','ntuple':str(path),'aligned_friend':str(aligned)}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--ntuple',required=True);p.add_argument('--supplement',required=True);p.add_argument('--aligned-output',required=True);p.add_argument('--expected-lfn',required=True);p.add_argument('--trees',nargs='+',default=TREEPATHS);p.add_argument('--replace',action='store_true');o=p.parse_args()
 print(json.dumps(align(o.ntuple,o.supplement,o.aligned_output,o.expected_lfn,o.trees),indent=2))
 if o.replace:print(json.dumps(replace(o.ntuple,o.aligned_output,o.trees)))
