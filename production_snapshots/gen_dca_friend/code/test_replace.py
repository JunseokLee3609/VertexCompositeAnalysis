from pathlib import Path
import ROOT,json,sys,math
from align_gen_dca import align,replace
P=Path('/eos/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117/validation/native120');original=Path('/eos/user/j/junseok/analysis/production_studies/gen_dca_20261116/refit-diag/reco_check_numEvent120.root');test=P/'without_dca.root';target=P/'without_dca_aligned.root';paths=['d0/PATCompositeNtuple','dstar/PATCompositeNtuple'];drop={'genDca3D','genDca3DValid','gen_D0dca3D','gen_D0dca3DValid'}
source=ROOT.TFile.Open(str(original));out=ROOT.TFile.Open(str(test),'CREATE')
def copy(src,dst):
 for key in src.GetListOfKeys():
  obj=key.ReadObj()
  if obj.InheritsFrom('TDirectory'):copy(obj,dst.mkdir(key.GetName()))
  elif obj.InheritsFrom('TTree'):
   dst.cd();obj.SetBranchStatus('*',1)
   for n in drop:
    if obj.GetBranch(n):obj.SetBranchStatus(n,0)
   clone=obj.CloneTree(-1,'fast');clone.Write();obj.SetBranchStatus('*',1)
  else:dst.cd();obj.Write()
copy(source,out);out.Close();source.Close()
f=ROOT.TFile.Open(str(P/'gen_dca_friend.root'));lfn=f.Get('friend/inputLFN').GetTitle();f.Close()
r=align(test,P/'gen_dca_friend.root',target,lfn,paths);replace(test,target,paths)
a=ROOT.TFile.Open(str(original));b=ROOT.TFile.Open(str(test));values=0
for path in paths:
 x=a.Get(path);y=b.Get(path);assert x.GetEntries()==y.GetEntries();assert set(v.GetName()for v in x.GetListOfBranches())==set(v.GetName()for v in y.GetListOfBranches())
 for e in range(x.GetEntries()):
  x.GetEntry(e);y.GetEntry(e)
  for branch in x.GetListOfBranches():
   name=branch.GetName();u=branch.GetListOfLeaves().At(0);v=y.GetBranch(name).GetListOfLeaves().At(0);assert u.GetNdata()==v.GetNdata()
   for i in range(u.GetNdata()):
    aa,bb=u.GetValue(i),v.GetValue(i);assert aa==bb or(math.isnan(aa)and math.isnan(bb)),(path,e,name,i,aa,bb);values+=1
r.update(status='passed',all_original_and_added_values_compared=values);(P/'replacement_test.json').write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r,indent=2))
