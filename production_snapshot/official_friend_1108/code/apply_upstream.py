from pathlib import Path
import os
for k in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS'):os.environ[k]='1'
import json,gzip,time,sys,stat,math
from array import array
import numpy as np,uproot,awkward as ak,ROOT
ROOT.gROOT.SetBatch(True)
if ROOT.IsImplicitMTEnabled():ROOT.DisableImplicitMT()
S=Path(__file__).resolve().parents[1];TREE='dStarana_mc/PATCompositeNtuple';EX=uproot.TrivialExecutor()
M={int(x['file_index']):x for x in json.loads((S/'configuration/input_manifest.json').read_text())}
META=json.loads((S/'configuration/metadata_columns.json').read_text())
TYPES={'Float_t':('f','F',np.dtype('float32')),'Double_t':('d','D',np.dtype('float64')),'Bool_t':('b','O',np.dtype('bool')),'Int_t':('i','I',np.dtype('int32')),'UInt_t':('I','i',np.dtype('uint32'))}
def op(p):return uproot.open(p,num_workers=1,decompression_executor=EX,interpretation_executor=EX)
def same(a,b):
 a=np.ascontiguousarray(a);b=np.ascontiguousarray(b)
 return a.shape==b.shape and a.dtype==b.dtype and np.array_equal(a.view('uint8'),b.view('uint8'))
def stat_record(p):
 z=p.stat();return dict(size=z.st_size,mtime_ns=z.st_mtime_ns,inode=z.st_ino)
def equal_arrays(a,b,description):
 fa,na,ba=ak.to_buffers(a);fb,nb,bb=ak.to_buffers(b)
 if na!=nb or fa!=fb or ba.keys()!=bb.keys():raise RuntimeError('Array layout differs: '+description)
 for k in ba:
  if not same(ba[k],bb[k]):raise RuntimeError('Nonmatching data differs: '+description+' '+k)
def run(fid):
 start=time.monotonic();meta=M[fid];original=Path(meta['local']);report=S/'validation'/f'file_{fid:06d}.json'
 protected=json.loads((S/'configuration/knu_protection.json').read_text())
 if not protected.get('KNU_original_unchanged_and_separate_from_GP'):raise RuntimeError('KNU protection not verified')
 gp_root=Path('/home/CMS/junseok/DStarAna/Data/Data/HIPhysicsRawPrime/newBDT_0311/MC/OfficialSample_0902')
 if not original.resolve().is_relative_to(gp_root) or original.is_symlink():raise RuntimeError('Replacement target is not a regular GP analysis copy')
 if report.exists():
  done=json.loads(report.read_text())
  if done.get('status')=='PASS' and stat_record(original)==done.get('replacement_stat'):
   done.update(status='replaced',post_stat=stat_record(original),pre_replacement_file_removed=True,temporary_file_removed=True);report.write_text(json.dumps(done,indent=2)+'\n')
  if done.get('status')=='replaced':
   if stat_record(original)!=done['post_stat']:raise RuntimeError('Replaced upstream changed after verification')
   return done
 prep=json.loads((S/'prepare'/f'file_{fid:06d}.json').read_text())
 if stat_record(original)!=prep['raw_stat']:raise RuntimeError('Original upstream changed since input extraction')
 result=json.load(gzip.open(S/'results'/f'file_{fid:06d}.json.gz','rt'));summary=result['summary'];cols={k:i for i,k in enumerate(result['columns'])}
 if summary['status']!='complete' or summary['file_index']!=fid or summary['source']!=meta['sample']:raise RuntimeError('Incorrect matching result identity')
 thread=summary['thread']
 if thread['ROOT_implicit_MT'] or thread['ROOT_thread_pool'] or any(v!='1' for v in thread['environment'].values()):raise RuntimeError('Matching worker is not single-threaded')
 rows=np.asarray(result['records'],dtype=np.float64)
 if rows.size==0:rows=np.empty((0,len(cols)),np.float64)
 n=prep['rows'];assert summary['all_candidates']==n and len(rows)==n
 ids=rows[:,cols['central_entry']].astype(np.int64);assert np.array_equal(np.sort(ids),np.arange(n))
 order=np.argsort(ids);rows=rows[order]
 # An unidentified daughter cannot change a rejection already proved by a
 # uniquely identified daughter with no eligible GEN match. Never guess its ID.
 unresolved=(rows[:,cols['missing_identity']]!=0)|(rows[:,cols['ambiguous_identity']]!=0)
 assert int(np.sum(rows[:,cols['missing_identity']]!=0))==summary['missing_identity']
 assert int(np.sum(rows[:,cols['ambiguous_identity']]!=0))==summary['ambiguous_identity']
 identity_proofs=[]
 for row_id in np.flatnonzero(unresolved):
  row=rows[row_id];known_bad=[]
  for leg in range(3):
   raw=int(row[cols[f'raw_track_index_{leg}']]);collection=int(row[cols[f'track_GEN_collection_{leg}']]);gen=int(row[cols[f'track_GEN_index_{leg}']])
   if raw>=0 and collection<0 and gen<0:known_bad.append((leg,raw))
  if not known_bad:raise RuntimeError('Unresolved track can affect matching outcome: row '+str(int(row_id)))
  if any(row[cols[k]]!=0 for k in ['matchGEN','matchHYDJET','isSwap']) or row[cols['metadata_index']]!=-1 or row[cols['matchGen_pruned_index']]!=-1 or row[cols['matchGen_index']]!=-1:
   raise RuntimeError('Invalid unmatched-outcome proof: row '+str(int(row_id)))
  leg,raw=known_bad[0]
  identity_proofs.append(dict(flat_candidate=int(row_id),source_entry=int(row[cols['source_entry']]),candidate_index=int(row[cols['candidate_index']]),known_unmatched_daughter=leg,known_raw_track_index=raw,proof='A uniquely identified daughter has no eligible nearest GEN hadron. Candidate cannot match regardless of the unidentified daughter.'))
 with op(original) as f:
  t=f[TREE];counts=t['candSize'].array(library='np').astype(np.int64);offsets=np.r_[0,np.cumsum(counts)]
  assert len(counts)==prep['all_events']==summary['all_events'] and int(offsets[-1])==n
  entries=rows[:,cols['source_entry']].astype(np.int64);ci=rows[:,cols['candidate_index']].astype(np.int64)
  assert np.all(entries>=0) and np.all(entries<len(counts)) and np.all(ci>=0) and np.all(ci<counts[entries])
  assert np.array_equal(offsets[entries]+ci,np.arange(n))
  kinematics=t.arrays(['eta','phi','EtaD2','PhiD2','matchGEN'],library='ak')
  old_matched=int(ak.sum(kinematics['matchGEN']))
 original_root=ROOT.TFile.Open(str(original),'READ')
 if not original_root or original_root.IsZombie():raise RuntimeError('Unreadable original ROOT')
 rt=original_root.Get(TREE)
 replaced={str(x.GetName()) for x in rt.GetListOfBranches() if str(x.GetName()).startswith('matchGen') or str(x.GetName()) in ('matchGEN','matchHYDJET','isSwap','idmom_reco','idBAnc_reco')}
 new_types={'matchHYDJET':'Bool_t','matchGen_index':'Int_t','matchGen_pruned_index':'Int_t','matchGen_collisionId':'Int_t','matchGen_reason':'Int_t','matchGen_deltaR':'Float_t'}
 replaced.update(new_types)
 dtypes={};leaflists={}
 for name in sorted(replaced):
  branch=rt.GetBranch(name)
  if branch:
   leaves=branch.GetListOfLeaves();assert leaves.GetEntries()==1
   kind=str(leaves.At(0).GetTypeName());leaflists[name]=str(branch.GetTitle())
  else:
   kind=new_types[name];leaflists[name]=f'{name}[candSize]/{TYPES[kind][1]}'
  if kind not in TYPES:raise RuntimeError('Unsupported original matching type '+name+' '+kind)
  dtypes[name]=TYPES[kind]
 expected={k:np.full(n,False if v[2].kind=='b' else -99,dtype=v[2]) for k,v in dtypes.items()}
 for name in ['matchGEN','matchHYDJET','isSwap','matchGen_index','matchGen_pruned_index','matchGen_reason']:
  key='reason' if name=='matchGen_reason' else name;expected[name][:]=rows[:,cols[key]].astype(expected[name].dtype)
 expected['matchGen_collisionId'].fill(-1)
 truth=np.asarray(result['metadata'],dtype=np.float64);mid=rows[:,cols['metadata_index']].astype(np.int64);matched=mid>=0
 assert np.array_equal(matched,expected['matchGEN']|expected['matchHYDJET'])
 if matched.any():
  assert truth.ndim==2 and truth.shape[1]==len(META) and np.all(mid[matched]<len(truth))
  for j,k in enumerate(META):expected[k][matched]=truth[mid[matched],j].astype(expected[k].dtype)
 for k,source in [('matchGen3DPointingAngle','matchGen_D1angle3D_'),('matchGen2DPointingAngle','matchGen_D1angle2D_'),('matchGen3DDecayLength','matchGen_D1decayLength3D_'),('matchGen2DDecayLength','matchGen_D1decayLength2D_')]:expected[k][:]=expected[source]
 for k,re,rf,ge,gf in [('matchGen_deltaR','eta','phi','matchGen_DStareta','matchGen_DStarphi'),('matchGen_slowPion_dR','EtaD2','PhiD2','matchGen_D1eta','matchGen_D1phi')]:
  eta=ak.to_numpy(ak.flatten(kinematics[re]));phi=ak.to_numpy(ak.flatten(kinematics[rf]));dp=(phi[matched]-expected[gf][matched]+np.pi)%(2*np.pi)-np.pi
  expected[k][matched]=np.hypot(eta[matched]-expected[ge][matched],dp)
 ref=np.load(S/'central_reference'/f'file_{fid:06d}.npz',allow_pickle=False);ix=offsets[ref['source_entry'].astype(np.int64)]+ref['candidate_index'].astype(np.int64)
 for name in ref.files:
  if name in ('source_file_index','source_entry','candidate_index'):continue
  wanted=ref[name];actual=expected[name][ix].astype(wanted.dtype)
  if not same(actual,wanted):
   bad=np.flatnonzero(actual!=wanted);raise RuntimeError('Central matching disagreement: '+name+' rows='+str(bad[:10].tolist()))
 central_rows=len(ix);ref.close()
 temporary=original.with_name(original.stem+'.matching-writing.root')
 if temporary.exists():temporary.unlink()
 out=ROOT.TFile.Open(str(temporary),'RECREATE')
 if not out or out.IsZombie():raise RuntimeError('Cannot write replacement ROOT')
 out.SetCompressionSettings(original_root.GetCompressionSettings())
 def copy_dir(src,dst,prefix=''):
  latest={}
  for key in src.GetListOfKeys():
   name=str(key.GetName())
   if name not in latest or key.GetCycle()>latest[name].GetCycle():latest[name]=key
  for name,key in latest.items():
   obj=key.ReadObj();path=prefix+name;dst.cd()
   if obj.InheritsFrom('TDirectory'):
    child=dst.mkdir(name,obj.GetTitle());copy_dir(obj,child,path+'/')
   elif obj.InheritsFrom('TTree'):
    obj.SetBranchStatus('*',1)
    if path==TREE:
     for k in replaced:
      if obj.GetBranch(k):obj.SetBranchStatus(k,0)
     clone=obj.CloneTree(-1,'fast');clone.SetDirectory(dst)
     counter=array('i',[0]);obj.SetBranchStatus('*',0);obj.SetBranchStatus('candSize',1);obj.SetBranchAddress('candSize',counter);clone.GetLeaf('candSize').SetAddress(counter)
     size=max(1,int(counts.max(initial=0)));buffers={k:array(v[0],[0])*size for k,v in dtypes.items()};branches={}
     for k,v in dtypes.items():branches[k]=clone.Branch(k,buffers[k],leaflists[k])
     for en in range(len(counts)):
      obj.GetEntry(en);assert counter[0]==int(counts[en]);lo=int(offsets[en]);hi=int(offsets[en+1])
      for k,buf in buffers.items():
       buf[:counter[0]]=array(dtypes[k][0],expected[k][lo:hi].astype('int8' if dtypes[k][0]=='b' else expected[k].dtype).tolist());branches[k].Fill()
     clone.SetEntries(len(counts));clone.Write('',ROOT.TObject.kOverwrite)
     ROOT.TNamed('matchingProvenance',json.dumps(dict(task='upstream_official_full_20261108',file_index=fid,source=meta['sample'],input_LFN=meta['input_LFN'],matching='independent original-track nearest hadron, deltaR<0.03, abs(dPt)/recoPt<0.5, exact common D0/Dstar',scope='all stored Dstar candidates',all_events=len(counts),all_candidates=n,matching_worker_threads=1))).Write()
    else:
     clone=obj.CloneTree(-1,'fast');clone.SetDirectory(dst);clone.Write('',ROOT.TObject.kOverwrite)
   else:obj.Write(name,ROOT.TObject.kOverwrite)
 copy_dir(original_root,out);out.Close();original_root.Close()
 fd=os.open(temporary,os.O_RDONLY);os.fsync(fd);os.close(fd)
 checked_branches=0;checked_trees=0;checked_histograms=0
 with op(original) as a,op(temporary) as b:
  ca={k.rsplit(';',1)[0]:v for k,v in a.classnames(recursive=True).items()};cb={k.rsplit(';',1)[0]:v for k,v in b.classnames(recursive=True).items()}
  for path,cls in ca.items():
   if cb.get(path)!=cls:raise RuntimeError('Missing or changed ROOT object '+path)
   if cls=='TTree':
    ta,tb=a[path],b[path];assert ta.num_entries==tb.num_entries
    old_names=ta.keys();assert set(old_names)<=set(tb.keys())
    unchanged=[k for k in old_names if path!=TREE or k not in replaced]
    for lo in range(0,ta.num_entries,1000):
     hi=min(lo+1000,ta.num_entries);left=ta.arrays(unchanged,entry_start=lo,entry_stop=hi,library='ak');right=tb.arrays(unchanged,entry_start=lo,entry_stop=hi,library='ak');equal_arrays(left,right,f'{path}:{lo}:{hi}')
    checked_branches+=len(unchanged);checked_trees+=1
   elif cls.startswith(('TH1','TH2','TH3','TProfile')):
    if not same(a[path].values(flow=True),b[path].values(flow=True)):raise RuntimeError('Histogram content changed '+path)
    if not same(a[path].variances(flow=True),b[path].variances(flow=True)):raise RuntimeError('Histogram variance changed '+path)
    if a[path].member('fTitle')!=b[path].member('fTitle'):raise RuntimeError('Histogram title changed '+path)
    checked_histograms+=1
  t=b[TREE]
  for k,wanted in expected.items():
   actual=t[k].array(library='ak')
   if not np.array_equal(ak.to_numpy(ak.num(actual)),counts):raise RuntimeError('Missing matching candidates '+k)
   values=ak.to_numpy(ak.flatten(actual))
   if not same(values,wanted):raise RuntimeError('Stored matching differs from computed result '+k)
 if stat_record(original)!=prep['raw_stat']:raise RuntimeError('Original changed while comparing')
 st=original.stat();os.chmod(temporary,stat.S_IMODE(st.st_mode));os.chown(temporary,-1,st.st_gid)
 receipt=dict(unresolved_track_identity_candidates=int(unresolved.sum()),unmatched_outcome_proofs=identity_proofs,status='PASS',file_index=fid,sample=meta['sample'],path=str(original),events=len(counts),candidates=n,old_matched=old_matched,new_matched=int(expected['matchGEN'].sum()),new_hydjet_matched=int(expected['matchHYDJET'].sum()),new_swapped=int(expected['isSwap'].sum()),central_rows_checked=central_rows,nonmatching_branches_checked=checked_branches,trees_checked=checked_trees,histograms_checked=checked_histograms,all_candidate_keys_covered=True,matching_values_equal=True,nonmatching_values_bitwise_equal=True,single_thread=True,pre_stat=prep['raw_stat'],replacement_stat=stat_record(temporary),seconds=time.monotonic()-start)
 report.write_text(json.dumps(receipt,indent=2)+'\n')
 os.replace(temporary,original)
 receipt.update(status='replaced',post_stat=stat_record(original),pre_replacement_file_removed=True,temporary_file_removed=not temporary.exists(),seconds=time.monotonic()-start)
 report.write_text(json.dumps(receipt,indent=2)+'\n');print('REPLACED',json.dumps(receipt),flush=True);return receipt
if __name__=='__main__':
 for arg in sys.argv[1:]:run(int(arg))
