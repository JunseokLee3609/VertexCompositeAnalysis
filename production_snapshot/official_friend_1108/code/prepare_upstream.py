from pathlib import Path
import os
for k in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS'):os.environ[k]='1'
import json,gzip,time,argparse,concurrent.futures,multiprocessing
import numpy as np,uproot,awkward as ak
S=Path(__file__).resolve().parents[1]
M={int(x['file_index']):x for x in json.loads((S/'configuration/input_manifest.json').read_text())}
EX=uproot.TrivialExecutor()
COLS=['RunNb','LSNb','EventNb','candSize','flavor','massGrandD1','massGrandD2','gen_pT','gen_eta','gen_phi']
for stem in ('GrandD1','GrandD2','D2'):COLS+=['pTerr'+stem]
COLS+=['NHitGrandD1','NHitGrandD2','NHitD2','zDCASignificanceGrandDaugther1','zDCASignificanceGrandDaugther2','zDCASignificanceDaugther2','xyDCASignificanceGrandDaugther1','xyDCASignificanceGrandDaugther2','xyDCASignificanceDaugther2']
def work(fid):
 start=time.monotonic();meta=M[fid];name=f'file_{fid:06d}';out=S/'payloads'/(name+'.json.gz');receipt=S/'prepare'/(name+'.json')
 if out.exists() and receipt.exists():return json.loads(receipt.read_text())
 p=Path(meta['local']);stat=p.stat()
 if stat.st_size!=int(meta['bytes']):raise RuntimeError(f'Original input size changed: {fid}')
 with uproot.open(p,num_workers=1,decompression_executor=EX,interpretation_executor=EX) as f:
  tree=f['dStarana_mc/PATCompositeNtuple'];a=tree.arrays(COLS,library='ak');all_events=tree.num_entries
 counts=ak.to_numpy(a['candSize']).astype(np.int64);offsets=np.r_[0,np.cumsum(counts)];events=[]
 for en in np.flatnonzero(counts):
  r=ak.to_list(a[int(en)]);n=int(counts[en]);reco={k:[] for k in ['error','charge','hits','dzsig','dxysig','kaon_pos','central_entry','candidate_index']}
  for ci in range(n):
   reco['error'].extend([r['pTerrGrandD1'][ci],r['pTerrGrandD2'][ci],r['pTerrD2'][ci]])
   reco['charge'].extend([1,-1,int(r['flavor'][ci])])
   reco['hits'].extend(int(r[k][ci]) for k in ['NHitGrandD1','NHitGrandD2','NHitD2'])
   reco['dzsig'].extend(r[k][ci] for k in ['zDCASignificanceGrandDaugther1','zDCASignificanceGrandDaugther2','zDCASignificanceDaugther2'])
   reco['dxysig'].extend(r[k][ci] for k in ['xyDCASignificanceGrandDaugther1','xyDCASignificanceGrandDaugther2','xyDCASignificanceDaugther2'])
   reco['kaon_pos'].append(int(r['massGrandD1'][ci]>r['massGrandD2'][ci]));reco['central_entry'].append(int(offsets[en]+ci));reco['candidate_index'].append(ci)
  gen=dict(index=list(range(len(r['gen_pT']))),parent_pt=r['gen_pT'],parent_eta=r['gen_eta'],parent_phi=r['gen_phi'])
  events.append(dict(run=int(r['RunNb']),lumi=int(r['LSNb']),event=int(r['EventNb']),source_entry=int(en),gen=gen,reco=reco))
 keys=[(x['run'],x['lumi'],x['event']) for x in events]
 if len(keys)!=len(set(keys)):raise RuntimeError(f'Duplicate source event identities {fid}')
 data=dict(file_index=fid,source=meta['sample'],input_LFN=meta['input_LFN'],raw_ntuple=str(p),events=events,all_events=all_events,all_candidates=int(offsets[-1]))
 tmp=out.with_suffix('.writing')
 with gzip.open(tmp,'wt',compresslevel=1) as f:json.dump(data,f,separators=(',',':'))
 tmp.replace(out)
 summary=dict(status='prepared',file_index=fid,sample=meta['sample'],all_events=all_events,nonempty_events=len(events),rows=int(offsets[-1]),bytes=out.stat().st_size,raw_stat=dict(size=stat.st_size,mtime_ns=stat.st_mtime_ns,inode=stat.st_ino),seconds=time.monotonic()-start)
 receipt.write_text(json.dumps(summary,indent=2)+'\n');return summary
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--ids',nargs='*',type=int);p.add_argument('--workers',type=int,default=8);args=p.parse_args();chosen=sorted(M) if args.ids is None else args.ids
 start=time.monotonic();results=[]
 with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers,mp_context=multiprocessing.get_context('fork')) as ex:
  for r in ex.map(work,chosen):
   results.append(r)
   if len(results)%50==0 or len(results)==len(chosen):print('PREPARE',len(results),'/',len(chosen),'rows',sum(x['rows'] for x in results),'seconds',round(time.monotonic()-start,1),flush=True)
 summary=dict(status='prepared',files=len(results),events=sum(x['all_events'] for x in results),nonempty_events=sum(x['nonempty_events'] for x in results),rows=sum(x['rows'] for x in results),payload_bytes=sum(x['bytes'] for x in results),seconds=time.monotonic()-start)
 (S/'configuration'/('prepare_summary.json' if args.ids is None else 'prepare_pilot.json')).write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary),flush=True)
