#!/usr/bin/env python3
import argparse,json
from pathlib import Path
import numpy as np
import awkward as ak
import uproot

def need(condition,message):
    if not bool(condition):
        raise RuntimeError(message)

def flat(a):
    return ak.to_numpy(ak.flatten(a))

def main():
    p=argparse.ArgumentParser()
    p.add_argument('raw')
    p.add_argument('friend')
    p.add_argument('output')
    p.add_argument('--allow-partial',action='store_true')
    args=p.parse_args()
    with uproot.open(args.raw) as raw_file,uproot.open(args.friend) as friend_file:
        raw=raw_file['d0ana_newreduced/PATCompositeNtuple']
        friend=friend_file['d0ana_newreduced/PATCompositeNtuple']
        provenance=json.loads(str(friend_file['provenance']))
        summary=json.loads(str(friend_file['summary']))
        partial=provenance['execution']['partial']
        need(not partial or args.allow_partial,'Partial friend cannot be adopted for a complete raw file')
        need(friend.num_entries==summary['events'],'Friend entry count differs from recorded events')
        need(friend.num_entries==raw.num_entries or partial,'Incomplete friend')
        totals=dict(events=0,candidates=0,gen=0,matchGEN=0,matchHYDJET=0,matched_reco_gen_links=0,old_matchGEN=0)
        for start in range(0,friend.num_entries,1000):
            stop=min(start+1000,friend.num_entries)
            f=friend.arrays(entry_start=start,entry_stop=stop)
            r=raw.arrays(['RunNb','LSNb','EventNb','candSize','matchGEN'],entry_start=start,entry_stop=stop)
            for name in ('RunNb','LSNb','EventNb','candSize'):
                need(ak.all(f[name]==r[name]),'Raw/friend differ in '+name)
            need(np.array_equal(ak.to_numpy(f['source_entry']),np.arange(start,stop)),'Entry order changed')
            for name in friend.keys():
                if isinstance(friend[name].interpretation,uproot.interpretation.jagged.AsJagged):
                    n=f['candSize_gen'] if name.startswith('gen_') else f['candSize']
                    need(ak.all(ak.num(f[name])==n),'Wrong branch length: '+name)
            need(not ak.any(f['matchGEN']&f['matchHYDJET']),'PYTHIA/HYDJET overlap')
            need(ak.all(f['gen_validD0chain']),'Invalid GEN decay in selected denominator')
            need(ak.all(abs(f['gen_pdgId'])==421),'Non-D0 in GEN denominator')
            need(ak.all(abs(f['gen_D0Dau1_pdgId'])==321),'GEN first daughter is not kaon')
            need(ak.all(abs(f['gen_D0Dau2_pdgId'])==211),'GEN second daughter is not pion')
            for event in f:
                mask=event['matchGEN']|event['matchHYDJET']
                index=event['matchGen_index'][mask]
                need(ak.all((index>=0)&(index<event['candSize_gen'])),'Matched candidate lacks a new GEN index')
                if len(index):
                    need(ak.all(event['matchGen_pruned_index'][mask]==event['gen_pruned_index'][index]),'Pruned-index mismatch')
                    need(ak.all(event['matchGen_collisionId'][mask]==event['gen_collisionId'][index]),'GEN collision mismatch')
                    need(ak.all(event['matchGEN'][mask]==(event['gen_collisionId'][index]==0)),'PYTHIA origin mismatch')
                    for key in ('pT','eta','phi','mass','y','charge','pdgId'):
                        need(ak.all(event['matchGen_D0'+key][mask]==event['gen_'+key][index]),'RECO/GEN parent mismatch: '+key)
                    for leg in (1,2):
                        for key in ('pT','eta','phi','mass','y','charge','pdgId'):
                            name='D0Dau%d_%s'%(leg,key)
                            need(ak.all(event['matchGen_'+name][mask]==event['gen_'+name][index]),'RECO/GEN daughter mismatch: '+name)
                totals['matched_reco_gen_links']+=len(index)
            totals['events']+=len(f)
            totals['candidates']+=int(ak.sum(f['candSize']))
            totals['gen']+=int(ak.sum(f['candSize_gen']))
            totals['matchGEN']+=int(ak.sum(f['matchGEN']))
            totals['matchHYDJET']+=int(ak.sum(f['matchHYDJET']))
            totals['old_matchGEN']+=int(ak.sum(r['matchGEN']))
        for key,other in (('candidates','candidates'),('gen','gen_candidates'),('matchGEN','new_matchGEN'),('matchHYDJET','new_matchHYDJET'),('old_matchGEN','old_matchGEN')):
            need(totals[key]==summary[other],'Summary differs from ROOT: '+key)
        result=dict(status='passed',raw=args.raw,friend=args.friend,matching_commit=provenance['matching_commit'],partial=partial,totals=totals,worker=summary)
    target=Path(args.output)
    target.parent.mkdir(parents=True,exist_ok=True)
    target.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))

if __name__=='__main__':
    main()
