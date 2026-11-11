#!/usr/bin/env python3
from pathlib import Path
import argparse,json,collections

ROOT=Path('/home/jun502s/DstarAna/DStarAnalysis/Fit/PbPb/Test/D0mesonspintest')
COMMIT='75af18a65a5a1c24aaaf722ab9d7fb1d5f6c8592'

def main():
    p=argparse.ArgumentParser()
    p.add_argument('study')
    a=p.parse_args()
    dest=Path(a.study)/'configuration'
    dest.mkdir(parents=True,exist_ok=True)
    campaign=ROOT/'output/campaign_26Oct31_d0_unit_weight_pt5_7_10_20_30'
    scope_path=campaign/'runtime/central_production/family_matched_mc/run_02/production_config.json'
    scope=json.loads(scope_path.read_text())
    audit=json.loads((ROOT/'docs/d0_downstream_audit.json').read_text())
    mapping_path=Path(audit['existing_official_event_weight_provenance']['source_jobs'])
    native={item['destination']:item for job in json.loads(mapping_path.read_text()) for item in job['inputs']}
    groups={}
    jobs=[]
    seen=set()
    for inp in scope['inputs']:
        group=inp['group']
        if group not in groups:
            groups[group]=(ROOT/'docs/inputs'/('%s.list'%group)).read_text().splitlines()
        raw=groups[group][inp['flat_index']]
        if raw in seen:
            raise RuntimeError('Duplicate source raw path')
        seen.add(raw)
        item=native[raw]
        if item['source_index']!=inp['source_index'] or item['original_job']!=inp['original_job']:
            raise RuntimeError('Raw/flat production mapping mismatch')
        index=len(jobs)
        relative='friends/'+group+'/d0ana_tree_step1_mc_%d.friend.root'%item['original_job']
        eos='/eos/user/j/junseok/analysis/matching_studies/d0_official_an_75af18a_20261111/'+relative
        jobs.append(dict(id=index,group=group,raw_path=raw,raw_url=item['source_url'],raw_bytes=Path(raw).stat().st_size,miniaod_lfn=item['miniaod_lfn'],source_index=inp['source_index'],source_file_index=inp['source_file_index'],original_job=inp['original_job'],matching_commit=COMMIT,output_url='root://eosuser.cern.ch/'+eos,output_path=eos))
    if len(jobs)!=5094:
        raise RuntimeError('Unexpected analysis scope size')
    summary=dict(files=len(jobs),raw_bytes=sum(x['raw_bytes'] for x in jobs),groups=dict(collections.Counter(x['group'] for x in jobs)),matching_commit=COMMIT,scope=str(scope_path),raw_to_miniaod_mapping=str(mapping_path),settings=dict(PID=421,twoLayerDecay=False,signalOnlyGenMatching=False,deltaR=0.03,maxGenDeltaPtRel=0.5,decayInGen=True),updates=['RECO matching flags and linked truth fields','GEN denominator and RECO-to-GEN indices'],preserve_raw=True)
    (dest/'jobs.json').write_text(json.dumps(jobs,indent=2)+'\n')
    (dest/'scope.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(summary,indent=2))
    for group in sorted(summary['groups']):
        print('PILOT_INDEX',group,next(x['id'] for x in jobs if x['group']==group))

if __name__=='__main__':
    main()
