from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone
import argparse, csv, gzip, json, os, time, traceback

S = Path(__file__).resolve().parents[1]
GP_ROOT = Path('/home/CMS/junseok/DStarAna/Data/Data/HIPhysicsRawPrime/newBDT_0311/MC/OfficialSample_0902')
VERSION = 'upstream_official_full_20261108'

def read_json(p):
    return json.loads(p.read_text())

def write_json(p, value):
    p.parent.mkdir(parents=True, exist_ok=True)
    temp = p.with_name(p.name + '.writing')
    temp.write_text(json.dumps(value, indent=2, ensure_ascii=False) + chr(10))
    temp.replace(p)

def stat_record(p):
    z = p.stat()
    return dict(size=z.st_size, mtime_ns=z.st_mtime_ns, inode=z.st_ino)

def worker_summary(p):
    with gzip.open(p, 'rt') as f:
        prefix = f.read(8192)
    marker = prefix.index('"summary":') + len('"summary":')
    result, _ = json.JSONDecoder().raw_decode(prefix, marker)
    return result

def audit():
    started = time.monotonic()
    manifest = read_json(S/'configuration/input_manifest.json')
    expected = {int(m['file_index']):m for m in manifest}
    assert len(manifest) == len(expected) == 5114
    assert set(expected) == set(range(5114))
    prep_total = read_json(S/'configuration/prepare_summary.json')
    central_total = read_json(S/'configuration/central_reference.json')
    protection = read_json(S/'configuration/knu_protection.json')
    progress = read_json(S/'configuration/progress.json')
    assert progress['status'] == 'complete'
    assert progress['replaced_files'] == progress['downloaded_results'] == progress['remote_results'] == 5114
    assert not read_json(S/'configuration/apply_failures.json')
    assert protection['KNU_original_unchanged_and_separate_from_GP']
    receipts = {int(p.stem[5:]):p for p in (S/'validation').glob('file_*.json')}
    results = {int(p.name[5:11]):p for p in (S/'results').glob('file_*.json.gz')}
    assert receipts.keys() == results.keys() == expected.keys()
    grouped = defaultdict(lambda:defaultdict(int))
    rows = []
    current_manifest = []
    for fid, m in sorted(expected.items()):
        r = read_json(receipts[fid])
        p = read_json(S/'prepare'/f'file_{fid:06d}.json')
        w = worker_summary(results[fid])
        assert r['status'] == 'replaced' and r['file_index'] == w['file_index'] == p['file_index'] == fid
        assert r['sample'] == w['source'] == p['sample'] == m['sample']
        assert r['events'] == p['all_events'] == w['all_events']
        assert r['candidates'] == p['rows'] == w['rows'] == w['all_candidates']
        assert p['nonempty_events'] == w['events'] == w['events_found']
        assert w['status'] == 'complete'
        proofs=r.get('unmatched_outcome_proofs',[])
        unresolved=r.get('unresolved_track_identity_candidates',0)
        assert len(proofs)==unresolved
        assert max(w['missing_identity'],w['ambiguous_identity'])<=unresolved<=w['missing_identity']+w['ambiguous_identity']
        for proof in proofs:
            assert 0<=proof['flat_candidate']<r['candidates'] and proof['known_raw_track_index']>=0 and proof['known_unmatched_daughter'] in [0,1,2]
        equivalence=w.get('identity_equivalence_repair',{})
        if equivalence:
            assert equivalence['thread']==1 and equivalence['candidates']>0
        assert not w['thread']['ROOT_implicit_MT'] and w['thread']['ROOT_thread_pool'] == 0
        assert all(v == '1' for v in w['thread']['environment'].values())
        assert r['new_matched'] == w['matched'] and r['new_hydjet_matched'] == w['hydjet_matched']
        assert r['new_swapped'] == w['swapped']
        for flag in ['all_candidate_keys_covered','matching_values_equal','nonmatching_values_bitwise_equal','single_thread','pre_replacement_file_removed','temporary_file_removed']:
            assert r[flag] is True, (fid, flag)
        assert r['nonmatching_branches_checked'] > 0 and r['trees_checked'] > 0 and r['histograms_checked'] > 0
        target = Path(m['local'])
        assert r['path'] == m['local'] and not target.is_symlink()
        assert target.resolve().is_relative_to(GP_ROOT)
        assert m['remote'].startswith('/store/user/junseok/DStarMC/')
        assert stat_record(target) == r['post_stat'] == r['replacement_stat']
        assert r['pre_stat'] == p['raw_stat'] and r['pre_stat']['inode'] != r['post_stat']['inode']
        assert not target.with_name(target.stem+'.matching-writing.root').exists()
        d = dict(file_index=fid, sample=m['sample'], events=r['events'], nonempty_events=p['nonempty_events'], candidates=r['candidates'], old_matched=r['old_matched'], new_matched=r['new_matched'], new_hydjet_matched=r['new_hydjet_matched'], new_swapped=r['new_swapped'], central_rows_checked=r['central_rows_checked'], unique_matched_GEN_Dstars=w['unique_matched_GEN_Dstars'], duplicate_candidates=w['duplicate_candidates'], unmatched_stored_GEN_indices=w['unmatched_stored_GEN_indices'], old_bytes=r['pre_stat']['size'], new_bytes=r['post_stat']['size'], unresolved_track_identity_candidates=unresolved, provably_unmatched_candidates=len(proofs), exact_state_equivalence_repaired_candidates=equivalence.get('candidates',0), status='replaced')
        rows.append(d)
        g = grouped[m['sample']]
        g['files'] += 1
        for k,v in d.items():
            if isinstance(v,int) and k != 'file_index':
                g[k] += v
        current_manifest.append(dict(file_index=fid, sample=m['sample'], local=m['local'], input_LFN=m['input_LFN'], matching_version=VERSION, gp_stat=r['post_stat'], events=r['events'], candidates=r['candidates'], original_knu=dict(url='root://cluster142.knu.ac.kr/'+m['remote'], bytes=m['bytes'], adler32=m['adler32'])))
    totals = {k:sum(g[k] for g in grouped.values()) for k in next(iter(grouped.values()))}
    assert set(grouped) == {'prompt_2_4','prompt_5_10','prompt_10_20','nonprompt_2_4','nonprompt_5_10','nonprompt_10_20'}
    assert totals['files'] == prep_total['files'] == 5114
    assert totals['events'] == prep_total['events'] == 43188377
    assert totals['nonempty_events'] == prep_total['nonempty_events'] == 29733490
    assert totals['candidates'] == prep_total['rows'] == 234519477
    assert totals['central_rows_checked'] == central_total['rows'] == 2921397
    out = S/'final'
    out.mkdir(exist_ok=True)
    with (out/'files.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
    sample_rows=[dict(sample=sample,**dict(values)) for sample,values in sorted(grouped.items())]
    with (out/'samples.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=list(sample_rows[0]));writer.writeheader();writer.writerows(sample_rows)
    report=dict(status='PASS',task=VERSION,finished_utc=datetime.now(timezone.utc).isoformat(),totals=totals,samples=sample_rows,all_expected_files_present=True,all_candidate_keys_covered=True,all_matching_values_equal=True,all_nonmatching_values_bitwise_equal=True,all_current_GP_files_equal_verified_replacements=True,all_pre_replacement_GP_files_removed=True,all_temporary_GP_files_removed=True,all_matching_workers_single_thread=True,all_matching_outcomes_determined=True,unresolved_track_identity_candidates=totals['unresolved_track_identity_candidates'],provably_unmatched_candidates=totals['provably_unmatched_candidates'],exact_state_equivalence_repaired_candidates=totals['exact_state_equivalence_repaired_candidates'],KNU_originals_modified=False,KNU_path_protection_evidence=str(S/'configuration/knu_protection.json'),input_manifest=str(S/'configuration/input_manifest.json'),current_GP_manifest=str(out/'gp_current_manifest.json'),scope='Dstar matching branches in all stored upstream candidates; no stitching, candidate selection, fitting or downstream flat rebuilding',seconds=time.monotonic()-started)
    write_json(out/'gp_current_manifest.json',current_manifest)
    write_json(out/'summary.json',report)
    print(json.dumps(report,ensure_ascii=False),flush=True)
    return report

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--watch',action='store_true');args=parser.parse_args()
    if args.watch:
        print('WAITING_FOR_ALL_5114_REPLACEMENTS',flush=True)
        while True:
            try:
                if read_json(S/'configuration/progress.json').get('status') == 'complete':
                    break
            except (OSError,json.JSONDecodeError):
                pass
            time.sleep(30)
    try:
        audit()
    except Exception:
        detail=traceback.format_exc()
        write_json(S/'final/failure.json',dict(status='FAIL',traceback=detail))
        print(detail,flush=True)
        raise
if __name__ == '__main__':
    main()
