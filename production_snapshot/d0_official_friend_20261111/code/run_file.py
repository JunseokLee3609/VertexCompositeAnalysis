#!/usr/bin/env python3
import os
for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS'):
    os.environ[key]='1'
import argparse,json,time,subprocess
from pathlib import Path

def stage(url,path):
    path=Path(path)
    subprocess.run(['xrdcp','--force','--retry','2','--streams','2',url,str(path)],check=True,timeout=1800)
    if not path.is_file() or path.stat().st_size==0:
        raise RuntimeError('Empty staged input: '+str(path))
    return str(path.resolve())

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('job')
    parser.add_argument('--max-events',type=int)
    parser.add_argument('--audit-tracks',action='store_true')
    parser.add_argument('--stream-miniaod',action='store_true')
    args=parser.parse_args()
    config=json.loads(Path(args.job).read_text())
    started=time.monotonic()
    raw=stage(config['raw_url'],'raw.root')
    lfn=config['miniaod_lfn']
    miniaod='root://cms-xrd-global.cern.ch/'+lfn
    if not args.stream_miniaod:
        miniaod='file:'+stage(miniaod,'miniaod.root')
    import ROOT
    from DataFormats.FWLite import Events,Handle
    ROOT.gROOT.SetBatch(True)
    ROOT.gSystem.Load('libFWCoreFWLite')
    ROOT.FWLiteEnabler.enable()
    ROOT.gSystem.Load('libDataFormatsPatCandidates')
    ROOT.gSystem.Load('libDataFormatsTrackReco')
    if ROOT.IsImplicitMTEnabled():
        ROOT.DisableImplicitMT()
    ROOT.gInterpreter.AddIncludePath(os.environ['CMSSW_RELEASE_BASE']+'/src')
    ROOT.gInterpreter.AddIncludePath(str(Path(__file__).resolve().parent))
    if not ROOT.gInterpreter.Declare((Path(__file__).resolve().parent/'D0Matching.hxx').read_text()):
        raise RuntimeError('D0 helper declaration failed')
    source=ROOT.d0friend.Input(raw)
    expected=int(source.entries)
    if args.max_events is not None:
        if args.max_events<=0:
            raise ValueError('max-events must be positive')
        expected=min(expected,args.max_events)
    config['execution']={'max_events':args.max_events,'audit_tracks':args.audit_tracks,'partial':expected<int(source.entries),'root':str(ROOT.gROOT.GetVersion()),'cmssw':os.environ['CMSSW_VERSION']}
    writer=ROOT.d0friend.Writer('friend.root',int(source.maximum),json.dumps(config,sort_keys=True))
    packed_reco=[Handle('std::vector<pat::PackedCandidate>') for _ in range(3)]
    vertices=Handle('std::vector<reco::Vertex>')
    beamspot=Handle('reco::BeamSpot')
    packed_gen=Handle('std::vector<pat::PackedGenParticle>')
    pruned=Handle('std::vector<reco::GenParticle>')
    seen=set()
    print('START',config['id'],expected,'of',int(source.entries),'stored events',flush=True)
    for ev in Events([miniaod]) if expected else []:
        a=ev.eventAuxiliary()
        event_id=int(a.event())
        entry=int(source.find(int(a.run()),int(a.luminosityBlock()),event_id))
        if entry<0 or entry>=expected:
            continue
        if entry in seen:
            raise RuntimeError('Repeated MiniAOD event aliases a stored event key')
        event=source.read(entry)
        for label,h in zip(('packedPFCandidates','lostTracks',('lostTracks','eleTracks')),packed_reco):
            ev.getByLabel(label,h)
            if not h.isValid():
                raise RuntimeError('Missing MiniAOD product '+str(label))
        for label,h in (('offlineSlimmedPrimaryVertices',vertices),('offlineBeamSpot',beamspot),('packedGenParticles',packed_gen),('prunedGenParticles',pruned)):
            ev.getByLabel(label,h)
            if not h.isValid():
                raise RuntimeError('Missing MiniAOD product '+label)
        writer.process(event,event_id,*[h.product() for h in packed_reco],vertices.product(),beamspot.product(),packed_gen.product(),pruned.product(),args.audit_tracks)
        seen.add(entry)
        if len(seen)%100==0:
            print('PROGRESS',len(seen),'/',expected,round(time.monotonic()-started,1),flush=True)
        if len(seen)==expected:
            break
    if len(seen)!=expected:
        raise RuntimeError('MiniAOD missing stored source events: '+str(sorted(set(range(expected))-seen)[:10]))
    summary={str(k):int(v) for k,v in writer.stats}
    summary.update(status='complete',id=config['id'],group=config['group'],raw_path=config['raw_path'],miniaod_lfn=lfn,matching_commit=config['matching_commit'],source_events=int(source.entries),partial=expected<int(source.entries),seconds=round(time.monotonic()-started,3))
    writer.close(expected,json.dumps(summary,sort_keys=True))
    summary['friend_bytes']=Path('friend.root').stat().st_size
    Path('summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print('SUMMARY',json.dumps(summary,sort_keys=True),flush=True)

if __name__=='__main__':
    main()
