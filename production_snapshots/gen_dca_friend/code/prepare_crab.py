from pathlib import Path
import json,collections
P=Path('/afs/cern.ch/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117');E=Path('/eos/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117');old=Path('/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113/configuration');jobs=json.loads((P/'configuration/all_jobs.json').read_text());assert len(jobs)==5114
by=collections.defaultdict(list)
for j in jobs:by[j['group']].append(j)
tasks=[]
for group,rows in sorted(by.items()):
 oldcfg=(old/f'crab_{group}.py').read_text();dataset=next(x.split('=',1)[1]for x in oldcfg.splitlines()if x.startswith('config.Data.inputDataset='))
 lfns=[r['miniaod_lfn']for r in rows];assert len(lfns)==len(set(lfns))
 mapping=P/'configuration'/f'mapping_{group}.json';mapping.write_text(json.dumps(rows,indent=2)+'\n')
 request='GenDcaFriend_17Nov26_'+group;cfg=P/'configuration'/f'crab_{group}.py'
 cfg.write_text(f'''from CRABClient.UserUtilities import config
import json
config=config()
config.General.requestName={request!r}
config.General.workArea={str(E/'crab_projects')!r}
config.General.transferOutputs=True
config.General.transferLogs=True
config.JobType.pluginName='Analysis'
config.JobType.psetName={str(P/'code/gen_dca_friend_cfg.py')!r}
config.JobType.scriptExe={str(P/'code/run_crab.sh')!r}
config.JobType.inputFiles={[str(P/'code/validate_worker.py')]!r}
config.JobType.outputFiles=['gen_dca_friend.root','summary.json']
config.JobType.maxMemoryMB=2000
config.JobType.maxJobRuntimeMin=240
config.JobType.numCores=1
config.Data.inputDataset={dataset}
config.Data.inputDBS='global'
config.Data.userInputFiles=[x['miniaod_lfn']for x in json.load(open({str(mapping)!r}))]
config.Data.splitting='FileBased'
config.Data.unitsPerJob=1
config.Data.publication=False
config.Data.outputDatasetTag={request!r}
config.Data.outLFNDirBase={'/store/user/junseok/GenDcaFriend/Official6_20261117/'+group!r}
config.Site.storageSite='T3_CH_CERNBOX'
''')
 tasks.append(dict(group=group,files=len(rows),request_name=request,configuration=str(cfg),project=str(E/'crab_projects'/('crab_'+request)),output_eos='/eos/user/j/junseok/GenDcaFriend/Official6_20261117/'+group))
(P/'configuration/tasks.json').write_text(json.dumps(tasks,indent=2)+'\n')
(P/'configuration/production_settings.json').write_text(json.dumps(dict(physics_base='baebad240f9764d2f3a3cbd27da909342e327c2a',mode='GEN-only D0/Dstar DCA supplement; PYTHIA and HYDJET with collision ID; no RECO, matching, event-plane or event filters rerun',cmssw='CMSSW_13_2_11',architecture='el8_amd64_gcc11',input_files=5114,samples=len(tasks),splitting='FileBased, one MiniAOD per job',identity='source LFN + run/lumi/event + original GEN float32 pT/eta/phi within 2 ULP, unique match required',output_fields=['genDca3D','genDca3DValid','gen_D0dca3D','gen_D0dca3DValid'],units='cm',crab_logs=str(E/'logs'),crab_projects=str(E/'crab_projects'),expected_eos_gb=[10,20],expected_new_afs_metadata_mb=15),indent=2)+'\n')
for p in [E/'crab_projects',E/'logs/crab',E/'runtime']:p.mkdir(parents=True,exist_ok=True)
print(json.dumps([{'group':t['group'],'files':t['files']}for t in tasks]))
