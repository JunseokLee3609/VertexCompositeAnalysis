#!/usr/bin/env python3
from pathlib import Path
import json,collections
ROOT=Path(__file__).resolve().parent.parent
target=ROOT/'configuration_v2'
target.mkdir(exist_ok=True)
jobs=json.loads((ROOT/'configuration/all_jobs.json').read_text())
groups=collections.defaultdict(list)
for row in jobs:groups[row['group']].append(row)
DATASETS={g:('/'+('nonprompt' if 'Nonprompt' in g else 'prompt')+'DStarToD0PiToKPiPi_pThat-'+('10_pT-20' if '10Pt20' in g else '5_pT-10' if '5Pt10' in g else '2_pT-4')+'_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v2/MINIAODSIM')for g in groups}
base='/store/user/junseok/D0Rematching/Official6_PythiaGEN_CRAB_20261112'
requests={}
for group,rows in sorted(groups.items()):
 mapping=target/('mapping_'+group+'.json')
 mapping.write_text(json.dumps(rows,indent=2)+'\n')
 request='D0Friend_PythiaGEN_12Nov26_'+group+'_v2'
 requests[group]=request
 config="\n".join([
  "from CRABClient.UserUtilities import config",
  "config=config()",
  "config.General.requestName="+repr(request),
  "config.General.workArea="+repr(str(ROOT/'crab_projects_v2')),
  "config.General.transferOutputs=True",
  "config.General.transferLogs=True",
  "config.JobType.pluginName='Analysis'",
  "config.JobType.psetName="+repr(str(ROOT/'code_v2/friend_cfg.py')),
  "config.JobType.pyCfgParams="+repr(['manifest='+mapping.name,'signalOnlyGenMatching=True']),
  "config.JobType.inputFiles="+repr([str(mapping)]),
  "config.JobType.scriptExe="+repr(str(ROOT/'code_v2/run_crab.sh')),
  "config.JobType.outputFiles=['d0_matching_friend.root','summary.json']",
  "config.JobType.maxMemoryMB=2500",
  "config.JobType.maxJobRuntimeMin=120",
  "config.JobType.numCores=1",
  "config.Data.inputDataset="+repr(DATASETS[group]),
  "config.Data.inputDBS='global'",
  "config.Data.userInputFiles="+repr([x['miniaod_lfn']for x in rows]),
  "config.Data.splitting='FileBased'",
  "config.Data.unitsPerJob=1",
  "config.Data.publication=False",
  "config.Data.outputDatasetTag="+repr(request),
  "config.Data.outLFNDirBase="+repr(base+'/'+group),
  "config.Site.storageSite='T3_CH_CERNBOX'",
  ""
 ])
 (target/('crab_'+group+'.py')).write_text(config)
summary=dict(total=len(jobs),preserved_completed=0,remaining=len(jobs),groups={g:len(rows)for g,rows in sorted(groups.items())},datasets=DATASETS,requests=requests,storage_site='T3_CH_CERNBOX',output_base=base,matching_reference_commit='75af18a65a5a1c24aaaf722ab9d7fb1d5f6c8592',signalOnlyGenMatching=True,identity_formula='D0Fitter_b109a27',reco_matching='Pythia and HYDJET classified independently of GEN storage',all_old_friends_preserved_but_not_adopted=True)
(target/'scope.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps({k:v for k,v in summary.items()if k!='datasets'}))
