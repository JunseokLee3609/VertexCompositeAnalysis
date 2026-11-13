from CRABClient.UserUtilities import config
import json
config=config()
config.General.requestName='D0DstarGenQ_13Nov26_OfficialNonpromptPthat5Pt10'
config.General.workArea='/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113/crab_projects'
config.General.transferOutputs=True
config.General.transferLogs=True
config.JobType.pluginName='Analysis'
config.JobType.psetName='/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113/code/genq_cfg.py'
config.JobType.pyCfgParams=['manifest=mapping_OfficialNonpromptPthat5Pt10.json', 'allowPartial=False', 'signalOnlyGenMatching=True']
config.JobType.inputFiles=['/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113/configuration/mapping_OfficialNonpromptPthat5Pt10.json', '/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113/code/stage_reference.py', '/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113/code/finalize_output.py', '/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113/credentials/an_reference.token']
config.JobType.scriptExe='/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113/code/run_crab.sh'
config.JobType.outputFiles=['d0_dstar_genq.root', 'summary.json']
config.JobType.maxMemoryMB=3000
config.JobType.maxJobRuntimeMin=240
config.JobType.numCores=1
config.Data.inputDataset='/nonpromptDStarToD0PiToKPiPi_pThat-5_pT-10_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v2/MINIAODSIM'
config.Data.inputDBS='global'
config.Data.splitting='FileBased'
config.Data.unitsPerJob=1
config.Data.publication=False
config.Data.outputDatasetTag='D0DstarGenQ_13Nov26_OfficialNonpromptPthat5Pt10'
config.Data.outLFNDirBase='/store/user/junseok/GenQ/D0Dstar_20261113/OfficialNonpromptPthat5Pt10'
config.Site.storageSite='T3_CH_CERNBOX'
config.Data.userInputFiles=[r["miniaod_lfn"] for r in json.load(open('/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113/configuration/mapping_OfficialNonpromptPthat5Pt10.json'))]
