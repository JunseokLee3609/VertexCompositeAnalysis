from CRABClient.UserUtilities import config
import json
config=config()
config.General.requestName='GenDcaFriend_17Nov26_OfficialPromptPthat10Pt20'
config.General.workArea='/eos/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117/crab_projects'
config.General.transferOutputs=True
config.General.transferLogs=True
config.JobType.pluginName='Analysis'
config.JobType.psetName='/afs/cern.ch/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117/code/gen_dca_friend_cfg.py'
config.JobType.scriptExe='/afs/cern.ch/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117/code/run_crab.sh'
config.JobType.inputFiles=['/afs/cern.ch/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117/code/validate_worker.py']
config.JobType.outputFiles=['gen_dca_friend.root','summary.json']
config.JobType.maxMemoryMB=2000
config.JobType.maxJobRuntimeMin=240
config.JobType.numCores=1
config.Data.inputDataset='/promptDStarToD0PiToKPiPi_pThat-10_pT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v2/MINIAODSIM'
config.Data.inputDBS='global'
config.Data.userInputFiles=[x['miniaod_lfn']for x in json.load(open('/afs/cern.ch/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117/configuration/mapping_OfficialPromptPthat10Pt20.json'))]
config.Data.splitting='FileBased'
config.Data.unitsPerJob=1
config.Data.publication=False
config.Data.outputDatasetTag='GenDcaFriend_17Nov26_OfficialPromptPthat10Pt20'
config.Data.outLFNDirBase='/store/user/junseok/GenDcaFriend/Official6_20261117/OfficialPromptPthat10Pt20'
config.Site.storageSite='T3_CH_CERNBOX'
