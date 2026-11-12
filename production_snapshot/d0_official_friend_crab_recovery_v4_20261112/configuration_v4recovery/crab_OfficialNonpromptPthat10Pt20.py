from CRABClient.UserUtilities import config
config=config()
config.General.requestName='D0Friend_PythiaGEN_12Nov26_OfficialNonpromptPthat10Pt20_v4recovery'
config.General.workArea='/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_official_crab_rematching_20261112/crab_projects_v4recovery'
config.General.transferOutputs=True
config.General.transferLogs=True
config.JobType.pluginName='Analysis'
config.JobType.psetName='/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_official_crab_rematching_20261112/code_v4/friend_cfg.py'
config.JobType.pyCfgParams=['manifest=mapping_OfficialNonpromptPthat10Pt20.json', 'signalOnlyGenMatching=True']
config.JobType.inputFiles=['/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_official_crab_rematching_20261112/configuration_v4recovery/mapping_OfficialNonpromptPthat10Pt20.json']
config.JobType.scriptExe='/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_official_crab_rematching_20261112/code_v4/run_crab.sh'
config.JobType.outputFiles=['d0_matching_friend.root','summary.json']
config.JobType.maxMemoryMB=2500
config.JobType.maxJobRuntimeMin=120
config.JobType.numCores=1
config.Data.inputDataset='/nonpromptDStarToD0PiToKPiPi_pThat-10_pT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v2/MINIAODSIM'
config.Data.inputDBS='global'
config.Data.userInputFiles=['/store/mc/HINPbPbSpring23MiniAOD/nonpromptDStarToD0PiToKPiPi_pThat-10_pT-20_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/2550000/c141d577-959f-4f33-b3fb-b1a5fef6da24.root']
config.Data.splitting='FileBased'
config.Data.unitsPerJob=1
config.Data.publication=False
config.Data.outputDatasetTag='D0Friend_PythiaGEN_12Nov26_OfficialNonpromptPthat10Pt20_v4recovery'
config.Data.outLFNDirBase='/store/user/junseok/D0Rematching/Official6_PythiaGEN_CRAB_20261112_v4recovery/OfficialNonpromptPthat10Pt20'
config.Site.storageSite='T3_CH_CERNBOX'
