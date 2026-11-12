from CRABClient.UserUtilities import config
config=config()
config.General.requestName='D0Friend_PythiaGEN_12Nov26_OfficialNonpromptPthat2Pt4_v4recovery'
config.General.workArea='/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_official_crab_rematching_20261112/crab_projects_v4recovery'
config.General.transferOutputs=True
config.General.transferLogs=True
config.JobType.pluginName='Analysis'
config.JobType.psetName='/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_official_crab_rematching_20261112/code_v4/friend_cfg.py'
config.JobType.pyCfgParams=['manifest=mapping_OfficialNonpromptPthat2Pt4.json', 'signalOnlyGenMatching=True']
config.JobType.inputFiles=['/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_official_crab_rematching_20261112/configuration_v4recovery/mapping_OfficialNonpromptPthat2Pt4.json']
config.JobType.scriptExe='/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_official_crab_rematching_20261112/code_v4/run_crab.sh'
config.JobType.outputFiles=['d0_matching_friend.root','summary.json']
config.JobType.maxMemoryMB=2500
config.JobType.maxJobRuntimeMin=120
config.JobType.numCores=1
config.Data.inputDataset='/nonpromptDStarToD0PiToKPiPi_pThat-2_pT-4_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v2/MINIAODSIM'
config.Data.inputDBS='global'
config.Data.userInputFiles=['/store/mc/HINPbPbSpring23MiniAOD/nonpromptDStarToD0PiToKPiPi_pThat-2_pT-4_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/2820000/1454da55-9b93-43f1-be9a-ad552280fbaa.root', '/store/mc/HINPbPbSpring23MiniAOD/nonpromptDStarToD0PiToKPiPi_pThat-2_pT-4_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/2820000/d7bc27d2-d1a1-4616-9b3c-32a448fbe72a.root', '/store/mc/HINPbPbSpring23MiniAOD/nonpromptDStarToD0PiToKPiPi_pThat-2_pT-4_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/120000/7903d957-1fb8-4aca-9425-90e8bf603f0f.root']
config.Data.splitting='FileBased'
config.Data.unitsPerJob=1
config.Data.publication=False
config.Data.outputDatasetTag='D0Friend_PythiaGEN_12Nov26_OfficialNonpromptPthat2Pt4_v4recovery'
config.Data.outLFNDirBase='/store/user/junseok/D0Rematching/Official6_PythiaGEN_CRAB_20261112_v4recovery/OfficialNonpromptPthat2Pt4'
config.Site.storageSite='T3_CH_CERNBOX'
