from CRABClient.UserUtilities import config
config=config()
config.General.requestName='D0Friend_PythiaGEN_12Nov26_OfficialNonpromptPthat5Pt10_v4recovery'
config.General.workArea='/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_official_crab_rematching_20261112/crab_projects_v4recovery'
config.General.transferOutputs=True
config.General.transferLogs=True
config.JobType.pluginName='Analysis'
config.JobType.psetName='/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_official_crab_rematching_20261112/code_v4/friend_cfg.py'
config.JobType.pyCfgParams=['manifest=mapping_OfficialNonpromptPthat5Pt10.json', 'signalOnlyGenMatching=True']
config.JobType.inputFiles=['/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_official_crab_rematching_20261112/configuration_v4recovery/mapping_OfficialNonpromptPthat5Pt10.json']
config.JobType.scriptExe='/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_official_crab_rematching_20261112/code_v4/run_crab.sh'
config.JobType.outputFiles=['d0_matching_friend.root','summary.json']
config.JobType.maxMemoryMB=2500
config.JobType.maxJobRuntimeMin=120
config.JobType.numCores=1
config.Data.inputDataset='/nonpromptDStarToD0PiToKPiPi_pThat-5_pT-10_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v2/MINIAODSIM'
config.Data.inputDBS='global'
config.Data.userInputFiles=['/store/mc/HINPbPbSpring23MiniAOD/nonpromptDStarToD0PiToKPiPi_pThat-5_pT-10_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/2520000/60113fb3-a174-4610-9696-81151476bb54.root', '/store/mc/HINPbPbSpring23MiniAOD/nonpromptDStarToD0PiToKPiPi_pThat-5_pT-10_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/2820000/fb1740eb-1f28-4ef2-ad6c-7ba1ad671403.root', '/store/mc/HINPbPbSpring23MiniAOD/nonpromptDStarToD0PiToKPiPi_pThat-5_pT-10_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/90000/8f6cd331-ec2a-4082-a20b-3c654a5bd378.root', '/store/mc/HINPbPbSpring23MiniAOD/nonpromptDStarToD0PiToKPiPi_pThat-5_pT-10_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/90000/a636c921-8e85-4dc0-9f95-f20714456b4e.root']
config.Data.splitting='FileBased'
config.Data.unitsPerJob=1
config.Data.publication=False
config.Data.outputDatasetTag='D0Friend_PythiaGEN_12Nov26_OfficialNonpromptPthat5Pt10_v4recovery'
config.Data.outLFNDirBase='/store/user/junseok/D0Rematching/Official6_PythiaGEN_CRAB_20261112_v4recovery/OfficialNonpromptPthat5Pt10'
config.Site.storageSite='T3_CH_CERNBOX'
