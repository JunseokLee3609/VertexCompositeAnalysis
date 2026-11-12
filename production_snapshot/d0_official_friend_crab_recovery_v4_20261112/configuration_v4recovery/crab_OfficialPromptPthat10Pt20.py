from CRABClient.UserUtilities import config
config=config()
config.General.requestName='D0Friend_PythiaGEN_12Nov26_OfficialPromptPthat10Pt20_v4recovery'
config.General.workArea='/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_official_crab_rematching_20261112/crab_projects_v4recovery'
config.General.transferOutputs=True
config.General.transferLogs=True
config.JobType.pluginName='Analysis'
config.JobType.psetName='/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_official_crab_rematching_20261112/code_v4/friend_cfg.py'
config.JobType.pyCfgParams=['manifest=mapping_OfficialPromptPthat10Pt20.json', 'signalOnlyGenMatching=True']
config.JobType.inputFiles=['/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_official_crab_rematching_20261112/configuration_v4recovery/mapping_OfficialPromptPthat10Pt20.json']
config.JobType.scriptExe='/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_official_crab_rematching_20261112/code_v4/run_crab.sh'
config.JobType.outputFiles=['d0_matching_friend.root','summary.json']
config.JobType.maxMemoryMB=2500
config.JobType.maxJobRuntimeMin=120
config.JobType.numCores=1
config.Data.inputDataset='/promptDStarToD0PiToKPiPi_pThat-10_pT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v2/MINIAODSIM'
config.Data.inputDBS='global'
config.Data.userInputFiles=['/store/mc/HINPbPbSpring23MiniAOD/promptDStarToD0PiToKPiPi_pThat-10_pT-20_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/2520000/c95a33ad-315b-4949-8923-90b57245e674.root', '/store/mc/HINPbPbSpring23MiniAOD/promptDStarToD0PiToKPiPi_pThat-10_pT-20_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/2520000/cffaffa9-3876-4c7d-bd50-822d99dc8cf3.root', '/store/mc/HINPbPbSpring23MiniAOD/promptDStarToD0PiToKPiPi_pThat-10_pT-20_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/2520000/d04ad6ef-32ba-484c-a8a3-6c67580c1a04.root', '/store/mc/HINPbPbSpring23MiniAOD/promptDStarToD0PiToKPiPi_pThat-10_pT-20_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/2520000/d195017b-a017-43af-9713-d878fa480218.root', '/store/mc/HINPbPbSpring23MiniAOD/promptDStarToD0PiToKPiPi_pThat-10_pT-20_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/2520000/d3086e6e-e824-4bba-a61d-43d4719d86f8.root', '/store/mc/HINPbPbSpring23MiniAOD/promptDStarToD0PiToKPiPi_pThat-10_pT-20_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/2550000/2f4366f8-010c-4524-83a6-52b95cf2465f.root', '/store/mc/HINPbPbSpring23MiniAOD/promptDStarToD0PiToKPiPi_pThat-10_pT-20_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/2550000/307f4551-b228-4cf3-8526-a6828aa29535.root', '/store/mc/HINPbPbSpring23MiniAOD/promptDStarToD0PiToKPiPi_pThat-10_pT-20_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/2550000/31c303a6-d456-446c-9a12-d0e45493f734.root', '/store/mc/HINPbPbSpring23MiniAOD/promptDStarToD0PiToKPiPi_pThat-10_pT-20_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/2550000/32b15ab7-0999-4ad6-9c7b-59ae4f523ae4.root', '/store/mc/HINPbPbSpring23MiniAOD/promptDStarToD0PiToKPiPi_pThat-10_pT-20_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/2550000/34326ad0-2563-459f-8020-6a5e4e49c6ab.root', '/store/mc/HINPbPbSpring23MiniAOD/promptDStarToD0PiToKPiPi_pThat-10_pT-20_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/2550000/370f59db-7c5d-49b0-aabd-20f15e04d1af.root', '/store/mc/HINPbPbSpring23MiniAOD/promptDStarToD0PiToKPiPi_pThat-10_pT-20_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/2550000/3b4bd696-25fb-4cf5-aada-72cf24b6aa56.root']
config.Data.splitting='FileBased'
config.Data.unitsPerJob=1
config.Data.publication=False
config.Data.outputDatasetTag='D0Friend_PythiaGEN_12Nov26_OfficialPromptPthat10Pt20_v4recovery'
config.Data.outLFNDirBase='/store/user/junseok/D0Rematching/Official6_PythiaGEN_CRAB_20261112_v4recovery/OfficialPromptPthat10Pt20'
config.Site.storageSite='T3_CH_CERNBOX'
