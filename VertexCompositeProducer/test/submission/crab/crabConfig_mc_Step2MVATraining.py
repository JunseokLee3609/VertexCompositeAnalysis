from WMCore.Configuration import Configuration

config = Configuration()

config.section_("General")
#config.General.requestName = 'D0Ana_MCNonPromptD0Kpi_DpT20_CentralityTable_HFtowers200_HydjetDrum5F_CMSSW_13_2_11_04Mar26_v1'
#config.General.requestName = 'D0Ana_MCNonPromptD0Kpi_DpT20_CentralityTable_HFtowers200_HydjetDrum5F_CMSSW_13_2_11_04Mar26_v1'
#config.General.requestName = 'D0Ana_MCNonPromptD0Kpi_DpT20_CentralityTable_HFtowers200_HydjetDrum5F_CMSSW_13_2_11_04Mar26_v1'
#config.General.requestName = 'D0Ana_MCNonPromptD0Kpi_DpT20_CentralityTable_HFtowers200_HydjetDrum5F_CMSSW_13_2_11_04Mar26_v1'
#config.General.requestName = 'D0Ana_MCNonPromptD0Kpi_DpT20_CentralityTable_HFtowers200_HydjetDrum5F_CMSSW_13_2_11_04Mar26_v1'

config.General.requestName = 'D0Ana_MCNonPromptD0Kpi_DpT20_CentralityTable_HFtowers200_HydjetDrum5F_CMSSW_13_2_11_04Mar26_v1'
config.General.workArea = 'crab_projects'
config.General.transferOutputs = True
config.General.transferLogs = True

config.section_("JobType")
config.JobType.allowUndistributedCMSSW = True
config.JobType.pluginName = "Analysis"
config.JobType.psetName = "../../validation/mva/PbPb2023_D0BothAndDStar_MB_cfg_mc_v2_Step2MVATraining.py"
config.JobType.numCores = 1
#config.JobType.maxJobRuntimeMin = 2400
config.JobType.maxMemoryMB = 3000         # request high memory machines.
# config.JobType.inputFiles=['CentralityTable_HFtowers200_HydjetDrum5F_v1302x04_HYD2023_official.db']
config.JobType.maxJobRuntimeMin = 2750    # request longer runtime, ~48 hours.

config.section_("Data")
#config.Data.inputDataset = '/nonpromptD0ToKPi_PT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM'
#config.Data.inputDataset = '/nonpromptD0ToKPi_PT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM'
#config.Data.inputDataset = '/nonpromptD0ToKPi_PT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM'
config.Data.inputDataset = '/nonpromptD0ToKPi_PT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM'
#config.Data.inputDataset = '/nonpromptD0ToKPi_PT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM'
#config.Data.inputDataset = '/nonpromptD0ToKPi_PT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM'
#config.Data.inputDataset = '/nonpromptD0ToKPi_PT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM'
#config.Data.inputDataset = '/nonpromptD0ToKPi_PT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM'
#config.Data.inputDataset = '/nonpromptD0ToKPi_PT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM'
#config.Data.inputDataset = '/nonpromptD0ToKPi_PT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM'
#config.Data.inputDataset = '/nonpromptD0ToKPi_PT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM'
#config.Data.inputDataset = '/nonpromptD0ToKPi_PT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM'
#config.Data.inputDataset = '/nonpromptD0ToKPi_PT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM'
#config.Data.inputDataset = '/nonpromptD0ToKPi_PT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM'
#config.Data.inputDataset = '/nonpromptD0ToKPi_PT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM'
config.Data.ignoreLocality = False
config.Data.inputDBS = 'global'
#config.Data.inputDBS = 'global'
config.Data.unitsPerJob = 2
#config.Data.splitting = 'Automatic'
config.Data.splitting = 'FileBased'
config.Data.outLFNDirBase = '/store/user/junseok/DstarAnalysis/BDTTrainingSample/%s' % (config.General.requestName)
config.Data.publication = False
config.Data.totalUnits = -1
#config.Data.lumiMask = '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions18/HI/PromptReco/Cert_326381-327564_HI_PromptReco_Collisions18_JSON_HF_and_MuonPhys.txt'

config.section_('Site')
# config.Site.storageSite = 'T2_CH_CERN'
config.Site.storageSite = 'T3_KR_KNU'
