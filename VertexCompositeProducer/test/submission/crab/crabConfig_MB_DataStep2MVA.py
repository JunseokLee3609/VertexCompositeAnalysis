from WMCore.Configuration import Configuration

config = Configuration()

config.section_("General")
#config.General.requestName = "Ntuplizer_test_AOD_Z_Run2018HI_Data"
#config.General.requestName = "DStarAna_Data_Step2MVA_HIPhysicsRawPrime0_7_wEvtplane_CMSSW_13_2_13_MVA0p9_18Jan26_v2"
#config.General.requestName = "DStarAna_Data_Step2MVA_HIPhysicsRawPrime8_15_wEvtplane_CMSSW_13_2_13_MVA0p9_09Mar26_v2"
config.General.requestName = "DStarAna_Data_Step2MVA_HIPhysicsRawPrime24_31_wEvtplane_CMSSW_13_2_13_MVA0p9_18Mar26_v1"
config.General.workArea = 'crab_projects'
config.General.transferLogs = True

config.section_("JobType")
config.JobType.allowUndistributedCMSSW = True
config.JobType.pluginName = "Analysis"
config.JobType.psetName = "../../production/pbpb2023/data/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py"
config.JobType.numCores = 1
config.JobType.maxMemoryMB = 3000         # request high memory machines.
#config.JobType.inputFiles=['CentralityTable_HFtowers200_DataPbPb_periHYDJETshape_run3v1302x04_offline_Nominal.db']
config.JobType.maxJobRuntimeMin = 2750    # request longer runtime, ~48 hours.

config.section_("Data")
#config.Data.inputDataset = '/HIDoubleMuon/HIRun2018A-04Apr2019-v1/AOD'
#config.Data.inputDataset = '/HIPhysicsRawPrime1/HIRun2023A-PromptReco-v2/MINIAOD'
#config.Data.userInputFiles = open('files2023MB8_15.txt').readlines()
#config.Data.userInputFiles = open('files2023MB16_23.txt').readlines()
config.Data.userInputFiles = open('../../files2023MB24_31.txt').readlines()
#config.Data.runRange = '374288-375823'

#config.Data.inputDataset = '/DStarKpipiPU/junseok-crab_RECO_MINIAOD_DStarKpipiPU_CMSSW_13_2_10_082724_v1-e7a893e470c0a14923ed410f031778e3/USER'
#config.Data.ignoreLocality = True
config.Data.inputDBS = 'global'
config.Data.unitsPerJob = 10
config.Data.splitting = 'FileBased'
#config.Data.runRange = '375513'
#config.Data.outLFNDirBase = '/store/group/phys_heavyions/junseok/DStarAna/Data/%s' % (config.General.requestName)
config.Data.outLFNDirBase = '/store/user/junseok/Run3_2023/Data/SkimMVA/%s' % (config.General.requestName)
config.Data.publication = False
config.Data.totalUnits = -1
#config.Data.lumiMask = '/eos/cms/store/group/phys_heavyions/soohwan/Cert_Collisions2023HI_374288_375823_Golden_RandomPartsFor100MTraining.json'
#config.Data.lumiMask = '/eos/user/c/cmsdqm/www/CAF/certification/Collisions23HI/Cert_Collisions2023HI_374288_375823_Golden.json'


config.section_('Site')
#config.Site.storageSite = 'T2_CH_CERN'
config.Site.storageSite = 'T3_KR_KNU'
#config.Site.storageSite = 'T2_KR_KISTI'
config.Site.whitelist = [ 'T2_US_*', 'T2_IT_*', 'T2_KR_*' ]
config.Site.ignoreGlobalBlacklist=True
