from WMCore.Configuration import Configuration

config = Configuration()

config.section_("General")
config.General.requestName = "EventPlaneMB_Data_HIMinimumBias0_CMSSW_13_2_11_18Mar26_v3"
config.General.workArea = "crab_projects"
config.General.transferLogs = True

config.section_("JobType")
config.JobType.allowUndistributedCMSSW = True
config.JobType.pluginName = "Analysis"
config.JobType.psetName = "../../validation/dstar_mass/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA_eventplaneMBOnly.py"
config.JobType.numCores = 1
config.JobType.maxMemoryMB = 3000
config.JobType.maxJobRuntimeMin = 2750

config.section_("Data")
config.Data.inputDataset = "/HIMinimumBias0/HIRun2023A-PromptReco-v2/MINIAOD"
config.Data.inputDBS = "global"
config.Data.unitsPerJob = 10
config.Data.splitting = "FileBased"
config.Data.outLFNDirBase = "/store/user/junseok/Run3_2023/Data/EventPlaneMB/%s" % (
    config.General.requestName
)
config.Data.publication = False
config.Data.totalUnits = -1
# config.Data.runRange = "374288-375823"
config.Data.lumiMask = "/eos/user/c/cmsdqm/www/CAF/certification/Collisions23HI/Cert_Collisions2023HI_374288_375823_Golden.json"

config.section_("Site")
config.Site.storageSite = "T3_KR_KNU"
# Keep execution sites unrestricted here: the current HIMinimumBias0 blocks are
# not available on the previous T2-only whitelist, which caused SUBMITREFUSED.
# config.Site.whitelist = ["T2_US_*", "T2_IT_*", "T2_KR_*"]
config.Site.ignoreGlobalBlacklist = True
