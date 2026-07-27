from CRABClient.UserUtilities import config


config = config()

request_name = "DStarDeltaMassHistRS_Data_RawPrime0_8_HistCuts_12Jul26_v1"

config.section_("General")
config.General.requestName = request_name
config.General.workArea = "crab_projects"
config.General.transferOutputs = True
config.General.transferLogs = True

config.section_("JobType")
config.JobType.allowUndistributedCMSSW = True
config.JobType.pluginName = "Analysis"
config.JobType.psetName = "../../validation/dstar_mass/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA_eventplaneMBOnly.py"
config.JobType.numCores = 1
config.JobType.maxMemoryMB = 3000
config.JobType.maxJobRuntimeMin = 2750
config.JobType.outputFiles = ["dstar_delta_mass_hist_step2_fiducial_mvacutm1_rs.root"]

config.section_("Data")
with open("../../files2023MB0_8.txt") as input_file:
    config.Data.userInputFiles = [line.split()[0] for line in input_file if line.strip()]
config.Data.inputDBS = "global"
config.Data.unitsPerJob = 6
config.Data.splitting = "FileBased"
config.Data.outLFNDirBase = "/store/user/junseok/Run3_2023/Data/DStarDeltaMassHist/%s" % request_name
config.Data.publication = False
config.Data.totalUnits = -1

config.section_("Site")
config.Site.storageSite = "T3_KR_KNU"
config.Site.whitelist = ["T2_US_*", "T2_IT_*", "T2_KR_*"]
config.Site.blacklist = ["T2_US_Nebraska"]
config.Site.ignoreGlobalBlacklist = False
