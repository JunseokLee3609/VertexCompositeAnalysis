from WMCore.Configuration import Configuration

config = Configuration()

config.section_("General")
config.General.requestName = "DStarDeltaMassHistRS_Data_HIPhysicsRawPrime0_8_Fiducial_MVACutM1_13May26_v1"
config.General.workArea = "crab_projects"
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
with open("../../files2023MB0_8.txt") as f:
    config.Data.userInputFiles = [line.split()[0] for line in f if line.strip()]
config.Data.inputDBS = "global"
config.Data.unitsPerJob = 6
config.Data.splitting = "FileBased"
config.Data.outLFNDirBase = "/store/user/junseok/Run3_2023/Data/DStarDeltaMassHist/%s" % (
    config.General.requestName
)
config.Data.publication = False
config.Data.totalUnits = -1
# Do not set lumiMask with userInputFiles: CRAB has no dataset run metadata
# and otherwise sees run 1 only, producing zero jobs after JSON filtering.

config.section_("Site")
config.Site.storageSite = "T3_KR_KNU"
config.Site.whitelist = ["T2_US_*", "T2_IT_*", "T2_KR_*"]
config.Site.ignoreGlobalBlacklist = True
