from WMCore.Configuration import Configuration


config = Configuration()

request_name = "DStarAna_OfficialPromptDstar5p36TeV_PAT6_11Jul26_v1"

config.section_("General")
config.General.requestName = request_name
config.General.workArea = "crab_projects"
config.General.transferOutputs = True
config.General.transferLogs = True

config.section_("JobType")
config.JobType.pluginName = "Analysis"
config.JobType.psetName = "../../production/pbpb2023/mc/PbPb2023_D0BothAndDStar_MB_cfg_mc_v2_Step2MVA.py"
config.JobType.allowUndistributedCMSSW = True
config.JobType.numCores = 1
config.JobType.maxMemoryMB = 3000
config.JobType.maxJobRuntimeMin = 2750

config.section_("Data")
config.Data.inputDataset = (
    "/promptDStarToD0PiToKPiPi_pT-0_TuneCP5_5p36TeV_pythia8-evtgen/"
    "HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v2/"
    "MINIAODSIM"
)
config.Data.inputDBS = "global"
config.Data.splitting = "FileBased"
config.Data.unitsPerJob = 1
config.Data.totalUnits = -1
config.Data.ignoreLocality = False
config.Data.outLFNDirBase = "/store/user/junseok/DStarMC/%s" % request_name
config.Data.publication = False

config.section_("Site")
config.Site.storageSite = "T3_KR_KNU"
