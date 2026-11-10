from CRABClient.UserUtilities import config as make_config

config = make_config()
config.General.requestName = 'DStarOnly_ANMC_Charge4a83166_10Nov26_Private8MPromptPt4to15Y12_v1'
config.General.workArea = '/eos/user/j/junseok/analysis/dstar_production/private_mc_3m_5m_8m_anmc_charge4a83166_20261110/submission/crab_projects'
config.General.transferOutputs = True
config.General.transferLogs = True
config.JobType.pluginName = "Analysis"
config.JobType.psetName = '/eos/user/j/junseok/analysis/dstar_production/private_mc_3m_5m_8m_anmc_charge4a83166_20261110/configuration/crab_pset_prompt.py'
config.JobType.allowUndistributedCMSSW = True
config.JobType.numCores = 1
config.JobType.maxMemoryMB = 3000
config.JobType.maxJobRuntimeMin = 2750
config.JobType.inputFiles = ['/eos/user/j/junseok/analysis/dstar_production/private_mc_3m_5m_8m_anmc_charge4a83166_20261110/input_payload/src']
config.JobType.outputFiles = ["dstar_tree_mc.root"]
config.Data.inputDataset = '/DstarPromptPthat2Pt4to15Y12PrivateMC/junseok-FullChainPt4to15Y12Additional8M_20260926-8117ce608359f6260196c7541c15b96e/USER'
config.Data.inputDBS = "phys03"
config.Data.splitting = "FileBased"
config.Data.unitsPerJob = 1
config.Data.totalUnits = -1
config.Data.publication = False
config.Data.ignoreLocality = True
config.Data.outLFNDirBase = '/store/user/junseok/DStarMC/ANMC01Sep26_DStarOnly_Charge4a83166_10Nov26/Private8MPromptPt4to15Y12'
config.Site.storageSite = "T3_KR_KNU"
config.Site.blacklist = ["T2_US_Vanderbilt"]
config.Site.whitelist = ['T2_CH_CERN', 'T2_KR_KISTI', 'T3_KR_KNU']

config.JobType.scriptExe = '/eos/user/j/junseok/analysis/dstar_production/private_mc_3m_5m_8m_anmc_charge4a83166_20261110/code/run_with_model.sh'
