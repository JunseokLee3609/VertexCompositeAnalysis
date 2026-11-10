import os
from pathlib import Path
from WMCore.Configuration import Configuration


def make_config(first_pd, last_pd):
    config_dir = Path(__file__).resolve().parent
    test_dir = config_dir.parents[1]
    output_user = os.environ["DSTAR_OUTPUT_USER"]
    group = f"{first_pd}_{last_pd}"
    request_name = f"DStarAna_Data_RawPrime{group}_MVA0p8_DIAG_NoD0Tuple_CMSSW13211_01Oct26_v1"
    input_list = config_dir / "input_lists" / "data_mva08_20260930" / f"files2023MB{group}.txt"

    config = Configuration()
    config.section_("General")
    config.General.requestName = request_name
    config.General.workArea = str(config_dir / "crab_projects" / "data_mva08_nod0tuple_01Oct26")
    config.General.transferOutputs = True
    config.General.transferLogs = True

    config.section_("JobType")
    config.JobType.pluginName = "Analysis"
    config.JobType.allowUndistributedCMSSW = True
    config.JobType.psetName = str(test_dir / "production" / "pbpb2023" / "data" / "PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA_MVA0p8_30Sep26.py")
    config.JobType.numCores = 1
    config.JobType.maxMemoryMB = 3000
    config.JobType.maxJobRuntimeMin = 2750

    config.section_("Data")
    with input_list.open() as source:
        config.Data.userInputFiles = [line.strip() for line in source if line.strip()]
    config.Data.inputDBS = "global"
    config.Data.outputPrimaryDataset = f"HIPhysicsRawPrime{group}"
    config.Data.splitting = "FileBased"
    config.Data.unitsPerJob = 10
    config.Data.totalUnits = -1
    config.Data.publication = False
    config.Data.outLFNDirBase = f"/store/user/{output_user}/Run3_2023/Data/SkimMVA/{request_name}"

    config.section_("Site")
    config.Site.storageSite = "T3_KR_KNU"
    config.Site.whitelist = ["T2_US_*", "T2_IT_*", "T2_KR_*"]
    config.Site.ignoreGlobalBlacklist = True
    return config
