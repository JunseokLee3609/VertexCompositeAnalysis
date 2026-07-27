#!/bin/bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${script_dir}"

# Define the datasets and request names
datasets=(
#"/DStarKpipi_woEtaCut/junseok-crab_RECO_MINIAOD_DStarKpipiPU_genNoFil_CMSSW_13_2_10_T2VanderBilt_250125_v1-e7a893e470c0a14923ed410f031778e3/USER"
"/promptD0ToKPi_PT-0_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v2/MINIAODSIM"
"/promptD0ToKPi_PT-1_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v2/MINIAODSIM"
"/promptD0ToKPi_PT-8_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v2/MINIAODSIM"
"/promptD0ToKPi_PT-10_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM"
"/promptD0ToKPi_PT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM"
"/nonpromptD0ToKPi_PT-0_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM"
"/nonpromptD0ToKPi_PT-1_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM"
"/nonpromptD0ToKPi_PT-8_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM"
 "/nonpromptD0ToKPi_PT-10_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM"
 "/nonpromptD0ToKPi_PT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM"
#"/DStarKpipiPU/junseok-crab_RECO_DStarKKpiPU_nofilter_CMSSW_13_2_10_T2Vandbilt_250504_v1-276727576f776097878185e411c5c644/USER"
#"/DStarKpipiPU/junseok-crab_RECO_DStarpipipiPU_nofilter_CMSSW_13_2_10_T2Vandbilt_250504_v1-276727576f776097878185e411c5c644/USER"
)
requestNames=(
    #"D0Ana_MC_Step2MVATraining_D0Kpi_DpT_NonSwap_CMSSW_13_2_13_MVA_30Mar2025_v3"
    #"DStarAna_MCPromptDStarKpipi_DpT1_VeryLooseCut_SelectionStudy_CMSSW_13_2_11_250308_v1"
    #"DStarAna_MCPromptD0Kpi_DpT1_wMVA_CMSSW_13_2_11_250309_v1"
   "D0Ana_MCPromptD0Kpi_DpT0_CentralityTable_HFtowers200_HydjetDrum5F_CMSSW_13_2_11_04Mar26_v1"
   "D0Ana_MCPromptD0Kpi_DpT1_CentralityTable_HFtowers200_HydjetDrum5F_CMSSW_13_2_11_04Mar26_v1"
   "D0Ana_MCPromptD0Kpi_DpT8_CentralityTable_HFtowers200_HydjetDrum5F_CMSSW_13_2_11_04Mar26_v1"
   "D0Ana_MCPromptD0Kpi_DpT10_CentralityTable_HFtowers200_HydjetDrum5F_CMSSW_13_2_11_04Mar26_v1"
   "D0Ana_MCPromptD0Kpi_DpT20_CentralityTable_HFtowers200_HydjetDrum5F_CMSSW_13_2_11_04Mar26_v1"
   "D0Ana_MCNonPromptD0Kpi_DpT0_CentralityTable_HFtowers200_HydjetDrum5F_CMSSW_13_2_11_04Mar26_v1"
   "D0Ana_MCNonPromptD0Kpi_DpT1_CentralityTable_HFtowers200_HydjetDrum5F_CMSSW_13_2_11_04Mar26_v1"
   "D0Ana_MCNonPromptD0Kpi_DpT8_CentralityTable_HFtowers200_HydjetDrum5F_CMSSW_13_2_11_04Mar26_v1"
   "D0Ana_MCNonPromptD0Kpi_DpT10_CentralityTable_HFtowers200_HydjetDrum5F_CMSSW_13_2_11_04Mar26_v1"
   "D0Ana_MCNonPromptD0Kpi_DpT20_CentralityTable_HFtowers200_HydjetDrum5F_CMSSW_13_2_11_04Mar26_v1"
    #"DStarAna_MCPromptD0KK_wMVA_CMSSW_13_2_11_07May25_v1"
    #"DStarAna_MCPromptD0pipi_wMVA_CMSSW_13_2_11_07May25_v1"

)
inputDBSs=(
"global"
"global"
"global"
"global"
"global"
"global"
"global"
"global"
"global"
"global"
#"phys03"
#"phys03"
)

# Loop over the datasets and request names
for i in "${!datasets[@]}"; do
    dataset=${datasets[$i]}
    requestName=${requestNames[$i]}
    inputDBS=${inputDBSs[$i]}

    # Modify the crabConfig_mc.py file
    sed -i "s|config.Data.inputDataset = .*|config.Data.inputDataset = '${dataset}'|" crabConfig_mc_Step2MVATraining.py
    sed -i "s|config.General.requestName = .*|config.General.requestName = '${requestName}'|" crabConfig_mc_Step2MVATraining.py
    sed -i "s|config.Data.inputDBS = .*|config.Data.inputDBS = '${inputDBS}'|" crabConfig_mc_Step2MVATraining.py

    # Submit the configuration file
    crab submit -c crabConfig_mc_Step2MVATraining.py
    wait
    # break
done
