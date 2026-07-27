#!/bin/bash

set -euo pipefail

if [ "$#" -gt 1 ]; then
    echo "Usage: $0 [request_tag]" >&2
    exit 1
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${script_dir}"

request_tag="${1:-18Mar26_v2}"
config_path="crabConfig_MB_DataStep2MVA_eventplaneMBOnly.py"

for pd in 0 1 2 3; do
    primary_dataset="HIMinimumBias${pd}"
    request_name="EventPlaneMB_Data_${primary_dataset}_CMSSW_13_2_11_${request_tag}"
    input_dataset="/${primary_dataset}/HIRun2023A-PromptReco-v2/MINIAOD"

    sed -i "s|^config.General.requestName = .*|config.General.requestName = \"${request_name}\"|" "${config_path}"
    sed -i "s|^config.Data.inputDataset = .*|config.Data.inputDataset = \"${input_dataset}\"|" "${config_path}"

    echo "Submitting ${primary_dataset}"
    echo "  requestName: ${request_name}"
    echo "  inputDataset: ${input_dataset}"

    crab submit -c "${config_path}"
done

sed -i "s|^config.General.requestName = .*|config.General.requestName = \"EventPlaneMB_Data_HIMinimumBias0_CMSSW_13_2_11_${request_tag}\"|" "${config_path}"
sed -i "s|^config.Data.inputDataset = .*|config.Data.inputDataset = \"/HIMinimumBias0/HIRun2023A-PromptReco-v2/MINIAOD\"|" "${config_path}"
