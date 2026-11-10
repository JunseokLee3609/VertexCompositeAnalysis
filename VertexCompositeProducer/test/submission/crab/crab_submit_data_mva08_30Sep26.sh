#!/bin/bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
group="$1"
export DSTAR_OUTPUT_USER="$2"
shift 2
crab submit -c "${script_dir}/crabConfig_MB_DataStep2MVA_RawPrime${group}_MVA0p8_30Sep26.py" "$@"
