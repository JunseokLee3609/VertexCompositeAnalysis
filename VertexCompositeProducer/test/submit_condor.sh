#!/bin/bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <mc|data> <file_list> [job_flavour]" >&2
  echo "Optional: set STAGE_DIR to copy needed files and submit from that directory (e.g. /afs/.../condor_stage)" >&2
  exit 1
fi

mode="$1"
list_in="$2"
flavour="${3:-tomorrow}"
stage_dir="${STAGE_DIR:-}"

# Resolve absolute list path before cd
if [[ "$list_in" = /* ]]; then
  list_abs="$list_in"
else
  list_abs="$(pwd)/$list_in"
fi

src_dir="$(cd "$(dirname "$0")" && pwd)"
# working directory may change if staging
script_dir="$src_dir"

# If staging is requested, copy minimal dependencies to STAGE_DIR first.
if [[ -n "$stage_dir" ]]; then
  mkdir -p "$stage_dir"
  cp -p "$list_abs" "$stage_dir"/
  list_abs="$stage_dir/$(basename "$list_abs")"
  cp -p "$src_dir"/runCondor_MC.sh "$src_dir"/runCondor_Data.sh "$src_dir"/myProxy "$stage_dir"/
  # mode-specific python cfg copied later after it is chosen
  script_dir="$stage_dir"
fi

cd "$script_dir"
mkdir -p logs

case "$mode" in
  mc|MC)
    executable="runCondor_MC.sh"
    py_script="PbPb2023_D0BothAndDStar_MB_cfg_mc_Step2MVA_Condor_v1.py"
    # Detect number of columns to optionally include subdir
    first_nf=$(awk 'NF>0{print NF; exit}' "$list_abs" 2>/dev/null || echo 0)
    if [[ "$first_nf" -ge 3 ]]; then
      queue_fields="inFName,idx,subdir"
      args_line="cmsRun \$(py_script) \$(inFName) \$(idx) \$(subdir)"
    else
      queue_fields="inFName,idx"
      args_line="cmsRun \$(py_script) \$(inFName) \$(idx)"
    fi
    ;;
  data|DATA)
    executable="runCondor_Data.sh"
    py_script="PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py"
    first_nf=$(awk 'NF>0{print NF; exit}' "$list_abs" 2>/dev/null || echo 0)
    if [[ "$first_nf" -ge 3 ]]; then
      queue_fields="inFName,idx,subdir"
      args_line="cmsRun \$(py_script) \$(inFName) \$(idx) \$(subdir)"
    else
      queue_fields="inFName,idx"
      args_line="cmsRun \$(py_script) \$(inFName) \$(idx)"
    fi
    ;;
  *)
    echo "Mode must be 'mc' or 'data'" >&2
    exit 2
    ;;
esac

# If staged, copy the chosen python cfg into the stage area now.
if [[ -n "$stage_dir" ]]; then
  cp -p "$src_dir/${py_script}" "$stage_dir"/
fi

# Create and submit the job description on the fly
condor_submit -terse - <<EOF
universe    = vanilla
executable  = ${executable}

py_script = ${py_script}

arguments   = ${args_line}
transfer_input_files = \
  	${py_script}, myProxy

#output      = /dev/null
#error       = /dev/null
#log         = /dev/null
+JobFlavour = "${flavour}"

should_transfer_files = YES
when_to_transfer_output = ON_EXIT

##### Per-job logs
output = logs/job_\$(ProcId).out
error  = logs/job_\$(ProcId).err
log    = logs/job_\$(ProcId).log
should_transfer_files   = YES

request_memory = 8 GB
request_cpus = 4
+SingularityImage = "/cvmfs/singularity.opensciencegrid.org/opensciencegrid/osgvo-el8:latest"

x509userproxy = myProxy
requirements = (OpSysAndVer =?= "AlmaLinux9")

queue ${queue_fields} from ${list_abs}
EOF

echo "Submitted ${mode} jobs from ${list_abs} with flavour ${flavour}"
