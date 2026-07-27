#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
test_dir="$(cd "${script_dir}/../.." && pwd)"
outfile="${1:-${test_dir}/files_MC_all_merged_uniqueidx_04Mar26_v1.list}"

entries=(
  "/promptD0ToKPi_PT-0_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v2/MINIAODSIM|Prompt_dPt0"
  "/promptD0ToKPi_PT-1_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v2/MINIAODSIM|Prompt_dPt1"
  "/promptD0ToKPi_PT-8_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v2/MINIAODSIM|Prompt_dPt8"
  "/promptD0ToKPi_PT-10_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM|Prompt_dPt10"
  "/promptD0ToKPi_PT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM|Prompt_dPt20"
  "/nonpromptD0ToKPi_PT-0_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM|NonPrompt_dPt0"
  "/nonpromptD0ToKPi_PT-1_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM|NonPrompt_dPt1"
  "/nonpromptD0ToKPi_PT-8_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM|NonPrompt_dPt8"
  "/nonpromptD0ToKPi_PT-10_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM|NonPrompt_dPt10"
  "/nonpromptD0ToKPi_PT-20_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v1/MINIAODSIM|NonPrompt_dPt20"
)

files=()
subdirs=()

for entry in "${entries[@]}"; do
  dataset="${entry%%|*}"
  subdir="${entry##*|}"
  while IFS= read -r file_path; do
    [[ -n "${file_path}" ]] || continue
    files+=("${file_path}")
    subdirs+=("${subdir}")
  done < <(dasgoclient --query="file dataset=${dataset} instance=prod/global")
done

total="${#files[@]}"
width="${#total}"

: > "${outfile}"
for i in "${!files[@]}"; do
  printf '%s %0*d %s\n' "${files[$i]}" "${width}" "${i}" "${subdirs[$i]}" >> "${outfile}"
done

printf 'Wrote %d lines to %s\n' "${total}" "${outfile}"
