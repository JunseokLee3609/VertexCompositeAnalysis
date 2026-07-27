#!/bin/bash -x

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
test_dir="$(cd "${script_dir}/../.." && pwd)"
outfile="${1:-${test_dir}/files2023MB24_31.txt}"

rm -f "${outfile}"

for i in {24..31}
do
    dasgoclient --query="file dataset=/HIPhysicsRawPrime$i/HIRun2023A-PromptReco-v2/MINIAOD" >> "${outfile}"
done
