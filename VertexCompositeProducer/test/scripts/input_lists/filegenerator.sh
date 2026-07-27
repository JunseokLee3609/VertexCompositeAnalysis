#!/usr/bin/env bash
# filegenerator.sh
# Lines -> append right-side index number.
# - From file/stdin, or directly from dasgoclient --query.
# - Defaults: base=0 (0-based), zero-pad to width=len(total lines), separator=" ".

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  1) From existing file:
     scripts/input_lists/filegenerator.sh -i files.txt -o files_indexed.txt
  2) Direct from DAS query:
     scripts/input_lists/filegenerator.sh -Q 'file dataset=/Your/Dataset/Name' -o files_indexed.txt
  3) No padding, 1-based, tab separator:
     scripts/input_lists/filegenerator.sh -i files.txt -b 1 -p 0 -t > out.txt
  4) Filter by physics rawprime index range:
     scripts/input_lists/filegenerator.sh -Q 'file dataset=/HIPhysicsRawPrime*/HIRun2023A-PromptReco-v2/MINIAOD instance=prod/global' -r '0:9' -o files_rawprime.txt

Options:
  -i <path>     Input file (default: stdin)
  -o <path>     Output file (default: stdout)
  -Q <query>    Run: dasgoclient --query="<query>" as input
  -r <range>    Filter by index range (format: "start:end" or "start:" or ":end"); if -o is set, filename gets suffixed with _start_end (':' → '_').
                For HIPhysicsRawPrime datasets, adds third column = HIPhysicsRawPrimeX per file.
                Example: -r 0:3 → files_0_3.list with lines: <file> <idx> HIPhysicsRawPrime0..3
  -b <int>      Base index (default: 0 → 0,1,2,...; use 1 for 1,2,...)
  -p <auto|N>   Pad width (default: auto = len(total lines); 0 = no padding)
  -S <str>      Separator string (default: space)
  -d <subdir>   Append subdir as 3rd column (for cfg arg3)
  -t            Use tab as separator (overrides -S)
  -h            Show this help

Notes:
- Uses an in-memory buffer to compute total count; fine for typical DAS file lists.
- For in-place overwrite: write to a temp then mv, e.g.,
    scripts/input_lists/filegenerator.sh -i files.txt -o files.txt.tmp && mv files.txt.tmp files.txt
- When using -r with -Q for HIPhysicsRawPrime*/HIRun2023A-PromptReco-v*/MINIAOD, it filters datasets by RawPrime index before fetching files; otherwise -r filters by line index.
USAGE
}

# Defaults
input="/dev/stdin"
output="/dev/stdout"
query=""
range=""
base=0
pad="auto"
sep=" "
use_tab="0"
subdir=""
use_subdir="0"

while getopts ":i:o:Q:r:b:p:S:d:th" opt; do
  case "$opt" in
    i) input="$OPTARG" ;;
    o) output="$OPTARG" ;;
    Q) query="$OPTARG" ;;
    r) range="$OPTARG" ;;
    b) base="$OPTARG" ;;
    p) pad="$OPTARG" ;;
    S) sep="$OPTARG" ;;
    d) subdir="$OPTARG"; use_subdir="1" ;;
    t) use_tab="1" ;;
    h) usage; exit 0 ;;
    \?) echo "Unknown option: -$OPTARG" >&2; usage; exit 2 ;;
    :)  echo "Missing arg for -$OPTARG" >&2; usage; exit 2 ;;
  esac
done

# If output file specified and range given, suffix filename with _<range> (':' -> '_')
if [[ -n "$range" && "$output" != "/dev/stdout" ]]; then
  range_tag=${range//:/_}
  out_dir=$(dirname -- "$output")
  out_base=$(basename -- "$output")
  if [[ "$out_base" == *.* ]]; then
    out_name="${out_base%.*}"
    out_ext=".${out_base##*.}"
  else
    out_name="$out_base"
    out_ext=""
  fi
  output="$out_dir/${out_name}_${range_tag}${out_ext}"
fi

tmp=""
cleanup() { [[ -n "${tmp}" && -f "${tmp}" ]] && rm -f "${tmp}"; }
trap cleanup EXIT

dynamic_rawprime=0
if [[ -n "$query" ]]; then
  tmp="$(mktemp)"
  if [[ -n "$range" && "$query" == *"dataset=/HIPhysicsRawPrime"*"/HIRun2023A-PromptReco-v"*"/MINIAOD"* ]]; then
    dynamic_rawprime=1
    # Parse range: start:end, start:, :end
    IFS=':' read -r rstart rend <<< "$range"
    if [[ -z "${rstart:-}" ]]; then rstart=0; fi
    if [[ -z "${rend:-}" ]]; then rend=$rstart; fi
    if ! [[ "$rstart" =~ ^[0-9]+$ && "$rend" =~ ^[0-9]+$ && $rstart -le $rend ]]; then
      echo "Error: -r expects numeric range start:end (start<=end)." >&2
      exit 2
    fi
    # Extract dataset pattern and instance from user query
    ds_pat=$(echo "$query" | sed -n 's/.*\(dataset=[^ ]*\).*/\1/p')
    instance_part=$(echo "$query" | sed -n 's/.*\(instance=[^ ]*\).*/\1/p')
    if [[ -z "$ds_pat" ]]; then
      echo "Error: could not parse dataset pattern from -Q" >&2
      exit 2
    fi
    for ((idx=rstart; idx<=rend; idx++)); do
      ds_i=$(echo "$ds_pat" | sed "s#HIPhysicsRawPrime\*#HIPhysicsRawPrime${idx}#")
      dasgoclient --query="file ${ds_i} ${instance_part}" >> "$tmp" || true
    done
  else
    dasgoclient --query="$query" > "$tmp"
  fi
  input="$tmp"
fi

# Separator handling
if [[ "$use_tab" == "1" ]]; then
  sep=$'\t'
fi

# Pad width normalization: auto -> -1, numeric -> as-is, 0 -> no-padding
pad_val="-1"
if [[ "$pad" == "auto" ]]; then
  pad_val="-1"
else
  if ! [[ "$pad" =~ ^[0-9]+$ ]]; then
    echo "Error: -p expects 'auto' or non-negative integer" >&2
    exit 2
  fi
  pad_val="$pad"
fi

# Do the work with awk (buffer to know total N)
awk -v base="$base" -v pad="$pad_val" -v sep="$sep" -v subdir="$subdir" -v use_subdir="$use_subdir" -v range_str="$range" -v dynamic_rawprime="$dynamic_rawprime" '
BEGIN {
  range_start = -1
  range_end = -1
  parse_error = 0
  if (range_str != "") {
    n = split(range_str, parts, ":")
    if (n != 2) {
      print "Error: invalid range format. Use start:end, start:, or :end" > "/dev/stderr"
      parse_error = 1
    } else {
      range_start = (parts[1] == "") ? 0 : int(parts[1])
      range_end = (parts[2] == "") ? 999999999 : int(parts[2])
    }
  }
}
{
  a[NR]=$0
}
END {
  if (parse_error) {
    exit 2
  }
  N=NR
  w = (pad < 0) ? length(N) : pad
  for (i=1; i<=N; i++) {
    idx = base + i - 1
    if (dynamic_rawprime == 0 && range_start >= 0 && (idx < range_start || idx > range_end)) {
      continue
    }
    if (w == 0) {
      out = sprintf("%s%s%d", a[i], sep, idx)
    } else {
      out = sprintf("%s%s%0*d", a[i], sep, w, idx)
    }
    if (dynamic_rawprime == 1 && use_subdir != 1) {
      # Extract HIPhysicsRawPrimeX token from the file path
      if (match(a[i], /HIPhysicsRawPrime[0-9]+/, m)) {
        out = out sep m[0]
      }
    } else if (use_subdir == 1) {
      out = out sep subdir
    }
    print out
  }
}
' "$input" > "$output"
status=$?
if [[ $status -ne 0 ]]; then
  exit $status
fi
