#!/bin/bash
set -euo pipefail
model_rel="VertexCompositeAnalysis/VertexCompositeProducer/data/XGBoost_Model_OnlyNonPrompt_05Mar26_Centrality_pTerr_ptErr011_1.onnx"
source_model="$PWD/src/$model_rel"
test -s "$source_model"
mkdir -p "$CMSSW_BASE/src/VertexCompositeAnalysis/VertexCompositeProducer/data"
cp "$source_model" "$CMSSW_BASE/src/$model_rel"
test -s "$CMSSW_BASE/src/$model_rel"
printf 'ONNX_MODEL_READY %s\n' "$CMSSW_BASE/src/$model_rel"
exec cmsRun -j FrameworkJobReport.xml PSet.py
