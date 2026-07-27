import FWCore.ParameterSet.Config as cms

dStarana_onnxlite = cms.EDAnalyzer('PATCompositeTreeProducer5OnnxLite',
  CompositeCollection = cms.untracked.InputTag("generalDStarCandidatesNew:DStar"),
  VertexCollection = cms.untracked.InputTag("offlinePrimaryVertices"),
  onnxModelFileName = cms.string("XGBoost_Model_0428_0_OnlyPrompt.onnx"),
  input_names = cms.vstring("float_input"),
  output_names = cms.vstring("probabilities"),
  saveTree = cms.untracked.bool(True),
  saveHist = cms.untracked.bool(True),
  printDiffThreshold = cms.untracked.double(0.001),
  applyCuts = cms.untracked.bool(True),
  vtxChi2Cut = cms.untracked.double(9999.0),
  VtxChiProbCut = cms.untracked.double(0.00),
  collinearityCut2D = cms.untracked.double(-2.0),
  collinearityCut3D = cms.untracked.double(-2.0),
  alphaCut = cms.untracked.double(1.0),
  alpha2DCut = cms.untracked.double(999.0),
  rVtxCut = cms.untracked.double(0.0),
  lVtxCut = cms.untracked.double(0.0),
  vtxSignificance2DCut = cms.untracked.double(0.0),
  vtxSignificance3DCut = cms.untracked.double(3.0)
)
