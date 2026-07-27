// Lightweight ONNX debug analyzer: compute D0 MVA from stored D* two-layer variables

#ifndef VertexCompositeAnalysis_VertexCompositeAnalyzer_PATCompositeTreeProducer5_onnxlite_h
#define VertexCompositeAnalysis_VertexCompositeAnalyzer_PATCompositeTreeProducer5_onnxlite_h

#include <array>
#include <memory>
#include <string>
#include <vector>

#include <TTree.h>
#include <TMath.h>
#include <TH1F.h>
#include <TVector3.h>

#include "FWCore/Framework/interface/one/EDAnalyzer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/EventSetup.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/ServiceRegistry/interface/Service.h"
#include "CommonTools/UtilAlgos/interface/TFileService.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"

#include "DataFormats/TrackReco/interface/Track.h"
#include "DataFormats/TrackReco/interface/TrackFwd.h"
#include "DataFormats/RecoCandidate/interface/RecoChargedCandidate.h"
#include "DataFormats/PatCandidates/interface/CompositeCandidate.h"
#include "DataFormats/VertexReco/interface/Vertex.h"
#include "DataFormats/VertexReco/interface/VertexFwd.h"
#include "DataFormats/BeamSpot/interface/BeamSpot.h"

#include "PhysicsTools/ONNXRuntime/interface/ONNXRuntime.h"

class PATCompositeTreeProducer5OnnxLite : public edm::one::EDAnalyzer<> {
public:
  explicit PATCompositeTreeProducer5OnnxLite(const edm::ParameterSet&);
  ~PATCompositeTreeProducer5OnnxLite() override = default;

  void beginJob() override;
  void analyze(const edm::Event&, const edm::EventSetup&) override;
  void endJob() override {}

private:
  void initOnnx();
  bool extractInputs(const pat::CompositeCandidate& dstar,
                     const reco::Vertex* bestVtx,
                     int centrality,
                     std::array<float, 19>& inputs,
                     float& storedMva);
  bool extractInputsFromReco(const pat::CompositeCandidate& dstar,
                             const reco::Vertex* bestVtx,
                             const reco::BeamSpot* beamSpot,
                             int centrality,
                             std::array<float, 19>& inputs,
                             bool allowAlphaUserFloat,
                             bool& usedAlphaUserFloat);

  edm::EDGetTokenT<pat::CompositeCandidateCollection> tok_composites_;
  edm::EDGetTokenT<reco::VertexCollection> tok_vertices_;
  edm::EDGetTokenT<reco::BeamSpot> tok_beamSpot_;
  edm::EDGetTokenT<int> tok_centBin_;

  bool saveTree_;
  bool saveHist_;
  double printThreshold_;
  bool printAllCandidates_;
  bool applyCuts_;
  bool compareRecalc_;
  double compareThreshold_;
  bool useStoredInputs_;
  double vtxChi2Cut_;
  double vtxChiProbCut_;
  double collinCut2D_;
  double collinCut3D_;
  double alphaCut_;
  double alpha2DCut_;
  double rVtxCut_;
  double rVtxSigCut_;
  double lVtxCut_;
  double lVtxSigCut_;
  std::string onnxModelFileName_;
  std::vector<std::string> onnxInputNames_;
  std::vector<std::string> onnxOutputNames_;
  std::vector<std::vector<int64_t>> onnxInputShapes_;
  std::unique_ptr<cms::Ort::ONNXRuntime> onnxRuntime_;

  edm::Service<TFileService> fs_;
  TTree* tree_;
  int candSize_;
  TH1F* hAlpha3D_;
  TH1F* hAlpha2D_;
  TH1F* hAlpha3DRatio_;
  TH1F* hAlpha2DRatio_;
  TH1F* hCos3DRatio_;
  TH1F* hCos2DRatio_;

  std::vector<float> v_pt_;
  std::vector<float> v_y_;
  std::vector<float> v_vtxProb_;
  std::vector<float> v_cent_;
  std::vector<float> v_cos3D_;
  std::vector<float> v_alpha3D_;
  std::vector<float> v_cos2D_;
  std::vector<float> v_alpha2D_;
  std::vector<float> v_lVtxMag_;
  std::vector<float> v_lVtxSig_;
  std::vector<float> v_rVtxMag_;
  std::vector<float> v_rVtxSig_;
  std::vector<float> v_ptPos_;
  std::vector<float> v_etaPos_;
  std::vector<float> v_ptNeg_;
  std::vector<float> v_etaNeg_;
  std::vector<float> v_ptErrPos_;
  std::vector<float> v_ptErrNeg_;
  std::vector<float> v_dca_;
  std::vector<float> v_mvaStored_;
  std::vector<float> v_mvaDebug_;
  std::vector<float> v_mvaDiff_;
};

#endif
