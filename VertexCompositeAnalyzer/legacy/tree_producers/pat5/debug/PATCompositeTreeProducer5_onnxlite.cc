// Lightweight ONNX debug analyzer implementation

#include "VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer5_onnxlite.h"

#include <cmath>
#include <fstream>

#include "FWCore/ParameterSet/interface/FileInPath.h"
#include "FWCore/Utilities/interface/Exception.h"
#include "DataFormats/Math/interface/angle.h"

namespace {
constexpr int kInputSize = 19;
constexpr int kD0PdgId = 421;
}

PATCompositeTreeProducer5OnnxLite::PATCompositeTreeProducer5OnnxLite(const edm::ParameterSet& iConfig)
    : tree_(nullptr),
      candSize_(0) {
  tok_composites_ = consumes<pat::CompositeCandidateCollection>(
      iConfig.getUntrackedParameter<edm::InputTag>("CompositeCollection"));
  tok_vertices_ = consumes<reco::VertexCollection>(
      iConfig.getUntrackedParameter<edm::InputTag>("VertexCollection",
                                                  edm::InputTag("offlinePrimaryVertices")));
  tok_beamSpot_ = consumes<reco::BeamSpot>(
      iConfig.getUntrackedParameter<edm::InputTag>("BeamSpotTag",
                                                  edm::InputTag("offlineBeamSpot")));
  tok_centBin_ = consumes<int>(
      iConfig.getUntrackedParameter<edm::InputTag>("CentralityBinTag",
                                                  edm::InputTag("centralityBin", "HFtowers")));

  saveTree_ = iConfig.getUntrackedParameter<bool>("saveTree", true);
  saveHist_ = iConfig.getUntrackedParameter<bool>("saveHist", true);
  printThreshold_ = iConfig.getUntrackedParameter<double>("printDiffThreshold", 0.001);
  printAllCandidates_ = iConfig.getUntrackedParameter<bool>("printAllCandidates", false);
  applyCuts_ = iConfig.getUntrackedParameter<bool>("applyCuts", true);
  compareRecalc_ = iConfig.getUntrackedParameter<bool>("compareRecalc", false);
  compareThreshold_ = iConfig.getUntrackedParameter<double>("compareThreshold", 1.0e-6);
  useStoredInputs_ = iConfig.getUntrackedParameter<bool>("useStoredInputs", false);
  vtxChi2Cut_ = iConfig.getUntrackedParameter<double>("vtxChi2Cut", 9999.0);
  vtxChiProbCut_ = iConfig.getUntrackedParameter<double>("VtxChiProbCut", 0.0);
  collinCut2D_ = iConfig.getUntrackedParameter<double>("collinearityCut2D", -2.0);
  collinCut3D_ = iConfig.getUntrackedParameter<double>("collinearityCut3D", -2.0);
  alphaCut_ = iConfig.getUntrackedParameter<double>("alphaCut", 999.0);
  alpha2DCut_ = iConfig.getUntrackedParameter<double>("alpha2DCut", 999.0);
  rVtxCut_ = iConfig.getUntrackedParameter<double>("rVtxCut", 0.0);
  rVtxSigCut_ = iConfig.getUntrackedParameter<double>("vtxSignificance2DCut", 0.0);
  lVtxCut_ = iConfig.getUntrackedParameter<double>("lVtxCut", 0.0);
  lVtxSigCut_ = iConfig.getUntrackedParameter<double>("vtxSignificance3DCut", 0.0);

  onnxModelFileName_ = iConfig.getParameter<std::string>("onnxModelFileName");
  onnxInputNames_ = iConfig.getParameter<std::vector<std::string>>("input_names");
  onnxOutputNames_ = iConfig.getParameter<std::vector<std::string>>("output_names");

  hAlpha3D_ = nullptr;
  hAlpha2D_ = nullptr;
  hAlpha3DRatio_ = nullptr;
  hAlpha2DRatio_ = nullptr;
  hCos3DRatio_ = nullptr;
  hCos2DRatio_ = nullptr;
}

void PATCompositeTreeProducer5OnnxLite::beginJob() {
  initOnnx();

  if (saveHist_) {
    hAlpha3D_ = fs_->make<TH1F>("hOnnxLiteAlpha3D",";alpha3D;count",200,0.,3.2);
    hAlpha2D_ = fs_->make<TH1F>("hOnnxLiteAlpha2D",";alpha2D;count",200,0.,3.2);
    hAlpha3DRatio_ = fs_->make<TH1F>("hOnnxLiteAlpha3DRatio",";alpha3D/alpha3D_userFloat;count",200,0.5,1.5);
    hAlpha2DRatio_ = fs_->make<TH1F>("hOnnxLiteAlpha2DRatio",";alpha2D/alpha2D_userFloat;count",200,0.5,1.5);
    hCos3DRatio_ = fs_->make<TH1F>("hOnnxLiteCos3DRatio",";cos3D/cos3D_userFloat;count",200,0.5,1.5);
    hCos2DRatio_ = fs_->make<TH1F>("hOnnxLiteCos2DRatio",";cos2D/cos2D_userFloat;count",200,0.5,1.5);
  }

  if (!saveTree_) return;
  tree_ = fs_->make<TTree>("OnnxLiteTree", "OnnxLiteTree");
  tree_->Branch("candSize", &candSize_, "candSize/I");
  tree_->Branch("pt", &v_pt_);
  tree_->Branch("y", &v_y_);
  tree_->Branch("vtxProb", &v_vtxProb_);
  tree_->Branch("cent", &v_cent_);
  tree_->Branch("cos3D", &v_cos3D_);
  tree_->Branch("alpha3D", &v_alpha3D_);
  tree_->Branch("cos2D", &v_cos2D_);
  tree_->Branch("alpha2D", &v_alpha2D_);
  tree_->Branch("lVtxMag", &v_lVtxMag_);
  tree_->Branch("lVtxSig", &v_lVtxSig_);
  tree_->Branch("rVtxMag", &v_rVtxMag_);
  tree_->Branch("rVtxSig", &v_rVtxSig_);
  tree_->Branch("ptPos", &v_ptPos_);
  tree_->Branch("etaPos", &v_etaPos_);
  tree_->Branch("ptNeg", &v_ptNeg_);
  tree_->Branch("etaNeg", &v_etaNeg_);
  tree_->Branch("ptErrPos", &v_ptErrPos_);
  tree_->Branch("ptErrNeg", &v_ptErrNeg_);
  tree_->Branch("dca", &v_dca_);
  tree_->Branch("mvaStored", &v_mvaStored_);
  tree_->Branch("mvaDebug", &v_mvaDebug_);
  tree_->Branch("mvaDiff", &v_mvaDiff_);
}

void PATCompositeTreeProducer5OnnxLite::initOnnx() {
  edm::FileInPath fip(Form("VertexCompositeAnalysis/VertexCompositeProducer/data/%s",
                           onnxModelFileName_.c_str()));
  std::string fullPath = fip.fullPath();
  std::ifstream testFile(fullPath);
  if (!testFile.good()) {
    throw cms::Exception("Configuration") << "cannot find ONNX Model in : " << fullPath;
  }
  testFile.close();
  onnxRuntime_ = std::make_unique<cms::Ort::ONNXRuntime>(fullPath);
}

bool PATCompositeTreeProducer5OnnxLite::extractInputs(
    const pat::CompositeCandidate& dstar,
    const reco::Vertex* bestVtx,
    int centrality,
    std::array<float, kInputSize>& inputs,
    float& storedMva) {
  if (!bestVtx) return false;
  const reco::Candidate* d0Cand = dstar.daughter(0);
  if (!d0Cand && dstar.numberOfDaughters() > 1) {
    d0Cand = dstar.daughter(1);
  }
  if (!d0Cand) return false;

  if (std::abs(d0Cand->pdgId()) != kD0PdgId && dstar.numberOfDaughters() > 1) {
    const reco::Candidate* alt = dstar.daughter(1);
    if (alt && std::abs(alt->pdgId()) == kD0PdgId) d0Cand = alt;
  }

  const auto* d0 = dynamic_cast<const pat::CompositeCandidate*>(d0Cand);
  if (!d0 || d0->numberOfDaughters() < 2) return false;

  if (useStoredInputs_) {
    bool hasAll = true;
    for (int i = 0; i < kInputSize; ++i) {
      const std::string key = "onnx_in_" + std::to_string(i);
      if (!d0->hasUserFloat(key)) {
        hasAll = false;
        break;
      }
    }
    if (hasAll) {
      for (int i = 0; i < kInputSize; ++i) {
        const std::string key = "onnx_in_" + std::to_string(i);
        inputs[i] = d0->userFloat(key);
      }
      if (d0->hasUserFloat("mva")) {
        storedMva = d0->userFloat("mva");
        return true;
      }
      if (dstar.hasUserFloat("D0mva")) {
        storedMva = dstar.userFloat("D0mva");
        return true;
      }
      return false;
    }
  }

  if (!(d0->hasUserFloat("VtxChi2") &&
        d0->hasUserFloat("VtxNdof") &&
        d0->hasUserFloat("decaylength3D") &&
        d0->hasUserFloat("decaylengthsignif3D") &&
        d0->hasUserFloat("decaylength2D") &&
        d0->hasUserFloat("decaylengthsignif2D") &&
        d0->hasUserFloat("track3DDCA"))) {
    return false;
  }

  const auto* dauPos = d0->daughter(0);
  const auto* dauNeg = d0->daughter(1);
  if (!dauPos || !dauNeg) return false;
  if (dauPos->charge() < 0 && dauNeg->charge() > 0) {
    std::swap(dauPos, dauNeg);
  }

  const auto posRef = dauPos->get<reco::TrackRef>();
  const auto negRef = dauNeg->get<reco::TrackRef>();
  if (posRef.isNull() || negRef.isNull()) return false;

  const float vtxChi2 = d0->userFloat("VtxChi2");
  const float vtxNdf = d0->userFloat("VtxNdof");
  const float vtxProb = TMath::Prob(vtxChi2, vtxNdf);

  const float lVtxMag = d0->userFloat("decaylength3D");
  const float lVtxSig = d0->userFloat("decaylengthsignif3D");
  const float rVtxMag = d0->userFloat("decaylength2D");
  const float rVtxSig = d0->userFloat("decaylengthsignif2D");

  const bool hasFitVtx =
      d0->hasUserFloat("d0FitVx") &&
      d0->hasUserFloat("d0FitVy") &&
      d0->hasUserFloat("d0FitVz");
  const float secvx = hasFitVtx ? d0->userFloat("d0FitVx") : d0->vx();
  const float secvy = hasFitVtx ? d0->userFloat("d0FitVy") : d0->vy();
  const float secvz = hasFitVtx ? d0->userFloat("d0FitVz") : d0->vz();
  const TVector3 ptosvec(secvx - bestVtx->x(), secvy - bestVtx->y(), secvz - bestVtx->z());
  const TVector3 secvec(d0->px(), d0->py(), d0->pz());
  if (ptosvec.Mag() == 0.0 || secvec.Mag() == 0.0) return false;

  float alpha3D = angle(static_cast<double>(ptosvec.x()),
                        static_cast<double>(ptosvec.y()),
                        static_cast<double>(ptosvec.z()),
                        static_cast<double>(secvec.x()),
                        static_cast<double>(secvec.y()),
                        static_cast<double>(secvec.z()));
  float cos3D = std::cos(alpha3D);

  const TVector3 ptosvec2D(secvx - bestVtx->x(), secvy - bestVtx->y(), 0.0);
  const TVector3 secvec2D(d0->px(), d0->py(), 0.0);
  if (ptosvec2D.Mag() == 0.0 || secvec2D.Mag() == 0.0) return false;

  float alpha2D = angle(static_cast<double>(ptosvec2D.x()),
                        static_cast<double>(ptosvec2D.y()),
                        0.0,
                        static_cast<double>(secvec2D.x()),
                        static_cast<double>(secvec2D.y()),
                        0.0);
  float cos2D = std::cos(alpha2D);

  if (d0->hasUserFloat("alpha3D") && d0->hasUserFloat("alpha2D")) {
    alpha3D = d0->userFloat("alpha3D");
    alpha2D = d0->userFloat("alpha2D");
    cos3D = std::cos(alpha3D);
    cos2D = std::cos(alpha2D);
  }

  inputs[0] = d0->pt();
  inputs[1] = d0->y();
  inputs[2] = vtxProb;
  inputs[3] = static_cast<float>(centrality);
  inputs[4] = cos3D;
  inputs[5] = alpha3D;
  inputs[6] = cos2D;
  inputs[7] = alpha2D;
  inputs[8] = lVtxMag;
  inputs[9] = lVtxSig;
  inputs[10] = rVtxMag;
  inputs[11] = rVtxSig;
  inputs[12] = dauPos->pt();
  inputs[13] = dauPos->eta();
  inputs[14] = dauNeg->pt();
  inputs[15] = dauNeg->eta();
  inputs[16] = posRef->ptError();
  inputs[17] = negRef->ptError();
  inputs[18] = d0->userFloat("track3DDCA");

  if (saveHist_) {
    if (hAlpha3D_) hAlpha3D_->Fill(alpha3D);
    if (hAlpha2D_) hAlpha2D_->Fill(alpha2D);
    if (d0->hasUserFloat("alpha3D") && d0->hasUserFloat("alpha2D")) {
      const float ufAlpha3D = d0->userFloat("alpha3D");
      const float ufAlpha2D = d0->userFloat("alpha2D");
      if (ufAlpha3D != 0.0f && hAlpha3DRatio_) hAlpha3DRatio_->Fill(alpha3D / ufAlpha3D);
      if (ufAlpha2D != 0.0f && hAlpha2DRatio_) hAlpha2DRatio_->Fill(alpha2D / ufAlpha2D);
      const float ufCos3D = std::cos(ufAlpha3D);
      const float ufCos2D = std::cos(ufAlpha2D);
      if (ufCos3D != 0.0f && hCos3DRatio_) hCos3DRatio_->Fill(cos3D / ufCos3D);
      if (ufCos2D != 0.0f && hCos2DRatio_) hCos2DRatio_->Fill(cos2D / ufCos2D);
    }
  }

  if (applyCuts_) {
    const float normChi2 = vtxNdf > 0 ? vtxChi2 / vtxNdf : 99999.0f;
    if (vtxProb < vtxChiProbCut_) return false;
    if (normChi2 > vtxChi2Cut_) return false;
    if (rVtxMag < rVtxCut_) return false;
    if (rVtxSig < rVtxSigCut_) return false;
    if (lVtxMag < lVtxCut_) return false;
    if (lVtxSig < lVtxSigCut_) return false;
    if (cos3D < collinCut3D_) return false;
    if (cos2D < collinCut2D_) return false;
    if (alpha3D > alphaCut_) return false;
    if (alpha2D > alpha2DCut_) return false;
  }

  if (d0->hasUserFloat("mva")) {
    storedMva = d0->userFloat("mva");
  } else if (dstar.hasUserFloat("D0mva")) {
    storedMva = dstar.userFloat("D0mva");
  } else {
    return false;
  }

  return true;
}

bool PATCompositeTreeProducer5OnnxLite::extractInputsFromReco(
    const pat::CompositeCandidate& dstar,
    const reco::Vertex* bestVtx,
    const reco::BeamSpot* beamSpot,
    int centrality,
    std::array<float, kInputSize>& inputs,
    bool allowAlphaUserFloat,
    bool& usedAlphaUserFloat) {
  usedAlphaUserFloat = false;
  (void)beamSpot;
  if (!bestVtx) return false;
  const reco::Candidate* d0Cand = dstar.daughter(0);
  if (!d0Cand && dstar.numberOfDaughters() > 1) {
    d0Cand = dstar.daughter(1);
  }
  if (!d0Cand) return false;

  if (std::abs(d0Cand->pdgId()) != kD0PdgId && dstar.numberOfDaughters() > 1) {
    const reco::Candidate* alt = dstar.daughter(1);
    if (alt && std::abs(alt->pdgId()) == kD0PdgId) d0Cand = alt;
  }

  const auto* d0 = dynamic_cast<const pat::CompositeCandidate*>(d0Cand);
  if (!d0 || d0->numberOfDaughters() < 2) return false;

  if (!(d0->hasUserFloat("VtxChi2") &&
        d0->hasUserFloat("VtxNdof") &&
        d0->hasUserFloat("decaylength3D") &&
        d0->hasUserFloat("decaylengthsignif3D") &&
        d0->hasUserFloat("decaylength2D") &&
        d0->hasUserFloat("decaylengthsignif2D") &&
        d0->hasUserFloat("track3DDCA"))) {
    return false;
  }

  const auto* dauPos = d0->daughter(0);
  const auto* dauNeg = d0->daughter(1);
  if (!dauPos || !dauNeg) return false;
  if (dauPos->charge() < 0 && dauNeg->charge() > 0) {
    std::swap(dauPos, dauNeg);
  }

  const auto posRef = dauPos->get<reco::TrackRef>();
  const auto negRef = dauNeg->get<reco::TrackRef>();
  if (posRef.isNull() || negRef.isNull()) return false;

  const float vtxChi2 = d0->userFloat("VtxChi2");
  const float vtxNdf = d0->userFloat("VtxNdof");
  const float vtxProb = TMath::Prob(vtxChi2, vtxNdf);

  const float lVtxMag = d0->userFloat("decaylength3D");
  const float lVtxSig = d0->userFloat("decaylengthsignif3D");
  const float rVtxMag = d0->userFloat("decaylength2D");
  const float rVtxSig = d0->userFloat("decaylengthsignif2D");

  const bool hasFitVtx =
      d0->hasUserFloat("d0FitVx") &&
      d0->hasUserFloat("d0FitVy") &&
      d0->hasUserFloat("d0FitVz");
  const float secvx = hasFitVtx ? d0->userFloat("d0FitVx") : d0->vx();
  const float secvy = hasFitVtx ? d0->userFloat("d0FitVy") : d0->vy();
  const float secvz = hasFitVtx ? d0->userFloat("d0FitVz") : d0->vz();
  const float pvx = bestVtx->x();
  const float pvy = bestVtx->y();
  const float pvz = bestVtx->z();
  const TVector3 ptosvec(secvx - pvx, secvy - pvy, secvz - pvz);
  const TVector3 secvec(d0->px(), d0->py(), d0->pz());
  if (ptosvec.Mag() == 0.0 || secvec.Mag() == 0.0) return false;

  float alpha3D = secvec.Angle(ptosvec);
  float cos3D = std::cos(alpha3D);

  const TVector3 ptosvec2D(secvx - pvx, secvy - pvy, 0.0);
  const TVector3 secvec2D(d0->px(), d0->py(), 0.0);
  if (ptosvec2D.Mag() == 0.0 || secvec2D.Mag() == 0.0) return false;

  float alpha2D = secvec2D.Angle(ptosvec2D);
  float cos2D = std::cos(alpha2D);

  if (allowAlphaUserFloat &&
      d0->hasUserFloat("alpha3D") && d0->hasUserFloat("alpha2D")) {
    usedAlphaUserFloat = true;
    alpha3D = d0->userFloat("alpha3D");
    alpha2D = d0->userFloat("alpha2D");
    cos3D = std::cos(alpha3D);
    cos2D = std::cos(alpha2D);
  }

  if (applyCuts_) {
    const float normChi2 = vtxNdf > 0 ? vtxChi2 / vtxNdf : 99999.0f;
    if (vtxProb < vtxChiProbCut_) return false;
    if (normChi2 > vtxChi2Cut_) return false;
    if (rVtxMag < rVtxCut_) return false;
    if (rVtxSig < rVtxSigCut_) return false;
    if (lVtxMag < lVtxCut_) return false;
    if (lVtxSig < lVtxSigCut_) return false;
    if (cos3D < collinCut3D_) return false;
    if (cos2D < collinCut2D_) return false;
    if (alpha3D > alphaCut_) return false;
    if (alpha2D > alpha2DCut_) return false;
  }

  inputs[0] = d0->pt();
  inputs[1] = d0->y();
  inputs[2] = vtxProb;
  inputs[3] = static_cast<float>(centrality);
  inputs[4] = cos3D;
  inputs[5] = alpha3D;
  inputs[6] = cos2D;
  inputs[7] = alpha2D;
  inputs[8] = lVtxMag;
  inputs[9] = lVtxSig;
  inputs[10] = rVtxMag;
  inputs[11] = rVtxSig;
  inputs[12] = dauPos->pt();
  inputs[13] = dauPos->eta();
  inputs[14] = dauNeg->pt();
  inputs[15] = dauNeg->eta();
  inputs[16] = posRef->ptError();
  inputs[17] = negRef->ptError();
  inputs[18] = d0->userFloat("track3DDCA");

  return true;
}

void PATCompositeTreeProducer5OnnxLite::analyze(const edm::Event& iEvent,
                                                const edm::EventSetup&) {
  edm::Handle<pat::CompositeCandidateCollection> cands;
  iEvent.getByToken(tok_composites_, cands);
  if (!cands.isValid()) return;

  edm::Handle<reco::VertexCollection> vertices;
  iEvent.getByToken(tok_vertices_, vertices);
  const reco::Vertex* bestVtx = nullptr;
  if (vertices.isValid() && !vertices->empty()) {
    bestVtx = &vertices->front();
  }

  edm::Handle<reco::BeamSpot> beamSpot;
  iEvent.getByToken(tok_beamSpot_, beamSpot);
  const reco::BeamSpot* beamSpotPtr = beamSpot.isValid() ? beamSpot.product() : nullptr;

  candSize_ = 0;
  v_pt_.clear();
  v_y_.clear();
  v_vtxProb_.clear();
  v_cent_.clear();
  v_cos3D_.clear();
  v_alpha3D_.clear();
  v_cos2D_.clear();
  v_alpha2D_.clear();
  v_lVtxMag_.clear();
  v_lVtxSig_.clear();
  v_rVtxMag_.clear();
  v_rVtxSig_.clear();
  v_ptPos_.clear();
  v_etaPos_.clear();
  v_ptNeg_.clear();
  v_etaNeg_.clear();
  v_ptErrPos_.clear();
  v_ptErrNeg_.clear();
  v_dca_.clear();
  v_mvaStored_.clear();
  v_mvaDebug_.clear();
  v_mvaDiff_.clear();

  int centrality = -1;
  edm::Handle<int> cbin;
  iEvent.getByToken(tok_centBin_, cbin);
  if (cbin.isValid()) centrality = *cbin;

  for (size_t idx = 0; idx < cands->size(); ++idx) {
    const auto& dstar = (*cands)[idx];

    std::array<float, kInputSize> inputs{};
    float storedMva = 0.0f;
    if (!extractInputs(dstar, bestVtx, centrality, inputs, storedMva)) continue;

    if (compareRecalc_) {
      std::array<float, kInputSize> recomputed{};
      bool usedAlphaUserFloat = false;
      if (extractInputsFromReco(dstar, bestVtx, beamSpotPtr, centrality, recomputed, false, usedAlphaUserFloat)) {
        static const char* kNames[kInputSize] = {
          "pt", "y", "vtxProb", "cent",
          "cos3D", "alpha3D", "cos2D", "alpha2D",
          "lVtxMag", "lVtxSig", "rVtxMag", "rVtxSig",
          "ptPos", "etaPos", "ptNeg", "etaNeg",
          "ptErrPos", "ptErrNeg", "dca"
        };
        for (int i = 0; i < kInputSize; ++i) {
          const float diffVar = recomputed[i] - inputs[i];
          if (std::abs(diffVar) > compareThreshold_) {
            edm::LogPrint("OnnxLiteCompare")
                << "run=" << iEvent.id().run()
                << " lumi=" << iEvent.luminosityBlock()
                << " event=" << iEvent.id().event()
                << " cand=" << idx
                << " var=" << kNames[i]
                << " stored=" << inputs[i]
                << " recomputed=" << recomputed[i]
                << " diff=" << diffVar
                << " alphaSrc=reco";
          }
        }
        const reco::Candidate* d0Cand = dstar.daughter(0);
        if (!d0Cand && dstar.numberOfDaughters() > 1) {
          d0Cand = dstar.daughter(1);
        }
        if (d0Cand && std::abs(d0Cand->pdgId()) != kD0PdgId && dstar.numberOfDaughters() > 1) {
          const reco::Candidate* alt = dstar.daughter(1);
          if (alt && std::abs(alt->pdgId()) == kD0PdgId) d0Cand = alt;
        }
        const auto* d0 = dynamic_cast<const pat::CompositeCandidate*>(d0Cand);
        if (d0 && d0->hasUserFloat("d0FitPVx") &&
            d0->hasUserFloat("d0FitPVy") &&
            d0->hasUserFloat("d0FitPVz") &&
            d0->hasUserFloat("d0FitPVIsPV")) {
          const float fitPVx = d0->userFloat("d0FitPVx");
          const float fitPVy = d0->userFloat("d0FitPVy");
          const float fitPVz = d0->userFloat("d0FitPVz");
          const float fitPVIsPV = d0->userFloat("d0FitPVIsPV");
          const float pvx = (bestVtx && !bestVtx->isFake() && bestVtx->tracksSize() >= 2)
                                ? bestVtx->x()
                                : (beamSpotPtr ? beamSpotPtr->x0() : 0.0f);
          const float pvy = (bestVtx && !bestVtx->isFake() && bestVtx->tracksSize() >= 2)
                                ? bestVtx->y()
                                : (beamSpotPtr ? beamSpotPtr->y0() : 0.0f);
          const float pvz = (bestVtx && !bestVtx->isFake() && bestVtx->tracksSize() >= 2)
                                ? bestVtx->z()
                                : (beamSpotPtr ? beamSpotPtr->z0() : 0.0f);
          const float fitSVx = d0->hasUserFloat("d0FitVx") ? d0->userFloat("d0FitVx") : d0->vx();
          const float fitSVy = d0->hasUserFloat("d0FitVy") ? d0->userFloat("d0FitVy") : d0->vy();
          const float fitSVz = d0->hasUserFloat("d0FitVz") ? d0->userFloat("d0FitVz") : d0->vz();
          const float fitPx = d0->hasUserFloat("d0FitPx") ? d0->userFloat("d0FitPx") : d0->px();
          const float fitPy = d0->hasUserFloat("d0FitPy") ? d0->userFloat("d0FitPy") : d0->py();
          const float fitPz = d0->hasUserFloat("d0FitPz") ? d0->userFloat("d0FitPz") : d0->pz();
          const float recoSVx = d0->vx();
          const float recoSVy = d0->vy();
          const float recoSVz = d0->vz();
          const float recoPx = d0->px();
          const float recoPy = d0->py();
          const float recoPz = d0->pz();
          const TVector3 fitPtos(fitSVx - fitPVx, fitSVy - fitPVy, fitSVz - fitPVz);
          const TVector3 fitSec(fitPx, fitPy, fitPz);
          const TVector3 recoPtos(recoSVx - pvx, recoSVy - pvy, recoSVz - pvz);
          const TVector3 recoSec(recoPx, recoPy, recoPz);
          const float fitAlpha3D = angle(static_cast<double>(fitPtos.x()),
                                         static_cast<double>(fitPtos.y()),
                                         static_cast<double>(fitPtos.z()),
                                         static_cast<double>(fitSec.x()),
                                         static_cast<double>(fitSec.y()),
                                         static_cast<double>(fitSec.z()));
          const float recoAlpha3D = angle(static_cast<double>(recoPtos.x()),
                                          static_cast<double>(recoPtos.y()),
                                          static_cast<double>(recoPtos.z()),
                                          static_cast<double>(recoSec.x()),
                                          static_cast<double>(recoSec.y()),
                                          static_cast<double>(recoSec.z()));
          edm::LogPrint("OnnxLiteCompare")
              << "run=" << iEvent.id().run()
              << " lumi=" << iEvent.luminosityBlock()
              << " event=" << iEvent.id().event()
              << " cand=" << idx
              << " pvFit=(" << fitPVx << "," << fitPVy << "," << fitPVz << ")"
              << " pvFitIsPV=" << fitPVIsPV
              << " pvReco=(" << pvx << "," << pvy << "," << pvz << ")"
              << " pvRecoIsPV=" << ((bestVtx && !bestVtx->isFake() && bestVtx->tracksSize() >= 2) ? 1 : 0)
              << " svFit=(" << fitSVx << "," << fitSVy << "," << fitSVz << ")"
              << " pFit=(" << fitPx << "," << fitPy << "," << fitPz << ")"
              << " svReco=(" << recoSVx << "," << recoSVy << "," << recoSVz << ")"
              << " pReco=(" << recoPx << "," << recoPy << "," << recoPz << ")"
              << " alphaFit3D=" << fitAlpha3D
              << " alphaReco3D=" << recoAlpha3D;
        }
        bool usedAlphaUserFloatFallback = false;
        std::array<float, kInputSize> recomputedUf{};
        if (extractInputsFromReco(dstar, bestVtx, beamSpotPtr, centrality, recomputedUf, true, usedAlphaUserFloatFallback) &&
            usedAlphaUserFloatFallback) {
          const float diffAlpha3D = recomputedUf[5] - inputs[5];
          const float diffAlpha2D = recomputedUf[7] - inputs[7];
          if (std::abs(diffAlpha3D) > compareThreshold_ ||
              std::abs(diffAlpha2D) > compareThreshold_) {
            edm::LogPrint("OnnxLiteCompare")
                << "run=" << iEvent.id().run()
                << " lumi=" << iEvent.luminosityBlock()
                << " event=" << iEvent.id().event()
                << " cand=" << idx
                << " var=alphaFallback"
                << " storedAlpha3D=" << inputs[5]
                << " recomputedAlpha3D=" << recomputedUf[5]
                << " diffAlpha3D=" << diffAlpha3D
                << " storedAlpha2D=" << inputs[7]
                << " recomputedAlpha2D=" << recomputedUf[7]
                << " diffAlpha2D=" << diffAlpha2D
                << " alphaSrc=userFloat";
          }
        }
      }
    }

    cms::Ort::FloatArrays data_;
    data_.emplace_back(kInputSize, 0.0f);
    for (int i = 0; i < kInputSize; ++i) data_[0][i] = inputs[i];

    const auto outputs = onnxRuntime_->run(onnxInputNames_, data_, onnxInputShapes_, onnxOutputNames_);
    if (outputs.empty() || outputs[0].size() < 2) continue;

    const float mvaDebug = outputs[0][1];
    const float diff = mvaDebug - storedMva;

    if (printAllCandidates_ || std::abs(diff) > printThreshold_) {
      edm::LogPrint("OnnxLiteDebug")
          << "run=" << iEvent.id().run()
          << " lumi=" << iEvent.luminosityBlock()
          << " event=" << iEvent.id().event()
          << " cand=" << idx
          << " mvaStored=" << storedMva
          << " mvaDebug=" << mvaDebug
          << " diff=" << diff;
    }

    if (!saveTree_) continue;

    ++candSize_;
    v_pt_.push_back(inputs[0]);
    v_y_.push_back(inputs[1]);
    v_vtxProb_.push_back(inputs[2]);
    v_cent_.push_back(inputs[3]);
    v_cos3D_.push_back(inputs[4]);
    v_alpha3D_.push_back(inputs[5]);
    v_cos2D_.push_back(inputs[6]);
    v_alpha2D_.push_back(inputs[7]);
    v_lVtxMag_.push_back(inputs[8]);
    v_lVtxSig_.push_back(inputs[9]);
    v_rVtxMag_.push_back(inputs[10]);
    v_rVtxSig_.push_back(inputs[11]);
    v_ptPos_.push_back(inputs[12]);
    v_etaPos_.push_back(inputs[13]);
    v_ptNeg_.push_back(inputs[14]);
    v_etaNeg_.push_back(inputs[15]);
    v_ptErrPos_.push_back(inputs[16]);
    v_ptErrNeg_.push_back(inputs[17]);
    v_dca_.push_back(inputs[18]);
    v_mvaStored_.push_back(storedMva);
    v_mvaDebug_.push_back(mvaDebug);
    v_mvaDiff_.push_back(diff);
  }

  if (saveTree_ && tree_) {
    tree_->Fill();
  }
}

DEFINE_FWK_MODULE(PATCompositeTreeProducer5OnnxLite);
