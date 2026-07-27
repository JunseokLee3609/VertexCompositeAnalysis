#ifndef VertexCompositeAnalysis_VertexCompositeAnalyzer_PATCompositeTreeProducer7_h
#define VertexCompositeAnalysis_VertexCompositeAnalyzer_PATCompositeTreeProducer7_h

#include <array>
#include <cmath>
#include <sstream>
#include <string>
#include <vector>

#include "FWCore/Framework/interface/Frameworkfwd.h"
#include "FWCore/Framework/interface/one/EDAnalyzer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"

#include "DataFormats/Candidate/interface/Candidate.h"
#include "DataFormats/HeavyIonEvent/interface/Centrality.h"
#include "DataFormats/HeavyIonEvent/interface/EvtPlane.h"
#include "DataFormats/HepMCCandidate/interface/GenParticle.h"
#include "DataFormats/Math/interface/Point3D.h"
#include "DataFormats/Math/interface/deltaR.h"
#include "DataFormats/PatCandidates/interface/CompositeCandidate.h"
#include "DataFormats/RecoCandidate/interface/RecoCandidate.h"
#include "DataFormats/TrackReco/interface/DeDxData.h"
#include "DataFormats/TrackReco/interface/Track.h"
#include "DataFormats/VertexReco/interface/Vertex.h"
#include "TVector3.h"

class TH1F;
class TTree;

static constexpr int kPdgGamma = 22;
static constexpr int kPdgKaon = 321;
static constexpr int kPdgPion = 211;
static constexpr int kPdgD0 = 421;
static constexpr int kMaxGenCand = 50000;
static constexpr float kInvalidValue = -99.f;

class PATCompositeTreeProducer7 : public edm::one::EDAnalyzer<> {
public:
  explicit PATCompositeTreeProducer7(const edm::ParameterSet& iConfig);
  ~PATCompositeTreeProducer7() override = default;
  using CC = pat::CompositeCandidate;
  using CCC = pat::CompositeCandidateCollection;
  using MVACollection = std::vector<float>;

private:
  void beginJob() override;
  void analyze(const edm::Event& iEvent, const edm::EventSetup&) override;
  void endJob() override;
  void initTree();
  void initMassHistograms();
  void fillDstarMassHistograms(unsigned int idx);
  void fillRECO(const edm::Event& iEvent, const edm::EventSetup& iSetup);
  void fillGEN(const edm::Event& iEvent, const edm::EventSetup& iSetup);
  void getAncestorId(const reco::Candidate& gCand, int& genAncestorId, int& genAncestorFlavor) const;
  void processCentralityInfo(const edm::Event& iEvent);
  void processEventPlaneInfo(const edm::Event& iEvent);
  void processRunInfo(const edm::Event& iEvent);
  void processVertexAndTrackInfo(const edm::Handle<reco::VertexCollection>& vertices,
                                 const edm::Handle<reco::TrackCollection>& tracks);
  void processCandidates(const CCC* v0candidates_,
                         const edm::Handle<MVACollection>& mvavalues,
                         const std::vector<reco::GenParticleRef>& genRefs,
                         const edm::Handle<edm::ValueMap<reco::DeDxData>>& dEdxHandle1,
                         const edm::Handle<edm::ValueMap<reco::DeDxData>>& dEdxHandle2,
                         const edm::Event& iEvent,
                         const edm::Handle<reco::VertexCollection>& vertices,
                         const edm::Handle<reco::GenParticleCollection>& genpars,
                         const edm::Handle<math::XYZPointF>& genOrigin);
  std::vector<reco::GenParticleRef> processGenMatching(const edm::Handle<reco::GenParticleCollection>& genpars);

  struct D0DaughterSummary {
    int gammaCount = 0;
    int nonGammaCount = 0;
    bool hasKaon = false;
    bool hasPion = false;
  };
  D0DaughterSummary summarizeD0Daughters(const reco::Candidate* d0) const;
  void resetCandidateOutputs(unsigned int idx);
  void resetRecoGenMatch(unsigned int idx);
  std::string daughterPdgList(const reco::Candidate* cand) const {
    if (!cand) return "[]";
    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < cand->numberOfDaughters(); ++i) {
      if (i) oss << ",";
      const reco::Candidate* d = cand->daughter(i);
      oss << (d ? d->pdgId() : -99);
    }
    oss << "]";
    return oss.str();
  }
  bool matchTrackdR(const reco::Candidate* recoTrk, const reco::Candidate* genTrk, bool chkchrg = true) const {
    if (!recoTrk || !genTrk) return false;
    if (chkchrg && (recoTrk->charge() != genTrk->charge())) return false;
    return (reco::deltaR(*recoTrk, *genTrk) < deltaR_);
  }
  bool matchHadron(const reco::Candidate* dmeson, const reco::GenParticle& gen, bool isMatchD0) const {
    return matchHadron(dmeson, static_cast<const reco::Candidate&>(gen), isMatchD0);
  }
  bool matchHadron(const reco::Candidate* dmeson, const reco::Candidate& gen, bool isMatchD0) const {
    if (!dmeson) return false;
    if (!isMatchD0) return matchTrackdR(dmeson, &gen, true);
    if (dmeson->numberOfDaughters() < 2 || gen.numberOfDaughters() < 2) return false;

    const reco::Candidate* recoTrk1 = dmeson->daughter(0);
    const reco::Candidate* recoTrk2 = dmeson->daughter(1);
    if (!recoTrk1 || !recoTrk2) return false;

    // Fast path: most GEN D0 come as daughter(0,1) = K/pi (no gamma at these slots).
    const reco::Candidate* g0 = gen.daughter(0);
    const reco::Candidate* g1 = gen.daughter(1);
    if (g0 && g1 && std::abs(g0->pdgId()) != kPdgGamma && std::abs(g1->pdgId()) != kPdgGamma) {
      if (matchTrackdR(recoTrk1, g0, true) && matchTrackdR(recoTrk2, g1, true)) return true;
      if (matchTrackdR(recoTrk2, g0, true) && matchTrackdR(recoTrk1, g1, true)) return true;
      if (gen.numberOfDaughters() == 2) return false;
    }

    // Fallback: inclusive for D0->Kpi(+gamma), ignore gamma and match the two non-gamma tracks.
    std::vector<const reco::Candidate*> genNonGamma;
    genNonGamma.reserve(gen.numberOfDaughters());
    for (size_t idau = 0; idau < gen.numberOfDaughters(); ++idau) {
      const reco::Candidate* gd = gen.daughter(idau);
      if (!gd) continue;
      if (std::abs(gd->pdgId()) == kPdgGamma) continue;
      genNonGamma.push_back(gd);
    }
    if (genNonGamma.size() != 2) return false;
    const reco::Candidate* genTrk1 = genNonGamma[0];
    const reco::Candidate* genTrk2 = genNonGamma[1];
    if (!genTrk1 || !genTrk2) return false;
    if (matchTrackdR(recoTrk1, genTrk1, true) && matchTrackdR(recoTrk2, genTrk2, true)) return true;
    if (matchTrackdR(recoTrk2, genTrk1, true) && matchTrackdR(recoTrk1, genTrk2, true)) return true;
    return false;
  }
  bool checkSwap(const reco::Candidate* dmeson, const reco::GenParticle& gen) const {
    return dmeson->pdgId() != gen.pdgId();
  }
  bool checkSwap(const reco::Candidate* dmeson, const reco::Candidate& gen) const {
    return dmeson->pdgId() != gen.pdgId();
  }
  void countDstarGenMatchTail(double deltaMass, bool strictNoFSR, bool swap, double slowPionDR);
  void countDstarSlowPionRecoDiagnosis(double deltaMass,
                                       double rawSlowPionDeltaMass,
                                       bool strictNoFSR,
                                       bool swap,
                                       bool candidateMatched,
                                       bool rawTrackMatched,
                                       double candidateSlowPionDR,
                                       double rawTrackSlowPionDR,
                                       double rawTrackToCandidateDR,
                                       double candidatePtOverGenPt,
                                       double rawTrackPtOverGenPt);
  void countDstarTailAncestry(double deltaMass,
                              const reco::Candidate* recoSlowPi,
                              const reco::GenParticle* genDStar,
                              const reco::GenParticle* genSlowPi,
                              double slowPionDR);
  reco::GenParticleRef findMother(const reco::GenParticleRef& genParRef) {
    if (genParRef.isNull()) return genParRef;
    reco::GenParticleRef genMomRef = genParRef;
    int pdg = genParRef->pdgId();
    const int pdg_OLD = pdg;
    while (pdg == pdg_OLD && genMomRef->numberOfMothers() > 0) {
      genMomRef = genMomRef->motherRef(0);
      pdg = genMomRef->pdgId();
    }
    if (pdg == pdg_OLD) genMomRef = reco::GenParticleRef();
    return genMomRef;
  }
  void genDecayLength(const reco::Candidate& gCand,
                      float& gen_decayLength2D_,
                      float& gen_decayLength3D_,
                      float& gen_angle2D_,
                      float& gen_angle3D_) {
    gen_decayLength2D_ = -99.;
    gen_decayLength3D_ = -99.;
    gen_angle2D_ = -99;
    gen_angle3D_ = -99;

    if (gCand.numberOfDaughters() == 0 || !gCand.daughter(0)) return;
    const auto& dauVtx = gCand.daughter(0)->vertex();
    const auto& genVertex_ = gCand.vertex();
    TVector3 ptosvec(dauVtx.X() - genVertex_.x(), dauVtx.Y() - genVertex_.y(), dauVtx.Z() - genVertex_.z());
    TVector3 secvec(gCand.px(), gCand.py(), gCand.pz());
    gen_angle3D_ = secvec.Angle(ptosvec);
    gen_decayLength3D_ = ptosvec.Mag();
    TVector3 ptosvec2D(dauVtx.X() - genVertex_.x(), dauVtx.Y() - genVertex_.y(), 0.0);
    TVector3 secvec2D(gCand.px(), gCand.py(), 0.0);
    gen_angle2D_ = secvec2D.Angle(ptosvec2D);
    gen_decayLength2D_ = ptosvec2D.Mag();
  }

  edm::EDGetTokenT<reco::GenParticleCollection> tok_genParticle_;
  edm::EDGetTokenT<math::XYZPointF> tok_genOrigin_;
  edm::EDGetTokenT<reco::VertexCollection> tok_offlinePV_;
  edm::EDGetTokenT<reco::TrackCollection> tok_generalTrk_;
  edm::EDGetTokenT<CCC> tok_compositeCandidates_;
  edm::EDGetTokenT<MVACollection> tok_mvaValues_;
  edm::EDGetTokenT<edm::ValueMap<reco::DeDxData>> tok_dedx1_;
  edm::EDGetTokenT<edm::ValueMap<reco::DeDxData>> tok_dedx2_;
  edm::EDGetTokenT<reco::EvtPlaneCollection> tok_eventplaneSrc_;
  edm::EDGetTokenT<int> tok_centBinLabel_;
  edm::EDGetTokenT<reco::Centrality> tok_centSrc_;

  bool doRecoNtuple_;
  bool doGenMatching_;
  bool doGenNtuple_;
  bool doGenMatchingTOF_;
  bool hasSwap_;
  bool decayInGen_;
  bool twoLayerDecay_;
  bool useAnyMVA_;
  bool isSkimMVA_;
  bool doRunInfo_;
  bool isCentrality_;
  bool debugGenMatching_;
  bool verboseDebug_;
  bool debugDstarTailAncestry_;
  bool isEventPlane_;
  bool hasEventplaneSrcToken_;
  bool hasCompositeCandidatesToken_;
  bool saveHistogram_;

  int pid_;
  int pidDau1_;
  int pidDau2_;
  double deltaR_;
  double multMax_;
  double multMin_;
  double massHistPeak_;
  double massHistWidth_;
  int massHistBins_;
  std::vector<double> dstarMassHistPtBins_;
  std::vector<double> dstarMassHistYBins_;
  std::vector<double> dstarMassHistHiBins_;
  std::vector<double> dstarMassHistDcaBins_;
  std::vector<double> dstarMassHistMvaCuts_;

  int Ntrkoffline;
  int Npixel;
  int centrality;
  int candSize;
  unsigned int runNb;
  unsigned int lsNb;
  unsigned int eventNb;
  float bestvx;
  float bestvy;
  float bestvz;
  float HFsumETPlus;
  float HFsumETMinus;
  float ZDCPlus;
  float ZDCMinus;

  float ephfpAngle[2];
  float ephfmAngle[2];
  float ephfpQ[2];
  float ephfmQ[2];
  float ephfpSumW;
  float ephfmSumW;
  float ephfpSumWSub[2];
  float ephfmSumWSub[2];
  float ephfmAngleoff[2];
  float ephfpAngleoff[2];
  float ephfmAngleRaw[2];
  float ephfmsumCosRaw[2];
  float ephfmsumSinRaw[2];
  float ephfmsumCos[2];
  float ephfmsumSin[2];
  float ephfmsumPtOrEt[2];
  float ephfpAngleRaw[2];
  float ephfpsumCosRaw[2];
  float ephfpsumSinRaw[2];
  float ephfpsumCos[2];
  float ephfpsumSin[2];
  float ephfpsumPtOrEt[2];
  float eptrackmidAngle[2];
  float eptrackmidAngleoff[2];
  float eptrackmidAngleRaw[2];
  float eptrackmidQ[2];
  float eptrackmidSumCos[2];
  float eptrackmidSumSin[2];
  float eptrackmidSumCosRaw[2];
  float eptrackmidSumSinRaw[2];
  float eptrackmidSumW;
  float eptrackmidSumWSub[2];
  float eptrackmidSumPtOrEt[2];
  float eptrackpAngle[2];
  float eptrackpAngleoff[2];
  float eptrackpAngleRaw[2];
  float eptrackpQ[2];
  float eptrackpSumCos[2];
  float eptrackpSumSin[2];
  float eptrackpSumCosRaw[2];
  float eptrackpSumSinRaw[2];
  float eptrackpSumW[2];
  float eptrackpSumPtOrEt[2];
  float eptrackmAngle[2];
  float eptrackmAngleoff[2];
  float eptrackmAngleRaw[2];
  float eptrackmQ[2];
  float eptrackmSumCos[2];
  float eptrackmSumSin[2];
  float eptrackmSumCosRaw[2];
  float eptrackmSumSinRaw[2];
  float eptrackmSumW[2];
  float eptrackmSumPtOrEt[2];
  float ephfAngle[2];
  float ephfAngleoff[2];
  float ephfAngleRaw[2];
  float ephfQ[2];
  float ephfSumW;
  float ephfSumWSub[2];
  float ephfsumCos[2];
  float ephfsumSin[2];
  float ephfsumCosRaw[2];
  float ephfsumSinRaw[2];
  float ephfsumPtOrEt[2];

  TTree* PATCompositeNtuple_;
  static constexpr int kDstarMassHistCategoryBins = 31;
  static constexpr int kDstarMassHistDcaBins = 10;
  static constexpr int kDstarMassHistMvaBins = 11;
  TH1F* hDstarDeltaMass[kDstarMassHistCategoryBins][kDstarMassHistDcaBins][kDstarMassHistMvaBins];
  float mva[kMaxGenCand];
  float pt[kMaxGenCand];
  float eta[kMaxGenCand];
  float phi[kMaxGenCand];
  float flavor[kMaxGenCand];
  float y[kMaxGenCand];
  float mass[kMaxGenCand];
  float VtxProb[kMaxGenCand];
  float dlos[kMaxGenCand];
  float dl[kMaxGenCand];
  float dlerror[kMaxGenCand];
  float agl[kMaxGenCand];
  float vtxChi2[kMaxGenCand];
  float ndf[kMaxGenCand];
  float agl_abs[kMaxGenCand];
  float agl2D[kMaxGenCand];
  float agl2D_abs[kMaxGenCand];
  float dlos2D[kMaxGenCand];
  float dl2D[kMaxGenCand];
  float trk3Ddca[kMaxGenCand];
  float trk3DdcaErr[kMaxGenCand];
  float dca3D[kMaxGenCand];
  float dca3DErr[kMaxGenCand];
  float dca2D[kMaxGenCand];
  bool isSwap[kMaxGenCand];
  bool matchGEN[kMaxGenCand];
  int idBAnc_reco[kMaxGenCand];
  int idmom_reco[kMaxGenCand];
  float gen_agl_abs[kMaxGenCand];
  float gen_agl2D_abs[kMaxGenCand];
  float gen_dl[kMaxGenCand];
  float gen_dl2D[kMaxGenCand];
  float matchGen_D0dca3D_genPV_[kMaxGenCand];
  float matchGen_D0vx_[kMaxGenCand];
  float matchGen_D0vy_[kMaxGenCand];
  float matchGen_D0vz_[kMaxGenCand];

  float grand_mass[kMaxGenCand];
  float grand_VtxProb[kMaxGenCand];
  float grand_dlos[kMaxGenCand];
  float grand_dl[kMaxGenCand];
  float grand_dlerror[kMaxGenCand];
  float grand_agl[kMaxGenCand];
  float grand_vtxChi2[kMaxGenCand];
  float grand_ndf[kMaxGenCand];
  float grand_agl_abs[kMaxGenCand];
  float grand_agl2D[kMaxGenCand];
  float grand_agl2D_abs[kMaxGenCand];
  float grand_dlos2D[kMaxGenCand];
  float grand_dl2D[kMaxGenCand];

  float dzos1[kMaxGenCand];
  float dzos2[kMaxGenCand];
  float dxyos1[kMaxGenCand];
  float dxyos2[kMaxGenCand];
  float dzval1[kMaxGenCand];
  float dzval2[kMaxGenCand];
  float dxyval1[kMaxGenCand];
  float dxyval2[kMaxGenCand];
  float nhit1[kMaxGenCand];
  float nhit2[kMaxGenCand];
  bool trkquality1[kMaxGenCand];
  bool trkquality2[kMaxGenCand];
  float pt1[kMaxGenCand];
  float pt2[kMaxGenCand];
  float ptErr1[kMaxGenCand];
  float ptErr2[kMaxGenCand];
  float p1[kMaxGenCand];
  float p2[kMaxGenCand];
  float massD2[kMaxGenCand];
  float eta1[kMaxGenCand];
  float eta2[kMaxGenCand];
  float phi1[kMaxGenCand];
  float phi2[kMaxGenCand];
  int charge1[kMaxGenCand];
  int charge2[kMaxGenCand];
  int pid1[kMaxGenCand];
  int pid2[kMaxGenCand];
  int pid3[kMaxGenCand];
  float tof1[kMaxGenCand];
  float tof2[kMaxGenCand];
  float H2dedx1[kMaxGenCand];
  float H2dedx2[kMaxGenCand];
  float T4dedx1[kMaxGenCand];
  float T4dedx2[kMaxGenCand];
  float trkChi1[kMaxGenCand];
  float trkChi2[kMaxGenCand];

  float grand_dzos1[kMaxGenCand];
  float grand_dzos2[kMaxGenCand];
  float grand_dxyos1[kMaxGenCand];
  float grand_dxyos2[kMaxGenCand];
  float grand_nhit1[kMaxGenCand];
  float grand_nhit2[kMaxGenCand];
  bool grand_trkquality1[kMaxGenCand];
  bool grand_trkquality2[kMaxGenCand];
  float grand_pt1[kMaxGenCand];
  float grand_pt2[kMaxGenCand];
  float grand_ptErr1[kMaxGenCand];
  float grand_ptErr2[kMaxGenCand];
  float grand_mass1[kMaxGenCand];
  float grand_mass2[kMaxGenCand];
  float grand_p1[kMaxGenCand];
  float grand_p2[kMaxGenCand];
  float grand_eta1[kMaxGenCand];
  float grand_eta2[kMaxGenCand];
  float grand_phi1[kMaxGenCand];
  float grand_phi2[kMaxGenCand];
  int grand_charge1[kMaxGenCand];
  int grand_charge2[kMaxGenCand];
  float grand_H2dedx1[kMaxGenCand];
  float grand_H2dedx2[kMaxGenCand];
  float grand_T4dedx1[kMaxGenCand];
  float grand_T4dedx2[kMaxGenCand];
  float grand_trkChi1[kMaxGenCand];
  float grand_trkChi2[kMaxGenCand];

  int candSize_gen;
  float mass_gen[kMaxGenCand];
  float pt_gen[kMaxGenCand];
  float eta_gen[kMaxGenCand];
  float phi_gen[kMaxGenCand];
  int status_gen[kMaxGenCand];
  int pdgId_gen[kMaxGenCand];
  int charge_gen[kMaxGenCand];
  int idmom[kMaxGenCand];
  float y_gen[kMaxGenCand];
  int iddau1[kMaxGenCand];
  int iddau2[kMaxGenCand];
  float gen_D0pT_[kMaxGenCand];
  float gen_D0eta_[kMaxGenCand];
  float gen_D0phi_[kMaxGenCand];
  float gen_D0mass_[kMaxGenCand];
  float gen_D0y_[kMaxGenCand];
  int gen_D0charge_[kMaxGenCand];
  int gen_D0pdgId_[kMaxGenCand];
  int gen_D0ancestorId_[kMaxGenCand];
  int gen_D0ancestorFlavor_[kMaxGenCand];
  float gen_D0Dau1_pT_[kMaxGenCand];
  float gen_D0Dau1_eta_[kMaxGenCand];
  float gen_D0Dau1_phi_[kMaxGenCand];
  float gen_D0Dau1_mass_[kMaxGenCand];
  float gen_D0Dau1_y_[kMaxGenCand];
  int gen_D0Dau1_charge_[kMaxGenCand];
  int gen_D0Dau1_pdgId_[kMaxGenCand];
  float gen_D0Dau2_pT_[kMaxGenCand];
  float gen_D0Dau2_eta_[kMaxGenCand];
  float gen_D0Dau2_phi_[kMaxGenCand];
  float gen_D0Dau2_mass_[kMaxGenCand];
  float gen_D0Dau2_y_[kMaxGenCand];
  int gen_D0Dau2_charge_[kMaxGenCand];
  int gen_D0Dau2_pdgId_[kMaxGenCand];
  float gen_D1pT_[kMaxGenCand];
  float gen_D1eta_[kMaxGenCand];
  float gen_D1phi_[kMaxGenCand];
  float gen_D1mass_[kMaxGenCand];
  float gen_D1y_[kMaxGenCand];
  int gen_D1charge_[kMaxGenCand];
  int gen_D1pdgId_[kMaxGenCand];
  bool gen_validDstarChain_[kMaxGenCand];
  bool gen_validD0chain_[kMaxGenCand];

  float matchGen_DStarpT_[kMaxGenCand];
  float matchGen_DStareta_[kMaxGenCand];
  float matchGen_DStarphi_[kMaxGenCand];
  float matchGen_DStarmass_[kMaxGenCand];
  float matchGen_DStary_[kMaxGenCand];
  int matchGen_DStarcharge_[kMaxGenCand];
  int matchGen_DStarpdgId_[kMaxGenCand];

  float matchGen_D0pT_[kMaxGenCand];
  float matchGen_D0eta_[kMaxGenCand];
  float matchGen_D0phi_[kMaxGenCand];
  float matchGen_D0mass_[kMaxGenCand];
  float matchGen_D0y_[kMaxGenCand];
  int matchGen_D0charge_[kMaxGenCand];
  int matchGen_D0pdgId_[kMaxGenCand];

  float matchGen_D0Dau1_pT_[kMaxGenCand];
  float matchGen_D0Dau1_eta_[kMaxGenCand];
  float matchGen_D0Dau1_phi_[kMaxGenCand];
  float matchGen_D0Dau1_mass_[kMaxGenCand];
  float matchGen_D0Dau1_y_[kMaxGenCand];
  int matchGen_D0Dau1_charge_[kMaxGenCand];
  int matchGen_D0Dau1_pdgId_[kMaxGenCand];

  float matchGen_D0Dau2_pT_[kMaxGenCand];
  float matchGen_D0Dau2_eta_[kMaxGenCand];
  float matchGen_D0Dau2_phi_[kMaxGenCand];
  float matchGen_D0Dau2_mass_[kMaxGenCand];
  float matchGen_D0Dau2_y_[kMaxGenCand];
  int matchGen_D0Dau2_charge_[kMaxGenCand];
  int matchGen_D0Dau2_pdgId_[kMaxGenCand];

  float matchGen_D1pT_[kMaxGenCand];
  float matchGen_D1eta_[kMaxGenCand];
  float matchGen_D1phi_[kMaxGenCand];
  float matchGen_D1mass_[kMaxGenCand];
  float matchGen_D1y_[kMaxGenCand];
  float matchGen_D1decayLength2D_[kMaxGenCand];
  float matchGen_D1decayLength3D_[kMaxGenCand];
  float matchGen_D1angle2D_[kMaxGenCand];
  float matchGen_D1angle3D_[kMaxGenCand];
  int matchGen_D1ancestorId_[kMaxGenCand];
  int matchGen_D1ancestorFlavor_[kMaxGenCand];
  int matchGen_D1charge_[kMaxGenCand];
  int matchGen_D1pdgId_[kMaxGenCand];
  float matchGen_slowPion_dR_[kMaxGenCand];
  float matchGen_slowPion_rawTrack_dR_[kMaxGenCand];
  float matchGen_slowPion_rawTrackToCandidate_dR_[kMaxGenCand];
  float matchGen_slowPion_rawTrackPt_[kMaxGenCand];
  float matchGen_slowPion_candidatePtOverGenPt_[kMaxGenCand];
  float matchGen_slowPion_rawTrackPtOverGenPt_[kMaxGenCand];
  float matchGen_deltaMass_current_[kMaxGenCand];
  float matchGen_deltaMass_rawSlowPion_[kMaxGenCand];
  bool matchGen_slowPion_candidateMatched_[kMaxGenCand];
  bool matchGen_slowPion_rawTrackMatched_[kMaxGenCand];
  int matchGen_D0Dau1_motherPdgId_[kMaxGenCand];
  int matchGen_D0Dau1_motherNDau_[kMaxGenCand];
  int matchGen_D0Dau2_motherPdgId_[kMaxGenCand];
  int matchGen_D0Dau2_motherNDau_[kMaxGenCand];
  int matchGen_D1_motherPdgId_[kMaxGenCand];
  int matchGen_D1_motherNDau_[kMaxGenCand];
  bool matchGen_validDstarChain_[kMaxGenCand];
  bool matchGen_validD0chain_[kMaxGenCand];

  unsigned int nEventsProcessed_;
  unsigned int nEventsFillRECO_;
  unsigned int nRecoCandidatesProcessed_;
  unsigned int nRecoCandidatesMatched_;
  unsigned int nEventsFillGEN_;
  std::array<std::array<unsigned long long, 4>, 5> dstarGenMatchTailCounts_;
  std::array<std::array<unsigned long long, 4>, 4> dstarTailAncestryCounts_;
  std::array<std::array<unsigned long long, 4>, 9> dstarSlowPionRecoDiagnosisCounts_;
  std::array<std::array<unsigned long long, 4>, 4> dstarCurrentVsRawDeltaMassCounts_;
  int dstarTailMaxPrint_;
  unsigned int dstarTailAncestryPrintCount_;
  unsigned int dstarSlowPionDiagnosisPrintCount_;
};

#endif
