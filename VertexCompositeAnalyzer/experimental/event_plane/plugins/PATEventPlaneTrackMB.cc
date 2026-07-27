// system include files
#include <algorithm>
#include <cmath>
#include <memory>
#include <string>
#include <vector>

#include <TTree.h>

// user include files
#include "FWCore/Common/interface/TriggerNames.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/Frameworkfwd.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/Framework/interface/Run.h"
#include "FWCore/Framework/interface/one/EDAnalyzer.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/Utilities/interface/InputTag.h"

#include "CommonTools/UtilAlgos/interface/TFileService.h"

#include "DataFormats/BeamSpot/interface/BeamSpot.h"
#include "DataFormats/Common/interface/TriggerResults.h"
#include "DataFormats/HeavyIonEvent/interface/Centrality.h"
#include "DataFormats/HeavyIonEvent/interface/CentralityBins.h"
#include "DataFormats/HeavyIonEvent/interface/EvtPlane.h"
#include "DataFormats/Math/interface/deltaR.h"
#include "DataFormats/PatCandidates/interface/CompositeCandidate.h"
#include "DataFormats/RecoCandidate/interface/RecoCandidate.h"
#include "DataFormats/TrackReco/interface/Track.h"
#include "DataFormats/TrackReco/interface/TrackFwd.h"
#include "DataFormats/VertexReco/interface/Vertex.h"
#include "DataFormats/VertexReco/interface/VertexFwd.h"
#include "DataFormats/Provenance/interface/ProductID.h"

#define PI 3.1416

class PATEventPlaneTrackMB : public edm::one::EDAnalyzer<edm::one::WatchRuns> {
public:
  explicit PATEventPlaneTrackMB(const edm::ParameterSet&);
  ~PATEventPlaneTrackMB() override = default;

private:
  void beginJob() override;
  void beginRun(const edm::Run&, const edm::EventSetup&) override;
  void endRun(const edm::Run&, const edm::EventSetup&) override {}
  void analyze(const edm::Event&, const edm::EventSetup&) override;
  void endJob() override;

  void fillRECO(const edm::Event&, const edm::EventSetup&);
  void initTree();
  void collectFinalStateDaughters(const reco::Candidate& cand);
  void resetReproducedEventPlane();
  void fillReproducedEventPlane(const edm::Event& iEvent);
  float safeNorm(float q, float w) const { return (w > 0.f ? q / w : -999.f); }

  edm::Service<TFileService> fs;
  TTree* eventPlaneTree_;

  bool doRecoNtuple_;
  bool saveTree_;
  bool isCentrality_;
  bool hasCompositeCandidatesToken_;
  bool hasEventplaneSrcToken_;

  double massMinForExclusion_;
  double massMaxForExclusion_;

  unsigned int runNb;
  unsigned int eventNb;
  unsigned int lsNb;
  short centrality;
  int Ntrkoffline;
  int NtrkHP;
  short nPV;
  float bestvx;
  float bestvy;
  float bestvz;
  float bestvxError;
  float bestvyError;
  float bestvzError;

  Float_t trkQx;
  Float_t trkQy;
  Float_t all_trkQx;
  Float_t all_trkQy;
  Float_t all_trkW;
  Float_t all_trkQx_forw;
  Float_t all_trkQy_forw;
  Float_t all_trkW_forw;
  Float_t all_trkQx_afterw;
  Float_t all_trkQy_afterw;
  Float_t all_trkW_afterw;
  Float_t all_trkQx_eta16;
  Float_t all_trkQy_eta16;
  Float_t all_trkW_eta16;
  Float_t all_trkQx_forw_eta16;
  Float_t all_trkQy_forw_eta16;
  Float_t all_trkW_forw_eta16;
  Float_t all_trkQx_afterw_eta16;
  Float_t all_trkQy_afterw_eta16;
  Float_t all_trkW_afterw_eta16;
  Float_t all_trkQx_eta08;
  Float_t all_trkQy_eta08;
  Float_t all_trkW_eta08;
  Float_t all_trkQx_forw_eta08;
  Float_t all_trkQy_forw_eta08;
  Float_t all_trkW_forw_eta08;
  Float_t all_trkQx_afterw_eta08;
  Float_t all_trkQy_afterw_eta08;
  Float_t all_trkW_afterw_eta08;
  Float_t trkQx_forw;
  Float_t trkQy_forw;
  Float_t trkQx_afterw;
  Float_t trkQy_afterw;
  Float_t trkQx_v3;
  Float_t trkQy_v3;
  Float_t trkQx_v3_forw;
  Float_t trkQy_v3_forw;
  Float_t trkQx_v3_afterw;
  Float_t trkQy_v3_afterw;
  Float_t trkQx_eta16;
  Float_t trkQy_eta16;
  Float_t trkQx_forw_eta16;
  Float_t trkQy_forw_eta16;
  Float_t trkQx_afterw_eta16;
  Float_t trkQy_afterw_eta16;
  Float_t trkQx_v3_eta16;
  Float_t trkQy_v3_eta16;
  Float_t trkQx_v3_forw_eta16;
  Float_t trkQy_v3_forw_eta16;
  Float_t trkQx_v3_afterw_eta16;
  Float_t trkQy_v3_afterw_eta16;
  Float_t trkQx_eta08;
  Float_t trkQy_eta08;
  Float_t trkQx_forw_eta08;
  Float_t trkQy_forw_eta08;
  Float_t trkQx_afterw_eta08;
  Float_t trkQy_afterw_eta08;
  Float_t trkQx_v3_eta08;
  Float_t trkQy_v3_eta08;
  Float_t trkQx_v3_forw_eta08;
  Float_t trkQy_v3_forw_eta08;
  Float_t trkQx_v3_afterw_eta08;
  Float_t trkQy_v3_afterw_eta08;

  Float_t ephfpAngle[2];
  Float_t ephfmAngle[2];
  Float_t ephfpQ[2];
  Float_t ephfmQ[2];
  Float_t ephfpSumW;
  Float_t ephfmSumW;
  Float_t ephfpSumWSub[2];
  Float_t ephfmSumWSub[2];
  Float_t ephfmAngleoff[2];
  Float_t ephfpAngleoff[2];
  Float_t ephfmAngleRaw[2];
  Float_t ephfmsumCosRaw[2];
  Float_t ephfmsumSinRaw[2];
  Float_t ephfmsumCos[2];
  Float_t ephfmsumSin[2];
  Float_t ephfmsumPtOrEt[2];
  Float_t ephfpAngleRaw[2];
  Float_t ephfpsumCosRaw[2];
  Float_t ephfpsumSinRaw[2];
  Float_t ephfpsumCos[2];
  Float_t ephfpsumSin[2];
  Float_t ephfpsumPtOrEt[2];
  Float_t eptrackmidAngle[2];
  Float_t eptrackmidAngleoff[2];
  Float_t eptrackmidAngleRaw[2];
  Float_t eptrackmidQ[2];
  Float_t eptrackmidSumCos[2];
  Float_t eptrackmidSumSin[2];
  Float_t eptrackmidSumCosRaw[2];
  Float_t eptrackmidSumSinRaw[2];
  Float_t eptrackmidSumW;
  Float_t eptrackmidSumWSub[2];
  Float_t eptrackmidSumPtOrEt[2];
  Float_t eptrackpAngle[2];
  Float_t eptrackpAngleoff[2];
  Float_t eptrackpAngleRaw[2];
  Float_t eptrackpQ[2];
  Float_t eptrackpSumCos[2];
  Float_t eptrackpSumSin[2];
  Float_t eptrackpSumCosRaw[2];
  Float_t eptrackpSumSinRaw[2];
  Float_t eptrackpSumW[2];
  Float_t eptrackpSumPtOrEt[2];
  Float_t eptrackmAngle[2];
  Float_t eptrackmAngleoff[2];
  Float_t eptrackmAngleRaw[2];
  Float_t eptrackmQ[2];
  Float_t eptrackmSumCos[2];
  Float_t eptrackmSumSin[2];
  Float_t eptrackmSumCosRaw[2];
  Float_t eptrackmSumSinRaw[2];
  Float_t eptrackmSumW[2];
  Float_t eptrackmSumPtOrEt[2];
  Float_t ephfAngle[2];
  Float_t ephfAngleoff[2];
  Float_t ephfAngleRaw[2];
  Float_t ephfQ[2];
  Float_t ephfSumW;
  Float_t ephfSumWSub[2];
  Float_t ephfsumCos[2];
  Float_t ephfsumSin[2];
  Float_t ephfsumCosRaw[2];
  Float_t ephfsumSinRaw[2];
  Float_t ephfsumPtOrEt[2];

  edm::EDGetTokenT<reco::BeamSpot> tok_offlineBS_;
  edm::EDGetTokenT<reco::VertexCollection> tok_offlinePV_;
  edm::EDGetTokenT<reco::TrackCollection> tok_tracks_;
  edm::EDGetTokenT<pat::CompositeCandidateCollection> tok_compositeCandidates_;
  edm::EDGetTokenT<reco::EvtPlaneCollection> tok_eventplaneSrc_;
  edm::EDGetTokenT<int> tok_centBinLabel_;
  edm::EDGetTokenT<reco::Centrality> tok_centSrc_;

  std::vector<float> dauEta;
  std::vector<float> dauPhi;
  std::vector<float> dauPt;
  std::vector<bool> dauHasTrackRef;
  std::vector<edm::ProductID> dauTrackProductId;
  std::vector<unsigned int> dauTrackKey;
};

PATEventPlaneTrackMB::PATEventPlaneTrackMB(const edm::ParameterSet& iConfig)
    : eventPlaneTree_(nullptr),
      hasCompositeCandidatesToken_(false),
      hasEventplaneSrcToken_(false) {
  doRecoNtuple_ = iConfig.getUntrackedParameter<bool>("doRecoNtuple", true);
  saveTree_ = iConfig.getUntrackedParameter<bool>("saveTree", true);
  isCentrality_ = (iConfig.exists("isCentrality") ? iConfig.getParameter<bool>("isCentrality")
                                                  : iConfig.getUntrackedParameter<bool>("isCentrality", false));

  if (iConfig.existsAs<double>("massMinForExclusion")) {
    massMinForExclusion_ = iConfig.getParameter<double>("massMinForExclusion");
  } else {
    massMinForExclusion_ = iConfig.getUntrackedParameter<double>("massMinForExclusion", 1.7);
  }
  if (iConfig.existsAs<double>("massMaxForExclusion")) {
    massMaxForExclusion_ = iConfig.getParameter<double>("massMaxForExclusion");
  } else {
    massMaxForExclusion_ = iConfig.getUntrackedParameter<double>("massMaxForExclusion", 2.1);
  }

  tok_offlineBS_ =
      consumes<reco::BeamSpot>(iConfig.getUntrackedParameter<edm::InputTag>("beamSpotSrc", edm::InputTag("offlineBeamSpot")));
  tok_offlinePV_ = consumes<reco::VertexCollection>(
      iConfig.getUntrackedParameter<edm::InputTag>("VertexCollection", edm::InputTag("offlinePrimaryVertices")));
  tok_tracks_ = consumes<reco::TrackCollection>(
      edm::InputTag(iConfig.getUntrackedParameter<edm::InputTag>("TrackCollection", edm::InputTag("generalTracks"))));

  const auto compTag =
      iConfig.getUntrackedParameter<edm::InputTag>("VertexCompositeCollection", edm::InputTag());
  if (!compTag.label().empty()) {
    tok_compositeCandidates_ = consumes<pat::CompositeCandidateCollection>(compTag);
    hasCompositeCandidatesToken_ = true;
  }

  edm::InputTag epTag;
  if (iConfig.existsAs<edm::InputTag>("eventplaneSrcRecalc")) {
    epTag = iConfig.getParameter<edm::InputTag>("eventplaneSrcRecalc");
  } else {
    epTag = iConfig.getUntrackedParameter<edm::InputTag>("eventplaneSrcRecalc", edm::InputTag());
  }
  if (epTag.label().empty()) {
    if (iConfig.existsAs<edm::InputTag>("eventplaneSrc")) {
      epTag = iConfig.getParameter<edm::InputTag>("eventplaneSrc");
    } else {
      epTag = iConfig.getUntrackedParameter<edm::InputTag>("eventplaneSrc", edm::InputTag());
    }
  }
  if (!epTag.label().empty()) {
    tok_eventplaneSrc_ = consumes<reco::EvtPlaneCollection>(epTag);
    hasEventplaneSrcToken_ = true;
  }

  if (isCentrality_) {
    tok_centBinLabel_ = consumes<int>(
        iConfig.existsAs<edm::InputTag>("centralityBinLabel")
            ? iConfig.getParameter<edm::InputTag>("centralityBinLabel")
            : iConfig.getUntrackedParameter<edm::InputTag>("centralityBinLabel", edm::InputTag()));
    tok_centSrc_ = consumes<reco::Centrality>(
        iConfig.existsAs<edm::InputTag>("centralitySrc")
            ? iConfig.getParameter<edm::InputTag>("centralitySrc")
            : iConfig.getUntrackedParameter<edm::InputTag>("centralitySrc", edm::InputTag()));
  }
}

void PATEventPlaneTrackMB::beginJob() {
  if (!doRecoNtuple_) {
    throw cms::Exception("PATEventPlaneTrackMB") << "No output configured for RECO mode.";
  }
  if (saveTree_) initTree();
}

void PATEventPlaneTrackMB::beginRun(const edm::Run&, const edm::EventSetup&) {}

void PATEventPlaneTrackMB::analyze(const edm::Event& iEvent, const edm::EventSetup& iSetup) {
  if (doRecoNtuple_) fillRECO(iEvent, iSetup);
  if (saveTree_ && eventPlaneTree_) eventPlaneTree_->Fill();
}

void PATEventPlaneTrackMB::endJob() {}

void PATEventPlaneTrackMB::collectFinalStateDaughters(const reco::Candidate& cand) {
  if (cand.numberOfDaughters() == 0) {
    if (cand.charge() == 0) return;
    bool hasTrackRef = false;
    edm::ProductID productId;
    unsigned int key = 0;
    const auto* recoCand = dynamic_cast<const reco::RecoCandidate*>(&cand);
    if (recoCand) {
      const reco::TrackRef trkRef = recoCand->track();
      if (trkRef.isNonnull()) {
        hasTrackRef = true;
        productId = trkRef.id();
        key = trkRef.key();
      }
    }
    const reco::Track* bestTrack = cand.bestTrack();
    dauEta.push_back(bestTrack ? bestTrack->eta() : cand.eta());
    dauPhi.push_back(bestTrack ? bestTrack->phi() : cand.phi());
    dauPt.push_back(bestTrack ? bestTrack->pt() : cand.pt());
    dauHasTrackRef.push_back(hasTrackRef);
    dauTrackProductId.push_back(productId);
    dauTrackKey.push_back(key);
    return;
  }

  for (size_t i = 0; i < cand.numberOfDaughters(); ++i) {
    const reco::Candidate* daughter = cand.daughter(i);
    if (daughter) collectFinalStateDaughters(*daughter);
  }
}

void PATEventPlaneTrackMB::resetReproducedEventPlane() {
  constexpr float kInvalid = -99.f;
  auto fill2 = [kInvalid](float (&arr)[2]) {
    arr[0] = kInvalid;
    arr[1] = kInvalid;
  };

  fill2(ephfpAngle);
  fill2(ephfmAngle);
  fill2(ephfpQ);
  fill2(ephfmQ);
  ephfpSumW = kInvalid;
  ephfmSumW = kInvalid;
  fill2(ephfpSumWSub);
  fill2(ephfmSumWSub);
  fill2(ephfmAngleoff);
  fill2(ephfpAngleoff);
  fill2(ephfmAngleRaw);
  fill2(ephfmsumCosRaw);
  fill2(ephfmsumSinRaw);
  fill2(ephfmsumCos);
  fill2(ephfmsumSin);
  fill2(ephfmsumPtOrEt);
  fill2(ephfpAngleRaw);
  fill2(ephfpsumCosRaw);
  fill2(ephfpsumSinRaw);
  fill2(ephfpsumCos);
  fill2(ephfpsumSin);
  fill2(ephfpsumPtOrEt);
  fill2(eptrackmidAngle);
  fill2(eptrackmidAngleoff);
  fill2(eptrackmidAngleRaw);
  fill2(eptrackmidQ);
  fill2(eptrackmidSumCos);
  fill2(eptrackmidSumSin);
  fill2(eptrackmidSumCosRaw);
  fill2(eptrackmidSumSinRaw);
  eptrackmidSumW = kInvalid;
  fill2(eptrackmidSumWSub);
  fill2(eptrackmidSumPtOrEt);
  fill2(eptrackpAngle);
  fill2(eptrackpAngleoff);
  fill2(eptrackpAngleRaw);
  fill2(eptrackpQ);
  fill2(eptrackpSumCos);
  fill2(eptrackpSumSin);
  fill2(eptrackpSumCosRaw);
  fill2(eptrackpSumSinRaw);
  fill2(eptrackpSumW);
  fill2(eptrackpSumPtOrEt);
  fill2(eptrackmAngle);
  fill2(eptrackmAngleoff);
  fill2(eptrackmAngleRaw);
  fill2(eptrackmQ);
  fill2(eptrackmSumCos);
  fill2(eptrackmSumSin);
  fill2(eptrackmSumCosRaw);
  fill2(eptrackmSumSinRaw);
  fill2(eptrackmSumW);
  fill2(eptrackmSumPtOrEt);
  fill2(ephfAngle);
  fill2(ephfAngleoff);
  fill2(ephfAngleRaw);
  fill2(ephfQ);
  ephfSumW = kInvalid;
  fill2(ephfSumWSub);
  fill2(ephfsumCos);
  fill2(ephfsumSin);
  fill2(ephfsumCosRaw);
  fill2(ephfsumSinRaw);
  fill2(ephfsumPtOrEt);
}

void PATEventPlaneTrackMB::fillReproducedEventPlane(const edm::Event& iEvent) {
  resetReproducedEventPlane();
  if (!hasEventplaneSrcToken_) return;

  edm::Handle<reco::EvtPlaneCollection> eventplanes;
  iEvent.getByToken(tok_eventplaneSrc_, eventplanes);
  const bool hasEP = eventplanes.isValid() && eventplanes->size() > 11;
  if (!hasEP) return;

  ephfmAngle[0] = (*eventplanes)[0].angle(2);
  ephfmAngle[1] = (*eventplanes)[6].angle(2);
  ephfpAngle[0] = (*eventplanes)[1].angle(2);
  ephfpAngle[1] = (*eventplanes)[7].angle(2);
  ephfAngle[0] = (*eventplanes)[2].angle(2);
  ephfAngle[1] = (*eventplanes)[8].angle(2);

  eptrackmidAngle[0] = (*eventplanes)[3].angle(2);
  eptrackmidAngle[1] = (*eventplanes)[9].angle(2);
  eptrackpAngle[0] = (*eventplanes)[4].angle(2);
  eptrackpAngle[1] = (*eventplanes)[10].angle(2);
  eptrackmAngle[0] = (*eventplanes)[5].angle(2);
  eptrackmAngle[1] = (*eventplanes)[11].angle(2);

  eptrackmidAngleoff[0] = (*eventplanes)[3].angle(1);
  eptrackmidAngleoff[1] = (*eventplanes)[9].angle(1);
  eptrackpAngleoff[0] = (*eventplanes)[4].angle(1);
  eptrackpAngleoff[1] = (*eventplanes)[10].angle(1);
  eptrackmAngleoff[0] = (*eventplanes)[5].angle(1);
  eptrackmAngleoff[1] = (*eventplanes)[11].angle(1);

  eptrackmidAngleRaw[0] = (*eventplanes)[3].angle(0);
  eptrackmidAngleRaw[1] = (*eventplanes)[9].angle(0);
  eptrackpAngleRaw[0] = (*eventplanes)[4].angle(0);
  eptrackpAngleRaw[1] = (*eventplanes)[10].angle(0);
  eptrackmAngleRaw[0] = (*eventplanes)[5].angle(0);
  eptrackmAngleRaw[1] = (*eventplanes)[11].angle(0);

  ephfAngleoff[0] = (*eventplanes)[2].angle(1);
  ephfAngleoff[1] = (*eventplanes)[8].angle(1);
  ephfmAngleoff[0] = (*eventplanes)[0].angle(1);
  ephfmAngleoff[1] = (*eventplanes)[6].angle(1);
  ephfpAngleoff[0] = (*eventplanes)[1].angle(1);
  ephfpAngleoff[1] = (*eventplanes)[7].angle(1);

  ephfAngleRaw[0] = (*eventplanes)[2].angle(0);
  ephfAngleRaw[1] = (*eventplanes)[8].angle(0);
  ephfmAngleRaw[0] = (*eventplanes)[0].angle(0);
  ephfmAngleRaw[1] = (*eventplanes)[6].angle(0);
  ephfpAngleRaw[0] = (*eventplanes)[1].angle(0);
  ephfpAngleRaw[1] = (*eventplanes)[7].angle(0);

  ephfmQ[0] = (*eventplanes)[0].q(2);
  ephfmQ[1] = (*eventplanes)[6].q(2);
  ephfpQ[0] = (*eventplanes)[1].q(2);
  ephfpQ[1] = (*eventplanes)[7].q(2);
  ephfQ[0] = (*eventplanes)[2].q(2);
  ephfQ[1] = (*eventplanes)[8].q(2);
  eptrackmidQ[0] = (*eventplanes)[3].q(2);
  eptrackmidQ[1] = (*eventplanes)[9].q(2);
  eptrackpQ[0] = (*eventplanes)[4].q(2);
  eptrackpQ[1] = (*eventplanes)[10].q(2);
  eptrackmQ[0] = (*eventplanes)[5].q(2);
  eptrackmQ[1] = (*eventplanes)[11].q(2);

  ephfmSumW = (*eventplanes)[6].sumw();
  ephfpSumW = (*eventplanes)[7].sumw();
  ephfSumW = (*eventplanes)[8].sumw();
  ephfmSumWSub[0] = (*eventplanes)[0].sumw();
  ephfmSumWSub[1] = (*eventplanes)[6].sumw();
  ephfpSumWSub[0] = (*eventplanes)[1].sumw();
  ephfpSumWSub[1] = (*eventplanes)[7].sumw();
  ephfSumWSub[0] = (*eventplanes)[2].sumw();
  ephfSumWSub[1] = (*eventplanes)[8].sumw();
  eptrackmidSumW = (*eventplanes)[3].sumw();
  eptrackmidSumWSub[0] = (*eventplanes)[3].sumw();
  eptrackmidSumWSub[1] = (*eventplanes)[9].sumw();
  eptrackpSumW[0] = (*eventplanes)[4].sumw();
  eptrackpSumW[1] = (*eventplanes)[10].sumw();
  eptrackmSumW[0] = (*eventplanes)[5].sumw();
  eptrackmSumW[1] = (*eventplanes)[11].sumw();

  ephfmsumCosRaw[0] = (*eventplanes)[0].sumCos(0);
  ephfmsumCosRaw[1] = (*eventplanes)[6].sumCos(0);
  ephfmsumSinRaw[0] = (*eventplanes)[0].sumSin(0);
  ephfmsumSinRaw[1] = (*eventplanes)[6].sumSin(0);
  ephfmsumCos[0] = (*eventplanes)[0].sumCos(2);
  ephfmsumCos[1] = (*eventplanes)[6].sumCos(2);
  ephfmsumSin[0] = (*eventplanes)[0].sumSin(2);
  ephfmsumSin[1] = (*eventplanes)[6].sumSin(2);
  ephfmsumPtOrEt[0] = (*eventplanes)[0].sumPtOrEt();
  ephfmsumPtOrEt[1] = (*eventplanes)[6].sumPtOrEt();

  ephfpsumCosRaw[0] = (*eventplanes)[1].sumCos(0);
  ephfpsumCosRaw[1] = (*eventplanes)[7].sumCos(0);
  ephfpsumSinRaw[0] = (*eventplanes)[1].sumSin(0);
  ephfpsumSinRaw[1] = (*eventplanes)[7].sumSin(0);
  ephfpsumCos[0] = (*eventplanes)[1].sumCos(2);
  ephfpsumCos[1] = (*eventplanes)[7].sumCos(2);
  ephfpsumSin[0] = (*eventplanes)[1].sumSin(2);
  ephfpsumSin[1] = (*eventplanes)[7].sumSin(2);
  ephfpsumPtOrEt[0] = (*eventplanes)[1].sumPtOrEt();
  ephfpsumPtOrEt[1] = (*eventplanes)[7].sumPtOrEt();

  ephfsumCosRaw[0] = (*eventplanes)[2].sumCos(0);
  ephfsumCosRaw[1] = (*eventplanes)[8].sumCos(0);
  ephfsumSinRaw[0] = (*eventplanes)[2].sumSin(0);
  ephfsumSinRaw[1] = (*eventplanes)[8].sumSin(0);
  ephfsumCos[0] = (*eventplanes)[2].sumCos(2);
  ephfsumCos[1] = (*eventplanes)[8].sumCos(2);
  ephfsumSin[0] = (*eventplanes)[2].sumSin(2);
  ephfsumSin[1] = (*eventplanes)[8].sumSin(2);
  ephfsumPtOrEt[0] = (*eventplanes)[2].sumPtOrEt();
  ephfsumPtOrEt[1] = (*eventplanes)[8].sumPtOrEt();

  eptrackmidSumCosRaw[0] = (*eventplanes)[3].sumCos(0);
  eptrackmidSumCosRaw[1] = (*eventplanes)[9].sumCos(0);
  eptrackmidSumSinRaw[0] = (*eventplanes)[3].sumSin(0);
  eptrackmidSumSinRaw[1] = (*eventplanes)[9].sumSin(0);
  eptrackmidSumCos[0] = (*eventplanes)[3].sumCos(2);
  eptrackmidSumCos[1] = (*eventplanes)[9].sumCos(2);
  eptrackmidSumSin[0] = (*eventplanes)[3].sumSin(2);
  eptrackmidSumSin[1] = (*eventplanes)[9].sumSin(2);
  eptrackmidSumPtOrEt[0] = (*eventplanes)[3].sumPtOrEt();
  eptrackmidSumPtOrEt[1] = (*eventplanes)[9].sumPtOrEt();

  eptrackpSumCosRaw[0] = (*eventplanes)[4].sumCos(0);
  eptrackpSumCosRaw[1] = (*eventplanes)[10].sumCos(0);
  eptrackpSumSinRaw[0] = (*eventplanes)[4].sumSin(0);
  eptrackpSumSinRaw[1] = (*eventplanes)[10].sumSin(0);
  eptrackpSumCos[0] = (*eventplanes)[4].sumCos(2);
  eptrackpSumCos[1] = (*eventplanes)[10].sumCos(2);
  eptrackpSumSin[0] = (*eventplanes)[4].sumSin(2);
  eptrackpSumSin[1] = (*eventplanes)[10].sumSin(2);
  eptrackpSumPtOrEt[0] = (*eventplanes)[4].sumPtOrEt();
  eptrackpSumPtOrEt[1] = (*eventplanes)[10].sumPtOrEt();

  eptrackmSumCosRaw[0] = (*eventplanes)[5].sumCos(0);
  eptrackmSumCosRaw[1] = (*eventplanes)[11].sumCos(0);
  eptrackmSumSinRaw[0] = (*eventplanes)[5].sumSin(0);
  eptrackmSumSinRaw[1] = (*eventplanes)[11].sumSin(0);
  eptrackmSumCos[0] = (*eventplanes)[5].sumCos(2);
  eptrackmSumCos[1] = (*eventplanes)[11].sumCos(2);
  eptrackmSumSin[0] = (*eventplanes)[5].sumSin(2);
  eptrackmSumSin[1] = (*eventplanes)[11].sumSin(2);
  eptrackmSumPtOrEt[0] = (*eventplanes)[5].sumPtOrEt();
  eptrackmSumPtOrEt[1] = (*eventplanes)[11].sumPtOrEt();
}

void PATEventPlaneTrackMB::fillRECO(const edm::Event& iEvent, const edm::EventSetup&) {
  edm::Handle<reco::BeamSpot> beamspot;
  iEvent.getByToken(tok_offlineBS_, beamspot);
  edm::Handle<reco::VertexCollection> vertices;
  iEvent.getByToken(tok_offlinePV_, vertices);
  if (!vertices.isValid()) {
    throw cms::Exception("PATEventPlaneTrackMB") << "Primary vertex collection not found.";
  }

  edm::Handle<reco::TrackCollection> trackColl;
  iEvent.getByToken(tok_tracks_, trackColl);
  if (!trackColl.isValid()) {
    throw cms::Exception("PATEventPlaneTrackMB") << "Track collection not found.";
  }

  runNb = iEvent.id().run();
  eventNb = iEvent.id().event();
  lsNb = iEvent.luminosityBlock();

  centrality = -1;
  Ntrkoffline = -1;
  if (isCentrality_) {
    const auto& cent = iEvent.getHandle(tok_centSrc_);
    Ntrkoffline = (cent.isValid() ? cent->Ntracks() : -1);
    edm::Handle<int> cbin;
    iEvent.getByToken(tok_centBinLabel_, cbin);
    centrality = (cbin.isValid() ? *cbin : -1);
  }

  NtrkHP = 0;
  for (const auto& trk : *trackColl) {
    if (trk.quality(reco::TrackBase::highPurity)) ++NtrkHP;
  }

  nPV = vertices->size();
  const auto& vtxPrimary = (!vertices->empty() ? (*vertices)[0] : reco::Vertex());
  const bool isPV = (!vtxPrimary.isFake() && vtxPrimary.tracksSize() >= 2);
  const auto& bs =
      (!isPV ? reco::Vertex(beamspot->position(), beamspot->covariance3D()) : reco::Vertex());
  const reco::Vertex& vtx = (isPV ? vtxPrimary : bs);
  bestvz = vtx.z();
  bestvx = vtx.x();
  bestvy = vtx.y();
  bestvzError = vtx.zError();
  bestvxError = vtx.xError();
  bestvyError = vtx.yError();
  const math::XYZPoint bestvtx(bestvx, bestvy, bestvz);

  dauEta.clear();
  dauPhi.clear();
  dauPt.clear();
  dauHasTrackRef.clear();
  dauTrackProductId.clear();
  dauTrackKey.clear();

  if (hasCompositeCandidatesToken_) {
    edm::Handle<pat::CompositeCandidateCollection> v0candidates;
    iEvent.getByToken(tok_compositeCandidates_, v0candidates);
    if (!v0candidates.isValid()) {
      throw cms::Exception("PATEventPlaneTrackMB") << "Configured VertexCompositeCollection was not found.";
    }

    for (const auto& cand : *v0candidates) {
      const float mass = cand.mass();
      if (mass > massMinForExclusion_ && mass < massMaxForExclusion_) {
        collectFinalStateDaughters(cand);
      }
    }
  }

  float trkqx = 0.f;
  float trkqy = 0.f;
  float trkPt = 0.f;
  float trkqx_forw = 0.f;
  float trkqy_forw = 0.f;
  float trkPt_forw = 0.f;
  float trkqx_afterw = 0.f;
  float trkqy_afterw = 0.f;
  float trkPt_afterw = 0.f;
  float trkqx_v3 = 0.f;
  float trkqy_v3 = 0.f;
  float trkqx_v3_forw = 0.f;
  float trkqy_v3_forw = 0.f;
  float trkqx_v3_afterw = 0.f;
  float trkqy_v3_afterw = 0.f;
  float trkqx_16 = 0.f;
  float trkqy_16 = 0.f;
  float trkPt_16 = 0.f;
  float trkqx_forw_16 = 0.f;
  float trkqy_forw_16 = 0.f;
  float trkPt_forw_16 = 0.f;
  float trkqx_afterw_16 = 0.f;
  float trkqy_afterw_16 = 0.f;
  float trkPt_afterw_16 = 0.f;
  float trkqx_v3_16 = 0.f;
  float trkqy_v3_16 = 0.f;
  float trkqx_v3_forw_16 = 0.f;
  float trkqy_v3_forw_16 = 0.f;
  float trkqx_v3_afterw_16 = 0.f;
  float trkqy_v3_afterw_16 = 0.f;
  float trkqx_08 = 0.f;
  float trkqy_08 = 0.f;
  float trkPt_08 = 0.f;
  float trkqx_forw_08 = 0.f;
  float trkqy_forw_08 = 0.f;
  float trkPt_forw_08 = 0.f;
  float trkqx_afterw_08 = 0.f;
  float trkqy_afterw_08 = 0.f;
  float trkPt_afterw_08 = 0.f;
  float trkqx_v3_08 = 0.f;
  float trkqy_v3_08 = 0.f;
  float trkqx_v3_forw_08 = 0.f;
  float trkqy_v3_forw_08 = 0.f;
  float trkqx_v3_afterw_08 = 0.f;
  float trkqy_v3_afterw_08 = 0.f;

  float all_trkqx = 0.f;
  float all_trkqy = 0.f;
  float all_trkPt = 0.f;
  float all_trkqx_forw = 0.f;
  float all_trkqy_forw = 0.f;
  float all_trkPt_forw = 0.f;
  float all_trkqx_afterw = 0.f;
  float all_trkqy_afterw = 0.f;
  float all_trkPt_afterw = 0.f;
  float all_trkqx_16 = 0.f;
  float all_trkqy_16 = 0.f;
  float all_trkPt_16 = 0.f;
  float all_trkqx_forw_16 = 0.f;
  float all_trkqy_forw_16 = 0.f;
  float all_trkPt_forw_16 = 0.f;
  float all_trkqx_afterw_16 = 0.f;
  float all_trkqy_afterw_16 = 0.f;
  float all_trkPt_afterw_16 = 0.f;
  float all_trkqx_08 = 0.f;
  float all_trkqy_08 = 0.f;
  float all_trkPt_08 = 0.f;
  float all_trkqx_forw_08 = 0.f;
  float all_trkqy_forw_08 = 0.f;
  float all_trkPt_forw_08 = 0.f;
  float all_trkqx_afterw_08 = 0.f;
  float all_trkqy_afterw_08 = 0.f;
  float all_trkPt_afterw_08 = 0.f;

  for (unsigned int it = 0; it < trackColl->size(); ++it) {
    reco::TrackRef track(trackColl, it);

    const float dzvtx = track->dz(bestvtx);
    const float dxyvtx = track->dxy(bestvtx);
    const float dzerror = std::sqrt(track->dzError() * track->dzError() + bestvzError * bestvzError);
    const float dxyerror = track->dxyError(bestvtx, vtx.covariance());

    if (!track->quality(reco::TrackBase::highPurity)) continue;
    if (std::abs(track->ptError()) / track->pt() > 0.10) continue;
    if (std::abs(dzvtx / dzerror) > 3) continue;
    if (std::abs(dxyvtx / dxyerror) > 3) continue;
    if (track->pt() <= 0.3 || track->pt() >= 3.0) continue;
    if (std::abs(track->eta()) >= 2.4) continue;

    const float pt = track->pt();
    const float phi = track->phi();
    const float eta = track->eta();

    all_trkqx += pt * std::cos(2 * phi);
    all_trkqy += pt * std::sin(2 * phi);
    all_trkPt += pt;
    if (eta > 0.05f && eta < 2.4f) {
      all_trkPt_forw += pt;
      all_trkqx_forw += pt * std::cos(2 * phi);
      all_trkqy_forw += pt * std::sin(2 * phi);
    }
    if (eta > -2.4f && eta < -0.05f) {
      all_trkPt_afterw += pt;
      all_trkqx_afterw += pt * std::cos(2 * phi);
      all_trkqy_afterw += pt * std::sin(2 * phi);
    }
    if (std::abs(eta) < 1.6f) {
      all_trkqx_16 += pt * std::cos(2 * phi);
      all_trkqy_16 += pt * std::sin(2 * phi);
      all_trkPt_16 += pt;
      if (eta > 0.05f && eta < 1.6f) {
        all_trkPt_forw_16 += pt;
        all_trkqx_forw_16 += pt * std::cos(2 * phi);
        all_trkqy_forw_16 += pt * std::sin(2 * phi);
      }
      if (eta > -1.6f && eta < -0.05f) {
        all_trkPt_afterw_16 += pt;
        all_trkqx_afterw_16 += pt * std::cos(2 * phi);
        all_trkqy_afterw_16 += pt * std::sin(2 * phi);
      }
    }
    if (std::abs(eta) < 0.8f) {
      all_trkqx_08 += pt * std::cos(2 * phi);
      all_trkqy_08 += pt * std::sin(2 * phi);
      all_trkPt_08 += pt;
      if (eta > 0.05f && eta < 0.8f) {
        all_trkPt_forw_08 += pt;
        all_trkqx_forw_08 += pt * std::cos(2 * phi);
        all_trkqy_forw_08 += pt * std::sin(2 * phi);
      }
      if (eta > -0.8f && eta < -0.05f) {
        all_trkPt_afterw_08 += pt;
        all_trkqx_afterw_08 += pt * std::cos(2 * phi);
        all_trkqy_afterw_08 += pt * std::sin(2 * phi);
      }
    }

    bool dauTrack = false;
    for (unsigned int i = 0; i < dauEta.size(); ++i) {
      const bool refMatch = (i < dauHasTrackRef.size() && i < dauTrackProductId.size() && i < dauTrackKey.size() &&
                             dauHasTrackRef[i] && dauTrackProductId[i] == track.id() && dauTrackKey[i] == track.key());
      if (refMatch) {
        dauTrack = true;
        break;
      }
      const float dEta = std::abs(dauEta[i] - eta);
      const float dPhi = std::abs(reco::deltaPhi(dauPhi[i], phi));
      const float dPtRel = (pt > 0.f ? std::abs(dauPt[i] - pt) / pt : 999.f);
      if (dEta < 1e-3f && dPhi < 1e-3f && dPtRel < 1e-3f) {
        dauTrack = true;
        break;
      }
    }
    if (dauTrack) continue;

    trkqx += pt * std::cos(2 * phi);
    trkqy += pt * std::sin(2 * phi);
    trkPt += pt;
    trkqx_v3 += pt * std::cos(3 * phi);
    trkqy_v3 += pt * std::sin(3 * phi);

    if (eta > 0.05f && eta < 2.4f) {
      trkPt_forw += pt;
      trkqx_forw += pt * std::cos(2 * phi);
      trkqy_forw += pt * std::sin(2 * phi);
      trkqx_v3_forw += pt * std::cos(3 * phi);
      trkqy_v3_forw += pt * std::sin(3 * phi);
    }
    if (eta > -2.4f && eta < -0.05f) {
      trkPt_afterw += pt;
      trkqx_afterw += pt * std::cos(2 * phi);
      trkqy_afterw += pt * std::sin(2 * phi);
      trkqx_v3_afterw += pt * std::cos(3 * phi);
      trkqy_v3_afterw += pt * std::sin(3 * phi);
    }
    if (std::abs(eta) < 1.6f) {
      trkqx_16 += pt * std::cos(2 * phi);
      trkqy_16 += pt * std::sin(2 * phi);
      trkPt_16 += pt;
      trkqx_v3_16 += pt * std::cos(3 * phi);
      trkqy_v3_16 += pt * std::sin(3 * phi);
      if (eta > 0.05f && eta < 1.6f) {
        trkPt_forw_16 += pt;
        trkqx_forw_16 += pt * std::cos(2 * phi);
        trkqy_forw_16 += pt * std::sin(2 * phi);
        trkqx_v3_forw_16 += pt * std::cos(3 * phi);
        trkqy_v3_forw_16 += pt * std::sin(3 * phi);
      }
      if (eta > -1.6f && eta < -0.05f) {
        trkPt_afterw_16 += pt;
        trkqx_afterw_16 += pt * std::cos(2 * phi);
        trkqy_afterw_16 += pt * std::sin(2 * phi);
        trkqx_v3_afterw_16 += pt * std::cos(3 * phi);
        trkqy_v3_afterw_16 += pt * std::sin(3 * phi);
      }
    }
    if (std::abs(eta) < 0.8f) {
      trkqx_08 += pt * std::cos(2 * phi);
      trkqy_08 += pt * std::sin(2 * phi);
      trkPt_08 += pt;
      trkqx_v3_08 += pt * std::cos(3 * phi);
      trkqy_v3_08 += pt * std::sin(3 * phi);
      if (eta > 0.05f && eta < 0.8f) {
        trkPt_forw_08 += pt;
        trkqx_forw_08 += pt * std::cos(2 * phi);
        trkqy_forw_08 += pt * std::sin(2 * phi);
        trkqx_v3_forw_08 += pt * std::cos(3 * phi);
        trkqy_v3_forw_08 += pt * std::sin(3 * phi);
      }
      if (eta > -0.8f && eta < -0.05f) {
        trkPt_afterw_08 += pt;
        trkqx_afterw_08 += pt * std::cos(2 * phi);
        trkqy_afterw_08 += pt * std::sin(2 * phi);
        trkqx_v3_afterw_08 += pt * std::cos(3 * phi);
        trkqy_v3_afterw_08 += pt * std::sin(3 * phi);
      }
    }
  }

  trkQx = safeNorm(trkqx, trkPt);
  trkQy = safeNorm(trkqy, trkPt);
  all_trkQx = safeNorm(all_trkqx, all_trkPt);
  all_trkQy = safeNorm(all_trkqy, all_trkPt);
  all_trkW = all_trkPt;
  all_trkQx_forw = safeNorm(all_trkqx_forw, all_trkPt_forw);
  all_trkQy_forw = safeNorm(all_trkqy_forw, all_trkPt_forw);
  all_trkW_forw = all_trkPt_forw;
  all_trkQx_afterw = safeNorm(all_trkqx_afterw, all_trkPt_afterw);
  all_trkQy_afterw = safeNorm(all_trkqy_afterw, all_trkPt_afterw);
  all_trkW_afterw = all_trkPt_afterw;
  all_trkQx_eta16 = safeNorm(all_trkqx_16, all_trkPt_16);
  all_trkQy_eta16 = safeNorm(all_trkqy_16, all_trkPt_16);
  all_trkW_eta16 = all_trkPt_16;
  all_trkQx_forw_eta16 = safeNorm(all_trkqx_forw_16, all_trkPt_forw_16);
  all_trkQy_forw_eta16 = safeNorm(all_trkqy_forw_16, all_trkPt_forw_16);
  all_trkW_forw_eta16 = all_trkPt_forw_16;
  all_trkQx_afterw_eta16 = safeNorm(all_trkqx_afterw_16, all_trkPt_afterw_16);
  all_trkQy_afterw_eta16 = safeNorm(all_trkqy_afterw_16, all_trkPt_afterw_16);
  all_trkW_afterw_eta16 = all_trkPt_afterw_16;
  all_trkQx_eta08 = safeNorm(all_trkqx_08, all_trkPt_08);
  all_trkQy_eta08 = safeNorm(all_trkqy_08, all_trkPt_08);
  all_trkW_eta08 = all_trkPt_08;
  all_trkQx_forw_eta08 = safeNorm(all_trkqx_forw_08, all_trkPt_forw_08);
  all_trkQy_forw_eta08 = safeNorm(all_trkqy_forw_08, all_trkPt_forw_08);
  all_trkW_forw_eta08 = all_trkPt_forw_08;
  all_trkQx_afterw_eta08 = safeNorm(all_trkqx_afterw_08, all_trkPt_afterw_08);
  all_trkQy_afterw_eta08 = safeNorm(all_trkqy_afterw_08, all_trkPt_afterw_08);
  all_trkW_afterw_eta08 = all_trkPt_afterw_08;

  trkQx_forw = safeNorm(trkqx_forw, trkPt_forw);
  trkQy_forw = safeNorm(trkqy_forw, trkPt_forw);
  trkQx_afterw = safeNorm(trkqx_afterw, trkPt_afterw);
  trkQy_afterw = safeNorm(trkqy_afterw, trkPt_afterw);
  trkQx_v3 = safeNorm(trkqx_v3, trkPt);
  trkQy_v3 = safeNorm(trkqy_v3, trkPt);
  trkQx_v3_forw = safeNorm(trkqx_v3_forw, trkPt_forw);
  trkQy_v3_forw = safeNorm(trkqy_v3_forw, trkPt_forw);
  trkQx_v3_afterw = safeNorm(trkqx_v3_afterw, trkPt_afterw);
  trkQy_v3_afterw = safeNorm(trkqy_v3_afterw, trkPt_afterw);

  trkQx_eta16 = safeNorm(trkqx_16, trkPt_16);
  trkQy_eta16 = safeNorm(trkqy_16, trkPt_16);
  trkQx_forw_eta16 = safeNorm(trkqx_forw_16, trkPt_forw_16);
  trkQy_forw_eta16 = safeNorm(trkqy_forw_16, trkPt_forw_16);
  trkQx_afterw_eta16 = safeNorm(trkqx_afterw_16, trkPt_afterw_16);
  trkQy_afterw_eta16 = safeNorm(trkqy_afterw_16, trkPt_afterw_16);
  trkQx_v3_eta16 = safeNorm(trkqx_v3_16, trkPt_16);
  trkQy_v3_eta16 = safeNorm(trkqy_v3_16, trkPt_16);
  trkQx_v3_forw_eta16 = safeNorm(trkqx_v3_forw_16, trkPt_forw_16);
  trkQy_v3_forw_eta16 = safeNorm(trkqy_v3_forw_16, trkPt_forw_16);
  trkQx_v3_afterw_eta16 = safeNorm(trkqx_v3_afterw_16, trkPt_afterw_16);
  trkQy_v3_afterw_eta16 = safeNorm(trkqy_v3_afterw_16, trkPt_afterw_16);

  trkQx_eta08 = safeNorm(trkqx_08, trkPt_08);
  trkQy_eta08 = safeNorm(trkqy_08, trkPt_08);
  trkQx_forw_eta08 = safeNorm(trkqx_forw_08, trkPt_forw_08);
  trkQy_forw_eta08 = safeNorm(trkqy_forw_08, trkPt_forw_08);
  trkQx_afterw_eta08 = safeNorm(trkqx_afterw_08, trkPt_afterw_08);
  trkQy_afterw_eta08 = safeNorm(trkqy_afterw_08, trkPt_afterw_08);
  trkQx_v3_eta08 = safeNorm(trkqx_v3_08, trkPt_08);
  trkQy_v3_eta08 = safeNorm(trkqy_v3_08, trkPt_08);
  trkQx_v3_forw_eta08 = safeNorm(trkqx_v3_forw_08, trkPt_forw_08);
  trkQy_v3_forw_eta08 = safeNorm(trkqy_v3_forw_08, trkPt_forw_08);
  trkQx_v3_afterw_eta08 = safeNorm(trkqx_v3_afterw_08, trkPt_afterw_08);
  trkQy_v3_afterw_eta08 = safeNorm(trkqy_v3_afterw_08, trkPt_afterw_08);

  fillReproducedEventPlane(iEvent);
}

void PATEventPlaneTrackMB::initTree() {
  eventPlaneTree_ = fs->make<TTree>("EventPlane", "EventPlane");

  eventPlaneTree_->Branch("RunNb", &runNb, "RunNb/i");
  eventPlaneTree_->Branch("LSNb", &lsNb, "LSNb/i");
  eventPlaneTree_->Branch("EventNb", &eventNb, "EventNb/i");
  eventPlaneTree_->Branch("nPV", &nPV, "nPV/S");
  eventPlaneTree_->Branch("bestvtxX", &bestvx, "bestvtxX/F");
  eventPlaneTree_->Branch("bestvtxY", &bestvy, "bestvtxY/F");
  eventPlaneTree_->Branch("bestvtxZ", &bestvz, "bestvtxZ/F");

  if (isCentrality_) {
    eventPlaneTree_->Branch("centrality", &centrality, "centrality/S");
    eventPlaneTree_->Branch("Ntrkoffline", &Ntrkoffline, "Ntrkoffline/I");
    eventPlaneTree_->Branch("NtrkHP", &NtrkHP, "NtrkHP/I");
  }

  eventPlaneTree_->Branch("trkQx", &trkQx, "trkQx/F");
  eventPlaneTree_->Branch("trkQy", &trkQy, "trkQy/F");
  eventPlaneTree_->Branch("all_trkQx", &all_trkQx, "all_trkQx/F");
  eventPlaneTree_->Branch("all_trkQy", &all_trkQy, "all_trkQy/F");
  eventPlaneTree_->Branch("all_trkW", &all_trkW, "all_trkW/F");
  eventPlaneTree_->Branch("all_trkQx_forw", &all_trkQx_forw, "all_trkQx_forw/F");
  eventPlaneTree_->Branch("all_trkQy_forw", &all_trkQy_forw, "all_trkQy_forw/F");
  eventPlaneTree_->Branch("all_trkW_forw", &all_trkW_forw, "all_trkW_forw/F");
  eventPlaneTree_->Branch("all_trkQx_afterw", &all_trkQx_afterw, "all_trkQx_afterw/F");
  eventPlaneTree_->Branch("all_trkQy_afterw", &all_trkQy_afterw, "all_trkQy_afterw/F");
  eventPlaneTree_->Branch("all_trkW_afterw", &all_trkW_afterw, "all_trkW_afterw/F");
  eventPlaneTree_->Branch("all_trkQx_eta16", &all_trkQx_eta16, "all_trkQx_eta16/F");
  eventPlaneTree_->Branch("all_trkQy_eta16", &all_trkQy_eta16, "all_trkQy_eta16/F");
  eventPlaneTree_->Branch("all_trkW_eta16", &all_trkW_eta16, "all_trkW_eta16/F");
  eventPlaneTree_->Branch("all_trkQx_forw_eta16", &all_trkQx_forw_eta16, "all_trkQx_forw_eta16/F");
  eventPlaneTree_->Branch("all_trkQy_forw_eta16", &all_trkQy_forw_eta16, "all_trkQy_forw_eta16/F");
  eventPlaneTree_->Branch("all_trkW_forw_eta16", &all_trkW_forw_eta16, "all_trkW_forw_eta16/F");
  eventPlaneTree_->Branch("all_trkQx_afterw_eta16", &all_trkQx_afterw_eta16, "all_trkQx_afterw_eta16/F");
  eventPlaneTree_->Branch("all_trkQy_afterw_eta16", &all_trkQy_afterw_eta16, "all_trkQy_afterw_eta16/F");
  eventPlaneTree_->Branch("all_trkW_afterw_eta16", &all_trkW_afterw_eta16, "all_trkW_afterw_eta16/F");
  eventPlaneTree_->Branch("all_trkQx_eta08", &all_trkQx_eta08, "all_trkQx_eta08/F");
  eventPlaneTree_->Branch("all_trkQy_eta08", &all_trkQy_eta08, "all_trkQy_eta08/F");
  eventPlaneTree_->Branch("all_trkW_eta08", &all_trkW_eta08, "all_trkW_eta08/F");
  eventPlaneTree_->Branch("all_trkQx_forw_eta08", &all_trkQx_forw_eta08, "all_trkQx_forw_eta08/F");
  eventPlaneTree_->Branch("all_trkQy_forw_eta08", &all_trkQy_forw_eta08, "all_trkQy_forw_eta08/F");
  eventPlaneTree_->Branch("all_trkW_forw_eta08", &all_trkW_forw_eta08, "all_trkW_forw_eta08/F");
  eventPlaneTree_->Branch("all_trkQx_afterw_eta08", &all_trkQx_afterw_eta08, "all_trkQx_afterw_eta08/F");
  eventPlaneTree_->Branch("all_trkQy_afterw_eta08", &all_trkQy_afterw_eta08, "all_trkQy_afterw_eta08/F");
  eventPlaneTree_->Branch("all_trkW_afterw_eta08", &all_trkW_afterw_eta08, "all_trkW_afterw_eta08/F");
  eventPlaneTree_->Branch("trkQx_forw", &trkQx_forw, "trkQx_forw/F");
  eventPlaneTree_->Branch("trkQy_forw", &trkQy_forw, "trkQy_forw/F");
  eventPlaneTree_->Branch("trkQx_afterw", &trkQx_afterw, "trkQx_afterw/F");
  eventPlaneTree_->Branch("trkQy_afterw", &trkQy_afterw, "trkQy_afterw/F");
  eventPlaneTree_->Branch("trkQx_v3", &trkQx_v3, "trkQx_v3/F");
  eventPlaneTree_->Branch("trkQy_v3", &trkQy_v3, "trkQy_v3/F");
  eventPlaneTree_->Branch("trkQx_v3_forw", &trkQx_v3_forw, "trkQx_v3_forw/F");
  eventPlaneTree_->Branch("trkQy_v3_forw", &trkQy_v3_forw, "trkQy_v3_forw/F");
  eventPlaneTree_->Branch("trkQx_v3_afterw", &trkQx_v3_afterw, "trkQx_v3_afterw/F");
  eventPlaneTree_->Branch("trkQy_v3_afterw", &trkQy_v3_afterw, "trkQy_v3_afterw/F");
  eventPlaneTree_->Branch("trkQx_eta16", &trkQx_eta16, "trkQx_eta16/F");
  eventPlaneTree_->Branch("trkQy_eta16", &trkQy_eta16, "trkQy_eta16/F");
  eventPlaneTree_->Branch("trkQx_forw_eta16", &trkQx_forw_eta16, "trkQx_forw_eta16/F");
  eventPlaneTree_->Branch("trkQy_forw_eta16", &trkQy_forw_eta16, "trkQy_forw_eta16/F");
  eventPlaneTree_->Branch("trkQx_afterw_eta16", &trkQx_afterw_eta16, "trkQx_afterw_eta16/F");
  eventPlaneTree_->Branch("trkQy_afterw_eta16", &trkQy_afterw_eta16, "trkQy_afterw_eta16/F");
  eventPlaneTree_->Branch("trkQx_v3_eta16", &trkQx_v3_eta16, "trkQx_v3_eta16/F");
  eventPlaneTree_->Branch("trkQy_v3_eta16", &trkQy_v3_eta16, "trkQy_v3_eta16/F");
  eventPlaneTree_->Branch("trkQx_v3_forw_eta16", &trkQx_v3_forw_eta16, "trkQx_v3_forw_eta16/F");
  eventPlaneTree_->Branch("trkQy_v3_forw_eta16", &trkQy_v3_forw_eta16, "trkQy_v3_forw_eta16/F");
  eventPlaneTree_->Branch("trkQx_v3_afterw_eta16", &trkQx_v3_afterw_eta16, "trkQx_v3_afterw_eta16/F");
  eventPlaneTree_->Branch("trkQy_v3_afterw_eta16", &trkQy_v3_afterw_eta16, "trkQy_v3_afterw_eta16/F");
  eventPlaneTree_->Branch("trkQx_eta08", &trkQx_eta08, "trkQx_eta08/F");
  eventPlaneTree_->Branch("trkQy_eta08", &trkQy_eta08, "trkQy_eta08/F");
  eventPlaneTree_->Branch("trkQx_forw_eta08", &trkQx_forw_eta08, "trkQx_forw_eta08/F");
  eventPlaneTree_->Branch("trkQy_forw_eta08", &trkQy_forw_eta08, "trkQy_forw_eta08/F");
  eventPlaneTree_->Branch("trkQx_afterw_eta08", &trkQx_afterw_eta08, "trkQx_afterw_eta08/F");
  eventPlaneTree_->Branch("trkQy_afterw_eta08", &trkQy_afterw_eta08, "trkQy_afterw_eta08/F");
  eventPlaneTree_->Branch("trkQx_v3_eta08", &trkQx_v3_eta08, "trkQx_v3_eta08/F");
  eventPlaneTree_->Branch("trkQy_v3_eta08", &trkQy_v3_eta08, "trkQy_v3_eta08/F");
  eventPlaneTree_->Branch("trkQx_v3_forw_eta08", &trkQx_v3_forw_eta08, "trkQx_v3_forw_eta08/F");
  eventPlaneTree_->Branch("trkQy_v3_forw_eta08", &trkQy_v3_forw_eta08, "trkQy_v3_forw_eta08/F");
  eventPlaneTree_->Branch("trkQx_v3_afterw_eta08", &trkQx_v3_afterw_eta08, "trkQx_v3_afterw_eta08/F");
  eventPlaneTree_->Branch("trkQy_v3_afterw_eta08", &trkQy_v3_afterw_eta08, "trkQy_v3_afterw_eta08/F");

  eventPlaneTree_->Branch("ephfpAngle", &ephfpAngle, "ephfpAngle[2]/F");
  eventPlaneTree_->Branch("ephfmAngle", &ephfmAngle, "ephfmAngle[2]/F");
  eventPlaneTree_->Branch("ephfpQ", &ephfpQ, "ephfpQ[2]/F");
  eventPlaneTree_->Branch("ephfmQ", &ephfmQ, "ephfmQ[2]/F");
  eventPlaneTree_->Branch("ephfpSumW", &ephfpSumW, "ephfpSumW/F");
  eventPlaneTree_->Branch("ephfmSumW", &ephfmSumW, "ephfmSumW/F");
  eventPlaneTree_->Branch("ephfpSumWSub", &ephfpSumWSub, "ephfpSumWSub[2]/F");
  eventPlaneTree_->Branch("ephfmSumWSub", &ephfmSumWSub, "ephfmSumWSub[2]/F");
  eventPlaneTree_->Branch("ephfmAngleoff", &ephfmAngleoff, "ephfmAngleoff[2]/F");
  eventPlaneTree_->Branch("ephfpAngleoff", &ephfpAngleoff, "ephfpAngleoff[2]/F");
  eventPlaneTree_->Branch("ephfmAngleRaw", &ephfmAngleRaw, "ephfmAngleRaw[2]/F");
  eventPlaneTree_->Branch("ephfmsumCosRaw", &ephfmsumCosRaw, "ephfmsumCosRaw[2]/F");
  eventPlaneTree_->Branch("ephfmsumSinRaw", &ephfmsumSinRaw, "ephfmsumSinRaw[2]/F");
  eventPlaneTree_->Branch("ephfmsumCos", &ephfmsumCos, "ephfmsumCos[2]/F");
  eventPlaneTree_->Branch("ephfmsumSin", &ephfmsumSin, "ephfmsumSin[2]/F");
  eventPlaneTree_->Branch("ephfmsumPtOrEt", &ephfmsumPtOrEt, "ephfmsumPtOrEt[2]/F");
  eventPlaneTree_->Branch("ephfpAngleRaw", &ephfpAngleRaw, "ephfpAngleRaw[2]/F");
  eventPlaneTree_->Branch("ephfpsumCosRaw", &ephfpsumCosRaw, "ephfpsumCosRaw[2]/F");
  eventPlaneTree_->Branch("ephfpsumSinRaw", &ephfpsumSinRaw, "ephfpsumSinRaw[2]/F");
  eventPlaneTree_->Branch("ephfpsumCos", &ephfpsumCos, "ephfpsumCos[2]/F");
  eventPlaneTree_->Branch("ephfpsumSin", &ephfpsumSin, "ephfpsumSin[2]/F");
  eventPlaneTree_->Branch("ephfpsumPtOrEt", &ephfpsumPtOrEt, "ephfpsumPtOrEt[2]/F");
  eventPlaneTree_->Branch("eptrackmidAngle", &eptrackmidAngle, "eptrackmidAngle[2]/F");
  eventPlaneTree_->Branch("eptrackmidAngleoff", &eptrackmidAngleoff, "eptrackmidAngleoff[2]/F");
  eventPlaneTree_->Branch("eptrackmidAngleRaw", &eptrackmidAngleRaw, "eptrackmidAngleRaw[2]/F");
  eventPlaneTree_->Branch("eptrackmidQ", &eptrackmidQ, "eptrackmidQ[2]/F");
  eventPlaneTree_->Branch("eptrackmidSumCos", &eptrackmidSumCos, "eptrackmidSumCos[2]/F");
  eventPlaneTree_->Branch("eptrackmidSumSin", &eptrackmidSumSin, "eptrackmidSumSin[2]/F");
  eventPlaneTree_->Branch("eptrackmidSumCosRaw", &eptrackmidSumCosRaw, "eptrackmidSumCosRaw[2]/F");
  eventPlaneTree_->Branch("eptrackmidSumSinRaw", &eptrackmidSumSinRaw, "eptrackmidSumSinRaw[2]/F");
  eventPlaneTree_->Branch("eptrackmidSumW", &eptrackmidSumW, "eptrackmidSumW/F");
  eventPlaneTree_->Branch("eptrackmidSumWSub", &eptrackmidSumWSub, "eptrackmidSumWSub[2]/F");
  eventPlaneTree_->Branch("eptrackmidSumPtOrEt", &eptrackmidSumPtOrEt, "eptrackmidSumPtOrEt[2]/F");
  eventPlaneTree_->Branch("eptrackpAngle", &eptrackpAngle, "eptrackpAngle[2]/F");
  eventPlaneTree_->Branch("eptrackpAngleoff", &eptrackpAngleoff, "eptrackpAngleoff[2]/F");
  eventPlaneTree_->Branch("eptrackpAngleRaw", &eptrackpAngleRaw, "eptrackpAngleRaw[2]/F");
  eventPlaneTree_->Branch("eptrackpQ", &eptrackpQ, "eptrackpQ[2]/F");
  eventPlaneTree_->Branch("eptrackpSumCos", &eptrackpSumCos, "eptrackpSumCos[2]/F");
  eventPlaneTree_->Branch("eptrackpSumSin", &eptrackpSumSin, "eptrackpSumSin[2]/F");
  eventPlaneTree_->Branch("eptrackpSumCosRaw", &eptrackpSumCosRaw, "eptrackpSumCosRaw[2]/F");
  eventPlaneTree_->Branch("eptrackpSumSinRaw", &eptrackpSumSinRaw, "eptrackpSumSinRaw[2]/F");
  eventPlaneTree_->Branch("eptrackpSumW", &eptrackpSumW, "eptrackpSumW[2]/F");
  eventPlaneTree_->Branch("eptrackpSumPtOrEt", &eptrackpSumPtOrEt, "eptrackpSumPtOrEt[2]/F");
  eventPlaneTree_->Branch("eptrackmAngle", &eptrackmAngle, "eptrackmAngle[2]/F");
  eventPlaneTree_->Branch("eptrackmAngleoff", &eptrackmAngleoff, "eptrackmAngleoff[2]/F");
  eventPlaneTree_->Branch("eptrackmAngleRaw", &eptrackmAngleRaw, "eptrackmAngleRaw[2]/F");
  eventPlaneTree_->Branch("eptrackmQ", &eptrackmQ, "eptrackmQ[2]/F");
  eventPlaneTree_->Branch("eptrackmSumCos", &eptrackmSumCos, "eptrackmSumCos[2]/F");
  eventPlaneTree_->Branch("eptrackmSumSin", &eptrackmSumSin, "eptrackmSumSin[2]/F");
  eventPlaneTree_->Branch("eptrackmSumCosRaw", &eptrackmSumCosRaw, "eptrackmSumCosRaw[2]/F");
  eventPlaneTree_->Branch("eptrackmSumSinRaw", &eptrackmSumSinRaw, "eptrackmSumSinRaw[2]/F");
  eventPlaneTree_->Branch("eptrackmSumW", &eptrackmSumW, "eptrackmSumW[2]/F");
  eventPlaneTree_->Branch("eptrackmSumPtOrEt", &eptrackmSumPtOrEt, "eptrackmSumPtOrEt[2]/F");
  eventPlaneTree_->Branch("ephfAngle", &ephfAngle, "ephfAngle[2]/F");
  eventPlaneTree_->Branch("ephfAngleoff", &ephfAngleoff, "ephfAngleoff[2]/F");
  eventPlaneTree_->Branch("ephfAngleRaw", &ephfAngleRaw, "ephfAngleRaw[2]/F");
  eventPlaneTree_->Branch("ephfQ", &ephfQ, "ephfQ[2]/F");
  eventPlaneTree_->Branch("ephfSumW", &ephfSumW, "ephfSumW/F");
  eventPlaneTree_->Branch("ephfSumWSub", &ephfSumWSub, "ephfSumWSub[2]/F");
  eventPlaneTree_->Branch("ephfsumCos", &ephfsumCos, "ephfsumCos[2]/F");
  eventPlaneTree_->Branch("ephfsumSin", &ephfsumSin, "ephfsumSin[2]/F");
  eventPlaneTree_->Branch("ephfsumCosRaw", &ephfsumCosRaw, "ephfsumCosRaw[2]/F");
  eventPlaneTree_->Branch("ephfsumSinRaw", &ephfsumSinRaw, "ephfsumSinRaw[2]/F");
  eventPlaneTree_->Branch("ephfsumPtOrEt", &ephfsumPtOrEt, "ephfsumPtOrEt[2]/F");
}

DEFINE_FWK_MODULE(PATEventPlaneTrackMB);
