// system include files
#include <memory>
#include <string>
#include <vector>
#include <iostream>
#include <math.h>

#include <TH1.h>
#include <TH2.h>
#include <TTree.h>
#include <TFile.h>
#include <TVector3.h>
#include <TMath.h>

#include <Math/Functions.h>
#include <Math/SVector.h>
#include <Math/SMatrix.h>

// user include files
#include "FWCore/Framework/interface/Frameworkfwd.h"
#include "FWCore/Framework/interface/one/EDAnalyzer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/Run.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Utilities/interface/InputTag.h"
#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/Common/interface/TriggerNames.h"

#include "DataFormats/TrackReco/interface/DeDxData.h"
#include "DataFormats/MuonReco/interface/Muon.h"
#include "DataFormats/MuonReco/interface/MuonFwd.h"
#include "DataFormats/TrackReco/interface/Track.h"
#include "DataFormats/TrackReco/interface/TrackFwd.h"
#include "DataFormats/VertexReco/interface/Vertex.h"
#include "DataFormats/VertexReco/interface/VertexFwd.h"
#include "DataFormats/BeamSpot/interface/BeamSpot.h"
#include "DataFormats/Candidate/interface/Candidate.h"
#include "DataFormats/RecoCandidate/interface/RecoCandidate.h"
#include "DataFormats/PatCandidates/interface/Muon.h"
#include "DataFormats/PatCandidates/interface/CompositeCandidate.h"
#include "DataFormats/HeavyIonEvent/interface/CentralityBins.h"
#include "DataFormats/HeavyIonEvent/interface/Centrality.h"
#include "DataFormats/HeavyIonEvent/interface/EvtPlane.h"
#include "DataFormats/HepMCCandidate/interface/GenParticle.h"
#include "SimDataFormats/GeneratorProducts/interface/GenEventInfoProduct.h"
#include "SimDataFormats/GeneratorProducts/interface/LHEEventProduct.h"
#include "DataFormats/Common/interface/TriggerResults.h"
#include "DataFormats/Math/interface/deltaR.h"

#include "HLTrigger/HLTcore/interface/HLTPrescaleProvider.h"
#include "TrackingTools/TransientTrack/interface/TransientTrackBuilder.h"
#include "TrackingTools/Records/interface/TransientTrackRecord.h"
#include "TrackingTools/PatternTools/interface/ClosestApproachInRPhi.h"
#include "TrackingTools/PatternTools/interface/TSCBLBuilderNoMaterial.h"
#include "CommonTools/UtilAlgos/interface/TFileService.h"

#include "DataFormats/CaloTowers/interface/CaloTower.h"
#include "DataFormats/CaloTowers/interface/CaloTowerDefs.h"

#include <iostream>
#include <fstream>

//
// constants, enums and typedefs
//

#define PI 3.1416
#define MAXCAN 500000
#define MAXDAU 3
#define MAXGDAU 2
#define MAXTRG 1024
#define MAXSEL 100

typedef ROOT::Math::SMatrix<float, 3, 3, ROOT::Math::MatRepSym<float, 3> > SMatrixSym3D;
typedef ROOT::Math::SVector<float, 3> SVector3;
typedef ROOT::Math::SVector<float, 6> SVector6;


//
// class decleration/home/jun502s/DstarAna/MVAAnalysis/MVAAnalysis/MVA/lib/treeIO.py
//

class PATEventPlaneTrackOfficial : public edm::one::EDAnalyzer<edm::one::WatchRuns> {
public:
  explicit PATEventPlaneTrackOfficial(const edm::ParameterSet&);
  ~PATEventPlaneTrackOfficial();


private:
  virtual void beginJob();
  virtual void beginRun(const edm::Run&, const edm::EventSetup&);
  virtual void endRun(const edm::Run&, const edm::EventSetup&) {};
  virtual void analyze(const edm::Event&, const edm::EventSetup&);
  virtual void fillRECO(const edm::Event&, const edm::EventSetup&);
  virtual void endJob() ;
  virtual void initTree();
  virtual void initHistogram();
  // ----------member data ---------------------------

  edm::Service<TFileService> fs;

  TTree* PATEventPlaneNtuple;
  TH1D* hdeltaEta;
  TH1D* hdeltaPhi;
  TH1D* hdeltaPt;
  TH1D* htrketa_forw;
  TH1D* htrketa_afterw;
  TH1D* htrkpt;
  TH1D* htrketa;

  bool   saveTree_;
  bool   saveHistogram_;

  //options
  bool doRecoNtuple_;
  bool useOfficialTrackCuts_;
  bool debugCompareOfficialCuts_;
  int officialCutEra_;
  float officialPtErrMaxRel_;
  float officialDzSigMax_;
  float officialDxySigMax_;
  float officialChi2PerLayerMax_;
  float officialDzSigPixMax_;
  float officialChi2PixMax_;
  int officialNhitsValidMin_;
  float officialMinPt_;
  float officialMaxPt_;
  float officialMaxAbsEta_;

  //cut variables

  //tree branches
  //event info
  uint  runNb;
  uint  eventNb;
  uint  lsNb;
  short centrality;
  int   Ntrkoffline;
  int   NtrkHP;
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

  //int nmuons = 0;
  bool isCentrality_;

  //Composite candidate info
  uint candSize;
  //float mva[MAXCAN];
  float pt[MAXCAN];
  //float eta[MAXCAN];
  //float phi[MAXCAN];
  //float flavor[MAXCAN];
  //float y[MAXCAN];
  float mass[MAXCAN];

  //dau info
  /*float dzos[MAXDAU][MAXCAN];
  float dxyos[MAXDAU][MAXCAN];
  float nhit[MAXDAU][MAXCAN];
  bool  trkquality[MAXDAU][MAXCAN];
  float ptDau[MAXDAU][MAXCAN];
  float ptErr[MAXDAU][MAXCAN];
  float pDau[MAXDAU][MAXCAN];
  float etaDau[MAXDAU][MAXCAN];
  float phiDau[MAXDAU][MAXCAN];
  short chargeDau[MAXDAU][MAXCAN];*/

  //token
  edm::EDGetTokenT<reco::BeamSpot> tok_offlineBS_;
  edm::EDGetTokenT<reco::VertexCollection> tok_offlinePV_;
  edm::EDGetTokenT<pat::CompositeCandidateCollection> patCompositeCandidateCollection_Token_;

  edm::EDGetTokenT<reco::TrackCollection> tok_tracks_;

  edm::EDGetTokenT<int> tok_centBinLabel_;
  edm::EDGetTokenT<reco::Centrality> tok_centSrc_;
 
  std::vector<float> dauEta;
  std::vector<float> dauPhi;
  std::vector<float> dauPt;

  unsigned long long nTracksComparedCuts_;
  unsigned long long nLegacyPassCuts_;
  unsigned long long nOfficialPassCuts_;
  unsigned long long nLegacyOnlyPassCuts_;
  unsigned long long nOfficialOnlyPassCuts_;

  void collectFinalStateDaughters(const reco::Candidate& cand);

};

//
// static data member definitions
//

//
// constructors and destructor
//

PATEventPlaneTrackOfficial::PATEventPlaneTrackOfficial(const edm::ParameterSet& iConfig) :
  patCompositeCandidateCollection_Token_(consumes<pat::CompositeCandidateCollection>(iConfig.getUntrackedParameter<edm::InputTag>("VertexCompositeCollection")))
{
  //options
  doRecoNtuple_ = iConfig.getUntrackedParameter<bool>("doRecoNtuple");
  saveTree_ = iConfig.getUntrackedParameter<bool>("saveTree");
  saveHistogram_ = iConfig.getUntrackedParameter<bool>("saveHistogram");
  useOfficialTrackCuts_ = iConfig.getUntrackedParameter<bool>("useOfficialTrackCuts", true);
  debugCompareOfficialCuts_ = iConfig.getUntrackedParameter<bool>("debugCompareOfficialCuts", true);
  officialCutEra_ = iConfig.getUntrackedParameter<int>("officialCutEra", 2); // 0:ppReco 1:HIReco 2:Pixel
  officialPtErrMaxRel_ = iConfig.getUntrackedParameter<double>("officialPtErrMaxRel", 0.10);
  officialDzSigMax_ = iConfig.getUntrackedParameter<double>("officialDzSigMax", 3.0);
  officialDxySigMax_ = iConfig.getUntrackedParameter<double>("officialDxySigMax", 3.0);
  officialChi2PerLayerMax_ = iConfig.getUntrackedParameter<double>("officialChi2PerLayerMax", 0.18);
  officialDzSigPixMax_ = iConfig.getUntrackedParameter<double>("officialDzSigPixMax", 10.0);
  officialChi2PixMax_ = iConfig.getUntrackedParameter<double>("officialChi2PixMax", 40.0);
  officialNhitsValidMin_ = iConfig.getUntrackedParameter<int>("officialNhitsValidMin", 11);
  officialMinPt_ = iConfig.getUntrackedParameter<double>("officialMinPt", 0.3);
  officialMaxPt_ = iConfig.getUntrackedParameter<double>("officialMaxPt", 3.0);
  officialMaxAbsEta_ = iConfig.getUntrackedParameter<double>("officialMaxAbsEta", 2.4);

  //input tokens
  tok_offlineBS_ = consumes<reco::BeamSpot>(iConfig.getUntrackedParameter<edm::InputTag>("beamSpotSrc"));
  tok_offlinePV_ = consumes<reco::VertexCollection>(iConfig.getUntrackedParameter<edm::InputTag>("VertexCollection"));
  tok_tracks_ = consumes<reco::TrackCollection>(edm::InputTag(iConfig.getUntrackedParameter<edm::InputTag>("TrackCollection")));
  
  isCentrality_ = (iConfig.exists("isCentrality") ? iConfig.getParameter<bool>("isCentrality") : false);
  if(isCentrality_)
  {
    tok_centBinLabel_ = consumes<int>(iConfig.getParameter<edm::InputTag>("centralityBinLabel"));
    tok_centSrc_ = consumes<reco::Centrality>(iConfig.getParameter<edm::InputTag>("centralitySrc"));
  }

  nTracksComparedCuts_ = 0;
  nLegacyPassCuts_ = 0;
  nOfficialPassCuts_ = 0;
  nLegacyOnlyPassCuts_ = 0;
  nOfficialOnlyPassCuts_ = 0;

}


PATEventPlaneTrackOfficial::~PATEventPlaneTrackOfficial()
{

  // do anything here that needs to be done at desctruction time
  // (e.g. close files, deallocate resources etc.)

}


//
// member functions
//

// ------------ method called to for each event  ------------
void
PATEventPlaneTrackOfficial::analyze(const edm::Event& iEvent, const edm::EventSetup& iSetup)
{
  //check event
  if(doRecoNtuple_) fillRECO(iEvent,iSetup);
  //if(saveTree_&&centrality>=80) PATEventPlaneNtuple->Fill();
  if(saveTree_) PATEventPlaneNtuple->Fill();
}


void
PATEventPlaneTrackOfficial::collectFinalStateDaughters(const reco::Candidate& cand)
{
  if(cand.numberOfDaughters()==0)
  {
    if(cand.charge()==0) return; // skip neutral leaves
    const reco::Track* bestTrack = cand.bestTrack();
    const float eta = (bestTrack ? bestTrack->eta() : cand.eta());
    const float phi = (bestTrack ? bestTrack->phi() : cand.phi());
    const float pt  = (bestTrack ? bestTrack->pt()  : cand.pt());
    dauEta.push_back(eta);
    dauPhi.push_back(phi);
    dauPt.push_back(pt);
    return;
  }

  for(size_t i = 0; i < cand.numberOfDaughters(); ++i)
  {
    const reco::Candidate* daughter = cand.daughter(i);
    if(daughter) collectFinalStateDaughters(*daughter);
  }
}


void
PATEventPlaneTrackOfficial::fillRECO(const edm::Event& iEvent, const edm::EventSetup& iSetup)
{
  //get collection
  edm::Handle<reco::BeamSpot> beamspot;
  iEvent.getByToken(tok_offlineBS_, beamspot);
  edm::Handle<reco::VertexCollection> vertices;
  iEvent.getByToken(tok_offlinePV_, vertices);
  if(!vertices.isValid()) throw cms::Exception("PATEventPlaneTrackOfficial") << "Primary vertices  collection not found!" << std::endl;

  edm::Handle<pat::CompositeCandidateCollection> v0candidates;
  iEvent.getByToken(patCompositeCandidateCollection_Token_, v0candidates);
  if(!v0candidates.isValid()) throw cms::Exception("PATEventPlaneTrackOfficial") << "V0 candidate collection not found!" << std::endl;

  runNb = iEvent.id().run();
  eventNb = iEvent.id().event();
  lsNb = iEvent.luminosityBlock();

  
  centrality = -1;
  if(isCentrality_)
  {
    const auto& cent = iEvent.getHandle(tok_centSrc_);
    Ntrkoffline = (cent.isValid() ? cent->Ntracks() : -1);
    edm::Handle<int> cbin;
    iEvent.getByToken(tok_centBinLabel_, cbin);
    centrality = (cbin.isValid() ? *cbin : -1);
  }
  
  NtrkHP = -1;
  edm::Handle<reco::TrackCollection> trackColl;
  iEvent.getByToken(tok_tracks_, trackColl);
  //const auto& trackColl = iEvent.getHandle(tok_tracks_);
  
  if(trackColl.isValid()) 
  {
    NtrkHP = 0;
    for (const auto& trk : *trackColl) { if (trk.quality(reco::TrackBase::highPurity)) NtrkHP++; }
  }

  nPV = vertices->size();
  //best vertex
  const auto& vtxPrimary = (vertices->size()>0 ? (*vertices)[0] : reco::Vertex());
  const bool& isPV = (!vtxPrimary.isFake() && vtxPrimary.tracksSize()>=2);
  const auto& bs = (!isPV ? reco::Vertex(beamspot->position(), beamspot->covariance3D()) : reco::Vertex());
  const reco::Vertex& vtx = (isPV ? vtxPrimary : bs);
  bestvz = vtx.z(); bestvx = vtx.x(); bestvy = vtx.y();
  const math::XYZPoint bestvtx(bestvx, bestvy, bestvz);
  const math::Error<3>::type vtxCov = vtx.covariance();
  bestvzError = vtx.zError(), bestvxError = vtx.xError(), bestvyError = vtx.yError();

  //RECO Candidate info
  candSize = v0candidates->size();
  if(candSize>MAXCAN) throw cms::Exception("PATEventPlaneTrackOfficial") << "Number of candidates (" << candSize << ") exceeds limit!" << std::endl; 
  float cohJpsiMassMin = 1.7;
  float cohJpsiMassMax = 2.1;

  dauEta.clear();
  dauPhi.clear();
  dauPt.clear();
  for(uint it=0; it<candSize; ++it)
  { 
    const auto& trk = (*v0candidates)[it];
    bool isJpsi = false;
	  
    const ushort& nDau = trk.numberOfDaughters();
    if(nDau==0) throw cms::Exception("PATCompositeAnalyzer") << "Candidate has no daughters!" << std::endl;
    
    pt[it] = trk.pt();
    mass[it] = trk.mass();

    if (mass[it] > cohJpsiMassMin && mass[it] < cohJpsiMassMax) isJpsi = true;
    if (isJpsi == false) continue;

    
    collectFinalStateDaughters(trk);
  }
  //nmuons += dauEta.size();
  
  //track info
  float trkqx = 0;
  float trkqy = 0;
  float trkPt = 0;
  trkQx = -1;
  trkQy = -1;
  float all_trkqx= 0;
  float all_trkqy = 0;
  float all_trkPt = 0;
  all_trkQx = -1;
  all_trkQy = -1;

  bool DauTrk = false;

  float trkPt_forw = 0;
  float trkPt_afterw = 0;
  float trkqx_forw = 0;
  float trkqy_forw = 0;
  float trkqx_afterw = 0;
  float trkqy_afterw = 0;
  trkQx_forw = -1;
  trkQy_forw = -1;
  trkQx_afterw = -1;
  trkQy_afterw = -1;

  float trkqx_v3 = 0;
  float trkqy_v3 = 0;
  float trkqx_v3_forw = 0;
  float trkqy_v3_forw = 0;
  float trkqx_v3_afterw = 0;
  float trkqy_v3_afterw = 0;
  trkQx_v3 = -1;
  trkQy_v3 = -1;
  trkQx_v3_forw = -1;
  trkQy_v3_forw = -1;
  trkQx_v3_afterw = -1;
  trkQy_v3_afterw = -1;


  uint subt = 0;
  static int dxyDebugPrintCount = 0;
  static int cutCompareDebugPrintCount = 0;
  for(unsigned it=0; it<trackColl->size(); ++it){

	DauTrk = false;
	reco::TrackRef track(trackColl, it);

	float dzvtx = track->dz(bestvtx);
        float dxyvtx = track->dxy(bestvtx);
        float dzerror = sqrt(track->dzError()*track->dzError()+bestvzError*bestvzError);
        float dxyerror = track->dxyError(bestvtx, vtxCov);

        if (dxyDebugPrintCount < 10) {
          const float dxySig = (dxyerror > 0.f ? std::abs(dxyvtx / dxyerror) : -1.f);
          std::cout << "[PATEventPlaneTrackOfficial][dxyDebug] run=" << runNb
                    << " event=" << eventNb
                    << " trackIdx=" << it
                    << " dxy=" << dxyvtx
                    << " dxyErr=" << dxyerror
                    << " |dxy/dxyErr|=" << dxySig
                    << std::endl;
          ++dxyDebugPrintCount;
        }
        
                const float pt = track->pt();
                const float eta = track->eta();
                const float phi = track->phi();
                const float absPtErrOverPt = (pt > 0.f ? std::abs(track->ptError()) / pt : 999.f);
                const float dzSig = (dzerror > 0.f ? std::abs(dzvtx / dzerror) : 999.f);
                const float dxySig = (dxyerror > 0.f ? std::abs(dxyvtx / dxyerror) : 999.f);
                const int nHits = track->numberOfValidHits();
                const int algo = track->algo();
                const float chi2layer = track->normalizedChi2() /
                  std::max(1, static_cast<int>(track->hitPattern().trackerLayersWithMeasurement()));
                const bool isAlgoAllowed =
                  (algo == reco::TrackBase::initialStep ||
                   algo == reco::TrackBase::lowPtTripletStep ||
                   algo == reco::TrackBase::pixelPairStep ||
                   algo == reco::TrackBase::detachedTripletStep);

                // Legacy (current analyzer) cut definition.
                const bool passLegacy =
                  track->quality(reco::TrackBase::highPurity) &&
                  absPtErrOverPt <= 0.10 &&
                  dzerror > 0.f && dxyerror > 0.f &&
                  dzSig <= 3.0 &&
                  dxySig <= 3.0 &&
                  pt > 0.3f && pt < 3.0f &&
                  std::abs(eta) < 2.4f;

                // Official-like EPCuts + EvtPlane pT/eta windows.
                bool passOfficial = true;
                if (!track->quality(reco::TrackBase::highPurity) || track->charge() == 0 || pt <= 0.f) passOfficial = false;
                if (absPtErrOverPt > officialPtErrMaxRel_) passOfficial = false;
                if (pt <= officialMinPt_ || pt >= officialMaxPt_) passOfficial = false;
                if (std::abs(eta) >= officialMaxAbsEta_) passOfficial = false;
                if (officialCutEra_ == 0 || officialCutEra_ == 1) {
                  if (nHits < officialNhitsValidMin_) passOfficial = false;
                  if (chi2layer > officialChi2PerLayerMax_) passOfficial = false;
                  if (dxyerror <= 0.f || dzerror <= 0.f) passOfficial = false;
                  if (dxySig > officialDxySigMax_) passOfficial = false;
                  if (dzSig > officialDzSigMax_) passOfficial = false;
                  if (officialCutEra_ == 1 && !isAlgoAllowed) passOfficial = false;
                } else if (officialCutEra_ == 2) {
                  const bool isPixLike = (pt < 2.4f && nHits <= 6);
                  if (isPixLike) {
                    if (chi2layer > officialChi2PixMax_) passOfficial = false;
                    if (dzerror <= 0.f) passOfficial = false;
                    if (dzSig > officialDzSigPixMax_) passOfficial = false;
                  } else {
                    if (nHits < officialNhitsValidMin_) passOfficial = false;
                    if (chi2layer > officialChi2PerLayerMax_) passOfficial = false;
                    if (pt > 2.4f && !isAlgoAllowed) passOfficial = false;
                    if (dxyerror <= 0.f || dzerror <= 0.f) passOfficial = false;
                    if (dxySig > officialDxySigMax_) passOfficial = false;
                    if (dzSig > officialDzSigMax_) passOfficial = false;
                  }
                } else {
                  passOfficial = passLegacy;
                }

                ++nTracksComparedCuts_;
                if (passLegacy) ++nLegacyPassCuts_;
                if (passOfficial) ++nOfficialPassCuts_;
                if (passLegacy && !passOfficial) ++nLegacyOnlyPassCuts_;
                if (!passLegacy && passOfficial) ++nOfficialOnlyPassCuts_;

                if (debugCompareOfficialCuts_ && cutCompareDebugPrintCount < 10 && passLegacy != passOfficial) {
                  std::cout << "[PATEventPlaneTrackOfficial][cutCompareDebug] run=" << runNb
                            << " event=" << eventNb
                            << " trackIdx=" << it
                            << " passLegacy=" << passLegacy
                            << " passOfficial=" << passOfficial
                            << " pt=" << pt
                            << " eta=" << eta
                            << " nHits=" << nHits
                            << " chi2layer=" << chi2layer
                            << " dzSig=" << dzSig
                            << " dxySig=" << dxySig
                            << " algo=" << algo
                            << std::endl;
                  ++cutCompareDebugPrintCount;
                }

                if (useOfficialTrackCuts_ ? !passOfficial : !passLegacy) continue;

	htrkpt->Fill(track->pt());
	htrketa->Fill(track->eta());
	  
	    	all_trkqx += pt*cos(2*phi);
	    	all_trkqy += pt*sin(2*phi);
	    	all_trkPt += pt;
      

    	for (unsigned i=0; i<dauEta.size(); ++i)
    	{
            if( abs(dauEta[i] - eta) < 1.E-3 && abs(reco::deltaPhi(dauPhi[i], phi)) < 1.E-3) DauTrk = true; 
	    float deltaEta = std::abs(dauEta[i] - eta);
    	    float deltaPhi = std::abs(reco::deltaPhi(dauPhi[i], phi));
	    float deltaPt = std::abs(dauPt[i] - pt) / pt;
	    hdeltaEta->Fill(deltaEta);
	    hdeltaPhi->Fill(deltaPhi);
	    hdeltaPt->Fill(deltaPt);
   	}

    	if (DauTrk == true) {
	    //cout << "it = " << it << "; DauTrk = "<< DauTrk << endl;
           // cout << "Matched track Pt Eta Phi = " << track->pt() <<' '<< track->eta()<<' '<<track->phi()<<endl;
            subt++;
      	    continue;
            
    	}
    
    	trkqx += pt*cos(2*phi);
    	trkqy += pt*sin(2*phi);
    	trkPt += pt;

	trkqx_v3 += pt*cos(3*phi);
	trkqy_v3 += pt*sin(3*phi);

	if (eta>0.5&&eta<2.4) {
	   trkPt_forw += pt;
	   trkqx_forw += pt*cos(2*phi);
	   trkqy_forw += pt*sin(2*phi);
	   trkqx_v3_forw += pt*cos(3*phi);
	   trkqy_v3_forw += pt*sin(3*phi);
           htrketa_forw->Fill(eta);
	}
	if (eta>-2.4&&eta<-0.5) {
	   trkPt_afterw += pt;
	   trkqx_afterw += pt*cos(2*phi);
	   trkqy_afterw += pt*sin(2*phi);
	   trkqx_v3_afterw += pt*cos(3*phi);
	   trkqy_v3_afterw += pt*sin(3*phi);
     	   htrketa_afterw->Fill(eta);
	}

  }
  
  trkQx = trkqx/trkPt;
  trkQy = trkqy/trkPt;
  all_trkQx = all_trkqx/all_trkPt;
  all_trkQy = all_trkqy/all_trkPt;
  trkQx_v3 = trkqx_v3/trkPt;
  trkQy_v3 = trkqy_v3/trkPt;

  trkQx_forw = trkqx_forw/trkPt_forw;
  trkQy_forw = trkqy_forw/trkPt_forw;
  trkQx_v3_forw = trkqx_v3_forw/trkPt_forw;
  trkQy_v3_forw = trkqy_v3_forw/trkPt_forw;
  trkQx_afterw = trkqx_afterw/trkPt_afterw;
  trkQy_afterw = trkqy_afterw/trkPt_afterw;
  trkQx_v3_afterw = trkqx_v3_afterw/trkPt_afterw;
  trkQy_v3_afterw = trkqy_v3_afterw/trkPt_afterw;
}


// ------------ method called once each job just before starting event
//loop  ------------
void
PATEventPlaneTrackOfficial::beginJob()
{
  TH1D::SetDefaultSumw2();

  // Check inputs
  if(!doRecoNtuple_) throw cms::Exception("PATCompositeAnalyzer") << "No output for RECO Fix config!!" << std::endl;
  if(saveTree_) initTree();
  if(saveHistogram_) initHistogram();
  
}


void 
PATEventPlaneTrackOfficial::initTree()
{ 
  PATEventPlaneNtuple = fs->make< TTree>("EventPlane","EventPlane");

  if(doRecoNtuple_)
  {
    // Event info
    
    PATEventPlaneNtuple->Branch("RunNb",&runNb,"RunNb/i");
    PATEventPlaneNtuple->Branch("LSNb",&lsNb,"LSNb/i");
    PATEventPlaneNtuple->Branch("EventNb",&eventNb,"EventNb/i");
    PATEventPlaneNtuple->Branch("nPV",&nPV,"nPV/S");
    PATEventPlaneNtuple->Branch("bestvtxX",&bestvx,"bestvtxX/F");
    PATEventPlaneNtuple->Branch("bestvtxY",&bestvy,"bestvtxY/F");
    PATEventPlaneNtuple->Branch("bestvtxZ",&bestvz,"bestvtxZ/F");
    
    if(isCentrality_) 
    {
      PATEventPlaneNtuple->Branch("centrality",&centrality,"centrality/S");
      PATEventPlaneNtuple->Branch("Ntrkoffline",&Ntrkoffline,"Ntrkoffline/I");
      PATEventPlaneNtuple->Branch("NtrkHP",&NtrkHP,"NtrkHP/I");
    }
    
    PATEventPlaneNtuple->Branch("trkQx",&trkQx,"trkQx/F");
    PATEventPlaneNtuple->Branch("trkQy",&trkQy,"trkQy/F");
    PATEventPlaneNtuple->Branch("all_trkQx",&all_trkQx,"all_trkQx/F");
    PATEventPlaneNtuple->Branch("all_trkQy",&all_trkQy,"all_trkQy/F");
    PATEventPlaneNtuple->Branch("trkQx_forw",&trkQx_forw,"trkQx_forw/F");
    PATEventPlaneNtuple->Branch("trkQy_forw",&trkQy_forw,"trkQy_forw/F");
    PATEventPlaneNtuple->Branch("trkQx_afterw",&trkQx_afterw,"trkQx_afterw/F");
    PATEventPlaneNtuple->Branch("trkQy_afterw",&trkQy_afterw,"trkQy_afterw/F");
    PATEventPlaneNtuple->Branch("trkQx_v3",&trkQx_v3,"trkQx_v3/F");
    PATEventPlaneNtuple->Branch("trkQy_v3",&trkQy_v3,"trkQy_v3/F");
    PATEventPlaneNtuple->Branch("trkQx_v3_forw",&trkQx_v3_forw,"trkQx_v3_forw/F");
    PATEventPlaneNtuple->Branch("trkQy_v3_forw",&trkQy_v3_forw,"trkQy_v3_forw/F");
    PATEventPlaneNtuple->Branch("trkQx_v3_afterw",&trkQx_v3_afterw,"trkQx_v3_afterw/F");
    PATEventPlaneNtuple->Branch("trkQy_v3_afterw",&trkQy_v3_afterw,"trkQy_v3_afterw/F");

  } // doRecoNtuple_

}

void
PATEventPlaneTrackOfficial::initHistogram()
{
  hdeltaEta = fs->make<TH1D>("hdeltaEta",";#Delta#eta",100000,0,1e-2);
  hdeltaPhi = fs->make<TH1D>("hdeltaPhi",";#Delta#phi",100000,0,1e-2);
  hdeltaPt = fs->make<TH1D>("hdeltaPt",";#Deltap_{T}",100000,0,1e-2);
  htrketa_forw = fs->make<TH1D>("htrketa_forw",";Track Eta in forward",500,-2.5,2.5);
  htrketa_afterw = fs->make<TH1D>("htrketa_afterw",";Track Eta in afterward",500,-2.5,2.5);
  htrkpt = fs->make<TH1D>("htrkpt", ";track pt", 100,0,5);
  htrketa = fs->make<TH1D>("htrketa",";track eta", 300, -3.,3.);
}


//--------------------------------------------------------------------------------------------------
void 
PATEventPlaneTrackOfficial::beginRun(const edm::Run& iRun, const edm::EventSetup& iSetup)
{
}


// ------------ method called once each job just after ending the event
//loop  ------------
void 
PATEventPlaneTrackOfficial::endJob()
{
  const unsigned long long nPassBoth = nLegacyPassCuts_ - nLegacyOnlyPassCuts_;
  std::cout << "[PATEventPlaneTrackOfficial][cutCompare] mode=" << (useOfficialTrackCuts_ ? "official" : "legacy")
            << " compared_tracks=" << nTracksComparedCuts_
            << " legacy_pass=" << nLegacyPassCuts_
            << " official_pass=" << nOfficialPassCuts_
            << " legacy_only=" << nLegacyOnlyPassCuts_
            << " official_only=" << nOfficialOnlyPassCuts_
            << " pass_both=" << nPassBoth
            << std::endl;
}

//define this as a plug-in
DEFINE_FWK_MODULE(PATEventPlaneTrackOfficial);
