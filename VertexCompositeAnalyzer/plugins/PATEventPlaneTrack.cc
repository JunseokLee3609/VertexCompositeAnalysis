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

class PATEventPlaneTrack : public edm::one::EDAnalyzer<edm::one::WatchRuns> {
public:
  explicit PATEventPlaneTrack(const edm::ParameterSet&);
  ~PATEventPlaneTrack();


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

  void collectFinalStateDaughters(const reco::Candidate& cand);

};

//
// static data member definitions
//

//
// constructors and destructor
//

PATEventPlaneTrack::PATEventPlaneTrack(const edm::ParameterSet& iConfig) :
  patCompositeCandidateCollection_Token_(consumes<pat::CompositeCandidateCollection>(iConfig.getUntrackedParameter<edm::InputTag>("VertexCompositeCollection")))
{
  //options
  doRecoNtuple_ = iConfig.getUntrackedParameter<bool>("doRecoNtuple");
  saveTree_ = iConfig.getUntrackedParameter<bool>("saveTree");
  saveHistogram_ = iConfig.getUntrackedParameter<bool>("saveHistogram");

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

}


PATEventPlaneTrack::~PATEventPlaneTrack()
{

  // do anything here that needs to be done at desctruction time
  // (e.g. close files, deallocate resources etc.)

}


//
// member functions
//

// ------------ method called to for each event  ------------
void
PATEventPlaneTrack::analyze(const edm::Event& iEvent, const edm::EventSetup& iSetup)
{
  //check event
  if(doRecoNtuple_) fillRECO(iEvent,iSetup);
  //if(saveTree_&&centrality>=80) PATEventPlaneNtuple->Fill();
  if(saveTree_) PATEventPlaneNtuple->Fill();
}


void
PATEventPlaneTrack::collectFinalStateDaughters(const reco::Candidate& cand)
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
PATEventPlaneTrack::fillRECO(const edm::Event& iEvent, const edm::EventSetup& iSetup)
{
  //get collection
  edm::Handle<reco::BeamSpot> beamspot;
  iEvent.getByToken(tok_offlineBS_, beamspot);
  edm::Handle<reco::VertexCollection> vertices;
  iEvent.getByToken(tok_offlinePV_, vertices);
  if(!vertices.isValid()) throw cms::Exception("PATEventPlaneTrack") << "Primary vertices  collection not found!" << std::endl;

  edm::Handle<pat::CompositeCandidateCollection> v0candidates;
  iEvent.getByToken(patCompositeCandidateCollection_Token_, v0candidates);
  if(!v0candidates.isValid()) throw cms::Exception("PATEventPlaneTrack") << "V0 candidate collection not found!" << std::endl;

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
  bestvzError = vtx.zError(), bestvxError = vtx.xError(), bestvyError = vtx.yError();

  //RECO Candidate info
  candSize = v0candidates->size();
  if(candSize>MAXCAN) throw cms::Exception("PATEventPlaneTrack") << "Number of candidates (" << candSize << ") exceeds limit!" << std::endl; 
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
  for(unsigned it=0; it<trackColl->size(); ++it){

	DauTrk = false;
	reco::TrackRef track(trackColl, it);

	float dzvtx = track->dz(bestvtx);
        float dxyvtx = track->dxy(bestvtx);
        float dzerror = sqrt(track->dzError()*track->dzError()+bestvzError*bestvzError);
        float dxyerror = sqrt(track->d0Error()*track->d0Error()+bestvxError*bestvyError);
        
        if(!track->quality(reco::TrackBase::highPurity)) continue;
        if(fabs(track->ptError())/track->pt()>0.10) continue;
        if(fabs(dzvtx/dzerror) > 3) continue;
        if(fabs(dxyvtx/dxyerror) > 3) continue;
	if(track->pt()<=0.3 || track->pt()>=3.0) continue;
	if(abs(track->eta())>=2.4) continue;

	htrkpt->Fill(track->pt());
	htrketa->Fill(track->eta());
	  
	float pt  = track->pt();
	float phi = track->phi();
	float eta = track->eta();

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
PATEventPlaneTrack::beginJob()
{
  TH1D::SetDefaultSumw2();

  // Check inputs
  if(!doRecoNtuple_) throw cms::Exception("PATCompositeAnalyzer") << "No output for RECO Fix config!!" << std::endl;
  if(saveTree_) initTree();
  if(saveHistogram_) initHistogram();
  
}


void 
PATEventPlaneTrack::initTree()
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
PATEventPlaneTrack::initHistogram()
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
PATEventPlaneTrack::beginRun(const edm::Run& iRun, const edm::EventSetup& iSetup)
{
}


// ------------ method called once each job just after ending the event
//loop  ------------
void 
PATEventPlaneTrack::endJob()
{
}

//define this as a plug-in
DEFINE_FWK_MODULE(PATEventPlaneTrack);
