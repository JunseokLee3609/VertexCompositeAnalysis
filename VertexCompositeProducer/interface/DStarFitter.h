// -*- C++ -*-
//
// Package:    VertexCompositeProducer
// Class:      DStarFitter
// 
/**\class DStarFitter DStarFitter.h VertexCompositeAnalysis/VertexCompositeProducer/interface/DStarFitter.h

 Description: <one line class summary>

 Implementation:
     <Notes on implementation>
*/
//
// Original Class Author:  Wei Li
// Modified Class Author:  Soohwan Lee
//
//

#ifndef VertexCompositeAnalysis__DStar_FITTER_H
#define VertexCompositeAnalysis__DStar_FITTER_H

#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/ESHandle.h"
#include "FWCore/Utilities/interface/InputTag.h"
#include "FWCore/Framework/interface/ConsumesCollector.h"

#include "DataFormats/Common/interface/Ref.h"

#include "DataFormats/VertexReco/interface/Vertex.h"
#include "DataFormats/VertexReco/interface/VertexFwd.h"
#include "DataFormats/TrackReco/interface/Track.h"
#include "RecoVertex/VertexPrimitives/interface/TransientVertex.h"
#include "TrackingTools/TransientTrack/interface/TransientTrack.h"
#include "RecoVertex/KalmanVertexFit/interface/KalmanVertexFitter.h"
#include "RecoVertex/AdaptiveVertexFit/interface/AdaptiveVertexFitter.h"

#include "RecoVertex/KinematicFit/interface/KinematicParticleVertexFitter.h"
#include "RecoVertex/KinematicFit/interface/KinematicParticleFitter.h"
#include "RecoVertex/KinematicFit/interface/MassKinematicConstraint.h"
#include "RecoVertex/KinematicFitPrimitives/interface/KinematicParticle.h"
#include "RecoVertex/KinematicFitPrimitives/interface/RefCountedKinematicParticle.h"
#include "RecoVertex/KinematicFitPrimitives/interface/TransientTrackKinematicParticle.h"
#include "RecoVertex/KinematicFitPrimitives/interface/KinematicParticleFactoryFromTransientTrack.h"
#include "TrackingTools/TransientTrack/interface/TransientTrackFromFTSFactory.h"

#include "MagneticField/Records/interface/IdealMagneticFieldRecord.h"
#include "MagneticField/VolumeBasedEngine/interface/VolumeBasedMagneticField.h"

// #include "DataFormats/Candidate/interface/VertexCompositeCandidate.h"
#include "DataFormats/PatCandidates/interface/CompositeCandidate.h"
#include "DataFormats/RecoCandidate/interface/RecoChargedCandidate.h"
#include "DataFormats/Math/interface/angle.h"
#include "DataFormats/TrackingRecHit/interface/TrackingRecHit.h"
#include "DataFormats/TrackReco/interface/DeDxData.h"

// DCA
#include "DataFormats/GeometryCommonDetAlgo/interface/Measurement1D.h"

#include "FWCore/ParameterSet/interface/ParameterSet.h"

#include "Geometry/CommonDetUnit/interface/TrackingGeometry.h"
//#include "Geometry/CommonDetUnit/interface/GeomDetUnit.h"
#include "Geometry/TrackerGeometryBuilder/interface/TrackerGeometry.h"
#include "Geometry/Records/interface/TrackerDigiGeometryRecord.h"
#include "Geometry/CommonDetUnit/interface/GeomDet.h"
//#include "Geometry/TrackerGeometryBuilder/interface/GluedGeomDet.h"

#include "CommonTools/UtilAlgos/interface/TFileService.h"

#include "CondFormats/GBRForest/interface/GBRForest.h"
#include "CondFormats/DataRecord/interface/GBRWrapperRcd.h"

#include <string>
#include <fstream>
#include <typeinfo>
#include <memory>
#include <vector>
#include <utility>
#include <algorithm>
#include <map>
#include <array>

class TH1D;
class TH2D;

class DStarFitter {
  public:
  using CC = pat::CompositeCandidate;
  using CCC = pat::CompositeCandidateCollection;
  DStarFitter(const edm::ParameterSet& theParams, edm::ConsumesCollector && iC);
  ~DStarFitter();

  void fitAll(const edm::Event& iEvent, const edm::EventSetup& iSetup);

  // Switching to L. Lista's reco::Candidate infrastructure for D0 storage
  const CCC& getDStar() const;
  const std::vector<float>& getDCAVals() const;
  const std::vector<float>& getDCAErrs() const;
  const std::vector<float>& getMVAVals() const; 

//  auto_ptr<edm::ValueMap<float> > getMVAMap() const;
  void resetAll();

 private:
  // STL vector of VertexCompositeCandidate that will be filled with VertexCompositeCandidates by fitAll()
  CCC theDStars;

  // Tracker geometry for discerning hit positions
  const TrackerGeometry* trackerGeom;

  const MagneticField* magField;

  edm::ESGetToken<MagneticField, IdealMagneticFieldRecord> bField_esToken_;

  edm::InputTag recoAlg;
  edm::InputTag vtxAlg;
  edm::EDGetTokenT<reco::TrackCollection> token_tracks;
  edm::EDGetTokenT<reco::VertexCollection> token_vertices;
  edm::EDGetTokenT<CCC> token_d0cand;
  edm::EDGetTokenT<edm::ValueMap<reco::DeDxData> > token_dedx;
  edm::EDGetTokenT<reco::BeamSpot> token_beamSpot;

  // Cuts
  double mPiKCutMin;
  double mPiKCutMax;
  double tkDCACut;
  double tkChi2Cut;
  int    tkNhitsCut;
  double tkPtErrCut;
  double tkPtCut;
  double tkEtaCut;
  double tkPtSumCut;
  double tkEtaDiffCut;
  double chi2Cut;
  double rVtxCut;
  double rVtxSigCut;
  double lVtxCut;
  double lVtxSigCut;
  double collinCut2D;
  double collinCut3D;
  double dStarMassCut;
  double dStarAbsYCut;
  double d0MassCut;
  double dauTransImpactSigCut;
  double dauLongImpactSigCut;
  double VtxChiProbCut;
  double dPtCut;
  double alphaCut;
  double alpha2DCut;
  bool   isWrongSign;
  bool   useRawDStarKinematics_;
  bool   debugCategoryCutflow_;
  bool   debugSlowPionPtScan_;
  bool   rejectDuplicateSlowPion_;
  bool   debugHistogramsBooked_ = false;
  std::string debugLabel_;
  enum SlowPionPtScanStage {
    kSlowPiPtAttach = 0,
    kSlowPiPtDeltaM,
    kSlowPiPtCharge,
    kSlowPiPtDStarVertex,
    kSlowPiPtDStarPt,
    kSlowPiPtFinalMass,
    kSlowPiPtNStage
  };
  static constexpr int kSlowPiPtNThreshold = 3;
  std::array<std::array<unsigned long long, kSlowPiPtNStage>, kSlowPiPtNThreshold> slowPiPtScanCounts_{};
  void slowPiPtScanFill(double slowPiPt, SlowPionPtScanStage stage);
  void printSlowPionPtScan() const;

  enum DebugCategory { kDebugA = 0, kDebugB = 1, kDebugC = 2, kDebugD = 3, kDebugNCategory = 4 };
  enum DebugStep {
    kDebugSlowPionAttach = 0,
    kDebugDeltaMCalculated,
    kDebugDeltaMLt0500,
    kDebugDeltaMLt0300,
    kDebugDeltaMLt0250,
    kDebugDeltaMLt0200,
    kDebugDeltaMLt0180,
    kDebugDeltaMLt0165,
    kDebugDeltaMLt0160,
    kDebugCharge,
    kDebugD0Tree,
    kDebugDStarVertex,
    kDebugDStarState,
    kDebugDecayVertex,
    kDebugVtxProb,
    kDebugChildState,
    kDebugPt,
    kDebugY,
    kDebugTsos,
    kDebugTopology,
    kDebugFinalMass,
    kDebugNStep
  };
  std::array<std::array<unsigned long long, kDebugNStep>, kDebugNCategory> debugCutflow_{};
  std::array<double, kDebugNCategory> debugMinDeltaM_{};
  std::array<double, kDebugNCategory> debugMaxDeltaM_{};
  std::array<unsigned long long, kDebugNCategory> debugInvalidMass_{};
  std::array<unsigned long long, kDebugNCategory> debugDeltaMLtPionMass_{};
  std::array<unsigned long long, kDebugNCategory> debugDuplicateTrack_{};
  std::array<TH1D*, kDebugNCategory> hDebugRawDStarPt_{};
  std::array<TH1D*, kDebugNCategory> hDebugFitterDStarPt_{};
  std::array<TH1D*, kDebugNCategory> hDebugRawD0Pt_{};
  std::array<TH1D*, kDebugNCategory> hDebugFittedD0Pt_{};
  std::array<TH1D*, kDebugNCategory> hDebugSlowPiPt_{};
  std::array<TH1D*, kDebugNCategory> hDebugOpeningAngle_{};
  std::array<TH1D*, kDebugNCategory> hDebugQValue_{};
  std::array<TH2D*, kDebugNCategory> hDebugRawVsFitterDStarPt_{};
  std::array<TH2D*, kDebugNCategory> hDebugD0PtVsDStarPt_{};
  std::array<TH2D*, kDebugNCategory> hDebugSlowPiPtVsDStarPt_{};
  std::array<TH2D*, kDebugNCategory> hDebugQVsDStarPt_{};
  int debugCategoryIndex(int qK, int qPiD0, int qPiS) const;
  void debugFill(int category, DebugStep step);
  void bookDebugHistograms();
  void fillDebugPrePtHistograms(int category, double rawDStarPt, double fitterDStarPt, double rawD0Pt,
                                double fittedD0Pt, double slowPiPt, double openingAngle, double qValue);
  void printDebugCutflow() const;

  std::vector<reco::TrackBase::TrackQuality> qualities;

  //setup mva selector
  bool useAnyMVA_;
  std::vector<bool> useMVA_;
  std::vector<double> min_MVA_;
  std::string mvaType_;
  std::string forestLabel_;
  GBRForest * forest_;
  bool useForestFromDB_;

  std::vector<float> mvaVals_;

//  auto_ptr<edm::ValueMap<float> >mvaValValueMap;
//  MVACollection mvas; 
  // DCA
  std::vector<float> dcaVals_;
  std::vector<float> dcaErrs_;

  std::string dbFileName_;

};

#endif
