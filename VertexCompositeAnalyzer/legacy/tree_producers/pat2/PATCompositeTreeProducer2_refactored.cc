// Debug control options:
// 1. Compile-time control: uncomment the line below
//#define DEBUG_GEN_MATCHING 1

// 2. CMSSW logging system (preferred): use LogDebug
#include "FWCore/MessageLogger/interface/MessageLogger.h"

#include "VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer2_refactored.h"
#include <tuple>
#include <vector>
#include <algorithm>
#include <iterator>
#include <limits>
#include "DataFormats/Math/interface/deltaR.h"

// Debugging macros for better control
#ifdef DEBUG_GEN_MATCHING
    #define DEBUG_MSG(msg) std::cout << "DEBUG: " << msg << std::endl
    #define DEBUG_GEN(msg) std::cout << "DEBUG [GenMatching]: " << msg << std::endl
#else
    #define DEBUG_MSG(msg) do {} while(0)
    #define DEBUG_GEN(msg) do {} while(0)
#endif


// Use LogDebug and LogInfo directly instead of macros to avoid conflicts

// Using declarations for convenience
using namespace std;
using namespace edm;
using namespace reco;

PATCompositeTreeProducer2Refactored::PATCompositeTreeProducer2Refactored(const edm::ParameterSet& iConfig)
{
    doRecoNtuple_ = iConfig.getUntrackedParameter<bool>("doRecoNtuple");
    doGenNtuple_ = iConfig.getUntrackedParameter<bool>("doGenNtuple");
    twoLayerDecay_ = iConfig.getUntrackedParameter<bool>("twoLayerDecay");
    doGenMatching_ = iConfig.getUntrackedParameter<bool>("doGenMatching");
    doGenMatchingTOF_ = iConfig.getUntrackedParameter<bool>("doGenMatchingTOF");
    hasSwap_ = iConfig.getUntrackedParameter<bool>("hasSwap");
    decayInGen_ = iConfig.getUntrackedParameter<bool>("decayInGen");
    doMuon_ = iConfig.getUntrackedParameter<bool>("doMuon", false);
    doMuonFull_ = iConfig.getUntrackedParameter<bool>("doMuonFull", false);
    PID_ = iConfig.getUntrackedParameter<int>("PID");
    PID_dau1_ = iConfig.getUntrackedParameter<int>("PID_dau1");
    PID_dau2_ = iConfig.getUntrackedParameter<int>("PID_dau2");
    
    saveTree_ = iConfig.getUntrackedParameter<bool>("saveTree");
    saveHistogram_ = iConfig.getUntrackedParameter<bool>("saveHistogram");
    saveAllHistogram_ = iConfig.getUntrackedParameter<bool>("saveAllHistogram");
    massHistPeak_ = iConfig.getUntrackedParameter<double>("massHistPeak");
    massHistWidth_ = iConfig.getUntrackedParameter<double>("massHistWidth");
    massHistBins_ = iConfig.getUntrackedParameter<int>("massHistBins");

    useAnyMVA_ = iConfig.getParameter<bool>("useAnyMVA");
    isSkimMVA_ = iConfig.getUntrackedParameter<bool>("isSkimMVA"); 

    multMax_ = iConfig.getUntrackedParameter<double>("multMax", -1);
    multMin_ = iConfig.getUntrackedParameter<double>("multMin", -1);
    deltaR_ = iConfig.getUntrackedParameter<double>("deltaR", 0.1);
    
    // Debug control parameters (runtime configurable)
    debugGenMatching_ = iConfig.getUntrackedParameter<bool>("debugGenMatching", false);
    verboseDebug_ = iConfig.getUntrackedParameter<bool>("verboseDebug", false);

    pTBins_ = iConfig.getUntrackedParameter< std::vector<double> >("pTBins");
    yBins_  = iConfig.getUntrackedParameter< std::vector<double> >("yBins");

    tok_offlinePV_ = consumes<reco::VertexCollection>(iConfig.getUntrackedParameter<edm::InputTag>("VertexCollection"));
    tok_generalTrk_ = consumes<reco::TrackCollection>(iConfig.getUntrackedParameter<edm::InputTag>("TrackCollection"));
    PATCompositeCandidateCollection_Token_ = consumes<CCC>(iConfig.getUntrackedParameter<edm::InputTag>("CompositeCollection"));
    MVAValues_Token_ = consumes<MVACollection>(iConfig.getParameter<edm::InputTag>("MVACollection"));
    tok_muon_ = consumes<reco::MuonCollection>(iConfig.getUntrackedParameter<edm::InputTag>("MuonCollection", edm::InputTag()));
    Dedx_Token1_ = consumes<edm::ValueMap<reco::DeDxData> >(edm::InputTag("dedxHarmonic2"));
    Dedx_Token2_ = consumes<edm::ValueMap<reco::DeDxData> >(edm::InputTag("dedxTruncated40"));
    tok_genParticle_ = consumes<reco::GenParticleCollection>(edm::InputTag(iConfig.getUntrackedParameter<edm::InputTag>("GenParticleCollection")));
    tok_genInfo_ = consumes<GenEventInfoProduct>(edm::InputTag("generator"));

    isCentrality_ = false;
    if(iConfig.exists("isCentrality")) isCentrality_ = iConfig.getParameter<bool>("isCentrality");
    if(isCentrality_)
    {
      tok_centBinLabel_ = consumes<int>(iConfig.getParameter<edm::InputTag>("centralityBinLabel"));
      tok_centSrc_ = consumes<reco::Centrality>(iConfig.getParameter<edm::InputTag>("centralitySrc"));
    }

    isEventPlane_ = false;
    if(iConfig.exists("isEventPlane")) isEventPlane_ = iConfig.getParameter<bool>("isEventPlane");
    compareEventPlane_ = false;
    if(isEventPlane_)
    {
      tok_eventplaneSrc_ = consumes<reco::EvtPlaneCollection>(iConfig.getParameter<edm::InputTag>("eventplaneSrc"));
      if (iConfig.exists("eventplaneSrcRecalc")) {
        const auto recalcTag = iConfig.getParameter<edm::InputTag>("eventplaneSrcRecalc");
        if (!recalcTag.label().empty()) {
          tok_eventplaneSrcRecalc_ = consumes<reco::EvtPlaneCollection>(recalcTag);
          compareEventPlane_ = true;
        }
      }
    }

}
PATCompositeTreeProducer2Refactored::~PATCompositeTreeProducer2Refactored()
{
}

void
PATCompositeTreeProducer2Refactored::analyze(const edm::Event& iEvent, const edm::EventSetup&
iSetup)
{
    using std::vector;
    using namespace edm;
    using namespace reco;

    if(doGenNtuple_) fillGEN(iEvent,iSetup);
    if(doRecoNtuple_) fillRECO(iEvent,iSetup);

    if(saveTree_) PATCompositeNtuple->Fill();
}

void
PATCompositeTreeProducer2Refactored::fillRECO(const edm::Event& iEvent, const edm::EventSetup& iSetup)
{
    edm::Handle<reco::VertexCollection> vertices;
    iEvent.getByToken(tok_offlinePV_,vertices);
    
    edm::Handle<reco::TrackCollection> tracks;
    iEvent.getByToken(tok_generalTrk_, tracks);

    edm::Handle<CCC> v0candidates;
    iEvent.getByToken(PATCompositeCandidateCollection_Token_,v0candidates);
    const CCC * v0candidates_ = v0candidates.product();
    
    edm::Handle<MVACollection> mvavalues;
    if(useAnyMVA_)
    {
      iEvent.getByToken(MVAValues_Token_,mvavalues);
      assert( (*mvavalues).size() == v0candidates->size() );
    }

    edm::Handle<reco::GenParticleCollection> genpars;
    if(doGenMatching_ || doGenMatchingTOF_) iEvent.getByToken(tok_genParticle_,genpars);

    edm::Handle<edm::ValueMap<reco::DeDxData> > dEdxHandle1;
    iEvent.getByToken(Dedx_Token1_, dEdxHandle1);
    
    edm::Handle<edm::ValueMap<reco::DeDxData> > dEdxHandle2;
    iEvent.getByToken(Dedx_Token2_, dEdxHandle2);
    
    centrality = -1;
    Npixel = -1;
    HFsumETPlus = INVALID_VALUE;
    HFsumETMinus = INVALID_VALUE;
    ZDCPlus = INVALID_VALUE;
    ZDCMinus = INVALID_VALUE;
    if (isCentrality_) {
      edm::Handle<reco::Centrality> cent;
      iEvent.getByToken(tok_centSrc_, cent);
      iEvent.getByToken(tok_centBinLabel_, cbin_);
      centrality = *cbin_;
      HFsumETPlus = cent->EtHFtowerSumPlus();
      HFsumETMinus = cent->EtHFtowerSumMinus();
      Npixel = cent->multiplicityPixel();
      ZDCPlus = cent->zdcSumPlus();
      ZDCMinus = cent->zdcSumMinus();
      edm::LogPrint("CentralityDebug") << "PATCompositeTreeProducer2Refactored centrality run="
                                       << iEvent.id().run() << " lumi="
                                       << iEvent.luminosityBlock() << " event="
                                       << iEvent.id().event() << " cbin=" << centrality;
    }

    processEventPlaneInfo(iEvent);

    bestvz = -999.9;
    bestvx = -999.9;
    bestvy = -999.9;
    double bestvzError = -999.9;
    const reco::Vertex& vtx = (*vertices)[0];
    bestvz = vtx.z();
    bestvx = vtx.x();
    bestvy = vtx.y();
    bestvzError = vtx.zError();

    Ntrkoffline = 0;
    if (multMax_ != -1 && multMin_ != -1) {
      for (unsigned it = 0; it < tracks->size(); ++it) {
        const reco::Track& trk = (*tracks)[it];
        math::XYZPoint bestvtx(bestvx, bestvy, bestvz);

        const double dzvtx = trk.dz(bestvtx);
        const double dxyvtx = trk.dxy(bestvtx);
        const double dzerror = sqrt(trk.dzError() * trk.dzError() + bestvzError * bestvzError);
        const double dxyerror = trk.dxyError(bestvtx, vtx.covariance());

        if (!trk.quality(reco::TrackBase::highPurity))
          continue;
        if (fabs(trk.ptError()) / trk.pt() > PT_ERROR_THRESHOLD)
          continue;
        if (fabs(dzvtx / dzerror) > DZ_SIGNIFICANCE_CUT)
          continue;
        if (fabs(dxyvtx / dxyerror) > DXY_SIGNIFICANCE_CUT)
          continue;

        const double trackEta = trk.eta();
        const double trackPt = trk.pt();
        if (fabs(trackEta) > ETA_CUT)
          continue;
        if (trackPt <= PT_CUT)
          continue;
        Ntrkoffline++;
      }
    }
    
    std::vector<reco::GenParticleRef> genRefs;
    if(doGenMatching_) {
        genRefs = processGenMatching(genpars);
    }
    
    processCandidates(v0candidates_, mvavalues, genRefs, dEdxHandle1, dEdxHandle2, iEvent, vertices, genpars);
}

void PATCompositeTreeProducer2Refactored::processCandidates(const CCC* v0candidates_,
                                                   const edm::Handle<MVACollection>& mvavalues,
                                                   const std::vector<reco::GenParticleRef>& genRefs,
                                                   const edm::Handle<edm::ValueMap<reco::DeDxData>>& dEdxHandle1,
                                                   const edm::Handle<edm::ValueMap<reco::DeDxData>>& dEdxHandle2,
                                                   const edm::Event& iEvent,
                                                   const edm::Handle<reco::VertexCollection>& vertices,
                                                   const edm::Handle<reco::GenParticleCollection>& genpars) {
    
    candSize = v0candidates_->size();
    
    // Local variables for vertex coordinates
    double secvx = 0, secvy = 0, secvz = 0;
    double bestvzError = 0;
    
    // Initialize vertex errors from best vertex if available
    reco::Vertex vtx;
    if(!vertices->empty()) {
        vtx = vertices->front();
        bestvzError = sqrt(vtx.covariance(2,2));
    }
    
    // Debug: Event processing information
    LogDebug("PATCompositeTreeProducer") << "Processing event with " << candSize << " candidates";
    DEBUG_MSG("Processing event with " << candSize << " candidates");
    
    for(unsigned it = 0; it < v0candidates_->size(); ++it) {
        if(verboseDebug_) {
          LogDebug("PATCompositeTreeProducer") << "Processing candidate " << it << "/" << candSize;
          DEBUG_MSG("Processing candidate " << it << "/" << candSize);
        }
        const CC & trk = (*v0candidates_)[it];
        
        eta[it] = trk.eta();
        y[it] = trk.rapidity();
        pt[it] = trk.pt();
        phi[it] = trk.phi();
        flavor[it] = trk.pdgId()/abs(trk.pdgId());
        mass[it] = trk.mass();

        mva[it] = 0.0;
        if(useAnyMVA_) mva[it] = (*mvavalues)[it];
        if(trk.hasUserFloat("D0mva")) mva[it] = trk.userFloat("D0mva");
        if(trk.hasUserFloat("mva")) mva[it] = trk.userFloat("mva");

        const reco::Candidate * d1 = trk.daughter(0);
        const reco::Candidate * gd1;
        const reco::Candidate * gd2;
        if(twoLayerDecay_){
          gd1 = d1->daughter(0);
          gd2 = d1->daughter(1);

        }
        const reco::Candidate * d2 = trk.daughter(1);

        matchGen_slowPion_dR_[it] = INVALID_VALUE;
        massD2[it] = INVALID_VALUE;

	        if(doGenMatching_ )
	        {
	          if(debugGenMatching_) {
	            LogDebug("PATCompositeTreeProducer") << "Starting gen matching for candidate " << it;
	            DEBUG_GEN("Starting gen matching for candidate " << it);
	          }
	          // Reset all gen-match outputs for this candidate so unmatched entries cannot
	          // inherit values from previous events/candidates.
	          resetRecoGenMatch(it);
	          if( twoLayerDecay_ ){
            if(debugGenMatching_) {
              LogDebug("PATCompositeTreeProducer") << "Two-layer decay mode for candidate " << it;
              DEBUG_GEN("Two-layer decay mode for candidate " << it);
            }

		            const unsigned int nGen = genRefs.size();
		            if (nGen == 0 && debugGenMatching_) {
		              LogDebug("PATCompositeTreeProducer") << "No genRefs available for candidate " << it;
		              DEBUG_GEN("No genRefs available for candidate " << it);
		            }

	            const reco::Candidate* recoD0 = nullptr;
	            const reco::Candidate* recoSlow = nullptr;
	            if (d1 && std::abs(d1->pdgId()) == D0_PDG_ID) {
	              recoD0 = d1;
	              recoSlow = d2;
	            } else if (d2 && std::abs(d2->pdgId()) == D0_PDG_ID) {
	              recoD0 = d2;
	              recoSlow = d1;
	            } else {
	              recoD0 = d1;
	              recoSlow = d2;
	            }

		            const reco::Candidate* recoD0DauA =
		                (recoD0 && recoD0->numberOfDaughters() >= 2) ? recoD0->daughter(0) : nullptr;
		            const reco::Candidate* recoD0DauB =
		                (recoD0 && recoD0->numberOfDaughters() >= 2) ? recoD0->daughter(1) : nullptr;
		            const bool validReco = (recoD0 && recoSlow && recoD0DauA && recoD0DauB);

		            if (debugGenMatching_) {
		              LogInfo("GenMatchingFlow")
		                  << "[PreMatch][Reco] cand=" << it
		                  << " nGenRefs=" << nGen
		                  << " recoD0(pdg,q,nDau)=("
		                  << (recoD0 ? recoD0->pdgId() : -99999) << ","
		                  << (recoD0 ? recoD0->charge() : -99) << ","
		                  << (recoD0 ? static_cast<int>(recoD0->numberOfDaughters()) : -1) << ")"
		                  << " recoSlow(pdg,q)=("
		                  << (recoSlow ? recoSlow->pdgId() : -99999) << ","
		                  << (recoSlow ? recoSlow->charge() : -99) << ")"
			                  << " recoD0DauA(pdg,q,pt,eta)=("
			                  << (recoD0DauA ? recoD0DauA->pdgId() : -99999) << ","
			                  << (recoD0DauA ? recoD0DauA->charge() : -99) << ","
			                  << (recoD0DauA ? recoD0DauA->pt() : INVALID_VALUE) << ","
			                  << (recoD0DauA ? recoD0DauA->eta() : INVALID_VALUE) << ")"
			                  << " recoD0DauB(pdg,q,pt,eta)=("
			                  << (recoD0DauB ? recoD0DauB->pdgId() : -99999) << ","
			                  << (recoD0DauB ? recoD0DauB->charge() : -99) << ","
			                  << (recoD0DauB ? recoD0DauB->pt() : INVALID_VALUE) << ","
			                  << (recoD0DauB ? recoD0DauB->eta() : INVALID_VALUE) << ")";

		              for (unsigned int igen = 0; igen < nGen; ++igen) {
		                const auto& genRef = genRefs.at(igen);
		                const reco::GenParticle* genDStar = genRef.get();
		                const reco::GenParticle* genD0 = nullptr;
		                const reco::GenParticle* genSlowPi = nullptr;
		                int dstarGamma = 0;
		                int dstarNonGamma = 0;
		                int dstarD0Count = 0;
		                int dstarPiCount = 0;
		                if (genDStar) {
		                  for (size_t idau = 0; idau < genDStar->numberOfDaughters(); ++idau) {
		                    const auto* dau = genDStar->daughter(idau);
		                    if (!dau) continue;
		                    const int absId = std::abs(dau->pdgId());
		                    if (absId == 22) {
		                      ++dstarGamma;
		                      continue;
		                    }
		                    ++dstarNonGamma;
		                    if (absId == D0_PDG_ID && !genD0) genD0 = dynamic_cast<const reco::GenParticle*>(dau);
		                    if (absId == PION_PDG_ID && !genSlowPi) genSlowPi = dynamic_cast<const reco::GenParticle*>(dau);
		                    if (absId == D0_PDG_ID) ++dstarD0Count;
		                    if (absId == PION_PDG_ID) ++dstarPiCount;
		                  }
		                }

		                const auto d0Summary = summarizeD0Daughters(genD0);
		                LogInfo("GenMatchingFlow")
		                    << "[PreMatch][GenRef] cand=" << it
		                    << " igen=" << igen
		                    << " dstar(pdg,q,nDau)=("
		                    << (genDStar ? genDStar->pdgId() : -99999) << ","
		                    << (genDStar ? genDStar->charge() : -99) << ","
		                    << (genDStar ? static_cast<int>(genDStar->numberOfDaughters()) : -1) << ")"
		                    << " dstarCounts(nonGamma,gamma,D0,pi)=("
		                    << dstarNonGamma << "," << dstarGamma << ","
		                    << dstarD0Count << "," << dstarPiCount << ")"
		                    << " d0(pdg,q,nDau)=("
		                    << (genD0 ? genD0->pdgId() : -99999) << ","
		                    << (genD0 ? genD0->charge() : -99) << ","
		                    << (genD0 ? static_cast<int>(genD0->numberOfDaughters()) : -1) << ")"
		                    << " d0Summary(nonGamma,gamma,hasK,hasPi)=("
		                    << d0Summary.nonGammaCount << "," << d0Summary.gammaCount << ","
		                    << d0Summary.hasKaon << "," << d0Summary.hasPion << ")"
		                    << " slowPi(pdg,q)=("
		                    << (genSlowPi ? genSlowPi->pdgId() : -99999) << ","
		                    << (genSlowPi ? genSlowPi->charge() : -99) << ")";
		              }
		            }

		            if (!validReco) {
		              if(debugGenMatching_) {
		                LogDebug("PATCompositeTreeProducer") << "Incomplete reco daughters for candidate " << it;
		                DEBUG_GEN("Incomplete reco daughters for candidate " << it);
	              }
		            } else {
		              auto findGenD0KPi =
		                  [&](const reco::GenParticle* genD0) -> std::pair<const reco::GenParticle*, const reco::GenParticle*> {
		                std::pair<const reco::GenParticle*, const reco::GenParticle*> out(nullptr, nullptr);
		                if (!genD0) return out;
		                for (size_t idau = 0; idau < genD0->numberOfDaughters(); ++idau) {
		                  const auto* dau = genD0->daughter(idau);
		                  if (!dau || std::abs(dau->pdgId()) == 22) continue;
		                  const auto* genDau = dynamic_cast<const reco::GenParticle*>(dau);
		                  if (!genDau) continue;
		                  const int absId = std::abs(genDau->pdgId());
		                  if (absId == KAON_PDG_ID && !out.first) out.first = genDau;
		                  if (absId == PION_PDG_ID && !out.second) out.second = genDau;
		                }
		                return out;
		              };

		              auto scoreRecoPair =
		                  [&](const reco::GenParticle* genKaon,
		                      const reco::GenParticle* genPion,
		                      double& drKaon,
		                      double& drPion) -> bool {
		                drKaon = std::numeric_limits<double>::max();
		                drPion = std::numeric_limits<double>::max();
		                if (!genKaon || !genPion) return false;
			                const reco::Candidate* recoDaus[2] = {recoD0DauA, recoD0DauB};
		                int perm[2] = {0, 1};
		                bool matched = false;
		                do {
		                  const reco::Candidate* recoForKaon = recoDaus[perm[0]];
		                  const reco::Candidate* recoForPion = recoDaus[perm[1]];
		                  if (!recoForKaon || !recoForPion) continue;
		                  if (recoForKaon->charge() != genKaon->charge()) continue;
		                  if (recoForPion->charge() != genPion->charge()) continue;

		                  const double dRKaon = reco::deltaR(*recoForKaon, *genKaon);
		                  const double dRPion = reco::deltaR(*recoForPion, *genPion);
		                  if (dRKaon >= deltaR_ || dRPion >= deltaR_) continue;

		                  const double curScore = dRKaon + dRPion;
		                  if (!matched || curScore < (drKaon + drPion)) {
		                    matched = true;
		                    drKaon = dRKaon;
		                    drPion = dRPion;
		                  }
			                } while (std::next_permutation(std::begin(perm), std::end(perm)));
		                return matched;
		              };

		              const reco::GenParticle* bestGenDStar = nullptr;
		              const reco::GenParticle* bestGenD0 = nullptr;
		              const reco::GenParticle* bestGenSlowPi = nullptr;
		              const reco::GenParticle* bestGenKaon = nullptr;
		              const reco::GenParticle* bestGenPion = nullptr;
		              double bestSlowDr = std::numeric_limits<double>::max();
		              double bestScore = std::numeric_limits<double>::max();
		              int bestScoreIgen = -1;

		              for (unsigned int igen = 0; igen < nGen; ++igen) {
		                const auto& genRef = genRefs.at(igen);
		                const reco::GenParticle* genDStar = genRef.get();
		                if (!genDStar) {
		                  if (debugGenMatching_) {
		                    LogInfo("GenMatchingFlow")
		                        << "[MatchStep] cand=" << it << " igen=" << igen
		                        << " reject: null genDStar";
		                  }
		                  continue;
		                }
		                if (std::abs(genDStar->pdgId()) != std::abs(PID_)) {
		                  if (debugGenMatching_) {
		                    LogInfo("GenMatchingFlow")
		                        << "[MatchStep] cand=" << it << " igen=" << igen
		                        << " reject: wrong D* PDG="
		                        << genDStar->pdgId() << " expectedAbs=" << std::abs(PID_);
		                  }
		                  continue;
		                }

		                const reco::GenParticle* genD0 = nullptr;
		                const reco::GenParticle* genSlowPi = nullptr;
		                for (size_t idau = 0; idau < genDStar->numberOfDaughters(); ++idau) {
		                  const auto* dau = genDStar->daughter(idau);
		                  if (!dau || std::abs(dau->pdgId()) == 22) continue;
		                  const auto* genDau = dynamic_cast<const reco::GenParticle*>(dau);
		                  if (!genDau) continue;
		                  const int absId = std::abs(genDau->pdgId());
		                  if (absId == D0_PDG_ID && !genD0) genD0 = genDau;
		                  if (absId == PION_PDG_ID && !genSlowPi) genSlowPi = genDau;
		                }
		                if (!genD0 || !genSlowPi) {
		                  if (debugGenMatching_) {
		                    LogInfo("GenMatchingFlow")
		                        << "[MatchStep] cand=" << it << " igen=" << igen
		                        << " reject: missing D0/slowPi in D* daughters"
		                        << " genD0=" << (genD0 ? genD0->pdgId() : -99999)
		                        << " genSlowPi=" << (genSlowPi ? genSlowPi->pdgId() : -99999);
		                  }
		                  continue;
		                }
		                const auto d0Summary = summarizeD0Daughters(genD0);
			                const bool d0MatchedWithFSR = matchD0WithFSR(recoD0DauA, recoD0DauB, genD0, deltaR_);
		                if (!d0MatchedWithFSR) {
		                  if (debugGenMatching_) {
		                    LogInfo("GenMatchingFlow")
		                        << "[MatchStep] cand=" << it << " igen=" << igen
		                        << " reject: D0 daughters fail FSR-aware reco match"
		                        << " d0Summary(nonGamma,gamma,hasK,hasPi)=("
		                        << d0Summary.nonGammaCount << "," << d0Summary.gammaCount
		                        << "," << d0Summary.hasKaon << "," << d0Summary.hasPion << ")";
		                  }
		                  continue;
		                }
		                const bool slowPiMatched = matchTrackdR(recoSlow, genSlowPi, true);
		                const double slowDr = reco::deltaR(*recoSlow, *genSlowPi);
		                if (!slowPiMatched) {
		                  if (debugGenMatching_) {
		                    LogInfo("GenMatchingFlow")
		                        << "[MatchStep] cand=" << it << " igen=" << igen
		                        << " reject: slow pion fails dR/charge match"
		                        << " slowDr=" << slowDr;
		                  }
		                  continue;
		                }

		                const auto genKPi = findGenD0KPi(genD0);
		                const reco::GenParticle* genKaon = genKPi.first;
		                const reco::GenParticle* genPion = genKPi.second;
		                double drKaon = std::numeric_limits<double>::max();
		                double drPion = std::numeric_limits<double>::max();
		                const bool daughterPairMatched = scoreRecoPair(genKaon, genPion, drKaon, drPion);
		                if (!daughterPairMatched) {
		                  if (debugGenMatching_) {
		                    LogInfo("GenMatchingFlow")
		                        << "[MatchStep] cand=" << it << " igen=" << igen
		                        << " reject: K/pi pair assignment failed"
		                        << " genK=" << (genKaon ? genKaon->pdgId() : -99999)
		                        << " genPi=" << (genPion ? genPion->pdgId() : -99999);
		                  }
		                  continue;
		                }

		                const double score = drKaon + drPion + slowDr;
		                if (debugGenMatching_) {
		                  LogInfo("GenMatchingFlow")
		                      << "[MatchStep] cand=" << it << " igen=" << igen
		                      << " pass: drK=" << drKaon
		                      << " drPi=" << drPion
		                      << " drSlow=" << slowDr
		                      << " score=" << score
		                      << " bestScoreSoFar=" << bestScore;
		                }
		                if (score < bestScore) {
		                  bestScore = score;
		                  bestScoreIgen = static_cast<int>(igen);
		                  bestGenDStar = genDStar;
		                  bestGenD0 = genD0;
		                  bestGenSlowPi = genSlowPi;
		                  bestGenKaon = genKaon;
		                  bestGenPion = genPion;
		                  bestSlowDr = slowDr;
		                  if (debugGenMatching_) {
		                    LogInfo("GenMatchingFlow")
		                        << "[BestUpdate] cand=" << it
		                        << " newBestIgen=" << igen
		                        << " bestScore=" << bestScore;
		                  }
		                }
		              }

		              if (debugGenMatching_) {
		                LogDebug("GenMatchingCompare")
		                    << "Candidate " << it
		                    << " bestScoreIdx=" << bestScoreIgen
		                    << " bestScore=" << bestScore;
		              }

	              const bool allTracksMatched = bestGenDStar && bestGenD0 && bestGenSlowPi && bestGenKaon && bestGenPion;
		              if (!allTracksMatched) {
		                if(debugGenMatching_) {
		                  LogDebug("PATCompositeTreeProducer") << "No genRefs-based two-layer match for candidate " << it;
		                  DEBUG_GEN("No genRefs-based two-layer match for candidate " << it);
		                  LogInfo("GenMatchingFlow")
		                      << "[PostMatch] cand=" << it
		                      << " bestIgen=" << bestScoreIgen
		                      << " allTracksMatched=0 validChain=0 radiativeChain=0 finalMatchGEN=0"
		                      << " bestScore=" << bestScore
		                      << " reason=noBestGenChain";
		                }
		                matchGEN[it] = false;
		                matchGen_validDstarChain_[it] = false;
		              } else {
	                if(debugGenMatching_) {
	                  LogInfo("PATCompositeTreeProducer") << "genRefs two-layer matching SUCCESS for candidate " << it;
	                  DEBUG_GEN("genRefs two-layer matching SUCCESS for candidate " << it);
	                }

	                matchGen_D0Dau1_pT_[it] = bestGenKaon->pt();
	                matchGen_D0Dau1_eta_[it] = bestGenKaon->eta();
	                matchGen_D0Dau1_phi_[it] = bestGenKaon->phi();
	                matchGen_D0Dau1_mass_[it] = bestGenKaon->mass();
	                matchGen_D0Dau1_y_[it] = bestGenKaon->rapidity();
	                matchGen_D0Dau1_charge_[it] = bestGenKaon->charge();
	                matchGen_D0Dau1_pdgId_[it] = bestGenKaon->pdgId();

	                matchGen_D0Dau2_pT_[it] = bestGenPion->pt();
	                matchGen_D0Dau2_eta_[it] = bestGenPion->eta();
	                matchGen_D0Dau2_phi_[it] = bestGenPion->phi();
	                matchGen_D0Dau2_mass_[it] = bestGenPion->mass();
	                matchGen_D0Dau2_y_[it] = bestGenPion->rapidity();
	                matchGen_D0Dau2_charge_[it] = bestGenPion->charge();
	                matchGen_D0Dau2_pdgId_[it] = bestGenPion->pdgId();

	                matchGen_D1pT_[it] = bestGenSlowPi->pt();
	                matchGen_D1eta_[it] = bestGenSlowPi->eta();
	                matchGen_D1phi_[it] = bestGenSlowPi->phi();
	                matchGen_D1mass_[it] = bestGenSlowPi->mass();
	                matchGen_D1y_[it] = bestGenSlowPi->rapidity();
	                matchGen_D1charge_[it] = bestGenSlowPi->charge();
	                matchGen_D1pdgId_[it] = bestGenSlowPi->pdgId();
	                matchGen_slowPion_dR_[it] = bestSlowDr;

	                const int invalidInt = -1;
	                const reco::GenParticle* m1 = bestGenKaon ? dynamic_cast<const reco::GenParticle*>(bestGenKaon->mother()) : nullptr;
	                const reco::GenParticle* m2 = bestGenPion ? dynamic_cast<const reco::GenParticle*>(bestGenPion->mother()) : nullptr;
	                const reco::GenParticle* m3 = bestGenSlowPi ? dynamic_cast<const reco::GenParticle*>(bestGenSlowPi->mother()) : nullptr;
	                matchGen_D0Dau1_motherPdgId_[it] = m1 ? m1->pdgId() : invalidInt;
	                matchGen_D0Dau1_motherNDau_[it] = m1 ? static_cast<int>(m1->numberOfDaughters()) : invalidInt;
	                matchGen_D0Dau2_motherPdgId_[it] = m2 ? m2->pdgId() : invalidInt;
	                matchGen_D0Dau2_motherNDau_[it] = m2 ? static_cast<int>(m2->numberOfDaughters()) : invalidInt;
	                matchGen_D1_motherPdgId_[it] = m3 ? m3->pdgId() : invalidInt;
	                matchGen_D1_motherNDau_[it] = m3 ? static_cast<int>(m3->numberOfDaughters()) : invalidInt;

	                matchGen_D0pT_[it] = bestGenD0->pt();
	                matchGen_D0eta_[it] = bestGenD0->eta();
	                matchGen_D0phi_[it] = bestGenD0->phi();
	                matchGen_D0mass_[it] = bestGenD0->mass();
	                matchGen_D0y_[it] = bestGenD0->rapidity();
	                matchGen_D0charge_[it] = bestGenD0->charge();
	                matchGen_D0pdgId_[it] = bestGenD0->pdgId();
	                isSwap[it] = checkSwap(recoD0, *bestGenD0);

	                matchGen_DStarpT_[it] = bestGenDStar->pt();
	                matchGen_DStareta_[it] = bestGenDStar->eta();
	                matchGen_DStarphi_[it] = bestGenDStar->phi();
	                matchGen_DStarmass_[it] = bestGenDStar->mass();
	                matchGen_DStary_[it] = bestGenDStar->rapidity();
	                matchGen_DStarcharge_[it] = bestGenDStar->charge();
	                matchGen_DStarpdgId_[it] = bestGenDStar->pdgId();
	                genDecayLength(*bestGenDStar, matchGen_D1decayLength2D_[it], matchGen_D1decayLength3D_[it], matchGen_D1angle2D_[it], matchGen_D1angle3D_[it]);
	                getAncestorId(*bestGenDStar, matchGen_D1ancestorId_[it], matchGen_D1ancestorFlavor_[it]);

	                int momPdg = -77;
	                int bAncestor = -77;
	                const reco::GenParticle* mother = dynamic_cast<const reco::GenParticle*>(bestGenDStar->mother());
	                if (mother) {
	                  momPdg = mother->pdgId();
	                }
	                const reco::GenParticle* ancestor = mother;
	                int depth = 0;
	                while (ancestor && depth < 50) {
	                  ancestor = dynamic_cast<const reco::GenParticle*>(ancestor->mother());
	                  ++depth;
	                  if (ancestor && ((std::abs(ancestor->pdgId()) % 1000) / 100 == 5)) {
	                    bAncestor = ancestor->pdgId();
	                    break;
	                  }
	                }
	                idmom_reco[it] = momPdg;
	                idBAnc_reco[it] = bAncestor;

	                const reco::GenParticle* genDStarFromD0 = bestGenD0 ? findAncestor(bestGenD0, std::abs(PID_)) : nullptr;
	                bool validChain = false;
	                bool radiativeChain = false;
	                if (genDStarFromD0 && bestGenD0) {
	                  const bool dstarHas2 = genDStarFromD0->numberOfDaughters() == 2;
	                  const auto d0Summary = summarizeD0Daughters(bestGenD0);
	                  const bool d0Strict2 = (d0Summary.gammaCount == 0 && d0Summary.nonGammaCount == 2 && d0Summary.hasKaon && d0Summary.hasPion);
	                  const bool d0Radiative = (d0Summary.gammaCount >= 1 && d0Summary.nonGammaCount == 2 && d0Summary.hasKaon && d0Summary.hasPion);
	                  bool dstarPDGs = false;
	                  bool d0PDGsStrict = false;
	                  bool d0PDGsAny = false;
	                  bool directSlowPi = false;
	                  if (dstarHas2) {
	                    const auto* ds_d0 = genDStarFromD0->daughter(0);
	                    const auto* ds_pi = genDStarFromD0->daughter(1);
	                    const int a0 = std::abs(ds_d0->pdgId());
	                    const int a1 = std::abs(ds_pi->pdgId());
	                    dstarPDGs = ((a0 == D0_PDG_ID && a1 == PION_PDG_ID) || (a1 == D0_PDG_ID && a0 == PION_PDG_ID));
	                    dstarPDGs = dstarPDGs && (ds_d0 == bestGenD0 || ds_pi == bestGenD0);
	                    directSlowPi = ((ds_d0 == bestGenD0 && ds_pi == bestGenSlowPi) || (ds_pi == bestGenD0 && ds_d0 == bestGenSlowPi));
	                  }
		                  d0PDGsAny = matchD0WithFSR(recoD0DauA, recoD0DauB, bestGenD0, deltaR_);
	                  if (d0Strict2) {
	                    d0PDGsStrict = d0PDGsAny;
	                  }
	                  validChain = (dstarPDGs && d0PDGsStrict && directSlowPi);
	                  radiativeChain = (dstarPDGs && d0PDGsAny && directSlowPi && d0Radiative);
	                }

		                matchGen_validDstarChain_[it] = validChain;
		                matchGEN[it] = (allTracksMatched && (validChain || radiativeChain));
		                LogDebug("GenMatching") << "Candidate " << it
		                                        << " matchGen_validDstarChain=" << validChain
		                                        << " matchGEN=" << matchGEN[it];
		                if (debugGenMatching_) {
		                  LogInfo("GenMatchingFlow")
		                      << "[PostMatch] cand=" << it
		                      << " bestIgen=" << bestScoreIgen
		                      << " allTracksMatched=" << allTracksMatched
		                      << " validChain=" << validChain
		                      << " radiativeChain=" << radiativeChain
		                      << " finalMatchGEN=" << matchGEN[it]
		                      << " bestDStarPdg=" << (bestGenDStar ? bestGenDStar->pdgId() : -99999)
		                      << " bestD0Pdg=" << (bestGenD0 ? bestGenD0->pdgId() : -99999)
		                      << " bestSlowPiPdg=" << (bestGenSlowPi ? bestGenSlowPi->pdgId() : -99999)
		                      << " bestKaonPdg=" << (bestGenKaon ? bestGenKaon->pdgId() : -99999)
		                      << " bestPionPdg=" << (bestGenPion ? bestGenPion->pdgId() : -99999)
		                      << " bestScore=" << bestScore;
		                }
		              }
		            }

          }
          else {
              if(debugGenMatching_) {
                LogDebug("PATCompositeTreeProducer") << "Single-layer decay mode for candidate " << it;
                DEBUG_GEN("Single-layer decay mode for candidate " << it);
	              }
	              matchGEN[it] = false;
	              matchGen_validD0chain_[it] = false;
	              unsigned int nGen = genRefs.size();
	              if(debugGenMatching_) {
	                LogDebug("PATCompositeTreeProducer") << "Found " << nGen << " gen particles to match against";
	                DEBUG_GEN("Found " << nGen << " gen particles to match against");
	              }
              isSwap[it] = false;
              idmom_reco[it] = -77;
              idBAnc_reco[it] = -77;

              for( unsigned int igen=0; igen<nGen; igen++){
                auto const theGenP = genRefs.at(igen);
                
                // DEBUG: Calculate deltaR for gen matching verification
                bool hadronMatch = matchHadron(&trk, *theGenP,true);
                bool manualMatch = matchD0WithFSR(d1, d2, theGenP.get(), deltaR_);


		                matchGEN[it] = matchGEN[it] || hadronMatch || manualMatch;
	                if(matchGEN[it]){
	                  const auto d0Summary = summarizeD0Daughters(theGenP.get());
	                  matchGen_validD0chain_[it] = (d0Summary.gammaCount == 0 && d0Summary.nonGammaCount == 2 &&
	                                                d0Summary.hasKaon && d0Summary.hasPion);
	                  if(debugGenMatching_) {
	                    LogInfo("PATCompositeTreeProducer") << "D0 gen matching SUCCESS for candidate " << it << " with gen " << igen;
	                    DEBUG_GEN("D0 gen matching SUCCESS for candidate " << it << " with gen " << igen);
	                  }
                  isSwap[it] = checkSwap(&trk, *theGenP);
                  auto mom_ref = findMother(theGenP);
                  if (mom_ref.isNonnull()) idmom_reco[it] = mom_ref->pdgId();
                  int __count_anc__ = 0;
                  auto __ref_anc__ = mom_ref;
                  while ( __ref_anc__.isNonnull() && __count_anc__ < 50 ){
                    __ref_anc__ = findMother(__ref_anc__);
                    if( __ref_anc__.isNonnull()){
                      if( ((int) abs(__ref_anc__->pdgId())) % 1000 / 100 == 5){ 
                        idBAnc_reco[it] = __ref_anc__->pdgId();
                  } } }

                  matchGen_D0pT_[it] = theGenP->pt();
                  matchGen_D0eta_[it] = theGenP->eta();
                  matchGen_D0phi_[it] = theGenP->phi();
                  matchGen_D0mass_[it] = theGenP->mass();
                  matchGen_D0y_[it] = theGenP->rapidity();
                  matchGen_D0charge_[it] = theGenP->charge();
                  matchGen_D0pdgId_[it] = theGenP->pdgId();

                  genDecayLength(*theGenP, matchGen_D1decayLength2D_[it], matchGen_D1decayLength3D_[it], matchGen_D1angle2D_[it], matchGen_D1angle3D_[it] );
                  getAncestorId(*theGenP, matchGen_D1ancestorId_[it], matchGen_D1ancestorFlavor_[it] );

                  const reco::Candidate* genDau0 = nullptr;
                  const reco::Candidate* genDau1 = nullptr;
                  const auto matchedIdxs = findDaughterPermutation(*theGenP, false);
                  if (matchedIdxs.size() == 2) {
                    genDau0 = theGenP->daughter(matchedIdxs[0]);  // PID_dau1_ (usually K)
                    genDau1 = theGenP->daughter(matchedIdxs[1]);  // PID_dau2_ (usually pi)
                  } else {
                    // Fallback: pick first two non-gamma daughters to avoid FSR gamma assignment.
                    for (size_t idau = 0; idau < theGenP->numberOfDaughters(); ++idau) {
                      const auto* dau = theGenP->daughter(idau);
                      if (!dau || std::abs(dau->pdgId()) == 22) continue;
                      if (!genDau0) genDau0 = dau;
                      else if (!genDau1) {
                        genDau1 = dau;
                        break;
                      }
                    }
                  }
                  if (!genDau0 || !genDau1) {
                    edm::LogWarning("GenMatching") << "Single-layer matched candidate has insufficient non-gamma daughters";
                    break;
                  }

                  matchGen_D0Dau1_pT_[it] = genDau0->pt();
                  matchGen_D0Dau1_eta_[it] = genDau0->eta();
                  matchGen_D0Dau1_phi_[it] = genDau0->phi();
                  matchGen_D0Dau1_mass_[it] = genDau0->mass();
                  matchGen_D0Dau1_y_[it] = genDau0->rapidity();
                  matchGen_D0Dau1_charge_[it] = genDau0->charge();
                  matchGen_D0Dau1_pdgId_[it] = genDau0->pdgId();

                  matchGen_D0Dau2_pT_[it] = genDau1->pt();
                  matchGen_D0Dau2_eta_[it] = genDau1->eta();
                  matchGen_D0Dau2_phi_[it] = genDau1->phi();
                  matchGen_D0Dau2_mass_[it] = genDau1->mass();
                  matchGen_D0Dau2_y_[it] = genDau1->rapidity();
                  matchGen_D0Dau2_charge_[it] = genDau1->charge();
                  matchGen_D0Dau2_pdgId_[it] = genDau1->pdgId();
                  break;
                }
              } // END for nGen
            }
          if(debugGenMatching_) {
            LogDebug("PATCompositeTreeProducer") << "Gen matching completed for candidate " << it 
                        << " | Result: " << (matchGEN[it] ? "MATCHED" : "NOT_MATCHED");
            DEBUG_GEN("Gen matching completed for candidate " << it 
                    << " | Result: " << (matchGEN[it] ? "MATCHED" : "NOT_MATCHED"));
          }
          }
          
          double pxd1 = d1->px();
          double pyd1 = d1->py();
          double pzd1 = d1->pz();
          double pxd2 = d2->px();
          double pyd2 = d2->py();
          double pzd2 = d2->pz();
          
          TVector3 dauvec1(pxd1,pyd1,pzd1);
          TVector3 dauvec2(pxd2,pyd2,pzd2);
          if (d2) massD2[it] = d2->mass();
          
          pt1[it] = d1->pt();
          pt2[it] = d2->pt();
          
          p1[it] = d1->p();
          p2[it] = d2->p();
          
          eta1[it] = d1->eta();
          eta2[it] = d2->eta();
          
          phi1[it] = d1->phi();
          phi2[it] = d2->phi();
          
          charge1[it] = d1->charge();
          charge2[it] = d2->charge();
          
          pid1[it] = -99999;
          pid2[it] = -99999;
          pid3[it] = -99999;
          tof1[it] = INVALID_VALUE;
          tof2[it] = INVALID_VALUE;
          
          if(doGenMatchingTOF_)
          {
            for(unsigned igen=0; igen<genpars->size(); ++igen){

                const reco::GenParticle & trk = (*genpars)[igen];

                if(trk.pt()<0.001) continue;

                int id = trk.pdgId();
                TVector3 trkvect(trk.px(),trk.py(),trk.pz());

                if(fabs(id)!=PID_ && trk.charge())
                {
                  double deltaR = trkvect.DeltaR(dauvec1);
                  if(deltaR < deltaR_ && fabs((trk.pt()-pt1[it])/pt1[it]) < 0.5 && trk.charge()==charge1[it] && pid1[it]==-99999)
                  {
                    pid1[it] = id;
                  }

                  deltaR = trkvect.DeltaR(dauvec2);
                  if(deltaR < deltaR_ && fabs((trk.pt()-pt2[it])/pt2[it]) < 0.5 && trk.charge()==charge2[it] && pid2[it]==-99999)
                  {
                    pid2[it] = id;
                  }
                }

                if(fabs(id)==PID_ && trk.numberOfDaughters()==2)
                {
                  const reco::Candidate * Dd1 = trk.daughter(0);
                  const reco::Candidate * Dd2 = trk.daughter(1);
                  TVector3 d1vect(Dd1->px(),Dd1->py(),Dd1->pz());
                  TVector3 d2vect(Dd2->px(),Dd2->py(),Dd2->pz());
                  int id1 = Dd1->pdgId();
                  int id2 = Dd2->pdgId();

                  double deltaR = d1vect.DeltaR(dauvec1);
                  if(deltaR < deltaR_ && fabs((Dd1->pt()-pt1[it])/pt1[it]) < 0.5 && Dd1->charge()==charge1[it] && pid1[it]==-99999)
                  {
                    pid1[it] = id1;
                  }
                  deltaR = d2vect.DeltaR(dauvec1);
                  if(deltaR < deltaR_ && fabs((Dd2->pt()-pt1[it])/pt1[it]) < 0.5 && Dd2->charge()==charge1[it] && pid1[it]==-99999)
                  {
                    pid1[it] = id1;
                  }

                  deltaR = d1vect.DeltaR(dauvec2);
                  if(deltaR < deltaR_ && fabs((Dd1->pt()-pt2[it])/pt2[it]) < 0.5 && Dd1->charge()==charge2[it] && pid2[it]==-99999)
                  {
                    pid2[it] = id2;
                  }
                  deltaR = d2vect.DeltaR(dauvec2);
                  if(deltaR < deltaR_ && fabs((Dd2->pt()-pt2[it])/pt2[it]) < 0.5 && Dd2->charge()==charge2[it] && pid2[it]==-99999)
                  {
                    pid2[it] = id2;
                  }
                }

                if(pid1[it]!=-99999 && pid2[it]!=-99999) break;
            }

          }

          vtxChi2[it] = trk.userFloat("VtxChi2");
          ndf[it] = trk.userFloat("VtxNdof");
          VtxProb[it] = TMath::Prob(vtxChi2[it],ndf[it]);
          
          agl[it] = cos(trk.userFloat("alpha3D"));
          agl_abs[it] = trk.userFloat("alpha3D");
          agl2D[it] = cos(trk.userFloat("alpha2D"));
          agl2D_abs[it] = trk.userFloat("alpha2D");
          dl[it] = trk.userFloat("decaylength3D");
          dlos[it] = trk.userFloat("decaylengthsignif3D");
          
          dlerror[it] = dl[it]/dlos[it];
          dl2D[it] = trk.userFloat("decaylength2D");
          
          dlos2D[it] = trk.userFloat("decaylengthsignif2D");
          trk3Ddca[it] = twoLayerDecay_? ((CC*)d1)->userFloat("track3DDCA") : trk.userFloat("track3DDCA");
          trk3DdcaErr[it] = twoLayerDecay_? ((CC*)d1)->userFloat("track3DDCAErr") : trk.userFloat("track3DDCAErr");
          dca3D[it] = twoLayerDecay_? ((CC*)d1)->userFloat("dca3D") : trk.userFloat("dca3D");
          dca3DErr[it] = twoLayerDecay_? ((CC*)d1)->userFloat("dca3DErr") : trk.userFloat("dca3DErr");
          dca2D[it] = dl2D[it] * std::sin(agl2D_abs[it]);

          auto dau1 = d1->get<reco::TrackRef>();
          ptErr1[it] = INVALID_VALUE;
          if(!twoLayerDecay_)
          {
              trkquality1[it] = dau1->quality(reco::TrackBase::highPurity);
              
              H2dedx1[it] = -999.9;
              
              if(dEdxHandle1.isValid()){
                  const edm::ValueMap<reco::DeDxData> dEdxTrack = *dEdxHandle1.product();
                  H2dedx1[it] = dEdxTrack[dau1].dEdx();
              }
              
              T4dedx1[it] = -999.9;
              
              if(dEdxHandle2.isValid()){
                  const edm::ValueMap<reco::DeDxData> dEdxTrack = *dEdxHandle2.product();
                  T4dedx1[it] = dEdxTrack[dau1].dEdx();
              }
              
              trkChi1[it] = dau1->normalizedChi2();
              
              ptErr1[it] = dau1->ptError();
              
              secvz = trk.vz(); secvx = trk.vx(); secvy = trk.vy();
              
              nhit1[it] = dau1->numberOfValidHits();
              
              math::XYZPoint bestvtx(bestvx,bestvy,bestvz);
              
              double dzbest1 = dau1->dz(bestvtx);
              double dxybest1 = dau1->dxy(bestvtx);
              double dzerror1 = sqrt(dau1->dzError()*dau1->dzError()+bestvzError*bestvzError);
              double dxyerror1 = dau1->dxyError(bestvtx, vtx.covariance());
              
              dzos1[it] = dzbest1/dzerror1;
              dxyos1[it] = dxybest1/dxyerror1;
              dzval1[it] = dzbest1;
              dxyval1[it] = dxybest1;
          }
          
          auto dau2 = d2->get<reco::TrackRef>();
          
          trkquality2[it] = dau2->quality(reco::TrackBase::highPurity);
          
          H2dedx2[it] = -999.9;
          
          if(dEdxHandle1.isValid()){
              const edm::ValueMap<reco::DeDxData> dEdxTrack = *dEdxHandle1.product();
              H2dedx2[it] = dEdxTrack[dau2].dEdx();
          }
          
          T4dedx2[it] = -999.9;
          
          if(dEdxHandle2.isValid()){
              const edm::ValueMap<reco::DeDxData> dEdxTrack = *dEdxHandle2.product();
              T4dedx2[it] = dEdxTrack[dau2].dEdx();
          }
          
          trkChi2[it] = dau2->normalizedChi2();
          
          ptErr2[it] = dau2->ptError();
          
          secvz = trk.vz(); secvx = trk.vx(); secvy = trk.vy();
          
          nhit2[it] = dau2->numberOfValidHits();
          
          math::XYZPoint bestvtx(bestvx,bestvy,bestvz);
          
          double dzbest2 = dau2->dz(bestvtx);
          double dxybest2 = dau2->dxy(bestvtx);
          double dzerror2 = sqrt(dau2->dzError()*dau2->dzError()+bestvzError*bestvzError);
          double dxyerror2 = dau2->dxyError(bestvtx, vtx.covariance());
          
          dzos2[it] = dzbest2/dzerror2;
          dxyos2[it] = dxybest2/dxyerror2;
          dzval2[it] = dzbest2;
          dxyval2[it] = dxybest2;
          
          if(doMuon_)
          {
            edm::Handle<reco::MuonCollection> theMuonHandle;
            iEvent.getByToken(tok_muon_, theMuonHandle);
              
            nmatchedch1[it] = -1;
            nmatchedst1[it] = -1;
            matchedenergy1[it] = -1;
            nmatchedch2[it] = -1;
            nmatchedst2[it] = -1;
            matchedenergy2[it] = -1;
            dx1_seg_[it] = INVALID_VALUE;
            dy1_seg_[it] = INVALID_VALUE;
            dxSig1_seg_[it] = INVALID_VALUE;
            dySig1_seg_[it] = INVALID_VALUE;
            ddxdz1_seg_[it] = INVALID_VALUE;
            ddydz1_seg_[it] = INVALID_VALUE;
            ddxdzSig1_seg_[it] = INVALID_VALUE;
            ddydzSig1_seg_[it] = INVALID_VALUE;
            dx2_seg_[it] = INVALID_VALUE;
            dy2_seg_[it] = INVALID_VALUE;
            dxSig2_seg_[it] = INVALID_VALUE;
            dySig2_seg_[it] = INVALID_VALUE;
            ddxdz2_seg_[it] = INVALID_VALUE;
            ddydz2_seg_[it] = INVALID_VALUE;
            ddxdzSig2_seg_[it] = INVALID_VALUE;
            ddydzSig2_seg_[it] = INVALID_VALUE;
              
            double x_exp = -999.;
            double y_exp = -999.;
            double xerr_exp = -999.;
            double yerr_exp = -999.;
            double dxdz_exp = -999.;
            double dydz_exp = -999.;
            double dxdzerr_exp = -999.;
            double dydzerr_exp = -999.;
              
            double x_seg = -999.;
            double y_seg = -999.;
            double xerr_seg = -999.;
            double yerr_seg = -999.;
            double dxdz_seg = -999.;
            double dydz_seg = -999.;
            double dxdzerr_seg = -999.;
            double dydzerr_seg = -999.;
              
            double dx_seg = 999.;
            double dy_seg = 999.;
            double dxerr_seg = 999.;
            double dyerr_seg = 999.;
            double dxSig_seg = 999.;
            double dySig_seg = 999.;
            double ddxdz_seg = 999.;
            double ddydz_seg = 999.;
            double ddxdzerr_seg = 999.;
            double ddydzerr_seg = 999.;
            double ddxdzSig_seg = 999.;
            double ddydzSig_seg = 999.;
              
            onestmuon1[it] = false;
            pfmuon1[it] = false;
            glbmuon1[it] = false;
            trkmuon1[it] = false;
            calomuon1[it] = false; 
            softmuon1[it] = false;
            onestmuon2[it] = false;
            pfmuon2[it] = false;
            glbmuon2[it] = false;
            trkmuon2[it] = false;
            calomuon2[it] = false;
            softmuon2[it] = false;

            const int muId1 = muAssocToTrack( dau1, theMuonHandle );
            const int muId2 = muAssocToTrack( dau2, theMuonHandle );

            if( muId1 != -1 )
            {
              const reco::Muon& cand = (*theMuonHandle)[muId1];

              onestmuon1[it] = muon::isGoodMuon(cand, muon::selectionTypeFromString("TMOneStationTight"));
              pfmuon1[it] =  cand.isPFMuon();
              glbmuon1[it] =  cand.isGlobalMuon();
              trkmuon1[it] =  cand.isTrackerMuon();
              calomuon1[it] =  cand.isCaloMuon();

              if( 
                  trkmuon1[it] &&
                  cand.innerTrack()->hitPattern().trackerLayersWithMeasurement() > 5 && 
                  cand.innerTrack()->hitPattern().pixelLayersWithMeasurement() > 0 && 
                  fabs(cand.innerTrack()->dxy(vtx.position())) < 0.3 &&
                  fabs(cand.innerTrack()->dz(vtx.position())) < 20.
                ) softmuon1[it] = true;
            }

            if( muId2 != -1 )
            {
              const reco::Muon& cand = (*theMuonHandle)[muId2];

              onestmuon2[it] = muon::isGoodMuon(cand, muon::selectionTypeFromString("TMOneStationTight"));
              pfmuon2[it] =  cand.isPFMuon();
              glbmuon2[it] =  cand.isGlobalMuon();
              trkmuon2[it] =  cand.isTrackerMuon();
              calomuon2[it] =  cand.isCaloMuon();

              if(
                  trkmuon2[it] &&
                  cand.innerTrack()->hitPattern().trackerLayersWithMeasurement() > 5 &&
                  cand.innerTrack()->hitPattern().pixelLayersWithMeasurement() > 0 &&
                  fabs(cand.innerTrack()->dxy(vtx.position())) < 0.3 &&
                  fabs(cand.innerTrack()->dz(vtx.position())) < 20.
                ) softmuon2[it] = true;
            }

            if(doMuonFull_)
            {

            if( muId1 != -1 )
            {
              const reco::Muon& cand = (*theMuonHandle)[muId1];

              nmatchedch1[it] = cand.numberOfMatches();
              nmatchedst1[it] = cand.numberOfMatchedStations();
                    
              reco::MuonEnergy muenergy = cand.calEnergy();
              matchedenergy1[it] = muenergy.hadMax;
                      
              const std::vector<reco::MuonChamberMatch>& muchmatches = cand.matches();
                    
              for(unsigned int ich=0;ich<muchmatches.size();ich++)
              {
                x_exp = muchmatches[ich].x;
                y_exp = muchmatches[ich].y;
                xerr_exp = muchmatches[ich].xErr;
                yerr_exp = muchmatches[ich].yErr;
                dxdz_exp = muchmatches[ich].dXdZ;
                dydz_exp = muchmatches[ich].dYdZ;
                dxdzerr_exp = muchmatches[ich].dXdZErr;
                dydzerr_exp = muchmatches[ich].dYdZErr;
                          
                std::vector<reco::MuonSegmentMatch> musegmatches = muchmatches[ich].segmentMatches;
                          
                if(!musegmatches.size()) continue;
                for(unsigned int jseg=0;jseg<musegmatches.size();jseg++)
                {
                  x_seg = musegmatches[jseg].x;
                  y_seg = musegmatches[jseg].y;
                  xerr_seg = musegmatches[jseg].xErr;
                  yerr_seg = musegmatches[jseg].yErr;
                  dxdz_seg = musegmatches[jseg].dXdZ;
                  dydz_seg = musegmatches[jseg].dYdZ;
                  dxdzerr_seg = musegmatches[jseg].dXdZErr;
                  dydzerr_seg = musegmatches[jseg].dYdZErr;
                              
                  if(sqrt((x_seg-x_exp)*(x_seg-x_exp)+(y_seg-y_exp)*(y_seg-y_exp))<sqrt(dx_seg*dx_seg+dy_seg*dy_seg))
                  {
                    dx_seg = x_seg - x_exp;
                    dy_seg = y_seg - y_exp;
                    dxerr_seg = sqrt(xerr_seg*xerr_seg+xerr_exp*xerr_exp);
                    dyerr_seg = sqrt(yerr_seg*yerr_seg+yerr_exp*yerr_exp);
                    dxSig_seg = dx_seg / dxerr_seg;
                    dySig_seg = dy_seg / dyerr_seg;
                    ddxdz_seg = dxdz_seg - dxdz_exp;
                    ddydz_seg = dydz_seg - dydz_exp;
                    ddxdzerr_seg = sqrt(dxdzerr_seg*dxdzerr_seg+dxdzerr_exp*dxdzerr_exp);
                    ddydzerr_seg = sqrt(dydzerr_seg*dydzerr_seg+dydzerr_exp*dydzerr_exp);
                    ddxdzSig_seg = ddxdz_seg / ddxdzerr_seg;
                    ddydzSig_seg = ddydz_seg / ddydzerr_seg;
                  }
                }
                        
                dx1_seg_[it]=dx_seg;
                dy1_seg_[it]=dy_seg;
                dxSig1_seg_[it]=dxSig_seg;
                dySig1_seg_[it]=dySig_seg;
                ddxdz1_seg_[it]=ddxdz_seg;
                ddydz1_seg_[it]=ddydz_seg;
                ddxdzSig1_seg_[it]=ddxdzSig_seg;
                ddydzSig1_seg_[it]=ddydzSig_seg;
              }
            } 

            if( muId2 != -1 )
            {
              const reco::Muon& cand = (*theMuonHandle)[muId2];

              nmatchedch2[it] = cand.numberOfMatches();
              nmatchedst2[it] = cand.numberOfMatchedStations();
                      
              reco::MuonEnergy muenergy = cand.calEnergy();
              matchedenergy2[it] = muenergy.hadMax;
                      
              const std::vector<reco::MuonChamberMatch>& muchmatches = cand.matches();
              for(unsigned int ich=0;ich<muchmatches.size();ich++)
              {
                x_exp = muchmatches[ich].x;
                y_exp = muchmatches[ich].y;
                xerr_exp = muchmatches[ich].xErr;
                yerr_exp = muchmatches[ich].yErr;
                dxdz_exp = muchmatches[ich].dXdZ;
                dydz_exp = muchmatches[ich].dYdZ;
                dxdzerr_exp = muchmatches[ich].dXdZErr;
                dydzerr_exp = muchmatches[ich].dYdZErr;
                          
                std::vector<reco::MuonSegmentMatch> musegmatches = muchmatches[ich].segmentMatches;
                          
                if(!musegmatches.size()) continue;
                for(unsigned int jseg=0;jseg<musegmatches.size();jseg++)
                {
                  x_seg = musegmatches[jseg].x;
                  y_seg = musegmatches[jseg].y;
                  xerr_seg = musegmatches[jseg].xErr;
                  yerr_seg = musegmatches[jseg].yErr;
                  dxdz_seg = musegmatches[jseg].dXdZ;
                  dydz_seg = musegmatches[jseg].dYdZ;
                  dxdzerr_seg = musegmatches[jseg].dXdZErr;
                  dydzerr_seg = musegmatches[jseg].dYdZErr;
                              
                  if(sqrt((x_seg-x_exp)*(x_seg-x_exp)+(y_seg-y_exp)*(y_seg-y_exp))<sqrt(dx_seg*dx_seg+dy_seg*dy_seg))
                  {
                    dx_seg = x_seg - x_exp;
                    dy_seg = y_seg - y_exp;
                    dxerr_seg = sqrt(xerr_seg*xerr_seg+xerr_exp*xerr_exp);
                    dyerr_seg = sqrt(yerr_seg*yerr_seg+yerr_exp*yerr_exp);
                    dxSig_seg = dx_seg / dxerr_seg;
                    dySig_seg = dy_seg / dyerr_seg;
                    ddxdz_seg = dxdz_seg - dxdz_exp;
                    ddydz_seg = dydz_seg - dydz_exp;
                    ddxdzerr_seg = sqrt(dxdzerr_seg*dxdzerr_seg+dxdzerr_exp*dxdzerr_exp);
                    ddydzerr_seg = sqrt(dydzerr_seg*dydzerr_seg+dydzerr_exp*dydzerr_exp);
                    ddxdzSig_seg = ddxdz_seg / ddxdzerr_seg;
                    ddydzSig_seg = ddydz_seg / ddydzerr_seg;
                  }
                }
                          
                dx2_seg_[it]=dx_seg;
                dy2_seg_[it]=dy_seg;
                dxSig2_seg_[it]=dxSig_seg;
                dySig2_seg_[it]=dySig_seg;
                ddxdz2_seg_[it]=ddxdz_seg;
                ddydz2_seg_[it]=ddydz_seg;
                ddxdzSig2_seg_[it]=ddxdzSig_seg;
                ddydzSig2_seg_[it]=ddydzSig_seg;
              }
            }
            } // doMuonFull
          }
          
          if(twoLayerDecay_)
          {
              grand_mass[it] = d1->mass();
              double gpxd1 = gd1->px();
              double gpyd1 = gd1->py();
              double gpzd1 = gd1->pz();
              double gpxd2 = gd2->px();
              double gpyd2 = gd2->py();
              double gpzd2 = gd2->pz();
              
              TVector3 gdauvec1(gpxd1,gpyd1,gpzd1);
              TVector3 gdauvec2(gpxd2,gpyd2,gpzd2);
              
              auto gdau1 = gd1->get<reco::TrackRef>();
              auto gdau2 = gd2->get<reco::TrackRef>();
              grand_trkquality1[it] = gdau1->quality(reco::TrackBase::highPurity);
              grand_trkquality2[it] = gdau2->quality(reco::TrackBase::highPurity);
              
              grand_H2dedx1[it] = -999.9;
              grand_H2dedx2[it] = -999.9;
              
              if(dEdxHandle1.isValid()){
                  const edm::ValueMap<reco::DeDxData> dEdxTrack = *dEdxHandle1.product();
                  grand_H2dedx1[it] = dEdxTrack[gdau1].dEdx();
                  grand_H2dedx2[it] = dEdxTrack[gdau2].dEdx();
              }
              
              grand_T4dedx1[it] = -999.9;
              grand_T4dedx2[it] = -999.9;
              
              if(dEdxHandle2.isValid()){
                  const edm::ValueMap<reco::DeDxData> dEdxTrack = *dEdxHandle2.product();
                  grand_T4dedx1[it] = dEdxTrack[gdau1].dEdx();
                  grand_T4dedx2[it] = dEdxTrack[gdau2].dEdx();
              }
              
              grand_pt1[it] = gd1->pt();
              grand_pt2[it] = gd2->pt();
              grand_mass1[it] = gd1->mass();
              grand_mass2[it] = gd2->mass();
              
              grand_p1[it] = gd1->p();
              grand_p2[it] = gd2->p();
              
              grand_eta1[it] = gd1->eta();
              grand_eta2[it] = gd2->eta();
              grand_phi1[it] = gd1->phi();
              grand_phi2[it] = gd2->phi();
              
              grand_charge1[it] = gd1->charge();
              grand_charge2[it] = gd2->charge();
              
              grand_trkChi1[it] = gdau1->normalizedChi2();
              grand_trkChi2[it] = gdau2->normalizedChi2();
              
              grand_ptErr1[it] = gdau1->ptError();
              grand_ptErr2[it] = gdau2->ptError();
              
              CC* d1CC = (CC*) d1;
              if (d1CC && d1CC->hasUserFloat("d0FitVx") &&
                  d1CC->hasUserFloat("d0FitVy") &&
                  d1CC->hasUserFloat("d0FitVz")) {
                secvx = d1CC->userFloat("d0FitVx");
                secvy = d1CC->userFloat("d0FitVy");
                secvz = d1CC->userFloat("d0FitVz");
              } else {
                secvz = d1->vz(); secvx = d1->vx(); secvy = d1->vy();
              }
              
              grand_nhit1[it] = gdau1->numberOfValidHits();
              grand_nhit2[it] = gdau2->numberOfValidHits();
              
              math::XYZPoint bestvtx(bestvx,bestvy,bestvz);
              
              double gdzbest1 = gdau1->dz(bestvtx);
              double gdxybest1 = gdau1->dxy(bestvtx);
              double gdzerror1 = sqrt(gdau1->dzError()*gdau1->dzError()+bestvzError*bestvzError);
              double gdxyerror1 = gdau1->dxyError(bestvtx, vtx.covariance());
              
              grand_dzos1[it] = gdzbest1/gdzerror1;
              grand_dxyos1[it] = gdxybest1/gdxyerror1;
              
              double gdzbest2 = gdau2->dz(bestvtx);
              double gdxybest2 = gdau2->dxy(bestvtx);
              double gdzerror2 = sqrt(gdau2->dzError()*gdau2->dzError()+bestvzError*bestvzError);
              double gdxyerror2 = gdau2->dxyError(bestvtx, vtx.covariance());
              
              grand_dzos2[it] = gdzbest2/gdzerror2;
              grand_dxyos2[it] = gdxybest2/gdxyerror2;
              
              grand_vtxChi2[it] = ((CC*) d1)->userFloat("VtxChi2");
              grand_ndf[it] = ((CC*) d1)->userFloat("VtxNdof");
              grand_VtxProb[it] = TMath::Prob(grand_vtxChi2[it],grand_ndf[it]);
              
              TVector3 ptosvec(secvx-bestvx,secvy-bestvy,secvz-bestvz);
              TVector3 secvec(d1->px(),d1->py(),d1->pz());
              
              TVector3 ptosvec2D(secvx-bestvx,secvy-bestvy,0);
              TVector3 secvec2D(d1->px(),d1->py(),0);
              
              if (d1CC->hasUserFloat("alpha3D")) {
                grand_agl_abs[it] = d1CC->userFloat("alpha3D");
                grand_agl[it] = cos(grand_agl_abs[it]);
              } else {
                grand_agl[it] = cos(secvec.Angle(ptosvec));
                grand_agl_abs[it] = secvec.Angle(ptosvec);
              }

              if (d1CC->hasUserFloat("alpha2D")) {
                grand_agl2D_abs[it] = d1CC->userFloat("alpha2D");
                grand_agl2D[it] = cos(grand_agl2D_abs[it]);
              } else {
                grand_agl2D[it] = cos(secvec2D.Angle(ptosvec2D));
                grand_agl2D_abs[it] = secvec2D.Angle(ptosvec2D);
              }
              
              grand_dl[it] = d1CC->userFloat("decaylength3D");
              grand_dlos[it] = d1CC->userFloat("decaylengthsignif3D");
              grand_dlerror[it] = grand_dl[it]/grand_dlos[it];
              grand_dlos2D[it] = d1CC->userFloat("decaylengthsignif2D");
              grand_dl2D[it] = d1CC->userFloat("decaylength2D");

          }

          if(saveHistogram_)
          {
            for(unsigned int ipt=0;ipt<pTBins_.size()-1;ipt++)
              for(unsigned int iy=0;iy<yBins_.size()-1;iy++)
              {
                if(pt[it]<pTBins_[ipt+1] && pt[it]>pTBins_[ipt] && y[it]<yBins_[iy+1] && y[it]>yBins_[iy])
                {
                  hMassVsMVA[iy][ipt]->Fill(mva[it],mass[it]);

                  if(saveAllHistogram_)
                  {
                  hpTVsMVA[iy][ipt]->Fill(mva[it],pt[it]);
                  hetaVsMVA[iy][ipt]->Fill(mva[it],eta[it]);
                  hyVsMVA[iy][ipt]->Fill(mva[it],y[it]);
                  hVtxProbVsMVA[iy][ipt]->Fill(mva[it],VtxProb[it]);
                  h3DCosPointingAngleVsMVA[iy][ipt]->Fill(mva[it],agl[it]);
                  h3DPointingAngleVsMVA[iy][ipt]->Fill(mva[it],agl_abs[it]);
                  h2DCosPointingAngleVsMVA[iy][ipt]->Fill(mva[it],agl2D[it]);
                  h2DPointingAngleVsMVA[iy][ipt]->Fill(mva[it],agl2D_abs[it]);
                  h3DDecayLengthSignificanceVsMVA[iy][ipt]->Fill(mva[it],dlos[it]);
                  h3DDecayLengthVsMVA[iy][ipt]->Fill(mva[it],dl[it]);
                  h2DDecayLengthSignificanceVsMVA[iy][ipt]->Fill(mva[it],dlos2D[it]);
                  h2DDecayLengthVsMVA[iy][ipt]->Fill(mva[it],dl2D[it]);
                  hzDCASignificanceDaugther1VsMVA[iy][ipt]->Fill(mva[it],dzos1[it]);
                  hxyDCASignificanceDaugther1VsMVA[iy][ipt]->Fill(mva[it],dxyos1[it]);
                  hNHitD1VsMVA[iy][ipt]->Fill(mva[it],nhit1[it]);
                  hpTD1VsMVA[iy][ipt]->Fill(mva[it],pt1[it]);
                  hpTerrD1VsMVA[iy][ipt]->Fill(mva[it],ptErr1[it]/pt1[it]);
                  hEtaD1VsMVA[iy][ipt]->Fill(mva[it],eta1[it]);
                  hdedxHarmonic2D1VsMVA[iy][ipt]->Fill(mva[it],H2dedx1[it]);
                  hdedxHarmonic2D1VsP[iy][ipt]->Fill(p1[it],H2dedx1[it]);
                  hzDCASignificanceDaugther2VsMVA[iy][ipt]->Fill(mva[it],dzos2[it]);
                  hxyDCASignificanceDaugther2VsMVA[iy][ipt]->Fill(mva[it],dxyos2[it]);
                  hNHitD2VsMVA[iy][ipt]->Fill(mva[it],nhit2[it]);
                  hpTD2VsMVA[iy][ipt]->Fill(mva[it],pt2[it]);
                  hpTerrD2VsMVA[iy][ipt]->Fill(mva[it],ptErr2[it]/pt2[it]);
                  hEtaD2VsMVA[iy][ipt]->Fill(mva[it],eta2[it]);
                  hdedxHarmonic2D2VsMVA[iy][ipt]->Fill(mva[it],H2dedx2[it]);
                  hdedxHarmonic2D2VsP[iy][ipt]->Fill(p2[it],H2dedx2[it]);
                  }
                }
              }
          }

      }
  }

  std::vector<unsigned int> PATCompositeTreeProducer2Refactored::findDaughterPermutation(
    const reco::GenParticle& particle, 
    bool twoLayerDecay) {
    
    std::vector<unsigned int> idxs;
    
    // Collect non-gamma daughter indices
    std::vector<unsigned int> nonGammaDauIndices;
    for(size_t i = 0; i < particle.numberOfDaughters(); ++i) {
      if(std::abs(particle.daughter(i)->pdgId()) != 22) {  // Skip FSR gamma
        nonGammaDauIndices.push_back(i);
      }
    }
    
    if(static_cast<int>(nonGammaDauIndices.size()) != 2) return idxs;  // Wrong number of non-gamma daughters
    
    std::vector<unsigned int> permutations(2);
    std::iota(permutations.begin(), permutations.end(), 0);
    std::sort(permutations.begin(), permutations.end());
    
    do {
      auto Dd1 = particle.daughter(nonGammaDauIndices[permutations.at(0)]);
      auto Dd2 = particle.daughter(nonGammaDauIndices[permutations.at(1)]);
      if (abs(Dd1->pdgId()) == PID_dau1_ && abs(Dd2->pdgId()) == PID_dau2_) {
        if (twoLayerDecay) {
          // D* → D0 + π → K + π + π decay chain validation (FSR allowed)
          if(isValidDStarDecayChain(Dd1, Dd2)) {
            idxs = {nonGammaDauIndices[permutations.at(0)], nonGammaDauIndices[permutations.at(1)]};
            break;
          }
        } else {
          idxs = {nonGammaDauIndices[permutations.at(0)], nonGammaDauIndices[permutations.at(1)]};
          break;
        }
      }
    } while (std::next_permutation(permutations.begin(), permutations.end()));
    
    return idxs;
  }

  // D* → D0 + π decay chain validation (allows FSR gamma)
  // Returns true if valid D* → D0 + π → K + π + π structure (with optional FSR)
  bool PATCompositeTreeProducer2Refactored::isValidDStarDecayChain(const reco::Candidate* Dd1, const reco::Candidate* Dd2) const {
      LogDebug("DStarDecayFilter") << "=== D* Decay Chain Validation (FSR allowed) ===";
      
      if(!Dd1 || !Dd2) {
          return false;
      }
      
      // Identify which is D0 (allow FSR gamma as extra daughter at D* level)
      const reco::Candidate* D0 = nullptr;
      
      // D0 has PDG ID = ±421, π has PDG ID = ±211
      if(abs(Dd1->pdgId()) == D0_PDG_ID && abs(Dd2->pdgId()) == PION_PDG_ID) {
          D0 = Dd1;
      } else if(abs(Dd1->pdgId()) == PION_PDG_ID && abs(Dd2->pdgId()) == D0_PDG_ID) {
          D0 = Dd2;
      } else {
          // Not a valid D* → D0 + π structure
          return false;
      }
      
	      if(!D0) return false;
	      
	      const auto d0Summary = summarizeD0Daughters(D0);
	      if(d0Summary.nonGammaCount != 2 || !d0Summary.hasKaon || !d0Summary.hasPion) {
	          return false;
	      }
      
      LogDebug("DStarDecayFilter") << "ACCEPT: Valid D* → D0+π → K+π+π decay chain (FSR allowed)!";
      return true;
  }

  void
	  PATCompositeTreeProducer2Refactored::fillGEN(const edm::Event& iEvent, const edm::EventSetup& iSetup)
	  {
	      edm::Handle<GenEventInfoProduct> geninfo;
	      iEvent.getByToken(tok_genInfo_, geninfo);
	      gen_weight = (geninfo.isValid() ? geninfo->weight() : -1.0);
	      
	      edm::Handle<reco::GenParticleCollection> genpars;
	      iEvent.getByToken(tok_genParticle_,genpars);
	      std::vector<reco::GenParticleRef> genRefs;
	      std::vector<unsigned int> genRefIdxFillGEN;
	      const bool logAllGen = (debugGenMatching_ && verboseDebug_);
	      int nPidTarget = 0;
	      int nAccepted = 0;
	      int nRejectedNonGamma = 0;
	      int nRejectedTwoLayerNau = 0;
	      int nRejectedFirstTwo = 0;
	      int nRejectedPermutation = 0;
	      for(unsigned it=0; it<genpars->size(); ++it){

	          const reco::GenParticle & trk = (*genpars)[it];

	          int id = trk.pdgId();
	          if(fabs(id)!=PID_) {
	            if (logAllGen) {
	              LogInfo("GenRefFilter")
	                  << "[GEN][Decision] idx=" << it
	                  << " pdg=" << id
	                  << " nDau=" << trk.numberOfDaughters()
	                  << " decision=REJECT reason=pidMismatch";
	            }
	            continue; //check is target
	          }
	          ++nPidTarget;
	          
	          // Count non-gamma daughters to allow FSR
	          int nNonGammaDaughters = 0;
	          for(size_t i = 0; i < trk.numberOfDaughters(); ++i) {
	              if(std::abs(trk.daughter(i)->pdgId()) != 22) nNonGammaDaughters++;
	          }
	          
	          if(decayInGen_ && nNonGammaDaughters != 2) {
	            ++nRejectedNonGamma;
	            if (debugGenMatching_) {
	              LogInfo("GenRefFilter")
	                  << "[GEN][Decision] idx=" << it
	                  << " pdg=" << id
	                  << " nDau=" << trk.numberOfDaughters()
	                  << " nNonGamma=" << nNonGammaDaughters
	                  << " decision=REJECT reason=nonGammaCountNot2";
	            }
	            continue;
	          }
	          if(twoLayerDecay_ && decayInGen_ && trk.numberOfDaughters() != 2) {
	            ++nRejectedTwoLayerNau;
	            if (debugGenMatching_) {
	              LogInfo("GenRefFilter")
	                  << "[GEN][Decision] idx=" << it
	                  << " pdg=" << id
	                  << " nDau=" << trk.numberOfDaughters()
	                  << " decision=REJECT reason=twoLayerDecay_requires_exactly2_daughters";
	            }
	            continue;
	          }
	          if(twoLayerDecay_) {
	              if(trk.numberOfDaughters() < 2) {
	                ++nRejectedFirstTwo;
	                if (debugGenMatching_) {
	                  LogInfo("GenRefFilter")
	                      << "[GEN][Decision] idx=" << it
	                      << " pdg=" << id
	                      << " nDau=" << trk.numberOfDaughters()
	                      << " decision=REJECT reason=twoLayerDecay_daughtersLessThan2";
	                }
	                continue;
	              }
	              const reco::Candidate* d0 = trk.daughter(0);
	              const reco::Candidate* d1 = trk.daughter(1);
	              if(!d0 || !d1) {
	                ++nRejectedFirstTwo;
	                if (debugGenMatching_) {
	                  LogInfo("GenRefFilter")
	                      << "[GEN][Decision] idx=" << it
	                      << " pdg=" << id
	                      << " decision=REJECT reason=twoLayerDecay_nullDaughterInFirstTwo";
	                }
	                continue;
	              }
	              const bool d0InFirstTwo = (std::abs(d0->pdgId()) == D0_PDG_ID || std::abs(d1->pdgId()) == D0_PDG_ID);
	              if(!d0InFirstTwo) {
	                ++nRejectedFirstTwo;
	                if (debugGenMatching_) {
	                  LogInfo("GenRefFilter")
	                      << "[GEN][Decision] idx=" << it
	                      << " pdg=" << id
	                      << " d0FirstTwo=(" << d0->pdgId() << "," << d1->pdgId() << ")"
	                      << " decision=REJECT reason=twoLayerDecay_d0NotInFirstTwo";
	                }
	                continue;
	              }
	          }
	          
	          std::vector<unsigned int> idxs = findDaughterPermutation(trk, twoLayerDecay_);
	          if (decayInGen_ && idxs.empty()) {
	            ++nRejectedPermutation;
	            if (debugGenMatching_) {
	              LogInfo("GenRefFilter")
	                  << "[GEN][Decision] idx=" << it
	                  << " pdg=" << id
	                  << " nDau=" << trk.numberOfDaughters()
	                  << " decision=REJECT reason=daughterPermutationFailed";
	            }
	            continue;
	          }
	          genRefs.push_back(reco::GenParticleRef(genpars, it));
	          genRefIdxFillGEN.push_back(it);
	          ++nAccepted;
	          if (debugGenMatching_) {
	            LogInfo("GenRefFilter")
	                << "[GEN][Decision] idx=" << it
	                << " pdg=" << id
	                << " nDau=" << trk.numberOfDaughters()
	                << " nNonGamma=" << nNonGammaDaughters
	                << " idxsSize=" << idxs.size()
	                << " decision=ACCEPT";
	          }
	      }

	      if (debugGenMatching_) {
	        const auto recoStyleGenRefs = processGenMatching(genpars);
	        std::vector<unsigned int> genRefIdxRECOStyle;
	        genRefIdxRECOStyle.reserve(recoStyleGenRefs.size());
	        for (const auto& ref : recoStyleGenRefs) {
	          if (ref.isNonnull()) genRefIdxRECOStyle.push_back(ref.key());
	        }

	        std::sort(genRefIdxFillGEN.begin(), genRefIdxFillGEN.end());
	        std::sort(genRefIdxRECOStyle.begin(), genRefIdxRECOStyle.end());
	        const bool sameSet = (genRefIdxFillGEN == genRefIdxRECOStyle);
	        LogInfo("GenRefConsistency")
	            << "[EventCheck] run:lumi:event="
	            << iEvent.id().run() << ":" << iEvent.id().luminosityBlock() << ":" << iEvent.id().event()
	            << " fillGEN_nGenRefs=" << genRefIdxFillGEN.size()
	            << " recoStyle_nGenRefs=" << genRefIdxRECOStyle.size()
	            << " sameSet=" << sameSet;

	        if (!sameSet) {
	          std::string genList = "[";
	          for (size_t i = 0; i < genRefIdxFillGEN.size(); ++i) {
	            if (i) genList += ",";
	            genList += std::to_string(genRefIdxFillGEN[i]);
	          }
	          genList += "]";
	          std::string recoList = "[";
	          for (size_t i = 0; i < genRefIdxRECOStyle.size(); ++i) {
	            if (i) recoList += ",";
	            recoList += std::to_string(genRefIdxRECOStyle[i]);
	          }
	          recoList += "]";
	          LogWarning("GenRefConsistency")
	              << "[Mismatch] run:lumi:event="
	              << iEvent.id().run() << ":" << iEvent.id().luminosityBlock() << ":" << iEvent.id().event()
	              << " fillGEN=" << genList
	              << " recoStyle=" << recoList;
	        }

	        LogInfo("GenRefFilter")
	            << "[GEN][Summary] run:lumi:event="
	            << iEvent.id().run() << ":" << iEvent.id().luminosityBlock() << ":" << iEvent.id().event()
	            << " totalGen=" << genpars->size()
	            << " pidTarget=" << nPidTarget
	            << " accepted=" << nAccepted
	            << " rejNonGammaCount=" << nRejectedNonGamma
	            << " rejTwoLayerNau=" << nRejectedTwoLayerNau
	            << " rejFirstTwoDaughters=" << nRejectedFirstTwo
	            << " rejPermutation=" << nRejectedPermutation;
	      }
	      
	      unsigned int nGen = genRefs.size();
	      candSize_gen = nGen;
      if(twoLayerDecay_){
        for( unsigned int igen=0; igen<nGen; igen++){
          auto const theGenDStar = genRefs.at(igen);
	          if (debugGenMatching_) {
	            // Debug: Check daughter ordering
	            LogDebug("DStarDebug") << "D* candidate " << igen << ": daughter(0) pdgId = "
	                                   << theGenDStar->daughter(0)->pdgId()
	                                   << ", daughter(1) pdgId = " << theGenDStar->daughter(1)->pdgId();
	          }
          
	          if (theGenDStar->numberOfDaughters() < 2) {
	            edm::LogWarning("DStarDebug") << "D* has <2 daughters, skipping GEN " << igen;
	            continue;
	          }

	          unsigned int idxD0 = std::numeric_limits<unsigned int>::max();
	          if( fabs(theGenDStar->daughter(0)->pdgId()) == 421 ) {
		            idxD0 = 0;
		            if (debugGenMatching_) LogDebug("DStarDebug") << "D0 found at position 0";
		          } else if( fabs(theGenDStar->daughter(1)->pdgId()) == 421 ) {
		            idxD0 = 1;
		            if (debugGenMatching_) LogDebug("DStarDebug") << "D0 found at position 1";
		          } else {
		            edm::LogWarning("DStarDebug") << "D0 not found in either daughter position!";
		            continue;
		          }
	          auto const* theGenD0 = genRefs.at(igen)->daughter(idxD0);
	          auto const* theGenPion = genRefs.at(igen)->daughter(1- idxD0);
          // Intentionally-unfilled branches: write deterministic sentinels.
          iddau1[igen] = -1;
          iddau2[igen] = -1;
          gen_D0charge_[igen] = -99;
          gen_D1charge_[igen] = -99;
          gen_D0Dau1_mass_[igen] = INVALID_VALUE;
          gen_D0Dau2_mass_[igen] = INVALID_VALUE;
          gen_D0Dau1_charge_[igen] = -99;
          gen_D0Dau2_charge_[igen] = -99;
          gen_validDstarChain_[igen] = false;
          gen_validD0chain_[igen] = false;
          mass_gen[igen] = theGenDStar->mass();
          pt_gen[igen] = theGenDStar->pt();
          eta_gen[igen] = theGenDStar->eta(); 
          phi_gen[igen] = theGenDStar->phi();
          y_gen[igen] = theGenDStar->rapidity();
          status_gen[igen] = theGenDStar->status();
          pdgId_gen[igen] = theGenDStar->pdgId();
          charge_gen[igen] = theGenDStar->charge();
          idmom[igen] = -77;
          if(theGenDStar->numberOfMothers()!=0){
            const reco::Candidate * mom = theGenDStar->mother();
            idmom[igen] = mom->pdgId();
          }

          getAncestorId(*theGenDStar, gen_D0ancestorId_[igen], gen_D0ancestorFlavor_[igen] );

          gen_D0mass_[igen] = theGenD0->mass();
          gen_D0pT_[igen] = theGenD0->pt();
          gen_D0eta_[igen] = theGenD0->eta();
          gen_D0phi_[igen] = theGenD0->phi();
          gen_D0y_[igen] = theGenD0->rapidity();
          gen_D0pdgId_[igen] = theGenD0->pdgId();
          gen_D1mass_[igen] = theGenPion->mass(); 
          gen_D1pT_[igen] = theGenPion->pt();
          gen_D1eta_[igen] = theGenPion->eta();
          gen_D1phi_[igen] = theGenPion->phi();
          gen_D1y_[igen] = theGenPion->rapidity();
          gen_D1pdgId_[igen] = theGenPion->pdgId();
	          const auto d0Summary = summarizeD0Daughters(theGenD0);
	          gen_validD0chain_[igen] =
	              (d0Summary.gammaCount == 0 && d0Summary.nonGammaCount == 2 && d0Summary.hasKaon && d0Summary.hasPion);

          const auto* genDau0 = theGenD0->daughter(0);
          const auto* genDau1 = theGenD0->daughter(1);
	          if (debugGenMatching_) {
	            LogDebug("D0DaughterOrder") << "D0 candidate " << igen << " daughters: "
	                                        << "daughter(0) mass=" << genDau0->mass() << " PDG=" << genDau0->pdgId() << ", "
	                                        << "daughter(1) mass=" << genDau1->mass() << " PDG=" << genDau1->pdgId();
	          }
          gen_D0Dau1_pT_[igen] = genDau0->pt();
          gen_D0Dau1_eta_[igen] = genDau0->eta();
          gen_D0Dau1_phi_[igen] = genDau0->phi();
          gen_D0Dau1_y_[igen] = genDau0->rapidity();
          gen_D0Dau1_pdgId_[igen] = genDau0->pdgId();
          gen_D0Dau2_pT_[igen] = genDau1->pt();

          gen_D0Dau2_eta_[igen] = genDau1->eta();
          gen_D0Dau2_phi_[igen] = genDau1->phi();
          gen_D0Dau2_y_[igen] = genDau1->rapidity();
          gen_D0Dau2_pdgId_[igen] = genDau1->pdgId();

          // Strict D* chain flag at GEN level (D* has only D0+π non-gamma, D0 has only K+π non-gamma)
          int dstarNonGamma = 0;
          int dstarGamma = 0;
          int dstarD0Count = 0;
          int dstarPiCount = 0;
          for (size_t idau = 0; idau < theGenDStar->numberOfDaughters(); ++idau) {
            const auto* dau = theGenDStar->daughter(idau);
            if (!dau) continue;
            const int absId = std::abs(dau->pdgId());
            if (absId == 22) { ++dstarGamma; continue; }
            ++dstarNonGamma;
            if (absId == D0_PDG_ID) ++dstarD0Count;
            if (absId == PION_PDG_ID) ++dstarPiCount;
          }
          gen_validDstarChain_[igen] = (dstarNonGamma == 2 && dstarD0Count == 1 && dstarPiCount == 1 && gen_validD0chain_[igen]);
          LogDebug("GenMatching") << "GEN " << igen
                                  << " gen_validDstarChain=" << gen_validDstarChain_[igen]
                                  << " gen_validD0chain=" << gen_validD0chain_[igen]
                                  << " dstarGamma=" << dstarGamma
                                  << " dstarNonGamma=" << dstarNonGamma;
      }
    }
    else{
        for( unsigned int igen=0; igen<nGen; igen++){
            auto const theGenP = genRefs.at(igen);
             iddau1[igen] = -1;
             iddau2[igen] = -1;
             gen_validDstarChain_[igen] = false;
             gen_validD0chain_[igen] = false;
             pt_gen[igen] = theGenP->pt();
             eta_gen[igen] = theGenP->eta();
             phi_gen[igen] = theGenP->phi();
             mass_gen[igen] = theGenP->mass();
             y_gen[igen] = theGenP->rapidity();
             status_gen[igen] = theGenP->status();
             pdgId_gen[igen] = theGenP->pdgId();
             charge_gen[igen] = theGenP->charge();

             idmom[igen] = -77;
             if(theGenP->numberOfMothers()!=0){
               const reco::Candidate * mom = theGenP->mother();
               idmom[igen] = mom->pdgId();
             }

             getAncestorId(*theGenP, gen_D0ancestorId_[igen], gen_D0ancestorFlavor_[igen] );

             const auto* genDau0 = theGenP->daughter(0);
             const auto* genDau1 = theGenP->daughter(1);

             // Strict D0 flag (no extra non-gamma daughters; must be Kπ)
             int d0NonGamma = 0;
             int d0Gamma = 0;
             bool d0HasK = false;
             bool d0HasPi = false;
             for (size_t idau = 0; idau < theGenP->numberOfDaughters(); ++idau) {
               const auto* dau = theGenP->daughter(idau);
               if (!dau) continue;
               const int absId = std::abs(dau->pdgId());
               if (absId == 22) { ++d0Gamma; continue; }
               ++d0NonGamma;
               if (absId == KAON_PDG_ID) d0HasK = true;
               if (absId == PION_PDG_ID) d0HasPi = true;
             }
          gen_validD0chain_[igen] = (d0Gamma == 0 && d0NonGamma == 2 && d0HasK && d0HasPi);

             gen_D0Dau1_pT_[igen] = genDau0->pt();
             gen_D0Dau1_eta_[igen] = genDau0->eta();
             gen_D0Dau1_phi_[igen] = genDau0->phi();
             gen_D0Dau1_mass_[igen] = genDau0->mass();
             gen_D0Dau1_y_[igen] = genDau0->rapidity();
             gen_D0Dau1_charge_[igen] = genDau0->charge();
             gen_D0Dau1_pdgId_[igen] = genDau0->pdgId();

             gen_D0Dau2_pT_[igen] = genDau1->pt();
             gen_D0Dau2_eta_[igen] = genDau1->eta();
             gen_D0Dau2_phi_[igen] = genDau1->phi();
             gen_D0Dau2_mass_[igen] = genDau1->mass();
             gen_D0Dau2_y_[igen] = genDau1->rapidity();
             gen_D0Dau2_charge_[igen] = genDau1->charge();
             gen_D0Dau2_pdgId_[igen] = genDau1->pdgId();
                }
              } // END for nGen
            }
  void
  PATCompositeTreeProducer2Refactored::beginJob()
  {
      TH1D::SetDefaultSumw2();
      
      if(!doRecoNtuple_ && !doGenNtuple_)
      {
          cout<<"No output for either RECO or GEN!! Fix config!!"<<endl; return;
      }

      if(twoLayerDecay_ && doMuon_)
      {
          cout<<"Muons cannot be coming from two layer decay!! Fix config!!"<<endl; return;
      }
      
      if(saveHistogram_) initHistogram();
      if(saveTree_) initTree();
  }

  void
  PATCompositeTreeProducer2Refactored::initHistogram()
  {
    for(unsigned int ipt=0;ipt<pTBins_.size()-1;ipt++)
    {
      for(unsigned int iy=0;iy<yBins_.size()-1;iy++)
    {
    hMassVsMVA[iy][ipt] = fs->make<TH2F>(Form("hMassVsMVA_y%d_pt%d",iy,ipt),";mva;mass(GeV)",100,-1.,1.,massHistBins_,massHistPeak_-massHistWidth_,massHistPeak_+massHistWidth_);

    if(saveAllHistogram_)
    {
    hpTVsMVA[iy][ipt] = fs->make<TH2F>(Form("hpTVsMVA_y%d_pt%d",iy,ipt),";mva;pT;",100,-1,1,100,0,10);
    hetaVsMVA[iy][ipt] = fs->make<TH2F>(Form("hetaVsMVA_y%d_pt%d",iy,ipt),";mva;eta;",100,-1.,1.,40,-4,4);
    hyVsMVA[iy][ipt] = fs->make<TH2F>(Form("hyVsMVA_y%d_pt%d",iy,ipt),";mva;y;",100,-1.,1.,40,-4,4);
    hVtxProbVsMVA[iy][ipt] = fs->make<TH2F>(Form("hVtxProbVsMVA_y%d_pt%d",iy,ipt),";mva;VtxProb;",100,-1.,1.,100,0,1);
    h3DCosPointingAngleVsMVA[iy][ipt] = fs->make<TH2F>(Form("h3DCosPointingAngleVsMVA_y%d_pt%d",iy,ipt),";mva;3DCosPointingAngle;",100,-1.,1.,100,-1,1);
    h3DPointingAngleVsMVA[iy][ipt] = fs->make<TH2F>(Form("h3DPointingAngleVsMVA_y%d_pt%d",iy,ipt),";mva;3DPointingAngle;",100,-1.,1.,50,-3.14,3.14);
    h2DCosPointingAngleVsMVA[iy][ipt] = fs->make<TH2F>(Form("h2DCosPointingAngleVsMVA_y%d_pt%d",iy,ipt),";mva;2DCosPointingAngle;",100,-1.,1.,100,-1,1);
    h2DPointingAngleVsMVA[iy][ipt] = fs->make<TH2F>(Form("h2DPointingAngleVsMVA_y%d_pt%d",iy,ipt),";mva;2DPointingAngle;",100,-1.,1.,50,-3.14,3.14);
    h3DDecayLengthSignificanceVsMVA[iy][ipt] = fs->make<TH2F>(Form("h3DDecayLengthSignificanceVsMVA_y%d_pt%d",iy,ipt),";mva;3DDecayLengthSignificance;",100,-1.,1.,300,0,30);
    h2DDecayLengthSignificanceVsMVA[iy][ipt] = fs->make<TH2F>(Form("h2DDecayLengthSignificanceVsMVA_y%d_pt%d",iy,ipt),";mva;2DDecayLengthSignificance;",100,-1.,1.,300,0,30);
    h3DDecayLengthVsMVA[iy][ipt] = fs->make<TH2F>(Form("h3DDecayLengthVsMVA_y%d_pt%d",iy,ipt),";mva;3DDecayLength;",100,-1.,1.,300,0,30);
    h2DDecayLengthVsMVA[iy][ipt] = fs->make<TH2F>(Form("h2DDecayLengthVsMVA_y%d_pt%d",iy,ipt),";mva;2DDecayLength;",100,-1.,1.,300,0,30);
    hzDCASignificanceDaugther1VsMVA[iy][ipt] = fs->make<TH2F>(Form("hzDCASignificanceDaugther1VsMVA_y%d_pt%d",iy,ipt),";mva;zDCASignificanceDaugther1;",100,-1.,1.,100,-10,10);
    hxyDCASignificanceDaugther1VsMVA[iy][ipt] = fs->make<TH2F>(Form("hxyDCASignificanceDaugther1VsMVA_y%d_pt%d",iy,ipt),";mva;xyDCASignificanceDaugther1;",100,-1.,1.,100,-10,10);
    hNHitD1VsMVA[iy][ipt] = fs->make<TH2F>(Form("hNHitD1VsMVA_y%d_pt%d",iy,ipt),";mva;NHitD1;",100,-1.,1.,100,0,100);
    hpTD1VsMVA[iy][ipt] = fs->make<TH2F>(Form("hpTD1VsMVA_y%d_pt%d",iy,ipt),";mva;pTD1;",100,-1.,1.,100,0,10);
    hpTerrD1VsMVA[iy][ipt] = fs->make<TH2F>(Form("hpTerrD1VsMVA_y%d_pt%d",iy,ipt),";mva;pTerrD1;",100,-1.,1.,50,0,0.5);
    hEtaD1VsMVA[iy][ipt] = fs->make<TH2F>(Form("hEtaD1VsMVA_y%d_pt%d",iy,ipt),";mva;EtaD1;",100,-1.,1.,40,-4,4);
    hdedxHarmonic2D1VsMVA[iy][ipt] = fs->make<TH2F>(Form("hdedxHarmonic2D1VsMVA_y%d_pt%d",iy,ipt),";mva;dedxHarmonic2D1;",100,-1.,1.,100,0,10);
    hdedxHarmonic2D1VsP[iy][ipt] = fs->make<TH2F>(Form("hdedxHarmonic2D1VsP_y%d_pt%d",iy,ipt),";p (GeV);dedxHarmonic2D1",100,0,10,100,0,10);
    hzDCASignificanceDaugther2VsMVA[iy][ipt] = fs->make<TH2F>(Form("hzDCASignificanceDaugther2VsMVA_y%d_pt%d",iy,ipt),";mva;zDCASignificanceDaugther2;",100,-1.,1.,100,-10,10);
    hxyDCASignificanceDaugther2VsMVA[iy][ipt] = fs->make<TH2F>(Form("hxyDCASignificanceDaugther2VsMVA_y%d_pt%d",iy,ipt),";mva;xyDCASignificanceDaugther2;",100,-1.,1.,100,-10,10);
    hNHitD2VsMVA[iy][ipt] = fs->make<TH2F>(Form("hNHitD2VsMVA_y%d_pt%d",iy,ipt),";mva;NHitD2;",100,-1.,1.,100,0,100);
    hpTD2VsMVA[iy][ipt] = fs->make<TH2F>(Form("hpTD2VsMVA_y%d_pt%d",iy,ipt),";mva;pTD2;",100,-1.,1.,100,0,10);
    hpTerrD2VsMVA[iy][ipt] = fs->make<TH2F>(Form("hpTerrD2VsMVA_y%d_pt%d",iy,ipt),";mva;pTerrD2;",100,-1.,1.,50,0,0.5);
    hEtaD2VsMVA[iy][ipt] = fs->make<TH2F>(Form("hEtaD2VsMVA_y%d_pt%d",iy,ipt),";mva;EtaD2;",100,-1.,1.,40,-4,4);
    hdedxHarmonic2D2VsMVA[iy][ipt] = fs->make<TH2F>(Form("hdedxHarmonic2D2VsMVA_y%d_pt%d",iy,ipt),";mva;dedxHarmonic2D2;",100,-1.,1.,100,0,10);
    hdedxHarmonic2D2VsP[iy][ipt] = fs->make<TH2F>(Form("hdedxHarmonic2D2VsP_y%d_pt%d",iy,ipt),";p (GeV);dedxHarmonic2D2",100,0,10,100,0,10);

    }
    }
  }
  }

  void 
  PATCompositeTreeProducer2Refactored::initTree()
  { 
      PATCompositeNtuple = fs->make< TTree>("PATCompositeNtuple","PATCompositeNtuple");
      
      if(doRecoNtuple_) 
      { 
    
      PATCompositeNtuple->Branch("Ntrkoffline",&Ntrkoffline,"Ntrkoffline/I");
      PATCompositeNtuple->Branch("Npixel",&Npixel,"Npixel/I");
      PATCompositeNtuple->Branch("HFsumETPlus",&HFsumETPlus,"HFsumETPlus/F");
      PATCompositeNtuple->Branch("HFsumETMinus",&HFsumETMinus,"HFsumETMinus/F");
      PATCompositeNtuple->Branch("ZDCPlus",&ZDCPlus,"ZDCPlus/F");
      PATCompositeNtuple->Branch("ZDCMinus",&ZDCMinus,"ZDCMinus/F");
      PATCompositeNtuple->Branch("bestvtxX",&bestvx,"bestvtxX/F");
      PATCompositeNtuple->Branch("bestvtxY",&bestvy,"bestvtxY/F");
      PATCompositeNtuple->Branch("bestvtxZ",&bestvz,"bestvtxZ/F");
      PATCompositeNtuple->Branch("candSize",&candSize,"candSize/I");
      if(isCentrality_) PATCompositeNtuple->Branch("centrality",&centrality,"centrality/I");
      if(isEventPlane_) 
      {
        PATCompositeNtuple->Branch("ephfpAngle",&ephfpAngle,"ephfpAngle[3]/F");
        PATCompositeNtuple->Branch("ephfmAngle",&ephfmAngle,"ephfmAngle[3]/F");
        PATCompositeNtuple->Branch("ephfpQ",&ephfpQ,"ephfpQ[3]/F");
        PATCompositeNtuple->Branch("ephfmQ",&ephfmQ,"ephfmQ[3]/F");
        PATCompositeNtuple->Branch("ephfpSumW",&ephfpSumW,"ephfpSumW/F");
        PATCompositeNtuple->Branch("ephfmSumW",&ephfmSumW,"ephfmSumW/F");
        PATCompositeNtuple->Branch("ephfmAngleoff",&ephfmAngleoff,"ephfmAngleoff[2]/F");
        PATCompositeNtuple->Branch("ephfpAngleoff",&ephfpAngleoff,"ephfpAngleoff[2]/F");
        PATCompositeNtuple->Branch("ephfQ",&ephfQ,"ephfQ[2]/F");
        PATCompositeNtuple->Branch("ephfSumW",&ephfSumW,"ephfSumW/F");

        // Raw HF +/- (v2,v3)
        PATCompositeNtuple->Branch("ephfmAngleRaw", &ephfmAngleRaw, "ephfmAngleRaw[2]/F");
        PATCompositeNtuple->Branch("ephfmsumCosRaw", &ephfmsumCosRaw, "ephfmsumCosRaw[2]/F");
        PATCompositeNtuple->Branch("ephfmsumSinRaw", &ephfmsumSinRaw, "ephfmsumSinRaw[2]/F");
        PATCompositeNtuple->Branch("ephfmsumPtOrEt", &ephfmsumPtOrEt, "ephfmsumPtOrEt[2]/F");

        PATCompositeNtuple->Branch("ephfpAngleRaw", &ephfpAngleRaw, "ephfpAngleRaw[2]/F");
        PATCompositeNtuple->Branch("ephfpsumCosRaw", &ephfpsumCosRaw, "ephfpsumCosRaw[2]/F");
        PATCompositeNtuple->Branch("ephfpsumSinRaw", &ephfpsumSinRaw, "ephfpsumSinRaw[2]/F");
        PATCompositeNtuple->Branch("ephfpsumPtOrEt", &ephfpsumPtOrEt, "ephfpsumPtOrEt[2]/F");

        // Track-mid (v2,v3)
        PATCompositeNtuple->Branch("eptrackmidAngle", &eptrackmidAngle, "eptrackmidAngle[2]/F");
        PATCompositeNtuple->Branch("eptrackmidQ", &eptrackmidQ, "eptrackmidQ[2]/F");
        PATCompositeNtuple->Branch("eptrackmidSumW", &eptrackmidSumW, "eptrackmidSumW/F");
        PATCompositeNtuple->Branch("eptrackpAngle", &eptrackpAngle, "eptrackpAngle[2]/F");
        PATCompositeNtuple->Branch("eptrackpQ", &eptrackpQ, "eptrackpQ[2]/F");
        PATCompositeNtuple->Branch("eptrackpSumW", &eptrackpSumW, "eptrackpSumW[2]/F");
        PATCompositeNtuple->Branch("eptrackmAngle", &eptrackmAngle, "eptrackmAngle[2]/F");
        PATCompositeNtuple->Branch("eptrackmQ", &eptrackmQ, "eptrackmQ[2]/F");
        PATCompositeNtuple->Branch("eptrackmSumW", &eptrackmSumW, "eptrackmSumW[2]/F");
        // Full HF (v2,v3): flat(level2), offset(level1), raw(level0)
        PATCompositeNtuple->Branch("ephfAngle", &ephfAngle, "ephfAngle[2]/F");
        PATCompositeNtuple->Branch("ephfAngleoff", &ephfAngleoff, "ephfAngleoff[2]/F");
        PATCompositeNtuple->Branch("ephfAngleRaw", &ephfAngleRaw, "ephfAngleRaw[2]/F");
        PATCompositeNtuple->Branch("ephfsumCos", &ephfsumCos, "ephfsumCos[2]/F");
        PATCompositeNtuple->Branch("ephfsumSin", &ephfsumSin, "ephfsumSin[2]/F");
        PATCompositeNtuple->Branch("ephfsumCosRaw", &ephfsumCosRaw, "ephfsumCosRaw[2]/F");
        PATCompositeNtuple->Branch("ephfsumSinRaw", &ephfsumSinRaw, "ephfsumSinRaw[2]/F");
        PATCompositeNtuple->Branch("ephfsumPtOrEt", &ephfsumPtOrEt, "ephfsumPtOrEt[2]/F");
      }

      PATCompositeNtuple->Branch("pT",&pt,"pT[candSize]/F");
      PATCompositeNtuple->Branch("y",&y,"y[candSize]/F");
      PATCompositeNtuple->Branch("eta",&eta,"eta[candSize]/F");
      PATCompositeNtuple->Branch("phi",&phi,"phi[candSize]/F");
      PATCompositeNtuple->Branch("mass",&mass,"mass[candSize]/F");
      PATCompositeNtuple->Branch("mva",&mva,"mva[candSize]/F");

      if(!isSkimMVA_)  
      {
          PATCompositeNtuple->Branch("flavor",&flavor,"flavor[candSize]/F");
          PATCompositeNtuple->Branch("VtxProb",&VtxProb,"VtxProb[candSize]/F");
          PATCompositeNtuple->Branch("VtxChi2",&vtxChi2,"VtxChi2[candSize]/F");
          PATCompositeNtuple->Branch("VtxNDF",&ndf,"VtxNDF[candSize]/F");
          PATCompositeNtuple->Branch("3DCosPointingAngle",&agl,"3DCosPointingAngle[candSize]/F");
          PATCompositeNtuple->Branch("3DPointingAngle",&agl_abs,"3DPointingAngle[candSize]/F");
          PATCompositeNtuple->Branch("2DCosPointingAngle",&agl2D,"2DCosPointingAngle[candSize]/F");
          PATCompositeNtuple->Branch("2DPointingAngle",&agl2D_abs,"2DPointingAngle[candSize]/F");
          PATCompositeNtuple->Branch("3DDecayLengthSignificance",&dlos,"3DDecayLengthSignificance[candSize]/F");
          PATCompositeNtuple->Branch("3DDecayLength",&dl,"3DDecayLength[candSize]/F");
          PATCompositeNtuple->Branch("2DDecayLengthSignificance",&dlos2D,"2DDecayLengthSignificance[candSize]/F");
          PATCompositeNtuple->Branch("2DDecayLength",&dl2D,"2DDecayLength[candSize]/F");
          PATCompositeNtuple->Branch("Trk3DDCA",&trk3Ddca,"Trk3DDCA[candSize]/F");
          PATCompositeNtuple->Branch("Trk3DDCAErr",&trk3DdcaErr,"Trk3DDCAErr[candSize]/F");
          PATCompositeNtuple->Branch("dca3D",&dca3D,"dca3D[candSize]/F");
          PATCompositeNtuple->Branch("dca3DErr",&dca3DErr,"dca3DErr[candSize]/F");
          PATCompositeNtuple->Branch("dca2D",&dca2D,"dca2D[candSize]/F");
      
          if(doGenMatching_)
          {
              PATCompositeNtuple->Branch("isSwap",&isSwap,"isSwap[candSize]/O");
              PATCompositeNtuple->Branch("idmom_reco",&idmom_reco,"idmom_reco[candSize]/I");
              PATCompositeNtuple->Branch("idBAnc_reco",&idBAnc_reco,"idBAnc_reco[candSize]/I");
              PATCompositeNtuple->Branch("matchGEN",&matchGEN,"matchGEN[candSize]/O");
              PATCompositeNtuple->Branch("matchGen3DPointingAngle",&gen_agl_abs,"gen3DPointingAngle[candSize]/F");
              PATCompositeNtuple->Branch("matchGen2DPointingAngle",&gen_agl2D_abs,"gen2DPointingAngle[candSize]/F");
              PATCompositeNtuple->Branch("matchGen3DDecayLength",&gen_dl,"gen3DDecayLength[candSize]/F");
              PATCompositeNtuple->Branch("matchGen2DDecayLength",&gen_dl2D,"gen2DDecayLength[candSize]/F");
              if(twoLayerDecay_){
                PATCompositeNtuple->Branch("matchGen_DStarpT",&matchGen_DStarpT_, "matchGen_DStarpT[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_DStareta",&matchGen_DStareta_, "matchGen_DStareta[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_DStarphi",&matchGen_DStarphi_, "matchGen_DStarphi[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_DStarmass",&matchGen_DStarmass_, "matchGen_DStarmass[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_DStary",&matchGen_DStary_, "matchGen_DStary[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_DStarcharge",&matchGen_DStarcharge_, "matchGen_DStarcharge[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_DStarpdgId",&matchGen_DStarpdgId_, "matchGen_DStarpdgId[candSize]/I");

                PATCompositeNtuple->Branch("matchGen_D0pT",&matchGen_D0pT_, "matchGen_D0pT[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0eta",&matchGen_D0eta_, "matchGen_D0eta[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0phi",&matchGen_D0phi_, "matchGen_D0phi[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0mass",&matchGen_D0mass_, "matchGen_D0mass[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0y",&matchGen_D0y_, "matchGen_D0y[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0charge",&matchGen_D0charge_, "matchGen_D0charge[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0pdgId",&matchGen_D0pdgId_, "matchGen_D0pdgId[candSize]/I");

                PATCompositeNtuple->Branch("matchGen_D0Dau1_pT",&matchGen_D0Dau1_pT_, "matchGen_D0Dau1_pT[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau1_eta",&matchGen_D0Dau1_eta_, "matchGen_D0Dau1_eta[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau1_phi",&matchGen_D0Dau1_phi_, "matchGen_D0Dau1_phi[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau1_mass",&matchGen_D0Dau1_mass_, "matchGen_D0Dau1_mass[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau1_y",&matchGen_D0Dau1_y_, "matchGen_D0Dau1_y[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau1_charge",&matchGen_D0Dau1_charge_, "matchGen_D0Dau1_charge[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau1_pdgId",&matchGen_D0Dau1_pdgId_, "matchGen_D0Dau1_pdgId[candSize]/I");

                PATCompositeNtuple->Branch("matchGen_D0Dau2_pT",&matchGen_D0Dau2_pT_, "matchGen_D0Dau2_pT[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau2_eta",&matchGen_D0Dau2_eta_, "matchGen_D0Dau2_eta[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau2_phi",&matchGen_D0Dau2_phi_, "matchGen_D0Dau2_phi[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau2_mass",&matchGen_D0Dau2_mass_, "matchGen_D0Dau2_mass[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau2_y",&matchGen_D0Dau2_y_, "matchGen_D0Dau2_y[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau2_charge",&matchGen_D0Dau2_charge_, "matchGen_D0Dau2_charge[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau2_pdgId",&matchGen_D0Dau2_pdgId_, "matchGen_D0Dau2_pdgId[candSize]/I");

                PATCompositeNtuple->Branch("matchGen_D1pT",&matchGen_D1pT_, "matchGen_D1pT[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D1eta",&matchGen_D1eta_, "matchGen_D1eta[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D1phi",&matchGen_D1phi_, "matchGen_D1phi[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D1mass",&matchGen_D1mass_, "matchGen_D1mass[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D1y",&matchGen_D1y_, "matchGen_D1y[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D1charge",&matchGen_D1charge_, "matchGen_D1charge[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D1pdgId",&matchGen_D1pdgId_, "matchGen_D1pdgId[candSize]/I");
                PATCompositeNtuple->Branch("matchGen_slowPion_dR",&matchGen_slowPion_dR_, "matchGen_slowPion_dR[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D1decayLength2D_",&matchGen_D1decayLength2D_, "matchGen_D1decayLength2D_[candSize]/I");
                PATCompositeNtuple->Branch("matchGen_D1decayLength3D_",&matchGen_D1decayLength3D_, "matchGen_D1decayLength3D_[candSize]/I");
                PATCompositeNtuple->Branch("matchGen_D1angle2D_",&matchGen_D1angle2D_, "matchGen_D1angle2D_[candSize]/I");
                PATCompositeNtuple->Branch("matchGen_D1angle3D_",&matchGen_D1angle3D_, "matchGen_D1angle3D_[candSize]/I");
                PATCompositeNtuple->Branch("matchGen_D1ancestorId_",&matchGen_D1ancestorId_, "matchGen_D1ancestorId_[candSize]/I");
                PATCompositeNtuple->Branch("matchGen_D1ancestorFlavor_",&matchGen_D1ancestorFlavor_, "matchGen_D1ancestorFlavor_[candSize]/I");

                // New provenance branches for background diagnosis
                PATCompositeNtuple->Branch("matchGen_D0Dau1_motherPdgId",&matchGen_D0Dau1_motherPdgId_, "matchGen_D0Dau1_motherPdgId[candSize]/I");
                PATCompositeNtuple->Branch("matchGen_D0Dau1_motherNDau",&matchGen_D0Dau1_motherNDau_, "matchGen_D0Dau1_motherNDau[candSize]/I");
                PATCompositeNtuple->Branch("matchGen_D0Dau2_motherPdgId",&matchGen_D0Dau2_motherPdgId_, "matchGen_D0Dau2_motherPdgId[candSize]/I");
                PATCompositeNtuple->Branch("matchGen_D0Dau2_motherNDau",&matchGen_D0Dau2_motherNDau_, "matchGen_D0Dau2_motherNDau[candSize]/I");
                PATCompositeNtuple->Branch("matchGen_D1_motherPdgId",&matchGen_D1_motherPdgId_, "matchGen_D1_motherPdgId[candSize]/I");
                PATCompositeNtuple->Branch("matchGen_D1_motherNDau",&matchGen_D1_motherNDau_, "matchGen_D1_motherNDau[candSize]/I");
                PATCompositeNtuple->Branch("matchGen_validDstarChain",&matchGen_validDstarChain_, "matchGen_validDstarChain[candSize]/O");
              }
              else{
                PATCompositeNtuple->Branch("matchGen_D0pT",&matchGen_D0pT_, "matchGen_D0pT[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0eta",&matchGen_D0eta_, "matchGen_D0eta[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0phi",&matchGen_D0phi_, "matchGen_D0phi[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0mass",&matchGen_D0mass_, "matchGen_D0mass[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0y",&matchGen_D0y_, "matchGen_D0y[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0charge",&matchGen_D0charge_, "matchGen_D0charge[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0pdgId",&matchGen_D0pdgId_, "matchGen_D0pdgId[candSize]/I");

                PATCompositeNtuple->Branch("matchGen_D0Dau1_pT",&matchGen_D0Dau1_pT_, "matchGen_D0Dau1_pT[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau1_eta",&matchGen_D0Dau1_eta_, "matchGen_D0Dau1_eta[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau1_phi",&matchGen_D0Dau1_phi_, "matchGen_D0Dau1_phi[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau1_mass",&matchGen_D0Dau1_mass_, "matchGen_D0Dau1_mass[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau1_y",&matchGen_D0Dau1_y_, "matchGen_D0Dau1_y[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau1_charge",&matchGen_D0Dau1_charge_, "matchGen_D0Dau1_charge[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau1_pdgId",&matchGen_D0Dau1_pdgId_, "matchGen_D0Dau1_pdgId[candSize]/I");

                PATCompositeNtuple->Branch("matchGen_D0Dau2_pT",&matchGen_D0Dau2_pT_, "matchGen_D0Dau2_pT[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau2_eta",&matchGen_D0Dau2_eta_, "matchGen_D0Dau2_eta[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau2_phi",&matchGen_D0Dau2_phi_, "matchGen_D0Dau2_phi[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau2_mass",&matchGen_D0Dau2_mass_, "matchGen_D0Dau2_mass[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau2_y",&matchGen_D0Dau2_y_, "matchGen_D0Dau2_y[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau2_charge",&matchGen_D0Dau2_charge_, "matchGen_D0Dau2_charge[candSize]/F");
                PATCompositeNtuple->Branch("matchGen_D0Dau2_pdgId",&matchGen_D0Dau2_pdgId_, "matchGen_D0Dau2_pdgId[candSize]/I");
                PATCompositeNtuple->Branch("matchGen_D1ancestorId_",&matchGen_D1ancestorId_, "matchGen_D1ancestorId_[candSize]/I");
                PATCompositeNtuple->Branch("matchGen_D1ancestorFlavor_",&matchGen_D1ancestorFlavor_, "matchGen_D1ancestorFlavor_[candSize]/I");
                PATCompositeNtuple->Branch("matchGen_validD0chain",&matchGen_validD0chain_, "matchGen_validD0chain[candSize]/O");
              }
          }
          
          if(doGenMatchingTOF_)
          {
            PATCompositeNtuple->Branch("PIDD1",&pid1,"PIDD1[candSize]/I");
            PATCompositeNtuple->Branch("PIDD2",&pid2,"PIDD2[candSize]/I");
            PATCompositeNtuple->Branch("TOFD1",&tof1,"TOFD1[candSize]/F");
            PATCompositeNtuple->Branch("TOFD2",&tof2,"TOFD2[candSize]/F");
          }

          if(twoLayerDecay_)
          {
              PATCompositeNtuple->Branch("massDaugther1",&grand_mass,"massDaugther1[candSize]/F");
              PATCompositeNtuple->Branch("massDaugther2",&massD2,"massDaugther2[candSize]/F");
              PATCompositeNtuple->Branch("pTD1",&pt1,"pTD1[candSize]/F");
              PATCompositeNtuple->Branch("EtaD1",&eta1,"EtaD1[candSize]/F");
              PATCompositeNtuple->Branch("PhiD1",&phi1,"PhiD1[candSize]/F");
              PATCompositeNtuple->Branch("VtxProbDaugther1",&grand_VtxProb,"VtxProbDaugther1[candSize]/F");
              PATCompositeNtuple->Branch("VtxChi2Daugther1",&grand_vtxChi2,"VtxChi2Daugther1[candSize]/F");
              PATCompositeNtuple->Branch("VtxNDFDaugther1",&grand_ndf,"VtxNDFDaugther1[candSize]/F");
              PATCompositeNtuple->Branch("3DCosPointingAngleDaugther1",&grand_agl,"3DCosPointingAngleDaugther1[candSize]/F");
              PATCompositeNtuple->Branch("3DPointingAngleDaugther1",&grand_agl_abs,"3DPointingAngleDaugther1[candSize]/F");
              PATCompositeNtuple->Branch("2DCosPointingAngleDaugther1",&grand_agl2D,"2DCosPointingAngleDaugther1[candSize]/F");
              PATCompositeNtuple->Branch("2DPointingAngleDaugther1",&grand_agl2D_abs,"2DPointingAngleDaugther1[candSize]/F");
              PATCompositeNtuple->Branch("3DDecayLengthSignificanceDaugther1",&grand_dlos,"3DDecayLengthSignificanceDaugther1[candSize]/F");
              PATCompositeNtuple->Branch("3DDecayLengthDaugther1",&grand_dl,"3DDecayLengthDaugther1[candSize]/F");
              PATCompositeNtuple->Branch("3DDecayLengthErrorDaugther1",&grand_dlerror,"3DDecayLengthErrorDaugther1[candSize]/F");
              PATCompositeNtuple->Branch("2DDecayLengthDaugther1",&grand_dl2D,"2DDecayLengthDaugther1[candSize]/F");
              PATCompositeNtuple->Branch("2DDecayLengthSignificanceDaugther1",&grand_dlos2D,"2DDecayLengthSignificanceDaugther1[candSize]/F");
              PATCompositeNtuple->Branch("zDCASignificanceDaugther2",&dzos2,"zDCASignificanceDaugther2[candSize]/F");
              PATCompositeNtuple->Branch("xyDCASignificanceDaugther2",&dxyos2,"xyDCASignificanceDaugther2[candSize]/F");
              PATCompositeNtuple->Branch("NHitD2",&nhit2,"NHitD2[candSize]/F");
              PATCompositeNtuple->Branch("HighPurityDaugther2",&trkquality2,"HighPurityDaugther2[candSize]/O");
              PATCompositeNtuple->Branch("pTD2",&pt2,"pTD2[candSize]/F");
              PATCompositeNtuple->Branch("EtaD2",&eta2,"EtaD2[candSize]/F");
              PATCompositeNtuple->Branch("PhiD2",&phi2,"PhiD2[candSize]/F");
              PATCompositeNtuple->Branch("pTerrD1",&ptErr1,"pTerrD1[candSize]/F");
              PATCompositeNtuple->Branch("pTerrD2",&ptErr2,"pTerrD2[candSize]/F");
              PATCompositeNtuple->Branch("dedxHarmonic2D2",&H2dedx2,"dedxHarmonic2D2[candSize]/F");
              PATCompositeNtuple->Branch("zDCASignificanceGrandDaugther1",&grand_dzos1,"zDCASignificanceGrandDaugther1[candSize]/F");
              PATCompositeNtuple->Branch("zDCASignificanceGrandDaugther2",&grand_dzos2,"zDCASignificanceGrandDaugther2[candSize]/F");
              PATCompositeNtuple->Branch("xyDCASignificanceGrandDaugther1",&grand_dxyos1,"xyDCASignificanceGrandDaugther1[candSize]/F");
              PATCompositeNtuple->Branch("xyDCASignificanceGrandDaugther2",&grand_dxyos2,"xyDCASignificanceGrandDaugther2[candSize]/F");
              PATCompositeNtuple->Branch("NHitGrandD1",&grand_nhit1,"NHitGrandD1[candSize]/F");
              PATCompositeNtuple->Branch("NHitGrandD2",&grand_nhit2,"NHitGrandD2[candSize]/F");
              PATCompositeNtuple->Branch("HighPurityGrandDaugther1",&grand_trkquality1,"HighPurityGrandDaugther1[candSize]/O");
              PATCompositeNtuple->Branch("HighPurityGrandDaugther2",&grand_trkquality2,"HighPurityGrandDaugther2[candSize]/O");
              PATCompositeNtuple->Branch("pTGrandD1",&grand_pt1,"pTGrandD1[candSize]/F");
              PATCompositeNtuple->Branch("pTGrandD2",&grand_pt2,"pTGrandD2[candSize]/F");
              PATCompositeNtuple->Branch("pTerrGrandD1",&grand_ptErr1,"pTerrGrandD1[candSize]/F");
              PATCompositeNtuple->Branch("pTerrGrandD2",&grand_ptErr2,"pTerrGrandD2[candSize]/F");
              PATCompositeNtuple->Branch("massGrandD1",&grand_mass1,"massGrandD1[candSize]/F");
              PATCompositeNtuple->Branch("massGrandD2",&grand_mass2,"massGrandD2[candSize]/F");
              PATCompositeNtuple->Branch("EtaGrandD1",&grand_eta1,"EtaGrandD1[candSize]/F");
              PATCompositeNtuple->Branch("EtaGrandD2",&grand_eta2,"EtaGrandD2[candSize]/F");
              PATCompositeNtuple->Branch("PhiGrandD1",&grand_phi1,"PhiGrandD1[candSize]/F");
              PATCompositeNtuple->Branch("PhiGrandD2",&grand_phi2,"PhiGrandD2[candSize]/F");
              PATCompositeNtuple->Branch("dedxHarmonic2GrandD1",&grand_H2dedx1,"dedxHarmonic2GrandD1[candSize]/F");
              PATCompositeNtuple->Branch("dedxHarmonic2GrandD2",&grand_H2dedx2,"dedxHarmonic2GrandD2[candSize]/F");
          }
          else
          {
              PATCompositeNtuple->Branch("zDCASignificanceDaugther1",&dzos1,"zDCASignificanceDaugther1[candSize]/F");
              PATCompositeNtuple->Branch("xyDCASignificanceDaugther1",&dxyos1,"xyDCASignificanceDaugther1[candSize]/F");
              PATCompositeNtuple->Branch("zDCADaugther1",&dzval1,"zDCADaugther1[candSize]/F");
              PATCompositeNtuple->Branch("xyDCADaugther1",&dxyval1,"xyDCADaugther1[candSize]/F");
              PATCompositeNtuple->Branch("NHitD1",&nhit1,"NHitD1[candSize]/F");
              PATCompositeNtuple->Branch("HighPurityDaugther1",&trkquality1,"HighPurityDaugther1[candSize]/O");
              PATCompositeNtuple->Branch("pTD1",&pt1,"pTD1[candSize]/F");
              PATCompositeNtuple->Branch("pTerrD1",&ptErr1,"pTerrD1[candSize]/F");
              PATCompositeNtuple->Branch("EtaD1",&eta1,"EtaD1[candSize]/F");
              PATCompositeNtuple->Branch("PhiD1",&phi1,"PhiD1[candSize]/F");
              PATCompositeNtuple->Branch("dedxHarmonic2D1",&H2dedx1,"dedxHarmonic2D1[candSize]/F");
             PATCompositeNtuple->Branch("normalizedChi2Daugther1",&trkChi1,"normalizedChi2Daugther1[candSize]/F");
              PATCompositeNtuple->Branch("zDCASignificanceDaugther2",&dzos2,"zDCASignificanceDaugther2[candSize]/F");
              PATCompositeNtuple->Branch("xyDCASignificanceDaugther2",&dxyos2,"xyDCASignificanceDaugther2[candSize]/F");
              PATCompositeNtuple->Branch("zDCADaugther2",&dzval2,"zDCADaugther2[candSize]/F");
              PATCompositeNtuple->Branch("xyDCADaugther2",&dxyval2,"xyDCADaugther2[candSize]/F");
              PATCompositeNtuple->Branch("NHitD2",&nhit2,"NHitD2[candSize]/F");
              PATCompositeNtuple->Branch("HighPurityDaugther2",&trkquality2,"HighPurityDaugther2[candSize]/O");
              PATCompositeNtuple->Branch("pTD2",&pt2,"pTD2[candSize]/F");
              PATCompositeNtuple->Branch("pTerrD2",&ptErr2,"pTerrD2[candSize]/F");
              PATCompositeNtuple->Branch("EtaD2",&eta2,"EtaD2[candSize]/F");
              PATCompositeNtuple->Branch("PhiD2",&phi2,"PhiD2[candSize]/F");
              PATCompositeNtuple->Branch("dedxHarmonic2D2",&H2dedx2,"dedxHarmonic2D2[candSize]/F");
             PATCompositeNtuple->Branch("normalizedChi2Daugther2",&trkChi2,"normalizedChi2Daugther2[candSize]/F");
          }
          
          if(doMuon_)
          {
              PATCompositeNtuple->Branch("OneStMuon1",&onestmuon1,"OneStMuon1[candSize]/O");
              PATCompositeNtuple->Branch("OneStMuon2",&onestmuon2,"OneStMuon2[candSize]/O");
              PATCompositeNtuple->Branch("PFMuon1",&pfmuon1,"PFMuon1[candSize]/O");
              PATCompositeNtuple->Branch("PFMuon2",&pfmuon2,"PFMuon2[candSize]/O");
              PATCompositeNtuple->Branch("GlbMuon1",&glbmuon1,"GlbMuon1[candSize]/O");
              PATCompositeNtuple->Branch("GlbMuon2",&glbmuon2,"GlbMuon2[candSize]/O");
              PATCompositeNtuple->Branch("trkMuon1",&trkmuon1,"trkMuon1[candSize]/O");
              PATCompositeNtuple->Branch("trkMuon2",&trkmuon2,"trkMuon2[candSize]/O");
              PATCompositeNtuple->Branch("caloMuon1",&calomuon1,"caloMuon1[candSize]/O");
              PATCompositeNtuple->Branch("caloMuon2",&calomuon2,"caloMuon2[candSize]/O");
              PATCompositeNtuple->Branch("SoftMuon1",&softmuon1,"SoftMuon1[candSize]/O");
              PATCompositeNtuple->Branch("SoftMuon2",&softmuon2,"SoftMuon2[candSize]/O");

              if(doMuonFull_)
            {
              PATCompositeNtuple->Branch("nMatchedChamberD1",&nmatchedch1,"nMatchedChamberD1[candSize]/F");
              PATCompositeNtuple->Branch("nMatchedStationD1",&nmatchedst1,"nMatchedStationD1[candSize]/F");
              PATCompositeNtuple->Branch("EnergyDepositionD1",&matchedenergy1,"EnergyDepositionD1[candSize]/F");
              PATCompositeNtuple->Branch("nMatchedChamberD2",&nmatchedch2,"nMatchedChamberD2[candSize]/F");
              PATCompositeNtuple->Branch("nMatchedStationD2",&nmatchedst2,"nMatchedStationD2[candSize]/F");
              PATCompositeNtuple->Branch("EnergyDepositionD2",&matchedenergy2,"EnergyDepositionD2[candSize]/F");
              PATCompositeNtuple->Branch("dx1_seg",        &dx1_seg_, "dx1_seg[candSize]/F");
              PATCompositeNtuple->Branch("dy1_seg",        &dy1_seg_, "dy1_seg[candSize]/F");
              PATCompositeNtuple->Branch("dxSig1_seg",     &dxSig1_seg_, "dxSig1_seg[candSize]/F");
              PATCompositeNtuple->Branch("dySig1_seg",     &dySig1_seg_, "dySig1_seg[candSize]/F");
              PATCompositeNtuple->Branch("ddxdz1_seg",     &ddxdz1_seg_, "ddxdz1_seg[candSize]/F");
              PATCompositeNtuple->Branch("ddydz1_seg",     &ddydz1_seg_, "ddydz1_seg[candSize]/F");
              PATCompositeNtuple->Branch("ddxdzSig1_seg",  &ddxdzSig1_seg_, "ddxdzSig1_seg[candSize]/F");
              PATCompositeNtuple->Branch("ddydzSig1_seg",  &ddydzSig1_seg_, "ddydzSig1_seg[candSize]/F");
              PATCompositeNtuple->Branch("dx2_seg",        &dx2_seg_, "dx2_seg[candSize]/F");
              PATCompositeNtuple->Branch("dy2_seg",        &dy2_seg_, "dy2_seg[candSize]/F");
              PATCompositeNtuple->Branch("dxSig2_seg",     &dxSig2_seg_, "dxSig2_seg[candSize]/F");
              PATCompositeNtuple->Branch("dySig2_seg",     &dySig2_seg_, "dySig2_seg[candSize]/F");
              PATCompositeNtuple->Branch("ddxdz2_seg",     &ddxdz2_seg_, "ddxdz2_seg[candSize]/F");
              PATCompositeNtuple->Branch("ddydz2_seg",     &ddydz2_seg_, "ddydz2_seg[candSize]/F");
              PATCompositeNtuple->Branch("ddxdzSig2_seg",  &ddxdzSig2_seg_, "ddxdzSig2_seg[candSize]/F");
              PATCompositeNtuple->Branch("ddydzSig2_seg",  &ddydzSig2_seg_, "ddydzSig2_seg[candSize]/F");
           }
        }
    }

    } // doRecoNtuple_

    if(doGenNtuple_)
    {
        PATCompositeNtuple->Branch("gen_weight",&gen_weight,"gen_weight/F");
        PATCompositeNtuple->Branch("candSize_gen",&candSize_gen,"candSize_gen/I");
        PATCompositeNtuple->Branch("gen_mass",&mass_gen,"mass_gen[candSize_gen]/F");
        PATCompositeNtuple->Branch("gen_pT",&pt_gen,"pT_gen[candSize_gen]/F");
        PATCompositeNtuple->Branch("gen_eta",&eta_gen,"eta_gen[candSize_gen]/F");
        PATCompositeNtuple->Branch("gen_phi",&phi_gen,"phi_gen[candSize_gen]/F");
        PATCompositeNtuple->Branch("gen_y",&y_gen,"y_gen[candSize_gen]/F");
        PATCompositeNtuple->Branch("gen_status",&status_gen,"status_gen[candSize_gen]/I");
        PATCompositeNtuple->Branch("gen_pdgId",&pdgId_gen,"pdgId_gen[candSize_gen]/I");
        PATCompositeNtuple->Branch("gen_charge",&charge_gen,"charge_gen[candSize_gen]/I");
        PATCompositeNtuple->Branch("gen_MotherID",&idmom,"MotherID_gen[candSize_gen]/I");

        if(decayInGen_)
        {

            PATCompositeNtuple->Branch("gen_DauID1",&iddau1,"DauID1_gen[candSize_gen]/I");
            PATCompositeNtuple->Branch("gen_DauID2",&iddau2,"DauID2_gen[candSize_gen]/I");
        }
        if(twoLayerDecay_){
          PATCompositeNtuple->Branch("gen_D0pT",&gen_D0pT_, "gen_D0pT[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0eta",&gen_D0eta_, "gen_D0eta[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0phi",&gen_D0phi_, "gen_D0phi[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0mass",&gen_D0mass_, "gen_D0mass[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0y",&gen_D0y_, "gen_D0y[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0charge",&gen_D0charge_, "gen_D0charge[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0pdgId",&gen_D0pdgId_, "gen_D0pdgId[candSize_gen]/I");
          PATCompositeNtuple->Branch("gen_D0ancestorId_",&gen_D0ancestorId_, "gen_D0ancestorId_[candSize_gen]/I");
          PATCompositeNtuple->Branch("gen_D0ancestorFlavor_",&gen_D0ancestorFlavor_, "gen_D0ancestorFlavor_[candSize_gen]/I");

          PATCompositeNtuple->Branch("gen_D0Dau1_pT",&gen_D0Dau1_pT_, "gen_D0Dau1_pT[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau1_eta",&gen_D0Dau1_eta_, "gen_D0Dau1_eta[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau1_phi",&gen_D0Dau1_phi_, "gen_D0Dau1_phi[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau1_mass",&gen_D0Dau1_mass_, "gen_D0Dau1_mass[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau1_y",&gen_D0Dau1_y_, "gen_D0Dau1_y[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau1_charge",&gen_D0Dau1_charge_, "gen_D0Dau1_charge[candSize_gen]/I");
          PATCompositeNtuple->Branch("gen_D0Dau1_pdgId",&gen_D0Dau1_pdgId_, "gen_D0Dau1_pdgId[candSize_gen]/I");

          PATCompositeNtuple->Branch("gen_D0Dau2_pT",&gen_D0Dau2_pT_, "gen_D0Dau2_pT[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau2_eta",&gen_D0Dau2_eta_, "gen_D0Dau2_eta[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau2_phi",&gen_D0Dau2_phi_, "gen_D0Dau2_phi[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau2_mass",&gen_D0Dau2_mass_, "gen_D0Dau2_mass[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau2_y",&gen_D0Dau2_y_, "gen_D0Dau2_y[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau2_charge",&gen_D0Dau2_charge_, "gen_D0Dau2_charge[candSize_gen]/I");
          PATCompositeNtuple->Branch("gen_D0Dau2_pdgId",&gen_D0Dau2_pdgId_, "gen_D0Dau2_pdgId[candSize_gen]/I");

          PATCompositeNtuple->Branch("gen_D1pT",&gen_D1pT_, "gen_D1pT[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D1eta",&gen_D1eta_, "gen_D1eta[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D1phi",&gen_D1phi_, "gen_D1phi[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D1mass",&gen_D1mass_, "gen_D1mass[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D1y",&gen_D1y_, "gen_D1y[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D1charge",&gen_D1charge_, "gen_D1charge[candSize_gen]/I");
          PATCompositeNtuple->Branch("gen_D1pdgId",&gen_D1pdgId_, "gen_D1pdgId[candSize_gen]/I");
          PATCompositeNtuple->Branch("gen_validDstarChain",&gen_validDstarChain_, "gen_validDstarChain[candSize_gen]/O");
          PATCompositeNtuple->Branch("gen_validD0chain",&gen_validD0chain_, "gen_validD0chain[candSize_gen]/O");
        }
        else{
          PATCompositeNtuple->Branch("gen_D0ancestorId_",&gen_D0ancestorId_, "gen_D0ancestorId_[candSize_gen]/I");
          PATCompositeNtuple->Branch("gen_D0ancestorFlavor_",&gen_D0ancestorFlavor_, "gen_D0ancestorFlavor_[candSize_gen]/I");
          PATCompositeNtuple->Branch("gen_D0Dau1_pT",&gen_D0Dau1_pT_, "gen_D0Dau1_pT[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau1_eta",&gen_D0Dau1_eta_, "gen_D0Dau1_eta[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau1_phi",&gen_D0Dau1_phi_, "gen_D0Dau1_phi[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau1_mass",&gen_D0Dau1_mass_, "gen_D0Dau1_mass[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau1_y",&gen_D0Dau1_y_, "gen_D0Dau1_y[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau1_charge",&gen_D0Dau1_charge_, "gen_D0Dau1_charge[candSize_gen]/I");
          PATCompositeNtuple->Branch("gen_D0Dau1_pdgId",&gen_D0Dau1_pdgId_, "gen_D0Dau1_pdgId[candSize_gen]/I");

          PATCompositeNtuple->Branch("gen_D0Dau2_pT",&gen_D0Dau2_pT_, "gen_D0Dau2_pT[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau2_eta",&gen_D0Dau2_eta_, "gen_D0Dau2_eta[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau2_phi",&gen_D0Dau2_phi_, "gen_D0Dau2_phi[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau2_mass",&gen_D0Dau2_mass_, "gen_D0Dau2_mass[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau2_y",&gen_D0Dau2_y_, "gen_D0Dau2_y[candSize_gen]/F");
          PATCompositeNtuple->Branch("gen_D0Dau2_charge",&gen_D0Dau2_charge_, "gen_D0Dau2_charge[candSize_gen]/I");
          PATCompositeNtuple->Branch("gen_D0Dau2_pdgId",&gen_D0Dau2_pdgId_, "gen_D0Dau2_pdgId[candSize_gen]/I");
          PATCompositeNtuple->Branch("gen_validD0chain",&gen_validD0chain_, "gen_validD0chain[candSize_gen]/O");
        }
    }
}

void PATCompositeTreeProducer2Refactored::processEventPlaneInfo(const edm::Event& iEvent) {
    if(!isEventPlane_) return;

    constexpr float kInvalid = -99.f;
    auto deltaPhiPeriodic = [](double a, double b, int order) {
      const double period = (order > 0 ? 2.0 * M_PI / static_cast<double>(order) : 2.0 * M_PI);
      double d = a - b;
      while (d > period / 2.0)
        d -= period;
      while (d <= -period / 2.0)
        d += period;
      return d;
    };
    auto set3 = [&](float (&arr)[3]) { for (auto& v : arr) v = kInvalid; };
    auto set2 = [&](float (&arr)[2]) { for (auto& v : arr) v = kInvalid; };
    auto setN = [&](float* arr, int n) { for (int i = 0; i < n; ++i) arr[i] = kInvalid; };

    set3(ephfpAngle);
    set3(ephfmAngle);
    set3(ephfpQ);
    set3(ephfmQ);
    ephfpSumW = kInvalid;
    ephfmSumW = kInvalid;
    set2(ephfmAngleoff);
    set2(ephfpAngleoff);

    set2(ephfmAngleRaw);
    set2(ephfmsumCosRaw);
    set2(ephfmsumSinRaw);
    set2(ephfmsumPtOrEt);

    set2(ephfpAngleRaw);
    set2(ephfpsumCosRaw);
    set2(ephfpsumSinRaw);
    set2(ephfpsumPtOrEt);

    set2(eptrackmidAngle);
    set2(eptrackmidQ);
    eptrackmidSumW = kInvalid;
    set2(eptrackpAngle);
    set2(eptrackpQ);
    set2(eptrackpSumW);
    set2(eptrackmAngle);
    set2(eptrackmQ);
    set2(eptrackmSumW);
    setN(epStoredAngle2, kCompareEPSize);
    setN(epStoredQ2, kCompareEPSize);
    setN(epStoredSumW, kCompareEPSize);
    setN(epRecalcAngle2, kCompareEPSize);
    setN(epRecalcQ2, kCompareEPSize);
    setN(epRecalcSumW, kCompareEPSize);
    setN(epDeltaAngle2, kCompareEPSize);
    setN(epDeltaQ2, kCompareEPSize);
    setN(epDeltaSumW, kCompareEPSize);
    epStoredSize = -1;
    epRecalcSize = -1;

    set2(ephfAngle);
    set2(ephfAngleoff);
    set2(ephfAngleRaw);
    set2(ephfQ);
    ephfSumW = kInvalid;
    set2(ephfsumCos);
    set2(ephfsumSin);
    set2(ephfsumCosRaw);
    set2(ephfsumSinRaw);
    set2(ephfsumPtOrEt);

    edm::Handle<reco::EvtPlaneCollection> eventplanes;
    iEvent.getByToken(tok_eventplaneSrc_, eventplanes);

    auto getEp = [&](size_t idx) -> const reco::EvtPlane* {
      if (!eventplanes.isValid()) return nullptr;
      if (eventplanes->size() <= idx) return nullptr;
      return &(*eventplanes)[idx];
    };

    // Additional branches: raw/off/flat + track-mid, using common hiEvtPlane index layout.
    const auto* hfMinusV2 = getEp(0);
    const auto* hfPlusV2 = getEp(1);
    const auto* hfV2 = getEp(2);
    const auto* trkMidV2 = getEp(3);
    const auto* trkPlusV2 = getEp(4);
    const auto* trkMinusV2 = getEp(5);

    const auto* hfMinusV3 = getEp(6);
    const auto* hfPlusV3 = getEp(7);
    const auto* hfV3 = getEp(8);
    const auto* trkMidV3 = getEp(9);
    const auto* trkPlusV3 = getEp(10);
    const auto* trkMinusV3 = getEp(11);

    // Legacy branches: keep the same indices/sign-convention as PATCompositeTreeProducer2.cc.
    // (HF-: 0,6 ; HF+: 1,7 ; "HF" combined: 2,8)
    if (hfMinusV2) { ephfmAngle[0] = hfMinusV2->angle(2); ephfmQ[0] = hfMinusV2->q(2); }
    if (hfMinusV3) { ephfmAngle[1] = hfMinusV3->angle(2); ephfmQ[1] = hfMinusV3->q(2); ephfmSumW = hfMinusV3->sumw(); }

    if (hfPlusV2) { ephfpAngle[0] = hfPlusV2->angle(2); ephfpQ[0] = hfPlusV2->q(2); }
    if (hfPlusV3) { ephfpAngle[1] = hfPlusV3->angle(2); ephfpQ[1] = hfPlusV3->q(2); ephfpSumW = hfPlusV3->sumw(); }
    if (hfMinusV2) { ephfmAngleoff[0] = hfMinusV2->angle(1); }
    if (hfMinusV3) { ephfmAngleoff[1] = hfMinusV3->angle(1); }
    if (hfPlusV2) { ephfpAngleoff[0] = hfPlusV2->angle(1); }
    if (hfPlusV3) { ephfpAngleoff[1] = hfPlusV3->angle(1); }

    if (hfMinusV2) {
      ephfmAngleRaw[0] = hfMinusV2->angle(0);
      ephfmsumCosRaw[0] = hfMinusV2->sumCos(0);
      ephfmsumSinRaw[0] = hfMinusV2->sumSin(0);
      ephfmsumPtOrEt[0] = hfMinusV2->sumPtOrEt();
    }
    if (hfMinusV3) {
      ephfmAngleRaw[1] = hfMinusV3->angle(0);
      ephfmsumCosRaw[1] = hfMinusV3->sumCos(0);
      ephfmsumSinRaw[1] = hfMinusV3->sumSin(0);
      ephfmsumPtOrEt[1] = hfMinusV3->sumPtOrEt();
    }

    if (hfPlusV2) {
      ephfpAngleRaw[0] = hfPlusV2->angle(0);
      ephfpsumCosRaw[0] = hfPlusV2->sumCos(0);
      ephfpsumSinRaw[0] = hfPlusV2->sumSin(0);
      ephfpsumPtOrEt[0] = hfPlusV2->sumPtOrEt();
    }
    if (hfPlusV3) {
      ephfpAngleRaw[1] = hfPlusV3->angle(0);
      ephfpsumCosRaw[1] = hfPlusV3->sumCos(0);
      ephfpsumSinRaw[1] = hfPlusV3->sumSin(0);
      ephfpsumPtOrEt[1] = hfPlusV3->sumPtOrEt();
    }

    if (trkMidV2) {
      eptrackmidAngle[0] = trkMidV2->angle(2);
      eptrackmidQ[0] = trkMidV2->q(2);
      eptrackmidSumW = trkMidV2->sumw();
    }
    if (trkMidV3) {
      eptrackmidAngle[1] = trkMidV3->angle(2);
      eptrackmidQ[1] = trkMidV3->q(2);
    }
    if (trkPlusV2) {
      eptrackpAngle[0] = trkPlusV2->angle(2);
      eptrackpQ[0] = trkPlusV2->q(2);
      eptrackpSumW[0] = trkPlusV2->sumw();
    }
    if (trkPlusV3) {
      eptrackpAngle[1] = trkPlusV3->angle(2);
      eptrackpQ[1] = trkPlusV3->q(2);
      eptrackpSumW[1] = trkPlusV3->sumw();
    }
    if (trkMinusV2) {
      eptrackmAngle[0] = trkMinusV2->angle(2);
      eptrackmQ[0] = trkMinusV2->q(2);
      eptrackmSumW[0] = trkMinusV2->sumw();
    }
    if (trkMinusV3) {
      eptrackmAngle[1] = trkMinusV3->angle(2);
      eptrackmQ[1] = trkMinusV3->q(2);
      eptrackmSumW[1] = trkMinusV3->sumw();
    }

    if (hfV2) {
      ephfAngle[0] = hfV2->angle(2);
      ephfAngleoff[0] = hfV2->angle(1);
      ephfAngleRaw[0] = hfV2->angle(0);
      ephfQ[0] = hfV2->q(2);
      ephfsumCos[0] = hfV2->sumCos(2);
      ephfsumSin[0] = hfV2->sumSin(2);
      ephfsumCosRaw[0] = hfV2->sumCos(0);
      ephfsumSinRaw[0] = hfV2->sumSin(0);
      ephfsumPtOrEt[0] = hfV2->sumPtOrEt();
    }
    if (hfV3) {
      ephfAngle[1] = hfV3->angle(2);
      ephfAngleoff[1] = hfV3->angle(1);
      ephfAngleRaw[1] = hfV3->angle(0);
      ephfQ[1] = hfV3->q(2);
      ephfSumW = hfV3->sumw();
      ephfsumCos[1] = hfV3->sumCos(2);
      ephfsumSin[1] = hfV3->sumSin(2);
      ephfsumCosRaw[1] = hfV3->sumCos(0);
      ephfsumSinRaw[1] = hfV3->sumSin(0);
      ephfsumPtOrEt[1] = hfV3->sumPtOrEt();
    }

    if (compareEventPlane_) {
      edm::Handle<reco::EvtPlaneCollection> storedEps;
      edm::Handle<reco::EvtPlaneCollection> recalcEps;
      iEvent.getByToken(tok_eventplaneSrc_, storedEps);
      iEvent.getByToken(tok_eventplaneSrcRecalc_, recalcEps);

      if (storedEps.isValid())
        epStoredSize = static_cast<int>(storedEps->size());
      if (recalcEps.isValid())
        epRecalcSize = static_cast<int>(recalcEps->size());

      for (int i = 0; i < kCompareEPSize; ++i) {
        const reco::EvtPlane* s = (storedEps.isValid() && static_cast<int>(storedEps->size()) > i) ? &(*storedEps)[i] : nullptr;
        const reco::EvtPlane* r = (recalcEps.isValid() && static_cast<int>(recalcEps->size()) > i) ? &(*recalcEps)[i] : nullptr;
        if (s) {
          epStoredAngle2[i] = s->angle(2);
          epStoredQ2[i] = s->q(2);
          epStoredSumW[i] = s->sumw();
        }
        if (r) {
          epRecalcAngle2[i] = r->angle(2);
          epRecalcQ2[i] = r->q(2);
          epRecalcSumW[i] = r->sumw();
        }
        if (s && r) {
          const bool sSentinel = (epStoredAngle2[i] <= -9.0f);
          const bool rSentinel = (epRecalcAngle2[i] <= -9.0f);
          if (sSentinel && rSentinel) {
            epDeltaAngle2[i] = 0.0f;
          } else if (!sSentinel && !rSentinel) {
            epDeltaAngle2[i] = static_cast<float>(deltaPhiPeriodic(epStoredAngle2[i], epRecalcAngle2[i], 2));
          }
          epDeltaQ2[i] = epStoredQ2[i] - epRecalcQ2[i];
          epDeltaSumW[i] = epStoredSumW[i] - epRecalcSumW[i];
        }
      }
    }
}

std::vector<reco::GenParticleRef> PATCompositeTreeProducer2Refactored::processGenMatching(const edm::Handle<reco::GenParticleCollection>& genpars) {
    std::vector<reco::GenParticleRef> genRefs;
    
    if(!genpars.isValid()) {
        edm::LogError("GenMatching") << "Gen matching cannot be done without Gen collection!!";
        return genRefs;
    }
    
    const bool logAllGen = (debugGenMatching_ && verboseDebug_);
    int pidMatches = 0;
    int nonTargetRejected = 0;
    int rejectedMissingTargetDau = 0;
    int rejectedDStarGamma = 0;
    int rejectedPermutation = 0;
    int daughterMatches = 0;
    int permutationMatches = 0;
    int finalAccepted = 0;
    
    for(unsigned int it = 0; it < genpars->size(); ++it) {
        const reco::GenParticle & trk = (*genpars)[it];
        int id = trk.pdgId();
        
        if(fabs(id) != PID_) {
            ++nonTargetRejected;
            if (logAllGen) {
                LogInfo("GenRefFilter")
                    << "[RECO][Decision] idx=" << it
                    << " pdg=" << id
                    << " nDau=" << trk.numberOfDaughters()
                    << " decision=REJECT reason=pidMismatch";
            }
            continue;
        }
        pidMatches++;
        
        // Check daughter PDG IDs based on decay mode (allow FSR gamma); count D* gamma
        bool hasDau1 = false;
        bool hasDau2 = false;
        int dstarGammaCount = 0;
        int nNonGammaDaughters = 0;
        for (size_t i = 0; i < trk.numberOfDaughters(); ++i) {
            const int dauId = std::abs(trk.daughter(i)->pdgId());
            if (dauId == 22) { ++dstarGammaCount; continue; }  // D* level gamma
            ++nNonGammaDaughters;
            if (dauId == std::abs(PID_dau1_)) hasDau1 = true;  // D0(421) for D*, K(321) for D0
            if (dauId == std::abs(PID_dau2_)) hasDau2 = true;  // π(211) for both
        }

        // Require exactly 2 non-gamma daughters (allow FSR)
        if(decayInGen_ && (!hasDau1 || !hasDau2)) {
            ++rejectedMissingTargetDau;
            if (debugGenMatching_) {
                LogInfo("GenRefFilter")
                    << "[RECO][Decision] idx=" << it
                    << " pdg=" << id
                    << " nDau=" << trk.numberOfDaughters()
                    << " nNonGamma=" << nNonGammaDaughters
                    << " hasDau1=" << hasDau1
                    << " hasDau2=" << hasDau2
                    << " decision=REJECT reason=missingTargetDaughters";
            }
            continue;
        }
        
        // Reject D* with explicit extra gamma; allow only D0 FSR
        if(twoLayerDecay_ && decayInGen_ && dstarGammaCount > 0) {
            ++rejectedDStarGamma;
            if (debugGenMatching_) {
                LogInfo("GenRefFilter")
                    << "[RECO][Decision] idx=" << it
                    << " pdg=" << id
                    << " nDau=" << trk.numberOfDaughters()
                    << " dstarGamma=" << dstarGammaCount
                    << " decision=REJECT reason=dstarHasGamma";
            }
            continue;
        }

        std::vector<unsigned int> idxs = findDaughterPermutation(trk, twoLayerDecay_);
        if (decayInGen_ && idxs.empty()) {
            ++rejectedPermutation;
            if (debugGenMatching_) {
                LogInfo("GenRefFilter")
                    << "[RECO][Decision] idx=" << it
                    << " pdg=" << id
                    << " nDau=" << trk.numberOfDaughters()
                    << " nNonGamma=" << nNonGammaDaughters
                    << " decision=REJECT reason=daughterPermutationFailed";
            }
            continue;
        }
        if (!idxs.empty()) {
            permutationMatches++;
        }
        daughterMatches++;
        genRefs.push_back(reco::GenParticleRef(genpars, it));
        finalAccepted++;
        if (debugGenMatching_) {
            LogInfo("GenRefFilter")
                << "[RECO][Decision] idx=" << it
                << " pdg=" << id
                << " nDau=" << trk.numberOfDaughters()
                << " nNonGamma=" << nNonGammaDaughters
                << " dstarGamma=" << dstarGammaCount
                << " idxsSize=" << idxs.size()
                << " decision=ACCEPT";
        }
    }
    
    if (debugGenMatching_) {
        LogInfo("GenRefFilter")
            << "[RECO][Summary]"
            << " totalGen=" << genpars->size()
            << " pidTarget=" << pidMatches
            << " pidMismatch=" << nonTargetRejected
            << " accepted=" << finalAccepted
            << " rejMissingTargetDau=" << rejectedMissingTargetDau
            << " rejDStarGamma=" << rejectedDStarGamma
            << " rejPermutation=" << rejectedPermutation
            << " daughterMatches=" << daughterMatches
            << " permutationMatches=" << permutationMatches;
    }
        
        return genRefs;
}

PATCompositeTreeProducer2Refactored::D0DaughterSummary
PATCompositeTreeProducer2Refactored::summarizeD0Daughters(const reco::Candidate* d0) const {
  D0DaughterSummary out;
  if (!d0) return out;
  for (size_t idau = 0; idau < d0->numberOfDaughters(); ++idau) {
    const auto* dau = d0->daughter(idau);
    if (!dau) continue;
    const int absId = std::abs(dau->pdgId());
    if (absId == 22) {
      ++out.gammaCount;
      continue;
    }
    ++out.nonGammaCount;
    if (absId == KAON_PDG_ID) out.hasKaon = true;
    if (absId == PION_PDG_ID) out.hasPion = true;
  }
  return out;
}

const reco::GenParticle*
PATCompositeTreeProducer2Refactored::findAncestor(const reco::GenParticle* particle, int absPdgId) const {
  const reco::GenParticle* current = particle;
  while (current) {
    const reco::Candidate* mother = current->mother();
    current = dynamic_cast<const reco::GenParticle*>(mother);
    if (!current) break;
    if (std::abs(current->pdgId()) == absPdgId) return current;
  }
  return nullptr;
}

bool PATCompositeTreeProducer2Refactored::matchD0WithFSR(const reco::Candidate* recoDau1,
                                              const reco::Candidate* recoDau2,
                                              const reco::GenParticle* genD0,
                                              double maxDr) const {
  if (!genD0 || !recoDau1 || !recoDau2) return false;

  bool invalidDau = false;
  bool hasKMatch = false;
  bool hasPiMatch = false;
  bool usedReco1 = false;
  bool usedReco2 = false;

  auto tryMatch = [&](const reco::Candidate* recoDau, bool& usedFlag, const reco::Candidate* genDau) -> bool {
    if (usedFlag || !recoDau || !genDau) return false;
    if (reco::deltaR(*genDau, *recoDau) < maxDr) {
      usedFlag = true;
      return true;
    }
    return false;
  };

  for (size_t idau = 0; idau < genD0->numberOfDaughters(); ++idau) {
    const auto* genDau = genD0->daughter(idau);
    if (!genDau) continue;
    const int absId = std::abs(genDau->pdgId());
    if (absId == 22) continue;  // allow FSR gamma
    if (absId != KAON_PDG_ID && absId != PION_PDG_ID) { invalidDau = true; break; }

    bool matchedThis = tryMatch(recoDau1, usedReco1, genDau);
    if (!matchedThis) matchedThis = tryMatch(recoDau2, usedReco2, genDau);

    if (matchedThis) {
      if (absId == KAON_PDG_ID) hasKMatch = true;
      else if (absId == PION_PDG_ID) hasPiMatch = true;
    }
  }

  return (!invalidDau && hasKMatch && hasPiMatch);
}

void PATCompositeTreeProducer2Refactored::resetRecoGenMatch(unsigned int idx) {
  matchGEN[idx] = false;
  isSwap[idx] = false;
  idmom_reco[idx] = -77;
  idBAnc_reco[idx] = -77;
  gen_agl_abs[idx] = INVALID_VALUE;
  gen_agl2D_abs[idx] = INVALID_VALUE;
  gen_dl[idx] = INVALID_VALUE;
  gen_dl2D[idx] = INVALID_VALUE;

  const float invalidFloat = INVALID_VALUE;
  const int invalidInt = -1;
  const int invalidCharge = -99;

  matchGen_DStarpT_[idx] = invalidFloat;
  matchGen_DStareta_[idx] = invalidFloat;
  matchGen_DStarphi_[idx] = invalidFloat;
  matchGen_DStarmass_[idx] = invalidFloat;
  matchGen_DStary_[idx] = invalidFloat;
  matchGen_DStarcharge_[idx] = invalidCharge;
  matchGen_DStarpdgId_[idx] = 0;

  matchGen_D0pT_[idx] = invalidFloat;
  matchGen_D0eta_[idx] = invalidFloat;
  matchGen_D0phi_[idx] = invalidFloat;
  matchGen_D0mass_[idx] = invalidFloat;
  matchGen_D0y_[idx] = invalidFloat;
  matchGen_D0charge_[idx] = invalidCharge;
  matchGen_D0pdgId_[idx] = 0;

  matchGen_D0Dau1_pT_[idx] = invalidFloat;
  matchGen_D0Dau1_eta_[idx] = invalidFloat;
  matchGen_D0Dau1_phi_[idx] = invalidFloat;
  matchGen_D0Dau1_mass_[idx] = invalidFloat;
  matchGen_D0Dau1_y_[idx] = invalidFloat;
  matchGen_D0Dau1_charge_[idx] = invalidCharge;
  matchGen_D0Dau1_pdgId_[idx] = 0;

  matchGen_D0Dau2_pT_[idx] = invalidFloat;
  matchGen_D0Dau2_eta_[idx] = invalidFloat;
  matchGen_D0Dau2_phi_[idx] = invalidFloat;
  matchGen_D0Dau2_mass_[idx] = invalidFloat;
  matchGen_D0Dau2_y_[idx] = invalidFloat;
  matchGen_D0Dau2_charge_[idx] = invalidCharge;
  matchGen_D0Dau2_pdgId_[idx] = 0;

  matchGen_D1pT_[idx] = invalidFloat;
  matchGen_D1eta_[idx] = invalidFloat;
  matchGen_D1phi_[idx] = invalidFloat;
  matchGen_D1mass_[idx] = invalidFloat;
  matchGen_D1y_[idx] = invalidFloat;
  matchGen_D1decayLength2D_[idx] = invalidFloat;
  matchGen_D1decayLength3D_[idx] = invalidFloat;
  matchGen_D1angle2D_[idx] = invalidFloat;
  matchGen_D1angle3D_[idx] = invalidFloat;
  matchGen_D1ancestorId_[idx] = invalidInt;
  matchGen_D1ancestorFlavor_[idx] = invalidInt;
  matchGen_D1charge_[idx] = invalidCharge;
  matchGen_D1pdgId_[idx] = 0;
  matchGen_slowPion_dR_[idx] = invalidFloat;
  matchGen_D0Dau1_motherPdgId_[idx] = invalidInt;
  matchGen_D0Dau1_motherNDau_[idx] = invalidInt;
  matchGen_D0Dau2_motherPdgId_[idx] = invalidInt;
  matchGen_D0Dau2_motherNDau_[idx] = invalidInt;
  matchGen_D1_motherPdgId_[idx] = invalidInt;
  matchGen_D1_motherNDau_[idx] = invalidInt;
  matchGen_validDstarChain_[idx] = false;
  matchGen_validD0chain_[idx] = false;
}

int PATCompositeTreeProducer2Refactored::muAssocToTrack( const reco::TrackRef& trackref,
                const edm::Handle<reco::MuonCollection>& muonh) const {
  auto muon = std::find_if(muonh->cbegin(),muonh->cend(),
                           [&](const reco::Muon& m) {
                             return ( m.track().isNonnull() &&
                                      m.track() == trackref    );
                           });
  return ( muon != muonh->cend() ? std::distance(muonh->cbegin(),muon) : -1 );
}

void 
PATCompositeTreeProducer2Refactored::endJob() {
    
}

DEFINE_FWK_MODULE(PATCompositeTreeProducer2Refactored);
