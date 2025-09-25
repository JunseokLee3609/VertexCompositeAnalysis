#ifndef VertexCompositeAnalysis_VertexCompositeAnalyzer_DStarGrandDauGenMatchProducer_h
#define VertexCompositeAnalysis_VertexCompositeAnalyzer_DStarGrandDauGenMatchProducer_h

#include <vector>

#include "FWCore/Framework/interface/Frameworkfwd.h"
#include "FWCore/Framework/interface/one/EDAnalyzer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Utilities/interface/InputTag.h"
#include "FWCore/ServiceRegistry/interface/Service.h"

#include "DataFormats/Candidate/interface/Candidate.h"
#include "DataFormats/PatCandidates/interface/CompositeCandidate.h"
#include "DataFormats/HepMCCandidate/interface/GenParticle.h"

#include "CommonTools/UtilAlgos/interface/TFileService.h"

#include "TTree.h"

class DStarGrandDauGenMatchProducer : public edm::one::EDAnalyzer<edm::one::SharedResources> {
public:
  explicit DStarGrandDauGenMatchProducer(const edm::ParameterSet&);
  ~DStarGrandDauGenMatchProducer() override = default;

  void analyze(const edm::Event&, const edm::EventSetup&) override;
  void beginJob() override;

private:
  struct MatchResult {
    const reco::GenParticle* particle = nullptr;
    double deltaR = 999.;
    int index = -1;
  };

  MatchResult findBestMatch(const reco::Candidate& recoCand,
                            const std::vector<const reco::GenParticle*>& genTracks,
                            const std::vector<bool>& usedFlags) const;
  const reco::GenParticle* findAncestor(const reco::GenParticle* particle, int absPdgId) const;
  void resetBranches();

  edm::EDGetTokenT<pat::CompositeCandidateCollection> dstarToken_;
  edm::EDGetTokenT<reco::GenParticleCollection> genToken_;

  double maxDeltaR_;
  bool keepChargeMismatch_;
  static constexpr float kInvalidFloat_ = -999.f;
  static constexpr int kInvalidInt_ = -999;

  TTree* tree_ = nullptr;

  int nCand_ = 0;
  std::vector<float> dstar_pt_;
  std::vector<float> dstar_eta_;
  std::vector<float> dstar_phi_;
  std::vector<float> dstar_mass_;

  std::vector<int> dstar_hasGenMatch_;
  std::vector<float> dstar_match_dr_;
  std::vector<int> dstar_match_pdgId_;
  std::vector<float> dstar_match_pt_;
  std::vector<float> dstar_match_eta_;
  std::vector<float> dstar_match_phi_;
  std::vector<float> dstar_match_mass_;

  std::vector<int> d0_hasGenMatch_;
  std::vector<float> d0_match_dr_;
  std::vector<int> d0_match_pdgId_;
  std::vector<float> d0_match_pt_;
  std::vector<float> d0_match_eta_;
  std::vector<float> d0_match_phi_;
  std::vector<float> d0_match_mass_;

  std::vector<int> gdau1_reco_pdgId_;
  std::vector<int> gdau1_reco_charge_;
  std::vector<float> gdau1_reco_pt_;
  std::vector<float> gdau1_reco_eta_;
  std::vector<float> gdau1_reco_phi_;

  std::vector<int> gdau1_hasMatch_;
  std::vector<float> gdau1_match_dr_;
  std::vector<int> gdau1_match_pdgId_;

  std::vector<int> gdau2_reco_pdgId_;
  std::vector<int> gdau2_reco_charge_;
  std::vector<float> gdau2_reco_pt_;
  std::vector<float> gdau2_reco_eta_;
  std::vector<float> gdau2_reco_phi_;

  std::vector<int> gdau2_hasMatch_;
  std::vector<float> gdau2_match_dr_;
  std::vector<int> gdau2_match_pdgId_;

  std::vector<int> slow_reco_pdgId_;
  std::vector<int> slow_reco_charge_;
  std::vector<float> slow_reco_pt_;
  std::vector<float> slow_reco_eta_;
  std::vector<float> slow_reco_phi_;

  std::vector<int> slow_hasMatch_;
  std::vector<float> slow_match_dr_;
  std::vector<int> slow_match_pdgId_;
};

#endif
