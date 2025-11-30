// user include files
#include "FWCore/Framework/interface/one/EDAnalyzer.h"
#include "FWCore/Framework/interface/Frameworkfwd.h"

#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/MakerMacros.h"

#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Utilities/interface/InputTag.h"

#include "DataFormats/Candidate/interface/Candidate.h"
#include "DataFormats/HepMCCandidate/interface/GenParticle.h"

#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <algorithm> 



class GenParticleSimpleAnalyzer : public edm::one::EDAnalyzer<>{
public:
    explicit GenParticleSimpleAnalyzer(const edm::ParameterSet &);
    ~GenParticleSimpleAnalyzer(){};
private:
    virtual void beginJob();
    virtual void analyze(const edm::Event &, const edm::EventSetup &);
    virtual void endJob();
    virtual void getAncestorId(const reco::Candidate&);


    std::vector<int> wanted;
    edm::EDGetTokenT<reco::GenParticleCollection> tok_genParticle_;
    
};

GenParticleSimpleAnalyzer::GenParticleSimpleAnalyzer(const edm::ParameterSet& iConfig){
    tok_genParticle_ = consumes<reco::GenParticleCollection>( edm::InputTag(iConfig.getUntrackedParameter<edm::InputTag>("GenParticleCollection")));
    wanted = iConfig.getUntrackedParameter<std::vector<int> >("pdgIDs");
};


void GenParticleSimpleAnalyzer::beginJob(){};
void GenParticleSimpleAnalyzer::endJob(){};
void GenParticleSimpleAnalyzer::getAncestorId(const reco::Candidate& gCand){
  int gen_ancestorId_ = 0;
  //int gen_ancestorFlavor_ = 0;
//  reco::GenParticle gCand1(gCand.charge(),gCand.p4(),gCand.vertex(),421,2,true);
  //for (auto mothers = gCand.motherRefVector();
  //    !mothers.empty(); ) {
  //  auto mom = mothers.at(0);
  //  mothers = mom->motherRefVector();
  //  gen_ancestorId_ = mom->pdgId();
  //  cout << "gen_ancestorId_ : " << gen_ancestorId_ << endl;
  //  const auto idstr = std::to_string(std::abs(gen_ancestorId_));
  //  gen_ancestorFlavor_ = std::stoi(std::string{idstr.begin(), idstr.begin()+1});
  //  cout << "gen_ancestorFlavor_ : " << gen_ancestorFlavor_ << endl;
  //  if (idstr[0] == '5') {
  //    break;
  //  }
  //  if (std::abs(gen_ancestorId_) <= 40) break;
  //}
//  for (auto mom = gCand.mother(); !(mom==nullptr);){
//    gen_ancestorId_=mom->pdgId();
//    static const auto idstr= std::to_string(std::abs(gen_ancestorId_));
//    //gen_ancestorFlavor_ = std::stoi(std::string{idstr.begin(), idstr.begin()+1});
  //if (idstr.find(5 != '5') {
  //          std::cout << "prompt decay, Ancestor ID : " << idstr <<std::endl;
  //}
//
//    if (std::abs(gen_ancestorId_) <= 40) break;
//          mom = mom->mother();
//}
//std::string idstr;
//
//// Iterate over the ancestors.
//for (auto mom = gCand.mother(); mom != nullptr; mom = mom->mother()) {
//    // Get the absolute value of the pdgId and convert it to a string.
//    gen_ancestorId_ = mom->pdgId();
//    idstr = std::to_string(std::abs(gen_ancestorId_));
//    if (idstr.find('5') == std::string::npos) {
//    std::cout << "Ancestor ID: " << idstr << " doesn't contai the digit '5'" << std::endl;
//    }    
//    
//    // Stop if you hit an ID with absolute value <= 40.
//   // if (std::abs(gen_ancestorId_) <= 40)
//   //     break;
//   // return;
//}
// Flag to track if any ancestor ID contains '5'
bool foundDigit5 = false;
std::string lastAncestorId;
// Loop over all ancestors.
for (auto mom = gCand.mother(); mom != nullptr; mom = mom->mother()) {
    int pdg = mom->pdgId();
    std::string idStr = std::to_string(std::abs(pdg));

    if (idStr.find('5') != std::string::npos) {
        foundDigit5 = true;
    }

    lastAncestorId = idStr;
}

if (!foundDigit5 && !lastAncestorId.empty()) {
    std::cout << "prompt decay, Ancestor ID: " << lastAncestorId << std::endl;
}




};

void GenParticleSimpleAnalyzer::analyze(const edm::Event &iEvent, const edm::EventSetup &iSteup){
    using namespace std;
    edm::Handle<reco::GenParticleCollection> genpars;
    iEvent.getByToken(tok_genParticle_, genpars);
    if(!genpars.isValid()){
        cout << "Faulty gen collection.. I'm out" << endl;
        return;
    }

    auto const nGens = genpars->size();
    cout << "nGens : " << nGens << endl;
    for(unsigned int igen=0; igen < nGens; igen++){
        const reco::GenParticle &trk = (*genpars)[igen];
        auto hasP = std::find(wanted.begin(), wanted.end(), trk.pdgId() );
        if(hasP == wanted.end()) continue;
        cout << trk.pdgId();
        cout << Form(" -> %zu (",trk.numberOfDaughters());
        string dauInfo = "    Dau : ";
        for(unsigned int idau=0; idau <trk.numberOfDaughters(); ++idau){
            auto const& dau = trk.daughter(idau);
            cout << dau->pdgId() << " ";
            dauInfo += Form("(%.3f, %.3f) ", dau->pt(), dau->eta());
        }
        getAncestorId(trk);
        cout << ")" << endl;
        cout << dauInfo << endl;
    }
};


DEFINE_FWK_MODULE(GenParticleSimpleAnalyzer);
