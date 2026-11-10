#include "full_rank.hxx"
#include <unordered_set>
#include <map>
#include <array>
#include <algorithm>
#include "DataFormats/PatCandidates/interface/PackedGenParticle.h"
#include "DataFormats/HepMCCandidate/interface/GenParticle.h"
#include "TVector3.h"
namespace independent {
struct G {const reco::Candidate*p;int collection,index;};
struct Row {
 int signal=0,hydjet=0,swap=0,gen=-1,missing=0,ambiguous=0,reason=0,metadata=-1;
 std::vector<int>raw,selectedCollection,selectedIndex;
 std::vector<double>distance;
};
struct Event {std::vector<Row>rows;std::vector<int>genKeys;std::vector<std::vector<double>>truth;};
inline const reco::Candidate* mother(const reco::Candidate*p) {return p&&p->numberOfMothers()?p->mother(0):nullptr;}
inline bool hadron(int id){id=std::abs(id);return id==211||id==321||id==2212;}
inline std::vector<double> metadata(const reco::Candidate*ds,const reco::Candidate*d0,const reco::Candidate*k,
 const reco::Candidate*pi,const reco::Candidate*s,int collision,int gamma) {
 std::vector<double>v;
 for(auto p:{ds,d0,k,pi,s}) {
  v.insert(v.end(),{p->pt(),p->eta(),p->phi(),p->mass(),p->rapidity(),double(p->charge()),double(p->pdgId())});
 }
 auto mom=mother(ds);int momid=mom?mom->pdgId():-99,bid=-99;
 auto anc=mom;int dep=0;
 while(anc&&dep<50){anc=mother(anc);++dep;if(anc&&(std::abs(anc->pdgId())%1000)/100==5){bid=anc->pdgId();break;}}
 int ancestorId=0,ancestorFlavor=0;auto p=ds;dep=0;
 while(p&&dep<50) {
  if(dep>0) {ancestorId=p->pdgId();std::string text=std::to_string(std::abs(ancestorId));
   ancestorFlavor=text[0]-'0';if(text[0]=='5'||std::abs(ancestorId)<=40)break;}
  p=mother(p);++dep;
 }
 double l2=-99,l3=-99,a2=-99,a3=-99;
 if(ds->numberOfDaughters()&&ds->daughter(0)) {
  auto vertex=ds->daughter(0)->vertex();auto origin=ds->vertex();
  TVector3 delta(vertex.x()-origin.x(),vertex.y()-origin.y(),vertex.z()-origin.z());
  TVector3 momentum(ds->px(),ds->py(),ds->pz());l3=delta.Mag();a3=momentum.Angle(delta);
  delta.SetZ(0);momentum.SetZ(0);l2=delta.Mag();a2=momentum.Angle(delta);
 }
 v.insert(v.end(),{double(collision),double(momid),double(bid),double(ancestorId),double(ancestorFlavor),
   l2,l3,a2,a3,double(gamma==0),double(gamma==0)});
 for(auto leaf:{k,pi,s}) {
  auto m=mother(leaf);v.push_back(m?m->pdgId():-99);v.push_back(m?m->numberOfDaughters():-99);
 }
 return v;
}
}
independent::Event independentMatch(
 const std::vector<pat::PackedCandidate>&pf,const std::vector<pat::PackedCandidate>&lost,
 const std::vector<pat::PackedCandidate>&ele,const std::vector<reco::Vertex>&vertices,const reco::BeamSpot&bs,
 const std::vector<pat::PackedGenParticle>&packed,const std::vector<reco::GenParticle>&pruned,
 const std::vector<double>&err,const std::vector<int>&rc,const std::vector<int>&hits,
 const std::vector<double>&dzsig,const std::vector<double>&dxysig,const std::vector<int>&kaonPos) {
 using namespace independent;
 std::vector<double>emptyd;std::vector<int>emptyi;
 auto identity=fullTrackMatch(pf,lost,ele,vertices,bs,emptyd,emptyd,emptyd,emptyi,err,rc,hits,dzsig,dxysig,kaonPos,true);
 std::vector<fullmatch::T>raw;
 auto add=[&](const auto&collection) {for(const auto&p:collection) {
  if(p.hasTrackDetails()) {const auto&t=p.pseudoTrack();raw.push_back({t.pt(),t.eta(),t.phi(),t.ptError(),0,0,int(t.charge()),int(t.numberOfValidHits())});}
  else if(p.covarianceVersion()>0) raw.push_back({p.ptTrk(),p.etaAtVtx(),p.phiAtVtx(),0,0,0,int(p.charge()),0});
 }};
 add(pf);add(lost);add(ele);
 std::vector<G>pool;std::unordered_set<unsigned>represented;
 std::unordered_map<const reco::Candidate*,int>prunedKey;
 for(unsigned i=0;i<pruned.size();++i)prunedKey[&pruned[i]]=i;
 for(unsigned i=0;i<packed.size();++i) {
  const auto&p=packed[i];if(p.charge()==0)continue;
  auto ref=p.lastPrunedRef();
  if(ref.isNonnull()&&ref.isAvailable()&&ref->status()==1) {
   represented.insert(ref.key());
   if(hadron(ref->pdgId()))pool.push_back({ref.get(),0,int(ref.key())});
  } else if(hadron(p.pdgId()))pool.push_back({&p,1,int(i)});
 }
 for(unsigned i=0;i<pruned.size();++i) {
  const auto&p=pruned[i];
  if(p.status()==1&&p.charge()!=0&&hadron(p.pdgId())&&!represented.count(i))pool.push_back({&p,0,int(i)});
 }
 // Spatial lookup preserves the original pool order and exact matching comparisons.
 constexpr int nPhi=180;const double twoPi=2*std::acos(-1.);
 std::map<std::pair<int,int>,std::vector<unsigned>>cells;
 auto phiCell=[&](double p){double q=std::fmod(p,twoPi);if(q<0)q+=twoPi;return std::min(nPhi-1,int(q/twoPi*nPhi));};
 for(unsigned j=0;j<pool.size();++j) {
  const auto*p=pool[j].p;if(!std::isfinite(p->eta())||!std::isfinite(p->phi()))continue;
  cells[{int(std::floor(p->eta()/.04)),phiCell(p->phi())}].push_back(j);
 }
 std::unordered_map<int,int>selected;std::unordered_map<int,double>quality;
 for(int rid:identity.rawIndex) {
  if(rid<0||selected.count(rid))continue;
  const auto&t=raw[rid];int best=-1;double distance=.03*.03;
  std::vector<unsigned>nearby;
  if(std::isfinite(t.eta)&&std::isfinite(t.phi)) {
   int e=int(std::floor(t.eta/.04)),p=phiCell(t.phi);
   for(int de=-1;de<=1;++de)for(int dp=-1;dp<=1;++dp) {
    auto found=cells.find({e+de,(p+dp+nPhi)%nPhi});
    if(found!=cells.end())nearby.insert(nearby.end(),found->second.begin(),found->second.end());
   }
  }
  std::sort(nearby.begin(),nearby.end());
  for(unsigned j:nearby) {
   const auto*p=pool[j].p;if(p->charge()!=t.charge||p->pt()<=0||t.pt<=0)continue;
   if(std::abs(t.pt-p->pt())/t.pt>=.5)continue;
   double d=fullmatch::dr2(p->eta(),p->phi(),t);
   if(d<distance){best=j;distance=d;}
  }
  selected[rid]=best;quality[rid]=best>=0?std::sqrt(distance):-99.;
 }
 Event out;std::map<int,int>metadataIndices;
 for(unsigned c=0;c<kaonPos.size();++c) {
  Row r;r.missing=identity.missing[c];r.ambiguous=identity.ambiguous[c];
  const reco::Candidate*leaf[3]={nullptr,nullptr,nullptr};
  for(unsigned j=0;j<3;++j) {
   int rid=identity.rawIndex[3*c+j];r.raw.push_back(rid);
   int chosen=rid>=0?selected.at(rid):-1;
   r.selectedCollection.push_back(chosen>=0?pool[chosen].collection:-1);
   r.selectedIndex.push_back(chosen>=0?pool[chosen].index:-1);
   r.distance.push_back(rid>=0?quality.at(rid):-99.);
   if(chosen>=0)leaf[j]=pool[chosen].p;
  }
  auto reject=[&](int code){r.reason=code;out.rows.push_back(r);};
  if(r.missing||r.ambiguous){reject(1);continue;}
  if(!leaf[0]||!leaf[1]||!leaf[2]){reject(2);continue;}
  if(leaf[0]==leaf[1]||leaf[0]==leaf[2]||leaf[1]==leaf[2]){reject(3);continue;}
  int p0=std::abs(leaf[0]->pdgId()),p1=std::abs(leaf[1]->pdgId());
  if(!((p0==321&&p1==211)||(p0==211&&p1==321))||std::abs(leaf[2]->pdgId())!=211){reject(4);continue;}
  auto d0=mother(leaf[0]);
  if(!d0||d0!=mother(leaf[1])||std::abs(d0->pdgId())!=421){reject(5);continue;}
  auto ds=mother(d0);
  if(!ds||ds!=mother(leaf[2])||std::abs(ds->pdgId())!=413){reject(6);continue;}
  int nd0=0,npi=0,nother=0;
  for(unsigned j=0;j<ds->numberOfDaughters();++j) {int id=std::abs(ds->daughter(j)->pdgId());if(id==421)++nd0;else if(id==211)++npi;else ++nother;}
  if(nd0!=1||npi!=1||nother){reject(7);continue;}
  int nk=0,np=0,ng=0;const reco::Candidate*k=nullptr,*pi=nullptr;nother=0;
  for(unsigned j=0;j<d0->numberOfDaughters();++j) {
   auto p=d0->daughter(j);int id=std::abs(p->pdgId());
   if(id==321){++nk;k=p;}else if(id==211){++np;pi=p;}else if(id==22)++ng;else++nother;
  }
  if(nk!=1||np!=1||nother||ng>1){reject(8);continue;}
  const auto*gen=dynamic_cast<const reco::GenParticle*>(ds);
  if(!gen||!prunedKey.count(ds))throw std::runtime_error("Dstar not in pruned GEN collection");
  int collision=gen->collisionId();r.gen=prunedKey.at(ds);r.signal=collision==0;r.hydjet=collision!=0;
  r.swap=std::abs(leaf[kaonPos[c]?0:1]->pdgId())!=321;r.reason=collision==0?10:11;
  auto found=metadataIndices.find(r.gen);
  if(found==metadataIndices.end()) {
   int ix=out.truth.size();metadataIndices[r.gen]=ix;out.genKeys.push_back(r.gen);
   out.truth.push_back(metadata(ds,d0,k,pi,leaf[2],collision,ng));r.metadata=ix;
  } else r.metadata=found->second;
  out.rows.push_back(r);
 }
 return out;
}
