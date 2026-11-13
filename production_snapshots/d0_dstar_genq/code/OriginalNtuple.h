#pragma once
#include <array>
#include <map>
#include <set>
#include <memory>
#include <vector>
#include <string>
#include <stdexcept>
#include <cstdint>
#include <cmath>
#include "TFile.h"
#include "TTree.h"
#include "TBranch.h"
#include "TLeaf.h"
namespace genq {
using Key=std::array<unsigned,3>;
struct LeafSignature {float error,dz,xy,eta,phi;int hits;};
struct OriginalEvent {
 Long64_t entry=-1;int hiBin=-1,nD0=0,nDstar=0;
 float vertex[3]{};
 float all[3][3]{},veto[3][2]{},hf[3][3]{};
 std::vector<LeafSignature> leaves;
};
class OriginalNtuple {
 std::unique_ptr<TFile> file_;
 TTree *d0_=nullptr,*star_=nullptr,*plane_=nullptr;
 std::map<Key,Long64_t> index_;
 static TLeaf* leaf(TTree* t,const std::string& name){
  auto*b=t->GetBranch(name.c_str());
  if(!b||b->GetListOfLeaves()->GetEntries()!=1)throw std::runtime_error("Missing/ambiguous branch "+name);
  return static_cast<TLeaf*>(b->GetListOfLeaves()->At(0));
 }
 static double value(TTree*t,const std::string&name,int i=0){return leaf(t,name)->GetValue(i);}
 static void enable(TTree*t,const std::string&name){leaf(t,name);t->SetBranchStatus(name.c_str(),1);}
 static void same(TTree*a,TTree*b){
  for(const char*k:{"RunNb","LSNb","EventNb"})
   if(value(a,k)!=value(b,k))throw std::runtime_error(std::string("Original tree alignment: ")+k);
 }
 public:
 Long64_t entries=0;
 explicit OriginalNtuple(const std::string& url){
  file_.reset(TFile::Open(url.c_str()));
  if(!file_||file_->IsZombie())throw std::runtime_error("Cannot open original ntuple "+url);
  d0_=dynamic_cast<TTree*>(file_->Get("d0ana_newreduced/PATCompositeNtuple"));
  star_=dynamic_cast<TTree*>(file_->Get("dStarana_mc/PATCompositeNtuple"));
  plane_=dynamic_cast<TTree*>(file_->Get("eventplane/EventPlane"));
  if(!d0_||!star_||!plane_)throw std::runtime_error("Original ntuple trees missing");
  entries=d0_->GetEntries();
  if(star_->GetEntries()!=entries||plane_->GetEntries()!=entries)throw std::runtime_error("Original tree entry counts differ");
  for(auto*t:{d0_,star_,plane_}){
   t->SetBranchStatus("*",0);
   for(const char*k:{"RunNb","LSNb","EventNb"})enable(t,k);
  }
  enable(d0_,"candSize");
  for(Long64_t i=0;i<entries;++i){
   if(d0_->GetEntry(i)<=0)throw std::runtime_error("Cannot index original D0 entry");
   Key key{{unsigned(value(d0_,"RunNb")),unsigned(value(d0_,"LSNb")),unsigned(value(d0_,"EventNb"))}};
   if(!index_.emplace(key,i).second||value(d0_,"candSize")<1)throw std::runtime_error("Original count-filter event index invalid");
  }
  for(const char*k:{"bestvtxX","bestvtxY","bestvtxZ"})enable(plane_,k);
  for(std::string side:{"","_forw","_afterw"}){
   for(std::string k:{"all_trkQx","all_trkQy","all_trkW","trkQx","trkQy"})enable(plane_,k+side);
  }
  for(const char*k:{"candSize","mass","centrality","pTerrGrandD1","pTerrGrandD2","pTerrD2",
    "NHitGrandD1","NHitGrandD2","NHitD2",
    "HighPurityGrandDaugther1","HighPurityGrandDaugther2","HighPurityDaugther2",
    "zDCASignificanceGrandDaugther1","zDCASignificanceGrandDaugther2","zDCASignificanceDaugther2",
    "xyDCASignificanceGrandDaugther1","xyDCASignificanceGrandDaugther2","xyDCASignificanceDaugther2",
    "EtaGrandD1","EtaGrandD2","EtaD2","PhiGrandD1","PhiGrandD2","PhiD2"})enable(star_,k);
  for(std::string side:{"","p","m"})for(std::string k:{"sumCosRaw","sumSinRaw","sumPtOrEt"})enable(star_,"ephf"+side+k);
 }
 Long64_t find(unsigned run,unsigned lumi,ULong64_t event)const{
  auto it=index_.find(Key{{run,lumi,unsigned(event)}});return it==index_.end()?-1:it->second;
 }
 OriginalEvent read(Long64_t entry){
  if(d0_->GetEntry(entry)<=0||star_->GetEntry(entry)<=0||plane_->GetEntry(entry)<=0)
   throw std::runtime_error("Cannot read original ntuple entry");
  same(d0_,star_);same(d0_,plane_);
  OriginalEvent o;o.entry=entry;o.nD0=value(d0_,"candSize");o.nDstar=value(star_,"candSize");o.hiBin=value(star_,"centrality");
  for(int i=0;i<3;++i)o.vertex[i]=value(plane_,std::string("bestvtx")+char('X'+i));
  const std::array<std::string,3> sides{{"","_forw","_afterw"}};
  for(int s=0;s<3;++s){
   o.all[s][0]=value(plane_,"all_trkQx"+sides[s]);o.all[s][1]=value(plane_,"all_trkQy"+sides[s]);o.all[s][2]=value(plane_,"all_trkW"+sides[s]);
   o.veto[s][0]=value(plane_,"trkQx"+sides[s]);o.veto[s][1]=value(plane_,"trkQy"+sides[s]);
   std::string hs=s==0?"":s==1?"p":"m";
   o.hf[s][0]=value(star_,"ephf"+hs+"sumCosRaw",0);o.hf[s][1]=value(star_,"ephf"+hs+"sumSinRaw",0);o.hf[s][2]=value(star_,"ephf"+hs+"sumPtOrEt",0);
  }
  for(int c=0;c<o.nDstar;++c){
   double mass=value(star_,"mass",c);if(!(mass>1.79&&mass<2.25))continue;
   for(int d=0;d<3;++d){
    std::string tag=d==0?"GrandDaugther1":d==1?"GrandDaugther2":"Daugther2";
    std::string end=d==0?"GrandD1":d==1?"GrandD2":"D2";
    if(!value(star_,"HighPurity"+tag,c))continue;
    LeafSignature x{float(value(star_,"pTerr"+end,c)),float(value(star_,"zDCASignificance"+tag,c)),float(value(star_,"xyDCASignificance"+tag,c)),float(value(star_,"Eta"+end,c)),float(value(star_,"Phi"+end,c)),int(value(star_,"NHit"+end,c))};
    o.leaves.push_back(x);
   }
  }
  return o;
 }
};
}

