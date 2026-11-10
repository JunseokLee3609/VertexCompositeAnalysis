#include <vector>
#include <unordered_map>
#include <tuple>
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include "DataFormats/PatCandidates/interface/PackedCandidate.h"
#include "DataFormats/TrackReco/interface/Track.h"
#include "DataFormats/VertexReco/interface/Vertex.h"
#include "DataFormats/BeamSpot/interface/BeamSpot.h"
struct FMResult {
 std::vector<int> genGroup,swap,missing,ambiguous,rawIndex,duplicate;
 unsigned long tracks=0,pairs=0,recovered=0;
};
namespace fullmatch {
struct T {double pt,eta,phi,err,dzsig,dxysig;int charge,hits;};
inline double dr2(double e,double p,const T&t) {
 double dp=std::remainder(p-t.phi,2*std::acos(-1.));
 return (e-t.eta)*(e-t.eta)+dp*dp;
}
inline uint64_t key(int ch,double err) {
 float f=err;uint32_t bits;std::memcpy(&bits,&f,4);
 return (uint64_t(bits)<<32)|uint32_t(ch+8);
}
}
FMResult fullTrackMatch(
 const std::vector<pat::PackedCandidate>&pf,const std::vector<pat::PackedCandidate>&lost,
 const std::vector<pat::PackedCandidate>&ele,const std::vector<reco::Vertex>&vertices,const reco::BeamSpot&bs,
 const std::vector<double>&gp,const std::vector<double>&ge,const std::vector<double>&gf,const std::vector<int>&gc,
 const std::vector<double>&err,const std::vector<int>&rc,const std::vector<int>&hits,
 const std::vector<double>&dzsig,const std::vector<double>&dxysig,const std::vector<int>&kaonPos,
 bool genDenominator) {
 using namespace fullmatch;
 std::vector<reco::Track>raw;
 unsigned pvAdded=0,recovered=0;
 auto add=[&](const auto&objects) {
  for(const auto&p:objects) {
   if(p.hasTrackDetails()) {
    const auto&t=p.pseudoTrack();
    raw.emplace_back(t.chi2(),t.ndof()!=0?t.ndof():.1,t.referencePoint(),t.momentum(),t.charge(),t.covariance());
    const_cast<reco::HitPattern&>(raw.back().hitPattern())=t.hitPattern();
   } else if(p.covarianceVersion()>0) {
    math::RhoEtaPhiVector p3(p.ptTrk(),p.etaAtVtx(),p.phiAtVtx());
    raw.emplace_back(.1,.1,p.vertex(),math::XYZVector(p3.x(),p3.y(),p3.z()),p.charge(),
      reco::TrackBase::CovarianceMatrix(ROOT::Math::SMatrixIdentity())*1.E12);
    ++recovered;
    if(p.lostInnerHits()==pat::PackedCandidate::validHitInFirstPixelBarrelLayer)
     raw.back().appendTrackerHitPattern(PixelSubdetector::PixelBarrel,1,0,TrackingRecHit::valid);
   } else continue;
   if(!vertices.empty()&&p.vertexRef().isNonnull()&&p.vertexRef().key()==0
      &&p.pvAssociationQuality()>pat::PackedCandidate::UsedInFitLoose)++pvAdded;
  }
 };
 add(pf);add(lost);add(ele);
 bool usePV=!vertices.empty()&&!(vertices[0].isFake()&&pvAdded==0)
    &&vertices[0].tracksSize()+pvAdded>=2;
 math::XYZPoint point(usePV?vertices[0].x():bs.position().x(),usePV?vertices[0].y():bs.position().y(),
    usePV?vertices[0].z():0.);
 auto covariance=usePV?vertices[0].covariance():bs.rotatedCovariance3D();
 double ze=usePV?vertices[0].zError():0.;
 std::vector<T>tracks;
 std::unordered_map<uint64_t,std::vector<int>>lookup;
 for(const auto&t:raw) {
  int i=tracks.size();
  double dz=t.dz(point)/std::sqrt(t.dzError()*t.dzError()+ze*ze);
  double xy=t.dxy(point)/t.dxyError(point,covariance);
  tracks.push_back({t.pt(),t.eta(),t.phi(),t.ptError(),dz,xy,int(t.charge()),int(t.numberOfValidHits())});
  lookup[key(t.charge(),t.ptError())].push_back(i);
 }
 std::vector<std::tuple<double,int,int>>pairs;
 for(unsigned g=0;g<gp.size();++g)for(unsigned i=0;i<tracks.size();++i) {
  const auto&t=tracks[i];
  if(gc[g]!=t.charge||!(t.pt>0)||!(gp[g]>0))continue;
  double d=dr2(ge[g],gf[g],t);
  double rel=std::abs(t.pt-gp[g])/(genDenominator?gp[g]:std::sqrt(t.pt*gp[g]));
  if(d<.03*.03&&rel<.5)pairs.emplace_back(d,i,g);
 }
 std::sort(pairs.begin(),pairs.end());
 std::vector<int>assigned(gp.size(),-1);std::vector<bool>used(tracks.size(),false);
 for(const auto&p:pairs) {
  int i=std::get<1>(p),g=std::get<2>(p);
  if(used[i]||assigned[g]>=0)continue;
  used[i]=true;assigned[g]=i;
 }
 FMResult out;out.tracks=tracks.size();out.pairs=pairs.size();out.recovered=recovered;
 unsigned n=kaonPos.size();
 out.genGroup.assign(n,-1);out.swap.assign(n,0);out.missing.assign(n,0);out.ambiguous.assign(n,0);
 out.rawIndex.assign(3*n,-1);out.duplicate.assign(n,0);
 for(unsigned c=0;c<n;++c) {
  int ids[3]={-1,-1,-1};
  for(unsigned d=0;d<3;++d) {
   unsigned z=3*c+d;auto it=lookup.find(key(rc[z],err[z]));
   if(it==lookup.end()){out.missing[c]|=1<<d;continue;}
   std::vector<int>same;
   for(int i:it->second)if(tracks[i].hits==hits[z])same.push_back(i);
   if(same.size()>1) {
    std::vector<int>exact;
    for(int i:same)if(float(tracks[i].dzsig)==float(dzsig[z])&&float(tracks[i].dxysig)==float(dxysig[z]))exact.push_back(i);
    if(exact.empty()) {
     for(int i:same)if(float(tracks[i].dxysig)==float(dxysig[z]))exact.push_back(i);
    }
    same.swap(exact);
   }
   if(same.empty()){out.missing[c]|=1<<d;continue;}
   if(same.size()>1){out.ambiguous[c]|=1<<d;continue;}
   ids[d]=same[0];out.rawIndex[z]=ids[d];
  }
  if(out.missing[c]||out.ambiguous[c])continue;
  auto compatible=[&](int i,unsigned g) {
   const auto&t=tracks[i];
   return t.charge==gc[g]&&t.pt>0&&gp[g]>0&&dr2(ge[g],gf[g],t)<.03*.03
    &&std::abs(t.pt-gp[g])/(genDenominator?gp[g]:std::sqrt(t.pt*gp[g]))<.5;
  };
  for(unsigned g=0;g+2<assigned.size();g+=3) {
   if(compatible(ids[2],g+2)&&((compatible(ids[0],g)&&compatible(ids[1],g+1))
      ||(compatible(ids[1],g)&&compatible(ids[0],g+1))))out.duplicate[c]=1;
   int k=assigned[g],pi=assigned[g+1],s=assigned[g+2];
   if(k<0||pi<0||s<0)continue;
   if(ids[2]==s&&((ids[0]==k&&ids[1]==pi)||(ids[1]==k&&ids[0]==pi))) {
    out.genGroup[c]=g/3;out.swap[c]=(ids[kaonPos[c]?0:1]!=k);out.duplicate[c]=0;break;
   }
  }
 }
 return out;
}
