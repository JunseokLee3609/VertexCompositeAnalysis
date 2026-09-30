#ifndef VertexCompositeAnalysis_VertexCompositeProducer_FitDiagnostics_h
#define VertexCompositeAnalysis_VertexCompositeProducer_FitDiagnostics_h

#include <array>
#include <cmath>
#include <string>

#include "DataFormats/PatCandidates/interface/CompositeCandidate.h"
#include "RecoVertex/KinematicFitPrimitives/interface/KinematicState.h"
#include "TrackingTools/TrajectoryState/interface/FreeTrajectoryState.h"

namespace vca::fitdiag {
using P4 = reco::Particle::LorentzVector;
using CovP4 = ROOT::Math::SMatrix<double, 4, 4, ROOT::Math::MatRepSym<double, 4>>;
using CrossP4 = ROOT::Math::SMatrix<double, 4, 4>;
using MassCross = ROOT::Math::SVector<double, 2>;
inline constexpr std::array<const char*, 6> stateNames = {{
    "DstarBefore", "DstarAfter", "D0Before", "D0After", "SlowPiBefore", "SlowPiAfter"}};

inline P4 p4(const KinematicState& state) {
  const auto& p = state.kinematicParameters();
  return P4(p(3), p(4), p(5), std::sqrt(p(3)*p(3) + p(4)*p(4) + p(5)*p(5) + p(6)*p(6)));
}

// Native state order: (x,y,z,px,py,pz,m); diagnostic order: (px,py,pz,E).
inline ROOT::Math::SMatrix<double, 4, 7> jacobian(const P4& p) {
  ROOT::Math::SMatrix<double, 4, 7> j;
  j(0,3) = j(1,4) = j(2,5) = 1.;
  j(3,3) = p.px()/p.energy();
  j(3,4) = p.py()/p.energy();
  j(3,5) = p.pz()/p.energy();
  j(3,6) = p.mass()/p.energy();
  return j;
}

inline CovP4 covariance(const KinematicState& state, const P4& p) {
  return ROOT::Math::Similarity(jacobian(p), state.kinematicParametersError().matrix());
}

inline CovP4 covariance(const FreeTrajectoryState& state, const P4& p, double massSigma) {
  AlgebraicSymMatrix77 c;
  const auto trackCov = state.cartesianError().matrix();
  for (unsigned int i = 0; i < 6; ++i)
    for (unsigned int j = i; j < 6; ++j) c(i,j) = trackCov(i,j);
  c(6,6) = massSigma*massSigma;
  return ROOT::Math::Similarity(jacobian(p), c);
}

// The native Kalman smoother correlates distinct inputs through the common vertex:
// C_ij = C_i(momentum,vertex) * C_vertex^-1 * C_j(vertex,momentum).
// This applies to the unconstrained KinematicParticleVertexFitter used here.
inline CrossP4 fittedCrossCovariance(const KinematicState& a, const KinematicState& b) {
  const auto& ca = a.kinematicParametersError().matrix();
  const auto& cb = b.kinematicParametersError().matrix();
  AlgebraicSymMatrix33 vertexCov;
  ROOT::Math::SMatrix<double, 7, 3> av;
  ROOT::Math::SMatrix<double, 3, 7> vb;
  for (unsigned int i = 0; i < 3; ++i) {
    for (unsigned int j = i; j < 3; ++j) vertexCov(i,j) = ca(i,j);
    for (unsigned int j = 0; j < 7; ++j) {
      av(j,i) = ca(j,i);
      vb(i,j) = cb(i,j);
    }
  }
  int inversionStatus = 0;
  const auto vertexInverse = vertexCov.Inverse(inversionStatus);
  return jacobian(p4(a)) * av * vertexInverse * vb * ROOT::Math::Transpose(jacobian(p4(b)));
}

inline ROOT::Math::SVector<double, 4> massGradient(const P4& p) {
  return {-p.px()/p.mass(), -p.py()/p.mass(), -p.pz()/p.mass(), p.energy()/p.mass()};
}

// Cov(M(D*),M(D0)), with C_Dstar,D0 = C_D0 + C_slow,D0.
inline double massCrossCovariance(const P4& parent, const P4& d0, const CovP4& d0Cov,
                                 const CrossP4& d0SlowCross) {
  return ROOT::Math::Dot(massGradient(parent),
                        (d0Cov + ROOT::Math::Transpose(d0SlowCross)) * massGradient(d0));
}

inline double ptError(const P4& p, const CovP4& c) {
  return std::sqrt((p.px()*p.px()*c(0,0) + 2.*p.px()*p.py()*c(0,1) + p.py()*p.py()*c(1,1))/(p.pt()*p.pt()));
}

inline void storeState(pat::CompositeCandidate& candidate, const char* name, const P4& p, const CovP4& c) {
  candidate.addUserData(std::string("diag") + name + "P4", p);
  candidate.addUserData(std::string("diag") + name + "CovP4", c);
}
}  // namespace vca::fitdiag

#endif
