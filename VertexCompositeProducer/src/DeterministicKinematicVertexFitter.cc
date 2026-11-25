#include "VertexCompositeAnalysis/VertexCompositeProducer/interface/DeterministicKinematicVertexFitter.h"

#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "RecoVertex/KalmanVertexFit/interface/KalmanSmoothedVertexChi2Estimator.h"
#include "RecoVertex/KalmanVertexFit/interface/KalmanVertexTrackUpdator.h"
#include "RecoVertex/KalmanVertexFit/interface/KalmanVertexUpdator.h"
#include "RecoVertex/KinematicFit/interface/FinalTreeBuilder.h"
#include "RecoVertex/KinematicFit/interface/InputSort.h"
#include "RecoVertex/KinematicFitPrimitives/interface/KinematicVertexFactory.h"
#include "RecoVertex/KinematicFitPrimitives/interface/ParticleKinematicLinearizedTrackStateFactory.h"
#include "RecoVertex/KinematicFitPrimitives/interface/TrackKinematicStatePropagator.h"
#include "RecoVertex/VertexPrimitives/interface/TrackToTrackCovCalculator.h"
#include "RecoVertex/VertexPrimitives/interface/VertexException.h"
#include "RecoVertex/VertexTools/interface/SequentialVertexSmoother.h"

namespace {
  // CMSSW_13_2_11 KalmanTrackToTrackCovCalculator<6> with a fixed arithmetic order.
  // CachingVertex still expects its covariance map indexed by pointer order.
  class OrderedTrackCovariance final : public TrackToTrackCovCalculator<6> {
  public:
    OrderedTrackCovariance* clone() const override { return new OrderedTrackCovariance(*this); }

    CachingVertex<6>::TrackToTrackMap operator()(const CachingVertex<6>& vertex) const override {
      using Matrix43 = ROOT::Math::SMatrix<double, 4, 3>;
      using Matrix34 = ROOT::Math::SMatrix<double, 3, 4>;
      using Matrix44 = ROOT::Math::SMatrix<double, 4, 4>;
      using Sym44 = ROOT::Math::SMatrix<double, 4, 4, ROOT::Math::MatRepSym<double, 4>>;
      using Sym66 = ROOT::Math::SMatrix<double, 6, 6, ROOT::Math::MatRepSym<double, 6>>;

      CachingVertex<6>::TrackToTrackMap covariances;
      const auto& tracks = vertex.tracksRef();
      const AlgebraicSymMatrix33 vertexCovariance = vertex.error().matrix();
      int failure = 0;
      for (auto left = tracks.begin(); left != tracks.end(); ++left) {
        const auto& leftA = (*left)->linearizedTrack()->positionJacobian();
        const auto& leftB = (*left)->linearizedTrack()->momentumJacobian();
        const Sym66 leftG = (*left)->linearizedTrack()->predictedStateWeight(failure);
        Sym44 leftW = ROOT::Math::SimilarityT(leftB, leftG);
        failure = !leftW.Invert();
        if (failure != 0)
          throw VertexException("OrderedTrackCovariance: left matrix inversion failed");

        const Matrix43 leftPart = leftW * ROOT::Math::Transpose(leftB) * leftG * leftA;
        covariances.try_emplace(*left);
        for (auto right = left + 1; right != tracks.end(); ++right) {
          const auto& rightA = (*right)->linearizedTrack()->positionJacobian();
          const auto& rightB = (*right)->linearizedTrack()->momentumJacobian();
          const Sym66 rightG = (*right)->linearizedTrack()->predictedStateWeight(failure);
          Sym44 rightW = ROOT::Math::SimilarityT(rightB, rightG);
          failure = !rightW.Invert();
          if (failure != 0)
            throw VertexException("OrderedTrackCovariance: right matrix inversion failed");

          const Matrix34 rightPart = ROOT::Math::Transpose(rightA) * rightG * rightB * rightW;
          const Matrix44 covariance(leftPart * vertexCovariance * rightPart);
          if (*left < *right)
            covariances[*left][*right] = covariance;
          else
            covariances[*right][*left] = ROOT::Math::Transpose(covariance);
        }
      }
      return covariances;
    }
  };

  using VertexCut = std::function<bool(const RefCountedKinematicVertex&)>;
  class CutBeforeSmoothing final : public SequentialVertexSmoother<6> {
  public:
    explicit CutBeforeSmoothing(const VertexCut* const* activeCut)
        : SequentialVertexSmoother<6>(KalmanVertexTrackUpdator<6>(),
                                      KalmanSmoothedVertexChi2Estimator<6>(),
                                      OrderedTrackCovariance()), activeCut_(activeCut) {}
    CutBeforeSmoothing* clone() const override { return new CutBeforeSmoothing(*this); }
    CachingVertex<6> smooth(const CachingVertex<6>& vertex) const override {
      if (vertex.isValid() && *activeCut_ && !(**activeCut_)(KinematicVertexFactory().vertex(vertex)))
        return CachingVertex<6>();
      return SequentialVertexSmoother<6>::smooth(vertex);
    }
  private:
    const VertexCut* const* activeCut_;
  };
  struct ScopedVertexCut {
    const VertexCut*& current;
    const VertexCut* previous;
    ScopedVertexCut(const VertexCut*& slot, const VertexCut& cut) : current(slot), previous(slot) {
      current = cut ? &cut : nullptr;
    }
    ~ScopedVertexCut() { current = previous; }
  };
  SequentialVertexFitter<6> makeFitter(const LinearizationPointFinder& pointFinder, const VertexCut* const* activeCut) {
    edm::ParameterSet parameters;
    parameters.addParameter<double>("maxDistance", 0.01);
    parameters.addParameter<int>("maxNbrOfIterations", 100);
    const CutBeforeSmoothing smoother(activeCut);
    return SequentialVertexFitter<6>(
        parameters, pointFinder, KalmanVertexUpdator<6>(), smoother, ParticleKinematicLinearizedTrackStateFactory());
  }
}

vca::DeterministicKinematicVertexFitter::DeterministicKinematicVertexFitter() : fitter_(makeFitter(pointFinder_, &activeCut_)) {}

RefCountedKinematicTree vca::DeterministicKinematicVertexFitter::fit(
    const std::vector<RefCountedKinematicParticle>& particles,
    const std::function<bool(const RefCountedKinematicVertex&)>& acceptVertex) const {
  // Match KinematicParticleVertexFitter in CMSSW_13_2_11. Only the smoother's
  // track-to-track covariance calculator differs.
  if (particles.size() < 2)
    throw VertexException("DeterministicKinematicVertexFitter: fewer than two input particles");

  const auto [inputs, freeStates] = InputSort().sort(particles);
  const GlobalPoint point = pointFinder_.getLinearizationPoint(freeStates);
  AlgebraicSymMatrix33 seedError;
  seedError(0, 0) = seedError(1, 1) = seedError(2, 2) = 10000.;
  const VertexState seed(point, GlobalError(seedError));

  std::vector<ReferenceCountingPointer<VertexTrack<6>>> vertexTracks;
  const TrackKinematicStatePropagator propagator;
  for (const auto& particle : inputs) {
    const auto& state = particle->currentState();
    if (!state.isValid() || !propagator.willPropagateToTheTransversePCA(state, point))
      return RefCountedKinematicTree(new KinematicTree());
    vertexTracks.push_back(trackFactory_.vertexTrack(particle->particleLinearizedTrackState(point), seed, 1.));
  }

  const ScopedVertexCut scopedCut(activeCut_, acceptVertex);
  const CachingVertex<6> vertex = fitter_.vertex(vertexTracks);
  if (!vertex.isValid())
    return RefCountedKinematicTree(new KinematicTree());
  return FinalTreeBuilder().buildTree(vertex, inputs);
}
