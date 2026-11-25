#ifndef VertexCompositeAnalysis_VertexCompositeProducer_DeterministicKinematicVertexFitter_h
#define VertexCompositeAnalysis_VertexCompositeProducer_DeterministicKinematicVertexFitter_h

#include <functional>
#include <vector>

#include "RecoVertex/KinematicFitPrimitives/interface/RefCountedKinematicParticle.h"
#include "RecoVertex/KinematicFitPrimitives/interface/RefCountedKinematicTree.h"
#include "RecoVertex/KinematicFitPrimitives/interface/RefCountedKinematicVertex.h"
#include "RecoVertex/LinearizationPointFinders/interface/DefaultLinearizationPointFinder.h"
#include "RecoVertex/VertexTools/interface/VertexTrackFactory.h"
#include "RecoVertex/VertexTools/interface/SequentialVertexFitter.h"

namespace vca {
  class DeterministicKinematicVertexFitter {
  public:
    DeterministicKinematicVertexFitter();
    // Optionally select the fitted vertex before constructing the kinematic tree.
    RefCountedKinematicTree fit(
        const std::vector<RefCountedKinematicParticle>& particles,
        const std::function<bool(const RefCountedKinematicVertex&)>& acceptVertex = {}) const;

  private:
    DeterministicKinematicVertexFitter(const DeterministicKinematicVertexFitter&) = delete;
    DeterministicKinematicVertexFitter& operator=(const DeterministicKinematicVertexFitter&) = delete;
    mutable const std::function<bool(const RefCountedKinematicVertex&)>* activeCut_ = nullptr;
    DefaultLinearizationPointFinder pointFinder_;
    VertexTrackFactory<6> trackFactory_;
    SequentialVertexFitter<6> fitter_;
  };
}

#endif
