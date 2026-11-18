#ifndef VertexCompositeAnalysis_VertexCompositeProducer_DeterministicKinematicVertexFitter_h
#define VertexCompositeAnalysis_VertexCompositeProducer_DeterministicKinematicVertexFitter_h

#include <vector>

#include "RecoVertex/KinematicFitPrimitives/interface/RefCountedKinematicParticle.h"
#include "RecoVertex/KinematicFitPrimitives/interface/RefCountedKinematicTree.h"
#include "RecoVertex/LinearizationPointFinders/interface/DefaultLinearizationPointFinder.h"
#include "RecoVertex/VertexTools/interface/VertexTrackFactory.h"
#include "RecoVertex/VertexTools/interface/SequentialVertexFitter.h"

namespace vca {
  class DeterministicKinematicVertexFitter {
  public:
    DeterministicKinematicVertexFitter();
    RefCountedKinematicTree fit(const std::vector<RefCountedKinematicParticle>& particles) const;

  private:
    DefaultLinearizationPointFinder pointFinder_;
    VertexTrackFactory<6> trackFactory_;
    SequentialVertexFitter<6> fitter_;
  };
}

#endif
