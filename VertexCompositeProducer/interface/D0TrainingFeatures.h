#ifndef VertexCompositeAnalysis_VertexCompositeProducer_D0TrainingFeatures_h
#define VertexCompositeAnalysis_VertexCompositeProducer_D0TrainingFeatures_h

#include <cmath>
#include <limits>
#include <TMath.h>

namespace d0training {
  inline float decayLengthSignificance(double length, double sigma) {
    const float missing = std::numeric_limits<float>::quiet_NaN();
    if (!std::isfinite(length) || !std::isfinite(sigma) || sigma <= 0.) return missing;
    const float value = length / sigma;
    return std::isfinite(value) ? value : missing;
  }

  inline float vertexProbability(double chi2, double ndof) {
    if (!std::isfinite(chi2) || !std::isfinite(ndof) || chi2 < 0. || ndof <= 0.)
      return std::numeric_limits<float>::quiet_NaN();
    return TMath::Prob(chi2, ndof);
  }
}

#endif
