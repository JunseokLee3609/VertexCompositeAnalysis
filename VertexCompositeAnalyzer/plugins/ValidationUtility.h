#ifndef ValidationUtility_h
#define ValidationUtility_h

#include <vector>
#include <string>
#include <iostream>
#include <algorithm>
#include <numeric>
#include <cmath>
#include <exception>

// Forward declarations
namespace reco {
    class GenParticle;
    class Candidate;
}

namespace pat {
    class CompositeCandidate;
}

#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "DataFormats/HepMCCandidate/interface/GenParticle.h"
#include "DataFormats/PatCandidates/interface/CompositeCandidate.h"

/**
 * @class ValidationUtility
 * @brief Comprehensive validation and debugging utility for PATCompositeTreeProducer
 * 
 * This utility class provides extensive validation, debugging, and testing capabilities
 * for the improved PATCompositeTreeProducer code. It includes:
 * - Permutation logic testing
 * - Physics constants validation
 * - Memory safety checks
 * - Performance benchmarking
 * - Decay chain debugging
 */
class ValidationUtility {
public:
    // Constructor
    ValidationUtility();
    
    // Destructor
    ~ValidationUtility();
    
    // Main validation methods
    static bool validateCodeIntegrity();
    static bool validatePhysicsConstants();
    static bool testPermutationLogic();
    static bool testArrayBounds(int maxCandidates);
    
    // Debugging methods
    static void debugPrintDecayChain(const reco::GenParticle& particle, int level = 0);
    static void logSystemConfiguration(const std::string& analyzerName);
    static void printValidationSummary();
    
    // Performance testing
    static double benchmarkPermutations(int iterations = 1000);
    static void testMemoryUsage();
    
    // Specific physics validation
    static bool validatePDGIds();
    static bool validateKinematicCuts();
    static bool testDivisionSafety();
    
    // Error simulation for testing
    static void simulateErrorConditions();
    
    // Comprehensive self-test
    static bool performComprehensiveTest();
    
private:
    // Internal helper methods
    static bool testBasicPermutations();
    static bool testTwoLayerPermutations();
    static bool testThreeProngPermutations();
    static void logTestResult(const std::string& testName, bool result);
    static std::string formatDecayChain(const reco::GenParticle& particle, int level);
    
    // Test counters
    static int totalTests_;
    static int passedTests_;
    static int failedTests_;
    
    // Constants for testing
    static constexpr double EPSILON = 1e-10;
    static constexpr int MAX_RECURSION_DEPTH = 10;
    static constexpr int TEST_TIMEOUT_MS = 5000;
};

/**
 * @class MockGenParticle
 * @brief Mock class for testing permutation logic without real MC data
 */
class MockGenParticle {
public:
    MockGenParticle(int pdgId, double pt = 1.0, double eta = 0.0);
    
    void addDaughter(std::shared_ptr<MockGenParticle> daughter);
    int pdgId() const { return pdgId_; }
    double pt() const { return pt_; }
    double eta() const { return eta_; }
    size_t numberOfDaughters() const { return daughters_.size(); }
    
    const MockGenParticle* daughter(size_t index) const {
        return (index < daughters_.size()) ? daughters_[index].get() : nullptr;
    }
    
private:
    int pdgId_;
    double pt_;
    double eta_;
    std::vector<std::shared_ptr<MockGenParticle>> daughters_;
};

/**
 * @class TestScenarioGenerator
 * @brief Generates various test scenarios for validation
 */
class TestScenarioGenerator {
public:
    // Generate test decay chains
    static std::shared_ptr<MockGenParticle> createDStarDecay();      // D* -> D0 + pi
    static std::shared_ptr<MockGenParticle> createD0Decay();         // D0 -> K + pi  
    static std::shared_ptr<MockGenParticle> createDPlusDecay();      // D+ -> K + pi + pi
    static std::shared_ptr<MockGenParticle> createTwoLayerDecay();   // D* -> (D0 -> K + pi) + pi
    static std::shared_ptr<MockGenParticle> createCorruptedDecay();  // Invalid decay for error testing
    
    // Generate edge cases
    static std::vector<std::shared_ptr<MockGenParticle>> generateEdgeCases();
};

// Macro for easy testing
#define VALIDATE_CONDITION(condition, message) \
    do { \
        if (!(condition)) { \
            edm::LogError("ValidationUtility") << "VALIDATION FAILED: " << message; \
            return false; \
        } else { \
            edm::LogInfo("ValidationUtility") << "VALIDATION PASSED: " << message; \
        } \
    } while(0)

#endif // ValidationUtility_h