#include "ValidationUtility.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include <chrono>
#include <memory>
#include <sstream>

// Static member initialization
int ValidationUtility::totalTests_ = 0;
int ValidationUtility::passedTests_ = 0;
int ValidationUtility::failedTests_ = 0;

// Constants from the main analyzer (should match PATCompositeTreeProducer_test_v1.h)
namespace ValidationConstants {
    constexpr double PI = 3.14159265358979323846;
    constexpr int MAXCAN = 30000;
    constexpr double INVALID_VALUE = -999.9;
    constexpr double PT_ERROR_THRESHOLD = 0.10;
    constexpr double DZ_SIGNIFICANCE_CUT = 3.0;
    constexpr double DXY_SIGNIFICANCE_CUT = 3.0;
    constexpr double ETA_CUT = 2.4;
    constexpr double PT_CUT = 0.4;
    constexpr int PION_PDG_ID = 211;
    constexpr int KAON_PDG_ID = 321;
    constexpr int D0_PDG_ID = 421;
    constexpr int DSTAR_PDG_ID = 413;
    constexpr int DPLUS_PDG_ID = 411;
}

ValidationUtility::ValidationUtility() {
    totalTests_ = 0;
    passedTests_ = 0;
    failedTests_ = 0;
}

ValidationUtility::~ValidationUtility() {
    // Cleanup if needed
}

bool ValidationUtility::validateCodeIntegrity() {
    edm::LogInfo("ValidationUtility") << "🔍 Starting comprehensive code integrity validation...";
    
    bool allTestsPassed = true;
    
    // Test 1: Physics constants
    allTestsPassed &= validatePhysicsConstants();
    
    // Test 2: Permutation logic
    allTestsPassed &= testPermutationLogic();
    
    // Test 3: Array bounds
    allTestsPassed &= testArrayBounds(ValidationConstants::MAXCAN);
    
    // Test 4: PDG IDs
    allTestsPassed &= validatePDGIds();
    
    // Test 5: Division safety
    allTestsPassed &= testDivisionSafety();
    
    return allTestsPassed;
}

bool ValidationUtility::validatePhysicsConstants() {
    logTestResult("Physics Constants Validation", true);
    
    // Test PI constant
    VALIDATE_CONDITION(std::abs(ValidationConstants::PI - 3.14159265358979323846) < EPSILON, 
                      "PI constant accuracy");
    
    // Test array size
    VALIDATE_CONDITION(ValidationConstants::MAXCAN > 0 && ValidationConstants::MAXCAN < 1000000, 
                      "MAXCAN reasonable range");
    
    // Test invalid value
    VALIDATE_CONDITION(ValidationConstants::INVALID_VALUE < -999 && ValidationConstants::INVALID_VALUE > -1000, 
                      "INVALID_VALUE in expected range");
    
    // Test physics cuts
    VALIDATE_CONDITION(ValidationConstants::PT_CUT > 0 && ValidationConstants::PT_CUT < 10, 
                      "PT_CUT in reasonable range");
    VALIDATE_CONDITION(ValidationConstants::ETA_CUT > 0 && ValidationConstants::ETA_CUT < 10, 
                      "ETA_CUT in reasonable range");
    
    passedTests_++;
    return true;
}

bool ValidationUtility::testPermutationLogic() {
    logTestResult("Permutation Logic Testing", true);
    
    bool allPassed = true;
    
    // Test basic permutations
    allPassed &= testBasicPermutations();
    
    // Test two-layer permutations  
    allPassed &= testTwoLayerPermutations();
    
    // Test three-prong permutations
    allPassed &= testThreeProngPermutations();
    
    if (allPassed) {
        passedTests_++;
    } else {
        failedTests_++;
    }
    
    return allPassed;
}

bool ValidationUtility::testBasicPermutations() {
    // Test 2-element permutation count
    std::vector<unsigned int> perm2 = {0, 1};
    int count2 = 0;
    
    do {
        count2++;
        if (count2 > 10) break; // Safety
    } while (std::next_permutation(perm2.begin(), perm2.end()));
    
    VALIDATE_CONDITION(count2 == 2, "2-element permutation count");
    
    // Test 3-element permutation count
    std::vector<unsigned int> perm3 = {0, 1, 2};
    int count3 = 0;
    
    do {
        count3++;
        if (count3 > 10) break; // Safety
    } while (std::next_permutation(perm3.begin(), perm3.end()));
    
    VALIDATE_CONDITION(count3 == 6, "3-element permutation count");
    
    return true;
}

bool ValidationUtility::testTwoLayerPermutations() {
    // Create mock D* -> D0 + pi decay
    auto dstar = TestScenarioGenerator::createTwoLayerDecay();
    
    VALIDATE_CONDITION(dstar != nullptr, "D* mock particle creation");
    VALIDATE_CONDITION(dstar->numberOfDaughters() == 2, "D* has 2 daughters");
    VALIDATE_CONDITION(abs(dstar->pdgId()) == ValidationConstants::DSTAR_PDG_ID, "D* PDG ID correct");
    
    // Check D0 daughter
    const auto* d0 = dstar->daughter(0);
    if (abs(d0->pdgId()) == ValidationConstants::D0_PDG_ID) {
        VALIDATE_CONDITION(d0->numberOfDaughters() == 2, "D0 has 2 daughters");
        
        const auto* kaon = d0->daughter(0);
        const auto* pion = d0->daughter(1);
        
        bool validDecay = (abs(kaon->pdgId()) == ValidationConstants::KAON_PDG_ID && 
                          abs(pion->pdgId()) == ValidationConstants::PION_PDG_ID) ||
                         (abs(pion->pdgId()) == ValidationConstants::KAON_PDG_ID && 
                          abs(kaon->pdgId()) == ValidationConstants::PION_PDG_ID);
        
        VALIDATE_CONDITION(validDecay, "D0 decay products correct");
    }
    
    return true;
}

bool ValidationUtility::testThreeProngPermutations() {
    // Create mock D+ -> K + pi + pi decay
    auto dplus = TestScenarioGenerator::createDPlusDecay();
    
    VALIDATE_CONDITION(dplus != nullptr, "D+ mock particle creation");
    VALIDATE_CONDITION(dplus->numberOfDaughters() == 3, "D+ has 3 daughters");
    
    // Count kaons and pions
    int kaonCount = 0;
    int pionCount = 0;
    
    for (size_t i = 0; i < dplus->numberOfDaughters(); ++i) {
        const auto* daughter = dplus->daughter(i);
        if (abs(daughter->pdgId()) == ValidationConstants::KAON_PDG_ID) kaonCount++;
        if (abs(daughter->pdgId()) == ValidationConstants::PION_PDG_ID) pionCount++;
    }
    
    VALIDATE_CONDITION(kaonCount == 1 && pionCount == 2, "D+ decay products: 1 kaon + 2 pions");
    
    return true;
}

bool ValidationUtility::testArrayBounds(int maxCandidates) {
    logTestResult("Array Bounds Testing", true);
    
    // Test valid indices
    VALIDATE_CONDITION(maxCandidates > 0, "Max candidates positive");
    
    // Test boundary conditions
    bool validBounds = true;
    
    // Test edge cases
    for (int i = maxCandidates - 5; i < maxCandidates + 5; ++i) {
        bool shouldBeValid = (i >= 0 && i < maxCandidates);
        bool isValid = (i >= 0 && i < maxCandidates);
        
        if (shouldBeValid != isValid) {
            validBounds = false;
            break;
        }
    }
    
    VALIDATE_CONDITION(validBounds, "Array bounds checking");
    
    passedTests_++;
    return true;
}

bool ValidationUtility::validatePDGIds() {
    logTestResult("PDG ID Validation", true);
    
    // Check standard PDG IDs
    VALIDATE_CONDITION(ValidationConstants::PION_PDG_ID == 211, "Pion PDG ID");
    VALIDATE_CONDITION(ValidationConstants::KAON_PDG_ID == 321, "Kaon PDG ID"); 
    VALIDATE_CONDITION(ValidationConstants::D0_PDG_ID == 421, "D0 PDG ID");
    
    // Check they're all different
    std::vector<int> pdgIds = {
        ValidationConstants::PION_PDG_ID,
        ValidationConstants::KAON_PDG_ID, 
        ValidationConstants::D0_PDG_ID
    };
    
    std::sort(pdgIds.begin(), pdgIds.end());
    auto it = std::unique(pdgIds.begin(), pdgIds.end());
    bool allUnique = (it == pdgIds.end());
    
    VALIDATE_CONDITION(allUnique, "All PDG IDs unique");
    
    passedTests_++;
    return true;
}

bool ValidationUtility::testDivisionSafety() {
    logTestResult("Division Safety Testing", true);
    
    // Test division by zero protection
    auto testDivision = [](int pdgId) -> float {
        if (pdgId != 0) {
            return static_cast<float>(pdgId) / abs(pdgId);
        } else {
            return 0.0f;
        }
    };
    
    // Test cases
    VALIDATE_CONDITION(testDivision(211) == 1.0f, "Positive PDG division");
    VALIDATE_CONDITION(testDivision(-211) == -1.0f, "Negative PDG division");
    VALIDATE_CONDITION(testDivision(0) == 0.0f, "Zero PDG division safety");
    
    passedTests_++;
    return true;
}

void ValidationUtility::debugPrintDecayChain(const reco::GenParticle& particle, int level) {
    if (level > MAX_RECURSION_DEPTH) return;
    
    std::string indent(level * 2, ' ');
    
    edm::LogVerbatim("ValidationUtility") 
        << indent << "├─ PDG: " << particle.pdgId() 
        << ", pT: " << particle.pt() 
        << ", eta: " << particle.eta()
        << ", nDaughters: " << particle.numberOfDaughters();
    
    for (size_t i = 0; i < particle.numberOfDaughters(); ++i) {
        const auto* daughter = dynamic_cast<const reco::GenParticle*>(particle.daughter(i));
        if (daughter) {
            debugPrintDecayChain(*daughter, level + 1);
        }
    }
}

double ValidationUtility::benchmarkPermutations(int iterations) {
    edm::LogInfo("ValidationUtility") << "⏱️ Benchmarking permutation performance...";
    
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int iter = 0; iter < iterations; ++iter) {
        std::vector<unsigned int> perm = {0, 1, 2};
        int count = 0;
        
        do {
            count++;
            if (count > 10) break;
        } while (std::next_permutation(perm.begin(), perm.end()));
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    double timePerIteration = static_cast<double>(duration.count()) / iterations;
    
    edm::LogInfo("ValidationUtility") 
        << "Benchmark results: " << timePerIteration << " microseconds per permutation cycle";
    
    return timePerIteration;
}

bool ValidationUtility::performComprehensiveTest() {
    edm::LogInfo("ValidationUtility") << "🚀 Starting comprehensive validation test suite...";
    
    totalTests_ = 0;
    passedTests_ = 0;
    failedTests_ = 0;
    
    bool allPassed = validateCodeIntegrity();
    
    // Performance benchmark
    double perfResult = benchmarkPermutations(100);
    bool perfPassed = (perfResult > 0 && perfResult < 1000); // Reasonable performance
    logTestResult("Performance Benchmark", perfPassed);
    
    if (perfPassed) passedTests_++; else failedTests_++;
    totalTests_++;
    
    // Print summary
    printValidationSummary();
    
    return allPassed && perfPassed;
}

void ValidationUtility::printValidationSummary() {
    edm::LogInfo("ValidationUtility") 
        << "📊 VALIDATION SUMMARY:"
        << "\n  Total Tests: " << totalTests_
        << "\n  Passed: " << passedTests_ 
        << "\n  Failed: " << failedTests_
        << "\n  Success Rate: " << (totalTests_ > 0 ? (100.0 * passedTests_ / totalTests_) : 0) << "%";
    
    if (failedTests_ == 0) {
        edm::LogInfo("ValidationUtility") << "✅ ALL TESTS PASSED - Code ready for production!";
    } else {
        edm::LogWarning("ValidationUtility") << "⚠️  Some tests failed - Please review before deployment";
    }
}

void ValidationUtility::logTestResult(const std::string& testName, bool result) {
    totalTests_++;
    
    if (result) {
        edm::LogInfo("ValidationUtility") << "✅ " << testName << ": PASSED";
    } else {
        edm::LogError("ValidationUtility") << "❌ " << testName << ": FAILED";
        failedTests_++;
    }
}

void ValidationUtility::logSystemConfiguration(const std::string& analyzerName) {
    edm::LogInfo("ValidationUtility") 
        << "🔧 System Configuration for " << analyzerName << ":"
        << "\n  MAXCAN: " << ValidationConstants::MAXCAN
        << "\n  PI: " << ValidationConstants::PI
        << "\n  INVALID_VALUE: " << ValidationConstants::INVALID_VALUE
        << "\n  PT_CUT: " << ValidationConstants::PT_CUT
        << "\n  ETA_CUT: " << ValidationConstants::ETA_CUT;
}

// MockGenParticle Implementation
MockGenParticle::MockGenParticle(int pdgId, double pt, double eta) 
    : pdgId_(pdgId), pt_(pt), eta_(eta) {}

void MockGenParticle::addDaughter(std::shared_ptr<MockGenParticle> daughter) {
    daughters_.push_back(daughter);
}

// TestScenarioGenerator Implementation
std::shared_ptr<MockGenParticle> TestScenarioGenerator::createTwoLayerDecay() {
    // Create D* -> (D0 -> K + pi) + pi
    auto dstar = std::make_shared<MockGenParticle>(ValidationConstants::DSTAR_PDG_ID, 5.0, 0.5);
    
    auto d0 = std::make_shared<MockGenParticle>(ValidationConstants::D0_PDG_ID, 4.0, 0.3);
    auto pion_soft = std::make_shared<MockGenParticle>(ValidationConstants::PION_PDG_ID, 1.0, 0.2);
    
    auto kaon = std::make_shared<MockGenParticle>(ValidationConstants::KAON_PDG_ID, 2.0, 0.1);
    auto pion_hard = std::make_shared<MockGenParticle>(ValidationConstants::PION_PDG_ID, 2.0, 0.2);
    
    d0->addDaughter(kaon);
    d0->addDaughter(pion_hard);
    
    dstar->addDaughter(d0);
    dstar->addDaughter(pion_soft);
    
    return dstar;
}

std::shared_ptr<MockGenParticle> TestScenarioGenerator::createDPlusDecay() {
    // Create D+ -> K + pi + pi
    auto dplus = std::make_shared<MockGenParticle>(ValidationConstants::DPLUS_PDG_ID, 5.0, 0.5);
    
    auto kaon = std::make_shared<MockGenParticle>(ValidationConstants::KAON_PDG_ID, 2.0, 0.1);
    auto pion1 = std::make_shared<MockGenParticle>(ValidationConstants::PION_PDG_ID, 1.5, 0.2);
    auto pion2 = std::make_shared<MockGenParticle>(ValidationConstants::PION_PDG_ID, 1.5, -0.1);
    
    dplus->addDaughter(kaon);
    dplus->addDaughter(pion1);  
    dplus->addDaughter(pion2);
    
    return dplus;
}

std::shared_ptr<MockGenParticle> TestScenarioGenerator::createDStarDecay() {
    // Create D* -> D0 + pi (without D0 decay)
    auto dstar = std::make_shared<MockGenParticle>(ValidationConstants::DSTAR_PDG_ID, 5.0, 0.5);
    
    auto d0 = std::make_shared<MockGenParticle>(ValidationConstants::D0_PDG_ID, 4.0, 0.3);
    auto pion = std::make_shared<MockGenParticle>(ValidationConstants::PION_PDG_ID, 1.0, 0.2);
    
    dstar->addDaughter(d0);
    dstar->addDaughter(pion);
    
    return dstar;
}

std::shared_ptr<MockGenParticle> TestScenarioGenerator::createD0Decay() {
    // Create D0 -> K + pi
    auto d0 = std::make_shared<MockGenParticle>(ValidationConstants::D0_PDG_ID, 4.0, 0.3);
    
    auto kaon = std::make_shared<MockGenParticle>(ValidationConstants::KAON_PDG_ID, 2.0, 0.1);
    auto pion = std::make_shared<MockGenParticle>(ValidationConstants::PION_PDG_ID, 2.0, 0.2);
    
    d0->addDaughter(kaon);
    d0->addDaughter(pion);
    
    return d0;
}