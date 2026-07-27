#include <TCanvas.h>
#include <TClass.h>
#include <TDirectory.h>
#include <TFile.h>
#include <TH1F.h>
#include <TKey.h>
#include <TProfile.h>
#include <TString.h>
#include <TSystem.h>
#include <TTree.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace {

constexpr int kMaxCand = 4096;
constexpr int kMaxEpSize = 16;

std::string ToLower(std::string text) {
  std::transform(text.begin(), text.end(), text.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return text;
}

std::string NormalizePath(const std::string& path) {
  std::stringstream ss(path);
  std::string segment;
  std::vector<std::string> parts;
  while (std::getline(ss, segment, '/')) {
    if (segment.empty()) continue;
    const std::size_t semi = segment.find(';');
    if (semi != std::string::npos) segment = segment.substr(0, semi);
    if (!segment.empty()) parts.push_back(segment);
  }
  std::ostringstream out;
  for (std::size_t i = 0; i < parts.size(); ++i) {
    if (i) out << "/";
    out << parts[i];
  }
  return out.str();
}

void CollectTreePaths(TDirectory* dir, const std::string& prefix, std::vector<std::string>& out) {
  if (!dir) return;
  TIter nextKey(dir->GetListOfKeys());
  while (TKey* key = static_cast<TKey*>(nextKey())) {
    TClass* cls = TClass::GetClass(key->GetClassName());
    if (!cls) continue;
    const std::string name = key->GetName();
    if (cls->InheritsFrom(TDirectory::Class())) {
      auto* subdir = dynamic_cast<TDirectory*>(key->ReadObj());
      if (!subdir) continue;
      const std::string childPrefix = prefix.empty() ? name : (prefix + "/" + name);
      CollectTreePaths(subdir, childPrefix, out);
      delete subdir;
      continue;
    }
    if (cls->InheritsFrom(TTree::Class())) {
      const std::string fullPath = prefix.empty() ? name : (prefix + "/" + name);
      out.push_back(fullPath);
    }
  }
}

std::string ResolveTreePath(const std::vector<std::string>& available,
                            const std::string& preferred,
                            const std::vector<std::string>& keywords) {
  const std::string preferredNorm = NormalizePath(preferred);
  if (!preferredNorm.empty()) {
    for (const auto& cand : available) {
      if (NormalizePath(cand) == preferredNorm) return cand;
    }
    return "";
  }

  for (const auto& cand : available) {
    const std::string candLower = ToLower(NormalizePath(cand));
    bool allFound = true;
    for (const auto& key : keywords) {
      if (candLower.find(ToLower(key)) == std::string::npos) {
        allFound = false;
        break;
      }
    }
    if (allFound) return cand;
  }
  return "";
}

TTree* GetTree(TFile* file, const std::string& path) {
  if (!file || path.empty()) return nullptr;
  TObject* obj = file->Get(path.c_str());
  if (!obj || !obj->InheritsFrom(TTree::Class())) return nullptr;
  return static_cast<TTree*>(obj);
}

bool EnsureOutDir(const std::string& outdir) {
  if (outdir.empty()) {
    std::cerr << "ERROR: output directory string is empty.\n";
    return false;
  }
  if (!gSystem->AccessPathName(outdir.c_str())) return true;
  if (gSystem->mkdir(outdir.c_str(), kTRUE) != 0) {
    std::cerr << "ERROR: failed to create output directory: " << outdir << "\n";
    return false;
  }
  return true;
}

bool IsValidPsi2(const float psi, const double invalidAbs) {
  return std::isfinite(psi) && std::abs(psi) < invalidAbs;
}

struct SpeciesArtifacts {
  std::string label;
  std::unique_ptr<TH1F> hMass;
  std::unique_ptr<TH1F> hV2Value;
  std::unique_ptr<TProfile> pV2vsMass;
};

struct SpeciesSummary {
  bool ok = false;
  std::string label;
  std::string treePath;
  Long64_t events = 0;
  Long64_t candidates = 0;
  Long64_t candidatesWithValidEP = 0;
  Long64_t skippedOverflowEvents = 0;
  Long64_t skippedOverflowCandidates = 0;
  Long64_t missingEpEvents = 0;
  Long64_t invalidEpEvents = 0;
  Long64_t v2Count = 0;
  double v2Sum = 0.0;
  double v2SumSq = 0.0;
  double meanV2 = std::numeric_limits<double>::quiet_NaN();
  double meanV2Err = std::numeric_limits<double>::quiet_NaN();
};

SpeciesArtifacts MakeSpeciesArtifacts(const std::string& label,
                                      const int massBins,
                                      const double massMin,
                                      const double massMax,
                                      const int v2Bins) {
  SpeciesArtifacts a;
  a.label = label;
  a.hMass.reset(new TH1F(Form("hMass_%s", label.c_str()), Form("%s mass;mass [GeV];Counts", label.c_str()), massBins, massMin, massMax));
  a.hV2Value.reset(new TH1F(Form("hV2Value_%s", label.c_str()), Form("%s v2 values;cos(2*(#phi-#Psi_{2}));Counts", label.c_str()), v2Bins, -1.0, 1.0));
  a.pV2vsMass.reset(new TProfile(Form("pV2vsMass_%s", label.c_str()),
                                 Form("%s v2 vs mass;mass [GeV];<cos(2*(#phi-#Psi_{2}))>", label.c_str()),
                                 massBins,
                                 massMin,
                                 massMax));
  return a;
}

bool BuildDstarPsi2ByEvent(TTree* dstarTree,
                           const std::string& epBranch,
                           const int epIndex,
                           const double invalidEpAbs,
                           std::vector<float>& psi2ByEvent) {
  psi2ByEvent.clear();
  if (!dstarTree) return false;
  if (!dstarTree->GetBranch(epBranch.c_str())) return false;

  Float_t epValues[kMaxEpSize] = {0.0f};
  dstarTree->SetBranchStatus("*", 0);
  dstarTree->SetBranchStatus(epBranch.c_str(), 1);
  dstarTree->SetBranchAddress(epBranch.c_str(), epValues);

  const Long64_t nEntries = dstarTree->GetEntries();
  psi2ByEvent.reserve(static_cast<std::size_t>(nEntries));
  for (Long64_t i = 0; i < nEntries; ++i) {
    dstarTree->GetEntry(i);
    float psi2 = std::numeric_limits<float>::quiet_NaN();
    if (epIndex >= 0 && epIndex < kMaxEpSize) psi2 = epValues[epIndex];
    if (!IsValidPsi2(psi2, invalidEpAbs)) psi2 = std::numeric_limits<float>::quiet_NaN();
    psi2ByEvent.push_back(psi2);
  }
  return true;
}

SpeciesSummary ProcessSpecies(TTree* tree,
                              const std::string& label,
                              const std::string& treePath,
                              const std::string& epBranch,
                              const int epIndex,
                              const double invalidEpAbs,
                              const std::vector<float>* fallbackPsi2,
                              SpeciesArtifacts& artifacts) {
  SpeciesSummary summary;
  summary.label = label;
  summary.treePath = treePath;

  if (!tree) {
    std::cerr << "ERROR: [" << label << "] null tree pointer.\n";
    return summary;
  }

  TBranch* bCand = tree->GetBranch("candSize");
  TBranch* bMass = tree->GetBranch("mass");
  TBranch* bPhi = tree->GetBranch("phi");
  if (!bCand || !bMass || !bPhi) {
    std::cerr << "ERROR: [" << label << "] required branches missing in tree '" << treePath
              << "'. Need candSize, mass, phi.\n";
    return summary;
  }

  const bool hasEpInTree = (tree->GetBranch(epBranch.c_str()) != nullptr);
  if (!hasEpInTree && !fallbackPsi2) {
    std::cerr << "ERROR: [" << label << "] EP branch '" << epBranch
              << "' missing and no fallback EP vector provided.\n";
    return summary;
  }

  Int_t candSize = 0;
  Float_t mass[kMaxCand] = {0.0f};
  Float_t phi[kMaxCand] = {0.0f};
  Float_t epValues[kMaxEpSize] = {0.0f};

  tree->SetBranchStatus("*", 0);
  tree->SetBranchStatus("candSize", 1);
  tree->SetBranchStatus("mass", 1);
  tree->SetBranchStatus("phi", 1);
  if (hasEpInTree) tree->SetBranchStatus(epBranch.c_str(), 1);

  tree->SetBranchAddress("candSize", &candSize);
  tree->SetBranchAddress("mass", mass);
  tree->SetBranchAddress("phi", phi);
  if (hasEpInTree) tree->SetBranchAddress(epBranch.c_str(), epValues);

  const Long64_t nEntries = tree->GetEntries();
  for (Long64_t i = 0; i < nEntries; ++i) {
    tree->GetEntry(i);
    ++summary.events;

    if (candSize < 0) continue;
    summary.candidates += candSize;

    if (candSize > kMaxCand) {
      ++summary.skippedOverflowEvents;
      summary.skippedOverflowCandidates += candSize;
      continue;
    }

    float psi2 = std::numeric_limits<float>::quiet_NaN();
    if (hasEpInTree) {
      if (epIndex >= 0 && epIndex < kMaxEpSize) psi2 = epValues[epIndex];
    } else if (fallbackPsi2 && i < static_cast<Long64_t>(fallbackPsi2->size())) {
      psi2 = (*fallbackPsi2)[static_cast<std::size_t>(i)];
    }

    const bool validPsi = IsValidPsi2(psi2, invalidEpAbs);
    if (!validPsi) {
      if (std::isfinite(psi2)) ++summary.invalidEpEvents;
      else ++summary.missingEpEvents;
    }

    for (int j = 0; j < candSize; ++j) {
      artifacts.hMass->Fill(mass[j]);
      if (!validPsi) continue;
      const double v2 = std::cos(2.0 * (static_cast<double>(phi[j]) - static_cast<double>(psi2)));
      artifacts.hV2Value->Fill(v2);
      artifacts.pV2vsMass->Fill(mass[j], v2);
      summary.v2Sum += v2;
      summary.v2SumSq += v2 * v2;
      ++summary.v2Count;
      ++summary.candidatesWithValidEP;
    }
  }

  if (summary.v2Count > 0) {
    summary.meanV2 = summary.v2Sum / static_cast<double>(summary.v2Count);
    if (summary.v2Count > 1) {
      const double variance =
          (summary.v2SumSq - static_cast<double>(summary.v2Count) * summary.meanV2 * summary.meanV2) /
          static_cast<double>(summary.v2Count - 1);
      summary.meanV2Err = std::sqrt(std::max(0.0, variance) / static_cast<double>(summary.v2Count));
    } else {
      summary.meanV2Err = 0.0;
    }
  }

  summary.ok = true;
  return summary;
}

void SaveSpeciesPngs(const std::string& outdir, const SpeciesArtifacts& a) {
  TCanvas cMass(Form("cMass_%s", a.label.c_str()), "", 800, 600);
  a.hMass->SetLineWidth(2);
  a.hMass->Draw("hist");
  cMass.SaveAs((outdir + "/" + a.label + "_mass.png").c_str());

  TCanvas cV2(Form("cV2_%s", a.label.c_str()), "", 800, 600);
  a.hV2Value->SetLineWidth(2);
  a.hV2Value->Draw("hist");
  cV2.SaveAs((outdir + "/" + a.label + "_v2_values.png").c_str());

  TCanvas cProf(Form("cProf_%s", a.label.c_str()), "", 800, 600);
  a.pV2vsMass->SetMarkerStyle(20);
  a.pV2vsMass->SetMarkerSize(0.8);
  a.pV2vsMass->Draw();
  cProf.SaveAs((outdir + "/" + a.label + "_v2_vs_mass.png").c_str());
}

void PrintSummary(const SpeciesSummary& s) {
  std::cout << "[" << s.label << "]"
            << " tree=" << s.treePath
            << " events=" << s.events
            << " candidates=" << s.candidates
            << " withValidEP=" << s.candidatesWithValidEP
            << " overflowEvents=" << s.skippedOverflowEvents
            << " missingEPEvents=" << s.missingEpEvents
            << " invalidEPEvents=" << s.invalidEpEvents;
  if (s.v2Count > 0) {
    std::cout << std::fixed << std::setprecision(6)
              << " meanV2=" << s.meanV2
              << " meanV2Err=" << s.meanV2Err;
  } else {
    std::cout << " meanV2=nan meanV2Err=nan";
  }
  std::cout << "\n";
}

}  // namespace

void mass_v2_quickcheck(const char* input = "VertexCompositeProducer/d0ana_tree_step2.root",
                        const char* outdir = "VertexCompositeAnalyzer/test/DStar/mass_v2_quickcheck_output",
                        const char* d0Tree = "d0ana_newreduced/PATCompositeNtuple",
                        const char* dstarTree = "dStarana/PATCompositeNtuple",
                        const char* epBranch = "ephfAngle",
                        const int epIndex = 0,
                        const double invalidEpAbs = 10.0,
                        const int massBins = 120,
                        const int v2Bins = 80,
                        const double d0MassMin = 1.70,
                        const double d0MassMax = 2.00,
                        const double dstarMassMin = 1.95,
                        const double dstarMassMax = 2.10) {
  if (!input || std::string(input).empty()) {
    std::cerr << "ERROR: input path is empty.\n";
    return;
  }
  if (!outdir || std::string(outdir).empty()) {
    std::cerr << "ERROR: output directory is empty.\n";
    return;
  }
  if (epIndex < 0 || epIndex >= kMaxEpSize) {
    std::cerr << "ERROR: epIndex out of supported range [0," << (kMaxEpSize - 1) << "].\n";
    return;
  }
  if (!EnsureOutDir(outdir)) return;

  std::unique_ptr<TFile> inFile(TFile::Open(input, "READ"));
  if (!inFile || inFile->IsZombie()) {
    std::cerr << "ERROR: failed to open input ROOT file: " << input << "\n";
    return;
  }

  std::vector<std::string> trees;
  CollectTreePaths(inFile.get(), "", trees);
  std::sort(trees.begin(), trees.end());
  std::cout << "Detected TTrees:\n";
  for (const auto& t : trees) std::cout << "  - " << t << "\n";

  const std::string d0Resolved =
      ResolveTreePath(trees, d0Tree ? d0Tree : "", std::vector<std::string>{"d0", "patcompositentuple"});
  const std::string dstarResolved =
      ResolveTreePath(trees, dstarTree ? dstarTree : "", std::vector<std::string>{"dstar", "patcompositentuple"});

  if (d0Resolved.empty() && dstarResolved.empty()) {
    std::cerr << "ERROR: could not resolve either D0 or D* tree.\n";
    return;
  }

  TTree* d0 = d0Resolved.empty() ? nullptr : GetTree(inFile.get(), d0Resolved);
  TTree* dstar = dstarResolved.empty() ? nullptr : GetTree(inFile.get(), dstarResolved);
  if (!d0Resolved.empty() && !d0) std::cerr << "ERROR: resolved D0 path is not a valid TTree: " << d0Resolved << "\n";
  if (!dstarResolved.empty() && !dstar) std::cerr << "ERROR: resolved D* path is not a valid TTree: " << dstarResolved << "\n";
  if (!d0 && !dstar) return;

  std::cout << "[D0] " << (d0 ? ("using tree: " + d0Resolved) : "skipped/unresolved") << "\n";
  std::cout << "[DStar] " << (dstar ? ("using tree: " + dstarResolved) : "skipped/unresolved") << "\n";

  std::vector<float> dstarPsi2ByEvent;
  if (dstar) {
    const bool built = BuildDstarPsi2ByEvent(dstar, epBranch ? epBranch : "", epIndex, invalidEpAbs, dstarPsi2ByEvent);
    if (!built) {
      std::cerr << "ERROR: D* tree is available but EP branch '" << (epBranch ? epBranch : "") << "' is missing.\n";
      return;
    }
    std::cout << "[EP] fallback from D* tree with " << dstarPsi2ByEvent.size() << " events.\n";
  }

  SpeciesSummary d0Summary;
  SpeciesSummary dstarSummary;

  SpeciesArtifacts d0Artifacts = MakeSpeciesArtifacts("D0", massBins, d0MassMin, d0MassMax, v2Bins);
  SpeciesArtifacts dstarArtifacts = MakeSpeciesArtifacts("DStar", massBins, dstarMassMin, dstarMassMax, v2Bins);

  if (d0) {
    d0Summary = ProcessSpecies(
        d0, "D0", d0Resolved, epBranch ? epBranch : "", epIndex, invalidEpAbs, dstar ? &dstarPsi2ByEvent : nullptr, d0Artifacts);
    if (!d0Summary.ok) return;
  }
  if (dstar) {
    dstarSummary = ProcessSpecies(
        dstar, "DStar", dstarResolved, epBranch ? epBranch : "", epIndex, invalidEpAbs, nullptr, dstarArtifacts);
    if (!dstarSummary.ok) return;
  }

  const std::string outRootPath = std::string(outdir) + "/mass_v2_quickcheck.root";
  TFile outFile(outRootPath.c_str(), "RECREATE");
  if (outFile.IsZombie()) {
    std::cerr << "ERROR: failed to create output ROOT file: " << outRootPath << "\n";
    return;
  }

  if (d0) {
    TDirectory* d = outFile.mkdir("D0");
    if (d) {
      d->cd();
      d0Artifacts.hMass->Write("hMass");
      d0Artifacts.hV2Value->Write("hV2Value");
      d0Artifacts.pV2vsMass->Write("pV2vsMass");
    }
    outFile.cd();
    SaveSpeciesPngs(outdir, d0Artifacts);
    PrintSummary(d0Summary);
  }

  if (dstar) {
    TDirectory* d = outFile.mkdir("DStar");
    if (d) {
      d->cd();
      dstarArtifacts.hMass->Write("hMass");
      dstarArtifacts.hV2Value->Write("hV2Value");
      dstarArtifacts.pV2vsMass->Write("pV2vsMass");
    }
    outFile.cd();
    SaveSpeciesPngs(outdir, dstarArtifacts);
    PrintSummary(dstarSummary);
  }

  outFile.Close();
  std::cout << "Saved outputs under: " << outdir << "\n";
  std::cout << "Saved ROOT histogram file: " << outRootPath << "\n";
}
