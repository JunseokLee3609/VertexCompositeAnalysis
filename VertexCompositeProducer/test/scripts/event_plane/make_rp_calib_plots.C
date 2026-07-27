#include <TCanvas.h>
#include <TChain.h>
#include <TFile.h>
#include <TLegend.h>
#include <TROOT.h>
#include <TStyle.h>
#include <TSystem.h>
#include <TTree.h>

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <sys/stat.h>
#include <vector>

namespace {

struct SampleFile {
  std::string label;
  std::string path;
};

struct ChannelDef {
  std::string key;
  std::vector<std::string> flatCandidates;
  std::vector<std::string> rawCandidates;
  std::vector<std::string> offCandidates;
};

bool fileExists(const std::string& path) {
  return gSystem->AccessPathName(path.c_str()) == kFALSE;
}

long long fileSizeBytes(const std::string& path) {
  struct stat st;
  if (stat(path.c_str(), &st) != 0) return -1;
  return static_cast<long long>(st.st_size);
}

std::string detectTreePath(const std::string& filePath) {
  TFile f(filePath.c_str(), "READ");
  if (f.IsZombie()) return "";
  if (f.Get("dStarana/PATCompositeNtuple")) return "dStarana/PATCompositeNtuple";
  if (f.Get("PATCompositeNtuple")) return "PATCompositeNtuple";
  return "";
}

TTree* getTreeFromFile(TFile& f, const std::string& treePath) {
  TObject* obj = f.Get(treePath.c_str());
  if (!obj) return nullptr;
  return (TTree*)obj;
}

std::string findFirstExistingBranch(TChain* ch, const std::vector<std::string>& candidates) {
  if (!ch) return "";
  TObjArray* bl = ch->GetListOfBranches();
  if (!bl) return "";
  for (const auto& c : candidates) {
    if (bl->FindObject(c.c_str())) return c;
  }
  return "";
}

std::string buildExpr(const std::string& branch, int harmonicIndex) {
  return branch + "[" + std::to_string(harmonicIndex) + "]";
}

void writeInputsUsed(const std::string& outPath, const std::vector<SampleFile>& inputs) {
  std::ofstream out(outPath.c_str());
  for (const auto& f : inputs) {
    out << f.path << "\n";
  }
}

}  // namespace

void make_rp_calib_plots() {
  gROOT->SetBatch(kTRUE);
  gStyle->SetOptStat(0);

  const std::string baseDir = "VertexCompositeProducer/test/rp_calib_check_20260305_allEP";
  const std::string plotDir = baseDir + "/plots";
  const std::string summaryPath = baseDir + "/summary.txt";
  const std::string inputsPath = baseDir + "/inputs_used.txt";

  gSystem->mkdir(baseDir.c_str(), kTRUE);
  gSystem->mkdir(plotDir.c_str(), kTRUE);

  const std::vector<SampleFile> rpDefaultFiles = {
      {"rpdef_A", "/eos/cms/store/group/phys_heavyions/junseok/DstarAnalysis/RPcalibCheck_20260305/RPdefault/d0ana_tree_stepMVA_rpdef_A.root"},
      {"rpdef_B", "/eos/cms/store/group/phys_heavyions/junseok/DstarAnalysis/RPcalibCheck_20260305/RPdefault/d0ana_tree_stepMVA_rpdef_B.root"},
  };
  const std::vector<SampleFile> rp2023Files = {
      {"rp2023_A", "/eos/cms/store/group/phys_heavyions/junseok/DstarAnalysis/RPcalibCheck_20260305/RP2023/d0ana_tree_stepMVA_rp2023_A.root"},
      {"rp2023_B", "/eos/cms/store/group/phys_heavyions/junseok/DstarAnalysis/RPcalibCheck_20260305/RP2023/d0ana_tree_stepMVA_rp2023_B.root"},
  };

  std::vector<SampleFile> allFiles;
  allFiles.insert(allFiles.end(), rpDefaultFiles.begin(), rpDefaultFiles.end());
  allFiles.insert(allFiles.end(), rp2023Files.begin(), rp2023Files.end());

  writeInputsUsed(inputsPath, {
                           {"inputA", "/store/hidata/HIRun2023A/HIPhysicsRawPrime16/MINIAOD/PromptReco-v2/000/374/668/00000/b9af6718-1c9d-4199-98de-415f49d2b899.root"},
                           {"inputB", "/store/hidata/HIRun2023A/HIPhysicsRawPrime16/MINIAOD/PromptReco-v2/000/374/673/00000/bd29d6dd-309d-4901-b28d-bd746b01519a.root"},
                         });

  std::ofstream summary(summaryPath.c_str());
  summary << "RP calibration check summary (all EP channels)\n";
  summary << "Date: 2026-03-05\n\n";
  summary << "Configs used:\n";
  summary << "  RPdefault: PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1_RPdefault_test.py\n";
  summary << "  RP2023:    PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1_RP2023_test.py\n\n";

  bool validationOK = true;

  std::string treePath;
  for (const auto& f : allFiles) {
    if (fileExists(f.path) && treePath.empty()) {
      treePath = detectTreePath(f.path);
    }
  }

  summary << "Output ROOT validation:\n";
  summary << "  detected tree path: " << (treePath.empty() ? "<not found>" : treePath) << "\n";

  if (treePath.empty()) {
    summary << "  ERROR: PATCompositeNtuple tree path not found.\n";
    summary.close();
    return;
  }

  for (const auto& f : allFiles) {
    const bool exists = fileExists(f.path);
    const long long size = fileSizeBytes(f.path);
    bool hasTree = false;
    long long entries = -1;

    if (exists) {
      TFile tf(f.path.c_str(), "READ");
      if (!tf.IsZombie()) {
        TTree* t = getTreeFromFile(tf, treePath);
        if (t) {
          hasTree = true;
          entries = t->GetEntries();
        }
      }
    }

    summary << "  " << f.label << "\n";
    summary << "    path: " << f.path << "\n";
    summary << "    exists: " << (exists ? "yes" : "no") << "\n";
    summary << "    size_bytes: " << size << "\n";
    summary << "    has_tree: " << (hasTree ? "yes" : "no") << "\n";
    summary << "    entries: " << entries << "\n";

    if (!exists || !hasTree) validationOK = false;

  }
  summary << "\n";

  if (!validationOK) {
    summary << "Validation failed: at least one output file/tree is missing. Plot step skipped.\n";
    summary.close();
    return;
  }

  TChain chainDef(treePath.c_str());
  TChain chain2023(treePath.c_str());
  for (const auto& f : rpDefaultFiles) chainDef.Add(f.path.c_str());
  for (const auto& f : rp2023Files) chain2023.Add(f.path.c_str());

  const long long totalDef = chainDef.GetEntries();
  const long long total2023 = chain2023.GetEntries();

  summary << "Chain entries:\n";
  summary << "  RPdefault total entries: " << totalDef << "\n";
  summary << "  RP2023   total entries: " << total2023 << "\n\n";

  const std::vector<ChannelDef> channels = {
      {"hfm", {"ephfmAngle"}, {"ephfmAngleRaw"}, {"ephfmAngleoff", "ephfmAngleOff"}},
      {"hfp", {"ephfpAngle"}, {"ephfpAngleRaw"}, {"ephfpAngleoff", "ephfpAngleOff"}},
      {"hf", {"ephfAngle"}, {"ephfAngleRaw"}, {"ephfAngleoff", "ephfAngleOff"}},
      {"trackmid", {"eptrackmidAngle"}, {"eptrackmidAngleRaw"}, {"eptrackmidAngleoff", "eptrackmidAngleOff"}},
      {"trackp", {"eptrackpAngle"}, {"eptrackpAngleRaw"}, {"eptrackpAngleoff", "eptrackpAngleOff"}},
      {"trackm", {"eptrackmAngle"}, {"eptrackmAngleRaw"}, {"eptrackmAngleoff", "eptrackmAngleOff"}},
  };

  const std::vector<std::pair<std::string, std::string>> kinds = {
      {"flat", "flat"},
      {"raw", "raw"},
      {"off", "off"},
  };

  const std::vector<int> harmonics = {0, 1};
  const std::vector<std::string> harmonicLabels = {"v2", "v3"};

  summary << "Per-plot stats (cut: value > -9):\n";

  int plotCount = 0;
  for (const auto& ch : channels) {
    const std::string flatBranch = findFirstExistingBranch(&chainDef, ch.flatCandidates);
    const std::string rawBranch = findFirstExistingBranch(&chainDef, ch.rawCandidates);
    const std::string offBranch = findFirstExistingBranch(&chainDef, ch.offCandidates);

    std::map<std::string, std::string> branchByKind;
    branchByKind["flat"] = flatBranch;
    branchByKind["raw"] = rawBranch;
    branchByKind["off"] = offBranch;

    for (size_t ih = 0; ih < harmonics.size(); ++ih) {
      const int hidx = harmonics[ih];
      const std::string& hlabel = harmonicLabels[ih];

      for (const auto& kind : kinds) {
        const std::string& kindKey = kind.first;
        const std::string branch = branchByKind[kindKey];

        if (branch.empty()) {
          summary << "  " << kindKey << " " << ch.key << " " << hlabel
                  << " : MISSING BRANCH\n";
          continue;
        }

        const std::string expr = buildExpr(branch, hidx);
        const std::string cut = expr + " > -9";

        const std::string hDefName = "hDef_" + kindKey + "_" + ch.key + "_" + hlabel;
        const std::string h2023Name = "h2023_" + kindKey + "_" + ch.key + "_" + hlabel;

        TH1D hDef(hDefName.c_str(), "", 72, -3.2, 3.2);
        TH1D h2023(h2023Name.c_str(), "", 72, -3.2, 3.2);

        chainDef.Draw((expr + ">>" + hDefName).c_str(), cut.c_str(), "goff");
        chain2023.Draw((expr + ">>" + h2023Name).c_str(), cut.c_str(), "goff");

        const long long cutPassDef = chainDef.GetEntries(cut.c_str());
        const long long cutPass2023 = chain2023.GetEntries(cut.c_str());

        const double meanDef = hDef.GetMean();
        const double rmsDef = hDef.GetRMS();
        const double mean2023 = h2023.GetMean();
        const double rms2023 = h2023.GetRMS();
        const double ks = hDef.KolmogorovTest(&h2023);

        const double iDef = hDef.Integral();
        const double i2023 = h2023.Integral();
        if (iDef > 0) hDef.Scale(1.0 / iDef);
        if (i2023 > 0) h2023.Scale(1.0 / i2023);

        hDef.SetLineColor(kBlue + 1);
        hDef.SetLineWidth(2);
        h2023.SetLineColor(kRed + 1);
        h2023.SetLineWidth(2);

        const double ymax = std::max(hDef.GetMaximum(), h2023.GetMaximum());
        hDef.SetMaximum(ymax > 0 ? ymax * 1.25 : 1.0);

        TCanvas c("c", "c", 900, 700);
        hDef.SetTitle((kindKey + " " + ch.key + " " + hlabel + ";angle [rad];normalized entries").c_str());
        hDef.Draw("hist");
        h2023.Draw("hist same");

        TLegend leg(0.58, 0.73, 0.88, 0.88);
        leg.SetBorderSize(0);
        leg.SetFillStyle(0);
        leg.AddEntry(&hDef, "RPdefault", "l");
        leg.AddEntry(&h2023, "RP2023", "l");
        leg.Draw();

        const std::string outName = plotDir + "/" + kindKey + "_" + ch.key + "_" + hlabel + ".png";
        c.SaveAs(outName.c_str());
        ++plotCount;

        summary << "  " << kindKey << " " << ch.key << " " << hlabel << "\n";
        summary << "    branch: " << branch << "\n";
        summary << "    entries_before_cut: def=" << totalDef << " rp2023=" << total2023 << "\n";
        summary << "    entries_after_cut:  def=" << cutPassDef << " rp2023=" << cutPass2023 << "\n";
        summary << "    mean: def=" << meanDef << " rp2023=" << mean2023 << "\n";
        summary << "    rms:  def=" << rmsDef << " rp2023=" << rms2023 << "\n";
        summary << "    ks_pvalue: " << ks << "\n";
      }
    }
  }

  summary << "\nGenerated plot count: " << plotCount << "\n";

  summary.close();
}
