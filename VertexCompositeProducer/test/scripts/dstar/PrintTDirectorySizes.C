#include <TFile.h>
#include <TDirectory.h>
#include <TKey.h>
#include <TClass.h>
#include <TSystem.h>
#include <TTree.h>

#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>

namespace {

std::string humanSize(Long64_t bytes) {
  const char* units[] = {"B", "KiB", "MiB", "GiB", "TiB"};
  double size = static_cast<double>(bytes);
  int unit = 0;
  while (size >= 1024.0 && unit < 4) {
    size /= 1024.0;
    ++unit;
  }
  std::ostringstream os;
  os << std::fixed << std::setprecision(size < 10.0 ? 2 : 1) << size << " " << units[unit];
  return os.str();
}

Long64_t dirBytes(TDirectory* dir) {
  if (!dir) return 0;
  Long64_t total = 0;
  TIter nextKey(dir->GetListOfKeys());
  while (TKey* key = static_cast<TKey*>(nextKey())) {
    TClass* cls = TClass::GetClass(key->GetClassName());
    if (cls && cls->InheritsFrom(TDirectory::Class())) {
      auto* subdir = dynamic_cast<TDirectory*>(key->ReadObj());
      if (subdir) {
        total += dirBytes(subdir);
        delete subdir;
      }
      continue;
    }

    TObject* obj = key->ReadObj();
    if (!obj) {
      total += key->GetNbytes();
      continue;
    }
    if (obj->InheritsFrom(TTree::Class())) {
      auto* tree = static_cast<TTree*>(obj);
      total += tree->GetZipBytes();
    } else {
      total += key->GetNbytes();
    }
    delete obj;
  }
  return total;
}

void printDir(TDirectory* dir, const std::string& path) {
  if (!dir) return;
  Long64_t bytes = dirBytes(dir);
  std::cout << std::left << std::setw(60) << path << " " << humanSize(bytes) << "\n";

  TIter nextKey(dir->GetListOfKeys());
  while (TKey* key = static_cast<TKey*>(nextKey())) {
    TClass* cls = TClass::GetClass(key->GetClassName());
    if (cls && cls->InheritsFrom(TDirectory::Class())) {
      auto* subdir = dynamic_cast<TDirectory*>(key->ReadObj());
      if (subdir) {
        printDir(subdir, path + "/" + subdir->GetName());
        delete subdir;
      }
    }
  }
}

}  // namespace

void PrintTDirectorySizes(const char* filename="d0ana_tree_step2.root") {
  if (!filename || std::string(filename).empty()) {
    std::cerr << "Usage: PrintTDirectorySizes(\"file.root\")\n";
    return;
  }
  std::unique_ptr<TFile> file(TFile::Open(filename, "READ"));
  if (!file || file->IsZombie()) {
    std::cerr << "Failed to open file: " << filename << "\n";
    return;
  }

  printDir(file.get(), file->GetName());
}
