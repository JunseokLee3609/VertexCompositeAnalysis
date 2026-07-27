# PAT tree-producer version history

The numbered PAT tree producers are development generations, not interchangeable
production choices. Only `PATCompositeTreeProducer6` is used by the current
PbPb 2023 D0/D* baseline. The older sources are retained here for code comparison
and are not compiled.

## Why each generation existed

| Generation | Intended role | Main distinction |
| --- | --- | --- |
| Unnumbered `PATCompositeTreeProducer` | Original general-purpose PAT tree writer | Large multi-channel implementation. It remains buildable because the dimuon configuration still uses it; it is not the current D0/D* writer. |
| PAT2 | Extend the general tree with D0/D* daughter quantities, MVA histograms, DCA information, and generator matching | Still a monolithic implementation. `PATCompositeTreeProducer2_refactored` was an attempt to split gen processing and candidate filling into traceable functions. |
| PAT3 | Generalize PAT2-era decay handling | Added an explicit three-prong decay path while retaining the large common tree schema. It was a compatibility step for more candidate topologies, not a D*-specific simplification. |
| PAT4 | Improve D* generator ancestry and daughter-track association | Introduced a generator-track pool and explicit D0/D* ancestor checks so D0 daughters and the slow pion could be tied to the same decay chain. |
| PAT5 refactored | Last full-featured pre-PAT6 D* reference | Continued the refactored PAT2/PAT4 style matching and large output surface. The ONNX debug/lite variants were written to isolate MVA input/output behavior, not to define separate production physics. |
| PAT6 | Make the D0/D* production path direct and auditable | Started as the focused replacement that consumes the exact `D0Fitter`/`DStarFitter` candidate layout. It is now the production writer and contains the validated D* chain matching and diagnostic counters used by current studies. |
| PAT7 | Investigate gen-primary-vertex DCA and raw slow-pion matching tails | Validation-only fork of the PAT6 direction. It compares fitted-candidate and raw-track slow-pion behavior and remains under `experimental/pat7/`. |

The current PAT6 generator definition keeps only a D* with one D0 and one slow
pion at the D* level. The D0 must have kaon and pion daughters and may contain at
most one D0-level FSR photon. A photon directly at the D* level is rejected.
Reco matching then uses the D0 plus slow-pion layout produced by `DStarFitter`.

## Files kept in each directory

| Directory | Contents |
| --- | --- |
| `pat2/` | Original PAT2 and its refactoring attempt |
| `pat3/` | PAT3 source/header |
| `pat4/` | PAT4 source/header |
| `pat5/` | PAT5 refactored source/header, plus ONNX debug/lite sources and CFIs |
| `backups/pat2/` | Hidden/local PAT2 source snapshot |
| `backups/pat5/` | Original, back, and pre-gen-reference PAT5 snapshots |
| `backups/pat6/` | PAT6 before the mass histogram, D* debug, gen-match tail, ancestry, and raw-track diagnostic additions |

The backup names describe the question being added after the snapshot. They are
not releases and should not be selected as production versions. Git history is
the authoritative record for exact line-level changes.
