# VertexCompositeAnalyzer code map

This package contains the tree writers and validation analyzers used after
candidate reconstruction. A source being under `plugins/` means that CMSSW
builds it; it does not by itself mean that the PbPb 2023 D0/D* production runs
it.

## Current PbPb 2023 D0/D* analyzers

| Code | Purpose | Production use |
| --- | --- | --- |
| `plugins/PATCompositeTreeProducer6.cc/.h` | Current D0/D* tree writer. It reads the `pat::CompositeCandidate` objects produced by `D0Fitter` and `DStarFitter`, stores reconstructed quantities, and performs the production gen matching for MC. For D*, it expects `daughter(0) = D0` and `daughter(1) = slow pion`. | Data and MC Step 1/Step 2 |
| `plugins/PATEventPlaneTrack.cc` | Recomputes reconstructed-track v2/v3 Q vectors and removes the exact `TrackRef`s belonging to selected D* candidates to expose daughter autocorrelation. | Data and MC Step 2 |
| `plugins/MiniAODGenDstarEventPlaneTrack.cc` | Builds generator-level v2/v3 Q vectors from MiniAOD packed stable charged particles, both inclusive and with D* daughters removed. | MC Step 2 only |
| `plugins/EventInfoTreeProducer.cc` | Writes run/event, centrality, trigger/filter, vertex, and configured event-plane metadata independently of the candidate tree. | Step 2 metadata tree |

The corresponding active Python entry points are:

- `python/d0analyzer_tree_cfi.py` and `python/d0analyzer_tree_cff.py`
- `python/dStaranalyzer_tree_cfi.py` and `python/dStaranalyzer_tree_cff.py`
- `python/eventplaneanalyzer_cfi.py`
- `python/gendstareventplaneanalyzer_miniAOD_cfi.py`
- `python/eventinfotree_cfi.py` and `python/eventinfotree_cff.py`

The production reconstruction that feeds these analyzers is documented in
`../VertexCompositeProducer/README.md`.

## Other buildable analyzers

These remain in `plugins/` because other decay channels or older supported
configurations still use them. They are not alternative D* production versions.

| Code | Purpose |
| --- | --- |
| `PATCompositeTreeProducer.cc` | Older general PAT composite tree writer; still referenced by the dimuon tree configuration. |
| `PATCompositeNtupleProducer.cc` | Candidate-per-entry PAT ntuple format used by the generic PAT analyzer CFIs. |
| `VertexCompositeTreeProducer.cc` | Event-level tree writer for `reco::VertexCompositeCandidate` collections. |
| `VertexCompositeNtupleProducer.cc` | Candidate-per-entry ntuple writer for `reco::VertexCompositeCandidate` collections. |
| `VertexCompositeSelector.cc` | Applies configurable candidate, daughter, MVA, centrality, and gen-related selections and produces a selected collection. |
| `GenParticleSimpleAnalyzer.cc` | Small generator-particle tree/analyzer used by `simplegenana_cfi.py`. |
| `ValidationUtility.cc/.h` | Shared matching and ancestry helpers used by analyzer implementations; it is not a standalone CMSSW module. |
| `PATCompositeUtils.h` | Shared inline/helper definitions for PAT composite analyzers; it is not a standalone CMSSW module. |

## Archived and experimental code

| Directory | Meaning | Detailed code-purpose document |
| --- | --- | --- |
| `legacy/tree_producers/` | Superseded PAT2-PAT5 sources and point-in-time backups; not compiled | `legacy/tree_producers/README.md` |
| `legacy/event_plane/` | Replaced event-plane implementation snapshots; not compiled | `legacy/README.md` |
| `experimental/dstar_validation/` | Charge, delta-mass, efficiency, and MiniAOD gen-content checks | `experimental/dstar_validation/README.md` |
| `experimental/event_plane/` | AOD-gen, MB, official-cut, and stored-vs-recomputed event-plane comparisons | `experimental/event_plane/README.md` |
| `experimental/pat7/` | Gen-primary-vertex DCA and raw slow-pion matching experiment | `experimental/pat7/README.md` |

Files below `legacy/` and `experimental/` deliberately have no `BuildFile.xml`,
so they cannot accidentally enter a production build. To rerun one, restore
only the required source and configuration after reviewing its README.

## Status rule

- `plugins/` + a canonical production cfg reference: current production.
- `plugins/` without that reference: buildable support code for another channel
  or workflow.
- `experimental/`: a code-backed study answering a named validation question.
- `legacy/`: a historical comparison snapshot, not a runnable baseline.
