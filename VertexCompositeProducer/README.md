# VertexCompositeProducer code map

This package reconstructs composite candidates and supplies supporting event
filters/unpackers. The current PbPb 2023 D0/D* analysis uses a small subset of
the buildable package.

## Current D0/D* production chain

```text
generalTracks + offlinePrimaryVertices
  -> D0Producer / D0Fitter
  -> generalD0CandidatesNew:D0
  -> DStarProducer / DStarFitter
  -> generalDStarCandidatesNew:DStar
  -> PATCompositeTreeProducer6
```

`D0Fitter` first selects tracks, forms the configured right-sign or wrong-sign
pairs, tests both kaon/pion mass assignments, performs the two-track kinematic
vertex fit, applies mass/rapidity/topology cuts, optionally evaluates the D0
ONNX model, and stores fitted quantities as `pat::CompositeCandidate` user data.

`DStarFitter` consumes those fitted D0 candidates and selected slow-pion tracks.
It prevents reuse of a D0 daughter as the slow pion, applies the configured
kaon-times-slow-pion charge rule, constrains/refits the decay, applies D* pT,
rapidity, delta-mass, DCA, and vertex/topology requirements, and writes
`daughter(0) = D0`, `daughter(1) = slow pion`. That layout is the contract read
by PAT6 and by reconstructed-track event-plane daughter removal.

## Active D0/D* source and configuration

| Code | Purpose |
| --- | --- |
| `src/D0Fitter.cc`, `interface/D0Fitter.h` | D0 to K-pi reconstruction, fit, topology, user data, and optional ONNX inference |
| `src/D0Producer.cc`, `interface/D0Producer.h` | CMSSW wrapper that owns `D0Fitter` and publishes `D0` plus optional `MVAValuesD0` |
| `python/generalD0Candidates_cfi.py`, `python/generalD0Candidates_cff.py` | Default D0 module and sequence configuration |
| `src/DStarFitter.cc`, `interface/DStarFitter.h` | D0 plus slow-pion D* reconstruction and diagnostic options |
| `src/DStarProducer.cc`, `interface/DStarProducer.h` | CMSSW wrapper that publishes `DStar`, MVA/DCA products, and calls `DStarFitter` |
| `python/generalDStarCandidates_cfi.py`, `python/generalDStarCandidates_cff.py` | Default D* module and sequence configuration |

The current tree writer and event-plane analyzers live in
`../VertexCompositeAnalyzer/`; see its `README.md` for the code-level map.

`DStar5PFitter` and `DStar5PProducer` are a separate supported five-particle
channel. They are deliberately left in `src/` and `interface/` and were not
folded into the D* legacy archive.

## Other reconstruction channels

The following source/header pairs are buildable support for other analyses and
are not D* alternatives.

| Pair | Candidate/workflow |
| --- | --- |
| `BFitter` / `BProducer` | B candidates built from an existing D0 plus another track |
| `D04PFitter` / `D04PProducer` | Four-prong D0 channel |
| `D0FitterNew` / `D0ProducerNew` | Alternate older D0 implementation retained by existing package configurations |
| `DPlus3PFitter` / `DPlus3PProducer` | Three-prong D+ channel |
| `DStar5PFitter` / `DStar5PProducer` | Separate five-particle D* channel |
| `DiMuFitter` / `DiMuProducer` | Dimuon vertex candidates |
| `LamC3PFitter` / `LamC3PProducer` | Three-prong Lambda-c channel |
| `V0Fitter` / `V0Producer` | V0 and related multi-species composite candidates |
| `HIClusterCompatibilityFilter` | Heavy-ion cluster-compatibility event filter |

## Supporting plugins

| Code | Purpose |
| --- | --- |
| `TrackAndVertexUnpacker.cc` | Rebuilds track/vertex products and packed-candidate associations from MiniAOD inputs |
| `MuonUnpacker.cc` | Reconstructs PAT muons and their track associations from packed inputs |
| `PrimaryVertexRecoveryProducer.cc` | Re-runs configured primary-vertex recovery algorithms |
| `NTrackVertexMapper.cc` | Produces the number-of-tracks value map associated with vertices/tracks |
| `MultFilter.cc` | Applies multiplicity/centrality-based event filtering |
| `HiHFFilter.cc` | Filters events using heavy-ion HF filter information |
| `DeDxMuonProducer.cc` | Maps track dE/dx information onto the PAT muon collection |
| `GenParticleFixer.cc` | Repairs generator mother/daughter references in copied gen collections |
| `GenParticleSuperChicFixer.cc` | SUPERCHIC-specific generator reference repair |
| `LumiProducerFromBrilcalc.cc` | Reads brilcalc CSV luminosity values into event products |
| `QWZDC2018Producer2.cc`, `QWZDC2018RecHit.cc` | 2018 ZDC digi quantities and reconstructed hits |

## Directory roles

| Directory | Rule |
| --- | --- |
| `src/`, `interface/`, `plugins/`, `python/` | Buildable package implementation and configuration |
| `test/production/` | Four canonical PbPb 2023 data/MC Step 1/Step 2 configurations |
| `test/validation/` | Question-specific D* mass, dxy, gen matching, event-plane, MVA, and slow-pion checks |
| `test/submission/` | CRAB and Condor submission material |
| `test/scripts/` | Plotting, ROOT macros, and input-list helpers |
| `test/legacy/` | Superseded full configurations |
| `legacy/dstar/` | DStarFitter point-in-time source backups; never compiled |

See `test/README.md` for the configuration entry points and
`legacy/dstar/README.md` for the exact purpose of each DStarFitter backup.
