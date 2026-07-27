# D* validation analyzers

These modules were written to answer specific D* validation questions. They are
not part of the canonical production path and are deliberately outside the CMSSW
`plugins/` build directory.

## Production reference

The production reconstruction and ntuple path is:

```text
D0Producer/D0Fitter
  -> generalD0CandidatesNew:D0
DStarProducer/DStarFitter
  -> generalDStarCandidatesNew:DStar
PATCompositeTreeProducer6
  -> production D0/D* ntuple and gen matching
```

`DStarFitter` performs the D0 plus slow-pion combination, charge selection,
kinematic vertex fits, topology cuts, and final D* candidate construction.
PAT6 reads the fitted candidate with `daughter(0) = D0` and
`daughter(1) = slow pion`.

## Module purposes

| Module | Question it was designed to answer | Difference from production |
| --- | --- | --- |
| `DStarChargeCounterAnalyzer.cc` | Does the generator sample contain balanced D*+ and D*- counts, and how often do D0/anti-D0 K-pi decays appear? | Reads only `prunedGenParticles`, produces log counters, and does no reconstruction or PAT6 matching. Its event-level D0 check is not required to be a daughter of the counted D*. |
| `DStarDeltaMDebugAnalyzer.cc` | Which kaon/D0-pion/slow-pion charge category creates each delta-mass shape, and how do swapped-mass veto windows change it? | Recombines selected RS/SS D0 candidates with tracks before the DStarFitter refit. It studies all four charge categories, duplicate tracks, Q value, opening angle, and swap vetoes. It does not reproduce the full DStarFitter vertex/topology chain. |
| `DStarRecoEfficiency.cc` | What is the generator-level D* pT denominator and reconstructed-match numerator? | Produces `TH1D`/`TEfficiency` rather than the PAT6 tree. It is labelled as PAT5-style matching and accepts one-or-more D0 photons in its radiative branch, whereas the PAT6 production definition allows at most one D0-level photon. Results must not be presented as PAT6 efficiency without updating this contract. |
| `PAT6MiniAODGenSanityAnalyzer.cc` | Are stable K, pion, photon, and slow-pion daughters absent from `prunedGenParticles` but recoverable through `packedGenParticles` mother references? | Log-only MiniAOD content diagnostic. It does not consume reconstructed D* candidates or run PAT6 matching. |

## Associated configurations

- `test/dstar_charge_count_cfg.py`
- `test/dstar_reco_eff_test.py`
- `test/dstar_pat6_genmatching_cfg.py`
- `python/dstar_reco_eff_cfi.py`
- `python/dStaranalyzer_tree_pat6_cfi.py`

The standalone delta-mass configuration remains under
`VertexCompositeProducer/test/validation/dstar_mass/`.

These sources are not compiled in place. Restore the required module to
`VertexCompositeAnalyzer/plugins/` before rerunning a validation.
