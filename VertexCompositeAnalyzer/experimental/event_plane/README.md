# Event-plane validation variants

## Production modules

Two event-plane modules remain in `VertexCompositeAnalyzer/plugins/` because the
canonical Step 2 configurations execute them:

- `PATEventPlaneTrack.cc`: reconstructed-track Q vectors with D* daughter-track
  removal. It selects high-purity tracks with relative pT error below 0.10,
  absolute dz/dzError and dxy/dxyError below 3, 0.3 < pT < 3 GeV, and
  |eta| < 2.4. It stores inclusive and daughter-excluded v2/v3 vectors in the
  full, forward/backward, |eta| < 1.6, and |eta| < 0.8 regions.
- `MiniAODGenDstarEventPlaneTrack.cc`: MC generator-level event plane from packed
  stable charged particles. It stores inclusive and D* daughter-excluded Q
  vectors and is executed by the canonical MC Step 2 path.

PAT6 also stores configured `reco::EvtPlaneCollection` quantities. That is not a
replacement for `PATEventPlaneTrack`: the latter recomputes track Q vectors and
removes the reconstructed D* daughter tracks to study autocorrelation.

## Experimental variants

| Module | Question it was designed to answer | Comparison with production |
| --- | --- | --- |
| `GenDstarEventPlaneTrack.cc` | What is the generator-level v2/v3 plane in an AOD-style `reco::GenParticleCollection`, before and after removing each generated D* decay track? | AOD reference implementation. It writes both per-D* and per-event trees. Production MiniAOD uses pruned plus packed collections and an event-level tree. |
| `PATEventPlaneTrackMB.cc` | What does the reconstructed event-plane tree look like for a minimum-bias reference, optionally without a D* candidate collection, while also copying the recomputed official event-plane collection? | Uses the same main track cuts and Q-vector regions, but makes the composite collection optional and adds raw/offset/flattened HF and tracker event-plane branches from `eventplaneSrcRecalc`. |
| `PATEventPlaneTrackOfficial.cc` | How many tracks pass the legacy analysis cuts versus pp/HI/pixel-style official cuts, and how does the Q vector change? | Adds era-dependent hit, chi2/layer, algorithm, dz and dxy requirements and prints legacy-only/official-only counters. It has a smaller, older output schema and a hard-coded 1.7-2.1 candidate mass window. |
| `EvtPlaneComparator.cc` | Are stored and recomputed `reco::EvtPlaneCollection` objects identical index by index? | Compares angle, q, sumw, collection size, HF/tracker groups, sentinel values, and zero spikes. It does not reconstruct D* candidates or Q vectors. |

Associated CFI and test files are kept in `python/` and `test/` below this
directory. Existing ROOT files and logs remain under `VertexCompositeAnalyzer/test/`
and were not moved into the source archive.

The files under this directory are not compiled. Restore a selected module and
its CFI to the package `plugins/` and `python/` directories before rerunning it.
