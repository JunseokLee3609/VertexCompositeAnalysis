# DStarFitter backup history

The active production files remain:

```text
interface/DStarFitter.h
interface/DStarProducer.h
src/DStarFitter.cc
src/DStarProducer.cc
python/generalDStarCandidates_cfi.py
python/generalDStarCandidates_cff.py
```

`DStar5PFitter` and `DStar5PProducer` are a separate supported channel and were
not moved.

## Backup comparison

| Snapshot | Important difference from current `DStarFitter.cc` |
| --- | --- |
| `backups/20251211_legacy_snapshot/DStarFitter.cc` | Oldest copy. It has no D* rapidity cut, no applied D* pT cut at the post-fit stage, no raw/refit kinematics switch, no slow-pion impact-parameter userFloats, no duplicate-slow-pion option, and no diagnostic cutflow. Its wrong-sign condition uses the total daughter charge instead of the current kaon-times-slow-pion charge rule. It also uses the old approximate dxy uncertainty and always references the primary vertex in the D* impact-parameter extrapolation. |
| `backups/20260513_dstar_debug/` | Clean pre-debug physics baseline. It already contains the corrected covariance-aware dxy uncertainty, D* pT and rapidity cuts, beam-spot/PV-aware DCA reference, current wrong-sign rule, and slow-pion dz/dxy userFloats. It does not contain the category histograms, raw-kinematics option, duplicate option, or slow-pion threshold scan. |
| `backups/20260518_slowpi_ptscan/` | State immediately before the slow-pion pT threshold counters. It already contains the four charge-category cutflow/histograms, raw/refit kinematics option, and duplicate-slow-pion option. The current source mainly adds the 0.3/0.4/0.5 GeV slow-pion threshold scan and its reporting. |

The canonical MC Step 2 configuration explicitly sets
`useRawDStarKinematics`, `rejectDuplicateSlowPion`,
`debugCategoryCutflow`, and `debugSlowPionPtScan` to false. Other canonical
configurations omit them, which gives the same false defaults in the fitter.
Consequently the diagnostic additions do not change their normal production
selection by themselves.

These files are comparison snapshots only and are not compiled.
