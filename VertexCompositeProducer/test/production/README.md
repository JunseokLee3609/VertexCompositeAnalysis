# Production configurations

Only stable, standalone configurations used as production baselines belong here. Put short-lived
debugging, alternative charge selections, and event-count-limited checks under `../validation/`.

## PbPb 2023 baseline matrix

| Sample | Stage | Purpose |
| --- | --- | --- |
| Data | Step 1 | Reconstruct D0 and D*, write PAT6 trees without D0 MVA selection |
| Data | Step 2 MVA | Apply the configured D0 ONNX selection, write PAT6 trees and reconstructed-track event-plane information |
| MC | Step 1 | Reconstruct and gen-match D0/D* without D0 MVA selection |
| MC | Step 2 MVA | Apply D0 ONNX selection, run PAT6 gen matching, and write both reconstructed and MiniAOD-gen D* event-plane information |

The four files are under `pbpb2023/data/` and `pbpb2023/mc/`. Their exact names
and run examples are documented in `../README.md`.

All four follow the same candidate contract:

```text
D0Producer/D0Fitter
  -> generalD0CandidatesNew:D0
DStarProducer/DStarFitter
  -> generalDStarCandidatesNew:DStar
PATCompositeTreeProducer6
  -> D0/D* trees
```

Step 2 additionally runs `PATEventPlaneTrack`. MC Step 2 also runs
`MiniAODGenDstarEventPlaneTrack`. DStarFitter diagnostic switches in the
canonical MC Step 2 file are explicitly false, so category cutflows, the
slow-pion threshold scan, raw-kinematics mode, and duplicate rejection are not
part of the baseline selection.
