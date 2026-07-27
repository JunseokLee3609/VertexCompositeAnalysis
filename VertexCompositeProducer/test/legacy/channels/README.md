# Legacy channel configurations

These configurations are preserved for reproducibility but are not the current
PbPb 2023 D0/D* production baselines. The canonical production entry points are
listed in `../../README.md` and live under `../../production/`.

| Directory | Configuration purpose | Why it is outside production |
| --- | --- | --- |
| `d04p/` | PbPb 2023 data/MC reconstruction of the four-prong D0 channel followed by the separate five-particle D* channel (`D04PProducer` + `DStar5PProducer`) | Different decay channel from the main two-prong D0 plus slow-pion D* chain |
| `d0_only/` | PbPb 2023 data/MC D0-only reconstruction and tree/gen-tuple studies | Does not run the main D* producer and predates the current Step 1/Step 2 split |
| `dplus/` | PbPb 2023 right-sign and wrong-sign three-prong D+ reconstruction and ntuple output | Separate D+ analysis channel |
| `upc_dimu/` | PbPb 2023 UPC dimuon data, MC, and pp-reco Express workflows | Separate dimuon/UPC workflow, last developed against early 13_2_X conditions |
| `pp2024_dstar/` | 100-event pp 2024 Express test of D0 plus D* reconstruction | One-off test config using the 2023 era and an Express global tag; not a validated production baseline |

The `d04p/` files are retained together with the buildable `D04PFitter`,
`DStar5PFitter`, and corresponding producers. Moving the configurations here
does not remove or disable the supported five-particle D* implementation.

Some of these older configurations name SQLite payloads by basename because
their CRAB configurations stage the payload into the job working directory.
The versioned source copies now live under `../../conditions/`.
