# Validation configurations

Validation cfg files should load a canonical file from `../production/` and apply a small override.
Avoid copying an entire production cfg: duplicated D0 and D* fitter parameters drift easily.

- `dstar_mass/`: delta-mass, right-sign, D0 wrong-sign, and D* wrong-sign checks.
- `dxy/`: data/MC dxy-error A/B checks and their analysis helper.
- `gen_matching/`: generator matching and raw slow-pion investigations.
- `event_plane/`: reaction-plane and event-plane comparisons.
- `mva/`: ONNX debugging and MVA training variants.
- `slow_pion/`: slow-pion threshold scans.
