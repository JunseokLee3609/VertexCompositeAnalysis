# VertexCompositeProducer configurations

This directory separates stable production configurations from submission and validation workflows.
Run the commands below from this `test/` directory unless a section says otherwise.

## Production baselines

| Sample | Stage | Configuration |
| --- | --- | --- |
| PbPb 2023 data | Step 1 | `production/pbpb2023/data/PbPb2023_D0BothAndDStar_MB_cfg_v1_Step1.py` |
| PbPb 2023 data | Step 2 MVA | `production/pbpb2023/data/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py` |
| PbPb 2023 MC | Step 1 | `production/pbpb2023/mc/PbPb2023_D0BothAndDStar_MB_cfg_mc_v1_Step1.py` |
| PbPb 2023 MC | Step 2 MVA | `production/pbpb2023/mc/PbPb2023_D0BothAndDStar_MB_cfg_mc_v2_Step2MVA.py` |

Example:

```bash
cmsRun production/pbpb2023/data/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py
```

These four files are the canonical full configurations. Validation files should load one of these
baselines and override only the settings needed by the check.

## Directory roles

- `production/`: canonical Data and MC full configurations.
- `submission/crab/`: CRAB configurations and submission helpers.
- `submission/condor/`: Condor cfg files, wrappers, and submit descriptions.
- `validation/`: D* mass, gen-matching, event-plane, MVA, and slow-pion checks.
- `scripts/`: plotting, ROOT macros, and input-list generators.
- `legacy/configs/`: superseded full D0/D* configurations retained for reproducibility.
- `Old/` and `legacy/`: older workflows that are not current production baselines.

Payload databases and user-generated input lists remain at the top level for backward compatibility.

## CRAB

CRAB path settings are relative to `submission/crab/`, so submit from that directory:

```bash
cd submission/crab
crab submit -c crabConfig_MB_Step1.py
```

New CRAB work areas are created as `submission/crab/crab_projects/`. Existing top-level
`crab_projects/` directories were not moved or deleted.

## Condor

The main helper can be invoked from any directory. It resolves its cfg and proxy paths internally:

```bash
submission/condor/submit_condor.sh data files2023MB24_31.txt tomorrow
submission/condor/submit_condor.sh mc files_MC_all_merged_uniqueidx_04Mar26_v1.list tomorrow
```

The proxy is taken from `X509_USER_PROXY` when set, otherwise from `test/myProxy`. No proxy is copied
into version control.

## Validation

Validation cfg files use paths below `CMSSW_BASE/src/VertexCompositeAnalysis/VertexCompositeProducer/test`.
For example:

```bash
cmsRun validation/dstar_mass/PbPb2023_DStarDeltaMDebug_MB_cfg.py
cmsRun validation/gen_matching/PbPb2023_DStarGenMatchTailCounter_20k.py
```

Generated ROOT files, logs, plots, CRAB projects, caches, and AFS recovery files are intentionally ignored.
They are not deleted by the cleanup.
