# Versioned condition payloads

This directory contains small SQLite condition payloads required by retained
workflows. They are inputs, not generated job output.

| Directory | Payloads | Consumers |
| --- | --- | --- |
| `centrality/` | PbPb 2023 data nominal/374810 and HYDJET MC HF-tower centrality tables | Older PbPb charm configurations and their CRAB submission files |
| `reaction_plane/` | PbPb 2018 `HeavyIonRPRcd` payload | Retained 2018 dimuon/event-plane configurations |

CRAB submission files reference these source paths and stage each database into
the job sandbox. The old CMSSW configurations therefore continue to use the
database basename in their `sqlite_file:` connection string.
