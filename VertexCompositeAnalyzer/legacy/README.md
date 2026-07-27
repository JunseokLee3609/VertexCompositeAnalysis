# Legacy analyzer sources

This directory preserves superseded analyzer implementations outside the CMSSW
plugin build path. Files below `legacy/` are references only and are not compiled.

The active analyzers remain under `plugins/`:

- `PATCompositeTreeProducer.cc`: still used by the dimuon configuration.
- `PATCompositeTreeProducer6.cc/.h`: current D0/D* production ntuplizer.

## Tree-producer snapshots

| Directory | Role |
| --- | --- |
| `tree_producers/pat2/` | Superseded PAT2 implementation and refactoring work |
| `tree_producers/pat3/` | Superseded PAT3 implementation |
| `tree_producers/pat4/` | Superseded PAT4 implementation |
| `tree_producers/pat5/` | Last pre-PAT6 implementation and ONNX diagnostics |
| `tree_producers/backups/` | Local source snapshots retained for comparison |
| `event_plane/backups/` | Superseded event-plane source snapshots |

The intent and differences of PAT2 through PAT7 are documented in
`tree_producers/README.md`.

The archived source files intentionally keep their original include paths. To
rebuild one, restore its source and header to `plugins/`, review its configuration
contract, and add only the required files back to the plugin build.

Do not add a `BuildFile.xml` below this directory. Git history remains the primary
record of when and why each version changed.
