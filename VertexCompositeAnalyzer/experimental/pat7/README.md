# PATCompositeTreeProducer7 experiment

PAT7 is a validation implementation for gen-primary-vertex DCA and raw slow-pion
matching studies. It is not used by the canonical production configurations;
PAT6 remains the current D0/D* production ntuplizer.

The source is outside `plugins/`, so PAT7 is deliberately not compiled in this
checkout. To run the validation again, restore `PATCompositeTreeProducer7.cc/.h`
to `VertexCompositeAnalyzer/plugins/`, build, and then run the configuration in
`validation/pat7_gendca_cfg.py`.

Only source and reproducibility configurations were moved here. Existing ROOT
files, logs, and the nested validation CMSSW checkout remain under
`VertexCompositeAnalyzer/test/pat7_gendca_validation/` and are not part of this
source archive. The validation configurations still use that directory for their
local input and output artifacts.
