# DATA production: MVA > 0.8, eight PDs per CRAB task

## Groups

| Group | Dataset names | Input files | Approximate jobs at 10 files/job |
|---|---|---:|---:|
| 0_7 | HIPhysicsRawPrime0–7 | 45,477 | 4,548 |
| 8_15 | HIPhysicsRawPrime8–15 | 45,240 | 4,524 |
| 16_23 | HIPhysicsRawPrime16–23 | 45,421 | 4,543 |
| 24_31 | HIPhysicsRawPrime24–31 | 45,492 | 4,550 |

All inputs are HIRun2023A-PromptReco-v2/MINIAOD. The four committed file lists are disjoint and cover RawPrime0–31. Job counts are nominal arithmetic estimates; CRAB controls the actual splitting.

## Production settings

- CMSSW_13_2_11, el8_amd64_gcc11, current production modules.
- DATA cfg: PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA_MVA0p8_30Sep26.py.
- The DATA wrapper loads the canonical cfg and sets only D0 mvaCut to 0.8.
- From 01Oct26, the canonical DATA path saves Dstar, EventPlane and EventInfo only. The D0 producer and D0 count filter remain as inputs to Dstar reconstruction; the standalone D0 analyzer is absent from the execution path.
- New task names contain NoD0Tuple_CMSSW13211_01Oct26_v1; workArea is crab_projects/data_mva08_nod0tuple_01Oct26.
- Raw Dstar kinematics=True, DIAG enabled, GEN matching/GEN ntuples disabled.
- Existing Golden JSON, HLT/offline cuts, era, GlobalTag and ONNX model apply.
- The Golden JSON is applied through CMSSW source.lumisToProcess; these userInputFiles tasks have no CRAB Data.lumiMask.
- FileBased, 10 input files/job, all files, 1 core, 3000 MB, runtime request 2750 minutes.
- Storage site T3_KR_KNU; output /store/user/<CERN-primary-account>/Run3_2023/Data/SkimMVA/<request-name>.
- DSTAR_OUTPUT_USER is required. Set it to the CRAB-authenticated primary account, as reported by crab checkusername.

## Environment

On lxplus, enter the EL8 environment and activate the checked-out CMSSW area:

```bash
/cvmfs/cms.cern.ch/common/cmssw-el8
source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=el8_amd64_gcc11
cd <CMSSW_13_2_11>/src
eval "$(scram runtime -sh)"
source /cvmfs/cms.cern.ch/common/crab-setup.sh prod
cd VertexCompositeAnalysis/VertexCompositeProducer/test/submission/crab
voms-proxy-init --voms cms --valid 192:00
crab checkusername
```

Use cmsset_default.sh as provided by CVMFS. Build producer/analyzer libraries from this checkout before the first submission in another CMSSW area.

## Submit one group

Current authorized submission: **RawPrime8–15 only**. RawPrime0–7 was already submitted with the earlier cfg, which saved the D0 tuple. RawPrime16–23 and 24–31 remain prepared for later submission.

For the junseok account:

```bash
bash crab_submit_data_mva08_30Sep26.sh 8_15 junseok
```

For the other account, replace CERN_ACCOUNT with its CRAB primary username:

```bash
bash crab_submit_data_mva08_30Sep26.sh 16_23 CERN_ACCOUNT
bash crab_submit_data_mva08_30Sep26.sh 24_31 CERN_ACCOUNT
```

Each command submits exactly one eight-PD group. The helper forwards optional additional crab submit arguments. CRAB submission, dry-run tasks and storage write checks are separate deployment actions.

For direct config loading/submission, set DSTAR_OUTPUT_USER explicitly before using one of the four group cfgs.

## Provenance and verification

data_mva08_30Sep26_manifest.json records the file-list hashes, per-PD file/event counts and source/model provenance. The manifest counts were checked against the recent DAS summaries for all 32 PDs.

The MVA 0.8 DATA pipeline has already processed three independent 1,000-event file samples successfully. That sampling estimates storage; it is not full-production physics QA.

Prepared configs are checked with Python parsing, bash -n, real CMSSW cfg loading, CRAB configuration validation and input-list integrity checks. Preparation itself does not submit a CRAB task.
