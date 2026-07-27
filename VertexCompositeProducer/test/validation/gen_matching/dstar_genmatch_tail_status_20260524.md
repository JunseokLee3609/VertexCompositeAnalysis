# D* Gen-Matched Delta-Mass Tail Debug Status

Date: 2026-05-24, Europe/Zurich

This note records the 2026-05-24 investigation only. The older D0SS raw/refit pT collapse study was done on 2026-05-13, and the slow-pion pT threshold scan was done on 2026-05-18.

## Question

For MC gen-matched D* candidates, a high-side tail remains around the nominal DeltaM peak. The goal today was to check whether this tail is caused by:

- wrong gen ancestry, for example a slow pion from a different D*
- FSR or D0 swap
- loose or poor reco-gen matching of the slow pion
- raw track vs refitted/stored slow-pion candidate differences

## Relevant Producer Logic Checked

### DStarFitter

File:

- `VertexCompositeProducer/src/DStarFitter.cc`

Relevant behavior:

- The initial D* candidate DeltaM preselection is computed from `theD0.p4()` plus the raw slow-pion track four-vector:
  - `D0Vec = theD0.p4()`
  - `pPi = (slowPi track pt, eta, phi, pion mass)`
  - `debugDeltaM = M(D0Vec + pPi) - M(D0Vec)`
- The final stored D* p4 is either refitted or raw depending on `useRawDStarKinematics_`:
  - `dStarP4 = useRawDStarKinematics_ ? rawDStarP4 : refitDStarP4`
- The stored slow-pion daughter uses the refit child momentum while keeping the original track reference:
  - daughter p4 from `negCandTotalP`
  - `theNegCand.setTrack(pionTrackRef)`

Important consequence:

- PAT-level candidate kinematics can differ from the original raw slow-pion track kinematics.
- For tail debugging, both candidate slow-pion p4 and underlying raw track p4 have to be compared.

### PATCompositeTreeProducer6

File:

- `VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc`

Relevant behavior:

- The tail counter uses:
  - `deltaMass = trk.mass() - recoD0->mass()`
  - `slowPionDR = deltaR(recoSlowPi, genSlowPi)`
- The gen-match tail regions are:
  - low side: `0.139 <= dM < 0.144`
  - peak: `0.144 <= dM < 0.147`
  - high tail: `0.147 <= dM < 0.160`
  - outside: otherwise
- The staged counters are:
  - `matchGEN inclusive`
  - `strict no-FSR`
  - `strict no-FSR && !isSwap`
  - `strict no-FSR && !isSwap && slowPi dR < 0.01`
  - `strict no-FSR && !isSwap && slowPi dR < 0.005`
- The ancestry check confirms whether the matched gen slow pion:
  - has immediate mother with `|pdgId| == D*`
  - has a D* in the ancestor chain
  - has immediate mother pointer equal to the matched gen D*
- The raw-track debug print additionally compares:
  - candidate slow-pion dR to gen
  - raw track slow-pion dR to gen
  - raw-track-to-candidate dR
  - candidate and raw track pT over gen pT

## Outputs Checked

### Ancestry run

Files:

- `VertexCompositeProducer/test/dstar_genmatch_tail_ancestry_20k.log`
- `VertexCompositeProducer/test/dstar_genmatch_tail_ancestry_20k.root`

Run size:

- The cfg was named 20k, but the log shows a shutdown after 3532 processed events.
- `TrigReport Events total = 3532`

Counter result:

```text
matchGEN inclusive:
  low=1, peak=57, high=2, outside=0, high/peak=0.0350877

strict no-FSR:
  low=1, peak=49, high=2, outside=0, high/peak=0.0408163

strict no-FSR && !isSwap:
  low=1, peak=49, high=2, outside=0, high/peak=0.0408163

strict no-FSR && !isSwap && slowPi dR < 0.01:
  low=0, peak=46, high=0, outside=0, high/peak=0

strict no-FSR && !isSwap && slowPi dR < 0.005:
  low=0, peak=39, high=0, outside=0, high/peak=0
```

Ancestry result:

```text
matched gen slow pion checked:
  low=1, peak=57, high=2, outside=0

slow pion immediate mother |pdgId| == D*:
  low=1, peak=57, high=2, outside=0

slow pion ancestor chain contains D*:
  low=1, peak=57, high=2, outside=0

slow pion immediate mother pointer == matched gen D*:
  low=1, peak=57, high=2, outside=0
```

Interpretation:

- The high-tail slow pions are not from the wrong gen ancestry.
- The high-tail slow pions have the correct matched gen D* as direct mother.
- FSR removal and swap removal do not remove the high tail.
- Tightening the slow-pion reco-gen angular match to `dR < 0.01` removes the high tail in this small sample while keeping most peak candidates.

### Raw-track comparison run

Files:

- `VertexCompositeProducer/test/dstar_genmatch_tail_rawtrack_5k.log`
- `VertexCompositeProducer/test/dstar_genmatch_tail_rawtrack_5k.root`

Run size:

- The cfg was named 5k, but the log shows a shutdown after 2071 processed events.
- `TrigReport Events total = 2071`

Counter result:

```text
matchGEN inclusive:
  low=0, peak=29, high=2, outside=0, high/peak=0.0689655

strict no-FSR:
  low=0, peak=26, high=2, outside=0, high/peak=0.0769231

strict no-FSR && !isSwap:
  low=0, peak=26, high=2, outside=0, high/peak=0.0769231

strict no-FSR && !isSwap && slowPi dR < 0.01:
  low=0, peak=25, high=0, outside=0, high/peak=0

strict no-FSR && !isSwap && slowPi dR < 0.005:
  low=0, peak=23, high=0, outside=0, high/peak=0
```

Printed high-tail samples:

```text
sample 1:
  dM=0.14833
  recoCandidateSlowPiDR=0.0237961
  rawTrackSlowPiDR=0.0265705
  rawTrackToRecoCandidateDR=0.00696645
  recoCandidateSlowPiPt=0.450698
  rawTrackSlowPiPt=0.450439
  genSlowPiPt=0.774507
  recoCandidatePtOverGenPt=0.581915
  rawTrackPtOverGenPt=0.581582
  directMotherPointerIsMatchedDStar=1
  hasDStarAncestor=1

sample 2:
  dM=0.148192
  recoCandidateSlowPiDR=0.0209187
  rawTrackSlowPiDR=0.00945036
  rawTrackToRecoCandidateDR=0.0292623
  recoCandidateSlowPiPt=0.603952
  rawTrackSlowPiPt=0.61084
  genSlowPiPt=0.667829
  recoCandidatePtOverGenPt=0.904351
  rawTrackPtOverGenPt=0.914665
  directMotherPointerIsMatchedDStar=1
  hasDStarAncestor=1
```

Interpretation:

- Sample 1: raw track and candidate agree with each other, while the pT ratio to gen is low (`rawTrackPtOverGenPt=0.58`) and the angular match is loose but still inside the configured matching cone (`rawTrackSlowPiDR=0.0266 < 0.03`). This does not prove a wrong reco track or a wrong gen ancestry. It only proves that this high-tail candidate would fail a tighter slow-pion match such as `dR < 0.01`, and that the current matching definition does not require pT consistency.
- Sample 2: raw track is fairly close to the gen slow pion (`rawTrackSlowPiDR=0.00945`), but the stored candidate slow pion is farther away (`recoCandidateSlowPiDR=0.0209`) and differs from the raw track (`rawTrackToRecoCandidateDR=0.0293`). This points to refit/stored daughter p4 effects for at least part of the tail.

## Current Conclusion

The 2026-05-24 evidence does not support "wrong gen ancestry" as the cause of the high-side DeltaM tail.

The high-tail candidates are still matched to the correct gen D* decay chain:

- slow pion immediate mother is the matched gen D*
- no evidence that the high tail is from a different D*
- no evidence that FSR or D0 swap is the dominant cause

The strongest observed handle is slow-pion reco-gen matching quality:

- high tail remains after strict no-FSR and no-swap
- high tail disappears when requiring `slowPi dR < 0.01` in the checked samples

The raw-track printout suggests two possible mechanisms, but the current small-stat printout does not by itself prove which one dominates:

1. Loose matching or slow-pion reconstruction/resolution issue:
   - raw track and stored candidate are consistent with each other
   - the candidate is still inside the configured `dR < 0.03` match, but fails tighter `dR < 0.01`
   - pT consistency is not part of the current PAT6 slow-pion gen-match definition

2. Refit/stored daughter p4 issue:
   - raw track is close to gen
   - stored candidate slow pion is farther from gen than the raw track

## Caveats

- The 2026-05-24 runs are small because they stopped early:
  - ancestry run: 3532 events
  - raw-track run: 2071 events
- Therefore the conclusion is a strong direction, not yet a high-stat final statement.
- A larger run should repeat the raw-track comparison and preferably make histograms of:
  - `dM` vs candidate slow-pion dR
  - `dM` vs raw-track slow-pion dR
  - candidate slow-pion pT / gen pT
  - raw-track slow-pion pT / gen pT
  - raw-track-to-candidate dR

## Related Older Context

Not part of today's result, but relevant history:

- 2026-05-13:
  - D0SS / D* raw vs refit pT debug.
  - `catC` had raw D* pT above 4.5 for about 52.8 percent, but fitter-used D* pT above 4.5 for almost zero candidates.
  - Summary file: `VertexCompositeProducer/test/dstar_dpt_debug_abcd_10k_summary.txt`

- 2026-05-18:
  - slow-pion pT threshold scan.
  - final counts:
    - `pi_s pT > 0.3`: 114947
    - `pi_s pT > 0.4`: 80680
    - `pi_s pT > 0.5`: 25762
  - Log file: `VertexCompositeProducer/test/dstar_slowpi_ptscan_20k.log`

## 2026-05-24 Raw-Track Diagnostic Update

Code changes:

- Added PAT6 branches for raw slow-pion diagnostics:
  - `matchGen_slowPion_rawTrack_dR`
  - `matchGen_slowPion_rawTrackToCandidate_dR`
  - `matchGen_slowPion_rawTrackPt`
  - `matchGen_slowPion_candidatePtOverGenPt`
  - `matchGen_slowPion_rawTrackPtOverGenPt`
  - `matchGen_deltaMass_current`
  - `matchGen_deltaMass_rawSlowPion`
  - `matchGen_slowPion_candidateMatched`
  - `matchGen_slowPion_rawTrackMatched`
- Existing `matchGEN` keeps the original candidate-p4 slow-pion matching meaning.
- Raw-track matching is stored separately and also used for diagnosis counters.
- Backup before this edit:
  - `VertexCompositeAnalyzer/plugins/backup/20260524_pat6_raw_slowpi_diag/PATCompositeTreeProducer6.cc.before_raw_slowpi_diag`
  - `VertexCompositeAnalyzer/plugins/backup/20260524_pat6_raw_slowpi_diag/PATCompositeTreeProducer6.h.before_raw_slowpi_diag`

Run:

- Config: `VertexCompositeProducer/test/validation/gen_matching/PbPb2023_DStarGenMatchTailRawTrack_50k.py`
- Output: `VertexCompositeProducer/test/dstar_genmatch_tail_rawtrack_50k.root`
- Log: `VertexCompositeProducer/test/dstar_genmatch_tail_rawtrack_50k.log`
- Threads/streams: 16/16
- The job was gracefully interrupted because xrootd I/O was slow.
- Processed events: 3530 visited, 3528 passed through `dStarana_mc`.

Key counters from the processed 3528 events:

```text
strict no-FSR && !isSwap:
  low-side 0.139<=dM<0.144: 1
  peak     0.144<=dM<0.147: 49
  high     0.147<=dM<0.160: 2

candidate-p4 slowPi matched:
  peak: 49
  high: 2

raw-track slowPi matched:
  peak: 50
  high: 2

raw-track dR < 0.01:
  peak: 48
  high: 1

candidate-p4 dR < 0.01:
  peak: 46
  high: 0

raw-track dR < 0.01 && candidate-p4 dR >= 0.01:
  peak: 3
  high: 1

raw-track dR >= 0.01 && candidate-p4 matched:
  peak: 1
  high: 1

raw-track pT/gen pT outside [0.8,1.2]:
  peak: 0
  high: 1
```

Current stored delta-mass region vs raw-slow-pion delta-mass region:

```text
current peak 0.144<=dM<0.147:
  raw peak: 50

current high 0.147<=dM<0.160:
  raw peak: 1
  raw high: 1
```

Interpretation of the two current high-tail cases:

- Case 1:
  - `currentDM=0.14833`
  - `rawSlowPiDM=0.14898`
  - `rawTrackSlowPiDR=0.02657`
  - `rawTrackPtOverGenPt=0.58158`
  - This is a raw slow-pion reconstruction / loose-match-quality case. Using the raw track does not move it back to the peak.
- Case 2:
  - `currentDM=0.148192`
  - `rawSlowPiDM=0.146457`
  - `rawTrackSlowPiDR=0.00945`
  - `rawTrackToCandidateDR=0.02926`
  - `rawTrackPtOverGenPt=0.91467`
  - This is a stored candidate-p4/refit representation case. The raw slow-pion track moves the candidate back to the peak region.

Current conclusion:

- Both mechanisms are real in the processed sample.
- With only two high-tail candidates in this run, dominance cannot be claimed yet.
- In this limited sample, high tail splits 1 raw-track/reco-match-quality case and 1 candidate-p4/refit-representation case.
- The matrix shows that raw slow-pion DeltaM would recover one of the two high-tail candidates into the peak region.

## 2026-05-24 Condor Production Setup

Goal:

- Produce a larger MC sample with both slow-pion definitions stored:
  - stored/refit daughter-candidate quantities
  - raw slow-pion `bestTrack()` quantities
- Use this sample to decide whether the gen-matched DeltaM high-side tail is dominated by:
  - raw track reconstruction / loose reco-gen matching quality
  - DStarFitter stored daughter p4 / refit representation

Production cfg:

```text
VertexCompositeProducer/test/validation/gen_matching/PbPb2023_DStarGenMatchTailRawTrack_condor.py
```

Condor helper:

```text
VertexCompositeProducer/test/condor_dstar_genmatch_tail_rawtrack/submit_dstar_genmatch_tail_rawtrack_condor.sh
```

README:

```text
VertexCompositeProducer/test/condor_dstar_genmatch_tail_rawtrack/README.md
```

Default input list:

```text
VertexCompositeProducer/test/files_prompt_nov30
```

The list currently has 2232 input files. The condor helper submits one job per input file.

Default output directory:

```text
root://cluster142.knu.ac.kr//store/user/junseok/DstarAnalysis/dstar_genmatch_tail_rawtrack_20260524
```

Per-job output naming:

```text
dstar_genmatch_tail_rawtrack_<idx>.root
```

Default job settings:

```text
threads = 1
streams = 1
maxEvents = -1
JobFlavour = tomorrow
request_memory = 8 GB
```

Submit a short 10-file test:

```bash
cd /afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/condor_dstar_genmatch_tail_rawtrack
./submit_dstar_genmatch_tail_rawtrack_condor.sh 10
```

Submit all 2232 files:

```bash
cd /afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/condor_dstar_genmatch_tail_rawtrack
./submit_dstar_genmatch_tail_rawtrack_condor.sh all
```

Custom output example:

```bash
./submit_dstar_genmatch_tail_rawtrack_condor.sh all root://cluster142.knu.ac.kr//store/user/junseok/DstarAnalysis/dstar_genmatch_tail_rawtrack_20260524_v2 tomorrow 1 1 -1
```

Local cfg validation:

```text
Ran 1 event with PbPb2023_DStarGenMatchTailRawTrack_condor.py.
Output:
VertexCompositeProducer/test/condor_dstar_genmatch_tail_rawtrack/test_output/dstar_genmatch_tail_rawtrack_test001.root
Log:
VertexCompositeProducer/test/condor_dstar_genmatch_tail_rawtrack/test_cmsrun_1evt.log
```

Important branches for the study:

```text
pTD2
matchGen_slowPion_rawTrackPt
matchGen_D1pT
matchGen_slowPion_candidatePtOverGenPt
matchGen_slowPion_rawTrackPtOverGenPt
matchGen_slowPion_dR
matchGen_slowPion_rawTrack_dR
matchGen_slowPion_rawTrackToCandidate_dR
matchGen_deltaMass_current
matchGen_deltaMass_rawSlowPion
matchGen_slowPion_candidateMatched
matchGen_slowPion_rawTrackMatched
```

## 2026-05-25 Condor completion check

Full-list condor cluster `11623822` is no longer in `condor_q`.

Log status:

```text
total jobs/logs = 2232
return value 0 = 2231
return value 85 = 1
failed job = 0383
failed input = /store/user/junseok/Genproduction/RECO_MINIAOD_DStarKpipiPU_Prompt_T2Vandbilt_CMSSW_13_2_10_29Oct25_v1/DStarKpipi_Prompt_ForcedD0Decay/crab_RECO_MINIAOD_DStarKpipiPU_Prompt_T2Vandbilt_CMSSW_13_2_10_29Oct25_v1/251031_044822/0000/step4_742.root
failure reason = transient XRootD FileReadError / Operation expired while reading the input file
events reported in logs = 2872812
```

The failed job did not process events. This looks like an input-read / XRootD timeout, not a D0Fitter, DStarFitter, or PAT6 failure.

Aggregated PAT6 tail counters from the condor logs:

```text
matchGEN inclusive:
  low=1264 peak=41949 high=3039 outside=13 high/peak=0.072445

strict no-FSR && !isSwap:
  low=1075 peak=35536 high=2571 outside=13 high/peak=0.072349

strict no-FSR && !isSwap && slowPi dR < 0.01:
  low=403 peak=33351 high=920 outside=3 high/peak=0.027585

strict no-FSR && !isSwap && slowPi dR < 0.005:
  low=141 peak=27272 high=359 outside=1 high/peak=0.013164

raw-track slowPi matched:
  low=1118 peak=35662 high=2647 outside=34 high/peak=0.074225

candidate-p4 slowPi matched:
  low=1075 peak=35536 high=2571 outside=13 high/peak=0.072349

raw-track pT/gen pT outside [0.8,1.2]:
  low=130 peak=134 high=743 outside=23 high/peak=5.544776

candidate-p4 pT/gen pT outside [0.8,1.2]:
  low=129 peak=134 high=746 outside=21 high/peak=5.567164
```

Current stored delta-mass region versus raw-slow-pion delta-mass region:

```text
current low  -> raw low=659 raw peak=496 raw high=4 raw outside=0
current peak -> raw low=186 raw peak=35337 raw high=188 raw outside=0
current high -> raw low=3 raw peak=792 raw high=1954 raw outside=0
current out  -> raw low=2 raw peak=3 raw high=32 raw outside=0
```

Remote output access check:

```text
proxy file = VertexCompositeProducer/test/myProxy
proxy status = expired
xrdcp test = failed with "Auth failed: No protocols left to try"
```

The production logs are available locally under:

```text
VertexCompositeProducer/test/condor_dstar_genmatch_tail_rawtrack/logs
```

The ROOT output endpoint is:

```text
root://cluster142.knu.ac.kr//store/user/junseok/DstarAnalysis/dstar_genmatch_tail_rawtrack_20260524
```

## 2026-05-25 fetched ROOT check after proxy refresh

Default shell proxy became valid again. The submission proxy file `VertexCompositeProducer/test/myProxy` is still expired, but endpoint read access works with the default proxy.

Endpoint listing:

```text
files listed = 2233
production files = 2232
extra file = dstar_genmatch_tail_rawtrack_endpointcheck_20260524_214115.root
failed job output dstar_genmatch_tail_rawtrack_0383.root size = 54871 bytes
```

Fetched ROOT files:

```text
VertexCompositeProducer/test/condor_dstar_genmatch_tail_rawtrack/fetched_check/dstar_genmatch_tail_rawtrack_0000.root
VertexCompositeProducer/test/condor_dstar_genmatch_tail_rawtrack/fetched_check/dstar_genmatch_tail_rawtrack_2220.root
VertexCompositeProducer/test/condor_dstar_genmatch_tail_rawtrack/fetched_check/dstar_genmatch_tail_rawtrack_2230.root
```

The raw slow-pion diagnostic branches are present in `dStarana_mc/PATCompositeNtuple`:

```text
matchGen_deltaMass_current
matchGen_deltaMass_rawSlowPion
matchGen_slowPion_rawTrackPt
matchGen_slowPion_rawTrackPtOverGenPt
matchGen_slowPion_candidatePtOverGenPt
matchGen_slowPion_rawTrack_dR
matchGen_slowPion_rawTrackToCandidate_dR
matchGen_slowPion_candidateMatched
matchGen_slowPion_rawTrackMatched
matchGen_validDstarChain
isSwap
```

ROOT check macro:

```text
VertexCompositeProducer/test/condor_dstar_genmatch_tail_rawtrack/check_raw_slowpi.C
```

Results for files `0000 + 2220`, using `matchGen_validDstarChain && !isSwap && matchGen_slowPion_candidateMatched`:

```text
current peak [0.144,0.147) = 22
current high [0.147,0.160) = 2
raw peak     [0.144,0.147) = 22
raw high     [0.147,0.160) = 2
current high -> raw peak = 0
current high -> raw high = 2
rawTrackPt/gen outside [0.8,1.2] = 2
rawTrack dR < 0.01 = 22
rawTrack dR >= 0.01 = 2
```

In these two files, raw slow-pion delta mass does not recover the two high-tail candidates.

Result for file `2230`, selected because the log matrix had `current high -> raw peak`:

```text
current peak [0.144,0.147) = 17
current high [0.147,0.160) = 2
raw peak     [0.144,0.147) = 18
raw high     [0.147,0.160) = 2
current high -> raw peak = 1
current high -> raw high = 1
current peak -> raw high = 1
rawTrackPt/gen outside [0.8,1.2] = 1
rawTrack dR < 0.01 = 17
rawTrack dR >= 0.01 = 3
```

Interpretation from the fetched ROOT files: the raw slow-pion branch is usable and confirms a mixed origin. Some high-tail candidates move back to the peak when the raw slow-pion track is used, but some remain high even with raw slow-pion kinematics. The remaining high-tail component is correlated with worse slow-pion raw-track/gen agreement or larger raw-track dR.

## 2026-05-25 matched-signal double-Gaussian fit

Created fitting tools:

```text
VertexCompositeProducer/test/condor_dstar_genmatch_tail_rawtrack/fit_matched_signal_double_gaus.C
VertexCompositeProducer/test/condor_dstar_genmatch_tail_rawtrack/fit_matched_signal_double_gaus_batch.py
```

The batch Python tool is used because AFS quota is nearly full and full remote TTree streaming was too slow. It copies a small batch of remote ROOT files into a repo-local cache, fills the histograms, and deletes the cache before the next batch.

Selection used for the plots/fits:

```text
matchGen_validDstarChain && !isSwap && matchGen_slowPion_candidateMatched
```

This is a matched-signal diagnostic selection, not an analysis selection.

First 300 production ROOT files were processed:

```text
opened files = 300
skipped files = 0
tree entries = 386048
selected current candidates = 5315
selected raw candidates = 5316
fit range = 0.140 < DeltaM < 0.155 GeV
hist range = 0.139 < DeltaM < 0.160 GeV
model = common-mean double Gaussian
```

Fit output:

```text
VertexCompositeProducer/test/condor_dstar_genmatch_tail_rawtrack/plots/batch_first300_20260525
```

Current DeltaM:

```text
entries = 5315
mean = 0.14550140 +/- 0.00000862 GeV
sigma_core = 0.417592 +/- 0.018782 MeV
sigma_wide = 1.080102 +/- 0.057500 MeV
core_fraction = 0.627976
chi2_ndf = 290.348 / 194
```

Raw slow-pion DeltaM:

```text
entries = 5316
mean = 0.14549414 +/- 0.00000851 GeV
sigma_core = 0.558946 +/- 0.008154 MeV
sigma_wide = 5.320839 +/- 1.037690 MeV
core_fraction = 0.888147
chi2_ndf = 188.865 / 180
```

Plots:

```text
current_deltaM_double_gaus.png/pdf
raw_slowpi_deltaM_double_gaus.png/pdf
current_vs_raw_deltaM_normalized.png/pdf
matched_signal_double_gaus_fit.root
double_gaus_fit_results.txt
```
