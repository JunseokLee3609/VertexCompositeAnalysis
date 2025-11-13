# VertexCompositeAnalysis

Resonace decay reconstruction algorithms with ```VertexCompositeCandiate``` collection in cmssw. Compatible with 2023 PbPb datafomat. The package is fully orthogonal to the ```HiForest``` framework to be combined with other objects.

This branch support several channels, and to be updated in the future.
- $D^{0} \to K+\pi$
- $D^{*+/-} \to D^{0} + \pi \to K+\pi+\pi$
- $D^{+/-} \to K+\pi+\pi$

The  $D^{*+/-}$ decay involves 2-layer decay involving a $D^{0}$, thus first runs off from $D^{0}$ decay channel.

## Package description 
The package includes two section ```VertexCompositeProducer``` for candidate reconstruction, ```VertexCompositeAnalyzer``` for tree/Ntuplizer modules. 

- Producers includes candidate producer and fitter for each candidate.
    - E.g. ```VertexCompositeProducer/python/generalD0Candidates_cfi.py```
- For skimming, one can choose to save objects in tree or flat Ntuple. Refer to ```VertexCompositeAnalyzer/python/d0analyzer_tree_cfi.py``` for tree and ```VertexCompositeAnalyzer/python/d0analyzer_ntp_cfi``` for flat Ntuple.

Be aware of the default reconstruction parameters and check if it fits your requirements.

## To Do's
- Configuration for MC (easy)
- Update to latest event selection modules and GO's (easy)
- Decay channels involving leptonic decay, probably good idea to use subpackage ```HiSkim``` in oniaTree code. (normal)
- Optimize 3-prong decay reco, to avoid looping over hundreds of charged tracks. (need some study)

## How to run

For reconstruction of $D^{0}, D^{*+}$ with 2023 PbPb data
```bash 
#LXplus, bash, cmssw-el8 apptainer

mkdir  <your_directory>
cd <your_directpry>

cmsrel CMSSW_13_2_11

cd CMSSW_13_2_11/src
cmsenv

git clone git@github.com:JunseokLee3609/VertexCompositeAnalysis.git -b test

cd VertexCompositeAnalysis

scram b -j8
cd VertexCompositeProducer/test
```
-------
# How to submit Crab Job
edit crabConfig_MB_DataStep2MVA.py
* replace 'junseok' with your storage
* replace Prime0 with the dataset you are gonna process
crab submit crabConfig_MB_DataStep2MVA.py
## Run bulk of data file 
if you want to process bulk of file from PDs at once, make shell file.
``` bash 
#!/bin/bash -x

rm files2023MB.txt;

for i in {0..7}
do
    dasgoclient --query="file dataset=/HIPhysicsRawPrime$i/HIRun2023A-PromptReco-v2/MINIAOD" >> files2023MB.txt
done
```
* replace idx of dataset with  what you woudld like to process
* Open crabConfig_MB_DataStep2MVA.py
```python  
#comment out config.Data.inputDataset and Add 
config.Data.userInputFiles = open('files2023MB.txt').readlines()
```
crab submit crabConfig_MB_Step2MVA.py




Multi crab configuration in ```jobCfg``` to submit multiple jobs to PD's.

-------
# How to submit Condor Job

## IMPORTANT: Path updates required!
Before running any condor jobs, you **MUST** update the hardcoded paths in two files:

### 1. Update runCondor_Data.sh
Edit `VertexCompositeProducer/test/runCondor_Data.sh`:
```bash
# BEFORE (junseok's path):
cd /afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test

# AFTER (your path, example for user 'myname'):
cd /afs/cern.ch/user/m/myname/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test
```
This directory must contain your CMSSW installation where VertexCompositeAnalysis is cloned.

### 2. Update runCondor_MC.sh
Edit `VertexCompositeProducer/test/runCondor_MC.sh` with the same path:
```bash
# BEFORE (junseok's path):
cd /afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test

# AFTER (your path):
cd /afs/cern.ch/user/m/myname/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test
```

To find your actual path, run:
```bash
pwd  # when you're in VertexCompositeProducer/test directory
```

## Output file locations

### For Data (PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py)
Output files are automatically saved to:
```
/eos/cms/store/group/phys_heavyions/junseok/DstarAnalysis/{dirName}/d0ana_tree_stepMVA_{outputSuffix}.root
```

**You MUST change this path** in the config file:
* Line 13: Change `/eos/cms/store/group/phys_heavyions/junseok/DstarAnalysis/` to your EOS storage path
* Example: `/eos/cms/store/group/phys_heavyions/myname/DstarAnalysis/`
* Or use your personal EOS: `/eos/user/m/myname/analysis/dstaranalysis/`

The path is constructed as:
```python
outDir = f'/eos/cms/store/group/phys_heavyions/junseok/DstarAnalysis/{dirName}'
process.TFileService = cms.Service("TFileService",
    fileName = cms.string(f'file:{outDir}/d0ana_tree_stepMVA_{outputSuffix}.root')
)
```

### For MC (PbPb2023_D0BothAndDStar_MB_cfg_mc_Step2MVA_Condor_v1.py)
Output files are automatically saved to:
```
/eos/cms/store/group/phys_heavyions/junseok/DstarAnalysis/{subDir}/d0ana_tree_stepMVA_{outputSuffix}.root
```

**You MUST change this path** in the config file:
* Line 12: Change `/eos/cms/store/group/phys_heavyions/junseok/DstarAnalysis` to your EOS storage path
* Example: `/eos/cms/store/group/phys_heavyions/myname/DstarAnalysis`

The path is constructed as:
```python
out_base_dir = '/eos/cms/store/group/phys_heavyions/junseok/DstarAnalysis'
out_dir = f"{out_base_dir}/{subDir.strip('/')}" if subDir else out_base_dir
outfile = f"file:{out_dir}/d0ana_tree_stepMVA_{out_suffix}.root"
process.TFileService = cms.Service("TFileService",
    fileName = cms.string(outfile)
)
```

**Important notes on output location:**
* Make sure the EOS directory exists: `eos mkdir -p /eos/cms/store/group/phys_heavyions/yourname/DstarAnalysis`
* Verify you have write permission to the target directory
* Jobs will fail if the output directory doesn't exist or is not writable
* Do NOT use `/afs` for output - use `/eos` for large files

## Preparation
Before submitting condor jobs, you need to generate a proxy certificate for grid authentication. Add this alias to your `~/.bashrc`:
```bash
alias vomsout='function __voms() { voms-proxy-init --voms cms --out myProxy; unset -f __voms;}; __voms'
```

Then generate the proxy:
```bash
vomsout
```
This creates a `myProxy` file in your current directory which will be used by condor jobs.

## Generate file lists with filegenerator.sh
The `filegenerator.sh` script helps generate indexed file lists from DAS queries or existing files. Each line in the output contains: `<file_path> <index> [<dataset_or_subdir>]`

### Basic usage:
```bash
# Direct from DAS query with range filtering for HIPhysicsRawPrime datasets
./filegenerator.sh -Q 'file dataset=/HIPhysicsRawPrime*/HIRun2023A-PromptReco-v2/MINIAOD instance=prod/global' -r '0:3' -o files_rawprime_0_3.list

# From existing file list
./filegenerator.sh -i files.txt -o files_indexed.txt

# With custom subdirectory tag (useful for MC)
./filegenerator.sh -i files.txt -d nonprompt -o files_nonpromptMC.list
```

### Options:
* `-Q <query>`: Run dasgoclient query directly
* `-r <start:end>`: Filter by index range (e.g., `0:9`, `13:15`)
* `-i <file>`: Input file list
* `-o <file>`: Output file (auto-suffixed with range if `-r` specified)
* `-d <subdir>`: Append subdirectory as 3rd column
* `-b <N>`: Base index (default: 0)
* `-p <auto|N>`: Zero-pad width (default: auto)

Example output format:
```
/store/hidata/HIRun2023A/HIPhysicsRawPrime13/MINIAOD/PromptReco-v2/.../file.root 0000 HIPhysicsRawPrime13
/store/hidata/HIRun2023A/HIPhysicsRawPrime13/MINIAOD/PromptReco-v2/.../file.root 0001 HIPhysicsRawPrime13
```

## Submit jobs to condor

### For Data:
1. Generate the file list:
```bash
./filegenerator.sh -Q 'file dataset=/HIPhysicsRawPrime13/HIRun2023A-PromptReco-v2/MINIAOD instance=prod/global' -o file_Data_HIPhysicsRawPrime13.list -d HIPhysicsRawPrime13
```

2. **Copy and edit your config file** (do not modify the original):
```bash
cp PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py MyD0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py
# Edit MyD0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:
#   - Line 13: Change outDir path to your EOS storage
```

3. Edit `condor_Data.sub`:
   * Line 5: Update `py_script = MyD0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py` (your edited config)
   * Line 31: Update file list: `queue inFName,idx,dataset from file_Data_HIPhysicsRawPrime13.list`
   * Modify `request_memory` and `request_cpus` if needed

4. Create logs directory and submit:
```bash
mkdir -p logs
condor_submit condor_Data.sub
```

### For MC:
1. Generate the file list with subdirectory tag:
```bash
./filegenerator.sh -i your_mc_files.txt -d nonprompt -o files_nonpromptMC.list
```

2. **Copy and edit your config file** (do not modify the original):
```bash
cp PbPb2023_D0BothAndDStar_MB_cfg_mc_Step2MVA_Condor_v1.py MyD0BothAndDStar_MB_cfg_mc_Step2MVA_Condor_v1.py
# Edit MyD0BothAndDStar_MB_cfg_mc_Step2MVA_Condor_v1.py:
#   - Line 12: Change out_base_dir to your EOS storage
```

3. Edit `condor_MC.sub`:
   * Line 5: Update `py_script = MyD0BothAndDStar_MB_cfg_mc_Step2MVA_Condor_v1.py` (your edited config)
   * Line 31: Update file list: `queue inFName,idx,subdir from files_nonpromptMC.list`

4. Create logs directory and submit:
```bash
mkdir -p logs
condor_submit condor_MC.sub
```

## Monitor jobs
```bash
# Check job status
condor_q

# Check specific user jobs
condor_q -submitter $USER

# Remove jobs
condor_rm <job_id>

# Check logs
tail -f logs/job_0000.log
tail -f logs/job_0000.err
```

## Notes
* Condor submit files use `+JobFlavour = "tomorrow"` (max 1 day runtime)
* Jobs run in AlmaLinux9 singularity container with el8 CMSSW environment
* Output files are transferred back when jobs complete
* The `runCondor_Data.sh` and `runCondor_MC.sh` scripts set up the CMSSW environment and X509 proxy before running cmsRun
