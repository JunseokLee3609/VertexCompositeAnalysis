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
