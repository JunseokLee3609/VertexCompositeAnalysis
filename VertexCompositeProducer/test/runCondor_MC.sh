#!/bin/bash
export SCRAM_ARCH=el8_amd64_gcc11
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd /afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test
eval `scramv1 runtime -sh`
export X509_USER_PROXY=myProxy
exec "$@"
