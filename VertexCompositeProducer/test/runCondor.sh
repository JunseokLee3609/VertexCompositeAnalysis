#!/bin/bash
WKDIR=`pwd`

export SCRAM_ARCH=el8_amd64_gcc12
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd $WKDIR 
eval `scramv1 runtime -sh`

export X509_USER_PROXY=myProxy

echo "Setup complete"
echo "Will run:"
echo "$@"
eval "$@"

