#!/bin/bash
source setup_env.sh
cd $HOME/Cornus-2.0/src
mkdir -p ${HOME}/Cornus-2.0/outputs
./rundb $1
