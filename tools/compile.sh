#!/bin/bash
source $HOME/Cornus-2.0/tools/setup_env.sh
mkdir -p $HOME/Cornus-2.0/outputs/
cd $HOME/Cornus-2.0/src
make -j16 $1

