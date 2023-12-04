#!/bin/bash
repo=Cornus-2.0
source $HOME/$repo/tools/setup_env.sh
mkdir -p $HOME/$repo/outputs/
cd $HOME/$repo/src
make -j16 $1
