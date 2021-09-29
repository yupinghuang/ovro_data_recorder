#!/bin/bash

if [[ -e /home/ubuntu/anaconda3/envs/casa ]]; then
  mkdir -p /home/ubuntu/anaconda3/envs/casa/lib/casa/data/
  cd /home/ubuntu/anaconda3/envs/casa/lib/casa/data/
  rsync -avz rsync://casa-rsync.nrao.edu/casa-data .
fi