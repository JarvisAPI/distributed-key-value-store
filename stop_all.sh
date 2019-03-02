#!/bin/bash
# Kills all servers started on planet lab nodes in nodes.txt
# 
# Usage:
#   "./stop_all.sh"
#
# $KEY should be path of your id_rsa file


nodes="$(cat nodes.txt)"
key=$HOME/.ssh/id_rsa
user="ubc_cpen431_8"
file=$1
command="sudo pkill -9 java"

for node in $nodes; do
  echo "Stopping all servers started on planet lab nodes"
  ssh -v -l $user -i "$key" $node $command
done
