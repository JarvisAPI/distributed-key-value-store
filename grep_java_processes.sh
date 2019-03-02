#!/bin/bash
# Performs a grep for java processes on all planet lab nodes in nodes.txt and prints it out in terminal
#
# Usage: 
#   "./grep_java_process.sh"
#
# $KEY should be path of your id_rsa file  


nodes="$(cat nodes.txt)"
key=$HOME/.ssh/id_rsa
user="ubc_cpen431_8"
command="ps -A | grep java"

for node in $nodes; do
  echo $node
  ssh -l $user -i "$key" $node $command
done
