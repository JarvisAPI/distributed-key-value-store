#!/bin/bash
# Kills all servers started on planet lab nodes in nodes.txt
# 
# Usage:
#   "./stop_all.sh"
#
# $KEY should be path of your id_rsa file


nodes="$(cat nodes.txt)"
key=$KEY
user="ubc_cpen431_8"
file=$1
command="sudo pkill -9 java"

echo "Stopping all servers started on planet lab nodes"
for node in $nodes; do
    echo "Stopping $node"
    ssh -l $user -i "$key" $node $command
done
