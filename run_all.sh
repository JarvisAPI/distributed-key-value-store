#!/bin/bash
# Runs the specified jar file on all planet lab nodes in background
# Usage:
#   "./run_all.sh <jar file path>"

nodes="$(cat nodes.txt)"
config="$(cat config.txt)"
key=$KEY
user="ubc_cpen431_8"
file=$1
command="nohup java -Xmx64m -jar $file $config > out_will.log 2> err_will.log < /dev/null &"

if [ $# -eq 0 ]; then
  echo "No arguments supplied"
fi

if [ ${file: -3} == "jar" ] ; then
  for node in $nodes; do
    ssh -l $user -i "$key" $node $command
  done
else
  echo "File is not an executable .jar file";
fi
