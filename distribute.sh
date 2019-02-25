#!/bin/bash
# Distribute files or directories to planet lab nodes
# Usage:
#   "./distribute.sh <filename or dirname> <filename or dirname> ... "

nodes="$(cat nodes.txt)"
key=$KEY
user="ubc_cpen431_8"
files="$@"

if [ $# -eq 0 ]; then
  echo "No arguments supplied"
fi

for file in $files; do
  if [ -d "$file" ] ; then
    echo "Distributing directory $file to node";
    for node in $nodes; do
        echo "Host: $node"
        scp -o StrictHostKeyChecking=no -i "$key" -r $file $user@$node:~/
    done

  else
    if [ -f "$file" ] ; then
      echo "Distributing file $file to node";
      for node in $nodes; do
          echo "Host: $node"
        scp -o StrictHostKeyChecking=no -i "$key" $file $user@$node:~/
      done
    else
      echo "$file is not valid";
      exit 1
    fi
  fi
done
