#!/bin/bash

myurl=$1
filenamebase=$2

for i in 100 1000 10000 100000
do
  for j in $(seq 1 $i); do echo $myurl; done >> "${filenamebase}_${i}.txt"
done
