#!/bin/bash

PEERS=4
for i in $(seq 1 $PEERS)
do
CMD="./run_st4_multi.sh $PEERS $i"
echo $CMD
$CMD &
done
