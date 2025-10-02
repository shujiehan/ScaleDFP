#!/bin/bash

for ((COLLECTOR=1; COLLECTOR<=128; COLLECTOR=COLLECTOR*2))
do
    METHOD="random"
    SEED=42
    CMD="python straggler_simulator.py $COLLECTOR $METHOD $SEED"
    echo "$CMD"
    $CMD
    
    for SEED in {1000..4000..1000}
    do
      CMD="python straggler_simulator.py $COLLECTOR $METHOD $SEED"
      echo "$CMD"
      $CMD
    done
    
    METHOD="random-timeout"
    SEED=42
    CMD="python straggler_simulator.py $COLLECTOR $METHOD $SEED"
    echo "$CMD"
    $CMD
    
    for SEED in {1000..4000..1000}
    do
      CMD="python straggler_simulator.py $COLLECTOR $METHOD $SEED"
      echo "$CMD"
      $CMD
    done
    
    METHOD="intelligent"
    SEED=42
    PROB=0.52
    CMD="python straggler_simulator.py $COLLECTOR $METHOD $SEED $PROB"
    echo "$CMD"
    $CMD
    
    for SEED in {1000..4000..1000}
    do
      CMD="python straggler_simulator.py $COLLECTOR $METHOD $SEED $PROB"
      echo "$CMD"
      $CMD
    done

done
