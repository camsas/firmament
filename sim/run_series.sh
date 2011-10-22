#!/bin/bash

for prefs in 0 1; do
  export USE_PREFS=${prefs}
  for jsl in 0.05 0.1 0.2 1.0; do
    export JOB_SIZE_LAMBDA=${jsl}
    for jasf in 10 20 30; do
      export JOB_ARRIVAL_SCALING_FACTOR=${jasf}
      for tdsf in 10 20 30; do
        export TASK_DURATION_SCALING_FACTOR=${tdsf}
        ./run_experiment.sh
      done
    done
  done
done

