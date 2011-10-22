#!/bin/bash

BINARY_DIR="../build/sim"
VERBOSE_LEVEL=0
OUTPUT_DIR="out"
ANALYSIS_SCRIPTS_DIR="analysis"

non_pref_penalty_factor=2.0
use_prefs=${USE_PREFS:-1}

job_size_lambda=${JOB_SIZE_LAMBDA:-0.05}
job_arrival_scaling_factor=${JOB_ARRIVAL_SCALING_FACTOR:-20}
task_duration_scaling_factor=${TASK_DURATION_SCALING_FACTOR:-30}

simulation_runtime=10000

basename="rt-${simulation_runtime}--jsl-${job_size_lambda}--jasf-${job_arrival_scaling_factor}--tdsf-${task_duration_scaling_factor}--prefs-${use_prefs}--penalty-${non_pref_penalty_factor}"

echo "Running simulation experiment..."
echo "basename: ${basename}"
#${BINARY_DIR}/simulation --log_dir="logs/" --v=${VERBOSE_LEVEL} \
#  --simulation_runtime=${simulation_runtime} \
#  --job_size_lambda=${job_size_lambda} \
#  --task_duration_scaling_factor=${task_duration_scaling_factor} \
#  --job_arrival_scaling_factor=${job_arrival_scaling_factor} \
#  --use_prefs=${use_prefs} \
#  --non_preferred_penalty_factor=${non_pref_penalty_factor} \
#  --output="${OUTPUT_DIR}/${basename}.csv"

echo "Postprocessing results and generating graphs..."
mkdir -p ${OUTPUT_DIR}/${basename}
python ${ANALYSIS_SCRIPTS_DIR}/postprocess.py ${OUTPUT_DIR}/${basename}.csv \
  ${OUTPUT_DIR}/${basename}
