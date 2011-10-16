#!/bin/bash

BINARY_DIR="../build/sim"
VERBOSE_LEVEL=2
OUTPUT_DIR="out"
ANALYSIS_SCRIPTS_DIR="analysis"

non_pref_penalty_factor=2.0
use_prefs=1

job_arrival_scaling_factor=1.0
task_duration_scaling_factor=20

simulation_runtime=1000

basename="rt-${simulation_runtime}--jasf-${job_arrival_scaling_factor}--tdsf-${task_duration_scaling_factor}--prefs-${use_prefs}--penalty-${non_pref_penalty_factor}"

echo "Running simulation experiment..."
${BINARY_DIR}/simulation --log_dir="logs/" --v=${VERBOSE_LEVEL} \
  --simulation_runtime=${simulation_runtime} \
  --task_duration_scaling_factor=${task_duration_scaling_factor} \
  --job_arrival_scaling_factor=${job_arrival_scaling_factor} \
  --use_prefs=${use_prefs} \
  --non_preferred_penalty_factor=${non_pref_penalty_factor} \
  --output="${OUTPUT_DIR}/${basename}.csv"

echo "Postprocessing results and generating graphs..."
mkdir -p ${OUTPUT_DIR}/${basename}
python ${ANALYSIS_SCRIPTS_DIR}/tpanalysis.py ${OUTPUT_DIR}/${basename}.csv \
  ${OUTPUT_DIR}/${basename}
