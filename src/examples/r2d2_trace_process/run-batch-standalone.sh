#!/bin/bash
DIR=/mnt/datasets
COUNT=0
OUT_DIR=/mnt/datasets/results
PATTERN=$1

#for d in `cd ${DIR}; ls -d *${PATTERN}*/`; do
for d in `cd ${DIR}; ls *${PATTERN}*.tar.bz2`; do
  f=`echo ${d} | sed -s 's/.tar.bz2//'`
  if [[ ! -d ${DIR}/${f} ]]; then
    cd ${DIR}
    ./extract.sh ${f}
    cd -
  else
    echo "No extraction required."
  fi
#  mkdir -p ${OUT_DIR}/$d
  echo "PACKET JOIN"
  ./packet_join_task ${DIR}/${f}/dag_cap_0 ${DIR}/${f}/dag_cap_1 0 ${COUNT} --v=1 --logtostderr 1>${OUT_DIR}/${f}/results.csv
  echo "TRACE 0 ANALYSIS"
#  ./simple_trace_analysis_task ${DIR}/${f}/dag_cap_0 0 ${COUNT} --v=2 --logtostderr 1>${OUT_DIR}/${f}/results-dag0.csv
  echo "TRACE 1 ANALYSIS"
#  ./simple_trace_analysis_task ${DIR}/${f}/dag_cap_1 0 ${COUNT} --v=2 --logtostderr 1>${OUT_DIR}/${f}/results-dag1.csv
  echo "TRACE 0 BANDWIDTH"
  ./aggregate_bandwidth_analysis_task ${DIR}/${f}/dag_cap_0 0 ${COUNT} --v=1 --logtostderr 1>${OUT_DIR}/${f}/results-bw-dag0.csv
  echo "TRACE 1 BANDWIDTH"
  ./aggregate_bandwidth_analysis_task ${DIR}/${f}/dag_cap_1 0 ${COUNT} --v=1 --logtostderr 1>${OUT_DIR}/${f}/results-bw-dag1.csv
done
