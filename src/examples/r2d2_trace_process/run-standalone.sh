#!/bin/bash

TRACE_DIR=/home/mpg39/dag_cap_full_ts_tcp_pica8_x2cl_101212.1503
#TRACE_DIR=/home/mpg39/dag_cap_full_ts_tcp_pica8_101212.1401
TOTAL=100000
CHUNKS=2

CHUNKSIZE=$((${TOTAL} / ${CHUNKS}))
for i in `seq 0 $((${CHUNKS} - 1))`; do
  START=$((${CHUNKSIZE} * ${i}))
  time ./packet_join_task ${TRACE_DIR}/dag_cap_0 ${TRACE_DIR}/dag_cap_1 \
    ${START} ${CHUNKSIZE} --v=1 1>/tmp/test-part${i}.csv &
done
