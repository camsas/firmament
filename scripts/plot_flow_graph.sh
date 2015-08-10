#!/bin/bash
#
# The Firmament project
# Copyright (c) 2013-2015 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
#
# Script to plot the DIMACS debug output as a dot/Graphviz graph. When invoked
# without any arguments, the script plots all flow graphs stored in
# /tmp/firmament-debug; if an argument is provided, it is interpreted as the ID
# of the flow graph to plot.
#
# Example:
# $ plot_flow_graph.sh 123    # plots debug_123.dm and debug-flow_123.dm

OUTDIR="/tmp/firmament-debug"
TIMESTAMP=$(date '+%s')

mkdir -p ${OUTDIR}
if [[ $# < 1 ]]; then
  FILES=$(ls ${OUTDIR}/debug_*.dm)
else
  FILES="${OUTDIR}/debug_$1.dm"
fi

for f in ${FILES}; do
  DOT_FILE="${f}.gv"
  OUTPUT="${f}.png"
  echo ${f}
  python scripts/dimacs_to_dot.py ${f} `echo ${f} | sed -e s/debug_/debug-flow_/` > ${DOT_FILE}
  dot -Tpng ${DOT_FILE} > ${OUTPUT}
done
