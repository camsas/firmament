#!/bin/bash
OUTDIR="/tmp/firmament-debug"
TIMESTAMP=$(date '+%s')

mkdir -p ${OUTDIR}
for f in `ls ${OUTDIR}/debug_*.dm`; do
  #DOT_FILE="${f}_${TIMESTAMP}.gv"
  #OUTPUT="${f}_${TIMESTAMP}.png"
  DOT_FILE="${f}.gv"
  OUTPUT="${f}.png"
  python scripts/dimacs_to_dot.py ${f} `echo ${f} | sed -e s/debug_/debug-flow_/` > ${DOT_FILE}
  dot -Tpng ${DOT_FILE} > ${OUTPUT}
done
