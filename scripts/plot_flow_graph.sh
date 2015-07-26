#!/bin/bash
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
