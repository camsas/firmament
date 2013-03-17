#!/bin/bash
OUTDIR="/tmp/firmament-debug"
DOT_FILE="${OUTDIR}/${TIMESTAMP}.gv"
TIMESTAMP=$(date '+%s')
OUTPUT="${OUTDIR}/${TIMESTAMP}.png"

mkdir -p ${OUTDIR}
python dimacs_to_dot.py /tmp/debug.dm > ${DOT_FILE}
dot -Tpng ${DOT_FILE} > ${OUTPUT}
