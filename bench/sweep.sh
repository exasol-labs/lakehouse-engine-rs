#!/usr/bin/env bash
# Task 7.1 sweep driver: vary DataFusion threads/partitions per UDF instance,
# hold NR_OF_CORES/PARALLELISM_FACTOR (from .env). Reuses staged .so+SLC
# (BENCH_SKIP_UPLOAD=1). Prints Q1-Q4 elapsed per config. NOT a spec feature.
set -uo pipefail
cd "$(dirname "$0")/.."
OUT="${1:-/tmp/lh-sweep.txt}"
: > "$OUT"
# config rows: "MODE THREADS PARTITIONS label"
configs=(
  "FIXED 1 1 t1p1"
  "FIXED 2 2 t2p2"
  "FIXED 8 8 t8p8"
)
for c in "${configs[@]}"; do
  set -- $c; mode=$1 thr=$2 part=$3 label=$4
  echo "=================== SWEEP $label ($mode threads=$thr partitions=$part) ===================" | tee -a "$OUT"
  BENCH_DF_THREADING_MODE=$mode BENCH_DF_THREADS_PER_UDF=$thr BENCH_DF_TARGET_PARTITIONS=$part \
    LAKEHOUSE_UDF_DEBUG_LEVEL=info \
    ./bench/run.sh > "/tmp/lh-sweep-$label.log" 2>&1
  rc=$?
  if [ $rc -ne 0 ]; then echo "  RUN FAILED rc=$rc (see /tmp/lh-sweep-$label.log)" | tee -a "$OUT"; fi
  grep -E "^### Q|^elapsed:|9001|fingerprint|VM crashed|FAIL" "/tmp/lh-sweep-$label.log" | tee -a "$OUT"
  echo | tee -a "$OUT"
done
echo "=== SWEEP DONE ===" | tee -a "$OUT"
