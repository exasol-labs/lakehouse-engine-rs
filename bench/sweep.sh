#!/usr/bin/env bash
# Task 9 sweep driver: verify the "IO-bound throughput is capped by under-concurrent
# fetching" hypothesis by sweeping the shard/thread/connection shape. Each config row
# sets PARALLELISM_FACTOR (shard fan-out), DATAFUSION_THREADING_MODE, and
# S3_MAX_CONNECTIONS; "-" leaves a knob unset so .env / AUTO-derivation applies.
# Reuses staged .so+SLC (BENCH_SKIP_UPLOAD=1 recommended). Prints Q1-Q4 elapsed per
# config. NOT a spec feature.
#
# Hypothesis shape (few big shards + high S3 concurrency):
#   PARALLELISM_FACTOR=1  -> G = node_count shards (one per node)
#   DATAFUSION_THREADING_MODE=AUTO -> that one instance gets all the node's cores
#   S3_MAX_CONNECTIONS swept unset(AUTO)/32/64/128 -> saturate network/IO
set -uo pipefail
cd "$(dirname "$0")/.."
OUT="${1:-/tmp/lh-sweep.txt}"
: > "$OUT"
# config rows: "label PARALLELISM_FACTOR THREADING_MODE S3_MAX_CONNECTIONS" ("-" = unset)
configs=(
  "baseline_pf8          8  -     -"
  "pf1_auto_s3auto       1  AUTO  -"
  "pf1_auto_s3_32        1  AUTO  32"
  "pf1_auto_s3_64        1  AUTO  64"
  "pf1_auto_s3_128       1  AUTO  128"
)
for c in "${configs[@]}"; do
  set -- $c; label=$1 pf=$2 mode=$3 s3=$4
  echo "=================== SWEEP $label (PF=$pf mode=$mode s3_max_conn=$s3) ===================" | tee -a "$OUT"
  # "-" -> unset (leave to .env default / AUTO-derivation). Collect into one args
  # array so an unset knob adds no empty argument (safe under `set -u`).
  envargs=(LAKEHOUSE_UDF_DEBUG_LEVEL=info)
  [ "$pf"   != "-" ] && envargs+=("BENCH_PARALLELISM_FACTOR=$pf")
  [ "$mode" != "-" ] && envargs+=("BENCH_DF_THREADING_MODE=$mode")
  [ "$s3"   != "-" ] && envargs+=("BENCH_S3_MAX_CONNECTIONS=$s3")
  env "${envargs[@]}" ./bench/run.sh > "/tmp/lh-sweep-$label.log" 2>&1
  rc=$?
  if [ $rc -ne 0 ]; then echo "  RUN FAILED rc=$rc (see /tmp/lh-sweep-$label.log)" | tee -a "$OUT"; fi
  grep -E "^### Q|^elapsed:|CLUSTER_NODES|9001|fingerprint|VM crashed|FAIL" "/tmp/lh-sweep-$label.log" | tee -a "$OUT"
  echo | tee -a "$OUT"
done
echo "=== SWEEP DONE ===" | tee -a "$OUT"
