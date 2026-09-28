#!/usr/bin/env bash
# Sweeps the shard/thread/S3-connection shape to test whether IO-bound throughput is capped by
# under-concurrent fetching (few big shards + high S3 concurrency). BENCH_SKIP_UPLOAD=1
# recommended.
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
