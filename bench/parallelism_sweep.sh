#!/usr/bin/env bash
# PARALLELISM_FACTOR oversubscription sweep: can more shards per core hide the synchronous
# MT_EMIT ack stall of single-threaded shard scans? Captures Q2/Q3/Q5 (emit-heavy joins) and Q9b
# (regression check). BENCH_SKIP_UPLOAD=1 recommended.
set -uo pipefail
cd "$(dirname "$0")/.."
mkdir -p bench/reports
OUT="${1:-bench/reports/parallelism-sweep-$(date +%Y%m%d-%H%M%S).txt}"
: > "$OUT"
# config rows: "label parallelism_factor" (8 = bench/.env baseline)
configs=(
  "pf8_baseline   8"
  "pf16           16"
  "pf24           24"
)
extract_target_queries() {
  awk '
    /^### Q2 |^### Q3 |^### Q5 |^### Q9b / { print; keep=1; next }
    keep && /^elapsed:/ { print; keep=0; next }
  ' "$1"
}
for c in "${configs[@]}"; do
  set -- $c; label=$1 pf=$2
  echo "=================== SWEEP $label (PARALLELISM_FACTOR=$pf) ===================" | tee -a "$OUT"
  env BENCH_PARALLELISM_FACTOR="$pf" ./bench/run.sh > "/tmp/lh-pf-sweep-$label.log" 2>&1
  rc=$?
  if [ $rc -ne 0 ]; then echo "  RUN FAILED rc=$rc (see /tmp/lh-pf-sweep-$label.log)" | tee -a "$OUT"; fi
  extract_target_queries "/tmp/lh-pf-sweep-$label.log" | tee -a "$OUT"
  grep -E "CLUSTER_NODES|9001|fingerprint|VM crashed|^  FAIL" "/tmp/lh-pf-sweep-$label.log" | tee -a "$OUT"
  echo | tee -a "$OUT"
done
echo "=== SWEEP DONE (report: ${OUT}) ===" | tee -a "$OUT"
