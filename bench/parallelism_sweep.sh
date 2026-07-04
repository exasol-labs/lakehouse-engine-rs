#!/usr/bin/env bash
# Parallelism-factor oversubscription sweep (add-arithmetic-aggregate-pushdown-and-
# benchmark-suite, task 3.1; NOT a spec feature — same convention as sweep.sh).
#
# Hypothesis under test: each join-leg shard scan is single-threaded
# (df_threads_per_udf=1) and its ctx.emit_batch is a synchronous ZMQ MT_EMIT
# send-then-ack round-trip. At the shipped BENCH_PARALLELISM_FACTOR=8, this
# 2-node/8-core-per-node cluster gets G=16 shards = exactly 1 shard/core — no
# second shard queued up to keep a core busy while the first blocks on its emit
# ack. Oversubscribing (factor=16 -> G=32 = 2 shards/core, factor=24 -> G=48 =
# 3 shards/core) might hide that stall. UNVERIFIED going in; see
# crates/lakehouse-engine/src/adapter/mod.rs (resolve_parallelism_factor) and
# specs/_plans/add-arithmetic-aggregate-pushdown-and-benchmark-suite/decision-log.md.
#
# Reuses sweep.sh's driver shape (config rows run via `env KNOB=val ./bench/run.sh`,
# output filtered into a report) but sweeps a single knob and narrows the captured
# output to the queries this hypothesis is about: Q2/Q3/Q5 (raw-emit-heavy joins)
# plus Q9b (non-join wide-projection regression check — oversubscription must not
# make it worse). Reuses staged .so+SLC (BENCH_SKIP_UPLOAD=1 recommended, same as
# sweep.sh) to avoid redundant BucketFS uploads across sweep rows.
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
  # Prints only the "### Q2/Q3/Q5/Q9b ..." header + its "elapsed:" line, skipping
  # the CSV result body in between and every other query's header/elapsed pair.
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
