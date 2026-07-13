#!/usr/bin/env bash
# DATAFUSION_BATCH_SIZE emit-path sweep (NOT a spec feature).
#
# Isolates ONE variable — the DataFusion RecordBatch size, which sets how many
# rows land per `ctx.emit_batch` and therefore how many synchronous MT_EMIT
# round-trips a raw full-emit requires — on the reduced-scale raw-emit workload
# used by the 2026-07-02 re-gate (native IMPORT of 10/60 lineitem files ≈ 30 M
# rows = 2.07 M rows/s ceiling; VS `CREATE TABLE AS SELECT *` filtered to the
# matching ≈ 33 M rows). Everything else is held at the shipped default winning
# shape: PARALLELISM_FACTOR=8 (G = CLUSTER_NODES × 8 = 16), threading AUTO.
#
# For each batch size it recreates ONLY the virtual schema (the adapter/scan
# scripts, connection, staged .so + SLC are reused — a VS property is resolved at
# createVirtualSchema time, no recompile), verifies the resolved DF_BATCH_SIZE in
# adapterNotes, then times the filtered CTAS emit (2 passes), dropping + FLUSHing
# between runs to respect the shared cluster's 10 GiB raw-size license.
#
#   ./bench/batch_size_sweep.sh            # sweep + aggregate regression check
#
# Requires bench/.env (remote cluster creds); reuses objects created by run.sh.
set -uo pipefail
cd "$(dirname "$0")/.."
[ -f bench/.env ] || { echo "ERROR: bench/.env required"; exit 1; }
set -a; . bench/.env; set +a
DSN="exasol://sys:${EXASOL_SYS_PASSWORD}@${EXASOL_HOST}:${LH_EXASOL_PORT:-8563}?validateservercertificate=0"
NS="${ICEBERG_NAMESPACE:-tpch}"
CORES="${BENCH_NR_OF_CORES:-8}"
PF="${BENCH_PARALLELISM_FACTOR:-8}"
KEY=33007128                 # L_ORDERKEY < KEY -> 33,006,459 rows (matches the re-gate VS side)
NATIVE_RPS=2073703           # native IMPORT ceiling (10/60 files, 30,006,480 rows / 14.47 s); re-gate baseline
SIZES="${1:-8192 32768 65536 131072}"
mkdir -p bench/reports
REPORT="bench/reports/batch-size-sweep-$(date +%Y%m%d-%H%M%S).txt"
: > "$REPORT"

SCHEMA=LHVS; ADAPTER=LAKEHOUSE_ADAPTER; CONN=LAKEHOUSE_CATALOG_CREDS; VS=TPCH
q()  { printf '%s' "$1" | exapump sql -d "$DSN" >/dev/null 2>&1; }
qout(){ printf '%s' "$1" | exapump sql -d "$DSN" -f csv 2>&1; }
scalar(){ printf '%s' "$1" | exapump sql -d "$DSN" -f csv 2>/dev/null | tail -n +2 | head -1 | tr -d '"[:space:]'; }

recreate_vs() {  # batch_size
  local bs="$1"
  q "DROP VIRTUAL SCHEMA IF EXISTS ${VS} CASCADE"
  printf '%s' "CREATE VIRTUAL SCHEMA ${VS}
USING ${SCHEMA}.${ADAPTER} WITH
  CATALOG_CONNECTION    = '${CONN}'
  ICEBERG_NAMESPACE     = '${NS}'
  NR_OF_CORES           = '${CORES}'
  PARALLELISM_FACTOR    = '${PF}'
  DATAFUSION_BATCH_SIZE = '${bs}'" | exapump sql -d "$DSN" >/dev/null 2>&1
  # Confirm the value was resolved into adapterNotes (proves it reaches the scan).
  qout "SELECT ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME='${VS}'" \
    | tr ',' '\n' | grep -i 'DF_BATCH_SIZE' | grep -oE '[0-9]+' | tail -1
}

timed_ctas() {  # label  select_expr
  local lbl="$1" expr="$2" t0 t1 rc el cnt out
  q "DROP TABLE IF EXISTS BENCH.LI_BS"; q "FLUSH STATISTICS"
  t0=$(date +%s.%N)
  out=$(printf '%s' "CREATE OR REPLACE TABLE BENCH.LI_BS AS ${expr}" | timeout 400 exapump sql -d "$DSN" 2>&1); rc=$?
  t1=$(date +%s.%N); el=$(awk "BEGIN{printf \"%.2f\", $t1-$t0}")
  if [ $rc -ne 0 ]; then echo "  ${lbl}: FAILED rc=$rc :: $(printf '%s' "$out"|tail -2|tr '\n' ' ')" | tee -a "$REPORT"; q "DROP TABLE IF EXISTS BENCH.LI_BS"; q "FLUSH STATISTICS"; return 1; fi
  cnt=$(scalar "SELECT COUNT(*) FROM BENCH.LI_BS")
  local rps; rps=$(awk "BEGIN{printf \"%.0f\", ${cnt}/${el}}")
  echo "  ${lbl}: ${el}s rows=${cnt} rows_per_s=${rps}" | tee -a "$REPORT"
  q "DROP TABLE IF EXISTS BENCH.LI_BS"; q "FLUSH STATISTICS"
  echo "$rps"
}

q "CREATE SCHEMA IF NOT EXISTS BENCH"
echo "=== DATAFUSION_BATCH_SIZE emit sweep (PF=${PF}, cores=${CORES}, filter L_ORDERKEY<${KEY}) ===" | tee -a "$REPORT"
echo "native IMPORT ceiling (re-gate, 10/60 files, 30,006,480 rows): ${NATIVE_RPS} rows/s" | tee -a "$REPORT"
echo | tee -a "$REPORT"

for bs in $SIZES; do
  resolved="$(recreate_vs "$bs")"
  echo "--- batch_size=${bs} (adapterNotes DF_BATCH_SIZE=${resolved:-?}) ---" | tee -a "$REPORT"
  if [ "$resolved" != "$bs" ]; then echo "  WARN: resolved DF_BATCH_SIZE '${resolved}' != requested '${bs}'" | tee -a "$REPORT"; fi
  best=0
  for pass in 1 2; do
    rps="$(timed_ctas "emit_bs${bs}_p${pass}" "SELECT * FROM ${VS}.LINEITEM WHERE L_ORDERKEY < ${KEY}" | tail -1)"
    [ "${rps:-0}" -gt "$best" ] 2>/dev/null && best="$rps"
  done
  echo "  => best rows/s=${best}  gap vs native=$(awk "BEGIN{printf \"%.2fx\", ${NATIVE_RPS}/${best}}")" | tee -a "$REPORT"
  echo | tee -a "$REPORT"
done
echo "=== SWEEP DONE (report: ${REPORT}) ===" | tee -a "$REPORT"
