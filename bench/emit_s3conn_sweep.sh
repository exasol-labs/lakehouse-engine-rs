#!/usr/bin/env bash
# S3_MAX_CONNECTIONS sweep on the RAW-EMIT path (NOT a spec feature).
#
# A prior sweep tested S3_MAX_CONNECTIONS only against the Q4 aggregate query
# (few rows over the wire -> found <2% movement). This tests it on the raw
# full-emit path instead — the filtered `CREATE TABLE AS SELECT *` that streams
# ~33M rows out via synchronous MT_EMIT — to answer whether a wider S3 fetch
# pipeline keeps more decoded batches ready to emit and hides emit-wait, even
# though it did not matter for the aggregate shape.
#
# Batch size is fixed at the batch-size-sweep winner (default 65536); everything
# else at the shipped default (PARALLELISM_FACTOR=8 => G=16, threading AUTO).
# Recreates ONLY the VS per config (property resolved at createVirtualSchema).
#
#   ./bench/emit_s3conn_sweep.sh                 # AUTO 8 32 64 128 at bs=65536
#   ./bench/emit_s3conn_sweep.sh 131072 "AUTO 64"
set -uo pipefail
cd "$(dirname "$0")/.."
[ -f bench/.env ] || { echo "ERROR: bench/.env required"; exit 1; }
set -a; . bench/.env; set +a
DSN="exasol://sys:${EXASOL_SYS_PASSWORD}@${EXASOL_HOST}:${LH_EXASOL_PORT:-8563}?validateservercertificate=0"
NS="${ICEBERG_NAMESPACE:-tpch}"; CORES="${BENCH_NR_OF_CORES:-8}"; PF="${BENCH_PARALLELISM_FACTOR:-8}"
KEY=33007128; NATIVE_RPS=2073703
BS="${1:-65536}"
CONNS="${2:-AUTO 8 32 64 128}"
mkdir -p bench/reports
REPORT="bench/reports/emit-s3conn-sweep-$(date +%Y%m%d-%H%M%S).txt"; : > "$REPORT"
SCHEMA=LHVS; ADAPTER=LAKEHOUSE_ADAPTER; CONN=LAKEHOUSE_CATALOG_CREDS; VS=TPCH
q()   { printf '%s' "$1" | exapump sql -d "$DSN" >/dev/null 2>&1; }
qout(){ printf '%s' "$1" | exapump sql -d "$DSN" -f csv 2>&1; }
scalar(){ printf '%s' "$1" | exapump sql -d "$DSN" -f csv 2>/dev/null | tail -n +2 | head -1 | tr -d '"[:space:]'; }

recreate_vs() {  # s3conn ("AUTO" => omit property, let it AUTO-derive)
  local s3="$1" s3line=""
  [ "$s3" != "AUTO" ] && s3line="$(printf "\n  S3_MAX_CONNECTIONS    = '%s'" "$s3")"
  q "DROP VIRTUAL SCHEMA IF EXISTS ${VS} CASCADE"
  printf '%s' "CREATE VIRTUAL SCHEMA ${VS}
USING ${SCHEMA}.${ADAPTER} WITH
  CATALOG_CONNECTION    = '${CONN}'
  ICEBERG_NAMESPACE     = '${NS}'
  NR_OF_CORES           = '${CORES}'
  PARALLELISM_FACTOR    = '${PF}'
  DATAFUSION_BATCH_SIZE = '${BS}'${s3line}" | exapump sql -d "$DSN" >/dev/null 2>&1
  qout "SELECT ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME='${VS}'" \
    | tr ',' '\n' | grep -iE 'S3_MAX_CONN|MAX_CONNECTIONS' | grep -oE '[0-9]+' | tail -1
}

timed_ctas() {  # label
  local lbl="$1" t0 t1 rc el cnt out
  q "DROP TABLE IF EXISTS BENCH.LI_S3"; q "FLUSH STATISTICS"
  t0=$(date +%s.%N)
  out=$(printf '%s' "CREATE OR REPLACE TABLE BENCH.LI_S3 AS SELECT * FROM ${VS}.LINEITEM WHERE L_ORDERKEY < ${KEY}" | timeout 400 exapump sql -d "$DSN" 2>&1); rc=$?
  t1=$(date +%s.%N); el=$(awk "BEGIN{printf \"%.2f\", $t1-$t0}")
  if [ $rc -ne 0 ]; then echo "  ${lbl}: FAILED rc=$rc :: $(printf '%s' "$out"|tail -2|tr '\n' ' ')" | tee -a "$REPORT"; q "DROP TABLE IF EXISTS BENCH.LI_S3"; q "FLUSH STATISTICS"; return 1; fi
  cnt=$(scalar "SELECT COUNT(*) FROM BENCH.LI_S3")
  local rps; rps=$(awk "BEGIN{printf \"%.0f\", ${cnt}/${el}}")
  echo "  ${lbl}: ${el}s rows=${cnt} rows_per_s=${rps}" | tee -a "$REPORT"
  q "DROP TABLE IF EXISTS BENCH.LI_S3"; q "FLUSH STATISTICS"
  echo "$rps"
}

q "CREATE SCHEMA IF NOT EXISTS BENCH"
echo "=== S3_MAX_CONNECTIONS sweep on RAW-EMIT path (bs=${BS}, PF=${PF}, filter L_ORDERKEY<${KEY}) ===" | tee -a "$REPORT"
echo "native IMPORT ceiling: ${NATIVE_RPS} rows/s" | tee -a "$REPORT"; echo | tee -a "$REPORT"
for c in $CONNS; do
  resolved="$(recreate_vs "$c")"
  echo "--- S3_MAX_CONNECTIONS=${c} (adapterNotes resolved=${resolved:-?}) ---" | tee -a "$REPORT"
  best=0
  for pass in 1 2; do
    rps="$(timed_ctas "emit_s3_${c}_p${pass}" | tail -1)"
    [ "${rps:-0}" -gt "$best" ] 2>/dev/null && best="$rps"
  done
  echo "  => best rows/s=${best}  gap vs native=$(awk "BEGIN{printf \"%.2fx\", ${NATIVE_RPS}/${best}}")" | tee -a "$REPORT"
  echo | tee -a "$REPORT"
done
echo "=== SWEEP DONE (report: ${REPORT}) ===" | tee -a "$REPORT"
