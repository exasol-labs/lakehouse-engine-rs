#!/usr/bin/env bash
# Task 9: IMPORT FROM PARQUET goal-ceiling benchmark (NOT a spec feature).
# Exasol's native MPP Parquet reader vs the VS UDF path, same lineitem files,
# same far-VPC S3. Two comparisons:
#   scan-only    — COUNT(*) over both forces a full read with ~no output, so the
#                  delta is the UDF-layer overhead on top of the shared S3 read cost.
#   data-intensive — full-materialization of every lineitem row/column into a real
#                  Exasol table: native IMPORT INTO vs the VS `CREATE TABLE AS
#                  SELECT *` (emit path). This is the apples-to-apples data-transfer
#                  ceiling: both land identical data, so the delta is the UDF emit +
#                  Arrow->Value overhead vs the native loader.
set -uo pipefail
cd "$(dirname "$0")/.."
[ -f bench/.env ] && { set -a; . bench/.env; set +a; }
DSN="exasol://sys:${EXASOL_SYS_PASSWORD}@${EXASOL_HOST}:${LH_EXASOL_PORT:-8563}?validateservercertificate=0"
ENDPOINT="${AWS_S3_ENDPOINT:-https://s3.${AWS_REGION}.amazonaws.com}"
REPORT="${1:-/tmp/lh-import-ceiling.txt}"
: > "$REPORT"

# lineitem data files from the newest resolved scan spec (full s3:// URLs).
SRC_REPORT="$(ls -t bench/reports/bench-report-*.txt | head -1)"
mapfile -t URLS < <(grep -oE "s3://[^\"]*/lineitem/data/[^\"]*\.parquet" "$SRC_REPORT" | sort -u)
[ "${#URLS[@]}" -gt 0 ] || { echo "ERROR: no lineitem files in ${SRC_REPORT:-<none>} (run make bench first)"; exit 1; }
# Bucket is DERIVED from the resolved paths, never hardcoded: the VS reads these exact
# URLs, so IMPORT must target the same bucket or HeadObject ACCESS_DENIEs on a stale one.
BUCKET="$(printf '%s' "${URLS[0]}" | sed -E 's#^s3://([^/]+)/.*#\1#')"
mapfile -t FILES < <(printf '%s\n' "${URLS[@]}" | sed -E 's#^s3://[^/]+/##')  # bucket-relative
echo "files=${#FILES[@]} bucket=${BUCKET} endpoint=${ENDPOINT}" | tee -a "$REPORT"
FILE_CLAUSES=""
for f in "${FILES[@]}"; do FILE_CLAUSES="${FILE_CLAUSES} FILE '${f//\'/\'\'}'"; done

# INTO column types mirror the VS emit_exa_types for lineitem.
INTO="L_ORDERKEY DECIMAL(20,0), L_PARTKEY DECIMAL(20,0), L_SUPPKEY DECIMAL(20,0), L_LINENUMBER DECIMAL(20,0), L_QUANTITY DECIMAL(15,2), L_EXTENDEDPRICE DECIMAL(15,2), L_DISCOUNT DECIMAL(15,2), L_TAX DECIMAL(15,2), L_RETURNFLAG VARCHAR(2000000), L_LINESTATUS VARCHAR(2000000), L_SHIPDATE DATE, L_COMMITDATE DATE, L_RECEIPTDATE DATE, L_SHIPINSTRUCT VARCHAR(2000000), L_SHIPMODE VARCHAR(2000000), L_COMMENT VARCHAR(2000000)"
AT="s3://${BUCKET}/;Endpoint=${ENDPOINT#https://}"
IMPORT_SUBQ="IMPORT INTO (${INTO}) FROM PARQUET AT '${AT}' USER '${AWS_ACCESS_KEY_ID}' IDENTIFIED BY '${AWS_SECRET_ACCESS_KEY}'${FILE_CLAUSES}"

run_timed() {  # label  sql
  local label="$1" sql="$2" t0 t1 out
  t0=$(date +%s.%N)
  out=$(printf '%s' "$sql" | timeout 300 exapump sql -d "$DSN" -f csv 2>&1)
  local rc=$?
  t1=$(date +%s.%N)
  local el; el=$(awk "BEGIN{printf \"%.2f\", $t1-$t0}")
  local cnt; cnt=$(printf '%s' "$out" | tail -n +2 | head -1 | tr -d '"[:space:]')
  if [ $rc -ne 0 ]; then echo "  $label: FAILED rc=$rc :: $(printf '%s' "$out" | tail -2 | tr '\n' ' ')" | tee -a "$REPORT"
  else echo "  $label: ${el}s  count=${cnt}" | tee -a "$REPORT"; fi
}

echo "=== VALIDATE: IMPORT one file ===" | tee -a "$REPORT"
one="IMPORT INTO (${INTO}) FROM PARQUET AT '${AT}' USER '${AWS_ACCESS_KEY_ID}' IDENTIFIED BY '${AWS_SECRET_ACCESS_KEY}' FILE '${FILES[0]//\'/\'\'}'"
run_timed "import_1file" "SELECT COUNT(*) FROM (${one})"

echo "=== IMPORT ceiling: COUNT(*) over all ${#FILES[@]} files, 3x ===" | tee -a "$REPORT"
for i in 1 2 3; do run_timed "import_all_run$i" "SELECT COUNT(*) FROM (${IMPORT_SUBQ})"; done

echo "=== VS path: COUNT(*) FROM TPCH.LINEITEM, 3x ===" | tee -a "$REPORT"
for i in 1 2 3; do run_timed "vs_count_run$i" "SELECT COUNT(*) FROM TPCH.LINEITEM"; done

# ---- data-intensive: full materialization into real tables -------------------
# Times a statement (no result-set count to parse), then reports the target
# table's row count so both paths are asserted to land identical data.
run_timed_load() {  # label  target_table  sql
  local label="$1" tbl="$2" sql="$3" t0 t1 out rc el cnt
  t0=$(date +%s.%N)
  out=$(printf '%s' "$sql" | timeout 600 exapump sql -d "$DSN" -f csv 2>&1); rc=$?
  t1=$(date +%s.%N)
  el=$(awk "BEGIN{printf \"%.2f\", $t1-$t0}")
  if [ $rc -ne 0 ]; then echo "  $label: FAILED rc=$rc :: $(printf '%s' "$out" | tail -2 | tr '\n' ' ')" | tee -a "$REPORT"; return; fi
  cnt=$(printf '%s' "SELECT COUNT(*) FROM ${tbl}" | exapump sql -d "$DSN" -f csv 2>/dev/null | tail -n +2 | head -1 | tr -d '"[:space:]')
  echo "  $label: ${el}s  rows=${cnt}" | tee -a "$REPORT"
}

echo "=== data-intensive setup: schema + native IMPORT target table ===" | tee -a "$REPORT"
printf '%s' "CREATE SCHEMA IF NOT EXISTS BENCH" | exapump sql -d "$DSN" >/dev/null 2>&1 || true
printf '%s' "CREATE OR REPLACE TABLE BENCH.LINEITEM_IMPORT (${INTO})" | exapump sql -d "$DSN" >/dev/null 2>&1

echo "=== IMPORT INTO (native full load), 3x ===" | tee -a "$REPORT"
for i in 1 2 3; do
  printf '%s' "TRUNCATE TABLE BENCH.LINEITEM_IMPORT" | exapump sql -d "$DSN" >/dev/null 2>&1
  run_timed_load "import_into_run$i" "BENCH.LINEITEM_IMPORT" \
    "IMPORT INTO BENCH.LINEITEM_IMPORT FROM PARQUET AT '${AT}' USER '${AWS_ACCESS_KEY_ID}' IDENTIFIED BY '${AWS_SECRET_ACCESS_KEY}'${FILE_CLAUSES}"
done

echo "=== VS path: CREATE TABLE AS SELECT * FROM TPCH.LINEITEM (full emit), 3x ===" | tee -a "$REPORT"
for i in 1 2 3; do
  run_timed_load "vs_ctas_run$i" "BENCH.LINEITEM_VS" \
    "CREATE OR REPLACE TABLE BENCH.LINEITEM_VS AS SELECT * FROM TPCH.LINEITEM"
done

echo "=== DONE ===" | tee -a "$REPORT"
