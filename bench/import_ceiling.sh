#!/usr/bin/env bash
# Task 9: IMPORT FROM PARQUET goal-ceiling benchmark (NOT a spec feature).
# Exasol's native MPP Parquet reader vs the VS UDF path, same lineitem files,
# same far-VPC S3. COUNT(*) over both forces a full read with ~no output, so the
# delta is the UDF-layer overhead on top of the shared S3 read cost.
set -uo pipefail
cd "$(dirname "$0")/.."
[ -f bench/.env ] && { set -a; . bench/.env; set +a; }
DSN="exasol://sys:${EXASOL_SYS_PASSWORD}@${EXASOL_HOST}:${LH_EXASOL_PORT:-8563}?validateservercertificate=0"
BUCKET="strata-playground-216764142018"
ENDPOINT="${AWS_S3_ENDPOINT:-https://s3.${AWS_REGION}.amazonaws.com}"
REPORT="${1:-/tmp/lh-import-ceiling.txt}"
: > "$REPORT"

# 20 lineitem data files (bucket-relative), from the resolved scan spec.
mapfile -t FILES < <(grep -oE "s3://[^\"]*/lineitem/data/[^\"]*\.parquet" \
  "$(ls -t bench/reports/bench-report-*.txt | head -1)" | sort -u | sed -E 's#^s3://[^/]+/##')
echo "files=${#FILES[@]} endpoint=${ENDPOINT}" | tee -a "$REPORT"
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

echo "=== DONE ===" | tee -a "$REPORT"
