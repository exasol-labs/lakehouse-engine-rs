#!/usr/bin/env bash
# Exasol native IMPORT FROM JDBC vs the VS path: same Q1-Q9b/NQ1-NQ5 pushed down as sub-selects
# over a JDBC connection to Trino, instead of through the lakehouse-engine VS. Sibling to
# import_ceiling.sh (native-reader ceiling checks), just JDBC instead of the Parquet reader.
# NOT a spec feature — manually invoked, like the rest of bench/.
#
# Requires the SAME ephemeral Trino cluster as bench/trino_compare.sh (deploy/scripts/trino-up.sh
# <env>) — never auto-provisions (cost-safety: nothing shall be started unless used).
#
# SQL is copied verbatim (Trino/Presto dialect) from bench/trino_compare.sh's $Q1-$NQ5 — keep all
# copies (bench/run.sh, athena_compare.sh, trino_compare.sh, this file) in sync.
#
#   TRINO_HOST=<coordinator-ip> ./import_jdbc_trino.sh   # only the coordinator accepts client queries
# No -e: run_timed must survive a failing query and report it as FAILED rather than aborting the
# whole comparison — same convention as import_ceiling.sh / trino_compare.sh.
set -uo pipefail
cd "$(dirname "$0")/.."
[ -f bench/.env ] && { set -a; . bench/.env; set +a; }

if [ -z "${TRINO_HOST:-}" ]; then
  echo "SKIP: TRINO_HOST not set (run deploy/scripts/trino-up.sh <env> first)"
  exit 0
fi
TRINO_PORT="${TRINO_PORT:-8080}"
TRINO_JDBC_VERSION="${TRINO_IMAGE:-trinodb/trino:465}"
TRINO_JDBC_VERSION="${TRINO_JDBC_VERSION##*:}"
DSN="exasol://sys:${EXASOL_SYS_PASSWORD}@${EXASOL_HOST}:${LH_EXASOL_PORT:-8563}?validateservercertificate=0"
REPORT="${1:-bench/reports/import-jdbc-trino-$(date +%Y%m%d-%H%M%S).txt}"
mkdir -p "$(dirname "$REPORT")"
: > "$REPORT"
# Same FAILED convention as bench/run.sh: this script otherwise always exits 0 (its last command
# is a plain echo) regardless of how many queries inside failed, so a caller chaining this in
# (bench-remote.sh's BENCH_RUN_CEILING) would have no way to detect "every query failed" and would
# silently proceed as if the run succeeded.
FAILED=0

# ---- register the Trino JDBC driver in BucketFS (idempotent: just re-upload every run) --------
CACHE_DIR="bench/.cache"
JAR_NAME="trino-jdbc-${TRINO_JDBC_VERSION}.jar"
JAR_PATH="${CACHE_DIR}/${JAR_NAME}"
mkdir -p "$CACHE_DIR"
if [ ! -f "$JAR_PATH" ]; then
  echo "== fetching ${JAR_NAME} from Maven Central ==" | tee -a "$REPORT"
  curl -sfL -o "$JAR_PATH" \
    "https://repo1.maven.org/maven2/io/trino/trino-jdbc/${TRINO_JDBC_VERSION}/${JAR_NAME}" \
    || { echo "ERROR: failed to fetch ${JAR_NAME}"; exit 1; }
fi
SETTINGS_PATH="${CACHE_DIR}/settings.cfg"
# FETCHSIZE/INSERTSIZE are load-bearing, not cosmetic: Exasol silently drops the whole driver
# registration (ETL-1013 "Driver=... is unknown") without them, live-verified on test1.
# NOSECURITY=YES avoids the JDBC client's sandboxed-JVM permission denial on network calls
# (getProxySelector) that the driver needs to reach Trino.
cat > "$SETTINGS_PATH" <<EOF
DRIVERNAME=TRINO
JAR=${JAR_NAME}
DRIVERMAIN=io.trino.jdbc.TrinoDriver
PREFIX=jdbc:trino:
FETCHSIZE=100000
INSERTSIZE=-1
NOSECURITY=YES
EOF

BFS_ARGS=(--bfs-host "$EXASOL_HOST" --bfs-port "${LH_BUCKETFS_PORT:-2581}" --bfs-bucket default \
  --bfs-write-password "$BUCKETFS_WRITE_PASS")

echo "== registering TRINO JDBC driver in BucketFS ==" | tee -a "$REPORT"
exapump bucketfs cp "$JAR_PATH" "drivers/jdbc/TRINO/${JAR_NAME}" "${BFS_ARGS[@]}" \
  || { echo "ERROR: driver jar upload failed"; exit 1; }
exapump bucketfs cp "$SETTINGS_PATH" "drivers/jdbc/TRINO/settings.cfg" "${BFS_ARGS[@]}" \
  || { echo "ERROR: driver settings.cfg upload failed"; exit 1; }

# BENCH_WITH_DELETES (same flag as bench/run.sh): explicit TRINO_SCHEMA override always wins;
# otherwise "tpch" (baseline) or "tpch_deletes" (the Glue database
# deploy/scripts/make-deletes-remote.sh authors) when the flag is on. Shared with
# trino_compare.sh (same cluster, same queries).
WITH_DELETES="${BENCH_WITH_DELETES:-0}"
if [ -z "${TRINO_SCHEMA:-}" ]; then
  TRINO_SCHEMA="tpch"
  [ "$WITH_DELETES" = "1" ] && TRINO_SCHEMA="tpch_deletes"
fi
ENGINE_LABEL="import-jdbc-trino"
[ "$WITH_DELETES" = "1" ] && ENGINE_LABEL="import-jdbc-trino-deletes"

# ---- Q1-NQ5, copied verbatim from bench/trino_compare.sh -------------------------------------
Q1="SELECT n.n_name, r.r_name, COUNT(*) AS suppliers
FROM iceberg.${TRINO_SCHEMA}.supplier s JOIN iceberg.${TRINO_SCHEMA}.nation n ON s.s_nationkey = n.n_nationkey
JOIN iceberg.${TRINO_SCHEMA}.region r ON n.n_regionkey = r.r_regionkey
GROUP BY n.n_name, r.r_name ORDER BY n.n_name"

Q2="SELECT COUNT(*) AS rows_joined FROM iceberg.${TRINO_SCHEMA}.customer c
JOIN iceberg.${TRINO_SCHEMA}.orders o ON c.c_custkey = o.o_custkey
JOIN iceberg.${TRINO_SCHEMA}.lineitem l ON o.o_orderkey = l.l_orderkey"

Q3="SELECT o.o_orderpriority, COUNT(*) AS cnt, SUM(l.l_extendedprice) AS revenue
FROM iceberg.${TRINO_SCHEMA}.orders o JOIN iceberg.${TRINO_SCHEMA}.lineitem l ON o.o_orderkey = l.l_orderkey
WHERE o.o_orderdate >= DATE '1994-01-01' AND o.o_orderdate < DATE '1995-01-01'
GROUP BY o.o_orderpriority ORDER BY o.o_orderpriority"

Q4="SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price,
       AVG(l_discount) AS avg_disc, COUNT(*) AS count_order
FROM iceberg.${TRINO_SCHEMA}.lineitem WHERE l_shipdate <= DATE '1998-09-01'
GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"

Q5="SELECT o.o_orderpriority, COUNT(*) AS cnt, SUM(l.l_extendedprice) AS revenue
FROM iceberg.${TRINO_SCHEMA}.orders o JOIN iceberg.${TRINO_SCHEMA}.lineitem l ON o.o_orderkey = l.l_orderkey
GROUP BY o.o_orderpriority ORDER BY o.o_orderpriority"

Q6="SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price,
       AVG(l_discount) AS avg_disc, COUNT(*) AS count_order
FROM iceberg.${TRINO_SCHEMA}.lineitem
GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"

Q7="SELECT COUNT(*) FROM (SELECT l_orderkey, COUNT(*) AS cnt FROM iceberg.${TRINO_SCHEMA}.lineitem GROUP BY l_orderkey) t"

Q8="SELECT COUNT(*) FROM iceberg.${TRINO_SCHEMA}.lineitem WHERE l_shipdate = DATE '1995-06-15'"

Q9A="SELECT SUM(l_quantity) FROM iceberg.${TRINO_SCHEMA}.lineitem"

Q9B="SELECT COUNT(*),
       SUM(l_orderkey), SUM(l_partkey), SUM(l_suppkey), SUM(l_linenumber),
       SUM(l_quantity), SUM(l_extendedprice), SUM(l_discount), SUM(l_tax),
       COUNT(DISTINCT l_returnflag), COUNT(DISTINCT l_linestatus),
       MIN(l_shipdate), MAX(l_commitdate), MIN(l_receiptdate),
       COUNT(DISTINCT l_shipinstruct), COUNT(DISTINCT l_shipmode),
       SUM(length(l_comment))
FROM iceberg.${TRINO_SCHEMA}.lineitem"

NQ1="SELECT SUM(l_extendedprice * l_discount) AS revenue FROM iceberg.${TRINO_SCHEMA}.lineitem
WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"

NQ2="SELECT COUNT(*) FROM iceberg.${TRINO_SCHEMA}.lineitem
WHERE l_shipmode IN ('AIR','REG AIR') AND l_comment LIKE '%late%'"

NQ3="SELECT COUNT(*) AS cnt, SUM(ps.ps_supplycost) AS total_cost
FROM iceberg.${TRINO_SCHEMA}.part p JOIN iceberg.${TRINO_SCHEMA}.partsupp ps ON p.p_partkey = ps.ps_partkey
JOIN iceberg.${TRINO_SCHEMA}.supplier s ON ps.ps_suppkey = s.s_suppkey
JOIN iceberg.${TRINO_SCHEMA}.nation n ON s.s_nationkey = n.n_nationkey
WHERE p.p_size = 15 AND p.p_type LIKE '%BRASS%' AND n.n_name = 'GERMANY'"

NQ4="SELECT l_orderkey, l_extendedprice FROM iceberg.${TRINO_SCHEMA}.lineitem
ORDER BY l_extendedprice DESC LIMIT 20"

NQ5="SELECT o_orderpriority, o_orderstatus, COUNT(*) AS cnt, AVG(o_totalprice) AS avg_price
FROM iceberg.${TRINO_SCHEMA}.orders GROUP BY o_orderpriority, o_orderstatus
HAVING COUNT(*) > 1000000 ORDER BY o_orderpriority, o_orderstatus"

# INTO column lists mirror this repo's Arrow->Exasol type table (strings -> VARCHAR(2000000),
# counts/keys -> DECIMAL(20,0), money/avg -> DECIMAL(15,2)). SUM()s over lineitem's numeric key/
# quantity columns at sf=30 scale can exceed DECIMAL range, so those use DOUBLE PRECISION.
INTO_Q1="N_NAME VARCHAR(2000000), R_NAME VARCHAR(2000000), SUPPLIERS DECIMAL(20,0)"
INTO_Q2="ROWS_JOINED DECIMAL(20,0)"
INTO_Q3="O_ORDERPRIORITY VARCHAR(2000000), CNT DECIMAL(20,0), REVENUE DECIMAL(15,2)"
INTO_Q4="L_RETURNFLAG VARCHAR(2000000), L_LINESTATUS VARCHAR(2000000), SUM_QTY DECIMAL(15,2), SUM_BASE_PRICE DECIMAL(15,2), AVG_DISC DECIMAL(15,2), COUNT_ORDER DECIMAL(20,0)"
INTO_Q5="$INTO_Q3"
INTO_Q6="$INTO_Q4"
INTO_Q7="CNT DECIMAL(20,0)"
INTO_Q8="CNT DECIMAL(20,0)"
INTO_Q9A="SUM_QUANTITY DOUBLE PRECISION"
# DECIMAL not DOUBLE PRECISION for the BIGINT-sourced sums (orderkey/partkey/suppkey/linenumber/
# comment-length): Trino's JDBC driver can't transform a BIGINT sum into DOUBLE PRECISION
# (ETL-1299/ETL-1202 "Transformation for this combination of column types is not possible") —
# live-verified on test1.
INTO_Q9B="CNT DECIMAL(20,0), SUM_ORDERKEY DECIMAL(36,0), SUM_PARTKEY DECIMAL(36,0), SUM_SUPPKEY DECIMAL(36,0), SUM_LINENUMBER DECIMAL(36,0), SUM_QUANTITY DECIMAL(36,4), SUM_EXTENDEDPRICE DECIMAL(36,4), SUM_DISCOUNT DECIMAL(36,4), SUM_TAX DECIMAL(36,4), CNT_DISTINCT_RETURNFLAG DECIMAL(20,0), CNT_DISTINCT_LINESTATUS DECIMAL(20,0), MIN_SHIPDATE DATE, MAX_COMMITDATE DATE, MIN_RECEIPTDATE DATE, CNT_DISTINCT_SHIPINSTRUCT DECIMAL(20,0), CNT_DISTINCT_SHIPMODE DECIMAL(20,0), SUM_COMMENT_LEN DECIMAL(36,0)"
INTO_NQ1="REVENUE DECIMAL(15,2)"
INTO_NQ2="CNT DECIMAL(20,0)"
INTO_NQ3="CNT DECIMAL(20,0), TOTAL_COST DECIMAL(15,2)"
INTO_NQ4="L_ORDERKEY DECIMAL(20,0), L_EXTENDEDPRICE DECIMAL(15,2)"
INTO_NQ5="O_ORDERPRIORITY VARCHAR(2000000), O_ORDERSTATUS VARCHAR(2000000), CNT DECIMAL(20,0), AVG_PRICE DECIMAL(15,2)"

# Raw column-type list for the full-table streaming scan below (§ raw scan), copied verbatim from
# bench/import_ceiling.sh's INTO — same lineitem columns, already live-verified on test1 (including
# the BIGINT-sum DECIMAL gotcha noted on INTO_Q9B above), just untransformed rather than aggregated.
INTO_RAW="L_ORDERKEY DECIMAL(20,0), L_PARTKEY DECIMAL(20,0), L_SUPPKEY DECIMAL(20,0), L_LINENUMBER DECIMAL(20,0), L_QUANTITY DECIMAL(15,2), L_EXTENDEDPRICE DECIMAL(15,2), L_DISCOUNT DECIMAL(15,2), L_TAX DECIMAL(15,2), L_RETURNFLAG VARCHAR(2000000), L_LINESTATUS VARCHAR(2000000), L_SHIPDATE DATE, L_COMMITDATE DATE, L_RECEIPTDATE DATE, L_SHIPINSTRUCT VARCHAR(2000000), L_SHIPMODE VARCHAR(2000000), L_COMMENT VARCHAR(2000000)"

# TRINO_JDBC_HOST (private IP, same VPC as Exasol) if set, else TRINO_HOST (public IP). The JDBC
# connection here originates FROM the Exasol node, not the operator's machine — connecting to
# another same-VPC instance's PUBLIC IP round-trips through the IGW, which is unreliable for a
# long-lived paginated result fetch (live-verified: every query failed at "fetching next" after
# ~2 minutes/4 attempts, even with the SG opened to the whole VPC on port 8080).
JDBC_URL="jdbc:trino://${TRINO_JDBC_HOST:-$TRINO_HOST}:${TRINO_PORT}/iceberg/${TRINO_SCHEMA}"

import_stmt() {  # into  statement-sql
  local into="$1" stmt="$2"
  # IDENTIFIED BY must be empty: Trino's JDBC client refuses a non-empty password over plain HTTP
  # ("TLS/SSL is required for authentication with username and password") — live-verified on test1.
  printf "IMPORT INTO (%s) FROM JDBC DRIVER='TRINO' AT '%s' USER 'admin' IDENTIFIED BY '' STATEMENT '%s'" \
    "$into" "$JDBC_URL" "${stmt//\'/\'\'}"
}

run_timed() {  # name  into  statement-sql
  local name="$1" into="$2" stmt="$3" t0 t1 out rc el
  local sql; sql="SELECT * FROM ($(import_stmt "$into" "$stmt"))"
  t0=$(date +%s.%N)
  out=$(printf '%s' "$sql" | timeout 300 exapump sql -d "$DSN" -f csv 2>&1)
  rc=$?
  t1=$(date +%s.%N)
  el=$(awk "BEGIN{printf \"%.2f\", $t1-$t0}")
  if [ $rc -ne 0 ]; then
    echo "  $name: FAILED rc=$rc :: $(printf '%s' "$out" | tail -2 | tr '\n' ' ')" | tee -a "$REPORT"
    FAILED=1
    return
  fi
  local rows; rows=$(printf '%s' "$out" | tail -n +2 | wc -l | tr -d '[:space:]')
  echo "  $name: ${el}s  rows=${rows}" | tee -a "$REPORT"
  echo "TIMING ${ENGINE_LABEL} ${name} ${el}" >> "$REPORT"
}

# Materializing load (real IMPORT INTO <table>, not a client-side derived-table SELECT) — same
# apples-to-apples methodology as import_ceiling.sh's run_timed_load, so 180M rows never round-trip
# through the exapump CSV client. Row count is read back from the target table, not parsed from
# import output (IMPORT INTO returns no result set).
run_timed_load() {  # label  target_table  sql
  local label="$1" tbl="$2" sql="$3" t0 t1 out rc el cnt rps
  t0=$(date +%s.%N)
  # 1800s client-side, matching the ALTER SESSION QUERY_TIMEOUT set before this loop runs: a raw
  # full-table (180M row) transfer over a single JDBC connection is the slow path THIS SCRIPT EXISTS
  # TO MEASURE, so it must be allowed to actually finish rather than be cut off at Exasol's 300s
  # default (live-verified: every run hit "Query timeout after 300000ms" before this was raised).
  out=$(printf '%s' "$sql" | timeout 1800 exapump sql -d "$DSN" -f csv 2>&1); rc=$?
  t1=$(date +%s.%N)
  el=$(awk "BEGIN{printf \"%.2f\", $t1-$t0}")
  if [ $rc -ne 0 ]; then echo "  $label: FAILED rc=$rc :: $(printf '%s' "$out" | tail -2 | tr '\n' ' ')" | tee -a "$REPORT"; FAILED=1; return; fi
  cnt=$(printf '%s' "SELECT COUNT(*) FROM ${tbl}" | exapump sql -d "$DSN" -f csv 2>/dev/null | tail -n +2 | head -1 | tr -d '"[:space:]')
  rps=$(awk "BEGIN{if(${el:-0}+0>0) printf \"%.0f\", ${cnt:-0}/${el}; else print \"n/a\"}")
  echo "  $label: ${el}s  rows=${cnt}  throughput=${rps} rows/s" | tee -a "$REPORT"
}

echo "import-jdbc-trino benchmark — ${TRINO_HOST}:${TRINO_PORT} schema=${TRINO_SCHEMA} with_deletes=${WITH_DELETES} — $(date)" | tee -a "$REPORT"
run_timed "q1" "$INTO_Q1" "$Q1"
run_timed "q2" "$INTO_Q2" "$Q2"
run_timed "q3" "$INTO_Q3" "$Q3"
run_timed "q4" "$INTO_Q4" "$Q4"
run_timed "q5" "$INTO_Q5" "$Q5"
run_timed "q6" "$INTO_Q6" "$Q6"
run_timed "q7" "$INTO_Q7" "$Q7"
run_timed "q8" "$INTO_Q8" "$Q8"
run_timed "q9a" "$INTO_Q9A" "$Q9A"
run_timed "q9b" "$INTO_Q9B" "$Q9B"
run_timed "nq1" "$INTO_NQ1" "$NQ1"
run_timed "nq2" "$INTO_NQ2" "$NQ2"
run_timed "nq3" "$INTO_NQ3" "$NQ3"
run_timed "nq4" "$INTO_NQ4" "$NQ4"
run_timed "nq5" "$INTO_NQ5" "$NQ5"

# ---- raw streaming full-table scan: no aggregation, no filter — measures pure data-movement
# throughput over the single JDBC connection, to compare against import_ceiling.sh's VS-path
# CTAS (bench/import_ceiling.sh's vs_ctas_run*) at the same node count.
echo "=== raw full-table scan: IMPORT INTO (JDBC), SELECT * FROM lineitem, 3x ===" | tee -a "$REPORT"
printf '%s' "CREATE SCHEMA IF NOT EXISTS BENCH" | exapump sql -d "$DSN" >/dev/null 2>&1 || true
printf '%s' "CREATE OR REPLACE TABLE BENCH.LINEITEM_JDBC (${INTO_RAW})" | exapump sql -d "$DSN" >/dev/null 2>&1
for i in 1 2 3; do
  if ! printf '%s' "TRUNCATE TABLE BENCH.LINEITEM_JDBC" | exapump sql -d "$DSN" >/dev/null 2>&1; then
    echo "  jdbc_raw_scan_run$i: SKIPPED (TRUNCATE failed — would inflate rows/throughput on a re-run)" | tee -a "$REPORT"
    FAILED=1
    continue
  fi
  # QUERY_TIMEOUT must be raised IN THE SAME SESSION as the IMPORT itself (each exapump invocation
  # is its own session, so a separate ALTER SESSION call wouldn't carry over) — Exasol's 300s
  # default is well under how long a full 180M-row transfer over a single JDBC connection takes.
  run_timed_load "jdbc_raw_scan_run$i" "BENCH.LINEITEM_JDBC" \
    "ALTER SESSION SET QUERY_TIMEOUT=1800; IMPORT INTO BENCH.LINEITEM_JDBC FROM JDBC DRIVER='TRINO' AT '${JDBC_URL}' USER 'admin' IDENTIFIED BY '' STATEMENT 'SELECT * FROM lineitem'"
done

if [ "$FAILED" -ne 0 ]; then
  echo "IMPORT-JDBC-TRINO BENCHMARK FAILED (see FAILED entries above). Report: ${REPORT}"
  exit 1
fi
echo "Done. Report: $REPORT"
