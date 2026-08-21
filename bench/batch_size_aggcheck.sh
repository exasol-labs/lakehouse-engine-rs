#!/usr/bin/env bash
# Aggregate-path regression check for the DATAFUSION_BATCH_SIZE sweep.
# Recreates the VS at each given batch size and times the Q1-Q4 query set
# (same queries as run.sh) so a batch size chosen for the raw-emit path can be
# confirmed not to regress the compute-heavy aggregate/join path.
#   ./bench/batch_size_aggcheck.sh 8192 131072
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; . bench/.env; set +a
DSN="exasol://sys:${EXASOL_SYS_PASSWORD}@${EXASOL_HOST}:${LH_EXASOL_PORT:-8563}?validateservercertificate=0"
NS="${NAMESPACE:-tpch}"; CORES="${BENCH_NR_OF_CORES:-8}"; PF="${BENCH_PARALLELISM_FACTOR:-8}"
SIZES="${*:-8192 131072}"
REPORT="bench/reports/batch-size-aggcheck-$(date +%Y%m%d-%H%M%S).txt"; : > "$REPORT"
qout(){ printf '%s' "$1" | exapump sql -d "$DSN" -f csv 2>&1; }
recreate_vs(){ printf '%s' "DROP VIRTUAL SCHEMA IF EXISTS TPCH CASCADE" | exapump sql -d "$DSN" >/dev/null 2>&1
  printf '%s' "CREATE VIRTUAL SCHEMA TPCH USING LHVS.LAKEHOUSE_ADAPTER WITH
    CATALOG_CONNECTION='LAKEHOUSE_CATALOG_CREDS' NAMESPACE='${NS}'
    NR_OF_CORES='${CORES}' PARALLELISM_FACTOR='${PF}' DATAFUSION_BATCH_SIZE='${1}'" | exapump sql -d "$DSN" >/dev/null 2>&1; }
timed(){ local lbl="$1" q="$2" t0 t1; t0=$(date +%s.%N); printf '%s' "$q" | exapump sql -d "$DSN" -f csv >/dev/null 2>&1; t1=$(date +%s.%N)
  printf '  %-4s %ss\n' "$lbl" "$(awk "BEGIN{printf \"%.2f\", $t1-$t0}")" | tee -a "$REPORT"; }
Q1="SELECT n.N_NAME, r.R_NAME, COUNT(*) FROM TPCH.SUPPLIER s JOIN TPCH.NATION n ON s.S_NATIONKEY=n.N_NATIONKEY JOIN TPCH.REGION r ON n.N_REGIONKEY=r.R_REGIONKEY GROUP BY n.N_NAME,r.R_NAME"
Q2="SELECT COUNT(*) FROM TPCH.CUSTOMER c JOIN TPCH.ORDERS o ON c.C_CUSTKEY=o.O_CUSTKEY JOIN TPCH.LINEITEM l ON o.O_ORDERKEY=l.L_ORDERKEY"
Q3="SELECT o.O_ORDERPRIORITY, COUNT(*), SUM(l.L_EXTENDEDPRICE) FROM TPCH.ORDERS o JOIN TPCH.LINEITEM l ON o.O_ORDERKEY=l.L_ORDERKEY WHERE o.O_ORDERDATE>=DATE '1994-01-01' AND o.O_ORDERDATE<DATE '1995-01-01' GROUP BY o.O_ORDERPRIORITY"
Q4="SELECT L_RETURNFLAG,L_LINESTATUS,SUM(L_QUANTITY),SUM(L_EXTENDEDPRICE),AVG(L_DISCOUNT),COUNT(*) FROM TPCH.LINEITEM WHERE L_SHIPDATE<=DATE '1998-09-01' GROUP BY L_RETURNFLAG,L_LINESTATUS"
for bs in $SIZES; do
  recreate_vs "$bs"
  echo "=== batch_size=${bs} — Q1-Q4 ===" | tee -a "$REPORT"
  timed Q1 "$Q1"; timed Q2 "$Q2"; timed Q3 "$Q3"; timed Q4 "$Q4"
  echo | tee -a "$REPORT"
done
echo "=== AGGCHECK DONE (${REPORT}) ===" | tee -a "$REPORT"
