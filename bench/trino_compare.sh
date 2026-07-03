#!/usr/bin/env bash
# Competitive engine comparison: Trino vs the lakehouse engine, over the SAME Glue Iceberg TPC-H
# tables. NOT a spec feature — manually invoked, like the rest of bench/. Requires an ephemeral
# Trino node stood up via `deploy/scripts/trino-up.sh <env>` first — this script NEVER
# auto-provisions (cost-safety: nothing shall be started unless used).
#
# Query text matches bench/athena_compare.sh verbatim (both Presto-derived, identical SQL) —
# translated from bench/run.sh's Q1-Q4 (lines ~321-349). Keep both in sync if you edit one.
#
#   TRINO_HOST=<ip> ./trino_compare.sh
# No -e: run_timed must survive a failing query (OOM, syntax error, ...) and report it as FAILED
# rather than aborting the whole comparison — same convention as bench/import_ceiling.sh.
set -uo pipefail
cd "$(dirname "$0")/.."
[ -f bench/.env ] && { set -a; . bench/.env; set +a; }

if [ -z "${TRINO_HOST:-}" ]; then
  echo "SKIP: TRINO_HOST not set (run deploy/scripts/trino-up.sh <env> first)"
  exit 0
fi
TRINO_PORT="${TRINO_PORT:-8080}"
TRINO_IMAGE="${TRINO_IMAGE:-trinodb/trino:465}"
REPORT="${1:-bench/reports/trino-compare-$(date +%Y%m%d-%H%M%S).txt}"
mkdir -p "$(dirname "$REPORT")"
: > "$REPORT"

Q1="SELECT n.n_name, r.r_name, COUNT(*) AS suppliers
FROM iceberg.tpch.supplier s JOIN iceberg.tpch.nation n ON s.s_nationkey = n.n_nationkey
JOIN iceberg.tpch.region r ON n.n_regionkey = r.r_regionkey
GROUP BY n.n_name, r.r_name ORDER BY n.n_name"

Q2="SELECT COUNT(*) AS rows_joined FROM iceberg.tpch.customer c
JOIN iceberg.tpch.orders o ON c.c_custkey = o.o_custkey
JOIN iceberg.tpch.lineitem l ON o.o_orderkey = l.l_orderkey"

Q3="SELECT o.o_orderpriority, COUNT(*) AS cnt, SUM(l.l_extendedprice) AS revenue
FROM iceberg.tpch.orders o JOIN iceberg.tpch.lineitem l ON o.o_orderkey = l.l_orderkey
WHERE o.o_orderdate >= DATE '1994-01-01' AND o.o_orderdate < DATE '1995-01-01'
GROUP BY o.o_orderpriority ORDER BY o.o_orderpriority"

Q4="SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price,
       AVG(l_discount) AS avg_disc, COUNT(*) AS count_order
FROM iceberg.tpch.lineitem WHERE l_shipdate <= DATE '1998-09-01'
GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"

trino_exec() {
  docker run --rm "$TRINO_IMAGE" trino --server "http://${TRINO_HOST}:${TRINO_PORT}" \
    --catalog iceberg --schema tpch --output-format CSV --execute "$1"
}

run_timed() {  # name sql
  local name="$1" sql="$2" t0 t1 out rc el
  t0=$(date +%s.%N)
  out="$(trino_exec "$sql" 2>&1)"; rc=$?
  t1=$(date +%s.%N)
  el="$(awk "BEGIN{printf \"%.2f\", ${t1}-${t0}}")"
  if [ $rc -ne 0 ]; then
    echo "  $name: FAILED :: $(tail -2 <<<"$out")" | tee -a "$REPORT"; return
  fi
  echo "  $name: ${el}s" | tee -a "$REPORT"
  echo "TIMING trino ${name} ${el}" >> "$REPORT"
}

echo "trino benchmark — ${TRINO_HOST}:${TRINO_PORT} — $(date)" | tee -a "$REPORT"
run_timed "q1" "$Q1"
run_timed "q2" "$Q2"
run_timed "q3" "$Q3"
run_timed "q4" "$Q4"
echo "Done. Report: $REPORT"
