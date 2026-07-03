#!/usr/bin/env bash
# Competitive engine comparison: AWS Athena vs the lakehouse engine, over the SAME Glue Iceberg
# TPC-H tables. NOT a spec feature — manually invoked, like the rest of bench/. No new infra: the
# Athena workgroup already exists in deploy/data-stack (`tofu output athena_workgroup`).
#
# Query text is the Presto/Trino dialect of bench/run.sh's Q1-Q4 (lines ~321-349) — table names
# lowercase (Glue/DuckDB dbgen writes lowercase TPC-H columns/tables). Reused verbatim by
# trino_compare.sh and deploy/scripts/spark_queries.py; keep all three in sync if you edit one.
#
#   AWS_PROFILE=spot-strata-deployer ATHENA_WORKGROUP=spot-strata-test1-athena ./athena_compare.sh
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f bench/.env ] && { set -a; . bench/.env; set +a; }

: "${ATHENA_WORKGROUP:?set ATHENA_WORKGROUP (deploy/data-stack: tofu output athena_workgroup)}"
ATHENA_DATABASE="${ATHENA_DATABASE:-tpch}"
REPORT="${1:-bench/reports/athena-compare-$(date +%Y%m%d-%H%M%S).txt}"
mkdir -p "$(dirname "$REPORT")"
: > "$REPORT"

Q1="SELECT n.n_name, r.r_name, COUNT(*) AS suppliers
FROM supplier s JOIN nation n ON s.s_nationkey = n.n_nationkey
JOIN region r ON n.n_regionkey = r.r_regionkey
GROUP BY n.n_name, r.r_name ORDER BY n.n_name"

Q2="SELECT COUNT(*) AS rows_joined FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON o.o_orderkey = l.l_orderkey"

Q3="SELECT o.o_orderpriority, COUNT(*) AS cnt, SUM(l.l_extendedprice) AS revenue
FROM orders o JOIN lineitem l ON o.o_orderkey = l.l_orderkey
WHERE o.o_orderdate >= DATE '1994-01-01' AND o.o_orderdate < DATE '1995-01-01'
GROUP BY o.o_orderpriority ORDER BY o.o_orderpriority"

Q4="SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price,
       AVG(l_discount) AS avg_disc, COUNT(*) AS count_order
FROM lineitem WHERE l_shipdate <= DATE '1998-09-01'
GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"

run_timed() {  # name sql
  local name="$1" sql="$2" qid status ms el
  qid="$(aws athena start-query-execution \
    --work-group "$ATHENA_WORKGROUP" \
    --query-execution-context "Database=${ATHENA_DATABASE}" \
    --query-string "$sql" --query 'QueryExecutionId' --output text)"
  status=""
  for _ in $(seq 1 120); do
    status="$(aws athena get-query-execution --query-execution-id "$qid" \
      --query 'QueryExecution.Status.State' --output text)"
    case "$status" in SUCCEEDED|FAILED|CANCELLED) break ;; esac
    sleep 2
  done
  if [ "$status" != "SUCCEEDED" ]; then
    echo "  $name: FAILED status=$status qid=$qid" | tee -a "$REPORT"; return
  fi
  # Engine-only execution time (excludes queue/planning wait) — the apples-to-apples figure.
  ms="$(aws athena get-query-execution --query-execution-id "$qid" \
    --query 'QueryExecution.Statistics.EngineExecutionTimeInMillis' --output text)"
  el="$(awk "BEGIN{printf \"%.2f\", ${ms}/1000}")"
  echo "  $name: ${el}s (engine) qid=$qid" | tee -a "$REPORT"
  echo "TIMING athena ${name} ${el}" >> "$REPORT"
}

echo "athena benchmark — workgroup=${ATHENA_WORKGROUP} database=${ATHENA_DATABASE} — $(date)" | tee -a "$REPORT"
run_timed "q1" "$Q1"
run_timed "q2" "$Q2"
run_timed "q3" "$Q3"
run_timed "q4" "$Q4"
echo "Done. Report: $REPORT"
