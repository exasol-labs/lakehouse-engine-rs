#!/usr/bin/env bash
# Competitive engine comparison: AWS Athena vs the lakehouse engine, over the SAME Glue Iceberg
# TPC-H tables. NOT a spec feature — manually invoked, like the rest of bench/. No new infra: the
# Athena workgroup already exists in deploy/data-stack (`tofu output athena_workgroup`).
#
# Query text is the Presto/Trino dialect of bench/run.sh's Q1-Q9b — table names lowercase
# (Glue/DuckDB dbgen writes lowercase TPC-H columns/tables). Reused verbatim by trino_compare.sh
# and deploy/scripts/spark_queries.py; keep all three in sync if you edit one.
#
#   AWS_PROFILE=spot-strata-deployer ATHENA_WORKGROUP=spot-strata-test1-athena ./athena_compare.sh
# No -e: run_timed must survive a failing query and report it as FAILED rather than aborting the
# whole comparison — same convention as bench/import_ceiling.sh.
set -uo pipefail
cd "$(dirname "$0")/.."
[ -f bench/.env ] && { set -a; . bench/.env; set +a; }
# bench/.env's AWS_ACCESS_KEY_ID/SECRET are the scoped engine-reader creds (Glue+S3 read only,
# for the Exasol CONNECTION) — they have no athena:* permissions. Unset them so the `aws` CLI
# falls back to AWS_PROFILE / the default credential chain (the operator's own broader identity).
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN

: "${ATHENA_WORKGROUP:?set ATHENA_WORKGROUP (deploy/data-stack: tofu output athena_workgroup)}"
# BENCH_WITH_DELETES (same flag as bench/run.sh): explicit ATHENA_DATABASE override always wins;
# otherwise "tpch" (baseline) or "tpch_deletes" (the Glue database
# deploy/scripts/make-deletes-remote.sh authors) when the flag is on.
WITH_DELETES="${BENCH_WITH_DELETES:-0}"
if [ -z "${ATHENA_DATABASE:-}" ]; then
  ATHENA_DATABASE="tpch"
  [ "$WITH_DELETES" = "1" ] && ATHENA_DATABASE="tpch_deletes"
fi
ENGINE_LABEL="athena"
[ "$WITH_DELETES" = "1" ] && ENGINE_LABEL="athena-deletes"
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

# Q5-Q9b probe specific pushdown strengths/weaknesses beyond Q1-Q4 — identical SQL (dialect-
# adjusted) in bench/run.sh, bench/trino_compare.sh, deploy/scripts/spark_queries.py.
Q5="SELECT o.o_orderpriority, COUNT(*) AS cnt, SUM(l.l_extendedprice) AS revenue
FROM orders o JOIN lineitem l ON o.o_orderkey = l.l_orderkey
GROUP BY o.o_orderpriority ORDER BY o.o_orderpriority"

Q6="SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price,
       AVG(l_discount) AS avg_disc, COUNT(*) AS count_order
FROM lineitem
GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"

Q7="SELECT COUNT(*) FROM (SELECT l_orderkey, COUNT(*) AS cnt FROM lineitem GROUP BY l_orderkey) t"

Q8="SELECT COUNT(*) FROM lineitem WHERE l_shipdate = DATE '1995-06-15'"

Q9A="SELECT SUM(l_quantity) FROM lineitem"

Q9B="SELECT COUNT(*),
       SUM(l_orderkey), SUM(l_partkey), SUM(l_suppkey), SUM(l_linenumber),
       SUM(l_quantity), SUM(l_extendedprice), SUM(l_discount), SUM(l_tax),
       COUNT(DISTINCT l_returnflag), COUNT(DISTINCT l_linestatus),
       MIN(l_shipdate), MAX(l_commitdate), MIN(l_receiptdate),
       COUNT(DISTINCT l_shipinstruct), COUNT(DISTINCT l_shipmode),
       SUM(length(l_comment))
FROM lineitem"

# NQ1-NQ5 close the arithmetic-aggregate-pushdown gap + probe LIKE/IN filters, ORDER BY+LIMIT, a
# 4-way join, and GROUP BY+HAVING — identical SQL (dialect-adjusted) in bench/run.sh,
# bench/trino_compare.sh, deploy/scripts/spark_queries.py.
NQ1="SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"

NQ2="SELECT COUNT(*) FROM lineitem
WHERE l_shipmode IN ('AIR','REG AIR') AND l_comment LIKE '%late%'"

NQ3="SELECT COUNT(*) AS cnt, SUM(ps.ps_supplycost) AS total_cost
FROM part p JOIN partsupp ps ON p.p_partkey = ps.ps_partkey
JOIN supplier s ON ps.ps_suppkey = s.s_suppkey
JOIN nation n ON s.s_nationkey = n.n_nationkey
WHERE p.p_size = 15 AND p.p_type LIKE '%BRASS%' AND n.n_name = 'GERMANY'"

NQ4="SELECT l_orderkey, l_extendedprice FROM lineitem
ORDER BY l_extendedprice DESC LIMIT 20"

NQ5="SELECT o_orderpriority, o_orderstatus, COUNT(*) AS cnt, AVG(o_totalprice) AS avg_price
FROM orders GROUP BY o_orderpriority, o_orderstatus
HAVING COUNT(*) > 1000000 ORDER BY o_orderpriority, o_orderstatus"

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
  echo "TIMING ${ENGINE_LABEL} ${name} ${el}" >> "$REPORT"
}

echo "athena benchmark — workgroup=${ATHENA_WORKGROUP} database=${ATHENA_DATABASE} with_deletes=${WITH_DELETES} — $(date)" | tee -a "$REPORT"
run_timed "q1" "$Q1"
run_timed "q2" "$Q2"
run_timed "q3" "$Q3"
run_timed "q4" "$Q4"
run_timed "q5" "$Q5"
run_timed "q6" "$Q6"
run_timed "q7" "$Q7"
run_timed "q8" "$Q8"
run_timed "q9a" "$Q9A"
run_timed "q9b" "$Q9B"
run_timed "nq1" "$NQ1"
run_timed "nq2" "$NQ2"
run_timed "nq3" "$NQ3"
run_timed "nq4" "$NQ4"
run_timed "nq5" "$NQ5"
echo "Done. Report: $REPORT"
