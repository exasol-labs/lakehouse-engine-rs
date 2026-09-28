#!/usr/bin/env bash
# Trino vs the lakehouse engine over the same Glue Iceberg TPC-H tables. Needs a running Trino
# cluster (deploy/scripts/trino-up.sh <env>); never auto-provisions. Query text matches
# bench/athena_compare.sh; keep them in sync.
#
# One persistent Trino CLI session runs over SSH on a worker node: that avoids a JVM cold start per
# query and mirrors exapump's shape (one internet hop to the cluster, then an intra-VPC hop). A
# worker rather than the coordinator keeps a real network hop in the measurement.
#
#   TRINO_HOST=<coordinator-ip> TRINO_WORKER_HOST=<worker-ip> ./trino_compare.sh
# No -e: a failing query is reported as FAILED instead of aborting the comparison.
set -uo pipefail
cd "$(dirname "$0")/.."
[ -f bench/.env ] && { set -a; . bench/.env; set +a; }

if [ -z "${TRINO_HOST:-}" ]; then
  echo "SKIP: TRINO_HOST not set (run deploy/scripts/trino-up.sh <env> first)"
  exit 0
fi
if [ -z "${TRINO_WORKER_HOST:-}" ]; then
  echo "SKIP: TRINO_WORKER_HOST not set (the trino_worker_hosts[0] tofu output from trino-up.sh)"
  exit 0
fi
TRINO_PORT="${TRINO_PORT:-8080}"
TRINO_IMAGE="${TRINO_IMAGE:-trinodb/trino:465}"
KEY_FILE="${KEY_FILE:-$HOME/.ssh/spot-strata-rsa}"
[ -f "$KEY_FILE" ] || { echo "ERROR: SSH private key not found: $KEY_FILE (set KEY_FILE=..., and make sure the Trino stack was applied with -var key_pair_name matching it)"; exit 1; }
SSHOPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10"

# The worker must reach the coordinator by its private ip: the public ip from inside the VPC does
# not pass the security group's self-referencing internode rule. bench/.env's engine-reader keys
# lack EC2 permissions, so they are unset for the lookup.
TRINO_HOST_PRIVATE="${TRINO_HOST_PRIVATE:-$(env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY aws ec2 describe-instances \
  --filters "Name=ip-address,Values=${TRINO_HOST}" \
  --query 'Reservations[].Instances[].PrivateIpAddress' --output text 2>/dev/null)}"
[ -n "$TRINO_HOST_PRIVATE" ] || { echo "ERROR: could not resolve the coordinator's private ip from TRINO_HOST=$TRINO_HOST (set TRINO_HOST_PRIVATE=... explicitly)"; exit 1; }
REPORT="${1:-bench/reports/trino-compare-$(date +%Y%m%d-%H%M%S).txt}"
mkdir -p "$(dirname "$REPORT")"
: > "$REPORT"

WITH_DELETES="${BENCH_WITH_DELETES:-0}"
if [ -z "${TRINO_SCHEMA:-}" ]; then
  TRINO_SCHEMA="tpch"
  [ "$WITH_DELETES" = "1" ] && TRINO_SCHEMA="tpch_deletes"
fi
ENGINE_LABEL="trino"
[ "$WITH_DELETES" = "1" ] && ENGINE_LABEL="trino-deletes"

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

# "warmup" absorbs the SSH+container+JVM cold start so it stays out of q1's measurement.
NAMES=(warmup q1 q2 q3 q4 q5 q6 q7 q8 q9a q9b nq1 nq2 nq3 nq4 nq5)
QUERIES=("SELECT 1" "$Q1" "$Q2" "$Q3" "$Q4" "$Q5" "$Q6" "$Q7" "$Q8" "$Q9A" "$Q9B" "$NQ1" "$NQ2" "$NQ3" "$NQ4" "$NQ5")

# A sentinel SELECT after each query marks its completion in the streamed output. `--ignore-errors`
# is required: without it the CLI aborts the whole batch on the first failing statement.
BATCH=""
for i in "${!NAMES[@]}"; do
  BATCH="${BATCH}${QUERIES[$i]}; SELECT '__DONE_${NAMES[$i]}__'; "
done

echo "== launching persistent Trino CLI batch on worker $TRINO_WORKER_HOST ==" | tee -a "$REPORT"
echo "trino benchmark (one session via worker ${TRINO_WORKER_HOST}, coordinator ${TRINO_HOST_PRIVATE}:${TRINO_PORT} private) schema=${TRINO_SCHEMA} with_deletes=${WITH_DELETES} — $(date)" | tee -a "$REPORT"

declare -A PENDING
for name in "${NAMES[@]}"; do PENDING[$name]=1; done

# No external `timeout` here: it breaks the coproc pipe wiring. The read loop enforces timeouts.
# shellcheck disable=SC2016
coproc TRINOOUT {
  ssh $SSHOPTS -i "$KEY_FILE" ubuntu@"$TRINO_WORKER_HOST" \
    "sudo docker run --rm $TRINO_IMAGE trino --ignore-errors --server http://$TRINO_HOST_PRIVATE:$TRINO_PORT --catalog iceberg --schema $TRINO_SCHEMA --output-format CSV --execute \"$BATCH\"" \
    2>&1
}

start=$(date +%s.%N)
last=$start
while IFS= read -r -t 120 line <&"${TRINOOUT[0]}"; do
  now=$(date +%s.%N)
  for name in "${NAMES[@]}"; do
    if [ -n "${PENDING[$name]:-}" ] && [[ "$line" == *"__DONE_${name}__"* ]]; then
      el=$(awk "BEGIN{printf \"%.2f\", $now - $last}")
      last=$now
      unset "PENDING[$name]"
      [ "$name" = "warmup" ] && continue
      echo "  $name: ${el}s" | tee -a "$REPORT"
      echo "TIMING ${ENGINE_LABEL} ${name} ${el}" >> "$REPORT"
    fi
  done
  awk "BEGIN{exit !($now - $start > 900)}" && { echo "ERROR: overall batch timeout" | tee -a "$REPORT"; break; }
done
kill "${TRINOOUT_PID:-}" 2>/dev/null || true
wait 2>/dev/null || true

for name in "${NAMES[@]}"; do
  [ "$name" = "warmup" ] && continue
  [ -n "${PENDING[$name]:-}" ] && echo "  $name: FAILED (batch aborted before this query's marker — see raw output above)" | tee -a "$REPORT"
done
echo "Done. Report: $REPORT"
