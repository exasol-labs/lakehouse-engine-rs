#!/usr/bin/env bash
# Competitive engine comparison: Trino vs the lakehouse engine, over the SAME Glue Iceberg TPC-H
# tables. NOT a spec feature — manually invoked, like the rest of bench/. Requires an ephemeral
# Trino cluster stood up via `deploy/scripts/trino-up.sh <env>` first — this script NEVER
# auto-provisions (cost-safety: nothing shall be started unless used).
#
# Query text matches bench/athena_compare.sh verbatim (both Presto-derived, identical SQL) —
# translated from bench/run.sh's Q1-Q4 (lines ~321-349). Keep both in sync if you edit one.
#
# Methodology: ONE persistent Trino CLI session (not a fresh `docker run` per query) launched via
# SSH onto a TRINO WORKER node — not the operator's machine, not the coordinator. A prior version
# of this script spun up a fresh Docker container + fresh JVM cold start PER QUERY on the
# operator's own machine, reaching the coordinator over the public internet; that made native
# Trino look slower than it is, relative to bench/run.sh (VS) and bench/import_jdbc_trino.sh, both
# of which pay one lightweight `exapump` process (no JVM) per query talking to Exasol, which then
# does its own intra-VPC hop to whatever it's querying. Launching from a worker node instead gives
# this script the same two-hop shape: (operator machine -> the cluster's own node, over the
# internet) + (that node -> the thing being measured, intra-VPC) — matching exapump's (operator ->
# Exasol, over the internet) + (Exasol -> Trino, intra-VPC). A worker (not the coordinator) is
# used deliberately so there's still a real network hop to measure, not zero-latency localhost.
#
#   TRINO_HOST=<coordinator-ip> TRINO_WORKER_HOST=<worker-ip> ./trino_compare.sh
# No -e: run_timed must survive a failing query (OOM, syntax error, ...) and report it as FAILED
# rather than aborting the whole comparison — same convention as bench/import_ceiling.sh.
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

# The worker's docker container connects to the coordinator over the VPC-internal network, not
# the public internet — it must use the coordinator's PRIVATE ip. Addressing it via the public ip
# from inside the VPC does not reliably pass the security group's self-referencing "internode"
# rule (live-verified on test1: connections just hang/time out). Auto-resolve it from $TRINO_HOST
# via the AWS CLI (already a dependency of the rest of this benchmark suite); override with
# TRINO_HOST_PRIVATE if the operator machine has no AWS CLI/credentials configured. Explicitly
# unset the engine-reader static keys bench/.env just sourced (set -a above exported them) — they
# have no EC2 permissions, only Glue/S3, and would shadow AWS_PROFILE/the default credential chain.
TRINO_HOST_PRIVATE="${TRINO_HOST_PRIVATE:-$(env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY aws ec2 describe-instances \
  --filters "Name=ip-address,Values=${TRINO_HOST}" \
  --query 'Reservations[].Instances[].PrivateIpAddress' --output text 2>/dev/null)}"
[ -n "$TRINO_HOST_PRIVATE" ] || { echo "ERROR: could not resolve the coordinator's private ip from TRINO_HOST=$TRINO_HOST (set TRINO_HOST_PRIVATE=... explicitly)"; exit 1; }
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

# Q5-Q9b probe specific pushdown strengths/weaknesses beyond Q1-Q4 — identical SQL (dialect-
# adjusted) in bench/run.sh, bench/athena_compare.sh, deploy/scripts/spark_queries.py.
Q5="SELECT o.o_orderpriority, COUNT(*) AS cnt, SUM(l.l_extendedprice) AS revenue
FROM iceberg.tpch.orders o JOIN iceberg.tpch.lineitem l ON o.o_orderkey = l.l_orderkey
GROUP BY o.o_orderpriority ORDER BY o.o_orderpriority"

Q6="SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price,
       AVG(l_discount) AS avg_disc, COUNT(*) AS count_order
FROM iceberg.tpch.lineitem
GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"

Q7="SELECT COUNT(*) FROM (SELECT l_orderkey, COUNT(*) AS cnt FROM iceberg.tpch.lineitem GROUP BY l_orderkey) t"

Q8="SELECT COUNT(*) FROM iceberg.tpch.lineitem WHERE l_shipdate = DATE '1995-06-15'"

Q9A="SELECT SUM(l_quantity) FROM iceberg.tpch.lineitem"

Q9B="SELECT COUNT(*),
       SUM(l_orderkey), SUM(l_partkey), SUM(l_suppkey), SUM(l_linenumber),
       SUM(l_quantity), SUM(l_extendedprice), SUM(l_discount), SUM(l_tax),
       COUNT(DISTINCT l_returnflag), COUNT(DISTINCT l_linestatus),
       MIN(l_shipdate), MAX(l_commitdate), MIN(l_receiptdate),
       COUNT(DISTINCT l_shipinstruct), COUNT(DISTINCT l_shipmode),
       SUM(length(l_comment))
FROM iceberg.tpch.lineitem"

# NQ1-NQ5 close the arithmetic-aggregate-pushdown gap + probe LIKE/IN filters, ORDER BY+LIMIT, a
# 4-way join, and GROUP BY+HAVING — identical SQL (dialect-adjusted) in bench/run.sh,
# bench/athena_compare.sh, deploy/scripts/spark_queries.py.
NQ1="SELECT SUM(l_extendedprice * l_discount) AS revenue FROM iceberg.tpch.lineitem
WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"

NQ2="SELECT COUNT(*) FROM iceberg.tpch.lineitem
WHERE l_shipmode IN ('AIR','REG AIR') AND l_comment LIKE '%late%'"

NQ3="SELECT COUNT(*) AS cnt, SUM(ps.ps_supplycost) AS total_cost
FROM iceberg.tpch.part p JOIN iceberg.tpch.partsupp ps ON p.p_partkey = ps.ps_partkey
JOIN iceberg.tpch.supplier s ON ps.ps_suppkey = s.s_suppkey
JOIN iceberg.tpch.nation n ON s.s_nationkey = n.n_nationkey
WHERE p.p_size = 15 AND p.p_type LIKE '%BRASS%' AND n.n_name = 'GERMANY'"

NQ4="SELECT l_orderkey, l_extendedprice FROM iceberg.tpch.lineitem
ORDER BY l_extendedprice DESC LIMIT 20"

NQ5="SELECT o_orderpriority, o_orderstatus, COUNT(*) AS cnt, AVG(o_totalprice) AS avg_price
FROM iceberg.tpch.orders GROUP BY o_orderpriority, o_orderstatus
HAVING COUNT(*) > 1000000 ORDER BY o_orderpriority, o_orderstatus"

# Names/queries in run order. A leading "warmup" entry (discarded from the report) absorbs the
# one-time SSH+container+JVM cold start so it never lands inside q1's own measured window.
NAMES=(warmup q1 q2 q3 q4 q5 q6 q7 q8 q9a q9b nq1 nq2 nq3 nq4 nq5)
QUERIES=("SELECT 1" "$Q1" "$Q2" "$Q3" "$Q4" "$Q5" "$Q6" "$Q7" "$Q8" "$Q9A" "$Q9B" "$NQ1" "$NQ2" "$NQ3" "$NQ4" "$NQ5")

# One `--execute` batch, one JVM cold start for the whole run — not one per query. Each query is
# followed by a cheap sentinel SELECT so the orchestrator can timestamp completion by watching the
# streamed CSV output, without depending on the CLI's own internal timing-output format (which can
# vary by version/mode). `--ignore-errors` is load-bearing: without it, Trino CLI aborts the WHOLE
# batch on the first failing statement (verified live) — silently losing every later query's
# timing. With it, a failing query prints its error and the batch continues.
BATCH=""
for i in "${!NAMES[@]}"; do
  BATCH="${BATCH}${QUERIES[$i]}; SELECT '__DONE_${NAMES[$i]}__'; "
done

echo "== launching persistent Trino CLI batch on worker $TRINO_WORKER_HOST ==" | tee -a "$REPORT"
echo "trino benchmark (one session via worker ${TRINO_WORKER_HOST}, coordinator ${TRINO_HOST_PRIVATE}:${TRINO_PORT} private) — $(date)" | tee -a "$REPORT"

declare -A PENDING
for name in "${NAMES[@]}"; do PENDING[$name]=1; done

# NOTE: do not wrap this in an external `timeout` — empirically (live-verified on test1) that
# breaks bash's coproc pipe wiring and the read loop below never sees any output. The per-line
# `read -t` below plus the overall wall-clock check inside the loop are the safety net instead.
# shellcheck disable=SC2016
coproc TRINOOUT {
  ssh $SSHOPTS -i "$KEY_FILE" ubuntu@"$TRINO_WORKER_HOST" \
    "sudo docker run --rm $TRINO_IMAGE trino --ignore-errors --server http://$TRINO_HOST_PRIVATE:$TRINO_PORT --catalog iceberg --schema tpch --output-format CSV --execute \"$BATCH\"" \
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
      echo "TIMING trino ${name} ${el}" >> "$REPORT"
    fi
  done
  # Overall wall-clock ceiling (15 min) in case the batch stalls without closing the pipe.
  awk "BEGIN{exit !($now - $start > 900)}" && { echo "ERROR: overall batch timeout" | tee -a "$REPORT"; break; }
done
kill "${TRINOOUT_PID:-}" 2>/dev/null || true
wait 2>/dev/null || true

for name in "${NAMES[@]}"; do
  [ "$name" = "warmup" ] && continue
  [ -n "${PENDING[$name]:-}" ] && echo "  $name: FAILED (batch aborted before this query's marker — see raw output above)" | tee -a "$REPORT"
done
echo "Done. Report: $REPORT"
