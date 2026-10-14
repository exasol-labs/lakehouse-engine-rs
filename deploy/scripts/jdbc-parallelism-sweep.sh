#!/usr/bin/env bash
# Does raw-scan throughput scale with Exasol node count on the VS path but not on native
# IMPORT FROM JDBC (a single connection)? Brings Trino up once and always tears it down; runs
# bench-remote.sh once per node count.
#
#   AWS_PROFILE=... [NODE_COUNTS="2 4 8"] ./jdbc-parallelism-sweep.sh <exasol_env> <trino_env>
#
# KEEP_ALIVE=1 applies to the last trial only; honoring it earlier would leave a live cluster that
# the next trial's `tofu apply` mutates.
set -euo pipefail

EXASOL_ENV="${1:?usage: jdbc-parallelism-sweep.sh <exasol_env> <trino_env>}"
TRINO_ENV="${2:?usage: jdbc-parallelism-sweep.sh <exasol_env> <trino_env>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
NODE_COUNTS="${NODE_COUNTS:-2 4}"

teardown() {
  rc=$?
  echo "" >&2
  echo "==> Teardown (trap EXIT): tearing down Trino '$TRINO_ENV' ..." >&2
  if "$HERE/trino-down.sh" "$TRINO_ENV"; then
    echo "==> TEARDOWN OK: trino-down.sh $TRINO_ENV completed (sweep exited rc=$rc)." >&2
  else
    down_rc=$?
    echo "==> TEARDOWN FAILED: trino-down.sh $TRINO_ENV exited $down_rc — Trino '$TRINO_ENV' MAY STILL BE RUNNING AND BILLING. Re-run: $HERE/trino-down.sh $TRINO_ENV" >&2
  fi
  echo "==> Verify via 'aws ec2 describe-instances' that all '$EXASOL_ENV'/'$TRINO_ENV' nodes actually terminated before considering this run done." >&2
  exit "$rc"
}
trap teardown EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "==> [1/2] trino-up.sh $TRINO_ENV" >&2
"$HERE/trino-up.sh" "$TRINO_ENV"
TRINO_HOST="$(cd "$HERE/../trino-stack" && tofu output -raw trino_coordinator_host)"
[ -n "$TRINO_HOST" ] || { echo "ERROR: tofu output trino_coordinator_host was empty" >&2; exit 1; }
# The JDBC connection originates from the Exasol node in the same VPC, so it needs the private ip.
TRINO_JDBC_HOST="$(cd "$HERE/../trino-stack" && tofu output -raw trino_coordinator_private_ip)"
[ -n "$TRINO_JDBC_HOST" ] || { echo "ERROR: tofu output trino_coordinator_private_ip was empty" >&2; exit 1; }
export TRINO_HOST TRINO_JDBC_HOST

read -r -a COUNTS <<<"$NODE_COUNTS"
echo "==> [2/2] node-count trials: ${COUNTS[*]}" >&2
LAST_IDX=$(( ${#COUNTS[@]} - 1 ))
for i in "${!COUNTS[@]}"; do
  N="${COUNTS[$i]}"
  TRIAL_KEEP_ALIVE=0
  [ "$i" -eq "$LAST_IDX" ] && TRIAL_KEEP_ALIVE="${KEEP_ALIVE:-0}"
  echo "==> Trial: node_count=$N (exasol_env=$EXASOL_ENV, trino_host=$TRINO_HOST, trino_jdbc_host=$TRINO_JDBC_HOST, keep_alive=$TRIAL_KEEP_ALIVE)" >&2
  BENCH_RUN_CEILING=1 NODE_COUNT="$N" TRINO_HOST="$TRINO_HOST" TRINO_JDBC_HOST="$TRINO_JDBC_HOST" KEEP_ALIVE="$TRIAL_KEEP_ALIVE" \
    "$HERE/bench-remote.sh" "$EXASOL_ENV"
done

echo "==> Sweep complete for '$EXASOL_ENV' x $NODE_COUNTS; Trino teardown (trap EXIT) follows." >&2
