#!/usr/bin/env bash
# Runs the IMPORT FROM JDBC parallelism hypothesis experiment: does raw streaming-scan throughput
# scale with Exasol node count on the lakehouse-engine-rs VS path but NOT on the native
# IMPORT FROM JDBC path (bound to a single JDBC connection)? See specs/mission.md Core Capability #3
# (file-sharded GROUP BY shard_key fan-out) for the VS-side mechanism this is testing against.
#
# Owns the Trino comparison cluster's lifecycle end-to-end (bring up once, tear down once) — Trino
# has NO teardown safety today (trino-up.sh/trino-down.sh are two independent manual commands, no
# trap; deploy/README.md: "MUST be torn down immediately... no auto-stop"). Drives the Exasol side
# through bench-remote.sh (already self-contained/torn-down per invocation) once per node count in
# NODE_COUNTS. Run-and-kill by default for EVERY resource this script touches — no cost spilling.
#
#   AWS_PROFILE=spot-strata-deployer ./jdbc-parallelism-sweep.sh <exasol_env> <trino_env>
#   AWS_PROFILE=spot-strata-deployer NODE_COUNTS="2 4 8" ./jdbc-parallelism-sweep.sh test1 test1jdbc
#
# KEEP_ALIVE=1 forwards to bench-remote.sh, but ONLY on the last trial — bench-remote.sh's own
# KEEP_ALIVE check has no notion of "which trial", so passing it through unconditionally would skip
# Exasol teardown after EVERY trial, leaving the FIRST trial's cluster running and billing while the
# next trial's `tofu apply -var node_count=N` silently mutates that same still-live workspace out
# from under it. Trino is torn down regardless — no keep-alive option was requested for it, since
# it's a shared fixture across trials, not the thing under investigation.
set -euo pipefail

EXASOL_ENV="${1:?usage: jdbc-parallelism-sweep.sh <exasol_env> <trino_env>}"
TRINO_ENV="${2:?usage: jdbc-parallelism-sweep.sh <exasol_env> <trino_env>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
NODE_COUNTS="${NODE_COUNTS:-2 4}"

# Trino always torn down on exit — success, failure, or interrupt — same trap discipline as
# bench-remote.sh's own teardown().
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
# import_jdbc_trino.sh's JDBC connection originates FROM the Exasol node (same VPC as Trino), so it
# must use the coordinator's PRIVATE ip, not its public one — see outputs.tf's
# trino_coordinator_private_ip comment. TRINO_HOST (public) still flows through for anything else
# that might read it (logging, back-compat with bench/trino_compare.sh's own convention).
TRINO_JDBC_HOST="$(cd "$HERE/../trino-stack" && tofu output -raw trino_coordinator_private_ip)"
[ -n "$TRINO_JDBC_HOST" ] || { echo "ERROR: tofu output trino_coordinator_private_ip was empty" >&2; exit 1; }
export TRINO_HOST TRINO_JDBC_HOST

read -r -a COUNTS <<<"$NODE_COUNTS"
echo "==> [2/2] node-count trials: ${COUNTS[*]}" >&2
LAST_IDX=$(( ${#COUNTS[@]} - 1 ))
for i in "${!COUNTS[@]}"; do
  N="${COUNTS[$i]}"
  # Only the LAST trial may honor a caller-exported KEEP_ALIVE=1 — every earlier trial always
  # tears down, so the next trial's tofu apply never mutates a still-live cluster out from under it.
  TRIAL_KEEP_ALIVE=0
  [ "$i" -eq "$LAST_IDX" ] && TRIAL_KEEP_ALIVE="${KEEP_ALIVE:-0}"
  echo "==> Trial: node_count=$N (exasol_env=$EXASOL_ENV, trino_host=$TRINO_HOST, trino_jdbc_host=$TRINO_JDBC_HOST, keep_alive=$TRIAL_KEEP_ALIVE)" >&2
  BENCH_RUN_CEILING=1 NODE_COUNT="$N" TRINO_HOST="$TRINO_HOST" TRINO_JDBC_HOST="$TRINO_JDBC_HOST" KEEP_ALIVE="$TRIAL_KEEP_ALIVE" \
    "$HERE/bench-remote.sh" "$EXASOL_ENV"
done

echo "==> Sweep complete for '$EXASOL_ENV' x $NODE_COUNTS; Trino teardown (trap EXIT) follows." >&2
