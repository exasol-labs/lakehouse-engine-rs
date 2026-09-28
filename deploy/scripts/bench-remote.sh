#!/usr/bin/env bash
# Remote bench in one command: tofu apply -> cluster-up -> secrets -> make bench
# [-> BENCH_RUN_CEILING=1: import_jdbc_trino.sh, import_ceiling.sh] -> cluster-down.
# The cluster bills while it exists, so the teardown trap is installed before anything is brought
# up and fires on success, failure, and interrupt.
#
#   AWS_PROFILE=... [NODE_COUNT=4] [BENCH_WITH_DELETES=1] [KEEP_ALIVE=1] ./bench-remote.sh <env_name>
#
# Caller-exported BENCH_*/LAKEHOUSE_* env must reach bench/run.sh untouched; never reset it here.
set -euo pipefail

ENV="${1:?usage: bench-remote.sh <env_name>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
NODE_COUNT="${NODE_COUNT:-2}"
KEY_PAIR_NAME="${KEY_PAIR_NAME:-spot-strata-key}"

# KEEP_ALIVE=1 is an opt-in escape hatch for manual investigation; never default it. The script
# exits with the bench rc, not the teardown rc.
teardown() {
  rc=$?
  echo "" >&2
  if [ "${KEEP_ALIVE:-0}" = "1" ]; then
    echo "==> KEEP_ALIVE=1: skipping teardown. Cluster '$ENV' is STILL RUNNING AND BILLING." >&2
    echo "==> Tear it down when done investigating: $HERE/cluster-down.sh $ENV" >&2
    exit "$rc"
  fi
  echo "==> Teardown (trap EXIT): bench sequence exited rc=$rc; running cluster-down.sh $ENV ..." >&2
  if "$HERE/cluster-down.sh" "$ENV"; then
    echo "==> TEARDOWN OK: cluster-down.sh $ENV completed (wrapped bench sequence rc=$rc)." >&2
  else
    down_rc=$?
    echo "==> TEARDOWN FAILED: cluster-down.sh $ENV exited $down_rc — cluster '$ENV' MAY STILL BE RUNNING AND BILLING. Re-run: $HERE/cluster-down.sh $ENV" >&2
  fi
  echo "==> Verify via 'aws ec2 describe-instances' that all '$ENV' cluster nodes actually terminated before considering this run done — a successful cluster-down.sh is not by itself proof of termination." >&2
  exit "$rc"
}
trap teardown EXIT
# Untrapped SIGINT/SIGTERM skip the EXIT trap; route them through `exit` so teardown runs once.
trap 'exit 130' INT
trap 'exit 143' TERM

echo "==> [1/4] tofu apply (env=$ENV node_count=$NODE_COUNT)" >&2
( cd "$HERE/../cluster-stack" \
  && { tofu workspace select "$ENV" 2>/dev/null || tofu workspace new "$ENV"; } \
  && tofu apply -var "env_name=$ENV" -var "key_pair_name=$KEY_PAIR_NAME" -var "node_count=$NODE_COUNT" \
       -var "created_date=$(date -u +%F)" -auto-approve )

echo "==> [2/4] cluster-up.sh $ENV" >&2
"$HERE/cluster-up.sh" "$ENV"

echo "==> [3/4] secrets.sh $ENV" >&2
"$HERE/secrets.sh" "$ENV"

echo "==> [4/4] make bench" >&2
bench_failed=0
(cd "$HERE/../.." && make bench) || {
  rc=$?
  echo "==> WARN: make bench failed (rc=$rc) — docs/performance.md documents a known-benign flake in" >&2
  echo "==> run.sh's TRAILING pushdown-check block (EXPLAIN VIRTUAL, after all timed queries) that fails" >&2
  echo "==> the exit code without affecting the 15 queries' own correctness/timing. Not swallowed: still" >&2
  echo "==> counted as this trial's failure below, but doesn't block BENCH_RUN_CEILING's legs from running." >&2
  bench_failed=1
}

# Independent legs: they run even if make bench or the other leg failed.
jdbc_failed=0
if [ "${BENCH_RUN_CEILING:-0}" = "1" ]; then
  echo "==> BENCH_RUN_CEILING=1: running import_jdbc_trino.sh + import_ceiling.sh" >&2
  (cd "$HERE/../.." && bench/import_jdbc_trino.sh) || { echo "==> WARN: import_jdbc_trino.sh failed (rc=$?)" >&2; jdbc_failed=1; }
  # Not fatal: its full materializations can trip the test cluster's raw-size license cap (R0010).
  (cd "$HERE/../.." && bench/import_ceiling.sh) || echo "==> WARN: import_ceiling.sh failed (rc=$?) — not fatal, see comment" >&2
fi

# Lets a sweeping caller (jdbc-parallelism-sweep.sh) stop after a failed trial.
if [ "$bench_failed" -ne 0 ] || [ "$jdbc_failed" -ne 0 ]; then
  echo "==> This trial had at least one failed leg (make bench / JDBC) — reporting as failed." >&2
  exit 1
fi

echo "==> Bench sequence complete for '$ENV'; teardown (trap EXIT) follows." >&2
