#!/usr/bin/env bash
# Failure-safe, cost-safe wrapper for the manual test1 remote-bench sequence. Chains
# tofu-apply/"1. Test cluster" -> "2. Run the perf test" -> "3. Tear down" (deploy/README.md's
# numbering) into ONE command and, by default, ALWAYS tears the cluster down afterwards — on
# success, on failure, OR on interrupt (Ctrl-C / SIGTERM):
#
#   tofu apply (node_count)  ->  cluster-up.sh <env>  ->  secrets.sh <env>  ->  make bench
#     -> [BENCH_RUN_CEILING=1: import_ceiling.sh, import_jdbc_trino.sh]  ->  cluster-down.sh <env>
#
# A live r8i.2xlarge x N Exasol cluster bills continuously while it exists, so guaranteed teardown is
# the whole point: the trap is installed BEFORE anything is brought up, so even a half-finished
# cluster-up.sh (some nodes up, DB not started) or an aborted `make bench` is still destroyed. Same
# cost-safety discipline as deploy/README.md's Trino teardown warnings.
#
#   AWS_PROFILE=spot-strata-deployer ./bench-remote.sh <env_name>
#   AWS_PROFILE=spot-strata-deployer BENCH_WITH_DELETES=1 ./bench-remote.sh test1
#   AWS_PROFILE=spot-strata-deployer NODE_COUNT=4 ./bench-remote.sh test1
#   AWS_PROFILE=spot-strata-deployer KEEP_ALIVE=1 ./bench-remote.sh test1   # skip teardown (see below)
#
# Any caller-exported BENCH_*/LAKEHOUSE_* env flows through `make bench` -> bench/run.sh untouched
# (run.sh lets caller-exported env WIN over bench/.env — see its own comment at ~line 133), so no
# special forwarding is needed here. This script must NOT reset or swallow the environment anywhere.
set -euo pipefail

ENV="${1:?usage: bench-remote.sh <env_name>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
NODE_COUNT="${NODE_COUNT:-2}"
KEY_PAIR_NAME="${KEY_PAIR_NAME:-spot-strata-key}"

# Teardown runs on EVERY exit path unless KEEP_ALIVE=1 was exported — an explicit, opt-in escape
# hatch for follow-up manual investigation on a still-running cluster. Default is always run-and-
# kill; KEEP_ALIVE must never be the default for a sweep or an unattended/scripted invocation. $?
# inside an EXIT trap is the exit status of the wrapped sequence (the standard `trap '...$?...' EXIT`
# idiom); capture it first so the final line reports the bench result independently of whether
# teardown itself succeeded — and so we can exit with the ORIGINAL bench rc (a caller/CI needs to
# know the bench failed, not just that cleanup ran).
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
# Untrapped SIGINT/SIGTERM would kill the shell WITHOUT running the EXIT trap; funnel them into `exit`
# so the single EXIT handler above fires exactly once (no double cluster-down) with a meaningful rc.
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
(cd "$HERE/../.." && make bench)

# Opt-in: also run the native-reader/JDBC ceiling comparisons while the cluster is still up, inside
# the same guaranteed-teardown window (TRINO_HOST must already be exported for import_jdbc_trino.sh
# to run; it SKIPs cleanly on its own if unset). Off by default so ordinary bench-remote.sh usage
# keeps its existing cost/time profile. Each leg runs even if the OTHER one hard-errors (e.g.
# import_ceiling.sh's missing-report-file checks) — JDBC runs first since it's the one this
# experiment actually needs; import_ceiling.sh's VS-path numbers are the free bonus. But if EITHER
# leg failed, this run must NOT report success: both scripts now track their own FAILED flag and
# exit non-zero on a real failure (not just "the script ran"), so propagate that here — otherwise
# a caller sweeping multiple node counts (jdbc-parallelism-sweep.sh) would have no signal to stop
# after a doomed trial and would burn more real AWS time on a config that's already broken.
if [ "${BENCH_RUN_CEILING:-0}" = "1" ]; then
  echo "==> BENCH_RUN_CEILING=1: running import_jdbc_trino.sh + import_ceiling.sh" >&2
  ceiling_failed=0
  (cd "$HERE/../.." && bench/import_jdbc_trino.sh) || { echo "==> WARN: import_jdbc_trino.sh failed (rc=$?)" >&2; ceiling_failed=1; }
  (cd "$HERE/../.." && bench/import_ceiling.sh) || { echo "==> WARN: import_ceiling.sh failed (rc=$?)" >&2; ceiling_failed=1; }
  if [ "$ceiling_failed" -ne 0 ]; then
    echo "==> BENCH_RUN_CEILING=1: at least one leg failed — reporting this trial as failed." >&2
    exit 1
  fi
fi

echo "==> Bench sequence complete for '$ENV'; teardown (trap EXIT) follows." >&2
