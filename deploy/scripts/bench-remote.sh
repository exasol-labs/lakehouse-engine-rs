#!/usr/bin/env bash
# Failure-safe, cost-safe wrapper for the manual test1 remote-bench sequence. Chains the four steps
# from deploy/README.md ("2. Test cluster" / "3. Run the perf test" / "5. Tear down") into ONE command
# and ALWAYS tears the cluster down afterwards — on success, on failure, OR on interrupt (Ctrl-C /
# SIGTERM):
#
#   cluster-up.sh <env>  ->  secrets.sh <env>  ->  make bench  ->  cluster-down.sh <env> (via trap)
#
# A live r8i.2xlarge x N Exasol cluster bills continuously while it exists, so guaranteed teardown is
# the whole point: the trap is installed BEFORE anything is brought up, so even a half-finished
# cluster-up.sh (some nodes up, DB not started) or an aborted `make bench` is still destroyed. Same
# cost-safety discipline as deploy/README.md's Trino teardown warnings.
#
#   AWS_PROFILE=spot-strata-deployer ./bench-remote.sh <env_name>
#   AWS_PROFILE=spot-strata-deployer BENCH_WITH_DELETES=1 ./bench-remote.sh test1
#
# Any caller-exported BENCH_*/LAKEHOUSE_* env flows through `make bench` -> bench/run.sh untouched
# (run.sh lets caller-exported env WIN over bench/.env — see its own comment at ~line 133), so no
# special forwarding is needed here. This script must NOT reset or swallow the environment anywhere.
set -euo pipefail

ENV="${1:?usage: bench-remote.sh <env_name>}"
HERE="$(cd "$(dirname "$0")" && pwd)"

# Teardown runs on EVERY exit path. $? inside an EXIT trap is the exit status of the wrapped sequence
# (the standard `trap '...$?...' EXIT` idiom); capture it first so the final line reports the bench
# result independently of whether teardown itself succeeded — and so we can exit with the ORIGINAL
# bench rc (a caller/CI needs to know the bench failed, not just that cleanup ran).
teardown() {
  rc=$?
  echo "" >&2
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

echo "==> [1/3] cluster-up.sh $ENV" >&2
"$HERE/cluster-up.sh" "$ENV"

echo "==> [2/3] secrets.sh $ENV" >&2
"$HERE/secrets.sh" "$ENV"

echo "==> [3/3] make bench" >&2
(cd "$HERE/../.." && make bench)

echo "==> Bench sequence complete for '$ENV'; teardown (trap EXIT) follows." >&2
