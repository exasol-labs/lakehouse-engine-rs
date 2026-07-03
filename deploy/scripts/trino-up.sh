#!/usr/bin/env bash
# Stand up (or update) the ephemeral single-node Trino benchmark box for a named env, then wait for
# it to answer /v1/info. Cost-safety: this is the ONLY thing that creates the Trino EC2 node — never
# run implicitly by data-stack/cluster-stack/bench scripts. Tear it down with trino-down.sh when done.
#
#   AWS_PROFILE=spot-strata-deployer ./trino-up.sh <env_name>
set -euo pipefail

ENV="${1:?usage: trino-up.sh <env_name>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
STACK="$HERE/../trino-stack"

cd "$STACK"
tofu workspace select "$ENV" >/dev/null 2>&1 || tofu workspace new "$ENV"
tofu apply -var "env_name=$ENV" -var "key_pair_name=${KEY_PAIR_NAME:-spot-strata-key}" \
  -var "created_date=$(date -u +%F)" -auto-approve

HOST="$(tofu output -raw trino_host)"

echo "==> Waiting for Trino on $HOST:8080"
for _ in $(seq 1 60); do
  curl -sf "http://$HOST:8080/v1/info" >/dev/null 2>&1 && break
  sleep 5
done

cat <<EOF

Trino '$ENV' is up: http://$HOST:8080
  export TRINO_HOST=$HOST      # for bench/trino_compare.sh

REMEMBER: this node costs money while running.
  ./trino-down.sh $ENV         # destroy it when you're done benchmarking
EOF
