#!/usr/bin/env bash
# Costs real money while running: tear down with trino-down.sh right after the benchmark.
#   AWS_PROFILE=spot-strata-deployer ./trino-up.sh <env_name>
set -euo pipefail

ENV="${1:?usage: trino-up.sh <env_name>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
STACK="$HERE/../trino-stack"

cd "$STACK"
tofu workspace select "$ENV" >/dev/null 2>&1 || tofu workspace new "$ENV"
tofu apply -var "env_name=$ENV" -var "key_pair_name=${KEY_PAIR_NAME:-spot-strata-key}" \
  -var "created_date=$(date -u +%F)" -auto-approve

HOST="$(tofu output -raw trino_coordinator_host)"
NODE_COUNT="$(tofu output -raw node_count)"

echo "==> Waiting for coordinator on $HOST:8080"
for _ in $(seq 1 60); do
  curl -sf "http://$HOST:8080/v1/info" >/dev/null 2>&1 && break
  sleep 5
done

# /v1/node needs a user header even without auth, and omits the coordinator itself.
WORKER_TARGET=$((NODE_COUNT - 1))
echo "==> Waiting for all $WORKER_TARGET worker(s) to register with the coordinator"
for _ in $(seq 1 60); do
  JOINED="$(curl -sf -H 'X-Trino-User: trino-up' "http://$HOST:8080/v1/node" 2>/dev/null | jq 'length' 2>/dev/null || echo 0)"
  [ "${JOINED:-0}" -ge "$WORKER_TARGET" ] 2>/dev/null && break
  sleep 5
done
[ "${JOINED:-0}" -ge "$WORKER_TARGET" ] || {
  echo "ERROR: only ${JOINED:-0}/${WORKER_TARGET} worker(s) joined — check /var/log/trino-userdata.log on each node"
  exit 1
}

cat <<EOF

Trino '$ENV' is up: http://$HOST:8080 (coordinator + ${JOINED}/${WORKER_TARGET} workers joined = ${NODE_COUNT} nodes total)
  export TRINO_HOST=$HOST      # for bench/trino_compare.sh — point at the COORDINATOR

REMEMBER: this cluster costs real money while running (${NODE_COUNT} node(s), each a sizable box).
  ./trino-down.sh $ENV         # destroy it IMMEDIATELY after the benchmark run
EOF
