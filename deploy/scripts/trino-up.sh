#!/usr/bin/env bash
# Stand up (or update) the ephemeral Trino cluster for a named env (coordinator + workers, sized to
# match Exasol test1's node type/count — see deploy/trino-stack/variables.tf), then wait for the
# coordinator to answer /v1/info AND for every worker to register with it. Cost-safety: this is the
# ONLY thing that creates the Trino EC2 nodes — never run implicitly by data-stack/cluster-stack/
# bench scripts. Tear it down with trino-down.sh IMMEDIATELY after the benchmark run — it costs
# real money while running (2x r8i.2xlarge by default, not a single small box).
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

HOST="$(tofu output -raw trino_coordinator_host)"
NODE_COUNT="$(tofu output -raw node_count)"

echo "==> Waiting for coordinator on $HOST:8080"
for _ in $(seq 1 60); do
  curl -sf "http://$HOST:8080/v1/info" >/dev/null 2>&1 && break
  sleep 5
done

# /v1/node requires a user-identifying header even with no auth configured, and (found
# live-verifying) only lists nodes reached via the announcement/discovery protocol — the
# coordinator doesn't appear in its own list, so the target is workers only (node_count - 1).
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
