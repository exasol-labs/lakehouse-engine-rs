#!/usr/bin/env bash
# Destroy the Trino benchmark cluster for a named env (coordinator + all workers; data-stack / S3
# / Glue / the Exasol test1 cluster all untouched).
#
#   AWS_PROFILE=spot-strata-deployer ./trino-down.sh <env_name>
set -euo pipefail

ENV="${1:?usage: trino-down.sh <env_name>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
STACK="$HERE/../trino-stack"

cd "$STACK"
tofu workspace select "$ENV" >/dev/null 2>&1 || { echo "no workspace '$ENV'"; exit 1; }

echo "==> Destroying Trino '$ENV' (coordinator + worker EC2 nodes, SG, IAM role). Data-stack and test1 untouched."
tofu destroy -var "env_name=$ENV" -var "key_pair_name=${KEY_PAIR_NAME:-spot-strata-key}" -auto-approve

tofu workspace select default
tofu workspace delete "$ENV" 2>/dev/null || true
echo "==> Trino '$ENV' destroyed."
