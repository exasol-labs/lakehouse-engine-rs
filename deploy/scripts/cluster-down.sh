#!/usr/bin/env bash
#   AWS_PROFILE=spot-strata-deployer ./cluster-down.sh <env_name>
set -euo pipefail

ENV="${1:?usage: cluster-down.sh <env_name>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
STACK="$HERE/../cluster-stack"
# key_pair_name has no default; without it a non-interactive destroy errors and leaves the cluster billing.
KEY_PAIR_NAME="${KEY_PAIR_NAME:-spot-strata-key}"

cd "$STACK"
tofu workspace select "$ENV" >/dev/null 2>&1 || { echo "no workspace '$ENV'"; exit 1; }

echo "==> Destroying cluster '$ENV' (EC2, EBS, SG, SSM passwords). Data-stack untouched."
tofu destroy -var "env_name=$ENV" -var "key_pair_name=$KEY_PAIR_NAME" -auto-approve

tofu workspace select default
tofu workspace delete "$ENV" 2>/dev/null || true
echo "==> Cluster '$ENV' destroyed."
