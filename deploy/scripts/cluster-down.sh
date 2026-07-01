#!/usr/bin/env bash
# Destroy the Exasol cluster for a named env (keeps the data-stack / S3 / Glue intact).
#
#   AWS_PROFILE=spot-strata-deployer ./cluster-down.sh <env_name>
set -euo pipefail

ENV="${1:?usage: cluster-down.sh <env_name>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
STACK="$HERE/../cluster-stack"

cd "$STACK"
tofu workspace select "$ENV" >/dev/null 2>&1 || { echo "no workspace '$ENV'"; exit 1; }

echo "==> Destroying cluster '$ENV' (EC2, EBS, SG, SSM passwords). Data-stack untouched."
tofu destroy -var "env_name=$ENV" -auto-approve

# Drop the now-empty workspace (can't delete the one you're on -> switch to default first).
tofu workspace select default
tofu workspace delete "$ENV" 2>/dev/null || true
echo "==> Cluster '$ENV' destroyed."
