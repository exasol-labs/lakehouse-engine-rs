#!/usr/bin/env bash
# Destroy the Exasol cluster for a named env (keeps the data-stack / S3 / Glue intact).
#
#   AWS_PROFILE=spot-strata-deployer ./cluster-down.sh <env_name>
set -euo pipefail

ENV="${1:?usage: cluster-down.sh <env_name>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
STACK="$HERE/../cluster-stack"
# key_pair_name has no default in variables.tf and is referenced by aws_instance.node's key_name,
# so `tofu destroy` needs a value even to tear down — without it, a non-interactive run (no TTY to
# prompt) hard-errors on "No value for required variable" and leaves the cluster running/billing.
KEY_PAIR_NAME="${KEY_PAIR_NAME:-spot-strata-key}"

cd "$STACK"
tofu workspace select "$ENV" >/dev/null 2>&1 || { echo "no workspace '$ENV'"; exit 1; }

echo "==> Destroying cluster '$ENV' (EC2, EBS, SG, SSM passwords). Data-stack untouched."
tofu destroy -var "env_name=$ENV" -var "key_pair_name=$KEY_PAIR_NAME" -auto-approve

# Drop the now-empty workspace (can't delete the one you're on -> switch to default first).
tofu workspace select default
tofu workspace delete "$ENV" 2>/dev/null || true
echo "==> Cluster '$ENV' destroyed."
