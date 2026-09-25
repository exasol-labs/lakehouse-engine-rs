#!/usr/bin/env bash
# Usage: AWS_PROFILE=spot-strata-deployer ./lakekeeper-down.sh <env_name>
set -euo pipefail

ENV="${1:?usage: lakekeeper-down.sh <env_name>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
STACK="$HERE/../lakekeeper-stack"

cd "$STACK"
tofu workspace select "$ENV" >/dev/null 2>&1 || { echo "no workspace '$ENV'"; exit 1; }

echo "==> Destroying Lakekeeper '$ENV' (EC2, EBS, SG, IAM role/user, SSM parameters). Data-stack and test1 untouched."
tofu destroy -var "env_name=$ENV" -var "key_pair_name=${KEY_PAIR_NAME:-spot-strata-key}" -auto-approve

tofu workspace select default
tofu workspace delete "$ENV" 2>/dev/null || true
echo "==> Lakekeeper '$ENV' destroyed."
