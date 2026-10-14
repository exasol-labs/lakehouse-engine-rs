#!/usr/bin/env bash
# Seeds or rotates the one SSH key shared by all env_name workspaces; SSM is its source of truth.
#   AWS_PROFILE=spot-strata-deployer ./rotate-cluster-key.sh [key_pair_name]   # default spot-strata-key
# The private key goes to SSM via file://, never a command line or stdout.
# Running nodes keep the old key in authorized_keys until re-provisioned.
set -euo pipefail

KEY_NAME="${1:-spot-strata-key}"
SSH_KEY_SSM="/spot-strata/deploy/ssh_key/${KEY_NAME}"

command -v aws >/dev/null 2>&1        || { echo "aws CLI not found"; exit 1; }
command -v ssh-keygen >/dev/null 2>&1 || { echo "ssh-keygen not found"; exit 1; }

WORK="$(mktemp -d)"
chmod 700 "$WORK"
cleanup() {
  [ -d "$WORK" ] || return 0
  command -v shred >/dev/null 2>&1 && find "$WORK" -type f -exec shred -u {} + 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT INT TERM

KP="$WORK/${KEY_NAME}"

echo "==> Generating new ed25519 key pair for '$KEY_NAME'" >&2
ssh-keygen -t ed25519 -f "$KP" -N "" -C "spot-strata-cluster-shared-key" >/dev/null

echo "==> Re-importing EC2 key pair '$KEY_NAME' (delete old if present, then import new public key)" >&2
aws ec2 delete-key-pair --key-name "$KEY_NAME" >/dev/null 2>&1 || true
aws ec2 import-key-pair --key-name "$KEY_NAME" \
  --public-key-material "fileb://${KP}.pub" >/dev/null

echo "==> Storing new private key in SSM $SSH_KEY_SSM (SecureString, overwrite)" >&2
aws ssm put-parameter --name "$SSH_KEY_SSM" --type SecureString \
  --value "file://${KP}" --overwrite \
  --description "Shared private SSH key for spot-strata cluster nodes (EC2 key pair '$KEY_NAME'); auto-fetched by cluster-up.sh" >/dev/null

cat >&2 <<EOF

Rotated shared key '$KEY_NAME'. The new private key lives in SSM only (local copies shredded).
Re-provision affected clusters to pick up the new public key:
  cd deploy/cluster-stack
  tofu apply -var env_name=<env> -var key_pair_name=$KEY_NAME
  ../scripts/cluster-up.sh <env>   # auto-fetches the new key from SSM
EOF
