#!/usr/bin/env bash
# Seed or rotate the SHARED cluster SSH key pair. Generates a fresh ed25519 key, re-imports the EC2
# key pair under the same name, and stores the new private key in SSM SecureString at
# /spot-strata/deploy/ssh_key/<key_pair_name> — the single source of truth cluster-up.sh auto-fetches
# from. Occasional MANUAL operator action; run it when the key is lost, is compromised, or you rotate
# on a schedule. This is the ONE shared key across all env_name workspaces (test1 and any future
# named clusters); per-environment keys would be a separate change, not this script.
#
#   AWS_PROFILE=spot-strata-deployer ./rotate-cluster-key.sh [key_pair_name]   # default spot-strata-key
#
# Security: the private key exists only inside a 0700 tempdir, is pushed to SSM via file:// (never on
# a command line or stdout), and the tempdir is shredded + removed on ANY exit (trap). The key
# material is never printed.
#
# NOTE: rotating does NOT touch already-running nodes' authorized_keys — the new public key takes
# effect for nodes created AFTER this runs. Re-provision affected clusters (tofu apply + cluster-up)
# to move them onto the new key.
set -euo pipefail

KEY_NAME="${1:-spot-strata-key}"
SSH_KEY_SSM="/spot-strata/deploy/ssh_key/${KEY_NAME}"

command -v aws >/dev/null 2>&1        || { echo "aws CLI not found"; exit 1; }
command -v ssh-keygen >/dev/null 2>&1 || { echo "ssh-keygen not found"; exit 1; }

# Private tempdir; clean it on ANY exit so no key material is ever left on disk.
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
