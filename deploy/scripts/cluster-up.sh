#!/usr/bin/env bash
# Bring up the Exasol cluster for a named env. Runs c4 FROM THIS MACHINE (jumphost model): c4 must
# not run on a cluster node, because it SSHes to the nodes' external IPs and a node cannot hairpin to
# its own public IP. This machine is in the SG allowlist, so it can reach every node's external IP.
# SLC + .so install is done later by `make bench`.
#
#   AWS_PROFILE=spot-strata-deployer ./cluster-up.sh <env_name>
#
# Needs the EC2 private key. Override its path with KEY_FILE=... (default ~/.ssh/<key_pair_name>).
# If it isn't present locally, it is auto-fetched from SSM SecureString at
# /spot-strata/deploy/ssh_key/<key_pair_name> (the shared cluster key; same source of truth as the
# cluster passwords) — so a teammate with deployer credentials needs NO manual key-sharing step.
# Seed/rotate that SSM key with ./rotate-cluster-key.sh. c4 resolves CCC_HOST_KEY_PAIR_FILE (a bare
# name) in ~/.ssh, so the key is copied there.
set -euo pipefail

ENV="${1:?usage: cluster-up.sh <env_name>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
STACK="$HERE/../cluster-stack"
C4DIR="$HERE/../.c4"
C4="$C4DIR/c4"
EXASOL_TAG="${EXASOL_IMAGE_TAG:-exasol-2025.2.1}"
# DB memory = the MAX ALLOWABLE DB RAM (per DB architect), computed by c4's own formula
# `c4 _ calculate-memory NODE_MEMORY_MiB` (leaves headroom for host/COS/UDFs), summed over active
# nodes (CCC_PLAY_DB_MEM_SIZE is the total). We set it explicitly rather than leaving it empty:
# empty/omitted does NOT trigger auto-sizing on this c4 build — it leaves MemSize=0, which makes the
# DataVolume setup invalid so the DB never starts (db_start dies in _check_db_version:
# "Cannot parse version Setup does not seem to have a valid setup.").
SSHOPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10"

cd "$STACK"
tofu workspace select "$ENV" >/dev/null 2>&1 || { echo "no workspace '$ENV' (tofu apply first)"; exit 1; }
OUT="$(tofu output -json)"
jqr() { jq -r "$1" <<<"$OUT"; }
mapfile -t INT_IPS < <(jqr '.internal_ips.value[]')
mapfile -t EXT_IPS < <(jqr '.external_ips.value[]')
RESERVE="$(jqr '.reserve_nodes.value')"
KEY_NAME="$(jqr '.key_pair_name.value')"
CL_SSM="$(jqr '.ssm_root.value')"
NODE1="${EXT_IPS[0]}"
KEY_FILE="${KEY_FILE:-$HOME/.ssh/${KEY_NAME}}"
SSH_KEY_SSM="/spot-strata/deploy/ssh_key/${KEY_NAME}"

# If the private key isn't present locally, fetch the shared key from SSM SecureString — the same
# single source of truth as the cluster passwords below, so a teammate with deployer creds needs no
# manual key-sharing. Security: the key material is written straight into a 0600 file (mktemp always
# creates mode 0600) and moved atomically into place; it never touches stdout/stderr and is never
# world/group-readable, even transiently. A failed fetch (missing param / no permission) removes the
# temp and falls through to the original hard error — the failure is not swallowed.
if [ ! -f "$KEY_FILE" ]; then
  echo "==> private key not found locally ($KEY_FILE); fetching shared key from SSM $SSH_KEY_SSM" >&2
  mkdir -p "$(dirname "$KEY_FILE")"
  KEY_TMP="$(mktemp "$(dirname "$KEY_FILE")/.ssh-key-fetch.XXXXXX")"
  if aws ssm get-parameter --with-decryption --name "$SSH_KEY_SSM" \
        --query 'Parameter.Value' --output text >"$KEY_TMP" 2>/dev/null && [ -s "$KEY_TMP" ]; then
    chmod 600 "$KEY_TMP"
    mv -f "$KEY_TMP" "$KEY_FILE"
    echo "==> fetched shared key from SSM to $KEY_FILE (mode 0600)" >&2
  else
    rm -f "$KEY_TMP"
    echo "==> SSM fetch failed for $SSH_KEY_SSM (parameter missing or no permission)" >&2
  fi
fi

[ -f "$KEY_FILE" ] || { echo "private key not found: $KEY_FILE (set KEY_FILE=..., or seed it in SSM at $SSH_KEY_SSM via rotate-cluster-key.sh)"; exit 1; }
KEY_BASE="$(basename "$KEY_FILE")"

ssm() { aws ssm get-parameter --with-decryption --name "$1" --query 'Parameter.Value' --output text; }
DB_PASS="$(ssm "$CL_SSM/db_password")"
ADMIN_PASS="$(ssm "$CL_SSM/admin_password")"
BFS_PASS="$(ssm "$CL_SSM/bucketfs_password")"

# c4 looks for the key by bare name in the running user's ~/.ssh.
mkdir -p "$HOME/.ssh"
[ "$KEY_FILE" = "$HOME/.ssh/$KEY_BASE" ] || cp "$KEY_FILE" "$HOME/.ssh/$KEY_BASE"
chmod 600 "$HOME/.ssh/$KEY_BASE"

echo "==> Fetching c4 (local)" >&2
mkdir -p "$C4DIR"
[ -x "$C4" ] || { wget -q https://x-up.s3.amazonaws.com/releases/c4/linux/x86_64/latest/c4 -O "$C4" && chmod +x "$C4"; }

echo "==> Waiting for SSH on all nodes" >&2
for ip in "${EXT_IPS[@]}"; do
  for _ in $(seq 1 30); do
    ssh $SSHOPTS -i "$KEY_FILE" ubuntu@"$ip" true 2>/dev/null && break
    sleep 5
  done
done

# Max allowable DB RAM via c4's own formula: per-node from real node RAM, then × active nodes (total).
PER_NODE_MIB="$(ssh $SSHOPTS -i "$KEY_FILE" ubuntu@"$NODE1" "awk '/MemTotal/{print int(\$2/1024)}' /proc/meminfo")"
PER_NODE_DB="$("$C4" _ calculate-memory "$PER_NODE_MIB" 2>/dev/null)"
ACTIVE_NODES=$(( ${#INT_IPS[@]} - RESERVE )); [ "$ACTIVE_NODES" -ge 1 ] || ACTIVE_NODES=1
DB_MEM_SIZE=$(( PER_NODE_DB * ACTIVE_NODES ))
[ "$DB_MEM_SIZE" -ge 1024 ] || { echo "calculate-memory returned '$PER_NODE_DB' for ${PER_NODE_MIB}MiB — bad"; exit 1; }
echo "==> DB memory: c4 calculate-memory(${PER_NODE_MIB}MiB)=${PER_NODE_DB}MiB/node x ${ACTIVE_NODES} = ${DB_MEM_SIZE}MiB total (max allowable)" >&2

echo "==> Rendering ~/.ccc/config (${#INT_IPS[@]} hosts, reserve=$RESERVE, db_mem=${DB_MEM_SIZE}MiB)" >&2
mkdir -p "$HOME/.ccc"
cat >"$HOME/.ccc/config" <<EOF
CCC_HOST_ADDRS="${INT_IPS[*]}"
CCC_HOST_EXTERNAL_ADDRS="${EXT_IPS[*]}"
CCC_HOST_DATADISK=/dev/nvme1n1
CCC_HOST_IMAGE_USER=ubuntu
CCC_HOST_KEY_PAIR_FILE=$KEY_BASE
CCC_PLAY_RESERVE_NODES=${RESERVE}
CCC_PLAY_DB_MEM_SIZE=${DB_MEM_SIZE}
CCC_PLAY_DB_PASSWORD=${DB_PASS}
CCC_PLAY_ADMIN_PASSWORD=${ADMIN_PASS}
CCC_ADMINUI_START_SERVER=true
CCC_PLAY_WITH_DB=true
EOF

echo "==> Running c4 host play -T @$EXASOL_TAG (several minutes)" >&2
"$C4" host play -T @"$EXASOL_TAG"

# Ensure the DB is started (auto_start can lag; explicit start is idempotent once MemSize > 0).
echo "==> Starting Exasol DB" >&2
for _ in $(seq 1 12); do ssh $SSHOPTS -p 20002 -i "$KEY_FILE" root@"$NODE1" true 2>/dev/null && break; sleep 10; done
ssh $SSHOPTS -p 20002 -i "$KEY_FILE" root@"$NODE1" "confd_client db_start db_name: Exasol" 2>&1 | head -3 || true

echo "==> Waiting for Exasol DB on $NODE1:8563" >&2
for _ in $(seq 1 60); do
  (exec 3<>"/dev/tcp/$NODE1/8563") 2>/dev/null && { exec 3>&-; echo "   DB port open"; break; }
  sleep 10
done

# The BucketFS write password is generated by c4 and lives (base64) in EXAConf; secrets.sh reads it
# from there for bench. Nothing to set here.

cat <<EOF

Cluster '$ENV' is up.
  c4 (local):   $C4 ps
  DB:           $NODE1:8563   (sys / SSM $CL_SSM/db_password)
  Admin UI:     https://$NODE1:8443

Next:
  ./scripts/secrets.sh $ENV     # writes bench/.env
  make bench                    # builds .so, installs SLC + .so, runs the perf test
EOF
