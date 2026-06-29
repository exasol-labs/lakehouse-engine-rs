#!/usr/bin/env bash
# Bring up the Exasol cluster for a named env: render .ccc/config from cluster-stack outputs,
# install c4 on node 1, run `c4 host play`, wait for the DB. SLC + .so install is done later by
# `make bench` (which already calls install-slc / bucketfs-upload-so).
#
#   AWS_PROFILE=spot-strata-deployer ./cluster-up.sh <env_name>
#
# Requires the EC2 private key locally. Override its path with KEY_FILE=... (default
# ~/.ssh/<key_pair_name>.pem). The key is copied to node 1 so c4 can reach the sibling nodes.
set -euo pipefail

ENV="${1:?usage: cluster-up.sh <env_name>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
STACK="$HERE/../cluster-stack"

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
KEY_FILE="${KEY_FILE:-$HOME/.ssh/${KEY_NAME}.pem}"
EXASOL_TAG="${EXASOL_IMAGE_TAG:-exasol-2025.2.1}"

[ -f "$KEY_FILE" ] || { echo "private key not found: $KEY_FILE (set KEY_FILE=...)"; exit 1; }

ssm() { aws ssm get-parameter --with-decryption --name "$1" --query 'Parameter.Value' --output text; }
DB_PASS="$(ssm "$CL_SSM/db_password")"
ADMIN_PASS="$(ssm "$CL_SSM/admin_password")"
BFS_PASS="$(ssm "$CL_SSM/bucketfs_password")"

echo "==> Rendering .ccc/config (${#INT_IPS[@]} hosts, reserve=$RESERVE)"
CFG="$(mktemp)"
cat >"$CFG" <<EOF
CCC_HOST_ADDRS="${INT_IPS[*]}"
CCC_HOST_EXTERNAL_ADDRS="${EXT_IPS[*]}"
CCC_HOST_DATADISK=/dev/nvme1n1
CCC_HOST_IMAGE_USER=ubuntu
CCC_HOST_KEY_PAIR_FILE=$(basename "$KEY_FILE")
CCC_PLAY_RESERVE_NODES=${RESERVE}
CCC_PLAY_DB_PASSWORD=${DB_PASS}
CCC_PLAY_ADMIN_PASSWORD=${ADMIN_PASS}
CCC_ADMINUI_START_SERVER=true
CCC_PLAY_WITH_DB=true
EOF

SSH="ssh -o StrictHostKeyChecking=no -o ConnectTimeout=15 -i $KEY_FILE ubuntu@$NODE1"
echo "==> Waiting for SSH on node 1 ($NODE1)"
for _ in $(seq 1 30); do $SSH true 2>/dev/null && break; sleep 5; done

echo "==> Copying key + config to node 1, installing c4"
scp -o StrictHostKeyChecking=no -i "$KEY_FILE" "$KEY_FILE" "ubuntu@$NODE1:/home/ubuntu/$(basename "$KEY_FILE")"
$SSH "chmod 600 /home/ubuntu/$(basename "$KEY_FILE") && mkdir -p /home/ubuntu/.ccc"
scp -o StrictHostKeyChecking=no -i "$KEY_FILE" "$CFG" "ubuntu@$NODE1:/home/ubuntu/.ccc/config"
rm -f "$CFG"
$SSH 'test -x ./c4 || (wget -q https://x-up.s3.amazonaws.com/releases/c4/linux/x86_64/latest/c4 -O c4 && chmod +x c4)'

echo "==> Running c4 host play -T @$EXASOL_TAG (this takes several minutes)"
$SSH "./c4 host play -T @$EXASOL_TAG"

echo "==> Waiting for Exasol DB on $NODE1:8563"
for _ in $(seq 1 60); do
  (exec 3<>"/dev/tcp/$NODE1/8563") 2>/dev/null && { exec 3>&-; echo "   DB port open"; break; }
  sleep 10
done

# ponytail: BucketFS write-password seam. c4 sets its own default BucketFS write password; we set it
# to our SSM value so `make bench` can PUT the SLC + .so. Best-effort via confd on node 1 — if the
# confd verb differs on your Exasol build, set the password once in the Admin UI (https://$NODE1:8443,
# BucketFS 'bfsdefault' -> default bucket) to match SSM $CL_SSM/bucketfs_password, then re-run bench.
echo "==> Setting BucketFS write password (best-effort)"
$SSH "./c4 connect -t1.11/cos -- confd_client bucketfs_bucket_modify bucketfs_name: bfsdefault bucket_name: default write_password: '$BFS_PASS'" \
  2>/dev/null && echo "   set via confd" || echo "   WARN: confd set failed — set it in the Admin UI to match SSM (see note above)"

cat <<EOF

Cluster '$ENV' is up.
  Node 1 (SSH/c4):   ssh -i $KEY_FILE ubuntu@$NODE1
  DB:                $NODE1:8563   (sys / <db_password in SSM $CL_SSM/db_password>)
  Admin UI:          https://$NODE1:8443

Next:
  ./scripts/secrets.sh $ENV     # writes bench/.env
  make bench                    # builds .so, installs SLC + .so, runs the perf test
EOF
