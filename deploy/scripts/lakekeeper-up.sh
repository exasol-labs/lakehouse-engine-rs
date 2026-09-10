#!/usr/bin/env bash
# Stand up (or update) the ephemeral Lakekeeper catalog for a named env (postgres + keycloak +
# lakekeeper, one EC2 box), wait for it to answer its health endpoint, then register every
# TPC-H table already cataloged in the data-stack's Glue database into it by reference — no data
# rewrite, no separate warehouse credential to set up by hand. Cost-safety: this is the ONLY thing
# that creates the Lakekeeper EC2 box — never run implicitly by data-stack/cluster-stack/trino-
# stack/bench scripts. Tear it down with lakekeeper-down.sh once the benchmark or demo is done —
# it costs real money while running.
#
#   AWS_PROFILE=spot-strata-deployer ./lakekeeper-up.sh <env_name>
set -euo pipefail

ENV="${1:?usage: lakekeeper-up.sh <env_name>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
STACK="$HERE/../lakekeeper-stack"

cd "$STACK"
tofu workspace select "$ENV" >/dev/null 2>&1 || tofu workspace new "$ENV"
tofu apply -var "env_name=$ENV" -var "key_pair_name=${KEY_PAIR_NAME:-spot-strata-key}" \
  -var "created_date=$(date -u +%F)" -auto-approve

warn_still_billing_on_nonzero_exit() {
  local rc=$?
  [ "$rc" -eq 0 ] || echo "NOTE: the Lakekeeper EC2 box for $ENV was created and is BILLING. Destroy it: $HERE/lakekeeper-down.sh $ENV" >&2
  exit "$rc"
}
trap warn_still_billing_on_nonzero_exit EXIT

PUBLIC_HOST="$(tofu output -raw public_host)"
LAKEKEEPER_PORT="$(tofu output -raw lakekeeper_port)"
LK_SSM="$(tofu output -raw ssm_root)"
LK_WAREHOUSE="$(tofu output -raw warehouse_name)"
LK_CATALOG_URI="$(tofu output -raw catalog_uri_public)"
LK_TOKEN_URI="$(tofu output -raw token_uri_public)"
LK_CLIENT_ID="$(tofu output -raw oidc_client_id)"
# The data-stack's own SSM root, re-published by this stack from the data-stack remote state it
# already reads. Never composed from $ENV: that is the LAKEKEEPER env name, while the data-stack
# defaults its own to "data" — composing it sends every source read at a root no stack owns, and
# only after tofu apply above has already created the billable box.
DATA_SSM="$(tofu output -raw data_ssm_root)"

HEALTH_URL="http://$PUBLIC_HOST:$LAKEKEEPER_PORT/health"
echo "==> Waiting for Lakekeeper on $HEALTH_URL"
for _ in $(seq 1 60); do
  curl -sf "$HEALTH_URL" >/dev/null 2>&1 && break
  sleep 5
done
curl -sf "$HEALTH_URL" >/dev/null 2>&1 || {
  echo "ERROR: Lakekeeper never answered $HEALTH_URL — check /var/log/lakekeeper-userdata.log on the instance. The box was created and is still BILLING: destroy it with $HERE/lakekeeper-down.sh $ENV" >&2
  exit 1
}

# --- Read SSM SecureStrings for this box ---------------------------------------------------------
ssm() { aws ssm get-parameter --with-decryption --name "$1" --query 'Parameter.Value' --output text; }

LK_CLIENT_SECRET="$(ssm "$LK_SSM/oauth2/client_secret")"
LK_ACCESS_KEY_ID="$(ssm "$LK_SSM/storage/access_key_id")"
LK_SECRET_ACCESS_KEY="$(ssm "$LK_SSM/storage/secret_access_key")"

# --- Source: the data-stack's SecureStrings, under the root published above -----------------------
DATA_REGION="$(ssm "$DATA_SSM/region")"
DATA_BUCKET="$(ssm "$DATA_SSM/bucket")"
DATA_NAMESPACE="$(ssm "$DATA_SSM/namespace/tpch")"

echo "==> Provisioning Lakekeeper (warehouse '$LK_WAREHOUSE', namespace '$DATA_NAMESPACE') from Glue database '$DATA_NAMESPACE' in bucket '$DATA_BUCKET'"

# Maps this environment onto lakekeeper-provision.sh's LK_SOURCE_*/LK_TARGET_* contract — the
# operator sets none of these by hand. LK_SOURCE_KIND defaults to 'glue'. The target namespace is
# the SAME tpch namespace the source database and bench/run.sh's NAMESPACE default already use, so
# the existing TPC-H query set resolves unchanged under either catalog.
export LK_SOURCE_REGION="$DATA_REGION"
export LK_SOURCE_DATABASE="$DATA_NAMESPACE"
export LK_TARGET_CATALOG_URI="$LK_CATALOG_URI"
export LK_TARGET_TOKEN_URI="$LK_TOKEN_URI"
export LK_TARGET_CLIENT_ID="$LK_CLIENT_ID"
export LK_TARGET_CLIENT_SECRET="$LK_CLIENT_SECRET"
export LK_TARGET_WAREHOUSE="$LK_WAREHOUSE"
export LK_TARGET_NAMESPACE="$DATA_NAMESPACE"
export LK_TARGET_REGION="$DATA_REGION"
export LK_TARGET_ACCESS_KEY_ID="$LK_ACCESS_KEY_ID"
export LK_TARGET_SECRET_ACCESS_KEY="$LK_SECRET_ACCESS_KEY"

"$HERE/lakekeeper-provision.sh"

cat <<EOF

Lakekeeper '$ENV' is up and provisioned:
  Catalog URI (public): $LK_CATALOG_URI
  Token URI (public):   $LK_TOKEN_URI
  Warehouse:            $LK_WAREHOUSE
  Namespace:            $DATA_NAMESPACE

Next: deploy/scripts/secrets.sh $ENV adds the private-IP Lakekeeper block to bench/.env.
Then: BENCH_CATALOG=lakekeeper make bench

REMEMBER: this box costs real money while running (1x t3.large appliance host).
  ./lakekeeper-down.sh $ENV      # destroy it IMMEDIATELY after the benchmark run or demo
EOF
