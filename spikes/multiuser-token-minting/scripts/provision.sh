#!/usr/bin/env bash
# Provision the fixtures every option test runs against:
#   warehouse `spike` (STS-enabled, MinIO) -> namespace `analytics`
#   -> tables `alice_table` and `bob_table`
#   -> OpenFGA grants: alice sees only alice_table, bob only bob_table.
#
# Users are provisioned under BOTH IdP prefixes, because Lakekeeper's user id is
# `<idp-id>~<sub>` and this spike authenticates the same human through two
# issuers: Keycloak (`oidc~`) and the engine's own issuer (`engine~`).
# Idempotent: safe to re-run against a live stack.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"

TOKEN="$(engine_token)"
[ -n "$TOKEN" ] && [ "$TOKEN" != "null" ] || { echo "FATAL: no engine token" >&2; exit 1; }

say "Bootstrap"
if [ "$(curl -sS -H "Authorization: Bearer $TOKEN" "$MGMT/info" | jq -r '.bootstrapped')" = "true" ]; then
  note "already bootstrapped"
else
  req "$TOKEN" POST "$MGMT/bootstrap" \
    --data '{"accept-terms-of-use": true, "is-operator": true}'
fi

say "Warehouse '$WAREHOUSE' (sts-enabled, s3-compat, MinIO)"
# sts-enabled=true is what Option D needs: without it Lakekeeper hands back the
# warehouse's own static key instead of a scoped session credential.
WH_BODY=$(jq -n --arg w "$WAREHOUSE" '{
  "warehouse-name": $w,
  "storage-profile": {
    "type": "s3", "bucket": "warehouse", "key-prefix": "spike",
    "region": "us-east-1", "flavor": "s3-compat",
    "endpoint": "http://minio:9000", "path-style-access": true,
    "sts-enabled": true, "sts-token-validity-seconds": 3600
  },
  "storage-credential": {
    "type": "s3", "credential-type": "access-key",
    "access-key-id": "lakekeeper", "secret-access-key": "lakekeeper-secret-key"
  },
  "delete-profile": {"type": "soft", "expiration-seconds": 3600}
}')
CODE=$(req_code "$TOKEN" POST "$MGMT/warehouse" --data "$WH_BODY")
note "create-warehouse -> HTTP $CODE"
case "$CODE" in 2*|400|409) ;; *) echo "FATAL: create-warehouse HTTP $CODE" >&2;
  req "$TOKEN" POST "$MGMT/warehouse" --data "$WH_BODY"; exit 1;; esac

WH_ID=$(curl -sS -H "Authorization: Bearer $TOKEN" "$MGMT/warehouse" \
  | jq -r --arg w "$WAREHOUSE" '.warehouses[] | select(.name==$w) | .id')
[ -n "$WH_ID" ] || { echo "FATAL: warehouse '$WAREHOUSE' not found" >&2; exit 1; }
note "warehouse-id $WH_ID"

PREFIX="$CATALOG/v1/$WH_ID"

say "Namespace '$NAMESPACE'"
CODE=$(req_code "$TOKEN" POST "$PREFIX/namespaces" \
  --data "$(jq -n --arg n "$NAMESPACE" '{namespace: [$n], properties: {}}')")
note "create-namespace -> HTTP $CODE"

say "Tables"
# A minimal Iceberg schema is enough: the point is that each table writes a real
# metadata.json under its own S3 prefix, which Option D then tries to read
# directly from MinIO with another user's vended credentials.
create_table() {
  local name="$1" body
  body=$(jq -n --arg n "$name" '{
    name: $n,
    "stage-create": false,
    schema: {
      type: "struct", "schema-id": 0, "identifier-field-ids": [],
      fields: [
        {id: 1, name: "id",    required: true,  type: "long"},
        {id: 2, name: "owner", required: false, type: "string"}
      ]
    }
  }')
  local code
  code=$(req_code "$TOKEN" POST "$PREFIX/namespaces/$NAMESPACE/tables" --data "$body")
  note "create-table $name -> HTTP $code"
  [ "$code" = "409" ] || [ "${code:0:1}" = "2" ] || {
    req "$TOKEN" POST "$PREFIX/namespaces/$NAMESPACE/tables" --data "$body"; exit 1; }
}
create_table alice_table
create_table bob_table

say "Users (both IdP prefixes)"
provision_user() {
  local id="$1" name="$2" code
  code=$(req_code "$TOKEN" POST "$MGMT/user" --data "$(jq -n --arg i "$id" --arg n "$name" \
    '{id: $i, name: $n, "user-type": "human", "update-if-exists": true}')")
  note "user $id -> HTTP $code"
}
provision_user "oidc~$ALICE_SUB"   "alice (via Keycloak)"
provision_user "oidc~$BOB_SUB"     "bob (via Keycloak)"
provision_user "engine~$ALICE_SUB" "alice (via engine issuer)"
provision_user "engine~$BOB_SUB"   "bob (via engine issuer)"

say "Grants (OpenFGA, via Lakekeeper management API)"
# Deliberately NON-overlapping and table-scoped. Nothing is granted at warehouse
# or namespace level, so anything a user can see is a direct table grant.
table_id() {
  curl -sS -H "Authorization: Bearer $TOKEN" \
    "$PREFIX/namespaces/$NAMESPACE/tables/$1" -H 'Content-Type: application/json' \
    | jq -r '.metadata."table-uuid"'
}
ALICE_TBL=$(table_id alice_table)
BOB_TBL=$(table_id bob_table)
note "alice_table $ALICE_TBL"
note "bob_table   $BOB_TBL"

grant() {
  local tbl="$1" user="$2" code
  code=$(req_code "$TOKEN" POST "$MGMT/permissions/warehouse/$WH_ID/table/$tbl/assignments" \
    --data "$(jq -n --arg u "$user" '{writes: [{type: "select", user: $u}], deletes: []}')")
  note "grant select on $tbl to $user -> HTTP $code"
  [ "${code:0:1}" = "2" ] || {
    req "$TOKEN" POST "$MGMT/permissions/warehouse/$WH_ID/table/$tbl/assignments" \
      --data "$(jq -n --arg u "$user" '{writes: [{type: "select", user: $u}], deletes: []}')"; exit 1; }
}
# The Keycloak-prefixed identities are the grants a real deployment would make
# (a human granting access in the Lakekeeper UI, logged in via Keycloak).
grant "$ALICE_TBL" "oidc~$ALICE_SUB"
grant "$BOB_TBL"   "oidc~$BOB_SUB"

# Persist the ids the option scripts need, so each can run standalone.
cat > "$SPIKE_DIR/.spike-state" <<STATE
WH_ID=$WH_ID
PREFIX=$PREFIX
ALICE_TBL=$ALICE_TBL
BOB_TBL=$BOB_TBL
STATE
say "Provisioned"
note "state written to $SPIKE_DIR/.spike-state"
