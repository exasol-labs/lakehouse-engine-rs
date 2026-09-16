#!/usr/bin/env bash
# Option B-control support: wire up the FGAP v1 policies that legacy token
# exchange needs for `requested_subject` impersonation, on a given Keycloak.
#
#   kc-fgap-v1-impersonation.sh <keycloak-base-url>
#
# This is the configuration cost of the control case, written out in full:
# three admin-API steps and a policy object, none of which exist in V2.
set -euo pipefail
BASE="${1:?usage: kc-fgap-v1-impersonation.sh <keycloak-base-url>}"
REALM=iceberg
REQUESTER=lakehouse   # client that calls the exchange
TARGET=lakehouse      # client the exchanged token is issued to

AT="$(curl -sS -X POST "$BASE/realms/master/protocol/openid-connect/token" \
  -d grant_type=password -d client_id=admin-cli -d username=admin -d password=admin \
  | jq -r .access_token)"
api() { local m="$1" p="$2"; shift 2; curl -sS -X "$m" "$BASE/admin/realms/$REALM$p" \
  -H "Authorization: Bearer $AT" -H 'Content-Type: application/json' "$@"; }

cid() { api GET "/clients?clientId=$1" | jq -r '.[0].id'; }
RM_ID="$(cid realm-management)"
REQ_ID="$(cid "$REQUESTER")"
TGT_ID="$(cid "$TARGET")"
echo "   realm-management=$RM_ID requester=$REQ_ID target=$TGT_ID"

echo "== 1. enable fine-grained permissions on users (gives the 'impersonate' permission)"
api PUT "/users-management-permissions" --data '{"enabled":true}' | jq -c '{enabled, scopePermissions}'

echo "== 2. enable fine-grained permissions on the target client (gives 'token-exchange')"
api PUT "/clients/$TGT_ID/management/permissions" --data '{"enabled":true}' | jq -c '{enabled, scopePermissions}'

echo "== 3. client policy matching the requesting client"
POLICY_ID="$(api POST "/clients/$RM_ID/authz/resource-server/policy/client" \
  --data "$(jq -n --arg c "$REQ_ID" '{name:"allow-lakehouse-exchange",logic:"POSITIVE",clients:[$c]}')" \
  | jq -r '.id // empty')"
if [ -z "$POLICY_ID" ]; then
  POLICY_ID="$(api GET "/clients/$RM_ID/authz/resource-server/policy?name=allow-lakehouse-exchange" | jq -r '.[0].id')"
fi
echo "   policy id $POLICY_ID"

echo "== 4. attach the policy to the token-exchange and impersonate permissions"
attach() {
  local perm_id="$1" name="$2"
  local perm; perm="$(api GET "/clients/$RM_ID/authz/resource-server/permission/scope/$perm_id")"
  api PUT "/clients/$RM_ID/authz/resource-server/permission/scope/$perm_id" \
    --data "$(jq --arg p "$POLICY_ID" '. + {policies: [$p], decisionStrategy: "AFFIRMATIVE"}' <<<"$perm")" \
    >/dev/null
  echo "   attached to $name ($perm_id)"
}
TE_ID="$(api GET "/clients/$TGT_ID/management/permissions" | jq -r '.scopePermissions["token-exchange"]')"
IMP_ID="$(api GET "/users-management-permissions" | jq -r '.scopePermissions.impersonate')"
attach "$TE_ID"  "client token-exchange"
attach "$IMP_ID" "users impersonate"
