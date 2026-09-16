#!/usr/bin/env bash
# Round 2. Round 1 already established that the subject claim alone decides the
# principal (option-a.txt A.5) and carried the mapping as a work item. It did
# not say where the mapping COMES FROM, or what it costs. That is what this
# measures: the adapter knows the EXASOL user name (ctx.current_user()) and must
# mint a token whose `sub` is the IdP subject.
#
# Two candidate sources measured: the catalog, and the IdP.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"
source "$SPIKE_DIR/.spike-state"

TOKEN="$(engine_token)"
kc_admin() {
  curl -sS -X POST "$KC/realms/master/protocol/openid-connect/token" \
    -d grant_type=password -d client_id=admin-cli \
    -d username=admin -d password=admin | jq -r '.access_token'
}
ADM="$(kc_admin)"

say "S.1  Source A — ask the CATALOG (GET /management/v1/user?name=)"
note "engine service account (catalog admin): $(req_code "$TOKEN" GET "$MGMT/user?name=alice")"
curl -sS -H "Authorization: Bearer $TOKEN" "$MGMT/user?name=al" | jq -c '[.users[]|{id,name}]' | sed 's/^/     /'
cat <<'TXT'
   Three defects, all measured:
     * `name` is Lakekeeper's DISPLAY name (the token's `name` claim), not the
       login username.
     * the filter is a case-insensitive SUBSTRING search over that display name,
       so one query can return several users ("al" returns two above) and there
       is no exact-match mode. Any collision resolves to the wrong subject —
       and minting a token for the wrong subject is a silent authorization bug,
       not an error.
     * it needs catalog-admin privilege. A non-admin principal gets 403.
TXT
say "S.1b  Demonstrate both claims with a user who has never used the catalog"
# A realistic customer user: login name `carol`, display name "Carol Clark".
curl -sS -X POST -H "Authorization: Bearer $ADM" -H 'Content-Type: application/json' \
  "$KC/admin/realms/$REALM/users" --data '{
    "username":"carol","enabled":true,"emailVerified":true,
    "firstName":"Carol","lastName":"Clark","email":"carol@example.com",
    "credentials":[{"type":"password","value":"carol","temporary":false}]}' >/dev/null 2>&1 || true
CAROL_SUB=$(curl -sS -H "Authorization: Bearer $ADM" \
  "$KC/admin/realms/$REALM/users?username=carol&exact=true" | jq -r '.[0].id')
note "carol's IdP subject: $CAROL_SUB"
K=$(user_token carol carol)
note "whoami before she has ever used the catalog -> $(curl -sS -H "Authorization: Bearer $K" "$MGMT/whoami" | jq -r '.error.message // .id')"
note "listTables as carol                         -> HTTP $(req_code "$K" GET "$PREFIX/namespaces/$NAMESPACE/tables")"
note "catalog search ?name=carol                  -> $(curl -sS -H "Authorization: Bearer $TOKEN" "$MGMT/user?name=carol" | jq -c '[.users[]|{id,name}]')"
note "non-admin principal listing users           -> HTTP $(req_code "$K" GET "$MGMT/user?name=alice")"
note "now she self-registers (what the UI does on first login):"
curl -sS -X POST -H "Authorization: Bearer $K" -H 'Content-Type: application/json' \
  "$MGMT/user" --data '{"update-if-exists":true}' | jq -c '{id,name}' | sed 's/^/     /'
note "catalog search ?name=carol                  -> $(curl -sS -H "Authorization: Bearer $TOKEN" "$MGMT/user?name=carol" | jq -c '[.users[]|{id,name}]')"
note "The stored name is the DISPLAY name (\"Carol Clark\"), not the login name."
note "The search is a case-insensitive SUBSTRING match, so \"carol\" happens to hit"
note "it here — a coincidence of this particular name, not a lookup key."
note "Verdict: the catalog is not a reliable mapping source: fuzzy and ambiguous,"
note "admin-only, and blind to any user who has not logged into the catalog once."

say "S.2  Source B — ask the IdP (the engine's EXISTING service account)"
# One-time realm configuration, not a per-user step: give the client the engine
# already owns the built-in `view-users` role.
CID=$(curl -sS -H "Authorization: Bearer $ADM" "$KC/admin/realms/$REALM/clients?clientId=$ENGINE_CLIENT_ID" | jq -r '.[0].id')
SAU=$(curl -sS -H "Authorization: Bearer $ADM" "$KC/admin/realms/$REALM/clients/$CID/service-account-user" | jq -r '.id')
RM=$(curl -sS -H "Authorization: Bearer $ADM" "$KC/admin/realms/$REALM/clients?clientId=realm-management" | jq -r '.[0].id')
VU=$(curl -sS -H "Authorization: Bearer $ADM" "$KC/admin/realms/$REALM/clients/$RM/roles/view-users" | jq -c '{id,name}')
curl -sS -X POST -H "Authorization: Bearer $ADM" -H 'Content-Type: application/json' \
  "$KC/admin/realms/$REALM/users/$SAU/role-mappings/clients/$RM" --data "[$VU]" >/dev/null
note "granted realm-management:view-users to the engine's service account (ONE one-time step)"

T=$(engine_token)
say "S.3  Resolve an Exasol user name to the IdP subject"
for u in ALICE Alice alice BOB svc_etl; do
  printf '   current_user()=%-8s -> %s\n' "$u" \
    "$(curl -sS -H "Authorization: Bearer $T" "$KC/admin/realms/$REALM/users?username=$u&exact=true" \
       | jq -c '[.[]|{id,username}]')"
done
cat <<'TXT'
   Exasol folds unquoted identifiers to upper case, and Keycloak's username
   lookup is case-insensitive, so ALICE resolves to alice with no normalisation
   step. exact=true gives a single deterministic hit or none.
   A user with no IdP account resolves to [] — the adapter must fail closed with
   "no catalog identity for Exasol user SVC_ETL", never mint a token and let the
   catalog answer 404 "table does not exist".
TXT

say "S.4  Onboarding cost"
cat <<'TXT'
   One-time, per deployment:  1 step  (grant the engine's existing service
                              account the built-in view-users role).
   Per user:                  0 steps — provided the Exasol user name equals the
                              IdP user name.
   Where they differ, the deployment needs an explicit mapping (an Exasol table,
   or an IdP user attribute holding the Exasol name), and that IS a per-user
   step. The spike cannot decide this for a customer: it is the one number that
   depends on how they provision Exasol accounts today.
   Portability: `GET /admin/realms/{r}/users?username=&exact=true` is the
   Keycloak admin API. Every IdP has an equivalent (Entra ID Graph
   /users?$filter=userPrincipalName, Okta /api/v1/users?search=,
   Auth0 /api/v2/users-by-email), but the call is per-IdP — a small adapter
   surface, not a portable standard.
TXT
