#!/usr/bin/env bash
# Round 2, candidate 1d: don't host a JWKS at all — put the engine's PUBLIC key
# inside the JWKS the customer's IdP already publishes, as a verify-only
# (PASSIVE) realm key. Keycloak then advertises it; the engine signs tokens with
# `iss` = the customer's own issuer.
#
# This removes BOTH barriers at once:
#   barrier 1  no engine-hosted endpoint exists at all
#   barrier 2  iss and idp-id are the customer's, so the principal is oidc~<sub>
#              and every existing grant applies untouched
#
# Verified against a STOCK single-provider Lakekeeper with ZERO configuration
# change (a throwaway instance on the same database, no ADDITIONAL_ISSUERS, no
# second provider).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"
source "$SPIKE_DIR/.spike-state"

KEYS="$SPIKE_DIR/keys"
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"; docker rm -f lk-stock >/dev/null 2>&1 || true' EXIT
KC_ISSUER="$KC_URI"
PROBE="http://localhost:38183"

kc_admin() {
  curl -sS -X POST "$KC/realms/master/protocol/openid-connect/token" \
    -d grant_type=password -d client_id=admin-cli \
    -d username=admin -d password=admin | jq -r '.access_token'
}
ADM="$(kc_admin)"

say "1d.1  Register the engine's public key as a PASSIVE realm key"
# GOTCHA: parentId must be the realm's INTERNAL id. With the realm NAME the
# create returns 201 and then silently does not persist.
REALM_ID=$(curl -sS -H "Authorization: Bearer $ADM" "$KC/admin/realms/$REALM" | jq -r '.id')
note "realm internal id $REALM_ID"

# Keycloak's rsa provider wants a PKCS#8 private key plus a matching certificate.
# active=false => the key is PUBLISHED in the realm JWKS but NEVER used to sign
# anything Keycloak itself issues.
openssl pkcs8 -topk8 -nocrypt -in "$KEYS/engine-signing-key.pem" -out "$WORK/pkcs8.pem"
openssl req -new -x509 -key "$KEYS/engine-signing-key.pem" -days 3650 \
  -subj "/CN=lakehouse-engine" -out "$WORK/cert.pem" 2>/dev/null
PK=$(sed '1d;$d' "$WORK/pkcs8.pem" | tr -d '\n')
CERT=$(sed '1d;$d' "$WORK/cert.pem" | tr -d '\n')

# Idempotent: drop any previous copy first.
OLD=$(curl -sS -H "Authorization: Bearer $ADM" \
  "$KC/admin/realms/$REALM/components?parent=$REALM_ID&type=org.keycloak.keys.KeyProvider" \
  | jq -r '.[] | select(.name=="lakehouse-engine-verify-only") | .id')
[ -n "$OLD" ] && curl -sS -X DELETE -H "Authorization: Bearer $ADM" \
  "$KC/admin/realms/$REALM/components/$OLD" >/dev/null

BODY=$(jq -n --arg pid "$REALM_ID" --arg pk "$PK" --arg cert "$CERT" '{
  name: "lakehouse-engine-verify-only", providerId: "rsa",
  providerType: "org.keycloak.keys.KeyProvider", parentId: $pid,
  config: {priority:["0"], enabled:["true"], active:["false"],
           algorithm:["RS256"], keyUse:["sig"],
           privateKey:[$pk], certificate:[$cert]}}')
CODE=$(curl -sS -o /dev/null -w '%{http_code}' -X POST \
  -H "Authorization: Bearer $ADM" -H 'Content-Type: application/json' \
  "$KC/admin/realms/$REALM/components" --data "$BODY")
note "create key provider -> HTTP $CODE"

say "1d.2  The customer's OWN JWKS now carries the engine key"
curl -sS -H "Authorization: Bearer $ADM" "$KC/admin/realms/$REALM/keys" \
  | jq -c '.keys[] | select(.providerPriority==0) | {kid, algorithm, status, providerId}'
KID=$(curl -sS -H "Authorization: Bearer $ADM" "$KC/admin/realms/$REALM/keys" \
  | jq -r '.keys[] | select(.status=="PASSIVE" and .algorithm=="RS256") | .kid' | head -1)
note "engine kid = $KID   (status PASSIVE: published for verification, never used for signing)"
note "present in the public JWKS: $(curl -sS "$KC/realms/$REALM/protocol/openid-connect/certs" | jq --arg k "$KID" '[.keys[]|select(.kid==$k)]|length') entry"

say "1d.3  STOCK Lakekeeper — single provider, the customer's issuer, no extra config"
docker rm -f lk-stock >/dev/null 2>&1 || true
docker run -d --name lk-stock --network spike-multiuser-token-minting -p 38183:8181 \
  -e LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure! \
    -e LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
  -e LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
  -e LAKEKEEPER__AUTHZ_BACKEND=openfga \
  -e LAKEKEEPER__OPENFGA__ENDPOINT=http://openfga:8081 \
  -e "LAKEKEEPER__OPENID_PROVIDER_URI=$KC_ISSUER" \
  -e LAKEKEEPER__OPENID_AUDIENCE=lakekeeper \
  -e RUST_LOG=info quay.io/lakekeeper/catalog:v0.13.1 serve >/dev/null
for _ in $(seq 40); do curl -sf "$PROBE/health" >/dev/null 2>&1 && break; sleep 1; done
note "stock catalog health -> $(curl -so /dev/null -w '%{http_code}' "$PROBE/health")"
P2="$PROBE/catalog/v1/$WH_ID"

say "1d.4  Engine mints as the customer's issuer, signed with the registered key"
T_A=$(engine_mint "$ALICE_SUB" --iss "$KC_ISSUER" --kid "$KID")
decode_jwt "$T_A"
note "alice: alice_table -> HTTP $(req_code "$T_A" GET "$P2/namespaces/$NAMESPACE/tables/alice_table")   (expect 200)"
note "alice: bob_table   -> HTTP $(req_code "$T_A" GET "$P2/namespaces/$NAMESPACE/tables/bob_table")   (expect 404)"
T_B=$(engine_mint "$BOB_SUB" --iss "$KC_ISSUER" --kid "$KID")
note "bob:   alice_table -> HTTP $(req_code "$T_B" GET "$P2/namespaces/$NAMESPACE/tables/alice_table")   (expect 404)"
note "bob:   bob_table   -> HTTP $(req_code "$T_B" GET "$P2/namespaces/$NAMESPACE/tables/bob_table")   (expect 200)"
note "whoami -> $(curl -sS -H "Authorization: Bearer $T_A" "$PROBE/management/v1/whoami" | jq -c '{id,name}')"

say "1d.5  Negative: a key the IdP does not publish"
T_BAD=$(engine_mint "$ALICE_SUB" --iss "$KC_ISSUER" --kid "not-a-registered-kid")
note "unregistered kid -> HTTP $(req_code "$T_BAD" GET "$P2/namespaces/$NAMESPACE/tables/alice_table")   (expect 401)"

say "1d.6  Does the IdP offer a SUPPORTED grant that would avoid holding the key?"
note "jwt-bearer grant:"
curl -sS -X POST "$KC_TOKEN_URL" \
  -d grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer \
  -d "client_id=$ENGINE_CLIENT_ID" -d "client_secret=$ENGINE_CLIENT_SECRET" \
  -d "assertion=$(engine_mint "$ALICE_SUB" --iss "$KC_ISSUER" --kid "$KID")" \
  | jq -c '{error, error_description}' | sed 's/^/     /'
note "standard token exchange + requested_subject:"
curl -sS -X POST "$KC_TOKEN_URL" \
  -d grant_type=urn:ietf:params:oauth:grant-type:token-exchange \
  -d "client_id=$ENGINE_CLIENT_ID" -d "client_secret=$ENGINE_CLIENT_SECRET" \
  -d "subject_token=$(engine_token)" \
  -d subject_token_type=urn:ietf:params:oauth:token-type:access_token \
  -d "requested_subject=$ALICE_SUB" \
  | jq -c '{error, error_description}' | sed 's/^/     /'

cat <<'TXT'

   Verdict for 1d: it WORKS, and it is the only candidate that removes both
   barriers with zero Lakekeeper configuration change and zero grant change.
   Its two costs are not small:
     * the customer's IdP holds a copy of the engine's PRIVATE key. Keycloak's
       key-provider API has no public-key-only import; `rsa` requires a private
       key + certificate. Anyone who can read the realm's key material can mint
       tokens for any user of this engine.
     * importing a key is a self-hosted-IdP capability. Auth0, Entra ID and
       Cognito do not let a tenant add a signing key to the published JWKS, so
       this design pins the customer to a self-hosted IdP.
   That second cost is exactly what success criterion 4 rules out.
TXT
