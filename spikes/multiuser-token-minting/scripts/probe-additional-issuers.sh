#!/usr/bin/env bash
# Can the engine be added as an ADDITIONAL ISSUER of the existing "oidc"
# provider, instead of as its own provider? If it could, engine-minted tokens
# would resolve to `oidc~<sub>` and Option A's split identity namespace would
# disappear, so it is worth ruling in or out directly.
#
# Runs a throwaway second Lakekeeper against the SAME database, configured with
# one provider only, so the main stack is untouched.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"
source "$SPIKE_DIR/.spike-state"

NET="spike-multiuser-token-minting"
NAME="spike-lk-additional-issuers"
PORT=38182
ALT="http://localhost:$PORT"

cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

say "Throwaway Lakekeeper: ONE provider, engine listed only in ADDITIONAL_ISSUERS"
docker run -d --name "$NAME" --network "$NET" -p "$PORT:8181" \
  -e LAKEKEEPER__PG_ENCRYPTION_KEY='This-is-NOT-Secure!' \
  -e LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
  -e LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
  -e LAKEKEEPER__AUTHZ_BACKEND=openfga \
  -e LAKEKEEPER__OPENFGA__ENDPOINT=http://openfga:8081 \
  -e LAKEKEEPER__OPENID_PROVIDER_URI=http://keycloak:8080/realms/iceberg \
  -e "LAKEKEEPER__OPENID_ADDITIONAL_ISSUERS=http://localhost:$SPK_KEYCLOAK_PORT/realms/iceberg,$ENGINE_ISSUER" \
  -e LAKEKEEPER__OPENID_AUDIENCE=lakekeeper \
  -e LAKEKEEPER__OPENID_SUBJECT_CLAIM=sub \
  -e RUST_LOG=info,lakekeeper=debug \
  "${SPK_LAKEKEEPER_IMAGE:-quay.io/lakekeeper/catalog:v0.13.1}" serve >/dev/null

for _ in $(seq 1 30); do curl -sf "$ALT/health" >/dev/null 2>&1 && break; sleep 1; done
docker logs "$NAME" 2>&1 \
  | grep -oE '"message":"(Configuring [0-9]+ OIDC provider\(s\)|Setting additional issuers for [a-z]+: [^"]*|Successfully added OIDC authenticator: [a-z]+)"' \
  | sed 's/^/   /' | sort -u

ALT_PREFIX="$ALT/catalog/v1/$WH_ID"
say "Engine-minted token, iss=$ENGINE_ISSUER (a trusted issuer, unknown key)"
T="$(engine_mint "$ALICE_SUB")"
note "whoami      -> $(curl -sS -H "Authorization: Bearer $T" "$ALT/management/v1/whoami" | jq -c '.id // .error.message')"
note "alice_table -> HTTP $(req_code "$T" GET "$ALT_PREFIX/namespaces/$NAMESPACE/tables/alice_table")"

say "Control: genuine Keycloak token against the same server"
K="$(user_token alice alice)"
note "whoami      -> $(curl -sS -H "Authorization: Bearer $K" "$ALT/management/v1/whoami" | jq -c '.id // .error.message')"
note "alice_table -> HTTP $(req_code "$K" GET "$ALT_PREFIX/namespaces/$NAMESPACE/tables/alice_table")"

say "Reading"
note "ADDITIONAL_ISSUERS widens the accepted iss values of one authenticator; it"
note "does not add a key. build_oidc_authenticator fetches the JWKS once from"
note "provider.uri (JWKSWebAuthenticator::new) and then calls"
note "add_additional_issuers(...) on that same authenticator, so a token signed"
note "by the engine has no matching key and fails signature validation."
note "A dedicated provider entry is therefore the only way in, and that is what"
note "puts engine principals under the engine~ prefix."
