#!/usr/bin/env bash
# Round 2, candidate 2c: instead of teaching the catalog that engine~alice is
# oidc~alice, INVERT the provider roles — make the engine's issuer the primary
# provider (which always gets the reserved idp-id `oidc`) and demote the
# customer's IdP to a named secondary provider.
#
# Then engine-minted tokens land on oidc~<sub> and match the existing grants.
# The question is what that costs the customer's own clients.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"
source "$SPIKE_DIR/.spike-state"

WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"; docker rm -f lk-inverted >/dev/null 2>&1 || true' EXIT
KC_ISSUER="$KC_URI"
PROBE="http://localhost:38184"

say "2c.1  Why the ids fall out this way (source)"
cat <<'TXT'
   config.rs:175   a multi-provider entry may NOT be named `oidc` — the id is
                   reserved ("Invalid OIDC provider '<id>': IdP ID 'oidc' is
                   reserved"), as is `kubernetes`.
   So `oidc` is available to exactly one issuer: whichever is configured as the
   single/primary LAKEKEEPER__OPENID_PROVIDER_URI. 2c takes it for the engine.
TXT

say "2c.2  Throwaway Lakekeeper with the roles inverted"
docker rm -f lk-inverted >/dev/null 2>&1 || true
docker run -d --name lk-inverted --network spike-multiuser-token-minting -p 38184:8181 \
  -e LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure! \
    -e LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
  -e LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
  -e LAKEKEEPER__AUTHZ_BACKEND=openfga \
  -e LAKEKEEPER__OPENFGA__ENDPOINT=http://openfga:8081 \
  -e "LAKEKEEPER__OPENID_PROVIDER_URI=$ENGINE_ISSUER" \
  -e LAKEKEEPER__OPENID_AUDIENCE=lakekeeper \
  -e "LAKEKEEPER__OPENID_PROVIDERS__KEYCLOAK__URI=$KC_ISSUER" \
  -e LAKEKEEPER__OPENID_PROVIDERS__KEYCLOAK__AUDIENCE=lakekeeper \
  -e RUST_LOG=info quay.io/lakekeeper/catalog:v0.13.1 serve >/dev/null
for _ in $(seq 40); do curl -sf "$PROBE/health" >/dev/null 2>&1 && break; sleep 1; done
note "health -> $(curl -so /dev/null -w '%{http_code}' "$PROBE/health")"
P2="$PROBE/catalog/v1/$WH_ID"

say "2c.3  Engine-minted token now lands on oidc~<sub>"
T_A=$(engine_mint "$ALICE_SUB")
note "whoami -> $(curl -sS -H "Authorization: Bearer $T_A" "$PROBE/management/v1/whoami" | jq -c '{id,name}')"
note "alice: alice_table -> HTTP $(req_code "$T_A" GET "$P2/namespaces/$NAMESPACE/tables/alice_table")   (expect 200)"
note "alice: bob_table   -> HTTP $(req_code "$T_A" GET "$P2/namespaces/$NAMESPACE/tables/bob_table")   (expect 404)"

say "2c.4  The cost: the customer's OWN clients move to keycloak~<sub>"
T_KC=$(user_token_net alice alice)
note "whoami -> $(curl -sS -H "Authorization: Bearer $T_KC" "$PROBE/management/v1/whoami" | jq -c '{id,name} // .error.message')"
note "genuine Keycloak token, alice_table -> HTTP $(req_code "$T_KC" GET "$P2/namespaces/$NAMESPACE/tables/alice_table")"
cat <<'TXT'
   2c does not remove barrier 2 — it MOVES it onto the customer. Every existing
   grant, every Spark and Trino session, and every user in the Lakekeeper
   database is keyed on oidc~<sub>; handing that prefix to the engine renames
   the customer's entire user population to keycloak~<sub> overnight.
   It also breaks UI login: the UI redirects only to the primary provider, and
   the engine's issuer has no authorization endpoint (config.rs warns about
   exactly this when the primary is unset; here it is set to something that
   cannot serve a login).
   Verdict for 2c: rejected. It is strictly worse than 2b, which achieves the
   same identity unification without renaming anyone.
TXT
