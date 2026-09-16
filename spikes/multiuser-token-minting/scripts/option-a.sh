#!/usr/bin/env bash
# OPTION A — engine as a trusted issuer, no exchange.
#
# The adapter signs its own JWT asserting the querying user; Lakekeeper trusts
# the engine's JWKS directly. No IdP call in the query path.
#
# Answers, with live evidence:
#   * does v0.13.1 take a second trusted issuer alongside OPENID_PROVIDER_URI?
#   * how does it fetch the key?
#   * which claims are mandatory, and which audience?
#   * does the subject match what the OpenFGA grants reference?
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"
source "$SPIKE_DIR/.spike-state"

ADMIN="$(engine_token)"
TBL_URL="$PREFIX/namespaces/$NAMESPACE/tables"

say "A.0  Issuer registration — how Lakekeeper reached the engine's key"
note "config: LAKEKEEPER__OPENID_PROVIDERS__ENGINE__URI=$ENGINE_ISSUER (+ __AUDIENCE, __SUBJECT_CLAIMS)"
"${COMPOSE[@]}" logs lakekeeper 2>&1 \
  | grep -oE '"message":"(Configuring [0-9]+ OIDC provider\(s\)|Creating OIDC authenticator for [a-z]+ \([^"]*\)|Successfully added OIDC authenticator: [a-z]+)"' \
  | sed 's/^/   /' | sort -u
note "discovery document fetched from the issuer (this is the ONLY key channel):"
curl -sS "http://localhost:$SPK_ISSUER_PORT/.well-known/openid-configuration" | jq -c '{issuer, jwks_uri}' | sed 's/^/   /'
curl -sS "http://localhost:$SPK_ISSUER_PORT/jwks.json" | jq -c '.keys[0] | {kty, alg, kid}' | sed 's/^/   /'

say "A.1  Identity asserted by an engine-minted token"
T_ALICE="$(engine_mint "$ALICE_SUB")"
decode_jwt "$T_ALICE" | sed 's/^/   /'
note "GET $MGMT/whoami:"
req "$T_ALICE" GET "$MGMT/whoami" | sed 's/^/   /'

say "A.2  Grants made against the Keycloak identity do NOT apply"
note "provision.sh granted select on alice_table to oidc~$ALICE_SUB"
note "alice_table -> HTTP $(req_code "$T_ALICE" GET "$TBL_URL/alice_table")   (expected 404: identity is engine~, grant is oidc~)"
note "bob_table   -> HTTP $(req_code "$T_ALICE" GET "$TBL_URL/bob_table")"

say "A.3  Grant the engine-prefixed identity, then re-run"
for pair in "$ALICE_TBL:engine~$ALICE_SUB" "$BOB_TBL:engine~$BOB_SUB"; do
  tbl="${pair%%:*}"; usr="${pair#*:}"
  code=$(req_code "$ADMIN" POST "$MGMT/permissions/warehouse/$WH_ID/table/$tbl/assignments" \
    --data "$(jq -n --arg u "$usr" '{writes:[{type:"select",user:$u}],deletes:[]}')")
  note "grant select on $tbl to $usr -> HTTP $code"
done

T_BOB="$(engine_mint "$BOB_SUB")"
for who in alice bob; do
  case "$who" in alice) T="$T_ALICE";; bob) T="$T_BOB";; esac
  note "$who: alice_table -> HTTP $(req_code "$T" GET "$TBL_URL/alice_table") | bob_table -> HTTP $(req_code "$T" GET "$TBL_URL/bob_table") | listTables -> $(curl -sS -H "Authorization: Bearer $T" "$TBL_URL" | jq -c '.identifiers')"
done

say "A.4  Which claims are mandatory, and which audience"
probe() {
  local label="$1"; shift
  local t code body
  t="$("$MINT" --sub "$ALICE_SUB" "$@")"
  body="$(curl -sS -o - -w '\n%{http_code}' -X GET "$TBL_URL/alice_table" -H "Authorization: Bearer $t")"
  code="$(tail -1 <<<"$body")"
  local msg
  msg="$(sed '$d' <<<"$body" | jq -r '.error.message // empty' 2>/dev/null)"
  printf '   %-34s HTTP %s  %s\n' "$label" "$code" "$msg"
}
probe "baseline (all claims)"
probe "no aud"                 --omit aud
probe "aud=wrong"              --aud wrong-audience
probe "aud=[lakekeeper,other]" --claim 'aud=["lakekeeper","other"]'
probe "no azp"                 --omit azp
probe "no scope"               --omit scope
probe "scope=openid (no catalog)" --scope openid
probe "no exp"                 --omit exp
probe "expired 30s"            --ttl -30
probe "expired 61s"            --ttl -61
probe "expired 1h"             --ttl -3600
probe "no iat/nbf"             --omit iat --omit nbf
probe "no sub"                 --omit sub
probe "iss=keycloak's issuer"  --iss "http://keycloak:8080/realms/$REALM"
probe "unknown kid"            --kid no-such-key
probe "no preferred_username"  --omit preferred_username
probe "no typ"                 --omit typ
probe "no jti"                 --omit jti

# Signature is genuinely verified, not merely parsed.
FORGED="$(mktemp -u).pem"; openssl genrsa -out "$FORGED" 2048 2>/dev/null
probe "forged (other key, same kid)" --key "$FORGED"
rm -f "$FORGED"
UNSIGNED_H="$(printf '%s' '{"alg":"none","typ":"JWT"}' | base64 -w0 | tr '+/' '-_' | tr -d '=')"
UNSIGNED_P="$(jq -nc --arg s "$ALICE_SUB" --arg i "$ENGINE_ISSUER" \
  '{iss:$i,sub:$s,aud:"lakekeeper",exp:9999999999}' | base64 -w0 | tr '+/' '-_' | tr -d '=')"
printf '   %-34s HTTP %s\n' "alg=none (unsigned)" \
  "$(req_code "$UNSIGNED_H.$UNSIGNED_P." GET "$TBL_URL/alice_table")"

say "A.5  Subject-claim override — can the engine assert a non-sub claim?"
note "LAKEKEEPER__OPENID_PROVIDERS__ENGINE__SUBJECT_CLAIMS=sub is set in the compose file."
note "whoami with sub=alice but preferred_username=bob:"
curl -sS -H "Authorization: Bearer $("$MINT" --sub "$ALICE_SUB" --claim 'preferred_username=bob')" \
  "$MGMT/whoami" | jq -r '"   resolved user id: " + .id'
note "-> the subject claim decides; other claims are inert."

say "A.6  Query-path independence — stop Keycloak entirely, then query as alice"
# If Option A really removes the IdP from the query path, the catalog call must
# still succeed with the IdP down.
"${COMPOSE[@]}" stop keycloak >/dev/null 2>&1
note "keycloak: $("${COMPOSE[@]}" ps --format json keycloak | jq -r '.[0].State // "stopped"' 2>/dev/null || echo stopped)"
note "engine-minted alice: alice_table -> HTTP $(req_code "$(engine_mint "$ALICE_SUB")" GET "$TBL_URL/alice_table") | bob_table -> HTTP $(req_code "$(engine_mint "$ALICE_SUB")" GET "$TBL_URL/bob_table")"
note "engine service account (client_credentials against Keycloak) -> $(engine_token 2>&1 | head -c 60)"
"${COMPOSE[@]}" start keycloak >/dev/null 2>&1
"${COMPOSE[@]}" up -d --wait keycloak >/dev/null 2>&1
note "keycloak restarted"

say "A.7  Query-path cost"
T="$(engine_mint "$ALICE_SUB")"
mint_ms=$( { TIMEFORMAT=%R; time ("$MINT" --sub "$ALICE_SUB" >/dev/null) ; } 2>&1 )
note "IdP round trips per query: 0 (the token is signed in-process)"
note "local mint cost (python+RSA2048, an upper bound on an in-process Rust signer): ${mint_ms}s"
note "Lakekeeper re-fetches the engine JWKS on a 1 h refresh interval"
note "  (build_oidc_authenticator: JWKSWebAuthenticator::new(uri, Some(Duration::from_hours(1))))"

say "A.8  Mitigating the split identity namespace with a Lakekeeper role"
# Option A's one real cost is that the engine's principals live under `engine~`
# while a human granting access in the Lakekeeper UI creates `oidc~` grants. A
# role absorbs that: the two identities are assigned to one role, and every
# table grant names the role, so a grant is still made once.
ROLE_ID="$(curl -sS -X POST "$MGMT/role" -H "Authorization: Bearer $ADMIN" \
  -H 'Content-Type: application/json' \
  --data '{"name":"analyst-alice","description":"one grant, two identities"}' \
  | jq -r '.id // empty')"
[ -n "$ROLE_ID" ] || ROLE_ID="$(curl -sS -H "Authorization: Bearer $ADMIN" "$MGMT/role?name=analyst-alice" | jq -r '.roles[0].id')"
note "role analyst-alice = $ROLE_ID"
for u in "oidc~$ALICE_SUB" "engine~$ALICE_SUB"; do
  note "assign $u -> HTTP $(req_code "$ADMIN" POST "$MGMT/permissions/role/$ROLE_ID/assignments" \
    --data "$(jq -n --arg u "$u" '{writes:[{type:"assignee",user:$u}],deletes:[]}')")"
done
note "grant select on alice_table to the ROLE -> HTTP $(
  req_code "$ADMIN" POST "$MGMT/permissions/warehouse/$WH_ID/table/$ALICE_TBL/assignments" \
    --data "$(jq -n --arg r "$ROLE_ID" '{writes:[{type:"select",role:$r}],deletes:[]}')")"
note "revoke both direct user grants -> HTTP $(
  req_code "$ADMIN" POST "$MGMT/permissions/warehouse/$WH_ID/table/$ALICE_TBL/assignments" \
    --data "$(jq -n --arg a "oidc~$ALICE_SUB" --arg b "engine~$ALICE_SUB" \
      '{writes:[],deletes:[{type:"select",user:$a},{type:"select",user:$b}]}')")"
note "only the role grant remains:"
curl -sS -H "Authorization: Bearer $ADMIN" "$MGMT/permissions/warehouse/$WH_ID/table/$ALICE_TBL/assignments" \
  | jq -c '.assignments' | sed 's/^/     /'
note "keycloak alice -> HTTP $(req_code "$(user_token alice alice)" GET "$TBL_URL/alice_table")"
note "engine   alice -> HTTP $(req_code "$(engine_mint "$ALICE_SUB")" GET "$TBL_URL/alice_table")"
note "engine   bob   -> HTTP $(req_code "$(engine_mint "$BOB_SUB")" GET "$TBL_URL/alice_table")  (still denied)"
note "-> one grant, two identities. The duplication collapses to a one-off"
note "   user-to-role assignment per person instead of a second grant per table."
