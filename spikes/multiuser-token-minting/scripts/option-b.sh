#!/usr/bin/env bash
# OPTION B — RFC 8693 exchange with an engine-minted subject token.
#
# The adapter mints a short-lived JWT asserting the user and sends it as
# `subject_token` to Keycloak, which returns a user-scoped access token for
# Lakekeeper. Run against Keycloak 26.4.0 with BOTH
# `token-exchange-standard:v2` and `token-exchange-external-internal:v2` on.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"
source "$SPIKE_DIR/.spike-state"

TBL_URL="$PREFIX/namespaces/$NAMESPACE/tables"

exchange() {
  curl -sS -w '\n%{http_code}' -X POST "$KC_TOKEN_URL" \
    -d grant_type=urn:ietf:params:oauth:grant-type:token-exchange \
    -d client_id=lakehouse -d client_secret="$ENGINE_CLIENT_SECRET" "$@"
}
show() {
  local label="$1"; shift
  local r code body
  r="$(exchange "$@")"; code="$(tail -1 <<<"$r")"; body="$(sed '$d' <<<"$r")"
  printf '   %-46s HTTP %s  %s\n' "$label" "$code" \
    "$(jq -r 'if .error then "\(.error): \(.error_description)" else "issued_token_type=\(.issued_token_type)" end' <<<"$body" 2>/dev/null)"
  LAST_TOKEN="$(jq -r '.access_token // empty' <<<"$body" 2>/dev/null || true)"
}

say "B.0  Which token-exchange features this Keycloak actually has on"
ADMIN_TOKEN="$(curl -sS -X POST "$KC/realms/master/protocol/openid-connect/token" \
  -d grant_type=password -d client_id=admin-cli -d username=admin -d password=admin \
  | jq -r .access_token)"
curl -sS -H "Authorization: Bearer $ADMIN_TOKEN" "$KC/admin/serverinfo" \
  | jq -r '.features[] | select(.name|test("TOKEN_EXCHANGE")) | "   \(.name)  enabled=\(.enabled)  type=\(.type)"'
note "Keycloak image: $("${COMPOSE[@]}" images keycloak --format json | jq -r '.[0] | "\(.Repository):\(.Tag)"' 2>/dev/null || echo 'quay.io/keycloak/keycloak:26.4.0')"

say "B.1  The shape the brief proposed: engine-minted JWT as subject_token"
SUBJ="$("$MINT" --sub "$ALICE_SUB" --aud lakehouse-engine)"
note "subject token (engine-signed, asserting alice):"
decode_jwt "$SUBJ" | jq -c '.claims | {iss,sub,aud}' | sed 's/^/     /'
show "subject_token_type=...:jwt, no subject_issuer" \
  -d "subject_token=$SUBJ" -d subject_token_type=urn:ietf:params:oauth:token-type:jwt
show "subject_token_type=...:jwt, subject_issuer=engine" \
  -d "subject_token=$SUBJ" -d subject_token_type=urn:ietf:params:oauth:token-type:jwt -d subject_issuer=engine
show "subject_token_type=...:id_token, subject_issuer=engine" \
  -d "subject_token=$SUBJ" -d subject_token_type=urn:ietf:params:oauth:token-type:id_token -d subject_issuer=engine
note "V2's external path accepts ONLY subject_token_type=access_token; see"
note "OIDCIdentityProvider.exchangeExternalTokenV2Impl ->"
note "  AbstractOAuth2IdentityProvider.exchangeExternalUserInfoValidationOnly(),"
note "  which throws 'invalid token type' for anything but access_token."

say "B.2  Standard Token Exchange V2 (internal->internal) with the same token"
show "no subject_issuer, type=access_token" \
  -d "subject_token=$SUBJ" -d subject_token_type=urn:ietf:params:oauth:token-type:access_token
note "V2 internal->internal only accepts a token THIS realm issued."

say "B.3  External->internal V2, correctly shaped"
note "Requires on the IdP: tokenIntrospectionUrl AND userInfoUrl."
note "Both are served here by the engine-issuer stub (see issuer/nginx.conf)."
show "type=access_token, subject_issuer=engine" \
  -d "subject_token=$SUBJ" -d subject_token_type=urn:ietf:params:oauth:token-type:access_token \
  -d subject_issuer=engine
if [ -n "${LAST_TOKEN:-}" ]; then
  EXCHANGED="$LAST_TOKEN"
  note "returned token claims:"
  decode_jwt "$EXCHANGED" | jq -c '.claims | {iss,sub,aud,azp,scope,preferred_username}' | sed 's/^/     /'
  note "resolved Lakekeeper identity: $(curl -sS -H "Authorization: Bearer $EXCHANGED" "$MGMT/whoami" | jq -r .id)"
  note "alice_table -> HTTP $(req_code "$EXCHANGED" GET "$TBL_URL/alice_table")  bob_table -> HTTP $(req_code "$EXCHANGED" GET "$TBL_URL/bob_table")"
  note "-> unlike Option A, this lands in the oidc~ namespace, so the grants a"
  note "   human made in the Lakekeeper UI apply unchanged."
fi

say "B.4  What the exchange actually trusts"
# The point of a signed subject token is that the receiver verifies it. V2 does
# not: it forwards the opaque string to the issuer's introspection endpoint and
# builds the identity from the issuer's userinfo response.
for probe in "asserts bob:$("$MINT" --sub "$BOB_SUB" --aud lakehouse-engine)" \
             "literal garbage:not-a-jwt-at-all" \
             "empty-ish:x.y.z"; do
  label="${probe%%:*}"; tok="${probe#*:}"
  show "subject token $label" \
    -d "subject_token=$tok" -d subject_token_type=urn:ietf:params:oauth:token-type:access_token \
    -d subject_issuer=engine
  [ -n "${LAST_TOKEN:-}" ] && \
    note "  -> identity in returned token: $(decode_jwt "$LAST_TOKEN" | jq -r '.claims.preferred_username')"
done
note "Keycloak never parses or verifies the subject token on this path. The"
note "identity is whatever the engine's own introspection/userinfo endpoints say,"
note "so the engine's signature on the subject token adds nothing."

say "B.5  Query-path cost"
SUBJ="$("$MINT" --sub "$ALICE_SUB" --aud lakehouse-engine)"
TIMEFORMAT=%R
t=$( { time (curl -sS -o /dev/null -X POST "$KC_TOKEN_URL" \
        -d grant_type=urn:ietf:params:oauth:grant-type:token-exchange \
        -d client_id=lakehouse -d client_secret="$ENGINE_CLIENT_SECRET" \
        -d "subject_token=$SUBJ" -d subject_token_type=urn:ietf:params:oauth:token-type:access_token \
        -d subject_issuer=engine) ; } 2>&1 )
note "one exchange (local Docker, warm): ${t}s"
note "round trips per query: 1 adapter->Keycloak, plus Keycloak->engine introspection"
note "and Keycloak->engine userinfo, i.e. 3 HTTP calls and a Keycloak user session"
note "created per query (see 'session_state' in the exchange response)."
