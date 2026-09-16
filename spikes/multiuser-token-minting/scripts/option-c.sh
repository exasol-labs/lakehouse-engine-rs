#!/usr/bin/env bash
# OPTION C — one secret or two.
#
# Option B needs the adapter to authenticate as a client AND assert a subject.
# Does RFC 7523 private_key_jwt client authentication let ONE registered key do
# both, so the OAuth2 client secret leaves the CONNECTION object entirely?
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"
source "$SPIKE_DIR/.spike-state"

TBL_URL="$PREFIX/namespaces/$NAMESPACE/tables"
# Client `lakehouse-jwt` is registered with clientAuthenticatorType=client-jwt
# and jwks.url pointing at the SAME document Option A's trusted issuer serves.
CLIENT=lakehouse-jwt

client_assertion() { "$MINT" --sub "$CLIENT" --iss "$CLIENT" --aud "$KC_TOKEN_URL" --ttl 120; }

say "C.0  Registration — one key, referenced twice"
jq -r --arg c "$CLIENT" '.clients[] | select(.clientId==$c)
  | "   clientAuthenticatorType: \(.clientAuthenticatorType)\n   jwks.url:                \(.attributes["jwks.url"])\n   signing alg:             \(.attributes["token.endpoint.auth.signing.alg"])\n   secret present:          \(has("secret"))"' \
  "$SPIKE_DIR/keycloak/realm-iceberg-spike.json"
note "engine issuer JWKS (Option A) and client JWKS (Option C) are the same document:"
note "  $(curl -sS "http://localhost:$SPK_ISSUER_PORT/jwks.json" | jq -c '.keys[0]|{kid,kty,alg}')"

say "C.1  client_credentials with private_key_jwt — no client secret anywhere"
CA="$(client_assertion)"
decode_jwt "$CA" | jq -c '.claims | {iss,sub,aud,jti,exp}' | sed 's/^/   assertion: /'
R="$(curl -sS -w '\n%{http_code}' -X POST "$KC_TOKEN_URL" \
  -d grant_type=client_credentials -d "client_id=$CLIENT" \
  -d client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer \
  -d "client_assertion=$CA")"
note "HTTP $(tail -1 <<<"$R")  $(sed '$d' <<<"$R" | jq -c 'if .error then {error,error_description} else {token_type,scope} end')"

say "C.2  Negative control — the same client with a secret instead"
note "HTTP $(curl -sS -o /dev/null -w '%{http_code}' -X POST "$KC_TOKEN_URL" \
  -d grant_type=client_credentials -d "client_id=$CLIENT" -d client_secret=anything)  (client-jwt clients reject client_secret auth)"

say "C.3  Negative control — assertion signed by a different key"
FORGED="$(mktemp -u).pem"; openssl genrsa -out "$FORGED" 2048 2>/dev/null
BAD="$("$MINT" --sub "$CLIENT" --iss "$CLIENT" --aud "$KC_TOKEN_URL" --ttl 120 --key "$FORGED")"
rm -f "$FORGED"
R="$(curl -sS -w '\n%{http_code}' -X POST "$KC_TOKEN_URL" \
  -d grant_type=client_credentials -d "client_id=$CLIENT" \
  -d client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer \
  -d "client_assertion=$BAD")"
note "HTTP $(tail -1 <<<"$R")  $(sed '$d' <<<"$R" | jq -c '{error,error_description}')"

say "C.4  The full Option B exchange, secret-free"
CA="$(client_assertion)"
SUBJ="$("$MINT" --sub "$ALICE_SUB" --aud lakehouse-engine)"
R="$(curl -sS -w '\n%{http_code}' -X POST "$KC_TOKEN_URL" \
  -d grant_type=urn:ietf:params:oauth:grant-type:token-exchange \
  -d "client_id=$CLIENT" \
  -d client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer \
  -d "client_assertion=$CA" \
  -d "subject_token=$SUBJ" -d subject_token_type=urn:ietf:params:oauth:token-type:access_token \
  -d subject_issuer=engine)"
note "HTTP $(tail -1 <<<"$R")"
AT="$(sed '$d' <<<"$R" | jq -r '.access_token // empty')"
if [ -n "$AT" ]; then
  decode_jwt "$AT" | jq -c '.claims | {sub,aud,azp,preferred_username}' | sed 's/^/   /'
  note "Lakekeeper identity: $(curl -sS -H "Authorization: Bearer $AT" "$MGMT/whoami" | jq -r .id)"
  note "alice_table -> HTTP $(req_code "$AT" GET "$TBL_URL/alice_table")  bob_table -> HTTP $(req_code "$AT" GET "$TBL_URL/bob_table")"
fi

say "C.5  Secret count per option, as it would appear in the CONNECTION object"
cat <<'TABLE'
   Option A (trusted issuer, no exchange)   1 : the RSA signing key. No client id/secret at all.
   Option B with client_secret              2 : the RSA signing key + the OAuth2 client secret.
   Option B with private_key_jwt (C)        1 : the RSA signing key, used for BOTH client
                                                authentication and the subject assertion.
   Option B-control (requested_subject)     1 : the OAuth2 client secret only (no engine key) —
                                                but on a deprecated preview feature.
TABLE
note "Confirmed: Option A needs exactly one secret and no client credentials."
