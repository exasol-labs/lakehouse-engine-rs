#!/usr/bin/env bash
# ROUND 3, item 7 — negative controls, every one explicit.
#
# Each must fail, and the failure MODE is the point: 401 means "the catalog did
# not believe the token", 404 means "it believed the token and the principal has
# no grant". Those are very different alarms for an operator, and on this API
# they are the only two an engine-minted token can produce.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=r3-lib.sh
source "$HERE/r3-lib.sh"

ADMIN="$(r3_admin)"

# Print code AND the error type, so 404-not-granted is distinguishable from
# 404-table-missing in the transcript rather than by assertion.
probe() {  # $1 = label, rest = mint-jwt args
  local label="$1"; shift
  local t body code type msg
  t="$("$MINT" --iss "$R3_ISS" "$@" 2>/dev/null)" || { printf '     %-38s MINT FAILED\n' "$label"; return 0; }
  body="$(curl -sS -o - -w '\n%{http_code}' -X GET "$R3_TBL/alice_table" -H "Authorization: Bearer $t")"
  code="$(tail -1 <<<"$body")"
  type="$(sed '$d' <<<"$body" | jq -r '.error.type // empty' 2>/dev/null)"
  msg="$(sed '$d' <<<"$body" | jq -r '.error.message // empty' 2>/dev/null)"
  printf '     %-38s HTTP %-4s %-24s %s\n' "$label" "$code" "$type" "$msg"
}

say "3.7.0  Positive control — the token that must work"
probe "granted principal (exasol~alice)" --sub alice

say "3.7.1  Authentication failures (must be 401)"
FORGED="$(mktemp -u).pem"; openssl genrsa -out "$FORGED" 2048 2>/dev/null
probe "kid not in the JWKS"             --sub alice --kid no-such-key
probe "kid in JWKS, wrong private key"  --sub alice --key "$FORGED"
rm -f "$FORGED"
probe "expired 61s (beyond 60s skew)"   --sub alice --ttl -61
probe "expired 1h"                      --sub alice --ttl -3600
probe "wrong audience"                  --sub alice --aud not-lakekeeper
probe "no audience"                     --sub alice --omit aud
probe "wrong issuer (the customer IdP)" --sub alice --iss "$KC_URI"
UNS_H="$(printf '%s' '{"alg":"none","typ":"JWT"}' | base64 -w0 | tr '+/' '-_' | tr -d '=')"
UNS_P="$(jq -nc --arg s alice --arg i "$R3_ISS" '{iss:$i,sub:$s,aud:"lakekeeper",exp:9999999999}' \
  | base64 -w0 | tr '+/' '-_' | tr -d '=')"
printf '     %-38s HTTP %s\n' "alg=none (unsigned)" "$(r3_code "$UNS_H.$UNS_P." alice_table)"
note "boundary, for completeness — inside the 60s skew allowance:"
probe "expired 30s (inside skew)"       --sub alice --ttl -30

say "3.7.2  Authorization failures (valid token, no grant — must be 404)"
note "case A: the principal has a catalog user record but no grant"
note "  POST /management/v1/user exasol~erin -> HTTP $(req_code "$ADMIN" POST "$R3_MGMT/user" \
  --data '{"id":"exasol~erin","name":"Erin (Exasol)","user-type":"human","update-if-exists":true}')"
probe "exasol~erin (exists, not granted)" --sub erin
note "case B: the principal does not exist in the catalog at all"
probe "exasol~mallory (no record at all)" --sub mallory
note "-> the two are INDISTINGUISHABLE from the client: same status, same type,"
note "   same message. An operator cannot tell 'wrong user name' from 'missing"
note "   grant' from the response, only from the catalog's own user list."

say "3.7.3  What the two failure modes mean operationally"
cat <<'TXT'
     401 AuthenticationFailed -> the token was rejected before any grant lookup.
        Causes: key not published / not yet refreshed, clock skew past exp,
        wrong audience, wrong issuer. Every user of the engine fails at once.
        This is an engine- or catalog-configuration alarm.
     404 NoSuchTableError    -> the token was accepted and the principal has no
        grant on that object. Causes: the admin granted a different string
        (case, typo, wrong idp prefix), or the grant was never made. It affects
        one user or one table, and it is silent — it reads exactly like a
        missing table.
TXT
