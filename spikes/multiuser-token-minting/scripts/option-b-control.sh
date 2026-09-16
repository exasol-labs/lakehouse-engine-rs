#!/usr/bin/env bash
# OPTION B-CONTROL — Keycloak legacy `requested_subject` impersonation.
#
# Reproduced only as a baseline. Three servers, same realm import:
#   26.4.0, V2 defaults              (what a current deployment gets)
#   26.0.7, token-exchange:v1 + FGAP (the original spike path)
#   26.4.0, token-exchange:v1 + FGAP (is the legacy path still reachable?)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"

KC_V1_ON_264="http://localhost:${SPK_KEYCLOAK_V1_PORT:-38083}"

say "Bringing up the two legacy servers (compose profile 'legacy')"
"${COMPOSE[@]}" --profile legacy up -d --wait keycloak-legacy keycloak-v1 >/dev/null 2>&1
note "26.0.7 v1: $KC_LEGACY"
note "26.4.0 v1: $KC_V1_ON_264"

features() {
  local url="$1" at
  at="$(curl -sS -X POST "$url/realms/master/protocol/openid-connect/token" \
    -d grant_type=password -d client_id=admin-cli -d username=admin -d password=admin | jq -r .access_token)"
  curl -sS -H "Authorization: Bearer $at" "$url/admin/serverinfo" \
    | jq -r '.features[] | select(.name|test("TOKEN_EXCHANGE|FINE_GRAINED")) | "     \(.name) enabled=\(.enabled) type=\(.type)"' | sort
}

impersonate() {
  local url="$1" et
  et="$(curl -sS -X POST "$url/realms/$REALM/protocol/openid-connect/token" \
    -d grant_type=client_credentials -d client_id=lakehouse -d client_secret="$ENGINE_CLIENT_SECRET" \
    | jq -r .access_token)"
  curl -sS -w '\n%{http_code}' -X POST "$url/realms/$REALM/protocol/openid-connect/token" \
    -d grant_type=urn:ietf:params:oauth:grant-type:token-exchange \
    -d client_id=lakehouse -d client_secret="$ENGINE_CLIENT_SECRET" \
    -d "subject_token=$et" -d subject_token_type=urn:ietf:params:oauth:token-type:access_token \
    -d "requested_subject=alice" -d audience=lakehouse
}

report() {
  local label="$1" url="$2" r code body
  say "$label — $url"
  features "$url"
  r="$(impersonate "$url")"; code="$(tail -1 <<<"$r")"; body="$(sed '$d' <<<"$r")"
  note "requested_subject=alice -> HTTP $code"
  local at; at="$(jq -r '.access_token // empty' <<<"$body" 2>/dev/null || true)"
  if [ -n "$at" ]; then
    decode_jwt "$at" | jq -c '.claims | {iss,sub,aud,azp,preferred_username,scope}' | sed 's/^/     /'
  else
    jq -c '{error, error_description}' <<<"$body" 2>/dev/null | sed 's/^/     /' || echo "     $body"
  fi
}

report "26.4.0, V2 defaults (no legacy flag)"      "$KC"
say "Wiring FGAP v1 policies on the two legacy servers"
note "(users-management-permissions -> impersonate; client permissions -> token-exchange;"
note " one client policy attached to both — see scripts/kc-fgap-v1-impersonation.sh)"
for u in "$KC_LEGACY" "$KC_V1_ON_264"; do
  bash "$HERE/kc-fgap-v1-impersonation.sh" "$u" 2>&1 | sed 's/^/   /'
done
report "26.0.7, token-exchange:v1 + FGAP v1"       "$KC_LEGACY"
report "26.4.0, token-exchange:v1 + FGAP v1"       "$KC_V1_ON_264"

say "Reading of the control"
note "Impersonation works only with the PREVIEW token-exchange:v1 feature plus"
note "FGAP v1 policy wiring, on both 26.0.7 and 26.4.0. On a 26.4.0 with V2"
note "defaults it is rejected outright:"
note "  \"Parameter 'requested_subject' is not supported for standard token exchange\""
note "Turning token-exchange:v1 on also turns TOKEN_EXCHANGE_EXTERNAL_INTERNAL_V2"
note "OFF (see the feature listings above), so a deployment cannot run the legacy"
note "path and Option B's V2 path on the same server."
