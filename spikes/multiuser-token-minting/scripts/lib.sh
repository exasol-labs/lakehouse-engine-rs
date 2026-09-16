# Shared helpers for the spike scripts. Source, do not execute.
# shellcheck shell=bash
set -euo pipefail

SPIKE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "$SPIKE_DIR/docker-compose.spike.yml")

SPK_KEYCLOAK_PORT="${SPK_KEYCLOAK_PORT:-38080}"
SPK_KEYCLOAK_LEGACY_PORT="${SPK_KEYCLOAK_LEGACY_PORT:-38081}"
SPK_LAKEKEEPER_PORT="${SPK_LAKEKEEPER_PORT:-38181}"
SPK_MINIO_PORT="${SPK_MINIO_PORT:-39000}"
SPK_ISSUER_PORT="${SPK_ISSUER_PORT:-38090}"

KC="http://localhost:$SPK_KEYCLOAK_PORT"
KC_LEGACY="http://localhost:$SPK_KEYCLOAK_LEGACY_PORT"
REALM="iceberg"
KC_TOKEN_URL="$KC/realms/$REALM/protocol/openid-connect/token"
LK="http://localhost:$SPK_LAKEKEEPER_PORT"
MGMT="$LK/management/v1"
CATALOG="$LK/catalog"
MINIO="http://localhost:$SPK_MINIO_PORT"

ENGINE_CLIENT_ID="lakehouse"
ENGINE_CLIENT_SECRET="lakehouse-engine-secret"
ENGINE_ISSUER="http://engine-issuer:8090"
WAREHOUSE="spike"
NAMESPACE="analytics"
ALICE_SUB="11111111-1111-4111-8111-111111111111"
BOB_SUB="22222222-2222-4222-8222-222222222222"

MINT="$SPIKE_DIR/scripts/mint-jwt.py"
EVIDENCE="$SPIKE_DIR/evidence"

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }

# Pretty-print a JWT's header and claims (signature redacted) so the report can
# show what was actually asserted and by whom.
decode_jwt() { "$MINT" --decode "$1"; }

# Client-credentials token for the engine's service account.
engine_token() {
  curl -sS -X POST "$KC_TOKEN_URL" \
    -d grant_type=client_credentials \
    -d "client_id=$ENGINE_CLIENT_ID" \
    -d "client_secret=$ENGINE_CLIENT_SECRET" | jq -r '.access_token'
}

# Genuine Keycloak user token via ROPC (control identity source).
user_token() {
  curl -sS -X POST "$KC_TOKEN_URL" \
    -d grant_type=password -d client_id=user-cli \
    -d "username=$1" -d "password=$2" -d scope=openid | jq -r '.access_token'
}

# Engine-minted assertion for a user (Option A / Option B subject token).
engine_mint() { "$MINT" --sub "$1" "${@:2}"; }

# curl that prints "HTTP <code>" then the body — every claim in the report is
# backed by one of these transcripts.
req() {
  local token="$1" method="$2" url="$3"; shift 3
  local out code
  out="$(mktemp)"
  code="$(curl -sS -o "$out" -w '%{http_code}' -X "$method" "$url" \
    -H "Authorization: Bearer $token" -H 'Content-Type: application/json' "$@")"
  echo "HTTP $code"
  if [ -s "$out" ]; then jq . "$out" 2>/dev/null || cat "$out"; fi
  rm -f "$out"
}

# Same, but returns only the status code (for assertions).
req_code() {
  local token="$1" method="$2" url="$3"; shift 3
  curl -sS -o /dev/null -w '%{http_code}' -X "$method" "$url" \
    -H "Authorization: Bearer $token" -H 'Content-Type: application/json' "$@"
}
