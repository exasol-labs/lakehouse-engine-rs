#!/usr/bin/env bash
# ROUND 3, item 8 (follow-up) — is the fail-to-start on purpose, and can it be
# turned off?
#
# Source reading (lakekeeper v0.13.1, crates/lakekeeper/src/service/authn.rs)
# says yes and yes: every provider carries `require_connected_on_startup`,
# defaulting to true, and only a false value downgrades a boot-time discovery
# failure from fatal to "skip this provider". The primary provider's copy is
# hardcoded true and cannot be configured. This verifies all of that live,
# because the repo rule is that a capability claim is measured, not read.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=r3-lib.sh
source "$HERE/r3-lib.sh"

P="lk-r3-req"
trap 'docker rm -f "$P" >/dev/null 2>&1 || true' EXIT

extract_chain_root
export BFSPASS="$(bfs_password)"
write_docs "$R3_WORK/docs" "$R3_ISS" "$SPIKE_DIR/keys/engine-signing-key.pem:lakehouse-engine-1" >/dev/null
publish_to_bucketfs "$R3_WORK/docs"

REQ_PORT=38188
REQ_LK="http://localhost:$REQ_PORT"
REQ_TBL="$REQ_LK/catalog/v1/$WH_ID/namespaces/$NAMESPACE/tables"

# $1 = primary uri, $2 = exasol uri, $3 = "resolve"|"unreachable", rest = extra -e
boot() {
  local primary="$1" exasol="$2" dns="$3"; shift 3
  local extra=()
  # "resolve" pins the host; "proxy" lets the network alias resolve it; "unreachable" neither.
  [ "$dns" = "resolve" ] && extra=(--add-host "$BFS_HOST:$EXA_IP")
  docker rm -f "$P" >/dev/null 2>&1 || true
  docker run -d --name "$P" --network "$NET" -p "$REQ_PORT:8181" \
    "${extra[@]}" -v "$R3_ROOT_CA:/ca.pem:ro" -e SSL_CERT_FILE=/ca.pem \
    -e LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure! \
    -e LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
    -e LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
    -e LAKEKEEPER__AUTHZ_BACKEND=openfga -e LAKEKEEPER__OPENFGA__ENDPOINT=http://openfga:8081 \
    -e "LAKEKEEPER__OPENID_PROVIDER_URI=$primary" \
    -e LAKEKEEPER__OPENID_AUDIENCE=lakekeeper -e LAKEKEEPER__OPENID_SUBJECT_CLAIM=sub \
    -e "LAKEKEEPER__OPENID_PROVIDERS__EXASOL__URI=$exasol" \
    -e LAKEKEEPER__OPENID_PROVIDERS__EXASOL__AUDIENCE=lakekeeper \
    -e LAKEKEEPER__OPENID_PROVIDERS__EXASOL__SUBJECT_CLAIMS=sub \
    -e RUST_LOG=info "$@" "$LK_IMAGE" serve >/dev/null
  local i
  for i in $(seq 30); do
    curl -sf "$REQ_LK/health" >/dev/null 2>&1 && break
    [ "$(docker inspect -f '{{.State.Running}}' "$P" 2>/dev/null)" = "false" ] && break
    sleep 1
  done
  printf '     process running: %s   /health: %s\n' \
    "$(docker inspect -f '{{.State.Running}}' "$P" 2>/dev/null)" \
    "$(curl -s -o /dev/null -w '%{http_code}' "$REQ_LK/health" 2>/dev/null || echo none)"
  { docker logs "$P" 2>&1 | grep -oE '"message":"(Successfully added OIDC authenticator: [a-z0-9-]+|Failed to create OIDC authenticator for [a-z0-9-]+[^"]*)"' || true; } \
    | sort -u | cut -c1-170 | sed 's/^/     /'
  { docker logs "$P" 2>&1 | grep -oE '^Error: .*' || true; } | tail -1 | cut -c1-180 | sed 's/^/     /'
}

tokens() {
  [ "$(curl -s -o /dev/null -w '%{http_code}' "$REQ_LK/health" 2>/dev/null)" = "200" ] || {
    note "  catalog unreachable; no token probe possible"; return 0; }
  printf '     %-34s alice_table HTTP %s\n' "genuine Keycloak alice" \
    "$(req_code "$(user_token_net alice alice)" GET "$REQ_TBL/alice_table")"
  printf '     %-34s alice_table HTTP %s\n' "engine-minted exasol~alice" \
    "$(req_code "$(r3_mint alice)" GET "$REQ_TBL/alice_table")"
}

say "3.8.11  Engine issuer unreachable, REQUIRE_CONNECTED_ON_STARTUP=false"
boot "$KC_URI" "$R3_ISS" unreachable \
  -e LAKEKEEPER__OPENID_PROVIDERS__EXASOL__REQUIRE_CONNECTED_ON_STARTUP=false
tokens
note "-> the catalog boots with the engine provider SKIPPED. The customer's own"
note "   path is unaffected; only engine-minted tokens fail, and they fail 401."

say "3.8.12  Control — the same outage with the flag left at its default"
boot "$KC_URI" "$R3_ISS" unreachable
tokens

say "3.8.13  Control — the flag set to false while the issuer IS reachable"
boot "$KC_URI" "$R3_ISS" resolve \
  -e LAKEKEEPER__OPENID_PROVIDERS__EXASOL__REQUIRE_CONNECTED_ON_STARTUP=false
tokens
note "-> no downside when healthy: the provider loads normally."

say "3.8.14  The PRIMARY provider cannot be made optional"
# authn.rs builds the primary with require_connected_on_startup hardcoded true,
# and config.rs refuses to start when OPENID_PROVIDERS is set without it.
boot "http://no-such-idp:8080/realms/iceberg" "$R3_ISS" resolve \
  -e LAKEKEEPER__OPENID_PROVIDERS__EXASOL__REQUIRE_CONNECTED_ON_STARTUP=false \
  -e LAKEKEEPER__OPENID_REQUIRE_CONNECTED_ON_STARTUP=false
note "-> both a per-provider flag on the secondary and an invented primary-level"
note "   flag are set here, and the catalog still dies on the primary."

say "3.8.15  Does a skipped provider recover without a restart?"
# Reached through a socat proxy that owns the certificate's hostname on the spike
# network, so the issuer can be switched off and on for a container whose DNS
# genuinely resolves the name. Boot with the proxy DOWN and the flag false.
PROXY="bfs-proxy"
docker rm -f "$PROXY" >/dev/null 2>&1 || true
boot "$KC_URI" "$R3_ISS" proxy \
  -e LAKEKEEPER__OPENID_PROVIDERS__EXASOL__REQUIRE_CONNECTED_ON_STARTUP=false
note "booted with the issuer down:"
tokens

note "issuer brought back up (proxy started), catalog NOT restarted:"
docker run -d --name "$PROXY" --network "$NET" --network-alias "$BFS_HOST" \
  alpine/socat:1.8.0.0 "TCP-LISTEN:2581,fork,reuseaddr" "TCP:$EXA_IP:2581" >/dev/null
sleep 5
note "  the issuer answers from inside the network: HTTP $(docker run --rm --network "$NET" \
  -v "$R3_ROOT_CA:/ca.pem:ro" curlimages/curl:8.11.1 --cacert /ca.pem -s -o /dev/null \
  -w '%{http_code}' "$R3_ISS/.well-known/openid-configuration")"
tokens
sleep 25
note "and again after 30s total:"
tokens
note "-> a provider skipped at boot stays skipped for the process's lifetime;"
note "   the catalog never retries the discovery fetch. Recovery needs a restart."

note "control — restart the same container with the issuer now up:"
docker restart "$P" >/dev/null
for _ in $(seq 40); do curl -sf "$REQ_LK/health" >/dev/null 2>&1 && break; sleep 1; done
tokens
docker rm -f "$PROXY" >/dev/null 2>&1 || true
