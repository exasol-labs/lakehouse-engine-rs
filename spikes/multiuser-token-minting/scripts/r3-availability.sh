#!/usr/bin/env bash
# ROUND 3, item 8 (continued) — what an issuer outage costs.
#
# 3.8.2/3.8.3 showed that a secondary provider whose discovery document cannot be
# fetched is FATAL: the catalog process exits, taking the customer's own Keycloak
# path with it. Hosting the engine's documents on BucketFS therefore makes the
# Exasol cluster a hard startup dependency of the catalog. This measures the two
# cases separately — an outage while the catalog is already running, and an
# outage across a catalog restart.
#
# The issuer is reached through a socat proxy that owns the certificate's
# hostname on the spike network, so the "outage" is a stopped proxy rather than a
# stopped Exasol cluster.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=r3-lib.sh
source "$HERE/r3-lib.sh"

PROXY="bfs-proxy"
AV="lk-r3-avail"; AV_PORT=38187
AV_LK="http://localhost:$AV_PORT"; AV_TBL="$AV_LK/catalog/v1/$WH_ID/namespaces/$NAMESPACE/tables"
trap 'docker rm -f "$PROXY" "$AV" >/dev/null 2>&1 || true' EXIT

extract_chain_root

start_proxy() {
  docker rm -f "$PROXY" >/dev/null 2>&1 || true
  docker run -d --name "$PROXY" --network "$NET" \
    --network-alias "$BFS_HOST" alpine/socat:1.8.0.0 \
    "TCP-LISTEN:2581,fork,reuseaddr" "TCP:$EXA_IP:2581" >/dev/null
  sleep 2
}

start_av() {  # no --add-host: the name resolves to the proxy
  docker rm -f "$AV" >/dev/null 2>&1 || true
  docker run -d --name "$AV" --network "$NET" -p "$AV_PORT:8181" \
    -v "$R3_ROOT_CA:/ca.pem:ro" -e SSL_CERT_FILE=/ca.pem \
    -e LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure! \
    -e LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
    -e LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
    -e LAKEKEEPER__AUTHZ_BACKEND=openfga -e LAKEKEEPER__OPENFGA__ENDPOINT=http://openfga:8081 \
    -e "LAKEKEEPER__OPENID_PROVIDER_URI=$KC_URI" \
    -e LAKEKEEPER__OPENID_AUDIENCE=lakekeeper -e LAKEKEEPER__OPENID_SUBJECT_CLAIM=sub \
    -e "LAKEKEEPER__OPENID_PROVIDERS__EXASOL__URI=$R3_ISS" \
    -e LAKEKEEPER__OPENID_PROVIDERS__EXASOL__AUDIENCE=lakekeeper \
    -e LAKEKEEPER__OPENID_PROVIDERS__EXASOL__SUBJECT_CLAIMS=sub \
    -e RUST_LOG=info "$LK_IMAGE" serve >/dev/null
  local i
  for i in $(seq 40); do
    curl -sf "$AV_LK/health" >/dev/null 2>&1 && return 0
    [ "$(docker inspect -f '{{.State.Running}}' "$AV" 2>/dev/null)" = "false" ] && return 1
    sleep 1
  done
  return 1
}

av_code() { req_code "$1" GET "$AV_TBL/$2"; }
state()   { printf '     catalog process running: %s   /health: %s\n' \
              "$(docker inspect -f '{{.State.Running}}' "$AV" 2>/dev/null)" \
              "$(curl -s -o /dev/null -w '%{http_code}' "$AV_LK/health" || echo none)"; }

say "3.8.6  Healthy — issuer reachable through the proxy"
start_proxy
start_av && note "catalog came up" || note "catalog FAILED to come up"
state
note "exasol~alice  alice_table -> HTTP $(av_code "$(r3_mint alice)" alice_table)"
note "keycloak alice alice_table -> HTTP $(av_code "$(user_token_net alice alice)" alice_table)"

say "3.8.7  Issuer outage WHILE the catalog is already running"
docker stop "$PROXY" >/dev/null
note "proxy stopped; the discovery document and JWKS are now unreachable."
state
note "exasol~alice  alice_table -> HTTP $(av_code "$(r3_mint alice)" alice_table)"
note "keycloak alice alice_table -> HTTP $(av_code "$(user_token_net alice alice)" alice_table)"
note "-> a running catalog keeps serving from its cached key set. The outage is"
note "   invisible until the next refresh falls due."

say "3.8.8  Issuer outage ACROSS a catalog restart"
RESTART_TS="$(date -u +%Y-%m-%dT%H:%M:%S)"
docker restart "$AV" >/dev/null 2>&1 || true
sleep 12
state
note "startup log, SINCE the restart only (docker logs accumulates across restarts):"
{ docker logs --since "$RESTART_TS" "$AV" 2>&1 | grep -oE '"message":"(Configuring [0-9]+ OIDC provider\(s\)|Successfully added OIDC authenticator: [a-z0-9-]+)"' || true; } \
  | sort -u | sed 's/^/     /'
{ docker logs --since "$RESTART_TS" "$AV" 2>&1 | grep -oE '^Error: .*' || true; } | tail -1 | sed 's/^/     /'
note "engine token  -> $(curl -s -o /dev/null -w '%{http_code}' "$AV_LK/health" || echo unreachable)"
note "-> the catalog refuses to start. This is not degraded mode: the customer's"
note "   Keycloak path, Spark and Trino included, is down with it."

say "3.8.9  Recovery"
start_proxy
docker restart "$AV" >/dev/null
for _ in $(seq 40); do curl -sf "$AV_LK/health" >/dev/null 2>&1 && break; sleep 1; done
state
note "exasol~alice  alice_table -> HTTP $(av_code "$(r3_mint alice)" alice_table)"
note "keycloak alice alice_table -> HTTP $(av_code "$(user_token_net alice alice)" alice_table)"
note "-> recovery needs no intervention beyond the issuer becoming reachable again."

say "3.8.10  Control — is the PRIMARY provider fatal in the same way?"
# The blast radius above is only new information if the catalog does not already
# die when the customer's own IdP is unreachable. Same catalog, same secondary,
# primary pointed at a host that does not exist.
docker rm -f "$AV" >/dev/null 2>&1 || true
docker run -d --name "$AV" --network "$NET" -p "$AV_PORT:8181" \
  --add-host "$BFS_HOST:$EXA_IP" -v "$R3_ROOT_CA:/ca.pem:ro" -e SSL_CERT_FILE=/ca.pem \
  -e LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure! \
  -e LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
  -e LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
  -e LAKEKEEPER__AUTHZ_BACKEND=openfga -e LAKEKEEPER__OPENFGA__ENDPOINT=http://openfga:8081 \
  -e "LAKEKEEPER__OPENID_PROVIDER_URI=http://no-such-idp:8080/realms/iceberg" \
  -e LAKEKEEPER__OPENID_AUDIENCE=lakekeeper -e LAKEKEEPER__OPENID_SUBJECT_CLAIM=sub \
  -e "LAKEKEEPER__OPENID_PROVIDERS__EXASOL__URI=$R3_ISS" \
  -e LAKEKEEPER__OPENID_PROVIDERS__EXASOL__AUDIENCE=lakekeeper \
  -e LAKEKEEPER__OPENID_PROVIDERS__EXASOL__SUBJECT_CLAIMS=sub \
  -e RUST_LOG=info "$LK_IMAGE" serve >/dev/null
sleep 15
state
{ docker logs "$AV" 2>&1 | grep -oE '"message":"Successfully added OIDC authenticator: [a-z0-9-]+"' || true; } | sort -u | sed 's/^/     /'
{ docker logs "$AV" 2>&1 | grep -oE '^Error: .*' || true; } | tail -1 | cut -c1-200 | sed 's/^/     /'
note "-> if this also dies, an unreachable issuer was ALREADY fatal and 1a only"
note "   adds a second host to the list. If it survives, 1a introduces a new"
note "   single point of failure that the customer did not have before."
