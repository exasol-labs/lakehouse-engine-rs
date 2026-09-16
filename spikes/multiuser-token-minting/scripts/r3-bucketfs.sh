#!/usr/bin/env bash
# ROUND 3, item 8 — the BucketFS conditions, confirmed IN THE COMBINED CONFIG.
#
# Round 2 established both conditions against a single-provider catalog. The
# question round 3 has to answer is what they cost when the BucketFS issuer is
# the SECONDARY provider: does a bad certificate take down only the engine's
# path, or the customer's Keycloak path with it?
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=r3-lib.sh
source "$HERE/r3-lib.sh"

extract_chain_root
LEAF="$R3_WORK/exa-leaf.pem"
PROBE="lk-r3-probe"

# Start the SAME combined configuration under a different TLS/URL setup and
# report what the catalog does: which authenticators came up, or how it died.
probe_combined() {  # $1 = exasol provider uri, rest = extra docker args
  local uri="$1"; shift
  local out running
  docker rm -f "$PROBE" >/dev/null 2>&1 || true
  docker run -d --name "$PROBE" --network "$NET" "$@" \
    -e LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure! \
    -e LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
    -e LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
    -e LAKEKEEPER__AUTHZ_BACKEND=openfga -e LAKEKEEPER__OPENFGA__ENDPOINT=http://openfga:8081 \
    -e "LAKEKEEPER__OPENID_PROVIDER_URI=$KC_URI" \
    -e LAKEKEEPER__OPENID_AUDIENCE=lakekeeper -e LAKEKEEPER__OPENID_SUBJECT_CLAIM=sub \
    -e "LAKEKEEPER__OPENID_PROVIDERS__EXASOL__URI=$uri" \
    -e LAKEKEEPER__OPENID_PROVIDERS__EXASOL__AUDIENCE=lakekeeper \
    -e LAKEKEEPER__OPENID_PROVIDERS__EXASOL__SUBJECT_CLAIMS=sub \
    -e RUST_LOG=info "$LK_IMAGE" serve >/dev/null
  for _ in $(seq 30); do
    out="$(docker logs "$PROBE" 2>&1)"
    grep -q 'Successfully added OIDC authenticator: exasol' <<<"$out" && break
    [ "$(docker inspect -f '{{.State.Running}}' "$PROBE" 2>/dev/null)" = "false" ] && break
    sleep 1
  done
  out="$(docker logs "$PROBE" 2>&1)"
  running="$(docker inspect -f '{{.State.Running}}' "$PROBE" 2>/dev/null)"
  printf '     process running: %s\n' "$running"
  { grep -oE '"message":"(Configuring [0-9]+ OIDC provider\(s\)|Successfully added OIDC authenticator: [a-z0-9-]+)"' <<<"$out" || true; } \
    | sort -u | sed 's/^/     /'
  { grep -oE '^Error: .*' <<<"$out" || true; } | head -2 | sed 's/^/     /'
  { grep -oE '"message":"[^"]*([Ff]ailed|error)[^"]*"' <<<"$out" || true; } | head -2 | sed 's/^/     /'
  docker rm -f "$PROBE" >/dev/null 2>&1 || true
}

say "3.8.0  The conditions, restated from the live certificate"
for f in "$R3_WORK"/chain/cert-*.pem; do
  printf '     %s\n' "$(openssl x509 -in "$f" -noout -subject -issuer -ext subjectAltName 2>/dev/null | tr '\n' ' ')"
done
note "issuer URL under test: $R3_ISS"

say "3.8.1  Correct — SAN-covered host, chain ROOT in SSL_CERT_FILE"
probe_combined "$R3_ISS" --add-host "$BFS_HOST:$EXA_IP" -v "$R3_ROOT_CA:/ca.pem:ro" -e SSL_CERT_FILE=/ca.pem

say "3.8.2  Negative control — the LEAF trusted instead of the root"
probe_combined "$R3_ISS" --add-host "$BFS_HOST:$EXA_IP" -v "$LEAF:/ca.pem:ro" -e SSL_CERT_FILE=/ca.pem

say "3.8.3  Negative control — a hostname outside the certificate SAN"
probe_combined "https://exasol:2581/$BFS_PATH" -v "$R3_ROOT_CA:/ca.pem:ro" -e SSL_CERT_FILE=/ca.pem

say "3.8.4  Blast radius — does a broken secondary take the primary with it?"
note "read 'process running' and the authenticator lines in 3.8.2 and 3.8.3."
note "if the process is not running, the customer's own Keycloak path is down too."

say "3.8.5  The customer's own PKI — is any step needed?"
# Appending the Exasol root to the image's stock bundle models the case where the
# certificate is signed by a CA the catalog host already trusts. No SSL_CERT_FILE
# is set, so this is the default trust path, not an override.
cid="$(docker create "$LK_IMAGE")"
docker cp "$cid:/etc/ssl/certs/ca-certificates.crt" "$R3_WORK/system-ca.crt" >/dev/null 2>&1
docker rm "$cid" >/dev/null
chmod u+w "$R3_WORK/system-ca.crt"
cat "$R3_ROOT_CA" >> "$R3_WORK/system-ca.crt"
note "stock bundle ($(grep -c 'BEGIN CERT' "$R3_WORK/system-ca.crt") certs incl. the Exasol root) at the DEFAULT path, no SSL_CERT_FILE:"
probe_combined "$R3_ISS" --add-host "$BFS_HOST:$EXA_IP" \
  -v "$R3_WORK/system-ca.crt:/etc/ssl/certs/ca-certificates.crt:ro"
note "-> if this comes up, a customer whose Exasol certificate is issued by a CA"
note "   already in the catalog host's trust store needs NO step at all: no"
note "   SSL_CERT_FILE, no certificate copying. Only the SAN condition remains,"
note "   and that is satisfied by using the hostname the certificate was issued for."
