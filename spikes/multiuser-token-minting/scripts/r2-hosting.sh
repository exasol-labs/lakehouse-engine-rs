#!/usr/bin/env bash
# Round 2, barrier 1: the engine must publish an OIDC discovery document + JWKS
# that Lakekeeper can fetch. "Standing up and operating a web server is not
# acceptable", so the two candidates are places the customer ALREADY runs.
#
#   1a  BucketFS  (Exasol's own object store, already there)  -> FAILS
#   1b  object storage (the lakehouse bucket, already there)  -> WORKS
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"

KEYS="$SPIKE_DIR/keys"
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"; docker rm -f lk-bfs >/dev/null 2>&1 || true' EXIT

# BucketFS's certificate covers *.exacluster.local, so the provider URI has to use
# a name inside that SAN; EXA_IP is the Exasol container's address on the spike network.
BFS_HOST="${BFS_HOST:-exasol.exacluster.local}"
EXA_IP="${EXA_IP:-$(docker inspect lakehouse-engine-rs-exasol-1 \
  -f '{{(index .NetworkSettings.Networks "spike-multiuser-token-minting").IPAddress}}' 2>/dev/null)}"

# Re-publish the engine's JWKS under a chosen issuer URL.
publish_docs() {  # $1 = issuer url, $2 = out dir
  mkdir -p "$2"
  python3 - "$KEYS/engine-signing-key.pem" "$2" "$1" lakehouse-engine-1 <<'PY'
import base64, json, sys
from cryptography.hazmat.primitives import serialization
key_path, out, issuer, kid = sys.argv[1:5]
key = serialization.load_pem_private_key(open(key_path,'rb').read(), password=None)
pub = key.public_key().public_numbers()
b64u = lambda i: base64.urlsafe_b64encode(i.to_bytes((i.bit_length()+7)//8,'big')).rstrip(b'=').decode()
json.dump({"keys":[{"kty":"RSA","use":"sig","alg":"RS256","kid":kid,"n":b64u(pub.n),"e":b64u(pub.e)}]},
          open(f"{out}/jwks.json","w"), indent=2)
json.dump({"issuer":issuer,"jwks_uri":f"{issuer}/jwks.json",
           "authorization_endpoint":f"{issuer}/unsupported/authorize",
           "token_endpoint":f"{issuer}/unsupported/token",
           "response_types_supported":["token"],"subject_types_supported":["public"],
           "id_token_signing_alg_values_supported":["RS256"],"grant_types_supported":[],
           "scopes_supported":["openid"]},
          open(f"{out}/openid-configuration","w"), indent=2)
PY
}

# Start a throwaway Lakekeeper against the SAME database, pointed at one
# provider URI, and report whether it comes up. Never touches the live service.
probe_lakekeeper() {  # $1 = provider uri, $@ = extra docker run args
  local uri="$1"; shift
  local name="lk-probe-$$" out
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker run -d --name "$name" --network spike-multiuser-token-minting "$@" \
    -e LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure! \
    -e LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
    -e LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
    -e LAKEKEEPER__AUTHZ_BACKEND=openfga \
    -e LAKEKEEPER__OPENFGA__ENDPOINT=http://openfga:8081 \
    -e "LAKEKEEPER__OPENID_PROVIDER_URI=$uri" \
    -e LAKEKEEPER__OPENID_AUDIENCE=lakekeeper \
    -e RUST_LOG=info \
    quay.io/lakekeeper/catalog:v0.13.1 serve >/dev/null
  # Either it logs a working authenticator, or it dies on the discovery fetch.
  for _ in $(seq 30); do
    out="$(docker logs "$name" 2>&1)"
    grep -q 'Successfully added OIDC authenticator' <<<"$out" && break
    [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" = "false" ] && break
    sleep 1
  done
  out="$(docker logs "$name" 2>&1)"
  docker rm -f "$name" >/dev/null 2>&1 || true
  if grep -q 'Successfully added OIDC authenticator' <<<"$out"; then
    echo "ACCEPTED"
    { grep -o '"message":"[^"]*OIDC authenticator[^"]*"' <<<"$out" || true; } | head -2
  else
    echo "REJECTED"
    { grep -o '^Error: .*' <<<"$out" || true; } | head -1
    { grep -o '"message":"[^"]*[Ff]ailed[^"]*"' <<<"$out" || true; } | head -2
  fi
  return 0
}

say "1a  BucketFS"
EXA=$(docker ps --filter name=exasol --format '{{.Names}}' | head -1)
if [ -z "$EXA" ]; then
  note "SKIPPED: the repo's main stack (docker-compose.yml) is not running."
  note "Bring it up with 'docker compose up -d exasol' from the repo root to reproduce."
else
  # BucketFS serves HTTPS only (HttpPort = 0). Two things follow, and getting
  # either wrong looks like the same opaque "error sending request":
  #   * the URL must use a name the server certificate actually covers, and
  #   * Lakekeeper must trust the ROOT of the chain, not the leaf.
  docker run --rm --network spike-multiuser-token-minting alpine/openssl:3.3.2 \
    s_client -connect "$EXA_IP:2581" -servername "$BFS_HOST" -showcerts </dev/null 2>/dev/null \
    | awk '/BEGIN CERT/,/END CERT/' > "$WORK/exa-chain.pem"
  ( cd "$WORK" && csplit -z -f exacert- -b '%d.pem' exa-chain.pem '/BEGIN CERTIFICATE/' '{*}' >/dev/null )
  note "BucketFS presents a $(grep -c 'BEGIN CERTIFICATE' "$WORK/exa-chain.pem")-certificate chain:"
  for f in "$WORK"/exacert-*.pem; do
    printf '     %s\n' "$(openssl x509 -in "$f" -noout -subject -ext subjectAltName 2>/dev/null | tr '\n' ' ')"
  done
  ROOT="$WORK/exacert-1.pem"; LEAF="$WORK/exacert-0.pem"

  publish_docs "https://$BFS_HOST:2581/default/engine-oidc" "$WORK/bfs"
  EXA_PROFILE="${EXA_PROFILE:-docker}"
  # Same extraction the repo's Makefile uses for install-slc.
  BFSPASS="${BUCKETFS_WRITE_PASS:-$(docker exec "$EXA" bash -c \
    "awk '/\[\[Bucket.*default\]\]/{f=1} f&&/WritePasswd/{print \$3;exit}' /exa/etc/EXAConf | base64 -d")}"
  note "publishing via exapump --profile $EXA_PROFILE to the default bucket"
  bfs_cp() { exapump bucketfs cp --profile "$EXA_PROFILE" \
    --bfs-write-password "$BFSPASS" "$1" "$2" 2>&1 | tail -1 | sed 's/^/     /'; }
  bfs_cp "$WORK/bfs/jwks.json" "bfs://default/engine-oidc/jwks.json"
  bfs_cp "$WORK/bfs/openid-configuration" "bfs://default/engine-oidc/.well-known/openid-configuration"
  sleep 3

  note "anonymous read back (bucket is Public = True):"
  curl -skI "https://localhost:${LH_BUCKETFS_PORT:-22581}/default/engine-oidc/jwks.json" \
    | sed -n '1p;/[Cc]ontent-[Tt]ype/p' | sed 's/^/     /'
  note "the same fetch with the chain root trusted, no -k:"
  docker run --rm --network spike-multiuser-token-minting --add-host "$BFS_HOST:$EXA_IP" \
    -v "$ROOT:/ca.pem:ro" curlimages/curl:8.11.1 --cacert /ca.pem -so /dev/null \
    -w '     HTTP %{http_code} over %{http_version}\n' \
    "https://$BFS_HOST:2581/default/engine-oidc/.well-known/openid-configuration" || true

  note "Lakekeeper, chain ROOT trusted via SSL_CERT_FILE:"
  probe_lakekeeper "https://$BFS_HOST:2581/default/engine-oidc" \
    --add-host "$BFS_HOST:$EXA_IP" -v "$ROOT:/ca.pem:ro" -e SSL_CERT_FILE=/ca.pem | sed 's/^/     /'

  note "negative control 1 — the LEAF trusted instead of the root:"
  probe_lakekeeper "https://$BFS_HOST:2581/default/engine-oidc" \
    --add-host "$BFS_HOST:$EXA_IP" -v "$LEAF:/ca.pem:ro" -e SSL_CERT_FILE=/ca.pem | sed 's/^/     /'
  note "negative control 2 — a hostname outside the certificate's SAN:"
  probe_lakekeeper "https://exasol:2581/default/engine-oidc" \
    -v "$ROOT:/ca.pem:ro" -e SSL_CERT_FILE=/ca.pem | sed 's/^/     /'

  say "1a.2  Authorize real users through the BucketFS-hosted issuer"
  docker rm -f lk-bfs >/dev/null 2>&1 || true
  docker run -d --name lk-bfs --network spike-multiuser-token-minting -p 38185:8181 \
    --add-host "$BFS_HOST:$EXA_IP" -v "$ROOT:/ca.pem:ro" -e SSL_CERT_FILE=/ca.pem \
    -e LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure! \
    -e LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
    -e LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
    -e LAKEKEEPER__AUTHZ_BACKEND=openfga -e LAKEKEEPER__OPENFGA__ENDPOINT=http://openfga:8081 \
    -e "LAKEKEEPER__OPENID_PROVIDER_URI=https://$BFS_HOST:2581/default/engine-oidc" \
    -e LAKEKEEPER__OPENID_AUDIENCE=lakekeeper -e RUST_LOG=info \
    quay.io/lakekeeper/catalog:v0.13.1 serve >/dev/null
  for _ in $(seq 40); do curl -sf http://localhost:38185/health >/dev/null 2>&1 && break; sleep 1; done
  # shellcheck disable=SC1090
  source "$SPIKE_DIR/.spike-state"
  BP="http://localhost:38185"; BP2="$BP/catalog/v1/$WH_ID"
  ISS="https://$BFS_HOST:2581/default/engine-oidc"
  TA=$("$MINT" --sub "$ALICE_SUB" --iss "$ISS"); TB=$("$MINT" --sub "$BOB_SUB" --iss "$ISS")
  note "whoami -> $(curl -sS -H "Authorization: Bearer $TA" "$BP/management/v1/whoami" | jq -c '{id,name}')"
  note "alice: alice_table $(req_code "$TA" GET "$BP2/namespaces/$NAMESPACE/tables/alice_table")  bob_table $(req_code "$TA" GET "$BP2/namespaces/$NAMESPACE/tables/bob_table")   (expect 200 / 404)"
  note "bob:   alice_table $(req_code "$TB" GET "$BP2/namespaces/$NAMESPACE/tables/alice_table")  bob_table $(req_code "$TB" GET "$BP2/namespaces/$NAMESPACE/tables/bob_table")   (expect 404 / 200)"
  docker rm -f lk-bfs >/dev/null 2>&1 || true

  cat <<'TXT'
   Verdict for 1a: WORKS, with zero new components. BucketFS already runs inside
   the Exasol cluster the engine is deployed on, it serves the files anonymously
   when the bucket is Public = True, and it returns the right content type.
   Two conditions, and both look identical when violated — reqwest reports only
   "error sending request for url", with no TLS detail at any RUST_LOG level:
     * the provider URI must use a name the BucketFS certificate covers. The
       Docker image's cert is CN=exacluster.local with SAN *.exacluster.local, so
       `https://exasol:2581/...` fails however the trust is configured.
     * Lakekeeper must trust the ROOT of the chain. BucketFS presents two
       certificates that share the subject CN=exacluster.local and both carry
       CA:TRUE; trusting the leaf is not enough.
   The trust itself is ordinary: Lakekeeper honours SSL_CERT_FILE and the system
   bundle at /etc/ssl/certs/ca-certificates.crt. A deployment whose Exasol
   certificate comes from the customer's own PKI needs no special handling at all
   if that PKI is already in the catalog host's trust store.
TXT
  # Leave nothing behind in the repo's MAIN Exasol stack.
  exapump bucketfs rm --profile "$EXA_PROFILE" --bfs-write-password "$BFSPASS" \
    "bfs://default/engine-oidc/jwks.json" >/dev/null 2>&1 || true
  exapump bucketfs rm --profile "$EXA_PROFILE" --bfs-write-password "$BFSPASS" \
    "bfs://default/engine-oidc/.well-known/openid-configuration" >/dev/null 2>&1 || true
  note "removed the published files from BucketFS"
fi

say "1b  Object storage (the bucket the lakehouse data already lives in)"
publish_docs "http://minio:9000/engine-oidc" "$WORK/s3"
docker run --rm --network spike-multiuser-token-minting -v "$WORK/s3:/docs:ro" \
  --entrypoint /bin/sh quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z -c '
    mc alias set s3 http://minio:9000 minioadmin minioadmin >/dev/null &&
    mc mb --ignore-existing s3/engine-oidc >/dev/null &&
    mc anonymous set download s3/engine-oidc >/dev/null &&
    mc cp --attr Content-Type=application/json /docs/jwks.json s3/engine-oidc/jwks.json >/dev/null &&
    mc cp --attr Content-Type=application/json /docs/openid-configuration \
       s3/engine-oidc/.well-known/openid-configuration >/dev/null &&
    echo "   published to s3://engine-oidc (public read)"'
note "anonymous read back:"
curl -sI "$MINIO/engine-oidc/.well-known/openid-configuration" \
  | sed -n '1p;/[Cc]ontent-[Tt]ype/p' | sed 's/^/     /'
note "Lakekeeper against the object-storage URI:"
probe_lakekeeper "http://minio:9000/engine-oidc" | sed 's/^/     /'
cat <<'TXT'
   Verdict for 1b: WORKS. Zero new components: the discovery document and JWKS
   are two static objects in a bucket the deployment already has, served with
   the right content type, reachable by Lakekeeper over the same network path it
   already uses for table data.
   Caveat: the objects must be PUBLICLY readable (a JWKS is public by design,
   but it is a policy change on that prefix), and the URL must be stable —
   rotating it invalidates every token in flight.
TXT
