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
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT

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
probe_lakekeeper() {  # $1 = provider uri
  local name="lk-probe-$$" out
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker run -d --name "$name" --network spike-multiuser-token-minting \
    -e LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure! \
    -e LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
    -e LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
    -e LAKEKEEPER__AUTHZ_BACKEND=openfga \
    -e LAKEKEEPER__OPENFGA__ENDPOINT=http://openfga:8081 \
    -e "LAKEKEEPER__OPENID_PROVIDER_URI=$1" \
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
  publish_docs "https://exasol:2581/default/engine-oidc" "$WORK/bfs"
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
  note "anonymous read back (bucket is Public=True):"
  curl -skI "https://localhost:${LH_BUCKETFS_PORT:-22581}/default/engine-oidc/jwks.json" \
    | sed -n '1p;/[Cc]ontent-[Tt]ype/p' | sed 's/^/     /'
  note "the same fetch WITHOUT -k, from inside the compose network:"
  docker run --rm --network spike-multiuser-token-minting curlimages/curl:8.11.1 \
    -sS "https://exasol:2581/default/engine-oidc/.well-known/openid-configuration" \
    2>&1 | head -2 | sed 's/^/     /' || true
  note "Lakekeeper against the BucketFS URI:"
  probe_lakekeeper "https://exasol:2581/default/engine-oidc" | sed 's/^/     /'
  cat <<'TXT'
   Measured failure: BucketFS serves HTTPS only (HttpPort = 0 in EXAConf) with a
   self-signed certificate, and Lakekeeper has no trust-store or
   skip-verify knob for the discovery fetch:
     Failed to fetch openid configuration from
     https://exasol:2581/default/engine-oidc/.well-known/openid-configuration:
     error sending request for url (...)
   BucketFS does serve the right content type (application/json above), and the
   bucket is readable anonymously when Public = True — the publishing half works.
   It is the transport that kills it. Verdict for 1a: FAILS. Enabling plain HTTP
   on BucketFS (HttpPort) would publish the whole bucket unencrypted, which is
   not an acceptable trade for hosting one public key.
TXT
  # This probe writes into the repo's MAIN Exasol stack; leave nothing behind.
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
