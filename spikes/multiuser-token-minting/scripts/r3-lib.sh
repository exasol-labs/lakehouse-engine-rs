# Round 3 shared helpers: the ONE configuration under test — the customer's
# Keycloak as the primary `oidc` provider AND the engine as a secondary
# provider `exasol` whose URI is a BucketFS URL. Source, do not execute.
# shellcheck shell=bash
set -euo pipefail

HERE3="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$HERE3/lib.sh"
# shellcheck disable=SC1090
source "$SPIKE_DIR/.spike-state"

NET="spike-multiuser-token-minting"
LK_IMAGE="quay.io/lakekeeper/catalog:v0.13.1"

# The engine's provider id in round 3. Lakekeeper requires [a-z0-9-]+ and
# reserves `oidc` and `kubernetes`, so `exasol` is legal and is the name the
# admin will type in front of every principal: `exasol~alice`.
R3_IDP="exasol"

BFS_HOST="${BFS_HOST:-exasol.exacluster.local}"
BFS_PATH="${BFS_PATH:-default/engine-oidc}"
R3_ISS="https://$BFS_HOST:2581/$BFS_PATH"
EXA_CONTAINER="${EXA_CONTAINER:-lakehouse-engine-rs-exasol-1}"
EXA_PROFILE="${EXA_PROFILE:-docker}"
EXA_IP="${EXA_IP:-$(docker inspect "$EXA_CONTAINER" \
  -f "{{(index .NetworkSettings.Networks \"$NET\").IPAddress}}" 2>/dev/null)}"

R3_PORT="${R3_PORT:-38186}"
R3_NAME="lk-r3"
R3_LK="http://localhost:$R3_PORT"
R3_MGMT="$R3_LK/management/v1"
R3_CAT="$R3_LK/catalog/v1/$WH_ID"
R3_TBL="$R3_CAT/namespaces/$NAMESPACE/tables"

R3_WORK="${R3_WORK:-$SPIKE_DIR/.r3-work}"
mkdir -p "$R3_WORK"
R3_ROOT_CA="$R3_WORK/exa-root.pem"

# --- BucketFS ---------------------------------------------------------------

bfs_password() {
  docker exec "$EXA_CONTAINER" bash -c \
    "awk '/\[\[Bucket.*default\]\]/{f=1} f&&/WritePasswd/{print \$3;exit}' /exa/etc/EXAConf | base64 -d"
}

# Split the TLS chain BucketFS presents and keep the ROOT (round 2: trusting the
# leaf is not enough — both certificates carry CA:TRUE and the same subject).
extract_chain_root() {
  local d="$R3_WORK/chain"; rm -rf "$d"; mkdir -p "$d"
  docker run --rm --network "$NET" alpine/openssl:3.3.2 \
    s_client -connect "$EXA_IP:2581" -servername "$BFS_HOST" -showcerts </dev/null 2>/dev/null \
    | awk '/BEGIN CERT/,/END CERT/' > "$d/chain.pem"
  ( cd "$d" && csplit -z -f cert- -b '%d.pem' chain.pem '/BEGIN CERTIFICATE/' '{*}' >/dev/null )
  cp "$d/cert-1.pem" "$R3_ROOT_CA"
  cp "$d/cert-0.pem" "$R3_WORK/exa-leaf.pem"
}

# Write a discovery document + a JWKS containing an arbitrary set of keys, so the
# rotation probe can publish one key, then two, then the other one.
#   write_docs <outdir> <issuer> <pem>:<kid> [<pem>:<kid> ...]
write_docs() {
  local out="$1" issuer="$2"; shift 2
  mkdir -p "$out"
  python3 - "$out" "$issuer" "$@" <<'PY'
import base64, json, sys
from cryptography.hazmat.primitives import serialization
out, issuer, *pairs = sys.argv[1:]
b64u = lambda i: base64.urlsafe_b64encode(i.to_bytes((i.bit_length()+7)//8,'big')).rstrip(b'=').decode()
keys = []
for p in pairs:
    path, _, kid = p.rpartition(':')
    k = serialization.load_pem_private_key(open(path,'rb').read(), password=None)
    n = k.public_key().public_numbers()
    keys.append({"kty":"RSA","use":"sig","alg":"RS256","kid":kid,"n":b64u(n.n),"e":b64u(n.e)})
json.dump({"keys":keys}, open(f"{out}/jwks.json","w"), indent=2)
json.dump({"issuer":issuer,"jwks_uri":f"{issuer}/jwks.json",
           "authorization_endpoint":f"{issuer}/unsupported/authorize",
           "token_endpoint":f"{issuer}/unsupported/token",
           "response_types_supported":["token"],"subject_types_supported":["public"],
           "id_token_signing_alg_values_supported":["RS256"],"grant_types_supported":[],
           "scopes_supported":["openid"]},
          open(f"{out}/openid-configuration","w"), indent=2)
print("   jwks kids: " + ", ".join(k["kid"] for k in keys))
PY
}

publish_to_bucketfs() {  # $1 = dir holding jwks.json + openid-configuration
  local d="$1" pass; pass="${BFSPASS:-$(bfs_password)}"
  exapump bucketfs cp --profile "$EXA_PROFILE" --bfs-write-password "$pass" \
    "$d/jwks.json" "bfs://$BFS_PATH/jwks.json" >/dev/null
  exapump bucketfs cp --profile "$EXA_PROFILE" --bfs-write-password "$pass" \
    "$d/openid-configuration" "bfs://$BFS_PATH/.well-known/openid-configuration" >/dev/null
  sleep 3
}

unpublish_from_bucketfs() {
  local pass; pass="${BFSPASS:-$(bfs_password)}"
  exapump bucketfs rm --profile "$EXA_PROFILE" --bfs-write-password "$pass" \
    "bfs://$BFS_PATH/jwks.json" >/dev/null 2>&1 || true
  exapump bucketfs rm --profile "$EXA_PROFILE" --bfs-write-password "$pass" \
    "bfs://$BFS_PATH/.well-known/openid-configuration" >/dev/null 2>&1 || true
}

# --- the combined catalog ---------------------------------------------------

# The configuration under test, in one place. Primary provider = the customer's
# Keycloak (reserved id `oidc`); secondary provider `exasol` = the engine, served
# from BucketFS over HTTPS with the chain root in the trust store.
start_combined() {
  docker rm -f "$R3_NAME" >/dev/null 2>&1 || true
  docker run -d --name "$R3_NAME" --network "$NET" -p "$R3_PORT:8181" \
    --add-host "$BFS_HOST:$EXA_IP" -v "$R3_ROOT_CA:/ca.pem:ro" -e SSL_CERT_FILE=/ca.pem \
    -e LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure! \
    -e LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
    -e LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
    -e LAKEKEEPER__AUTHZ_BACKEND=openfga \
    -e LAKEKEEPER__OPENFGA__ENDPOINT=http://openfga:8081 \
    -e "LAKEKEEPER__OPENID_PROVIDER_URI=$KC_URI" \
    -e LAKEKEEPER__OPENID_AUDIENCE=lakekeeper \
    -e LAKEKEEPER__OPENID_SUBJECT_CLAIM=sub \
    -e "LAKEKEEPER__OPENID_PROVIDERS__EXASOL__URI=$R3_ISS" \
    -e LAKEKEEPER__OPENID_PROVIDERS__EXASOL__AUDIENCE=lakekeeper \
    -e LAKEKEEPER__OPENID_PROVIDERS__EXASOL__SUBJECT_CLAIMS=sub \
    -e RUST_LOG=info \
    "$LK_IMAGE" serve >/dev/null
  wait_combined
}

wait_combined() {
  local i
  for i in $(seq 60); do
    curl -sf "$R3_LK/health" >/dev/null 2>&1 && return 0
    [ "$(docker inspect -f '{{.State.Running}}' "$R3_NAME" 2>/dev/null)" = "false" ] && break
    sleep 1
  done
  echo "   FATAL: $R3_NAME did not become healthy" >&2
  docker logs "$R3_NAME" 2>&1 | tail -20 >&2
  return 1
}

# Force a JWKS re-fetch. Lakekeeper caches a provider's key set for 1 h and has
# no refresh API, so a restart is the only way to observe the post-refresh state
# inside one test run. Stated as a simulation everywhere it is used.
refetch_jwks() { docker restart "$R3_NAME" >/dev/null; wait_combined; }

authenticators() {
  docker logs "$R3_NAME" 2>&1 \
    | grep -oE '"message":"(Configuring [0-9]+ OIDC provider\(s\)|Creating OIDC authenticator for [a-z0-9-]+ \([^"]*\)|Successfully added OIDC authenticator: [a-z0-9-]+)"' \
    | sed 's/^/     /' | sort -u
}

# Mint an engine token for this round's issuer.
r3_mint() { "$MINT" --iss "$R3_ISS" --sub "$1" "${@:2}"; }

# Admin token for the throwaway catalog. It must be minted INSIDE the compose
# network so its `iss` is the url this catalog discovered (there is no
# ADDITIONAL_ISSUERS shim here — the config under test is the real one).
r3_admin() {
  docker run --rm --network "$NET" curlimages/curl:8.11.1 \
    -sS -X POST "$KC_URI/protocol/openid-connect/token" \
    -d grant_type=client_credentials \
    -d "client_id=$ENGINE_CLIENT_ID" -d "client_secret=$ENGINE_CLIENT_SECRET" \
    | jq -r '.access_token'
}

r3_code() { req_code "$1" GET "$R3_TBL/$2"; }

# alice/bob matrix in one line.
matrix() {  # $1 = label, $2 = token
  printf '     %-28s alice_table %s   bob_table %s\n' "$1" \
    "$(r3_code "$2" alice_table)" "$(r3_code "$2" bob_table)"
}

r3_whoami() { curl -sS -H "Authorization: Bearer $1" "$R3_MGMT/whoami"; }

# JSON body only — `req` prefixes an "HTTP <code>" line, which breaks a jq pipe.
body() {
  local token="$1" method="$2" url="$3"; shift 3
  curl -sS -X "$method" "$url" -H "Authorization: Bearer $token" \
    -H 'Content-Type: application/json' "$@"
}
