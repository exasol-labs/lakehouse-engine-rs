#!/usr/bin/env bash
# Round 2, candidate 2b (portable variant): make "alice is alice" true by giving
# Lakekeeper ONE provider that trusts two issuers and one key set.
#
#   provider URI      = an engine-published discovery document (object storage, 1b)
#   its JWKS          = the engine's key  +  every key of the customer's IdP
#   ADDITIONAL_ISSUERS= the customer's IdP issuer
#
# Because it is one provider, its idp-id is `oidc` for BOTH token sources, so a
# genuine Keycloak token and an engine-minted token resolve to the SAME
# principal `oidc~<sub>` and the customer's existing grants apply unchanged.
#
# Runs against a THROWAWAY Lakekeeper on the same database so the live service
# (which the other scripts use) is never reconfigured.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"
source "$SPIKE_DIR/.spike-state"

KEYS="$SPIKE_DIR/keys"
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"; docker rm -f lk-merged >/dev/null 2>&1 || true' EXIT
MERGED_ISSUER="http://minio:9000/engine-merged"
KC_ISSUER="$KC_URI"
PROBE="http://localhost:38182"

say "2b.1  Build the merged key set"
curl -sS "$KC/realms/$REALM/protocol/openid-connect/certs" -o "$WORK/kc-jwks.json"
note "customer IdP publishes $(jq '.keys|length' "$WORK/kc-jwks.json") keys"
python3 - "$KEYS/engine-signing-key.pem" "$WORK" "$MERGED_ISSUER" <<'PY'
import base64, json, sys
from cryptography.hazmat.primitives import serialization
key_path, work, issuer = sys.argv[1:4]
key = serialization.load_pem_private_key(open(key_path,'rb').read(), password=None)
pub = key.public_key().public_numbers()
b64u = lambda i: base64.urlsafe_b64encode(i.to_bytes((i.bit_length()+7)//8,'big')).rstrip(b'=').decode()
engine = {"kty":"RSA","use":"sig","alg":"RS256","kid":"lakehouse-engine-1","n":b64u(pub.n),"e":b64u(pub.e)}
kc = json.load(open(f"{work}/kc-jwks.json"))["keys"]
json.dump({"keys":[engine]+kc}, open(f"{work}/jwks.json","w"), indent=2)
json.dump({"issuer":issuer,"jwks_uri":f"{issuer}/jwks.json",
           "authorization_endpoint":f"{issuer}/unsupported/authorize",
           "token_endpoint":f"{issuer}/unsupported/token",
           "response_types_supported":["token"],"subject_types_supported":["public"],
           "id_token_signing_alg_values_supported":["RS256"],"grant_types_supported":[],
           "scopes_supported":["openid"]},
          open(f"{work}/openid-configuration","w"), indent=2)
print(f"   merged JWKS: {len([engine]+kc)} keys (1 engine + {len(kc)} customer IdP)")
PY

say "2b.2  Publish it to object storage (no new component — candidate 1b)"
docker run --rm --network spike-multiuser-token-minting -v "$WORK:/docs:ro" \
  --entrypoint /bin/sh quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z -c '
    mc alias set s3 http://minio:9000 minioadmin minioadmin >/dev/null &&
    mc mb --ignore-existing s3/engine-merged >/dev/null &&
    mc anonymous set download s3/engine-merged >/dev/null &&
    mc cp --attr Content-Type=application/json /docs/jwks.json s3/engine-merged/jwks.json >/dev/null &&
    mc cp --attr Content-Type=application/json /docs/openid-configuration \
       s3/engine-merged/.well-known/openid-configuration >/dev/null &&
    echo "   published s3://engine-merged"'

say "2b.3  Throwaway Lakekeeper: ONE provider, two trusted issuers"
note "OPENID_PROVIDER_URI=$MERGED_ISSUER"
note "OPENID_ADDITIONAL_ISSUERS=$KC_ISSUER"
docker rm -f lk-merged >/dev/null 2>&1 || true
docker run -d --name lk-merged --network spike-multiuser-token-minting -p 38182:8181 \
  -e LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure! \
    -e LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
  -e LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
  -e LAKEKEEPER__AUTHZ_BACKEND=openfga \
  -e LAKEKEEPER__OPENFGA__ENDPOINT=http://openfga:8081 \
  -e "LAKEKEEPER__OPENID_PROVIDER_URI=$MERGED_ISSUER" \
  -e "LAKEKEEPER__OPENID_ADDITIONAL_ISSUERS=$KC_ISSUER" \
  -e LAKEKEEPER__OPENID_AUDIENCE=lakekeeper \
  -e RUST_LOG=info quay.io/lakekeeper/catalog:v0.13.1 serve >/dev/null
for _ in $(seq 40); do
  curl -sf "$PROBE/health" >/dev/null 2>&1 && break; sleep 1
done
note "throwaway catalog health -> $(curl -so /dev/null -w '%{http_code}' "$PROBE/health")"

P2="$PROBE/catalog/v1/$WH_ID"

say "2b.4  Genuine customer token (issuer = Keycloak) "
T_KC=$(user_token_net alice alice)
decode_jwt "$T_KC" | head -20
note "loadTable alice_table -> HTTP $(req_code "$T_KC" GET "$P2/namespaces/$NAMESPACE/tables/alice_table")   (expect 200)"
note "loadTable bob_table   -> HTTP $(req_code "$T_KC" GET "$P2/namespaces/$NAMESPACE/tables/bob_table")   (expect 404)"

say "2b.5  Engine-minted token for the SAME human (issuer = merged doc)"
T_A=$(engine_mint "$ALICE_SUB" --iss "$MERGED_ISSUER")
decode_jwt "$T_A"
note "loadTable alice_table -> HTTP $(req_code "$T_A" GET "$P2/namespaces/$NAMESPACE/tables/alice_table")   (expect 200)"
note "loadTable bob_table   -> HTTP $(req_code "$T_A" GET "$P2/namespaces/$NAMESPACE/tables/bob_table")   (expect 404)"
T_B=$(engine_mint "$BOB_SUB" --iss "$MERGED_ISSUER")
note "bob:  alice_table -> HTTP $(req_code "$T_B" GET "$P2/namespaces/$NAMESPACE/tables/alice_table")   (expect 404)"
note "bob:  bob_table   -> HTTP $(req_code "$T_B" GET "$P2/namespaces/$NAMESPACE/tables/bob_table")   (expect 200)"
note "whoami: $(curl -sS -H "Authorization: Bearer $T_A" "$PROBE/management/v1/whoami" | jq -c '{id,name}')"
note "The engine-minted token resolves to oidc~<sub> — the SAME principal the"
note "customer already granted in the UI. Barrier 2 is gone; grants unchanged."

say "2b.6  Failure mode: what a stale merged JWKS costs"
# Publish a JWKS containing ONLY the engine key, as if the customer's IdP had
# rotated and the merged document had not been refreshed.
python3 - "$WORK" <<'PY'
import json, sys
work = sys.argv[1]
d = json.load(open(f"{work}/jwks.json"))
json.dump({"keys": [k for k in d["keys"] if k.get("kid") == "lakehouse-engine-1"]},
          open(f"{work}/jwks.json", "w"), indent=2)
PY
docker run --rm --network spike-multiuser-token-minting -v "$WORK:/docs:ro" \
  --entrypoint /bin/sh quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z -c '
    mc alias set s3 http://minio:9000 minioadmin minioadmin >/dev/null &&
    mc cp --attr Content-Type=application/json /docs/jwks.json s3/engine-merged/jwks.json >/dev/null'
docker restart lk-merged >/dev/null
for _ in $(seq 40); do curl -sf "$PROBE/health" >/dev/null 2>&1 && break; sleep 1; done
note "engine-minted token   -> HTTP $(req_code "$T_A" GET "$P2/namespaces/$NAMESPACE/tables/alice_table")"
note "GENUINE Keycloak token-> HTTP $(req_code "$T_KC" GET "$P2/namespaces/$NAMESPACE/tables/alice_table")"
cat <<'TXT'
   This is the real cost of 2b: the merged document becomes a hard dependency of
   the customer's OWN clients. If it goes stale or unreachable, Spark and Trino
   stop authenticating (401) — not just the engine. Keeping it fresh is an
   operational task whose failure blast radius is the whole catalog.
TXT
