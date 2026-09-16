#!/usr/bin/env bash
# Generate the engine's RSA signing key and publish it as a static OIDC issuer
# (discovery document + JWKS) under issuer/www/, which nginx serves at
# http://engine-issuer:8090 inside the compose network.
#
# The private key never leaves keys/ (gitignored). Regenerating rotates the key.
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
KEYS="$HERE/keys"
WWW="$HERE/issuer/www"
ISSUER="${ENGINE_ISSUER_URL:-http://engine-issuer:8090}"
KID="${ENGINE_KEY_ID:-lakehouse-engine-1}"

mkdir -p "$KEYS" "$WWW/.well-known"

if [ ! -f "$KEYS/engine-signing-key.pem" ]; then
  openssl genrsa -out "$KEYS/engine-signing-key.pem" 2048 2>/dev/null
  chmod 600 "$KEYS/engine-signing-key.pem"
  echo "==> generated $KEYS/engine-signing-key.pem"
else
  echo "==> reusing $KEYS/engine-signing-key.pem"
fi

python3 - "$KEYS/engine-signing-key.pem" "$WWW" "$ISSUER" "$KID" <<'PY'
import base64, json, sys
from cryptography.hazmat.primitives import serialization

key_path, www, issuer, kid = sys.argv[1:5]
with open(key_path, "rb") as fh:
    key = serialization.load_pem_private_key(fh.read(), password=None)
pub = key.public_key().public_numbers()

def b64u(i):
    b = i.to_bytes((i.bit_length() + 7) // 8, "big")
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()

jwks = {"keys": [{"kty": "RSA", "use": "sig", "alg": "RS256",
                  "kid": kid, "n": b64u(pub.n), "e": b64u(pub.e)}]}
with open(f"{www}/jwks.json", "w") as fh:
    json.dump(jwks, fh, indent=2)

# Minimal OIDC discovery document. Lakekeeper's JWKS authenticator only needs
# `issuer` and `jwks_uri`; the rest is filler so the document is well-formed.
disc = {
    "issuer": issuer,
    "jwks_uri": f"{issuer}/jwks.json",
    "authorization_endpoint": f"{issuer}/unsupported/authorize",
    "token_endpoint": f"{issuer}/unsupported/token",
    "introspection_endpoint": f"{issuer}/introspect",
    "userinfo_endpoint": f"{issuer}/userinfo",
    "response_types_supported": ["token"],
    "subject_types_supported": ["public"],
    "id_token_signing_alg_values_supported": ["RS256"],
    "grant_types_supported": [],
    "scopes_supported": ["openid"],
    "claims_supported": ["iss", "sub", "aud", "exp", "iat", "azp", "scope",
                         "preferred_username", "email"],
}
with open(f"{www}/.well-known/openid-configuration", "w") as fh:
    json.dump(disc, fh, indent=2)
print(f"==> wrote {www}/jwks.json and {www}/.well-known/openid-configuration (issuer={issuer}, kid={kid})")
PY

# Public key in PEM, handy for pasting into an IdP that takes a static key
# rather than a JWKS URL.
openssl rsa -in "$KEYS/engine-signing-key.pem" -pubout -out "$KEYS/engine-signing-key.pub.pem" 2>/dev/null
echo "==> wrote $KEYS/engine-signing-key.pub.pem"
