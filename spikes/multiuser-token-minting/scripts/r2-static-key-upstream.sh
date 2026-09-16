#!/usr/bin/env bash
# Round 2, candidate 1c: get Lakekeeper to accept a STATIC verification key (or
# an inline JWKS) so the engine never has to publish anything at all.
#
# This script does not patch Lakekeeper — it establishes, from the shipped
# source and the released crates, that the option does not exist today and sizes
# what adding it upstream would cost. No PR is opened.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"

LK_SRC="${LK_SRC:-}"

say "1c.1  Is there a static-key option in any released Lakekeeper?"
# If an inline-JWKS setting existed, this would configure one provider. Measure
# what the newest release actually does with it.
for v in v0.13.1 v0.13.5; do
  note "$v with LAKEKEEPER__OPENID_PROVIDER_JWKS set and no provider URI:"
  name="lk-1c-$$"
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker run -d --name "$name" --network spike-multiuser-token-minting \
    -e LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure! \
    -e LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
    -e LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@lakekeeper-db:5432/postgres \
    -e LAKEKEEPER__AUTHZ_BACKEND=openfga \
    -e LAKEKEEPER__OPENFGA__ENDPOINT=http://openfga:8081 \
    -e 'LAKEKEEPER__OPENID_PROVIDER_JWKS={"keys":[]}' \
    -e LAKEKEEPER__OPENID_AUDIENCE=lakekeeper \
    -e RUST_LOG=info "quay.io/lakekeeper/catalog:$v" serve >/dev/null
  sleep 6
  { docker logs "$name" 2>&1 | grep -o '"message":"[^"]*\(OIDC\|authentication\)[^"]*"' || true; } \
    | head -3 | sed 's/^/     /'
  docker rm -f "$name" >/dev/null 2>&1 || true
done
cat <<'TXT'
   The setting is simply unknown: the catalog starts with OIDC authentication
   disabled rather than with a statically-keyed provider. Every OIDC knob
   Lakekeeper exposes takes a URI (OPENID_PROVIDER_URI,
   OPENID_PROVIDERS__<id>__URI), and each is fed to
   limes::jwks::JWKSWebAuthenticator::new(uri, ttl), which performs OIDC
   discovery over HTTP. There is no _JWKS / _PUBLIC_KEY variant in v0.13.1 or in
   v0.13.5, the newest release, and no upstream issue requests one.
TXT

say "1c.2  Where a static option would have to be added"
cat <<'TXT'
   limes 0.6.0 (github.com/vakamo-labs/limes-rs — same org as Lakekeeper) ships
   exactly three authenticators: JWKSWebAuthenticator, KubernetesAuthenticator,
   AuthenticatorChain. None verifies against a supplied key. It does expose a
   public `Authenticator` trait, which is the seam a static one would plug into.

   Lakekeeper side, all in crates/lakekeeper/src:
     config.rs               add `jwks` / `public_key` to OidcProviderConfig,
                             make `uri` optional, extend validate_openid_provider_ids
     service/authn.rs        build_oidc_authenticator() is the ONE construction
                             site (it already branches on 6 optional settings);
                             add the static arm and a variant to AuthenticatorEnum
     docs + tests            the config test module already covers the structured
                             env parsing, so the new field follows the pattern
TXT

say "1c.3  Effort estimate"
cat <<'TXT'
   limes:      a StaticJwksAuthenticator implementing the existing Authenticator
               trait. The JWT decode/validate path is already written; the new
               type only replaces "fetch and cache the key set" with "hold it".
               ~150-250 lines plus tests.  1-2 days.
   Lakekeeper: config field + validation + one match arm + docs + tests.
               ~100-150 lines.  1 day.
   Review and release: two repos, one maintainer org, no API break (every new
               field is optional and the existing URI path is untouched).
               Realistically 2-6 weeks of calendar time from PR to a released
               image a customer can pull.

   Engineering cost is low. The risk is not the code, it is that the design
   would then depend on an unreleased upstream change, and on the maintainers
   agreeing that a statically configured signing key belongs in a catalog
   (a reasonable person could argue it weakens the "keys come from an IdP"
   invariant). Do not plan a delivery around it; propose it in parallel.
TXT
