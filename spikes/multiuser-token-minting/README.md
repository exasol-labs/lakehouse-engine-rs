# Spike: adapter-minted tokens for per-user catalog authorization

**Recommendation: Option A.** Register the engine as a second trusted OIDC issuer in Lakekeeper and
have the adapter sign a short-lived JWT per query. Skip the exchange entirely. One secret, zero IdP
calls in the query path, nothing Keycloak-specific. The one cost is that engine-asserted principals
land in a separate Lakekeeper identity namespace (`engine~<sub>` rather than `oidc~<sub>`), and a
Lakekeeper role absorbs that at one assignment per person.

Everything below was run live against the stack in this directory on Lakekeeper v0.13.1, Keycloak
26.4.0 / 26.0.7, OpenFGA v1.8.16 and MinIO. Every claim has a transcript in `evidence/`.

Reproduce end to end with `scripts/run-all.sh` (tears down first, so no run inherits state).

---

## 1. Decision matrix

| | **A. Trusted issuer, no exchange** | **B. RFC 8693 external->internal (V2)** | **B-control. Legacy `requested_subject`** | **C. B + `private_key_jwt`** | **D. Per-user vended credentials** |
|---|---|---|---|---|---|
| Works? | **Yes** | Yes, but only after the engine also serves an OAuth2 **introspection** endpoint and a **userinfo** endpoint | Yes, only on preview `token-exchange:v1` + FGAP v1 | Yes | **Yes** |
| Secrets in the CONNECTION | **1** (RSA signing key) | 2 (signing key + client secret) | 1 (client secret) | **1** (one key does client auth *and* the assertion) | none of its own |
| IdP config needed | none | OIDC identity provider with `tokenIntrospectionUrl` + `userInfoUrl`, plus `standard.token.exchange.enabled` on the requesting client | `users-management-permissions`, client `token-exchange` permission, a client policy attached to both | as B, plus `clientAuthenticatorType=client-jwt` and `jwks.url` | none |
| Catalog config needed | `LAKEKEEPER__OPENID_PROVIDERS__<id>__{URI,AUDIENCE,SUBJECT_CLAIMS}` | none beyond the existing Keycloak provider | none | none | `sts-enabled: true` on the warehouse storage profile |
| Engine must serve HTTP | a **static** discovery document + JWKS (two files; any web server, bucket website or sidecar) | a **dynamic** introspection endpoint and userinfo endpoint, answering per token | no | as B | no |
| Query-path round trips | **0** | 3 (adapter->Keycloak, Keycloak->engine introspection, Keycloak->engine userinfo) and one Keycloak user session created per query | 1 | 3 | 1 (folded into `loadTable`) |
| Deprecation risk | **none** (plain OIDC; Lakekeeper's documented multi-provider config) | high: `token-exchange-external-internal` is **EXPERIMENTAL** in 26.4.0 | **terminal**: PREVIEW `token-exchange:v1`, rejected outright on 26.4.0 defaults | same as B | none |
| Portable off Keycloak | **yes** (nothing Keycloak-side is involved) | no (Keycloak-specific external-exchange shape; Auth0/Cognito have no equivalent) | no | no | yes (Lakekeeper + any S3-compatible STS) |
| Identity namespace | `engine~<sub>`, separate from UI grants | `oidc~<sub>`, same as UI grants | `oidc~<sub>` | `oidc~<sub>` | inherits whatever the token resolved to |

---

## 2. Recommendation and the argument against the rest

### Take Option A

The adapter signs a JWT with its own key; Lakekeeper is configured with the engine's issuer as a
second OIDC provider and validates it exactly the way it validates a Keycloak token. Nothing else is
in the path.

* **Zero query-path round trips.** Demonstrated by stopping Keycloak outright and re-running the
  query: `alice_table -> HTTP 200`, `bob_table -> HTTP 404`, while a client-credentials call to
  Keycloak fails to connect (`evidence/option-a.txt`, A.6). Every other option puts the IdP on the
  critical path of every query.
* **One secret.** The signing key. No `client_id`, no `client_secret`, no token endpoint
  (`evidence/option-c.txt`, C.5).
* **Not Keycloak-specific.** Nothing in Option A touches Keycloak. It is a Lakekeeper configuration
  plus an RS256 signature, so it behaves identically with Auth0, Cognito, Entra or no IdP at all.
* **The security model is honest about what it is.** Exasol cannot hand a UDF the user's own OIDC
  token, so the engine asserts the identity and the catalog trusts the engine. Option A says that in
  one line of configuration. The alternatives dress the same trust up as a token exchange without
  adding any verification (see below).

### Against Option B

Option B is not an exchange of a verified assertion. On Keycloak's V2 external-to-internal path the
subject token is **never parsed or verified**. `OIDCIdentityProvider.exchangeExternalTokenV2Impl`
forwards the opaque string to the external issuer's introspection endpoint and then builds the
identity from that issuer's *userinfo* response:

```java
// Supporting only introspection-endpoint validation for now
validateExternalTokenWithIntrospectionEndpoint(tokenExchangeContext);
return exchangeExternalUserInfoValidationOnly(tokenExchangeContext.getEvent(), ...);
```

Measured consequence: with introspection and userinfo stubs that always answer "alice", a subject
token asserting **bob**, the literal string `not-a-jwt-at-all`, and `x.y.z` all return an access
token for **alice** (`evidence/option-b.txt`, B.4). The engine's signature buys nothing. The engine
still has to be fully trusted, so Option B pays three HTTP round trips, a second secret and a
Keycloak user session per query for no additional assurance.

It also costs more surface than Option A, not less: Option A needs two *static* files, Option B needs
a *dynamic* OAuth2 introspection service and a userinfo service inside or beside the adapter, which
is precisely the HTTP server the adapter does not have.

Finally the feature is `EXPERIMENTAL` in 26.4.0 (`TOKEN_EXCHANGE_EXTERNAL_INTERNAL_V2 enabled=true
type=EXPERIMENTAL`, `evidence/option-b.txt` B.0) and exists only in Keycloak.

### Against Option B-control

Confirmed dead, as expected. On 26.4.0 with V2 defaults:

```
requested_subject=alice -> HTTP 400
{"error":"invalid_request","error_description":"Parameter 'requested_subject' is not supported for standard token exchange"}
```

It still works on both 26.0.7 and 26.4.0 **if** `token-exchange:v1` (PREVIEW) and FGAP v1 are
enabled and three admin-API permission objects are wired up
(`scripts/kc-fgap-v1-impersonation.sh`). One further finding: enabling `token-exchange:v1` turns
`TOKEN_EXCHANGE_EXTERNAL_INTERNAL_V2` **off** (`evidence/option-b-control.txt`), so a single server
cannot run the legacy path and Option B's path at the same time. Do not build on it.

### Option C is the right answer to the wrong question

`private_key_jwt` works exactly as hoped: the same registered RSA key authenticates the client and
signs the assertion, so Option B drops from two secrets to one
(`evidence/option-c.txt`, C.1 and C.4; a forged assertion is rejected with `Signature on JWT token
failed validation`, C.3). It makes Option B tolerable but never better than Option A, which already
needs one secret and no exchange. Keep it in the back pocket if a deployment ever insists the
catalog trust only the IdP.

### Take Option D as well

It is orthogonal and it works (below). Adopt it with Option A.

---

## 3. The four questions, answered plainly

### Can we skip the exchange entirely (Option A)? Yes.

Lakekeeper v0.13.1 accepts additional trusted issuers alongside `LAKEKEEPER__OPENID_PROVIDER_URI`.
The exact configuration used:

```
LAKEKEEPER__OPENID_PROVIDER_URI=http://keycloak:8080/realms/iceberg      # still required
LAKEKEEPER__OPENID_ADDITIONAL_ISSUERS=http://localhost:38080/realms/iceberg
LAKEKEEPER__OPENID_AUDIENCE=lakekeeper
LAKEKEEPER__OPENID_SUBJECT_CLAIM=sub
LAKEKEEPER__OPENID_PROVIDERS__ENGINE__URI=http://engine-issuer:8090      # the engine
LAKEKEEPER__OPENID_PROVIDERS__ENGINE__AUDIENCE=lakekeeper
LAKEKEEPER__OPENID_PROVIDERS__ENGINE__SUBJECT_CLAIMS=sub
```

Startup log, verbatim: `Configuring 2 OIDC provider(s)` / `Creating OIDC authenticator for engine
(http://engine-issuer:8090/)` / `Successfully added OIDC authenticator: engine`.

Constraints found in `crates/lakekeeper/src/config.rs`:

* `LAKEKEEPER__OPENID_PROVIDER_URI` must be set whenever `OPENID_PROVIDERS` is used. The extra
  providers are additive, not a replacement.
* The IdP id must match `[a-z0-9-]+`. `oidc` and `kubernetes` are reserved.
* `ADDITIONAL_ISSUERS` only widens the accepted `iss` values of an existing provider. It does **not**
  add a key, so the engine cannot be smuggled in as an extra issuer of the Keycloak provider. Tested
  directly, because that shape would have removed the identity-namespace problem below: a Lakekeeper
  configured with one provider and `ADDITIONAL_ISSUERS=...,http://engine-issuer:8090` answers **401
  Authentication failed** to an engine-minted token while serving a genuine Keycloak token normally
  (`evidence/probe-additional-issuers.txt`). A dedicated provider entry is the only way in.

**How it fetches the key: OIDC discovery only.** `build_oidc_authenticator` constructs
`limes::jwks::JWKSWebAuthenticator::new(provider.uri, Some(Duration::from_hours(1)))`, so the URI
must serve `/.well-known/openid-configuration` and Lakekeeper follows its `jwks_uri`. There is no
static-key and no inline-JWKS option. The engine therefore needs *something* to serve two static
files, refreshed on a 1 hour interval. In this spike that is an nginx container
(`issuer/nginx.conf`); in a real deployment a bucket website, a sidecar or the IdP itself would do.
The adapter itself still needs no HTTP server.

**Mandatory claims** (probed one at a time, `evidence/option-a.txt` A.4):

| claim | required | notes |
|---|---|---|
| `iss` | **yes** | must equal the provider URI or one of its additional issuers. Substituting Keycloak's issuer gives 401 |
| `sub` | **yes** | or whatever `SUBJECT_CLAIMS` names. Default order is `oid`, then `sub`; set it explicitly |
| `aud` | **yes** | must contain `LAKEKEEPER__OPENID_PROVIDERS__ENGINE__AUDIENCE`. Extra audiences are fine (`["lakekeeper","other"]` passes); a wrong or absent `aud` is 401 |
| `exp` | **yes** | absent is 401. Expiry is enforced with a 60 s skew allowance: 30 s past `exp` passes, 61 s does not |
| `azp`, `scope`, `iat`, `nbf`, `jti`, `typ`, `preferred_username` | no | all optional, all inert. `scope=openid` with no `catalog` still passes, because no `__SCOPE` was configured |

Signature verification is real: an RS256 token signed with a different key under the same `kid` is
401, and `alg=none` is 401.

**Does the subject match what the OpenFGA grants reference? No, and this is the one thing to plan
around.** Lakekeeper's user id is `<idp-id>~<subject>` (`IDP_SEPARATOR = '~'`, and the management API
documents the id as "prefixed with `<idp-identifier>~`"). The primary Keycloak provider is the
reserved id `oidc`, so a human logging into the Lakekeeper UI is `oidc~<sub>`, while an
engine-asserted principal under provider id `engine` is `engine~<sub>` even when `sub` is byte-for-byte
identical. Measured: with `select` on `alice_table` granted to `oidc~1111...`, an engine-minted token
for the same `sub` gets **404 on both tables**. After granting `engine~1111...` as well, alice sees
`alice_table` (200) and not `bob_table` (404), and `listTables` is filtered to one row.

**Mitigation, verified:** create a Lakekeeper role, assign both identities to it once, and grant the
role instead of the user. One `select` grant on `alice_table` to the role then serves the Keycloak
login (200) and the engine-minted token (200) while bob stays at 404
(`evidence/option-a.txt`, A.8). The duplication collapses to a single user-to-role assignment per
person, not a second grant per table.

### One secret or two? One, and Option A needs no client credentials at all.

Confirmed. Option A carries only the RSA signing key: no `client_id`, no `client_secret`, no
`oauth2_server_uri`. Option B needs two unless Option C's `private_key_jwt` is used, which folds them
back into one key.

### Does per-user vending work? Yes, and it is scoped tighter than asked.

With `sts-enabled: true` and `flavor: s3-compat` on the warehouse profile, a `loadTable` carrying
`X-Iceberg-Access-Delegation: vended-credentials` returns an STS session whose
`storage-credentials[0].prefix` is the **table's own S3 prefix**, not the warehouse's. Measured with
alice's credentials (`evidence/option-d.txt`):

```
alice creds -> alice_table metadata        exit=0
alice creds -> bob_table metadata          exit=1  Insufficient permissions to access this path
alice creds -> list s3://warehouse/spike/  exit=1  Access Denied
alice creds -> list her own table prefix   exit=0
```

The catalog layer refuses first anyway: `loadTable bob_table` as alice is 404 and the vending call
returns `NoSuchTableException`, so alice can never obtain a credential for bob's data. The warehouse's
own static key remains unscoped and reaches every table, which is the control confirming the scoping
comes from the per-request STS session rather than from the stored credential. Authorization does
extend from the catalog layer to the storage layer with no new credential logic in the engine.

### Is any part of this Keycloak-specific? Only Option B and B-control.

Option A is plain OIDC discovery plus an RS256 signature validated by Lakekeeper. No Keycloak
involvement at any point, demonstrated by running a successful authorized query with Keycloak
stopped. Option D is Lakekeeper plus S3 STS. Both port unchanged to Auth0, Cognito, Entra, or to a
deployment with no interactive IdP at all.

Options B and B-control depend on Keycloak's own external-exchange implementation, its
`subject_issuer` parameter, its identity-provider objects and (for B-control) its FGAP permission
model. Auth0 and Cognito expose no equivalent of external-to-internal token exchange, so choosing B
pins the design to Keycloak.

---

## 4. Reusable artifacts

| Path | What it is |
|---|---|
| `docker-compose.spike.yml` | Self-contained stack: MinIO, the engine's static issuer, Keycloak 26.4.0 (V2 features), OpenFGA + Postgres, Lakekeeper v0.13.1 + Postgres. Profile `legacy` adds Keycloak 26.0.7 and a second 26.4.0 running `token-exchange:v1`. Own network `10.85.0.0/24` and own host ports, so it runs beside the E2E stack |
| `keycloak/realm-iceberg-spike.json` | Realm import: users `alice`/`bob` with pinned UUIDs, pinned service-account UUIDs, clients `lakehouse` (secret), `lakehouse-jwt` (`private_key_jwt`, no secret), `user-cli` (ROPC control), and the `engine` OIDC identity provider |
| `issuer/nginx.conf` | Serves the engine's discovery document and JWKS. Also carries the deliberately fake introspection/userinfo stubs Option B needs |
| `scripts/gen-keys.sh` | Generates the RSA key and writes `jwks.json` + the discovery document. The private key stays in `keys/`, which is gitignored |
| `scripts/mint-jwt.py` | Mints and decodes engine-signed JWTs. `--omit`/`--claim`/`--ttl`/`--key` drive the claim probes |
| `scripts/up.sh`, `down.sh` | Bring-up and teardown (teardown removes volumes) |
| `scripts/provision.sh` | Bootstrap, STS-enabled warehouse, namespace, `alice_table`/`bob_table`, users under both IdP prefixes, and the non-overlapping OpenFGA grants |
| `scripts/option-{a,b,b-control,c,d}.sh` | One script per option, each self-contained against a provisioned stack |
| `scripts/probe-additional-issuers.sh` | Runs a throwaway second Lakekeeper against the same database with one provider only, to test whether `ADDITIONAL_ISSUERS` can carry the engine key |
| `scripts/kc-fgap-v1-impersonation.sh` | The full FGAP v1 wiring the control case needs, written out as four admin-API steps |
| `scripts/run-all.sh` | Teardown, bring-up, provision, all five options, transcripts into `evidence/` |
| `evidence/*.txt` | Captured transcripts backing every claim above |

Lakekeeper OpenFGA configuration is two environment variables, `LAKEKEEPER__AUTHZ_BACKEND=openfga`
and `LAKEKEEPER__OPENFGA__ENDPOINT=http://openfga:8081`, set on **both** `serve` and `migrate`
(`migrate` installs the authorization model, so it must run after OpenFGA is healthy).

Secrets: the private key is generated locally and never committed. The public JWKS and the discovery
document are regenerated by `gen-keys.sh` rather than committed, so nothing in the repo pins a key.

---

## 5. What changes in PI-6547 (paste-ready)

* **Work item "obtain a user-scoped catalog token" drops the token exchange.** The adapter mints and
  signs the token in-process from `ctx.current_user()`. No IdP call, no `requested_subject`, no
  RFC 8693. Remove the Keycloak-version dependency from the epic: this works against any catalog
  configured to trust the engine's issuer.
* **New work item: publish the engine's issuer documents.** Two static files, `/.well-known/openid-configuration`
  and a JWKS, reachable from the Lakekeeper host. Decide the hosting (bucket website, sidecar, or the
  existing IdP) and the key-rotation procedure. Lakekeeper refreshes the JWKS on a 1 hour interval, so
  rotation means publishing the new key, waiting out the refresh window, then retiring the old one.
* **New work item: key management for the signing key.** It becomes the single most sensitive value
  in the CONNECTION object: anyone holding it can assert any identity to the catalog. Decide where it
  lives, who can read it and how it rotates. It replaces, rather than adds to, the OAuth2 client
  secret.
* **CONNECTION surface changes.** `ConnectionCreds` today offers a static `token` or the
  `client_id`/`client_secret`/`oauth2_server_uri`/`scope` client-credentials pair
  (`SuppliedCatalogAuth::{StaticToken, ClientCredentials}`). Add a third mode carrying the signing
  key, the issuer URL and the key id. Keep the existing modes: a deployment whose catalog is not
  configured to trust the engine still needs them.
* **Identity mapping gains a concrete requirement.** The catalog identity is `<idp-id>~<subject>`,
  so the adapter's asserted `sub` and the Lakekeeper IdP id together determine which grants apply.
  Grants made through the Lakekeeper UI (Keycloak-authenticated, `oidc~`) do **not** apply to
  engine-asserted principals. Add a work item for the grant-administration story, with the
  recommended shape being a Lakekeeper role per user holding both identities.
* **Work item 4 (storage-layer authorization) is already satisfied.** Per-user vended credentials
  come back scoped to the table prefix with `sts-enabled: true` on the warehouse and
  `use_vended_credentials = true` on the CONNECTION. No new credential logic is needed in the engine.
  Reduce this item to configuration plus a regression test.
* **Drop the Keycloak-upgrade item.** Nothing in the recommended path needs Keycloak >= 26.2. Keep
  the upgrade on its own merits if the deployment wants it.

---

## 6. Open questions

* **Where the issuer documents get hosted in each target deployment** is not decided here. The spike
  used nginx because it was free; the production answer (bucket website, sidecar, IdP-hosted) affects
  who can reach the JWKS and how rotation works.
* **Key rotation was not exercised.** The 1 hour JWKS refresh interval is read from Lakekeeper's
  source (`JWKSWebAuthenticator::new(uri, Some(Duration::from_hours(1)))`), not measured. Whether a
  two-key JWKS lets a rotation happen with no failed queries is untested.
* **Signing cost in the adapter was not measured.** Only the VS adapter talks to the catalog
  (`ScanSpec` carries no `catalog` block and the scan UDF does no catalog discovery), so exactly one
  token is minted per query and the 0.13 s figure in `evidence/option-a.txt` A.7 is a Python upper
  bound, not the cost of an in-process Rust RS256 signature. Worth a measurement before assuming it
  is free, and worth confirming that the adapter's node clock stays inside the 60 s `exp` skew
  allowance.
* **Whether Databricks-managed Unity Catalog accepts an equivalent trusted issuer** is out of scope
  here (Iceberg REST only) and unknown. If per-user authorization is wanted on the Unity path too, it
  needs its own spike.
* **Does Lakekeeper's `roles_claim` remove the role-assignment step?** `OPENID_PROVIDERS__<id>__ROLES_CLAIM`
  exists and would let the engine assert roles directly in the minted token, which might replace the
  per-user role assignment recommended above. Not tested.
* **Phase 2 was not run.** No adapter code was changed. The in-adapter proof that
  `ctx.current_user()` -> minted token -> catalog call works through a real Exasol query is still
  outstanding; everything up to the catalog call is proven here.
* **The Option B introspection/userinfo stubs are fakes.** They answer "alice" unconditionally. That
  was enough to characterize what Keycloak demands and to show it never reads the subject token, but
  it means Option B has not been observed working against a correct implementation of those two
  endpoints. Given the recommendation is A, that gap was left open deliberately.
