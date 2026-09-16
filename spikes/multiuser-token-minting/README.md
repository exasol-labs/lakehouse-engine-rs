# Spike: adapter-minted tokens for per-user catalog authorization

**Recommendation: Option A.** Register the engine as a second trusted OIDC issuer in Lakekeeper and
have the adapter sign a short-lived JWT per query. Skip the exchange entirely. One secret, zero IdP
calls in the query path, nothing Keycloak-specific. The one cost is that engine-asserted principals
land in a separate Lakekeeper identity namespace (`engine~<sub>` rather than `oidc~<sub>`), and a
Lakekeeper role absorbs that at one assignment per person.

Everything below was run live against the stack in this directory on Lakekeeper v0.13.1, Keycloak
26.4.0 / 26.0.7, OpenFGA v1.8.16 and MinIO. Every claim has a transcript in `evidence/`.

Reproduce end to end with `scripts/run-all.sh` (tears down first, so no run inherits state).

> **Superseded by round 2.** Sections 1-6 are round 1 and are left intact as the record of how
> we got here. Round 2 (sections 7-14) put Option A against the operability bar and it did not
> clear it: the `engine~<sub>` namespace costs one grant change per role *and* per direct grant,
> per person, not "one role assignment". **Read section 12 for the current recommendation.**

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
| `scripts/r2-hosting.sh` | Barrier 1: publishes the issuer documents to BucketFS and to object storage, boots a throwaway Lakekeeper against each, authorizes real users through the BucketFS-hosted issuer, and carries the two TLS negative controls |
| `scripts/r2-barrier2-roles-claim.sh` | Barrier 2 / candidate 2a: token-asserted roles, with the genuine-token control and the membership control |
| `scripts/r2-merged-jwks.sh` | Candidate 2b: one provider, two issuers, engine key merged with the IdP's, including the stale-JWKS failure mode |
| `scripts/r2-idp-key-registration.sh` | Candidate 1d: registers the engine key as a PASSIVE Keycloak realm key and proves it against a stock single-provider Lakekeeper |
| `scripts/r2-provider-inversion.sh` | Candidate 2c: gives the reserved `oidc` id to the engine and measures what it costs the customer's own clients |
| `scripts/r2-subject-resolution.sh` | Exasol user name -> IdP subject, via the catalog (unreliable) and via the IdP (one one-time grant) |
| `scripts/r2-per-user-connections.sh` | Exasol CONNECTION privilege model, and what identity each per-user credential shape presents |
| `scripts/r2-static-key-upstream.sh` | Candidate 1c: shows the static-key option does not exist in v0.13.1 or v0.13.5 and sizes the upstream change |
| `scripts/run-all.sh` | Teardown, bring-up, provision, all five round-1 options and all eight round-2 probes, transcripts into `evidence/` |
| `evidence/*.txt` | Captured transcripts backing every claim in both rounds (`r2-*.txt` are round 2) |

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

---

# Round 2: can Option A actually be operated?

**Round 1's recommendation does not survive round 2. Reversing it.**

Option A as recommended was: register the engine as a *second* OIDC provider in Lakekeeper. Round 1
counted the cost of the resulting `engine~<sub>` identity namespace as "one role assignment per
person". That count was wrong. A Lakekeeper role can hold both `oidc~alice` and `engine~alice`
(measured — `evidence/r2-barrier2-roles-claim.txt` 2a.4), so a deployment that grants *exclusively*
through roles pays one assignment per role per person. Any direct user grant has to be duplicated
against the second principal as well. So the real count is **one step per role the person holds, plus
one per direct grant** — unbounded, repeated for every new grant forever, and it means writing into
the customer's existing authorization data. That fails success criterion 2 outright and criterion 3
by an unbounded margin.

The rest of this section is what replaces it. The short version:

* **Barrier 1 (hosting) is solvable today with zero new components, two ways** — BucketFS, which is
  already running inside the Exasol cluster (1a), or the object storage the lakehouse data already
  lives in (1b). Both were verified end to end.
* **Barrier 2 (identity) has exactly one shape that works**: the engine's public key must be trusted
  *under the customer's own provider*, so the minted token resolves to `oidc~<sub>`. Three ways to
  arrange that were measured; each trades a different criterion.
* **No option clears all five criteria.** The one that would — Lakekeeper accepting an additional
  static verification key on the existing provider — does not exist yet and is a small upstream
  change. That is the recommendation.
* Per-user CONNECTION objects, counted honestly, are worse than every minting option on every
  criterion. They are not the interim answer.
* The Exasol-name → IdP-subject mapping round 1 called for costs **one one-time IdP grant and zero
  per-user steps** — from the IdP, not from the catalog, which cannot supply it reliably.

Every number below is measured; transcripts are in `evidence/r2-*.txt`.

## 7. The options table

Steps are counted as discrete actions a customer performs. **One-time** = per deployment, done once.
**Per user** = repeated for every human who queries through the engine, forever.

| | New components | Changes to existing grants | One-time steps | **Per-user steps** | IdP feature maturity | Portable off Keycloak |
|---|---|---|---|---|---|---|
| **Round 1 Option A** (second provider, `engine~`) | 1 (a web server) | **yes — every grant mirrored** | 2 | **1 per role + 1 per direct grant** (unbounded) | GA | yes |
| **1a** host docs on BucketFS | **0** | n/a | 2 (publish 2 objects to a public bucket; trust the Exasol chain root) | n/a | GA | yes |
| **1b** host docs on object storage | **0** | n/a | 1 (publish 2 objects, public-read) | n/a | GA | yes |
| **1c** Lakekeeper static key | **0** | n/a | 1 (set one config value) | n/a | **does not exist** | yes |
| **1d** engine key inside the IdP's JWKS | **0** | **none** | 2 | **0** | GA, but self-hosted IdP only | **no** |
| **2a** assert roles in the token (`ROLES_CLAIM`) | — | — | — | — | — | **dead** (see below) |
| **2b** merged JWKS on object storage | 0 objects, **1 refresher** | **none** | 3 | **0** | GA | yes |
| **2c** invert provider roles | 0 | **renames every existing user** | 2 | 0 | GA | yes |
| **Per-user CONNECTION (i)** per-user OAuth client | 0 | **yes — re-grant everything** | 1 | **4** | GA | yes |
| **Per-user CONNECTION (ii)** user's own password | 0 | none | 1 | **2** | **deprecated** (ROPC) | no |
| Subject resolution (needed by every minting option) | 0 | none | 1 (grant `view-users`) | **0**, if Exasol name = IdP name | GA | per-IdP call |

## 8. Barrier 1 — hosting the discovery document and JWKS

`evidence/r2-hosting.txt`.

**1a BucketFS: works, zero new components.** `exapump bucketfs cp` publishes both files, a
`Public = True` bucket serves them anonymously with `Content-Type: application/json`, and Lakekeeper
accepts the BucketFS URL as a provider URI. Verified all the way through authorization against a
single-provider catalog:

```
whoami -> oidc~1111...
alice  alice_table 200  bob_table 404
bob    alice_table 404  bob_table 200
```

BucketFS serves HTTPS only (`HttpPort = 0`), so two conditions apply — and violating either produces
the *same* opaque message, `error sending request for url`, with no TLS detail at any `RUST_LOG`
level:

* **The URL must use a name the certificate covers.** The Docker image's certificate is
  `CN=exacluster.local` with `SAN: *.exacluster.local, exacluster.local`, so `https://exasol:2581/...`
  fails however trust is configured. Measured as negative control 2.
* **Lakekeeper must trust the ROOT of the chain, not the leaf.** BucketFS presents two certificates
  that share the subject `CN=exacluster.local` and both carry `CA:TRUE`. Trusting the leaf is not
  enough. Measured as negative control 1.

The trust itself is ordinary, and this corrects an earlier claim in this report: **Lakekeeper does
honour a CA store** — both `SSL_CERT_FILE` and the system bundle at
`/etc/ssl/certs/ca-certificates.crt` work, confirmed against a separately-signed HTTPS issuer as well
as against BucketFS. The first round-2 pass reported 1a as failing; that was operator error (the leaf
trusted rather than the root, and a hostname outside the SAN), not a Lakekeeper limitation. A
deployment whose Exasol certificate comes from the customer's own PKI needs no special handling at
all when that PKI is already in the catalog host's trust store.

**1b object storage: works, zero new components.** The discovery document and JWKS are two static
objects in the bucket the customer's Iceberg data already lives in, published with
`Content-Type: application/json` and public read. Lakekeeper accepts the bucket URL as a provider
URI and logs `Successfully added OIDC authenticator`. Nothing runs that was not already running.

Two caveats worth stating: the objects must be *publicly* readable (a JWKS is public by design, but
it is still a bucket-policy change on that prefix), and the URL becomes permanent — changing it
invalidates every token in flight.

**1c Lakekeeper static key: does not exist.** Setting `LAKEKEEPER__OPENID_PROVIDER_JWKS` on v0.13.1
and on v0.13.5 (the newest release) is simply ignored; the catalog logs
`Running without OIDC authentication.` Every OIDC knob takes a URI and is fed to
`limes::jwks::JWKSWebAuthenticator::new(uri, ttl)`, which performs OIDC discovery over HTTP. No
upstream issue requests an alternative. Effort estimate in §11.

## 9. Barrier 2 — "alice is not alice"

### 2a: asserting roles in the minted token is dead, not merely awkward

`evidence/r2-barrier2-roles-claim.txt`. This was the obvious idea and it does not work at any level.

With `ROLES_CLAIM=roles` configured on both providers, an engine-minted token for alice carrying
`roles: ["customer-analysts"]` — where that Lakekeeper role holds `select` on `bob_table` — is
refused. So is a **genuine Keycloak token** carrying the same claim, which rules out any suspicion
that the engine-minted provider is treated specially. Assigning `engine~alice` to the role directly
immediately returns 200, so the grant itself is real; only the token-asserted path is inert.

The cause is structural, in both v0.13.1 and v0.13.5:

* `authn.rs` parses `ROLES_CLAIM` and calls `RequestMetadata::set_token_roles()`.
* `RequestMetadata::token_roles()` is the only reader, and it has **no caller anywhere outside its
  own module**.
* `Actor::Role` is constructed in exactly one place (`authn.rs:654`) and only from the
  `x-assume-role-id` request header — never from a token claim.
* `admission.rs` states that admission is "deliberately a distinct layer from ... authorization".

Token roles feed admission, not authorization. No configuration revives this.

### 2c: inverting the provider roles moves the barrier onto the customer

`evidence/r2-provider-inversion.txt`. `oidc` is the reserved idp-id of the *primary* provider
(`config.rs:175` rejects a named provider called `oidc`), so it can be given to the engine by making
the engine's issuer `OPENID_PROVIDER_URI` and demoting the customer's IdP to a named secondary.

Measured: engine-minted tokens then resolve to `oidc~<sub>` and read `alice_table` (200) but not
`bob_table` (404) — exactly right. And the customer's own token now resolves to `keycloak~<sub>`,
`whoami` fails, and alice gets **404 on her own table**. It renames the customer's entire user
population overnight and breaks Lakekeeper UI login, which redirects only to the primary provider.
Rejected: strictly worse than 2b, which unifies the identities without renaming anyone.

### 2b: one provider, two issuers, one merged key set — works, and is portable

`evidence/r2-merged-jwks.txt`. Configure a single provider whose URI is an engine-published
discovery document (hosted per 1b), whose JWKS contains the engine's key **plus every key the
customer's IdP publishes**, with the customer's issuer listed in `ADDITIONAL_ISSUERS`. Because it is
one provider, both token sources get the idp-id `oidc`.

Measured against a throwaway catalog on the same database:

```
genuine Keycloak token   alice_table 200   bob_table 404
engine-minted, alice     alice_table 200   bob_table 404   whoami oidc~1111...
engine-minted, bob       alice_table 404   bob_table 200
```

The engine-minted token resolves to the *same* principal the customer already granted in the UI.
Barrier 2 is gone and not one grant changes.

The cost is the merge. The document is now the only key source the catalog has, so it is a hard
dependency of the customer's own clients. Simulated a stale merge (the customer's IdP rotated, the
merged document did not):

```
engine-minted token    -> 200
GENUINE Keycloak token -> 401
```

Spark and Trino stop authenticating, not just the engine. And keeping it fresh needs something that
runs on a schedule — which is a new component in all but name, so 2b does not actually clear
criterion 1.

### 1d: put the engine's key inside the IdP's own JWKS — works, but pins the IdP

`evidence/r2-idp-key-registration.txt`. Register the engine's key in the customer's Keycloak realm as
an `rsa` key provider with `enabled=true, active=false`. Keycloak publishes it in the realm JWKS with
status `PASSIVE` — advertised for verification, never used to sign anything Keycloak issues. The
engine then signs with `iss` = the customer's own issuer.

Verified against a **stock single-provider Lakekeeper with zero configuration change** — no
`ADDITIONAL_ISSUERS`, no second provider, nothing:

```
alice  alice_table 200  bob_table 404    whoami oidc~1111...
bob    alice_table 404  bob_table 200
unregistered kid                401
```

This is the cleanest result in the whole spike: both barriers gone, no hosting, no catalog config,
no grant changes, zero per-user steps. Two costs, neither small:

* **The IdP holds the engine's private key.** Keycloak's key-provider API has no public-key-only
  import; `providerId: "rsa"` requires a private key plus a certificate. Anyone who can read the
  realm's key material can mint a token for any user of this engine. There is no supported grant that
  would avoid it: `jwt-bearer` returns `unsupported_grant_type`, and standard token exchange rejects
  `requested_subject` ("not supported for standard token exchange").
* **Key import is a self-hosted-IdP capability.** Auth0, Entra ID and Cognito do not let a tenant add
  a signing key to the published JWKS. This design pins the customer to a self-hosted IdP, which is
  what criterion 4 rules out.

## 10. Where the Exasol user name → IdP subject mapping comes from

`evidence/r2-subject-resolution.txt`. Round 1 already established the requirement: A.5 showed the
subject claim alone decides the principal, and section 5 carries it as the "identity mapping gains a
concrete requirement" work item. What round 1 did not do is say where the adapter *gets* the mapping,
or what it costs. That is what this measures.

The adapter knows `ctx.current_user()` (an Exasol user name); it must mint `sub` = the IdP subject.
Two candidate sources.

**The catalog cannot supply it.** `GET /management/v1/user?name=` stores Lakekeeper's *display* name
(the token's `name` claim — a real login shows up as "Carol Clark"), the filter is a case-insensitive
substring search with no exact-match mode (searching `al` returns two users), it requires
catalog-admin privilege (a normal principal gets 403), and a user is invisible until they have logged
into the catalog at least once — measured: `whoami` 404, `listTables` 404, `?name=carol` returns `[]`,
until a self-registration `POST /management/v1/user` creates them. A substring collision would mint a
token for the *wrong* subject, which is a silent authorization bug rather than an error.

**The IdP can, with one one-time step and zero per-user steps.** Granting the engine's *existing*
service account the built-in `realm-management:view-users` role is enough:

```
current_user()=ALICE    -> [{"id":"1111...","username":"alice"}]
current_user()=Alice    -> [{"id":"1111...","username":"alice"}]
current_user()=BOB      -> [{"id":"2222...","username":"bob"}]
current_user()=svc_etl  -> []
```

Exasol folds unquoted identifiers to upper case and Keycloak's lookup is case-insensitive, so no
normalisation step is needed, and `exact=true` gives one deterministic hit or none. A user with no
IdP account resolves to `[]` — the adapter must fail closed with a clear message, never mint and let
the catalog answer 404 "table does not exist".

The honest caveat: this is **0 per-user steps only if the Exasol user name equals the IdP user
name**. Where they differ the deployment needs an explicit mapping and that *is* a per-user step.
The spike cannot decide that for a customer; it depends on how they provision Exasol accounts. The
lookup itself is a per-IdP API call (Keycloak admin API here; Entra Graph, Okta and Auth0 each have
an equivalent) — a small adapter surface, not a portable standard.

## 11. Per-user CONNECTION objects, counted honestly

`evidence/r2-per-user-connections.txt`. Exasol's side is sound. Measured, only one grant form lets a
UDF read a CONNECTION:

```
GRANT CONNECTION LH_CAT_ALICE_EXA TO ALICE_EXA                      -> insufficient privileges
GRANT CONNECTION LH_CAT_ALICE_EXA TO ALICE_EXA WITH ADMIN OPTION    -> insufficient privileges
GRANT ACCESS ON CONNECTION ... FOR SCRIPT SPIKE.CONNPROBE TO ...    -> OK user=ALICE_EXA
ALICE_EXA reaching for BOB_EXA's connection                         -> insufficient privileges (22001)
```

The isolation is real. The problem is what the credential inside the CONNECTION can be:

* **(i) a per-user OAuth client.** Measured: the token is valid, but its `sub` is the service
  account's, so `whoami` 404s and `loadTable alice_table` 404s. Barrier 2 returns in full. **4 steps
  per user** (IdP client, CONNECTION, `GRANT ACCESS ... FOR SCRIPT`, and re-grant every table to the
  new machine principal), plus a repeat on every secret rotation. Violates criterion 2.
* **(ii) the user's own credentials (ROPC).** Measured: `whoami` returns `oidc~1111...`,
  `alice_table` 200, `bob_table` 404 — existing grants apply unchanged. **2 steps per user**, but the
  CONNECTION stores the user's IdP password, and the resource-owner-password grant is deprecated by
  OAuth 2.1, off by default in Entra ID and Okta, and incompatible with the MFA/SSO that is usually
  the whole reason the customer runs an IdP. Violates criterion 4.

So per-user CONNECTIONs are not a safer fallback. They are worse than 1d and 2b on every criterion
except one (they need no engine-side key management at all).

## 12. What to do — and the upstream change that would settle it

**No option clears all five criteria.** Stated plainly, because that is the result:

| | crit 1 no new components | crit 2 no grant changes | crit 3 per-user steps | crit 4 mature + portable | crit 5 trust model |
|---|---|---|---|---|---|
| Round 1 Option A | ✓ once hosted per 1a or 1b | ✗ | ✗ unbounded | ✓ | ✓ |
| 2b merged JWKS (hosted per 1a or 1b) | ✗ (the refresher) | ✓ | ✓ 0 | ✓ | ✓ |
| 1d IdP holds the key | ✓ | ✓ | ✓ 0 | ✗ self-hosted only | ✗ IdP can impersonate the engine |
| Per-user CONNECTION (i) | ✓ | ✗ | ✗ 4 | ✓ | ✓ |
| Per-user CONNECTION (ii) | ✓ | ✓ | ~ 2 | ✗ deprecated grant | ✗ engine stores user passwords |

The three working designs (1d, 2b, and round 1's A) are the same idea with the trust anchored in
three different places. The variant that anchors it in the *catalog* is the one that costs nothing —
and it is the one that does not exist yet:

> **Let a Lakekeeper OIDC provider accept one or more additional, statically configured verification
> keys alongside the ones it fetches from the IdP.**

With that, the customer sets one config value on the provider they already have. The engine's key is
trusted under the customer's own provider id, so minted tokens resolve to `oidc~<sub>`; there is no
document to host, no key set to merge and keep fresh, no private key handed to the IdP, and nothing
IdP-specific. Every criterion is met: 0 new components, 0 grant changes, 0 per-user steps, GA
configuration, portable, and the engine's private key stays where it already is.

**Effort** (`evidence/r2-static-key-upstream.txt`): limes 0.6.0 ships three authenticators
(`JWKSWebAuthenticator`, `KubernetesAuthenticator`, `AuthenticatorChain`) and a public
`Authenticator` trait, which is the seam. A static-key authenticator is ~150-250 lines plus tests
(the JWT decode/validate path already exists; only "fetch and cache the key set" is replaced by
"hold it") — 1-2 days. The Lakekeeper side is a config field, its validation, one match arm in
`build_oidc_authenticator` (which already branches on six optional settings), docs and tests — ~1
day. Both repos are Vakamo Labs, and nothing breaks: every new field is optional and the existing URI
path is untouched. Realistically 2-6 weeks from PR to a released image. No PR opened, as instructed.

### Recommendation

1. **Propose the upstream change now.** It is small, it is additive, and it is the only design that
   meets the brief. Everything else is a workaround for its absence.
2. **Ship 1d as the interim, only for customers running their own Keycloak**, and only with the
   private-key custody written down and accepted. It is the only option that is zero-new-components,
   zero-grant-change and zero-per-user-step today, and it needs no catalog configuration at all.
3. **Do not ship 2b as the default.** It is the portable one, but its failure mode takes down the
   customer's Spark and Trino sessions, not just ours. Keep it as the documented answer for a
   customer on a managed IdP who cannot wait for (1) and accepts operating the refresher.
4. **Do not adopt per-user CONNECTION objects.** They cost more per user than any minting option and
   still fail a criterion.
5. **Regardless of which lands, add the subject-resolution step**: grant the engine's existing IdP
   service account read access to users, resolve `ctx.current_user()` at plan time, and fail closed
   when it does not resolve.

## 13. Day one for a customer

Assumptions: Iceberg on Lakekeeper with their own IdP, existing grants, querying today with Spark or
Trino. Steps marked **(per user)** repeat; everything else is once.

**If the upstream change has landed (the target state):**

1. Generate the engine signing key; store it in the Exasol CONNECTION the engine already uses.
2. Add the engine's public key to the existing OIDC provider's static-key list and restart the
   catalog.
3. Grant the engine's existing IdP service account read access to users.
4. Done. No grant changes, no per-user steps, existing Spark and Trino sessions unaffected.

**Interim, self-hosted Keycloak (1d):**

1. Generate the engine signing key; store it in the engine's CONNECTION.
2. In the customer's realm, add a key provider with `providerId: rsa`, `enabled=true`, `active=false`,
   carrying that key and a matching self-signed certificate. Note the `kid` Keycloak assigns — the key
   is published as `PASSIVE` and is never used to sign anything Keycloak issues.
   *(Gotcha: `parentId` must be the realm's internal id. With the realm name the create returns 201
   and silently does not persist.)*
3. Grant the engine's existing service account `realm-management:view-users`.
4. Nothing changes in Lakekeeper. Nothing changes in the grants. No per-user steps.

**Interim, managed IdP (2b), only with the refresher accepted:**

1. Generate the engine signing key; store it in the engine's CONNECTION.
2. Publish `openid-configuration` + a merged `jwks.json` (engine key + the IdP's current keys) to a
   place the deployment already runs: the object-storage bucket (public-read,
   `Content-Type: application/json`) or a `Public = True` BucketFS bucket. For BucketFS, use a URL
   inside the Exasol certificate's SAN and put that chain's root in the catalog's trust store.
3. Point `LAKEKEEPER__OPENID_PROVIDER_URI` at that document and list the IdP's issuer in
   `LAKEKEEPER__OPENID_ADDITIONAL_ISSUERS`; restart the catalog.
4. Schedule a refresh of the merged JWKS from the IdP, and alert on it. **If it goes stale, the
   customer's own Spark and Trino clients get 401**, not just the engine.
5. Grant the engine's existing service account read access to users.

## 13a. PI-6547, revised (paste-ready, supersedes section 5)

* **Keep** "the adapter mints and signs the token in-process from `ctx.current_user()`. No IdP call,
  no `requested_subject`, no RFC 8693." Round 2 did not disturb this.
* **Keep** the key-management work item: the signing key is the single most sensitive value in the
  CONNECTION, and it replaces rather than adds to the OAuth2 client secret.
* **Keep** the CONNECTION-surface item: `ConnectionCreds` gains a third mode alongside
  `SuppliedCatalogAuth::{StaticToken, ClientCredentials}`, carrying the signing key, the issuer URL
  and the key id. The existing modes stay for deployments whose catalog does not trust the engine.
* **Keep** "work item 4 (storage-layer authorization) is already satisfied" and "drop the
  Keycloak-upgrade item".
* **Replace** "publish the engine's issuer documents" with **"get the engine's public key trusted
  under the customer's own OIDC provider"**. Hosting a discovery document is no longer the goal, it is
  one of three implementations, and the preferred one (a static key on the existing Lakekeeper
  provider) needs no hosting at all. If documents are hosted, both BucketFS and object storage work
  with zero new components; BucketFS needs its certificate chain root in the catalog's trust store and
  a URL inside the certificate's SAN.
* **Replace** the identity-mapping item's recommended shape. "A Lakekeeper role per user holding both
  identities" is withdrawn: it is one grant change per role *and* per direct grant, per person, and it
  writes into the customer's existing authorization data. The requirement is now that the minted token
  must resolve to the customer's own `oidc~<sub>`, so that **no grant changes at all**.
* **New work item: propose the upstream Lakekeeper change** (additional static verification keys on an
  existing provider). ~2-3 days of code across two Vakamo Labs repos, 2-6 weeks to a released image.
  It is on the critical path for the portable answer, so raise it early — and do not plan a date
  around a maintainer decision that has not been asked for yet.
* **Sharpen the existing identity-mapping item with a source and a cost.** Round 1 already required
  the mapping; round 2 says where it comes from: the IdP, not the catalog. One one-time IdP grant
  (`realm-management:view-users` or the equivalent), a per-IdP lookup call at plan time, and a
  fail-closed path when the name does not resolve. Zero per-user steps only where the Exasol user name
  equals the IdP user name — confirm that for the target deployment before committing to the number.
* **New work item: decide the interim.** 1d (engine key registered as a PASSIVE realm key) for
  self-hosted Keycloak, with the private-key custody written down and signed off; 2b (merged JWKS on
  existing object storage) only for a managed IdP, with the refresher and its catalog-wide blast
  radius owned by someone. Per-user CONNECTION objects are not the interim.

---

## 14. Round-2 open questions

* **Will the Lakekeeper maintainers accept a statically configured verification key?** A reasonable
  reviewer could argue it weakens the "keys come from an IdP" invariant. The recommendation depends
  on this and it has not been asked. Do not plan a delivery date around it.
* **Does the Exasol user name equal the IdP user name in the target deployment?** This is the single
  number that decides whether the recommended design is 0 or 1 per-user steps, and it is a property of
  the customer, not of the design. It was not answered here.
* **Key rotation is still unexercised**, now in three places: the engine's own key, the customer
  IdP's keys inside a merged JWKS (2b), and the PASSIVE key inside Keycloak (1d). Whether a two-key
  overlap rotates with zero failed queries is untested in all three.
* **1d was verified on Keycloak only.** Other self-hosted IdPs (Authentik, Zitadel, Ory Hydra) also
  expose key import, but none was measured. The claim "Auth0/Entra/Cognito cannot" is from their
  documented surface, not from a live test against those services.
* **The blast radius of 2b was measured but not bounded.** A stale merged JWKS 401s the customer's
  clients; whether Lakekeeper serves stale-but-valid keys during a fetch failure, or fails closed
  immediately, was not characterized beyond the restart case.
* **Nothing in round 2 touched adapter code either.** The in-adapter proof — `ctx.current_user()` →
  IdP lookup → mint → catalog call inside a real Exasol query — is still outstanding, and it now has
  one more moving part than round 1 assumed (the subject lookup).
* **`x-assume-role-id` was not explored.** It is the only path by which `Actor::Role` is ever
  constructed. It assumes a role the principal is already a member of, so it does not solve barrier 2,
  but it was not measured and may matter for a service-account-shaped design later.
