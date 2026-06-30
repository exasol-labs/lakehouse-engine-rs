# Decision Log: change-vended-credentials-auth-orthogonal

Date: 2026-06-30

## Interview

**Q:** On the unsigned (token/OAuth) path, how broadly should vended-credential extraction be supported?
**A:** The `use_vended_credentials` setting is COMPLETELY ORTHOGONAL to authentication with the Iceberg Catalog. Vended extraction must be auth-mode-agnostic — it applies whenever `use_vended_credentials` is set, irrespective of no-auth / static bearer-token / OAuth2 client-credentials / SigV4. Do NOT scope it to bearer-token only.

**Q:** Should this plan also add a Lakekeeper service to docker-compose for offline/CI vended-credential E2E?
**A:** Out of scope. No Lakekeeper. Do NOT add Lakekeeper or any docker-compose catalog service, and do not mention Lakekeeper anywhere in the plan or spec deltas.

## Design Decisions

### [1] Unify table loading behind one auth-mode-agnostic self-issued `loadTable` GET

- **Decision:** Replace the `use_sigv4` if/else split in `resolve_file_list` with a single `load_table_any_auth` that returns the raw `LoadTableResult`; its auth arm is chosen by catalog-auth mode (SigV4 | Bearer token | OAuth2-grant→bearer | none). The one response feeds both Iceberg file planning and vended extraction.
- **Alternatives:** Keep `RestCatalog::load_table` for unsigned modes and layer vending on top. Rejected: `iceberg-catalog-rest` 0.9.1's `load_table` returns only a `Table` and discards the response `config`/`storage_credentials`, with no public hook to recover them — the crate path structurally cannot vend.
- **Rationale:** Self-issuing mirrors the already-shipped SigV4 path and is the only way to surface vended creds uniformly. Makes vending gated solely on `use_vended_credentials`, satisfying the orthogonality principle.
- **Promotes to ADR:** yes

### [2] Perform the OAuth2 client-credentials grant in-adapter

- **Decision:** The adapter issues its own form-encoded `client_credentials` POST (`grant_type`, `client_id`, `client_secret`, optional `scope`) to `oauth2_server_uri` or the catalog default token endpoint and uses the returned `access_token` as the bearer for the self-issued `loadTable` GET.
- **Alternatives:** Reuse `iceberg-catalog-rest`'s internal token cache / `authenticate()`. Rejected: that machinery is `pub(crate)` and bound to the crate's own request pipeline; it cannot authenticate a self-issued request.
- **Rationale:** Orthogonality requires OAuth2 be covered, not deferred. The grant is small and runs once per query (resolve-once), so overhead is negligible.
- **Promotes to ADR:** yes

### [3] Send `X-Iceberg-Access-Delegation: vended-credentials` only when vending

- **Decision:** Attach the access-delegation header to the `loadTable` request only when `use_vended_credentials` is set.
- **Alternatives:** Always send it; never send it. Rejected respectively because (a) it would change the no-vending request shape and (b) spec-compliant catalogs may require it to return vended creds.
- **Rationale:** Databricks ignores the header (responses identical with/without), but compliant catalogs need it. Gating on vending keeps the no-vending path byte-identical and is harmless where ignored.
- **Promotes to ADR:** no

### [4] Surface vended `client.region` into `StorageProps.region`

- **Decision:** When the `loadTable` response config carries `client.region`, set the per-shard scan-spec storage `region` to it; otherwise preserve the static region.
- **Alternatives:** Add a new `ScanSpec`/`StorageProps` field; ignore vended region. Rejected: a new field is unnecessary (reuse existing `region`); ignoring it risks a region mismatch when static region is absent/wrong (Databricks vends e.g. `eu-central-1`).
- **Rationale:** No schema change, correct behaviour for Databricks UC, graceful fallback preserves all existing behaviour.
- **Promotes to ADR:** no

### [5] No token refresh / re-vending for long queries

- **Decision:** Keep resolve-once-per-query; do not implement STS refresh on `s3.session-token-expires-at-ms` expiry.
- **Alternatives:** Refresh vended creds mid-query. Rejected as out of scope.
- **Rationale:** Vended STS lifetime (~1h) far exceeds a single query; refresh would require cross-call state, which the stateless-UDF mission forbids. Documented as a known limitation.
- **Promotes to ADR:** no

### [6] Reconcile the affected specs rather than rewrite

- **Decision:** CHANGE the two vended scenarios in `pushdown-planning-cloud-credentials` to be auth-orthogonal, CHANGE "Unsigned catalog path is unchanged" to hold only when SigV4 AND vending are both off, add NEW per-mode vended scenarios, and CHANGE `rest-catalog-oauth-auth`'s "auth props never in scan spec" scenario so its "vended or static" wording is actually satisfied.
- **Alternatives:** Author a brand-new feature for orthogonal vending. Rejected: the behaviour belongs to the existing cloud-credentials feature; splitting it would fragment the spec library.
- **Rationale:** Keeps each behaviour in its home feature; preserves the backward-compatibility guarantees as explicit clauses.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
