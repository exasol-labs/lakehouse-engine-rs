# Plan: change-vended-credentials-auth-orthogonal

## Summary

Make Iceberg REST catalog vended S3 credentials reach the DataFusion scan under every catalog-authentication mode (no-auth, static bearer token, OAuth2 client-credentials, SigV4) by gating extraction solely on `use_vended_credentials`, decoupled from `use_sigv4`. Unify table loading behind a single auth-mode-agnostic self-issued `loadTable` GET whose response feeds both file planning and vended extraction.

## Design

### Context

Today `resolve_file_list` (`crates/lakehouse-engine/src/adapter/pushdown.rs`) extracts vended credentials ONLY on the `use_sigv4` branch. The unsigned branch builds an `iceberg-catalog-rest` `RestCatalog` and calls `catalog.load_table(...)`, which returns just a `Table` and discards the response `config`/`storage_credentials`. So on the no-auth / bearer-token / OAuth2 paths the adapter ships STATIC storage to every scan spec and DataFusion never receives the catalog-vended STS credentials. For Databricks Unity Catalog managed storage — where no usable static S3 creds exist and short-lived STS creds arrive in the `loadTable` response `config` map — this is a hard failure: the scan cannot read data files.

The user's explicit principle: `use_vended_credentials` is COMPLETELY ORTHOGONAL to catalog authentication. It must work on all auth modes, not be coupled to SigV4.

- **Goals** — (1) Vended extraction runs whenever `use_vended_credentials` is set, on every catalog-auth mode. (2) The catalog-auth mode chooses only how the load request is authenticated. (3) When `use_vended_credentials` is false, every path behaves exactly as before (static creds). (4) Catalog-auth secrets never enter any `ScanSpec`; no credential value appears in any returned SQL or error.
- **Non-Goals** — No token refresh / re-vending for long-running queries (resolve-once-per-query stays). No Lakekeeper or any docker-compose catalog service. No new E2E that depends on a vending catalog. No join pushdown or other scope creep. The UDF/scan side (`scan/mod.rs`, `scan/spec.rs`) is unchanged — it already consumes `ScanSpec.storage` including `session_token`.

### Decision

Unify table loading behind one auth-mode-agnostic loader that returns the raw `LoadTableResult`, so file planning AND vended extraction consume the same response on every mode. Vended extraction becomes a single cross-cutting step gated only on `use_vended_credentials`.

#### Architecture

```
resolve_file_list(catalog_uri, catalog_props, storage, creds, filter_json)
        │
        ▼
load_table_any_auth(catalog_uri, catalog_props, creds)  ── chooses auth ──┐
        │  returns raw LoadTableResult                                     │
        │   ┌──────────────┬───────────────┬──────────────┬────────────┐  │
        │   │ SigV4 sign   │ Bearer <token>│ OAuth2 grant  │ no auth    │  │
        │   │ (existing)   │               │ → bearer      │            │  │
        │   └──────────────┴───────────────┴──────────────┴────────────┘  │
        │   + header: X-Iceberg-Access-Delegation: vended-credentials      │
        ▼                                                                  │
  use_vended_credentials ?                                                 │
   ├─ true  → extract_vended_keys(result, table_location)  ◄── reused ─────┘
   │          + client.region from config → storage.region
   │          merge_vended_into_storage(static, vended)    ◄── reused
   └─ false → effective_storage = static storage.clone()
        ▼
  build Table from result.metadata + build_s3_file_io(effective_storage)
        ▼
  plan_files_from_table(...)  →  (files, effective_storage)
```

`effective_storage` flows into every per-shard `ScanSpec.storage` exactly as the SigV4 path already does. The whole branch on `use_sigv4` inside `resolve_file_list` collapses into the unified loader; the existing SigV4 vended-extraction logic (lines ~1352–1396) is the template for all modes.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Single auth-mode-agnostic loader returning raw `LoadTableResult` | `load_table_any_auth` (new), replaces the `use_sigv4` branch in `resolve_file_list` | One response feeds both planning and vending on every mode; eliminates the `RestCatalog`-drops-config problem |
| Self-issued `loadTable` GET on all modes | `load_table_any_auth` | `iceberg-catalog-rest` 0.9.1 `RestCatalog::load_table` returns only a `Table` and drops `config`/`storage_credentials`, so the crate path cannot vend |
| Strategy selection by catalog-auth mode | auth arm inside `load_table_any_auth` | SigV4 sign \| Bearer header \| OAuth2-grant→bearer \| none — orthogonal to vending |
| Reuse proven helpers | `extract_vended_keys`, `merge_vended_into_storage`, `sign_request`, `build_load_table_url`, `build_s3_file_io` | Already correct and unit-tested against the live Databricks shape; no behavioural change |
| Value-based redaction of every secret | `redact_catalog_auth_error` extended to the bearer/OAuth2/vended values | Preserve the no-leak guarantee across the new code paths |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Self-issue the `loadTable` GET on every mode | Keep `RestCatalog::load_table` for unsigned and only add vending on top | The crate's `load_table` returns a `Table` and drops the response `config`/`storage_credentials`; there is no public hook to recover them, so the crate path structurally cannot vend. Self-issuing mirrors the already-shipped SigV4 path. |
| Perform the OAuth2 client-credentials grant in-adapter | Reuse `iceberg-catalog-rest`'s internal token cache | The crate's `HttpClient`/token cache is `pub(crate)` and tied to its own request pipeline; it cannot authenticate a self-issued GET. The grant is a small form-POST (`grant_type=client_credentials`, `client_id`, `client_secret`, optional `scope`) → `access_token`. Resolve-once-per-query makes the extra request negligible. |
| Send `X-Iceberg-Access-Delegation: vended-credentials` only when vending | Always send it / never send it | Databricks ignores it (responses identical), but spec-compliant catalogs may require it to return vended creds. Sending it only when vending keeps the no-vending path byte-identical and is harmless where ignored. |
| Surface `client.region` from vended config into `StorageProps.region` | Ignore it, keep static region | Databricks vends `client.region`; using it avoids a region mismatch when the static region is absent/wrong. Falls back to static region when absent — no regression. No new `ScanSpec` field needed (reuses `StorageProps.region`). |
| No token refresh / re-vending | Refresh STS on expiry | Out of scope and unnecessary: creds are resolved once per query in the planning layer; the `s3.session-token-expires-at-ms` ~1h lifetime far exceeds a single query. Documented as a known limitation in the decision log. |
| Prefix/warehouse resolution via `GET /v1/config?warehouse=` → `overrides.prefix` for Databricks-style endpoints | Hardcode warehouse as the prefix (current Glue assumption) | Databricks loadTable addressing needs the `overrides.prefix` from the config endpoint; the existing `build_load_table_url` already documents this upgrade path. Implement it inside the unified loader so all modes address the table correctly. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning-cloud-credentials | CHANGED | `vs-adapter/pushdown-planning-cloud-credentials/spec.md` |
| vs-adapter/rest-catalog-oauth-auth | CHANGED | `vs-adapter/rest-catalog-oauth-auth/spec.md` |

## Dependencies

- `reqwest` (already a dependency) for the self-issued GET and the OAuth2 grant POST.
- Existing `crate::adapter::sigv4::sign_request`, `extract_vended_keys`, `merge_vended_into_storage`, `build_load_table_url`, `build_s3_file_io`, `redact_catalog_auth_error`, `redact_catalog_error`, `redact_secret_values`.
- No new crates.

## Tracking

- A GitHub issue MUST be created (`ghbrk gh issue create`) at implement time and referenced via `Closes #<n>` in the implementing commit. Not created during planning.

## Implementation Tasks

1. **Loader unification**
   - [ ] 1.1 Add `load_table_any_auth(catalog_uri, catalog_props, creds) -> Result<LoadTableResult, UdfError>` that selects auth by mode: SigV4 (reuse `sign_request`/existing `load_table_signed` body), Bearer `token`, OAuth2-grant-derived bearer, or none; sends `X-Iceberg-Access-Delegation: vended-credentials` when `use_vended_credentials`; reuses `build_load_table_url`. [expert]
   - [ ] 1.2 Add `oauth2_client_credentials_grant(creds) -> Result<String, UdfError>` performing the form-encoded `client_credentials` POST (`grant_type`, `client_id`, `client_secret`, optional `scope`) against `oauth2_server_uri` or the catalog default token endpoint, returning the `access_token`; redact `client_secret`/token on every error. [expert]
   - [ ] 1.3 Add `loadTable` prefix resolution: `GET {catalog_uri}/v1/config?warehouse=<warehouse>` → `overrides.prefix`, used by the unified loader when addressing the table (Databricks-style); fall back to the warehouse as today when absent.

2. **Orthogonal vended extraction in `resolve_file_list`**
   - [ ] 2.1 Replace the `if creds.use_sigv4 { ... } else { ... }` split in `resolve_file_list` with a single path: call `load_table_any_auth`, then gate vended extraction on `use_vended_credentials` alone (run `extract_vended_keys` + `merge_vended_into_storage` for ALL modes); build the Table from `result.metadata` via `build_s3_file_io(effective_storage)` and `plan_files_from_table`. [expert]
   - [ ] 2.2 Apply vended `client.region` from the response config to `effective_storage.region` (only when present); otherwise preserve static region.
   - [ ] 2.3 Update `resolve_table_schema` to use `load_table_any_auth` for metadata (schema resolution reads only `current_schema()`; vended creds do not affect it) so the SigV4-only branch there is also generalized.

3. **Redaction hardening**
   - [ ] 3.1 Extend `redact_catalog_auth_error` (or the error sites in the new loader) to strip the bearer token, the obtained OAuth2 access token, and the vended STS values from every error surfaced by `load_table_any_auth` / the grant.

4. **Unit tests** (see Scenario Coverage)
   - [ ] 4.1 Test vended extraction now runs on bearer-token, OAuth2, and no-auth modes when `use_vended_credentials` is set (reuse `make_load_table_result` + `extract_vended_keys`/`merge_vended_into_storage` patterns); static-only when it is not.
   - [ ] 4.2 Test auth-mode selection: bearer mode sets `Authorization: Bearer`; OAuth2 mode performs the grant; no-auth sends no `Authorization`; SigV4 still signs. Assert the access-delegation header is present only when vending.
   - [ ] 4.3 Test `client.region` from config overrides static region; absent → static preserved.
   - [ ] 4.4 Test catalog-auth secrets (`token`, `client_secret`, `client_id`, `oauth2_server_uri`, `scope`) never appear in any built `ScanSpec`.
   - [ ] 4.5 Test redaction: bearer token, OAuth2 token, and vended values never appear in errors from the new paths.

5. **Optional ignored live test**
   - [ ] 5.1 (Optional, non-blocking) Adapt the stashed `vended_probe` harness into an env-gated `#[ignore]` live test that exercises bearer-token + vended against a real Databricks UC endpoint; secret values never printed. Skip if the env is unavailable.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A (loader primitives) | 1.2, 1.3 |
| Group B (unification) | 1.1, 2.1, 2.2, 2.3 |
| Group C (redaction + tests) | 3.1, 4.1, 4.2, 4.3, 4.4, 4.5 |
| Group D (optional) | 5.1 |

Sequential dependencies:
- Group A → Group B (1.1 uses 1.2's grant and 1.3's prefix resolution; 2.x depend on 1.1)
- Group B → Group C (tests assert the unified behaviour; 3.1 hardens the new error sites)
- Group D independent (optional, may run any time)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Branch | `resolve_file_list` `use_sigv4` if/else in `crates/lakehouse-engine/src/adapter/pushdown.rs` | Collapsed into the unified `load_table_any_auth` path |
| Branch | `resolve_table_schema` `use_sigv4` if/else | Generalized to `load_table_any_auth` |
| Function (maybe) | `load_table_signed` | Fold its body into the SigV4 arm of `load_table_any_auth`; remove if no other caller remains |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Catalog REST requests to Glue are SigV4-signed when enabled | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (tests) | `signed_request_does_not_leak_keys_in_headers` (existing, retained) |
| Unsigned catalog path is unchanged when SigV4 and vending are both disabled | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (tests) | `no_vending_no_sigv4_uses_static_storage_unchanged` |
| Vended S3 credentials override static credentials regardless of catalog auth mode | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (tests) | `vended_overrides_static_across_all_auth_modes` |
| Vended credentials are extracted on the static bearer-token catalog path | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (tests) | `bearer_token_path_extracts_vended_from_config` |
| Vended credentials are extracted on the OAuth2 client-credentials catalog path | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (tests) | `oauth2_path_extracts_vended_credentials` |
| Vended-credentials request advertises access delegation and adopts the vended region | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (tests) | `vended_request_sends_access_delegation_and_adopts_client_region` |
| Static credentials are used for data files when vending is disabled | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (tests) | `vending_disabled_uses_static_on_every_mode` |
| Static bearer token is attached to unsigned catalog requests | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (tests) | `bearer_token_attached_to_load_table_request` |
| OAuth2 client credentials drive the catalog client-credentials grant | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (tests) | `oauth2_grant_built_from_client_credentials` |
| No catalog auth props are set when neither token nor OAuth credentials are supplied | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (tests) | `no_auth_load_table_sends_no_authorization` |
| Catalog auth props are never placed in any scan spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (tests) | `catalog_auth_secrets_never_in_scan_spec_with_vending` |

<!-- Pure planning-layer credential resolution with no DB side effects → unit tests in the adapter module, mirroring the existing `extract_vended_keys` / `signed_request_*` unit tests. A live vending catalog (Databricks UC) cannot be provisioned offline (apache/iceberg-rest-fixture does not vend), so coverage is unit-level plus the optional ignored live probe (Task 5.1). -->

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/pushdown-planning-cloud-credentials | `cargo test -p lakehouse-engine vended` | All vended/auth-mode tests pass; no credential literal in any assertion failure output |
| vs-adapter/rest-catalog-oauth-auth | `cargo test -p lakehouse-engine -- oauth bearer scan_spec` | Bearer/OAuth2 + scan-spec-no-leak tests pass |
| Both (optional live) | `VENDED_PROBE_DSN=... cargo test -p lakehouse-engine -- --ignored vended_probe` | Resolves files against a real Databricks UC endpoint with bearer-token + vended creds; secret values never printed |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build (host debug) | `cargo build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |
| Build (UDF .so, release) | `make cross-musl-udf-build` | Exit 0 (glibc 2.36 `.so` produced) |
