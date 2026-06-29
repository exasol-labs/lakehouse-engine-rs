# Plan: add-rest-catalog-oauth-auth

## Summary

Add REST-catalog authentication beyond the static-S3 model: a static bearer token and an
OAuth2 client-credentials flow, both threaded into the unsigned catalog build — while making
static S3 credentials unconditionally optional (they are orthogonal to catalog auth) and
preserving exact backward compatibility for existing static-S3 connections.

## Design

### Context

The VS today requires five static-S3 fields (`warehouse`, `endpoint`, `region`, `access_key`,
`secret_key`) in the CONNECTION password and always builds the REST catalog with only `uri`,
`warehouse`, and S3 props. Cloud REST catalogs increasingly require their own auth (bearer
token or OAuth2 client-credentials), and credential vending (`use_vended_credentials`) can
supply S3 credentials at table-load time independently of catalog auth — even an
unauthenticated catalog (e.g. Lakekeeper) can vend. Catalog auth and S3 storage credentials are
therefore fully orthogonal. We must add the two catalog-auth modes, make the four S3 fields
unconditionally optional (fixing a pre-existing over-strictness in `REQUIRED_CRED_KEYS`), and
keep secrets out of every error message — without disturbing the existing static-S3 path or the
SigV4 / vended-credential machinery.

- **Goals** — support a static `token` and an OAuth2 `client_id`/`client_secret`
  (+optional `oauth2_server_uri`, `scope`) mode; make the four static S3 fields
  UNCONDITIONALLY optional, independent of catalog auth and `use_vended_credentials`; keep
  `warehouse` as the only always-required field; keep token/secret out of all error text; full
  backward compatibility for static-S3 connections.
- **Non-Goals** — no changes to the SigV4 self-signed path internals; no token caching or
  refresh logic of our own (the `iceberg-catalog-rest` client owns the OAuth2 grant); no new
  `ScanSpec`/UDF-boundary fields (the scan UDF never calls the catalog); no Databricks-specific
  auth beyond what the REST props already cover; no coupling of `use_vended_credentials` to the
  catalog-auth mode (it stays an independent flag, default false).

### Decision

Carry the new auth fields on `ConnectionCreds` (parsed in `connection.rs`), keep them entirely
within the planning layer, and inject the corresponding `iceberg-catalog-rest` props inside
`build_rest_catalog` (which already receives `creds` through `resolve_file_list`). Required-field
validation is reduced to `warehouse` only; the four S3 fields become optional. Three catalog-auth
modes are modelled — no-auth, static token, OAuth2 client-credentials — with `oauth2_server_uri`
and `scope` relevant only to the client-credentials mode and optional even there. SigV4 and
catalog-token/OAuth are rejected as a combination at resolution time.

#### Architecture

```
CONNECTION password JSON
        │  parse + validate (mode-aware)            connection.rs
        ▼
  ConnectionCreds { ...static S3..., token?, client_id?, client_secret?,
                    oauth2_server_uri?, scope?, use_sigv4, use_vended_credentials }
        │
        ▼  resolve_file_list(creds)                 pushdown.rs
   ┌────────────────────────┬─────────────────────────────┐
   │ use_sigv4 == true       │ use_sigv4 == false (default) │
   │ self-signed path        │ build_rest_catalog(creds)    │
   │ (token/OAuth REJECTED   │  └─ add token | credential + │
   │  at validation)         │     oauth2-server-uri+scope  │
   └────────────────────────┴─────────────────────────────┘
        ▼
  S3 props in ScanSpec only (vended or static) — NO auth props cross UDF boundary
```

Exact `iceberg-catalog-rest` 0.9.1 prop keys (verified in crate source `src/catalog.rs`):
`"token"`, `"credential"` = `"<client_id>:<client_secret>"`, `"oauth2-server-uri"`, `"scope"`.
These are literal string keys (no exported constants); they flow through
`RestCatalogBuilder::load` because the builder copies every prop except `uri`/`warehouse`.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Auth fields on `ConnectionCreds`, not `CatalogProps`/`StorageProps` | `connection.rs` | Auth is planning-only; must not cross the stateless UDF boundary |
| `warehouse`-only base validation + conditional SigV4 guard | `connection.rs::read_connection` | The four S3 fields are unconditionally optional (orthogonal to catalog auth and vending); `warehouse` is the only unconditionally-required field. When `use_sigv4` is true, `access_key`/`secret_key`/`region` become required (they sign the Glue `load_table` request); `endpoint` stays optional |
| Three-mode catalog auth (none / token / client-credentials) | `pushdown.rs::build_rest_catalog` | `oauth2-server-uri`/`scope` apply only to the credential path and are optional even there; token mode injects only `token` |
| Inject catalog auth props at build time | `pushdown.rs::build_rest_catalog` | Single seam where the unsigned `RestCatalog` is constructed |
| Reject SigV4 + catalog-auth combination | `connection.rs` | The two strategies bypass each other; silent ignore would mislead operators |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Pass `creds` into `build_rest_catalog` to add auth props | Widen `CatalogProps` with auth fields | `CatalogProps` is serialized into `ScanSpec` and crosses the UDF boundary; secrets must not. `creds` is already available in `resolve_file_list`. |
| Make the four S3 fields unconditionally optional, EXCEPT guard SigV4 | Keep them required, or require them only when no catalog auth | Catalog auth and S3 storage credentials are orthogonal: `client.rs:211` `authenticate()` supports a no-auth mode, and `use_vended_credentials` governs S3 vending independently. An unauthenticated catalog can still vend S3 creds, so any catalog-auth-based S3 requirement is wrong. This also fixes pre-existing over-strictness in `REQUIRED_CRED_KEYS`. The single retained conditional is SigV4 (below). |
| When `use_sigv4` is true, require `access_key`/`secret_key`/`region` | Drop all S3 requirements unconditionally | The Glue path signs the `load_table` request with exactly those three fields (`sign_request`, `pushdown.rs:157-164`, service `glue`) BEFORE any vended creds are swapped in. Without this guard a `use_sigv4` connection missing them passes validation and fails later with an opaque signing error — a Glue-path regression. `endpoint` is NOT fed to the signer, so it stays optional. |
| Three catalog-auth modes; `oauth2_server_uri`/`scope` only on the credential path | Always inject `oauth2-server-uri` whenever any auth field is present | `get_token_endpoint()` (`catalog.rs:172`) is read ONLY in `exchange_credential_for_token` (`client.rs:112`); a static `token` is used directly as the bearer header and never consults it. It is also optional (defaults to `{uri}/v1/oauth/tokens`). |
| Reject SigV4 + token/OAuth together | Let SigV4 win and ignore token/OAuth | Silent precedence hides a misconfiguration; an explicit error is safer for an operated engine |
| Use literal prop key strings | Wait for crate to export constants | 0.9.1 exports no constants for these keys; the literals are stable per the Iceberg REST spec and pinned crate version |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/connection-credentials | CHANGED | `specs/_plans/add-rest-catalog-oauth-auth/vs-adapter/connection-credentials/spec.md` |
| vs-adapter/rest-catalog-oauth-auth | NEW | `specs/_plans/add-rest-catalog-oauth-auth/vs-adapter/rest-catalog-oauth-auth/spec.md` |

## Dependencies

- `iceberg-catalog-rest` 0.9.1 (already pinned) — supplies the `token` / `credential` /
  `oauth2-server-uri` / `scope` config props consumed by `RestCatalogBuilder::load`.

## Migration

| Current | New |
|---------|-----|
| `REQUIRED_CRED_KEYS` = `[warehouse, endpoint, region, access_key, secret_key]` (all always required) | `warehouse` always required; `endpoint` always optional; `region`/`access_key`/`secret_key` optional UNLESS `use_sigv4` is true (then required) |
| `ConnectionCreds` static-S3 + flags | adds `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope` (all `Option<String>`) |

Existing static-S3 connections: unchanged — they already supply all five fields (`warehouse`
plus the four S3 fields), so they continue to validate and emit no new props. Loosening the four
S3 fields to optional only widens what is accepted; it never rejects a previously valid password.

## Implementation Tasks

1. **connection.rs — extend `ConnectionCreds`**
   1.1 Add `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope` (all `Option<String>`) to the struct and parse them in `parse_creds`.
   1.2 Add a helper (e.g. `has_catalog_auth`) that reports whether `token` OR (`client_id`/`client_secret`) is present — used only for the SigV4 mutual-exclusivity check, NOT for S3 requiredness.

2. **connection.rs — `warehouse`-only base validation + conditional SigV4 guard in `read_connection`** [expert]
   2.1 Reduce base required-field checking to `warehouse` only. Replace the flat `REQUIRED_CRED_KEYS` with a single `REQUIRED_KEY = "warehouse"` check; the four S3 fields become optional at the base level (parsed via `str_field`, defaulting to empty). Do NOT gate S3 requiredness on catalog auth or `use_vended_credentials`.
   2.2 Conditional SigV4 guard: when `use_sigv4` is true, require `access_key`, `secret_key`, and `region` to be present and non-empty; reject with a credential-safe error naming the missing field(s) and stating they are required when SigV4 signing is enabled. Apply this regardless of `use_vended_credentials` (the three static fields sign `load_table` before vending). `endpoint` is NOT part of this guard — it stays optional. Validate this alongside the SigV4 + catalog-auth mutual-exclusivity check (2.4).
   2.3 Reject incomplete OAuth2 client credentials (exactly one of `client_id`/`client_secret` present) naming only the missing field; never echo values.
   2.4 Reject the SigV4 + catalog-auth combination (`use_sigv4` true AND `has_catalog_auth`) with a credential-safe message.
   2.5 Do NOT couple `use_vended_credentials` to the auth mode — it stays an independent flag defaulting to false.

3. **pushdown.rs — inject catalog auth props in `build_rest_catalog`** [expert]
   3.1 Thread `&ConnectionCreds` into `build_rest_catalog` (callers already have `creds` in `resolve_file_list`; update the three call sites and the dummy-catalog list-namespaces call).
   3.2 Three modes: (a) no token and no client credentials → inject none. (b) `token` present → inject only `"token"`. (c) `client_id`+`client_secret` present → inject `"credential" = "<id>:<secret>"`, plus `"oauth2-server-uri"` ONLY when `oauth2_server_uri` supplied and `"scope"` ONLY when `scope` supplied. token vs client-credentials is mutually exclusive by construction (do not inject `token` in mode c).
   3.3 Ensure `redact_catalog_error` (or equivalent) still strips any auth value that could surface from catalog errors; add the token/secret to the redaction set if needed.

4. **Tests — connection.rs unit tests**
   4.1 Token + warehouse only (S3 omitted) accepted; `token` exposed; `use_vended_credentials` still defaults false; no token leak.
   4.2 OAuth2 client-credentials + warehouse only (S3 omitted) accepted; fields exposed; `oauth2_server_uri`/`scope` absent when omitted; no secret leak.
   4.3 Incomplete OAuth2 (missing one of id/secret) rejected naming only the missing field, no value leak.
   4.4 SigV4 + token (and SigV4 + OAuth) rejected, no value leak.
   4.5 Warehouse-only password (no S3, no auth, `use_sigv4` false) accepted; the four S3 fields default to empty (orthogonality + over-strictness fix). Legacy full static-S3 password still validates and behaves identically (backward-compat guard).
   4.6 Optional-defaults test updated: warehouse-only password leaves the four S3 fields and the five new auth fields absent; `use_sigv4`/`use_vended_credentials` default false.
   4.7 `sigv4_requires_access_secret_region`: `use_sigv4` true with `warehouse` but missing one or more of `access_key`/`secret_key`/`region` is rejected, the error names the missing field(s) and references SigV4, and leaks no value; assert this also fires when `use_vended_credentials` is true; assert a missing `endpoint` alone does NOT trigger rejection under SigV4.

5. **Tests — pushdown.rs unit tests** [expert]
   5.1 `build_rest_catalog` sets `"token"` and none of `"credential"`/`"oauth2-server-uri"`/`"scope"` when token-only.
   5.2 `build_rest_catalog` sets `"credential"` when OAuth, omits `"oauth2-server-uri"`/`"scope"` when those are not supplied, and includes each only when supplied; never sets `"token"`.
   5.3 `build_rest_catalog` sets none of the four auth props when neither token nor client credentials configured (shape-identical to before).
   5.4 Assert no scan spec carries any auth field (guard the UDF-boundary invariant).

6. **E2E — cloud_e2e_test.rs**
   6.1 Add a token/OAuth catalog-auth E2E entry (gated like the existing Glue vended-credential E2E) that resolves a file list against a REST catalog requiring catalog auth; must fail (not skip) when the DB/catalog is unavailable per project rule.

7. **Docs/tracking**
   7.1 Open the GitHub issue (`ghbrk gh issue create`) for the feature and reference it in the implementing commit (`Closes #<n>`).

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | Task 1, Task 3.1 (struct + signature threading) |
| Group B | Task 2 (validation), Task 3.2/3.3 (prop injection) |
| Group C | Task 4, Task 5 (unit tests) |
| Group D | Task 6 (E2E), Task 7 (tracking) |

Sequential dependencies:
- Group A → Group B (validation and prop injection depend on the new fields/signature)
- Group B → Group C (tests assert the implemented behaviour)
- Group C → Group D (E2E after units green)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Const | `crates/lakehouse-engine/src/adapter/connection.rs::REQUIRED_CRED_KEYS` | The four S3 keys are no longer required; only `warehouse` is. Replace with a single `warehouse` check (or a one-element required list) — the five-element always-required list over-constrains. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| connection-credentials / Connection password missing required credential fields is rejected listing only the field names | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `missing_warehouse_rejected_s3_not_required` |
| connection-credentials / Static S3 credentials are optional regardless of catalog auth mode | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `s3_fields_optional_when_not_sigv4` |
| connection-credentials / When SigV4 is enabled, access_key, secret_key, and region are required | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `sigv4_requires_access_secret_region` |
| connection-credentials / Static bearer token is exposed on the resolved credentials | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `token_exposed_on_creds` |
| connection-credentials / OAuth2 client credentials are exposed on the resolved credentials | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `oauth_client_creds_exposed_on_creds` |
| connection-credentials / Incomplete OAuth2 client credentials are rejected naming only the missing field | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `incomplete_oauth_rejected_no_leak` |
| connection-credentials / Catalog token/OAuth auth and SigV4 are mutually exclusive | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `sigv4_and_catalog_auth_mutually_exclusive` |
| connection-credentials / Optional credential fields default sensibly | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `optional_fields_default` |
| rest-catalog-oauth-auth / Static bearer token is attached to unsigned catalog requests | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `build_rest_catalog_sets_token_prop` |
| rest-catalog-oauth-auth / OAuth2 client credentials drive the catalog client-credentials grant | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `build_rest_catalog_sets_credential_and_oauth_props` |
| rest-catalog-oauth-auth / No catalog auth props are set when neither token nor OAuth credentials are supplied | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `build_rest_catalog_no_auth_props_when_no_auth` |
| rest-catalog-oauth-auth / Catalog auth props are never placed in any scan spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `scan_spec_carries_no_catalog_auth_props` |
| rest-catalog-oauth-auth (live catalog auth, end to end) | Integration (E2E) | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `catalog_token_oauth_auth_resolves_files_e2e` |

Unit tests are justified: credential parsing/validation and catalog-prop mapping are pure
computation over JSON with no I/O. The one I/O-bearing scenario (real catalog auth handshake) is
covered by the E2E integration test, which must fail (not skip) when no DB/catalog is available.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| connection-credentials | `cargo test -p lakehouse-engine adapter::connection` | All connection-credential unit tests pass, including the new token/OAuth/mutual-exclusivity cases |
| rest-catalog-oauth-auth | `cargo test -p lakehouse-engine adapter::pushdown build_rest_catalog` | Catalog-prop mapping tests pass (token, OAuth, none) |
| rest-catalog-oauth-auth | `make test-e2e` (with a token/OAuth REST catalog reachable) | E2E query against the catalog-auth virtual schema resolves files and returns rows |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
