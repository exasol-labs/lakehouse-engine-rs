# Tasks: change-vended-credentials-auth-orthogonal

## Phase 2: Implementation (Group A — loader primitives)
- [x] 1.2 Add `oauth2_client_credentials_grant(creds)` — form-encoded `client_credentials` POST → `access_token`; redact `client_secret`/token on every error [expert]
- [x] 1.3 Add `loadTable` prefix resolution: `GET {catalog_uri}/v1/config?warehouse=<warehouse>` → `overrides.prefix`; fall back to warehouse when absent

## Phase 2: Implementation (Group B — unification)
- [x] 1.1 Add `load_table_any_auth(catalog_uri, catalog_props, creds) -> Result<LoadTableResult, UdfError>` selecting auth by mode (SigV4 | Bearer | OAuth2-bearer | none); send `X-Iceberg-Access-Delegation: vended-credentials` when vending; reuse `build_load_table_url` [expert]
- [x] 2.1 Replace `if creds.use_sigv4 {…} else {…}` split in `resolve_file_list` with single path: `load_table_any_auth` → gate vended extraction on `use_vended_credentials` alone → build Table from `result.metadata` via `build_s3_file_io` + `plan_files_from_table` [expert]
- [x] 2.2 Apply vended `client.region` from response config to `effective_storage.region` (only when present); otherwise preserve static region [expert]
- [x] 2.3 Update `resolve_table_schema` to use `load_table_any_auth` for metadata; generalize the SigV4-only branch [expert]

## Phase 2: Implementation (Group C — redaction + tests)
- [x] 3.1 Extend redaction to strip bearer token, OAuth2 access token, vended STS values from every error from `load_table_any_auth` / the grant
- [x] 4.1 Test vended extraction runs on bearer/OAuth2/no-auth when `use_vended_credentials` set; static-only when not
- [x] 4.2 Test auth-mode selection (bearer header / OAuth2 grant / no-auth / SigV4 sign); access-delegation header only when vending
- [x] 4.3 Test `client.region` from config overrides static region; absent → static preserved
- [x] 4.4 Test catalog-auth secrets never appear in any built `ScanSpec`
- [x] 4.5 Test redaction: bearer token, OAuth2 token, vended values never in errors from new paths

## Phase 2: Implementation (Group D — optional, non-blocking)
- [ ] 5.1 (Optional) Env-gated `#[ignore]` live `vended_probe` test against real Databricks UC; skip if env unavailable

## Phase 4: Code Review
- [x] 4.r Review all changed files (guardrails, dead-code removal of `use_sigv4` branches / `load_table_signed`)

## Phase 4: Review Fixes
- [x] R1 (should-fix) Gate `resolve_load_table_prefix` to non-SigV4 modes — restore byte-identical Glue/SigV4 loadTable URL (warehouse ARN used directly, no config round-trip); add guarding test
- [x] R2 (nice-to-have) URL-encode the warehouse query param in the `/v1/config` request
- [x] R3 (nice-to-have) Add `client_id`, `oauth2_server_uri`, `scope` to `redact_catalog_auth_error` value-redaction set so the no-leak guarantee matches its doc

## Phase 5: Verification
- [x] 5.b Build (host debug) — `cargo build` → exit 0 (v0.16.0)
- [x] 5.t Test — `cargo test` → 359 passed, 0 failed (297 lakehouse-engine + 53 vs-expression + integration)
- [x] 5.c Lint — `cargo clippy --all-targets` → 0 warnings
- [x] 5.f Format — `cargo fmt --check` → clean
- [x] 5.x Build UDF .so + E2E — `make test-e2e` → 34/34 passed (7 e2e_capability_test + 27 e2e_scan_test), 0 failed (after R4 fix)

## Phase 5: E2E Regression Fix
- [x] R4 (blocker) `resolve_load_table_prefix` non-SigV4 fallback must be EMPTY prefix (→ `/v1/namespaces/...`), not the warehouse. Warehouse fallback broke standard-REST loadTable URL (`/v1/s3://warehouse//namespaces/...` → HTTP 400). Add unit test for no-override non-SigV4 → empty; re-run `make test-e2e` green.
