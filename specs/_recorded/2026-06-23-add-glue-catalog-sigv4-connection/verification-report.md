# Verification Report: add-glue-catalog-sigv4-connection

## Bottom Line

**PASS.** All implementation tasks complete; both host and E2E suites fully green.

- Host unit/lib: **157 passed, 0 failed**
- Local E2E (Docker, SLC 0.16.0): **29 passed, 0 failed** (7 capability + 22 scan)
- `cargo clippy --all-targets --all-features`: **clean**
- `cargo fmt --check`: **clean**
- Engine version bumped `0.5.0 → 0.6.0`; `Cargo.lock` in sync.

## Automated Checks

| Step | Command | Result |
|------|---------|--------|
| Build (host debug) | `cargo build -p lakehouse-engine` | Exit 0 |
| Build (UDF .so) | `make cross-musl-udf-build` (via `make test-e2e`) | Exit 0; `.so` rebuilt in rust:1.92-bookworm, v0.6.0 |
| Test (host) | `cargo test -p lakehouse-engine --lib` | 157 passed, 0 failed |
| Test (local E2E) | `make test-e2e` | 29 passed, 0 failed (MAKE_EXIT=0) |
| Lint | `cargo clippy -p lakehouse-engine --all-targets --all-features` | No issues |
| Format | `cargo fmt --check` | Clean |

## Scenario Coverage

| Scenario | Test | Status |
|----------|------|--------|
| Adapter reads catalog + storage creds from a CONNECTION | `connection::tests::read_connection_parses_uri_and_creds` | ✅ |
| Missing connection name rejected, credential-safe | `connection::tests::missing_connection_name_errors` | ✅ |
| Malformed connection password rejected, no leak | `connection::tests::malformed_password_no_leak` | ✅ |
| Missing required fields listed by name only | `connection::tests::missing_required_fields_listed` | ✅ |
| Optional credential fields default sensibly | `connection::tests::optional_fields_default` | ✅ |
| Catalog requests SigV4-signed when enabled | `sigv4::tests::signed_request_carries_sigv4_header` | ✅ |
| Unsigned catalog path unchanged when disabled | `sigv4::tests::disabled_sigv4_produces_unsigned_request` + `pushdown::tests::disabled_sigv4_produces_no_auth_header_in_request` | ✅ |
| Vended S3 creds override static in scan spec | `pushdown::tests::vended_creds_override_static_in_spec` | ✅ |
| Static creds used when vending disabled | `pushdown::tests::vending_disabled_keeps_static_creds` | ✅ |
| Vended-key anchor is the S3 table location (not catalog URI) | `pushdown::tests::extract_vended_keys_anchor_is_s3_table_location_not_catalog_uri` | ✅ |
| `storage_credentials` preferred over `config`; longest-prefix wins | `pushdown::tests::extract_vended_keys_*` | ✅ |
| Scan sizes pool from reported per-instance limit | `scan::mod::tests::session_context_sizes_pool_from_ctx_limit` | ✅ |
| Scan falls back to default budget on 0 limit | `scan::mod::tests::session_context_uses_default_budget_on_zero_limit` | ✅ |
| Credential redaction covers bearer + vended STS keys, all occurrences | `scan::emit::tests::redact_credentials_*` | ✅ |
| Create VS maps Iceberg schema (CONNECTION-migrated) | `e2e_scan_test::create_vs_maps_iceberg_schema` | ✅ (E2E) |
| Create VS unreachable catalog errors, no secret | `e2e_scan_test::create_vs_unreachable_catalog_errors_no_secret` | ✅ (E2E) |
| Capability advertisement + pushdown (CONNECTION-migrated) | `e2e_capability_test::*` (7 tests) | ✅ (E2E) |
| Cloud smoke / perf / vended (Glue) | `cloud_e2e_test::*` | Opt-in; skips without AWS creds (`cloud_test_skips_when_creds_absent` ✅) |

## Notes / Deviations from Plan

- **SLC/SDK coupling (not anticipated in the plan):** bumping `exasol-udf-sdk` 0.14.0 → 0.16.0 changes the UDF ABI fingerprint, so the E2E harness must install the matching **SLC 0.16.0** (`lc-rust-0.16.0`, whose release adds `UdfContext::memory_limit()`). The E2E `SLC_VERSION` constant was bumped 0.14.0 → 0.16.0 in both `e2e_scan_test.rs` and `e2e_capability_test.rs`; `install_slc_0_14` renamed to `install_slc`. Without this, every UDF call (including the adapter `dropVirtualSchema`) fails with `F-UDF-CL-RUST-9001: Fingerprint mismatch`.
- **Code-review fixes applied (R1–R7):** redaction routed through `redact_catalog_error` on the sign + JSON-parse error paths; vended-credential prefix anchor corrected to the S3 table location; `redact_credentials` fixed to redact all occurrences (was an infinite loop that hung the test suite); Glue ARN/config-endpoint simplification documented + pinned by test; inline ponytail risk comments moved to doc blocks; `unsafe` env mutation removed from the cloud skip test; doc typo fixed.

## Manual Testing

| Feature | Command | Result |
|---------|---------|--------|
| connection-credentials + create-virtual-schema (local) | `make test-e2e` (CONNECTION path, `use_sigv4=false`) | VS created + queried against MinIO/REST exactly as before; no credentials in any SQL |
| cloud-e2e-harness (skip) | `cargo test --features cloud-e2e cloud_test_skips_when_creds_absent` | Skips cleanly, no network call |
| cloud-e2e-harness (live Glue) | Requires AWS creds env vars | Not run (no cloud account attached) — opt-in by design |
