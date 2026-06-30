# Verification Report: change-vended-credentials-auth-orthogonal

**Generated:** 2026-06-30

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Vended S3 credentials now reach the DataFusion scan under every catalog-auth mode (no-auth, static bearer, OAuth2 client-credentials, SigV4), gated solely on `use_vended_credentials`. All host tests, lint, format, the cross-musl `.so` build, and the full Exasol E2E suite are green. |

| Check | Status |
|-------|--------|
| Build (host debug, v0.16.0) | ✓ |
| Tests (host) | ✓ |
| Lint (clippy --all-targets) | ✓ |
| Format (fmt --check) | ✓ |
| UDF `.so` build (cross-musl) | ✓ |
| E2E (Exasol Docker) | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Failed | Ignored |
|------|-----|--------|--------|---------|
| Unit (lakehouse-engine lib) | 298 | 298 | 0 | 0 |
| Unit (vs-expression lib) | 53 | 53 | 0 | 0 |
| Integration (host: build_convention, scan_parquet_pruning, scan_plan_shape, scan_telemetry, micro_bench) | 9 | 9 | 0 | 2 (benches) |
| E2E (`make test-e2e`: e2e_capability_test + e2e_scan_test, threads=1) | 34 | 34 | 0 | 0 |

### Manual Tests

| Test | Command | Result |
|------|---------|--------|
| vended / auth-mode coverage | `cargo test -p lakehouse-engine vended` | ✓ |
| bearer / OAuth2 + scan-spec no-leak | `cargo test -p lakehouse-engine -- oauth bearer scan_spec` | ✓ |
| Live Databricks UC probe (optional 5.1) | n/a | Skipped — no vending catalog available offline (non-blocking per plan) |

## Tool Evidence

### Build

```
Compiling lakehouse-engine v0.16.0
Finished `dev` profile [unoptimized + debuginfo] target(s)
```

### Linter

```
cargo clippy --all-targets  →  Finished, 0 warnings / 0 errors
```

### Formatter

```
cargo fmt --check  →  clean (exit 0)
```

### E2E

```
make test-e2e  →  e2e_capability_test: 7 passed; e2e_scan_test: 27 passed; 0 failed
(cross-musl .so rebuilt in rust:1.92-bookworm; run against live Exasol + Iceberg-REST + MinIO Docker stack)
```

## Scenario Coverage

| Feature | Scenario | Test Name | Passes |
|---------|----------|-----------|--------|
| pushdown-planning-cloud-credentials | Catalog REST requests to Glue are SigV4-signed when enabled | `signed_request_does_not_leak_keys_in_headers` | Pass |
| pushdown-planning-cloud-credentials | Unsigned path unchanged when SigV4+vending both off | `no_vending_no_sigv4_uses_static_storage_unchanged` | Pass |
| pushdown-planning-cloud-credentials | Vended overrides static regardless of auth mode | `vended_overrides_static_across_all_auth_modes` | Pass |
| pushdown-planning-cloud-credentials | Vended extracted on bearer-token path | `bearer_token_path_extracts_vended_from_config` | Pass |
| pushdown-planning-cloud-credentials | Vended extracted on OAuth2 path | `oauth2_path_extracts_vended_credentials` | Pass |
| pushdown-planning-cloud-credentials | Access delegation advertised + vended region adopted | `vended_request_sends_access_delegation_and_adopts_client_region` | Pass |
| pushdown-planning-cloud-credentials | Static creds used when vending disabled | `vending_disabled_uses_static_on_every_mode` | Pass |
| rest-catalog-oauth-auth | Static bearer token attached to unsigned requests | `bearer_token_attached_to_load_table_request` | Pass |
| rest-catalog-oauth-auth | OAuth2 client-credentials grant built from creds | `oauth2_grant_built_from_client_credentials` | Pass |
| rest-catalog-oauth-auth | No auth props when none supplied | `no_auth_load_table_sends_no_authorization` | Pass |
| rest-catalog-oauth-auth | Catalog-auth secrets never in any scan spec | `catalog_auth_secrets_never_in_scan_spec_with_vending` | Pass |
| rest-catalog-oauth-auth (redaction) | Bearer/OAuth2/vended values never in errors | `bearer_and_oauth_secrets_not_in_error_messages`, `vended_sts_values_not_in_error_messages` | Pass |
| rest-catalog-oauth-auth (loadTable URL) | SigV4/Glue uses warehouse ARN directly (no config round-trip) | `sigv4_skips_config_prefix_lookup_uses_warehouse_directly` | Pass |
| rest-catalog-oauth-auth (loadTable URL) | Non-SigV4 uses config `overrides.prefix` when present | `non_sigv4_config_prefix_resolution_uses_config_endpoint` | Pass |
| rest-catalog-oauth-auth (loadTable URL) | Non-SigV4 no-override → empty prefix (`/v1/namespaces/...`), not warehouse | `non_sigv4_no_config_prefix_yields_empty_not_warehouse` | Pass |

## Notes

- **Code review** surfaced one should-fix (a Glue/SigV4 loadTable-URL regression risk from an unconditional `/v1/config` round-trip) and two nice-to-haves (URL-encode the warehouse query param; complete the redaction value set). All three were applied.
- **E2E caught a deeper regression** the unit tests missed: the unified loader's `resolve_load_table_prefix` fell back to the *warehouse* as a URL path segment when a standard REST catalog returns no `overrides.prefix`, producing the malformed `/v1/s3://warehouse//namespaces/...` (HTTP 400). Fixed: the non-SigV4 fallback now returns an empty prefix (matching the crate's prior `RestCatalog` behaviour and the REST `/v1/{prefix?}/namespaces/...` contract); SigV4/Glue keeps the warehouse-ARN prefix. A unit test (`non_sigv4_no_config_prefix_yields_empty_not_warehouse`) now pins this.
- **Dead code removed**: `load_table_signed` and both `use_sigv4` if/else branches in `resolve_file_list` / `resolve_table_schema`; `resolve_table_schema` dropped its unused `storage` param.
- **No-leak guarantee** confirmed airtight across all new paths (static token, OAuth2-obtained token, vended STS values, and the auth identifiers/endpoint/scope) — verified by unit tests and code review.
- Optional ignored live Databricks UC probe (task 5.1) skipped: a vending catalog cannot be provisioned offline (`apache/iceberg-rest-fixture` does not vend). Non-blocking per the plan.
