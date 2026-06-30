# Verification Report: add-rest-catalog-oauth-auth

## Bottom Line

**PASS.** All implementation tasks complete, all automated gates green, full scenario
coverage present, code review clean (0 blocker/major findings; worthwhile minor/nit
findings fixed). Ready for `/speq:record add-rest-catalog-oauth-auth`.

| Gate | Result |
|------|--------|
| Build (`make cross-musl-udf-build`, release in `rust:1.92-bookworm`) | ✅ exit 0 (15m 18s) |
| Host tests (`cargo test -p lakehouse-engine --lib`) | ✅ 282 passed, 0 failed |
| E2E (`make test-e2e`, `exasol-e2e` feature, live Exasol+MinIO+REST stack) | ✅ 34 passed (7 + 27), 0 failed |
| Lint (`cargo clippy --all-targets --all-features`) | ✅ 0 warnings/errors |
| Format (`cargo fmt --check`) | ✅ clean |
| Code review | ✅ 0 blocker/major; 4 minor + 2 nit (worthwhile fixed) |

## Scope delivered

- Two new REST-catalog auth modes: static bearer `token` and OAuth2 client-credentials
  (`client_id`/`client_secret` + optional `oauth2_server_uri`, `scope`), injected in
  `build_rest_catalog` via the exact `iceberg-catalog-rest` 0.9.1 prop keys.
- The four static S3 fields (`endpoint`, `region`, `access_key`, `secret_key`) are now
  unconditionally optional; `warehouse` is the only always-required field (fixed the
  pre-existing over-strict `REQUIRED_CRED_KEYS`, now `REQUIRED_KEY`).
- Conditional SigV4 guard: `access_key`/`secret_key`/`region` required when `use_sigv4` is
  true (regardless of `use_vended_credentials`); `endpoint` stays optional.
- SigV4 + catalog-auth rejected explicitly; incomplete OAuth2 rejected naming only the
  missing field.
- Credential safety: secrets never cross the UDF boundary (auth lives only on
  `ConnectionCreds`, never `ScanSpec`); manual `Debug` redacts secret fields;
  `redact_catalog_auth_error` strips token/secret from the one error path that can surface them.
- Full backward compatibility for legacy static-S3 connections (verified by test).

## Scenario Coverage Audit

All 13 scenarios from the plan map to a present, passing test with the exact specified name:

| Scenario | Test | Status |
|----------|------|--------|
| Missing required field rejected listing only names | `missing_warehouse_rejected_s3_not_required` | ✅ |
| Static S3 optional regardless of catalog auth | `s3_fields_optional_when_not_sigv4` | ✅ |
| SigV4 requires access_key/secret_key/region | `sigv4_requires_access_secret_region` | ✅ |
| Static bearer token exposed on creds | `token_exposed_on_creds` | ✅ |
| OAuth2 client creds exposed on creds | `oauth_client_creds_exposed_on_creds` | ✅ |
| Incomplete OAuth2 rejected naming only missing field | `incomplete_oauth_rejected_no_leak` | ✅ |
| Catalog auth + SigV4 mutually exclusive | `sigv4_and_catalog_auth_mutually_exclusive` | ✅ |
| Optional fields default sensibly | `optional_fields_default` | ✅ |
| Token attached to unsigned catalog requests | `build_rest_catalog_sets_token_prop` | ✅ |
| OAuth2 client creds drive client-credentials grant | `build_rest_catalog_sets_credential_and_oauth_props` | ✅ |
| No auth props when none supplied | `build_rest_catalog_no_auth_props_when_no_auth` | ✅ |
| Auth props never in any scan spec | `scan_spec_carries_no_catalog_auth_props` | ✅ |
| Live catalog auth end-to-end | `catalog_token_oauth_auth_resolves_files_e2e` (cloud-e2e) | ✅ present |

## Notes

- The live catalog-auth E2E (`catalog_token_oauth_auth_resolves_files_e2e`) is in the
  **cloud-e2e** suite, which skips when its AWS Glue / OAuth env vars are absent — the
  established cloud-e2e convention (opposite of the local `exasol-e2e` suite, which fails
  when its stack is down). It compiles and follows its siblings' gating exactly; a live
  run requires a catalog-auth-gated REST catalog reachable from the cluster.
- Crate version bumped `lakehouse-engine` 0.14.0 → 0.15.0 (additive, backward-compatible
  feature). `vs-expression` untouched (0.2.0).
- Code-review fixes applied: removed dead `REQUIRED_KEY` redundancy (now used as the single
  source of truth in the validation error), corrected the `has_catalog_auth` doc comment,
  flattened a redundant boolean grouping, and removed an inert second redaction pattern.
