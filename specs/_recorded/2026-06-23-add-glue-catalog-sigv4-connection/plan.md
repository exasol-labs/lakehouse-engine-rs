# Plan: add-glue-catalog-sigv4-connection

## Summary

Enable real testing against cloud Iceberg REST catalogs (AWS Glue) by sourcing catalog
and S3 credentials from an Exasol CONNECTION object, SigV4-signing the catalog REST
requests, applying Iceberg REST vended credentials to data-file access, wiring the real
per-instance memory limit via `ctx.memory_limit()`, and adding an opt-in cloud smoke/perf
E2E test — all on top of the existing thin-VS / disposable-UDF architecture.

## Design

### Context

The engine currently reads S3 + catalog credentials straight from plain VS properties
(`adapter/mod.rs:extract_connection_props`) and talks to a local MinIO + Iceberg REST
stack with unsigned HTTP. To query AWS Glue's Iceberg REST catalog it must (a) keep
credentials out of `CREATE VIRTUAL SCHEMA` text by reading an Exasol CONNECTION,
(b) SigV4-sign catalog requests, and (c) honour Glue's short-lived vended S3 credentials
for data-file access. Separately, the DataFusion memory pool is still hardcoded to the
0-sentinel (`scan/mod.rs:445`) because the SDK accessor was not yet wired.

- **Goals** — CONNECTION-sourced credentials (mirror strata-rs); SigV4-signed Glue catalog
  access; vended-credential data-file access; real `ctx.memory_limit()` budget; one opt-in
  cloud smoke/perf E2E test that skips when creds are absent.
- **Non-Goals** — the accurate `ResourcesExhausted` error message and spill-free chunking
  research (explicit follow-up plan, per `next.md`); changing the aggregate
  partial/merge decomposition; changing the capability set; Databricks/Unity vending
  (strata-rs uses an external binary — not in scope here).

### Decision

#### Architecture

```
CREATE VIRTUAL SCHEMA ... WITH CATALOG_CONNECTION = '<conn>'
        │
        ▼
adapter (createVirtualSchema / pushdown)
  read_connection(ctx, name)  ──►  Resolved { uri, creds: Json }
        │                               (mirror strata-rs read_connection/storage_block)
        ▼
  build signed RestCatalog client (aws-sigv4) when creds.use_sigv4
        │
        ▼
  resolve_file_list / load_table  ──►  when creds.use_vended_credentials:
        │                                extract vended s3.* keys from LoadTableResult
        │                                (storage_credentials → config fallback)
        ▼
  merge_vended_into_storage(static, vended)  ──►  StorageProps in each ScanSpec
        │
        ▼
scan SET UDF (per shard)
  build_session_context(spec, ctx.memory_limit())  ──► 0.6 × limit pool
  build_s3_store(spec.storage)  ──► reads files with vended creds, no re-vend
```

Key seam (from research): `iceberg-catalog-rest` 0.9.1 has **no** SigV4 hook and its
`load_table()` **drops** `storage_credentials`. So the adapter issues the load_table GET
itself with a SigV4-signing `reqwest` client, deserializes the public
`iceberg_catalog_rest::LoadTableResult`, and extracts vended creds — rather than relying
on `RestCatalogBuilder` to sign or vend. Unsigned/non-vended paths keep using the existing
`RestCatalogBuilder` + `OpenDalStorageFactory::S3` flow unchanged.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| CONNECTION → JSON-password credential block | `adapter` (new `connection.rs`) | Mirror strata-rs `read_connection`/`storage_block`; keeps creds out of SQL text |
| Self-signed loadTable GET + `LoadTableResult` parse | `adapter/pushdown.rs` | 0.9.1 has no SigV4 hook and drops `storage_credentials` |
| `merge_vended_into_storage` shape | `adapter` | Vended STS keys override static; endpoint/region/path_style preserved |
| Feature-flag default-off (`use_sigv4`, `use_vended_credentials`) | credential block | Local MinIO/REST stacks behave exactly as before |
| Thread `ctx.memory_limit()` into `build_session_context` | `scan/mod.rs` | Replace 0-sentinel with real per-instance budget |
| Separate `cloud-e2e` cargo feature, skip-when-absent | tests | Opt-in cloud test must NOT change `exasol-e2e` fail-when-down semantics |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Issue signed load_table GET ourselves; parse `LoadTableResult` | `RestCatalogBuilder::with_client` (plain client, no per-request signing; internal dispatch bypasses middleware); fork the crate | 0.9.1 exposes no per-request signing seam and silently drops `storage_credentials`; self-issued signed GET is the only clean in-tree path |
| `aws-sigv4` 1.4.5 + `aws-credential-types` 1.2.14 | `aws-sign-v4` (no declared MSRV); hand-rolled SigV4 | Official, maintained, MSRV 1.91.1 (builds on rustc 1.92); SigV4 canonicalization is error-prone to hand-roll |
| New `connection-credentials` feature; CHANGE existing credential scenarios | Fold into `create-virtual-schema` only | Credential sourcing is a cross-cutting capability used by both entry points; deserves its own feature |
| `cloud-e2e` skips when env creds absent | Reuse `exasol-e2e` (fails when down) | Cloud account is not always attached; opt-in is the user's explicit requirement |
| Bump SDK pin 0.14.0 → 0.16.0 | Stay on 0.14 | `memory_limit()` + `connection()` ship in 0.16; code already targets the 0.16-shape `Value` enum, so the pin is stale |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/connection-credentials | NEW | `vs-adapter/connection-credentials/spec.md` |
| vs-adapter/pushdown-planning | CHANGED | `vs-adapter/pushdown-planning/spec.md` |
| vs-adapter/create-virtual-schema | CHANGED | `vs-adapter/create-virtual-schema/spec.md` |
| datafusion-scan/scan-execution | CHANGED | `datafusion-scan/scan-execution/spec.md` |
| packaging/cloud-e2e-harness | NEW | `packaging/cloud-e2e-harness/spec.md` |

## Dependencies

- Workspace: bump `exasol-udf-sdk` / `exasol-udf-macros` `0.14.0` → `0.16.0`.
- New deps (must co-resolve under rustc 1.92, alongside the iceberg-0.9.1 arrow-57 / workspace
  arrow-58 split and the `fastnum 0.7.4` pin): `aws-sigv4 = "1.4"`, `aws-credential-types = "1.2"`.
  A `reqwest` client is already a dev-dependency; the adapter needs a runtime `reqwest`
  (or reuse of iceberg's) for the self-issued signed load_table GET — confirm the version
  co-resolves before adding.

## Implementation Tasks

1. SDK bump + memory wiring
   - [ ] 1.1 Bump `exasol-udf-sdk`/`exasol-udf-macros` to `0.16.0` in workspace + engine Cargo.toml; `cargo build -p lakehouse-engine` (host debug) and fix any fallout from the 0.15 dead-API removal.
   - [ ] 1.2 Thread `ctx.memory_limit()` from the scan `run()` into `build_session_context` (replace the `scan/mod.rs:445` 0-sentinel; remove the ponytail markers at `scan/mod.rs:438` and `scan/runtime.rs:17`).
   - [ ] 1.3 Unit test: a context reporting a positive limit sizes the pool to 0.6×limit; a 0 limit uses the default budget (extend `scan/runtime.rs` tests + a `build_session_context` seam test).

2. CONNECTION credential source (mirror strata-rs)
   - [ ] 2.1 Add `adapter/connection.rs`: `read_connection(ctx, name) -> Resolved {uri, creds}`, `storage_block(creds)`, `catalog_block(creds)`, with `REQUIRED_CRED_KEYS` = warehouse/endpoint/region/access_key/secret_key and credential-safe errors (never echo the password).
   - [ ] 2.2 Replace `extract_connection_props(&Json)` with a CONNECTION-based path; thread `&dyn UdfContext` into both `handle_create_virtual_schema` and `handle_pushdown_request` so they can call `ctx.connection`. Add `CATALOG_CONNECTION` property handling.
   - [ ] 2.3 Unit tests: missing connection name, malformed password, missing required fields, optional-field defaults — all asserting no credential leak.

3. SigV4 catalog signing
   - [ ] 3.1 Add `aws-sigv4` + `aws-credential-types`; confirm rustc-1.92 co-resolution (lock check, no `cargo update` surprises). [expert]
   - [ ] 3.2 Add a SigV4 request-signing client wrapper (`adapter/sigv4.rs`): given creds + region + `glue` service name, sign a `reqwest` request; keys never logged. [expert]
   - [ ] 3.3 Wire signing into catalog resolution: when `use_sigv4`, route `resolve_table_schema` and `resolve_file_list` catalog/load_table requests through the signing client; otherwise keep the existing unsigned `RestCatalogBuilder` path. [expert]
   - [ ] 3.4 Unit tests: signed request carries an `Authorization`/SigV4 header for the configured region+service; signing keys absent from error/debug output. Disabled path produces an unsigned request.

4. Iceberg REST credential vending
   - [ ] 4.1 When `use_vended_credentials`, issue the signed `load_table` GET, deserialize `iceberg_catalog_rest::LoadTableResult`, and extract vended `s3.access-key-id`/`s3.secret-access-key`/`s3.session-token` from `storage_credentials[*].config` (longest-prefix match) with fallback to the flat `config` map. [expert]
   - [ ] 4.2 Apply `merge_vended_into_storage(static, vended)` so each `ScanSpec.storage` carries the vended keys (static endpoint/region/path_style preserved); resolve-once in the planning layer. [expert]
   - [ ] 4.3 Unit tests: vended keys override static in the scan spec; `storage_credentials` preferred over `config`; vending-disabled path keeps static creds; no credential in error text.
   - [ ] 4.4 Extend `redact_*` coverage (emit.rs / adapter redaction) to the bearer token and vended STS keys.

5. Cloud E2E harness
   - [ ] 5.1 Add `cloud-e2e` cargo feature (distinct from `exasol-e2e`); add `tests/cloud_e2e_test.rs` gated on it.
   - [ ] 5.2 Add env-var discovery + skip-when-absent helper in `tests/common` (returns early/`return` when AWS creds env vars are unset; never fails on absence).
   - [ ] 5.3 Implement the smoke test: create CONNECTION from env, create Glue-backed VS, run projection+filter query, assert row sanity. Mirror existing `tests/common` harness conventions; DSNs include `validateservercertificate=0`.
   - [ ] 5.4 Implement the perf/aggregate smoke: grouped COUNT/SUM, assert non-zero/sane, record wall-clock duration (no hard threshold).
   - [ ] 5.5 Implement the vended-credentials end-to-end assertion (scan reads files via vended creds; no credential in output).

6. Verification
   - [ ] 6.1 `cargo test` (host) green; `cargo clippy --all-targets` and `cargo fmt` clean.
   - [ ] 6.2 `make test-e2e` (local Docker) still green — CONNECTION path works against the MinIO/REST stack with `use_sigv4`/`use_vended_credentials` false.
   - [ ] 6.3 Manual cloud run documented (see Manual Testing) — opt-in, requires AWS creds.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2, 1.3 (SDK bump + memory wiring — independent of credential work) |
| Group B | 2.1, 2.2, 2.3 (CONNECTION source) |
| Group C | 3.1, 3.2 (SigV4 client — depends on deps resolving) |
| Group D | 5.1, 5.2 (cloud feature + skip helper scaffolding) |

Sequential dependencies:
- Group B → 3.3 (signing wires into the CONNECTION-resolved catalog path)
- Group C → 3.3, 3.4
- 3.3 + Group B → 4.1, 4.2, 4.3 (vending uses the signed client and the storage block)
- 4.x → 4.4 (redaction extends the vended-cred fields)
- Group A + Group B + 4.x + Group D → 5.3, 5.4, 5.5 → 6.x

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `adapter/mod.rs::extract_connection_props` (plain-property variant) | Replaced by the CONNECTION-based credential path (2.2) |
| Constants | `adapter/mod.rs` `PROP_ACCESS_KEY`/`PROP_SECRET_KEY`/`PROP_SESSION_TOKEN`/`PROP_S3_ENDPOINT`/`PROP_S3_REGION`/`PROP_CATALOG_URI` | No longer read from VS properties once credentials come from the CONNECTION |
| Comment | `scan/mod.rs:438` and `scan/runtime.rs:17` ponytail markers | Memory accessor is now wired (1.2) |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Adapter reads catalog and storage credentials from a CONNECTION object | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `read_connection_parses_uri_and_creds` |
| Missing connection name is rejected with a clear, credential-safe error | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `missing_connection_name_errors` |
| Malformed connection password is rejected without leaking the password | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `malformed_password_no_leak` |
| Connection password missing required credential fields is rejected listing only the field names | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `missing_required_fields_listed` |
| Optional credential fields default sensibly | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `optional_fields_default` |
| Catalog REST requests to Glue are SigV4-signed when enabled | Unit | `crates/lakehouse-engine/src/adapter/sigv4.rs` | `signed_request_carries_sigv4_header` |
| Unsigned catalog path is unchanged when SigV4 is disabled | Unit | `crates/lakehouse-engine/src/adapter/sigv4.rs` | `disabled_sigv4_produces_unsigned_request` |
| Vended S3 credentials from load_table override static credentials in the scan spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `vended_creds_override_static_in_spec` |
| Static credentials are used for data files when vending is disabled | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `vending_disabled_keeps_static_creds` |
| Scan sizes its memory pool from the reported per-instance limit | Unit | `crates/lakehouse-engine/src/scan/mod.rs` | `session_context_sizes_pool_from_ctx_limit` |
| Scan falls back to the default budget when no memory limit is reported | Unit | `crates/lakehouse-engine/src/scan/runtime.rs` | `build_runtime_env_uses_default_budget_on_zero_limit` (existing, extended) |
| Scan reads data files with vended credentials carried in the scan spec | Integration | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_scan_reads_with_vended_credentials` |
| Create virtual schema maps the Iceberg table schema (CONNECTION + SigV4) | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `create_vs_maps_iceberg_schema` (existing, CONNECTION-migrated) |
| Create virtual schema fails clearly when the catalog is unreachable (no key leak) | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `create_vs_unreachable_catalog_errors_no_secret` (existing, extended) |
| Cloud smoke test queries a real Glue-backed virtual schema | Integration | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_smoke_projection_filter_query` |
| Cloud test skips cleanly when AWS credentials are absent | Integration | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_test_skips_when_creds_absent` |
| Cloud performance smoke records timing and row-count sanity | Integration | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_perf_grouped_aggregate_smoke` |
| Vended credentials are exercised end to end against Glue | Integration | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_scan_reads_with_vended_credentials` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| connection-credentials + create-virtual-schema | Local Docker: `make test-e2e` after migrating the test VS to `CATALOG_CONNECTION` (with `use_sigv4=false`) | VS created and queried against MinIO/REST exactly as before; no credentials in any query text |
| cloud-e2e-harness (smoke) | `AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… GLUE_CATALOG_URI=… cargo test -p lakehouse-engine --features cloud-e2e -- --nocapture` | Smoke + perf tests query the real Glue-backed VS; rows returned; duration printed; no credentials in output |
| cloud-e2e-harness (skip) | `cargo test -p lakehouse-engine --features cloud-e2e` with AWS env vars unset | Cloud tests report skipped (early return), suite passes, no network call attempted |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build (host debug) | `cargo build -p lakehouse-engine` | Exit 0 |
| Build (UDF .so) | `make cross-musl-udf-build` | Exit 0; `.so` rebuilt in rust:1.92-bookworm |
| Test (host) | `cargo test` | 0 failures |
| Test (local E2E) | `make test-e2e` | 0 failures (CONNECTION path against MinIO/REST) |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt` | No changes |
