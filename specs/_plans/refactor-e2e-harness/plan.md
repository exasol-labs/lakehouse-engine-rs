# Plan: refactor-e2e-harness

## Summary

Extract the duplicated E2E harness boilerplate from the seven `exasol-e2e` test binaries (and the
`cloud-e2e` binary) into a shared `common/e2e_harness` module plus a redacting `ExaConn`, deleting
~1,000 lines of copy-paste while preserving every observable test behaviour. Closes #168.

## Design

### Context

The `exasol-e2e` and `cloud-e2e` integration-test binaries under `crates/lakehouse-engine/tests/`
each re-declare the same harness boilerplate: 10 shared constants, `install_slc()`, `exa_conn()`,
`create_schema_and_scripts()`, `create_virtual_schema()`, `explain_virtual_sql()`,
`parse_int`/`parse_numeric`, and the `local_stack_creds`/`local_stack_storage`/`local_stack_catalog`/
`resolve_fixture_files` catalog-inspection helpers. `cloud_e2e_test.rs` re-implements a whole
WebSocket client (`CloudExaConn`) and `encrypt_password`, duplicating `common/exasol_ws.rs`.

The duplication is not uniform. `install_slc`, `exa_conn`, and `create_schema_and_scripts` are
byte-identical everywhere and merge cleanly. `create_virtual_schema` genuinely diverges — five
signatures across the binaries, differing in VS-property sets (`PARALLELISM_FACTOR`,
`JOIN_BROADCAST_MAX_BYTES`), Iceberg namespace, and catalog CONNECTION name. A blind merge would drop
those per-binary properties, so the shared helper must parameterize them.

`resolve_fixture_files` also diverges and is not a clean move. `e2e_int96_timestamp_test.rs` defines it
`async` (driven via `rt.block_on`, line 383); `e2e_positional_deletes_test.rs` defines it synchronous —
it builds its own current-thread runtime and `block_on`s internally, called directly at lines
359/420/621. Each closes over a different fixture-module-local `NAMESPACE`
(`int96_fixtures::NAMESPACE` vs `pos_delete_fixtures::NAMESPACE` — equal string values today, distinct
symbols). The shared helper is therefore `async fn resolve_fixture_files(namespace: &str, table: &str)`
with the namespace passed explicitly: int96 keeps its `rt.block_on` wrapper and gains the `NAMESPACE`
argument; positional_deletes' three call sites move to `rt.block_on(resolve_fixture_files(NAMESPACE,
table))`. `resolve_fixture_files` lives only in int96/positional_deletes — `e2e_scan_test.rs` shares
only `local_stack_creds`/`local_stack_storage`/`local_stack_catalog`.

`CloudExaConn` diverges from `ExaConn` in exactly one load-bearing way: it redacts SQL text and
response bodies from failure output so credential-bearing DDL never leaks SigV4 or vended keys.

- **Goals** — Define the harness once. Preserve every observable test behaviour (query results,
  fail-not-skip locally, skip-when-creds-absent for cloud, no credential leak). Keep per-binary
  variation (VS names, namespaces, extra VS properties, seeding) explicit and local.
- **Non-Goals** — No new test coverage, no product-code change, no change to the `exasol-e2e` /
  `cloud-e2e` feature gates or the `make test-e2e` invocation surface, no reshaping of file-specific
  seed data or assertions.

### Decision

Introduce one shared harness module and fold the cloud WebSocket client into the existing shared one.

#### Architecture

```
                    common/exasol_ws::ExaConn  (redact_sql option)
                         ▲                     ▲
        exasol-e2e binaries (7)          cloud-e2e binary (1)
                         │                     │
   common/e2e_harness ───┤                     └── file-local: CloudEnv, setup_cloud_vs
   (constants, install_slc, exa_conn,          (uses ExaConn; CloudExaConn deleted)
    upload_so, create_schema_and_scripts,
    create_virtual_schema(&VsProps),
    explain_virtual_sql, parse_int/numeric,
    local_stack_creds/storage/catalog,
    resolve_fixture_files)
                         │
   each binary: own OnceLock + thin setup_e2e() + file-local VS_NAME/namespace/seeding
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Shared helper module | `common/e2e_harness.rs` | Single source for byte-identical provisioning helpers + constants |
| Parameter struct (`VsProps`) | `create_virtual_schema(conn, &VsProps)` | Collapse five divergent `create_virtual_schema` signatures into one without dropping per-binary VS properties |
| Opt-in redaction flag | `ExaConn { redact_sql }` | Fold `CloudExaConn` into `ExaConn`; cloud connects in redacting mode, local keeps SQL-in-failures for debuggability |
| Per-binary `OnceLock` + thin `setup_e2e()` | each test binary | Each binary is a separate compilation/process; setup orchestration (waits, seeding, VS-prop choices) stays local and calls shared helpers |

#### VsProps shape

`VsProps { vs_name, namespace, catalog_conn_name (default `LAKEHOUSE_CATALOG_CREDS`),
parallelism_factor: Option<usize>, join_broadcast_max_bytes: Option<&str> }`, constructed via
`VsProps::new(vs_name, namespace)` plus builder setters. `create_virtual_schema` issues the idempotent
`CREATE OR REPLACE CONNECTION`, drops the VS, and emits `CREATE VIRTUAL SCHEMA` with base properties
plus the optional `PARALLELISM_FACTOR` / `JOIN_BROADCAST_MAX_BYTES` clauses. Call-site mapping:

| Binary | `create_virtual_schema` call |
|--------|------------------------------|
| scan / capability / count_distinct | `VsProps::new(VS_NAME, E2E_NAMESPACE)` |
| int96 | `VsProps::new(VS_NAME, NAMESPACE)` |
| positional_deletes | `VsProps::new(vs, NAMESPACE).with_parallelism_factor(f)` |
| join | `VsProps::new(vs, E2E_NAMESPACE).with_join_broadcast_max_bytes(b)` |
| refresh | `VsProps::new(vs, ns).with_catalog_conn_name("REFRESH_CATALOG_CREDS")` |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|-------------------------|-----------|
| One `VsProps` param struct for `create_virtual_schema` | Keep five per-binary variants; macro | Struct with defaults + builder is the minimal shape that preserves every per-binary property set with no behaviour change |
| `redact_sql` opt-in flag on `ExaConn` | Keep a separate `CloudExaConn`; a `RedactingExaConn` newtype | One flag folds ~150 duplicated lines while keeping the local suite's SQL-in-failure debuggability |
| Shared helper always re-issues idempotent `CREATE OR REPLACE CONNECTION` | Track connection creation separately (as join does today) | `CREATE OR REPLACE` is idempotent; re-issuing it is not observable, and folding it removes join's separate `create_connection` |
| Standardize the WebSocket `clientName`/`driverName` label | Parameterize the label | The login label is cosmetic and asserted nowhere; a single value is simplest and behaviour-neutral |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| packaging/e2e-harness | CHANGED | `packaging/e2e-harness/spec.md` |
| packaging/cloud-e2e-harness | CHANGED | `packaging/cloud-e2e-harness/spec.md` |

Behaviour-preserved, no delta (tests refactored, must stay green in `make test-e2e`):
`packaging/e2e-harness-grouped-agg`, `packaging/e2e-harness-grouped-order`,
`packaging/e2e-harness-positional-deletes`, `packaging/int96-timestamp-fixture`,
`packaging/positional-delete-fixtures`.

## Dependencies

None. Pure test-code refactor within the existing crate and its dev-dependencies.

## Migration

| Current | New |
|---------|-----|
| 10 constants + `install_slc`/`exa_conn`/`create_schema_and_scripts` duplicated in 7 binaries | Defined once in `common/e2e_harness`; each binary `use`s them |
| 5 divergent `create_virtual_schema` signatures | One `create_virtual_schema(conn, &VsProps)` |
| `explain_virtual_sql`, `parse_int`/`parse_numeric` duplicated | Shared in `common/e2e_harness` |
| `local_stack_creds`/`local_stack_storage`/`local_stack_catalog` duplicated in scan/int96/positional_deletes | Shared in `common/e2e_harness` |
| `resolve_fixture_files` in int96 (`async`, `rt.block_on`ed) and positional_deletes (sync, own internal runtime) — NOT byte-identical | One `async fn resolve_fixture_files(namespace, table)` in `common/e2e_harness`; positional_deletes' three call sites rewrapped in `rt.block_on(...)` |
| `CloudExaConn` + `encrypt_password` in `cloud_e2e_test.rs` | `common/exasol_ws::ExaConn` with `redact_sql`; `exasol_ws` gate widened to `cloud-e2e` |
| `common/mod.rs` gates `exasol_ws` behind `exasol-e2e` only | `exasol_ws` gated `any(exasol-e2e, cloud-e2e)`; `e2e_harness` added behind `exasol-e2e` |

## Implementation Tasks

1. Shared harness foundation
   - [ ] 1.1 Add `redact_sql` to `common/exasol_ws::ExaConn`: keep `connect(...)` (redact_sql=false) and add a redacting constructor; when set, the `execute()` failure panic omits the SQL statement and the Exasol response body. Scope: redaction governs the `execute()` DDL-failure path only — `query_scalar_i64`/`query_row_count` (their own parse-failure panics at lines 120/128) and the `connect(...)` auth-failure assertion (line 72) stay unredacted, since the cloud suite passes no credential-bearing SQL through them and the login response carries no credential value. Standardize the login `clientName`/`driverName` label. [expert]
   - [ ] 1.2 Widen `exasol_ws` gate in `common/mod.rs` to `any(feature = "exasol-e2e", feature = "cloud-e2e")`.
   - [ ] 1.3 Create `common/e2e_harness.rs` (gated `exasol-e2e`): shared constants, `install_slc`, `exa_conn`, `upload_so`, `create_schema_and_scripts`, `VsProps` + `create_virtual_schema`, `explain_virtual_sql`, `parse_int`/`parse_numeric`, `local_stack_creds`/`local_stack_storage`/`local_stack_catalog`, and `async fn resolve_fixture_files(namespace: &str, table: &str)` (async form matching int96; namespace passed explicitly — no hardcoded module constant); register `pub mod e2e_harness;` in `common/mod.rs`. [expert]
2. Cloud binary migration
   - [ ] 2.1 Delete `CloudExaConn` + `encrypt_password` from `cloud_e2e_test.rs`; switch to `common::exasol_ws::ExaConn` in redacting mode; verify no credential value can reach test output. [expert]
   - [ ] 2.2 Add negative test `cloud_redacting_conn_omits_credentials_on_failure` in `cloud_e2e_test.rs` (gated like the other cloud tests — skip when the Exasol env vars are absent): open a redacting `ExaConn` (`redact_sql=true`), issue a deliberately failing credential-bearing DDL whose SQL text embeds DUMMY sentinel credential values, capture the `execute()` failure via `std::panic::catch_unwind`, and assert the panic message contains neither the SQL text nor the sentinel values — mirrors the no-leak assertion at `e2e_refresh_test.rs:628-632`. [expert]
3. exasol-e2e binary migrations (each: delete local dups, `use common::e2e_harness::*`, adapt the `create_virtual_schema` call site to `VsProps`; keep file-local `VS_NAME`/namespace/seeding/`OnceLock`/assertions)
   - [ ] 3.1 `e2e_scan_test.rs` (+ shares `local_stack_*`/`resolve_fixture_files`)
   - [ ] 3.2 `e2e_capability_test.rs`
   - [ ] 3.3 `e2e_count_distinct_test.rs`
   - [ ] 3.4 `e2e_int96_timestamp_test.rs` (two-tier `setup()`/`setup_full_stack()`; `VsProps::new(VS_NAME, NAMESPACE)`); keep the existing `rt.block_on(resolve_fixture_files(...))` wrapper (line 383) and add `NAMESPACE` as the explicit first argument — no runtime-shape change
   - [ ] 3.5 `e2e_positional_deletes_test.rs` (`with_parallelism_factor`)
     - [ ] 3.5.1 Convert the three synchronous `resolve_fixture_files(table)` call sites (lines 359, 420, 621) to `rt.block_on(resolve_fixture_files(NAMESPACE, table))` now that the shared helper is `async`: delete the file-local sync `resolve_fixture_files` (with its internal `block_on`) and give each of the three calling `#[test]` fns a current-thread runtime to drive the async helper. [expert]
   - [ ] 3.6 `e2e_join_test.rs` (`with_join_broadcast_max_bytes`; drop separate `create_connection`; two VS) [expert]
   - [ ] 3.7 `e2e_refresh_test.rs` (`with_catalog_conn_name("REFRESH_CATALOG_CREDS")`; namespace param)
4. Validation
   - [ ] 4.1 `cargo check --features exasol-e2e` and `cargo check --features cloud-e2e` compile clean.
   - [ ] 4.2 `cargo test --workspace` (e2e off by default) stays green; `cargo clippy --all-targets` and `cargo fmt --check` clean.
   - [ ] 4.3 `make test-e2e` passes if a Docker stack is available.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2 (cloud-facing `ExaConn` + gate) |
| Group B | 1.3 (`e2e_harness` module) |
| Group C | 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7 (independent files) |
| Group D | 2.1, 2.2 (cloud binary + redaction negative test) |

Sequential dependencies:
- Group A → Group D (cloud binary needs the redacting `ExaConn`)
- Group B → Group C (each binary migration needs `e2e_harness`)
- Groups A/B → Group 4 (validation last)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Constants (10) | each of 7 `exasol-e2e` binaries | Moved to `common/e2e_harness` |
| `install_slc`, `exa_conn`, `create_schema_and_scripts` | each of 7 binaries | Moved to `common/e2e_harness` |
| `create_virtual_schema` (5 variants), join's `create_connection` | 7 binaries | Replaced by `create_virtual_schema(conn, &VsProps)` |
| `explain_virtual_sql`, `parse_int`, `parse_numeric` | scan/capability/join/count_distinct/positional_deletes | Moved to `common/e2e_harness` |
| `local_stack_creds`/`local_stack_storage`/`local_stack_catalog` | scan/int96/positional_deletes | Moved to `common/e2e_harness` |
| `resolve_fixture_files` (int96 `async`; positional_deletes sync w/ own runtime) | int96/positional_deletes only | Replaced by one `async fn resolve_fixture_files(namespace, table)` in `common/e2e_harness` |
| `struct CloudExaConn`, `fn encrypt_password` | `cloud_e2e_test.rs` | Replaced by `common/exasol_ws::ExaConn` |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Every E2E binary provisions the scan path from one shared harness definition (NEW) | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_projection_filter_limit_returns_correct_rows` (+ every binary in `make test-e2e`) |
| End-to-end projection + filter + LIMIT query returns correct rows | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_projection_filter_limit_returns_correct_rows` |
| E2E suite fails when the stack is unavailable | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_fails_when_stack_unavailable` |
| Oversubscribed shard fan-out is observable via EXPLAIN VIRTUAL | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_shard_key_fanout_explain` |
| Harness provisions the scalar scan and the LUA distributor scripts | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_projection_filter_limit_returns_correct_rows` |
| End-to-end filtered query over a partitioned table returns correct rows with file pruning | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_partition_filter_prunes_and_returns_correct_rows` |
| Cloud suite drives Exasol through the shared redacting WebSocket client (NEW) | Integration | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_redacting_conn_omits_credentials_on_failure` |
| Cloud smoke test queries a real Glue-backed virtual schema | Integration | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_smoke_projection_filter_query` |
| Cloud test skips cleanly when AWS credentials are absent | Integration | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_test_skips_when_creds_absent` |
| Cloud performance smoke records timing and row-count sanity | Integration | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_perf_grouped_aggregate_smoke` |
| Vended credentials are exercised end to end against Glue | Integration | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_scan_reads_with_vended_credentials` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| packaging/e2e-harness | `cargo check --features exasol-e2e` | Compiles clean; all 7 binaries build against the shared harness |
| packaging/cloud-e2e-harness | `cargo check --features cloud-e2e` | Compiles clean; `cloud_e2e_test` builds against `ExaConn` (no `CloudExaConn`) |
| packaging/e2e-harness | `make test-e2e` (Docker stack up) | All 7 `exasol-e2e` binaries pass; suite fails (not skips) if the stack is down |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 (`.so` for `make test-e2e`) |
| Compile (exasol-e2e) | `cargo check --features exasol-e2e` | Exit 0 |
| Compile (cloud-e2e) | `cargo check --features cloud-e2e` | Exit 0 |
| Test | `cargo test` | 0 failures (e2e off by default) |
| E2E | `make test-e2e` | 0 failures if Docker stack available; fails (never skips) if not |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt` | No changes |
