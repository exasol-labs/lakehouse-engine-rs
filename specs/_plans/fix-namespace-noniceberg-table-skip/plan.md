# Plan: fix-namespace-noniceberg-table-skip

## Summary

Make `createVirtualSchema` skip a non-Iceberg table (HTTP 404 on `loadTable`) with a warning instead of aborting the whole namespace, so a mixed Iceberg/Hive estate exposes its Iceberg tables. Closes #138.

## Design

### Context

Real AWS Glue databases are mixed estates: Iceberg tables and Hive external tables share one namespace. The adapter lists every table in the configured namespace, then loads each table's schema through a per-table `loadTable` GET. A Hive table's `loadTable` returns HTTP 404 (`NoSuchIcebergTableException: Input table is not an iceberg table`), which propagates through the `?` at `handle_create_virtual_schema` and aborts the entire `createVirtualSchema` call. One non-Iceberg table makes the whole virtual schema uncreatable (#138).

The failure sits in the per-table loop in `crates/lakehouse-engine/src/adapter/mod.rs` (the `resolve_table_schema` call), not in listing: `list_namespace_tables` returns the mixed identifier list correctly; the abort happens when the Hive identifier's schema is resolved.

- **Goals** — A namespace containing non-Iceberg tables yields a virtual schema of its Iceberg tables; skipped tables are named in a warning; genuine catalog faults still abort loudly.
- **Non-Goals** — No include/exclude table-filter property (see Consequences); no change to the listing path, credential resolution, pushdown, or type mapping; no attempt to expose non-Iceberg tables.

### Decision

Discriminate the per-table `loadTable` outcome on HTTP status. A 404 marks a table that is absent or not an Iceberg table — skip it. Every other failure (transport error, non-404 HTTP status) aborts enumeration, preserving the existing unreachable-catalog contract.

#### Architecture

```
handle_create_virtual_schema
  → list_namespace_tables        (unchanged: returns mixed Iceberg + non-Iceberg idents)
  → build_virtual_tables(configured_ns, idents, resolver)   (NEW seam — pure, no ctx)
        for each ident:
          resolver(ident) → Ok(fields)            → keep: push table + TABLE_MAP entry
                          → Err(is_table_not_found) → skip: record skipped ident
                          → Err(other)            → abort: propagate Err
        → returns (tables_json, table_map, skipped_idents)
  → handler: emit one udf_log!(ctx, warn, …) per skipped ident
  → build_adapter_notes(survivor TABLE_MAP)
```

The per-table loop and `build_table_map` move behind one function, `build_virtual_tables`, so the table list and `TABLE_MAP` are built from the same surviving set. `build_virtual_tables` stays pure and testable — it takes no `ctx` and emits nothing; it returns `skipped_idents` and the handler owns warning emission. The current order (build_table_map over all idents, then load loop) MUST change: the map is built from survivors, not from the raw listing.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Code-authored status-prefix discriminator | `is_table_not_found(&UdfError)` classifier in `mod.rs` | Match the deterministic `catalog returned HTTP 404` message prefix our own error site emits; skip only 404, abort every other error — the single robust signal for "not a loadable Iceberg table" |
| Dependency injection of the resolver | `build_virtual_tables(configured_ns, idents, resolver)` | Test skip/abort/exclusion control flow without a live catalog by injecting a resolver returning success or a simulated error |
| Best-effort side-channel warning | `udf_log!(ctx, warn, …)` in the handler, over `skipped_idents` | Surface skipped tables to script output without failing the request; matches the scan path's diagnostics channel. Emitted by the handler (not the pure `build_virtual_tables`) |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|-------------------------|-----------|
| Skip-and-warn on HTTP 404 only | Skip on any per-table error | Any-error skip masks auth/throttle/outage faults behind a silent partial schema; 404 is the REST-spec `NoSuchTableException` signal and the exact status Glue returns for non-Iceberg tables (#138 body) |
| No table-filter property in this fix | Add `TABLE_INCLUDE`/`TABLE_EXCLUDE` property | Skip-and-warn fully resolves the reported failure with no new configuration surface; an explicit filter is a separable enhancement (own issue) that also touches property persistence and adapterNotes — out of scope for a bug fix (headless default: do not add config surface unless needed) |
| Match the code-authored `catalog returned HTTP 404` message prefix | Introduce a crate-local `u16`-status error type threaded through `authed_get_json` → `resolve_table_schema` | `UdfError` is SDK-owned and carries only opaque `String` variants — there is no status field to key on. The single catalog error site (`credentials.rs:425`) already flattens the status into `catalog returned HTTP 404: …`. Matching that deterministic, code-controlled prefix (via `starts_with`) is narrow and pinned — it keys on *our own* emitted status prefix, not arbitrary catalog body text — and needs no new error type or `credentials.rs`/`file_resolution.rs` threading. The prefix format becomes a load-bearing contract, pinned by a unit test |
| Build `TABLE_MAP` from survivors | Keep building it from the raw listing, then filter | A map entry for a skipped table would advertise an unqueryable virtual table; the map and the table list MUST share one surviving set |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/create-virtual-schema | CHANGED | `vs-adapter/create-virtual-schema/spec.md` |

## Iceberg spec compliance

Per the project's Iceberg-spec-compliance rule, this change touches schema enumeration. The governing normative text is the Iceberg REST Catalog OpenAPI, not the Iceberg *table* spec: the table spec governs on-disk metadata/manifest/type format and says nothing about catalog HTTP error codes. `loadTable` (`GET /v1/{prefix}/namespaces/{namespace}/tables/{table}`) defines `404 — Not Found — NoSuchTableException, table to load does not exist` (Apache Iceberg `open-api/rest-catalog-open-api.yaml`). AWS Glue returns this 404 as `NoSuchIcebergTableException: Input table is not an iceberg table` for a Hive table (#138). This behavior is therefore a REST-protocol error-handling concern outside the Iceberg table spec's normative scope, and is not a table-spec deviation.

## Dependencies

None. No new crates. Uses the existing `udf_log!` macro and `UdfError`.

## Implementation Tasks

1. Add `is_table_not_found(err: &UdfError) -> bool` in `mod.rs`, a classifier that returns true only when the `UdfError::User` message begins (via `starts_with`) with the code-authored prefix `catalog returned HTTP 404` emitted by the single catalog error site. `starts_with` (not `contains`) ensures a non-404 response whose redacted body merely contains "404" cannot false-match. No `credentials.rs` change is needed — the status is already flattened into the message at `credentials.rs:425`, and `redact_error` preserves that prefix (it only strips secret/credential substrings). Unit-test the discriminator (404 true; 401/403/503/transport false) AND pin the contract: assert the `credentials.rs:425` non-success branch still emits the exact `catalog returned HTTP 404: …` prefix. [expert]
2. Extract the per-table enumeration loop into a pure `build_virtual_tables(configured_ns, idents, resolver)` (no `ctx`) returning `(tables_json, table_map, skipped_idents)`: keep on `Ok`, skip on `is_table_not_found`, propagate every other `Err`. Build `TABLE_MAP` from surviving idents only; retain the `__`-collision check over survivors. [expert]
3. Rewrite `handle_create_virtual_schema` to call `build_virtual_tables` (replacing the inline loop and the pre-loop `build_table_map` over all idents), then emit one `udf_log!(ctx, warn, …)` per returned skipped identifier, and pass the survivor `TABLE_MAP` to `build_adapter_notes`. Add `use exasol_udf_sdk::udf_log;` to `mod.rs` (not currently imported on the adapter path). Route the warning text through the existing `redact_error`/redaction path so no credential leaks.
4. Add integration tests for the scenarios (injected-resolver control flow): mixed listing → only Iceberg tables kept, skipped name excluded from `TABLE_MAP`, skipped ident returned for warning; all-non-Iceberg listing → empty tables and empty `TABLE_MAP`, one skipped ident per table, call still succeeds; non-404 error → whole call aborts; multi-table survivors keep collision detection.
5. Update `crates/lakehouse-engine/CHANGELOG.md`-equivalent / version bump handled by the implement/record flow (no action in planning).

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | Task 1 |
| Group B | Task 2 |
| Group C | Task 3, Task 4 |

Sequential dependencies:
- Group A → Group B (build_virtual_tables uses the classifier)
- Group B → Group C (handler and tests use build_virtual_tables)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Inline loop | `handle_create_virtual_schema` per-table loop (`crates/lakehouse-engine/src/adapter/mod.rs:252-278`) | Replaced by `build_virtual_tables` |
| Call site | pre-loop `build_table_map(&configured_ns, &table_idents)` (`mod.rs:249-250`) | Map now built from survivors inside `build_virtual_tables`; the standalone call over all idents is removed (the `build_table_map` helper itself is reused for the survivor set) |

All edits are confined to `crates/lakehouse-engine/src/adapter/mod.rs`. No `credentials.rs` change is required: the 404 status is already flattened into the `UdfError::User` message at `credentials.rs:425`, and the classifier reads it from there.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Create virtual schema enumerates every table in the configured namespace | Integration | `crates/lakehouse-engine/src/adapter/mod.rs` (tests) | `build_virtual_tables_keeps_all_when_every_table_is_iceberg` |
| One non-Iceberg table in the namespace is skipped rather than aborting the schema | Integration | `crates/lakehouse-engine/src/adapter/mod.rs` (tests) | `build_virtual_tables_skips_non_iceberg_table_and_warns` |
| A namespace whose every table is non-Iceberg yields an empty virtual schema | Integration | `crates/lakehouse-engine/src/adapter/mod.rs` (tests) | `build_virtual_tables_all_non_iceberg_yields_empty_schema` |
| A non-404 per-table load failure aborts createVirtualSchema | Integration | `crates/lakehouse-engine/src/adapter/mod.rs` (tests) | `build_virtual_tables_aborts_on_non_404_error` |
| (discriminator) HTTP 404 is the only skippable status | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` (tests) | `is_table_not_found_true_only_for_404` |
| (contract) the catalog error site emits the exact `catalog returned HTTP 404` prefix the classifier keys on | Unit | `crates/lakehouse-engine/src/adapter/pushdown/credentials.rs` (tests) | `catalog_error_message_uses_http_status_prefix` |

Note on test shape: the three scenarios are exercised through `build_virtual_tables` with an injected resolver rather than a live catalog. The local Docker Iceberg REST catalog is an all-Iceberg store and cannot seed a non-Iceberg (Hive) table, so a live-catalog E2E cannot reproduce #138; the injected-resolver integration test drives the real skip/abort/exclusion control flow and is the faithful, deterministic proof. This limitation is deliberate and stated here rather than left as a silent E2E gap.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/create-virtual-schema | `cargo test -p lakehouse-engine build_virtual_tables_skips_non_iceberg_table_and_warns` | Test passes: a mixed listing yields only the Iceberg table in `schemaMetadata.tables` and `TABLE_MAP`; the non-Iceberg identifier is reported for the warning |
| vs-adapter/create-virtual-schema | `cargo test -p lakehouse-engine build_virtual_tables_aborts_on_non_404_error` | Test passes: a non-404 per-table failure returns `Err`, proving genuine catalog faults still abort |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
