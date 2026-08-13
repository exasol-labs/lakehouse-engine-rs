# Plan: add-delta-table-planning

## Summary

Resolve a Delta Lake table into the engine's existing `ScanSpec` shape through `delta-kernel-rs` log
replay, behind a `FormatReader` seam that both table formats implement. Nothing is wired into
production pushdown: the recorded Unity Catalog refusal stays in force until #320 can apply deletion
vectors, partition values, and column mapping at scan time.

## Design

### Context

The engine plans exactly one table format. `resolve_file_list`
(`crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs`) self-issues a `loadTable` GET,
resolves storage credentials, builds an `iceberg::table::Table`, and returns a file list — all
Iceberg-specific, all reached directly by `handle_pushdown` and every join leg. Issue #319 adds Delta
as a second format that must emit the same `ScanSpec`, so sharding, the wire format, streaming emit,
and the memory model are reused unchanged.

Three constraints shape the design. `lakehouse-catalog` MUST NOT name `iceberg`, `datafusion`,
`arrow`, `parquet`, or `object_store` (`vs-adapter/catalog-crate-structure`), which extends to
`delta_kernel` — so the Delta reader cannot live there. `vs-adapter/catalog-kind-selection` freezes
`CatalogKind`'s match sites with a source-level probe — so format dispatch cannot key on the catalog
kind. And the Delta scan path does not exist yet — so any production wiring would return silently
wrong rows rather than a clean error.

- **Goals** — one plan-time abstraction both formats implement; a Delta file list carrying partition
  values, deletion-vector references, and column-mapping info; zero change to the shipped Iceberg
  planning path; the whole Delta path verified offline and against the live fixture stack.
- **Non-Goals** — Delta scan execution (#320); pushdown parity and stats-based file pruning (#321);
  reader-feature gating and broad Delta type mapping (#322); live Databricks coverage (#323); removing
  the Unity Catalog pushdown refusal (#320).

### Decision

#### Architecture

```
                        ┌──────────────────────────────────────────┐
   ScanSource::         │  adapter::pushdown::format               │
   IcebergRest ────────▶│    format_reader()  ← ONE exhaustive     │
   { session, props }   │                       match, fails loud  │
                        │            │          on a mismatch      │
   ScanSource::         │            ▼                             │
   UnityDelta ─────────▶│    Box<dyn FormatReader>                 │
   { session, table }   │       ├── IcebergFormatReader ───────────┼──▶ resolve_file_list()
                        │       │                                  │      (UNCHANGED)
                        │       └── DeltaFormatReader              │
                        │              ├─ storage: vend or static ─┼──▶ lakehouse-catalog
                        │              ├─ store: StorageBackend ───┼──▶ scan::object_store
                        │              ├─ log replay ──────────────┼──▶ delta_kernel 0.26
                        │              └─ schema → LogicalField    │
                        │                        │                 │
                        │                        ▼                 │
                        │                   ResolvedScan           │
                        └───────────────────────┬──────────────────┘
                                                ▼
                          ScanSpec: CommonScanSpec.delta  +  FileEntry.delta
                                    (absent on every Iceberg spec)
```

`handle_pushdown` reaches none of this in this plan. The seam is exercised by its own tests only.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Trait with one boxed implementation per format | `FormatReader` | The per-table-format counterpart of the per-catalog-kind `CatalogClient` trait; a third format is added, not spliced into a dispatch |
| Single exhaustive dispatch site | `format_reader` | A third format or catalog kind is a compile error there, mirroring `CatalogKind`'s one construction site — and matching `ScanSource` rather than `CatalogKind` keeps that enum's frozen probe intact |
| Each implementation owns its whole resolution | `IcebergFormatReader`, `DeltaFormatReader` | A shared caller cannot pre-fetch what each format needs to reach its file list; splitting it would reintroduce the fork the trait removes |
| One optional block per format, not scattered fields | `CommonScanSpec.delta`, `FileEntry.delta` | One skip-serialize gate keeps Iceberg encodings byte-identical, and #321/#322 extend the block without touching the shared structs |
| Injected object store | Delta log replay | Replay is exercised offline over a local filesystem store; building the store is the reader's job |
| Opaque neutral field | `CatalogTable.vended_credential_key` | Keeps the Unity `table_id` from crossing as a Unity concept: a caller hands it back, never parses it |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| `FormatReader` in `lakehouse-engine`, not `lakehouse-catalog` | Extend `CatalogClient` with a file-planning method | `lakehouse-catalog` may not name `iceberg`/`arrow`/`object_store`, so `delta_kernel` is barred for the same reason; and the Iceberg planning code plus `ScanSpec` already live in the engine |
| Dispatch matches `ScanSource` (session + table) | Match `CatalogKind`; match a bare `TableFormat` | A `CatalogKind` match site is forbidden by that feature's source-level probe. A bare format tag cannot carry the session each reader needs, and fetching one per format before dispatch would double-load the Delta table |
| `IcebergFormatReader` delegates to `resolve_file_list` unchanged | Move `resolve_file_list`'s body into the reader | Relocating ~160 lines of shipped, spec-covered, credential-carrying code buys nothing in #319 and risks a regression. The collapse is scheduled for #320, which removes the direct callers |
| `CatalogTable` gains an opaque vending key | A `CatalogClient::resolve_table_storage` method; a re-issued `GET /tables` to recover `table_id` | A trait method would force re-plumbing the shipped Iceberg path, whose equivalent prefix lives engine-side. A second GET breaks "resolve metadata once per query" |
| Delta file entry serializes as a JSON object | A fourth tuple slot on the existing wire enum | A fourth slot would force an always-empty `deletes` array onto every Delta entry; an object is self-describing and leaves the 2-tuple and 3-tuple encodings and their precedence untouched |
| Fail loud on an unmapped Delta type | Map `byte`/`short` to the nearest tag; map nested types to JSON `VARCHAR` now | A near-miss tag returns wrong values. Broad mapping is #322's, and #322 supersedes the refusal with the mapping |
| No reader-feature gating in this plan | Gate `deletionVectors`/`columnMapping` now | Gating now would refuse the very fixtures this plan resolves. #322 owns it, and #325 already recorded that the kernel reads "unsupported" tables without erroring, so gating must be engine-side anyway |
| No per-file min/max statistics | Carry stats alongside partition values | #321 owns stats pruning and has its own fixture; designing the wire shape before its consumer exists risks the wrong shape |
| Test lands in the existing `e2e_unity_test.rs` binary | A new `--test` target | The CI job and the Makefile each name one target and must stay flag-identical; a second binary runs in neither without editing both |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/delta-table-planning | NEW | `specs/_plans/add-delta-table-planning/vs-adapter/delta-table-planning/spec.md` |
| datafusion-scan/scan-execution-spec-reconstitution | CHANGED | `specs/_plans/add-delta-table-planning/datafusion-scan/scan-execution-spec-reconstitution/spec.md` |
| vs-adapter/unity-catalog-client | CHANGED | `specs/_plans/add-delta-table-planning/vs-adapter/unity-catalog-client/spec.md` |
| vs-adapter/catalog-crate-structure | CHANGED | `specs/_plans/add-delta-table-planning/vs-adapter/catalog-crate-structure/spec.md` |
| vs-adapter/pushdown-module-structure | CHANGED | `specs/_plans/add-delta-table-planning/vs-adapter/pushdown-module-structure/spec.md` |
| e2e-harness/unity-catalog-e2e-harness | CHANGED | `specs/_plans/add-delta-table-planning/e2e-harness/unity-catalog-e2e-harness/spec.md` |

## Impact

No query behavior changes. A Unity Catalog pushdown is still refused with the same message, and every
Iceberg query produces a byte-identical scan-driving SQL string and scan spec.

Operators see one change: the UDF `.so` grows and takes longer to build, because `delta_kernel` and
`delta_kernel_default_engine` enter the engine crate. No breaking change, no configuration change, no
new virtual-schema property.

## Dependencies

| Dependency | Version | Features | Notes |
|---|---|---|---|
| `delta_kernel` | 0.26 | `default-engine-base`, `arrow-58` | Declared on `crates/lakehouse-engine/Cargo.toml`, not in `[workspace.dependencies]` — member manifests enter CI's cache key, the workspace manifest does not |
| `delta_kernel_default_engine` | 0.26 | `arrow-58`, `rustls` | Holds `DefaultEngine` / `DefaultEngineBuilder`; accepts an `Arc<dyn ObjectStore>` the engine supplies |

Both were de-risked by the #317 spike against arrow 58, parquet 58, `object_store` 0.13.2, and
DataFusion 54.1 — the workspace's current resolved versions — with one arrow tree and one
`object_store` tree, building in `rust:1.94-bookworm`.

Prerequisite work, all landed: #327 (native Unity Catalog client), #333 (shared vended-storage
policy, the milestone's hard blocker on this issue), #335 (shared decimal-domain guard), #338 (Unity
Catalog CI job). The #325 fixture harness supplies the vendored Delta tables and the
`unity.delta_e2e` registrations.

## Implementation Tasks

1. Add `delta_kernel` 0.26 (`default-engine-base`, `arrow-58`) and `delta_kernel_default_engine` 0.26
   (`arrow-58`, `rustls`) to `crates/lakehouse-engine/Cargo.toml`. Confirm `cargo tree` resolves ONE
   `arrow` and ONE `object_store` version, and confirm `make cross-musl-udf-build` still produces the
   `.so`.
2. Add the neutral `TableFormat` enum and the `format` and `vended_credential_key` fields to
   `CatalogTable` in `crates/lakehouse-catalog/src/client.rs`. Map Unity's `data_source_format`
   (`DELTA`/`ICEBERG`, else a `UdfError` naming the value) and deserialize `table_id` in
   `crates/lakehouse-catalog/src/unity/client.rs`; keep the listing filter's admission decision
   unchanged and set the Delta tag unconditionally on listed tables. Set Iceberg/absent on
   `IcebergRestCatalogClient`. Correct `TableInfo`'s doc comment, which still claims `table_id` is not
   consumed. Edit `crates/lakehouse-catalog/tests/catalog_public_surface.rs` to name `TableFormat` and
   construct `CatalogTable` with both new fields. [expert]
3. Add the Delta wire types to `crates/lakehouse-engine/src/scan/spec.rs` — the table-level block
   (column-mapping mode, ordered columns with logical name / physical name / physical id, ordered
   partition-column names), the per-file block (partition values as a deterministic-order map with an
   explicit NULL, and an optional deletion-vector descriptor with a closed storage-kind enum) — plus
   `CommonScanSpec.delta` and `FileEntry.delta`, both absent from JSON when absent in the value. Add a
   self-describing JSON-OBJECT variant to `FileEntryWire` and keep the 2-tuple and 3-tuple variants
   and their precedence untouched. Prove the round trip is lossless in both directions and that every
   Iceberg encoding is byte-identical. [expert]
4. Extract the undecorated `StorageBackend` → `Arc<dyn ObjectStore>` construction out of
   `build_side_store` in `crates/lakehouse-engine/src/scan/object_store.rs` into one `pub(crate)`
   builder, and have `build_side_store` wrap its result in `SpecSizedObjectStore` as before. Move the
   S3 arm's body verbatim: `with_client_options` REPLACES the whole `ClientOptions` and MUST stay
   before `with_allow_http`, or plain-HTTP MinIO silently breaks. Plan time needs the undecorated
   store because `_delta_log` file sizes are unknown until the log is read.
5. Create the `format` submodule of `adapter::pushdown`: the `FormatReader` trait (one method taking
   the optional pushdown filter JSON and returning a boxed future of `ResolvedScan`), the
   `ResolvedScan` struct (files, effective storage, logical schema, table root, name mapping, optional
   Delta table block), the `ScanSource` enum, and the `format_reader` selection function — one
   exhaustive match that returns a boxed reader and fails loud, naming the table and its reported
   format, when the Unity Catalog variant is handed a non-Delta table. Add `IcebergFormatReader`,
   delegating to `resolve_file_list` unchanged and returning an absent Delta block. Update both
   pushdown surface probes and their stated counts (in-crate 21 → 25, external 11 → 15); keep both
   reader types private to the submodule. [expert]
6. Implement Delta log replay in its own submodule: given an injected `Arc<dyn ObjectStore>` and a
   table-root URL, resolve the log's current version, replay its JSON commits and any checkpoint, and
   return one `FileEntry` per ACTIVE data file carrying its path verbatim, its size, its partition
   values, and its deletion-vector reference. A path removed and re-added inside one commit MUST yield
   exactly one entry carrying the re-added action's deletion vector — a per-`add` collection returns it
   twice. Map the Hive default-partition case to an explicit NULL, never to the literal
   `__HIVE_DEFAULT_PARTITION__`. Leave `FileEntry::deletes` empty and carry no per-file statistic.
   [expert]
7. Implement the Delta schema step in its own submodule: build the ordered `LogicalField` list from
   the Delta schema — Arrow type tag, nullability, and a field-id taken from
   `delta.columnMapping.id` when the table assigns one and from the 1-based ordinal otherwise — and
   build the Delta table block (mode, per-column logical/physical name and id, ordered partition
   columns). Map `boolean`, `integer`, `long`, `float`, `double`, `string`, `date`, `timestamp`,
   `timestamp_ntz`, and `decimal(p,s)`; return a `UdfError` naming the column, its Delta type, and
   issue #322 for anything else. Perform no reader-feature gating.
8. Implement `DeltaFormatReader`, composing tasks 4, 6, and 7: reject an absent or empty storage
   location BEFORE the vended/static split so both credential modes report identical text; under
   vending, require the table's vending key (error naming the table when absent, never a static
   fallback), request temporary table credentials, and terminate them through
   `resolve_uc_vended_storage`; with vending off, use the CONNECTION's static backend; build the
   object store, replay the log, and return `ResolvedScan` carrying the EFFECTIVE storage. Redact every
   error from the split onward against the effective storage's secret values. [expert]
9. Add the offline integration test `crates/lakehouse-engine/tests/delta_log_replay.rs`, driving the
   replay and schema steps over a local-filesystem object store against the vendored fixtures under
   `scripts/unity/fixtures/`: `basic_partitioned` (6 active files across two commits, one NULL
   partition value), `table-with-dv-small` (1 active file, deletion vector `storageType` `u`,
   `pathOrInlineDv` `vBn[lx{q8@P<9BNH/isA`, `offset` 1, `sizeInBytes` 36, `cardinality` 2),
   `cdf-column-mapping-name-mode` (5 commits, 2 removes, 3 active files, `name` mode with `col-<uuid>`
   physical names), and `stats-all-types` (refused, naming an unmapped type). No cargo feature gate —
   the fixtures are in the repository.
10. Add the live-stack test to `crates/lakehouse-engine/tests/e2e_unity_test.rs` under the existing
    `unity-e2e` feature: resolve `unity.delta_e2e.basic_partitioned` through `format_reader` with
    vending enabled and with static MinIO credentials, assert both runs agree on file list, partition
    values, and table root, and assert `unity.delta_e2e.table_with_dv`'s single active file carries a
    deletion-vector reference. Inject the MinIO endpoint client-side; the OSS Unity Catalog server
    vends no endpoint. Fail, never skip, when the stack is unreachable.
11. Verify the scope boundary holds: `unity_kind_pushdown_is_refused_not_iceberg_routed`
    (`crates/lakehouse-engine/src/adapter/adapter_tests.rs`) passes UNEDITED, `handle_pushdown` names
    no item this plan adds, and the `CatalogKind` source-level probe passes unweakened.
12. Run the Iceberg characterization gate: `crates/lakehouse-engine/tests/scan_two_arg.rs`,
    `crates/lakehouse-engine/tests/scan_plan_shape.rs`, and
    `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs` MUST pass with no edit to
    any assertion, expected value, or committed golden.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1, 2, 3, 4 |
| Group B | 5, 6, 7 |
| Group C | 8 |
| Group D | 9, 10 |
| Group E | 11, 12 |

Sequential dependencies:
- Group A → Group B (task 5 needs task 2's format tag; tasks 6 and 7 need task 1's crate and task 3's
  wire types)
- Group B → Group C (task 8 composes tasks 6 and 7 behind task 5's trait, over task 4's store builder)
- Group C → Group D (both tests drive the composed reader)
- Group D → Group E (the regression gate runs last)

Within a group the tasks touch disjoint files: task 2 is `lakehouse-catalog`, task 3 is
`scan/spec.rs`, task 4 is `scan/object_store.rs`, task 1 is manifests; task 5 is `format/mod.rs` plus
`format/iceberg.rs`, task 6 and task 7 are their own `format` submodules.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | — | This plan only adds. `resolve_file_list`'s 5-tuple return and its direct call sites are deliberately RETAINED so the shipped Iceberg planning path changes zero bytes; collapsing it into `IcebergFormatReader` is scheduled for #320, which removes those call sites when it routes production pushdown through the seam |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A Delta table resolves its current version's active data files | Integration | `crates/lakehouse-engine/tests/delta_log_replay.rs` | `replay_returns_only_the_files_active_at_the_current_version` |
| Partition values are carried per data file, including a NULL partition value | Integration | `crates/lakehouse-engine/tests/delta_log_replay.rs` | `replay_carries_partition_values_and_an_explicit_null` |
| A data file's deletion vector reference is carried verbatim exactly once | Integration | `crates/lakehouse-engine/tests/delta_log_replay.rs` | `replay_carries_a_readded_files_deletion_vector_exactly_once` |
| Column-mapping mode and physical column names are carried once per table | Integration | `crates/lakehouse-engine/tests/delta_log_replay.rs` | `replay_carries_name_mode_column_mapping_and_physical_names` |
| Delta planning resolves its storage credential through the table's own catalog | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_planning_agrees_under_vended_and_static_credentials` |
| Delta planning resolves its storage credential through the table's own catalog | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_tests.rs` | `vending_without_a_vending_key_errors_and_never_falls_back_to_static` |
| An empty table storage location is rejected before any object-store access | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_tests.rs` | `empty_storage_location_errors_identically_under_both_credential_modes` |
| A Delta type this plan does not map is refused at plan time | Integration | `crates/lakehouse-engine/tests/delta_log_replay.rs` | `unmapped_delta_type_is_refused_naming_the_column_and_issue_322` |
| The format reader is selected at one site and refuses a mismatched pairing | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/format_tests.rs` | `format_reader_refuses_a_non_delta_table_under_the_unity_source` |
| Iceberg planning is byte-identical through the new seam | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs` | `iceberg_reader_returns_resolve_file_lists_result_with_no_delta_block` |
| Iceberg planning is byte-identical through the new seam | Integration | `crates/lakehouse-engine/tests/scan_two_arg.rs` | existing suite passes unedited (characterization gate) |
| Delta planning adds no production pushdown path in this plan | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `unity_kind_pushdown_is_refused_not_iceberg_routed` (unedited) |
| Reconstitution carries the Delta table block and per-file Delta blocks | Unit | `crates/lakehouse-engine/src/scan/spec_tests.rs` | `delta_blocks_round_trip_losslessly_and_leave_iceberg_encodings_byte_identical` |
| The Unity Catalog session is reached only through the shared catalog-client trait | Unit | `crates/lakehouse-catalog/src/unity/client_tests.rs` | `neutral_table_carries_the_format_tag_and_the_opaque_vending_key` |
| The client lists tables in a configured catalog and schema | Unit | `crates/lakehouse-catalog/src/unity/client_tests.rs` | `list_tables_tags_every_admitted_table_delta_and_keeps_the_skip_filter` |
| The client retrieves a table's metadata including its columns | Unit | `crates/lakehouse-catalog/src/unity/client_tests.rs` | `load_table_returns_format_tag_vending_key_and_ordered_columns` |
| The single-table load refuses a data source format the crate cannot name | Unit | `crates/lakehouse-catalog/src/unity/client_tests.rs` | `load_table_refuses_an_absent_or_unrecognized_data_source_format` |
| The neutral table's format tag and vending key extend the crate's public surface through an explicit reviewed edit | Integration | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | compile-time probe names `TableFormat` and both new fields |
| The format-reader seam extends the pushdown façade through an explicit reviewed edit | Integration | `crates/lakehouse-engine/tests/pushdown_public_surface.rs` | compile-time probe names 15 items (in-crate sibling probe names 25) |
| Create virtual schema over a Unity Catalog namespace lists the fixture tables and columns | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_create_virtual_schema_lists_fixture_tables_and_columns` (unedited) |
| The suite resolves a seeded Delta table's scan spec over MinIO under both credential modes | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_planning_agrees_under_vended_and_static_credentials` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/delta-table-planning | `cargo test -p lakehouse-engine --test delta_log_replay` | 5 tests pass; the `basic_partitioned` assertion reports 6 active files with partition values `a`, `b`, `c`, NULL, `a`, `e` |
| vs-adapter/delta-table-planning | `make unity-up && cargo test -p lakehouse-engine --features unity-e2e --test e2e_unity_test -- --test-threads=1` | All Unity tests pass, including the vended/static Delta planning test; no credential value in the output |
| vs-adapter/unity-catalog-client | `cargo test -p lakehouse-catalog unity::client` | The format-tag, vending-key, and unrecognized-format tests pass |
| vs-adapter/catalog-crate-structure | `cargo test -p lakehouse-catalog --test catalog_public_surface` | Compiles and passes; `TableFormat` is reachable at `pub` from outside the crate |
| vs-adapter/pushdown-module-structure | `cargo test -p lakehouse-engine --test pushdown_public_surface` | Compiles and passes with 15 named items |
| datafusion-scan/scan-execution-spec-reconstitution | `cargo test -p lakehouse-engine scan::spec` | Round-trip and Iceberg byte-identity tests pass |
| e2e-harness/unity-catalog-e2e-harness | `make unity-down && cargo test -p lakehouse-engine --features unity-e2e --test e2e_unity_test` | The suite FAILS (never skips) with the stack down |
| Iceberg regression gate | `cargo test -p lakehouse-engine --test scan_two_arg --test scan_plan_shape` | 0 failures, no assertion or golden edited |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build (`.so`) | `make cross-musl-udf-build` | Exit 0; one `arrow` and one `object_store` version in `cargo tree` |
| Test (host) | `cargo test` | 0 failures |
| Test (Unity E2E) | `make test-e2e-unity` | 0 failures; fails, never skips, without the stack |
| Test (Iceberg E2E) | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |
| Spec validation | `speq plan validate add-delta-table-planning` | pass |
