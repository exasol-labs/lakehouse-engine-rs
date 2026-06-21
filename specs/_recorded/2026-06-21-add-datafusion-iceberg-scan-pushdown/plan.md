# Plan: add-datafusion-iceberg-scan-pushdown

## Summary

Build the first implementable slice of the DataFusion Virtual Schema PoC: a stateless
Exasol Virtual Schema whose thin adapter resolves an Iceberg table's data files once
and drives a DataFusion scan SET UDF (both entry points in one `.so`) that scans the
assigned Iceberg/Parquet files in MinIO with projection, filter, and LIMIT pushdown and
streams rows back. Multi-node sharding, Databricks, and aggregation pushdown are
explicitly deferred to later plans.

## Design

### Context

The PoC hypothesis is that Exasol cluster parallelism × DataFusion vectorized execution
gives distributed lakehouse query execution. This plan proves only the single-node
inner loop plus the seams the later phases will exploit — it does not yet parallelize.
The forces: the `.so` boundary is hostile to Arrow (different `TypeId`s), metadata must
be resolved once per query (not per node), and the VS must stay thin so execution lives
in DataFusion. language-container-rs 0.14.0 newly allows multiple UDF entry points per
`.so`, which lets the thin adapter and the heavy scan ship as one artifact.

- **Goals** — (1) one crate / one `.so` exporting a VS adapter entry point and a
  DataFusion scan SET-UDF entry point; (2) metadata resolved once in the adapter, file
  list passed explicitly to the UDF; (3) projection + filter + LIMIT pushed into the
  DataFusion scan; (4) Arrow→`Value` conversion and incremental emit across the
  boundary; (5) an E2E proof against Exasol + Iceberg REST + MinIO in Docker.
- **Non-Goals** — multi-node IPROC file sharding; Databricks / Unity access;
  aggregation or partial-aggregation pushdown; any caching, persistence,
  materialization, or snapshot tracking; joins. (See § Roadmap / Deferred.)

### Decision

A single `cdylib` crate, `lakehouse-engine`, exports two `#[exasol_udf]` entry points. The
adapter entry point handles the VS JSON protocol and resolves the Iceberg file list
once; the scan entry point is a SET UDF that consumes a scan spec, runs DataFusion, and
emits rows. The adapter's `pushdown` response is SQL that invokes the scan SET UDF with
the resolved file list and the pushed-down projection/filter/limit.

#### Architecture

```
User SELECT (projection, filter, LIMIT)
  │
  ▼
┌──────────────────────────────┐        one .so, entry point #1
│  VS Adapter  (adapter_call)   │  ── resolve Iceberg snapshot + file list ONCE
│  - getCapabilities            │     (Iceberg REST catalog over MinIO)
│  - createVirtualSchema (schema│
│    mapping)                   │
│  - pushdown → scan-driving SQL│── builds: SELECT scan_udf(<files>,<proj>,<filter>,<limit>)
└──────────────────────────────┘
  │ Exasol executes the returned SQL
  ▼
┌──────────────────────────────┐        same .so, entry point #2
│  DataFusion Scan SET UDF      │  ── read scan spec via ctx.next()/getters
│  - SessionContext             │     register ONLY assigned files
│  - apply projection/filter/lim│     (DataFusion ListingTable / object_store→MinIO)
│  - scan → Arrow RecordBatches │
│  - batch → Value rows → emit  │── ctx.emit incrementally, drop each batch
└──────────────────────────────┘
  │
  ▼
Iceberg/Parquet files in MinIO  →  rows back to Exasol
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Two entry points in one `.so` (0.14.0) | `lakehouse-engine` crate | One artifact, one BucketFS upload; adapter + scan share build/deploy |
| Resolve-once seam | adapter `pushdown` → explicit file-list arg to UDF | Avoids N-node duplicate metadata fetches; the seam multi-node sharding later exploits |
| File-level work assignment | scan UDF receives a file list, never discovers files | Keeps the UDF stateless and shardable later |
| Batch-and-emit streaming | scan UDF Arrow→`Value` loop | Never materialize full result; rely on 4,000,000-byte auto-flush |
| Value-only boundary | Arrow→`Value` before `ctx.emit` | Arrow `TypeId`s differ across the `.so`; only SDK `Value` is FFI-safe |
| Mirror sibling conventions | Makefile, compose overlays, BucketFS port 2581 | `strata-rs` already solved SLC install + E2E; do not reinvent |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| One crate, two entry points | Two crates / two `.so` (the pre-0.14.0 shape) | 0.14.0 supports multi-entry; one artifact halves deploy surface and matches the new capability under test |
| Adapter returns SQL that calls the scan SET UDF | Adapter calls a library function directly and selects from a cache (the `strata-rs` shape) | The PoC's hypothesis is DataFusion-in-UDF as the execution substrate; caching is an explicit non-goal, so the scan must run as a UDF, not populate a cache |
| Pass file list as an explicit UDF argument | Let the UDF re-resolve metadata from the catalog | Mission constraint: resolve once per query; explicit hand-off is the multi-node sharding seam |
| Iceberg REST catalog + MinIO as first target | Nessie / Polaris / Lakekeeper / real S3+Glue | Interview decision; REST + MinIO is the simplest reproducible Docker target |
| Depend on 0.14.0 from crates.io, path fallback | Path-depend on local siblings unconditionally | 0.14.0 publish is in flight (crates.io still shows 0.13.1); version dep is cleaner once published, path is the documented fallback |
| Single UDF invocation over the whole file list | Shard files now | Multi-node is deferred; single invocation keeps the slice minimal while preserving the seam |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/create-virtual-schema | NEW | `vs-adapter/create-virtual-schema/spec.md` |
| vs-adapter/pushdown-planning | NEW | `vs-adapter/pushdown-planning/spec.md` |
| datafusion-scan/scan-execution | NEW | `datafusion-scan/scan-execution/spec.md` |
| datafusion-scan/type-mapping | NEW | `datafusion-scan/type-mapping/spec.md` |
| packaging/single-so-two-entry-points | NEW | `packaging/single-so-two-entry-points/spec.md` |
| packaging/e2e-harness | NEW | `packaging/e2e-harness/spec.md` |

## Dependencies

- `exasol-udf-sdk` 0.14.0 (feature `connect-back`) + `exasol-udf-macros` 0.14.0.
  crates.io currently shows 0.13.1 as latest while 0.14.0 lands; **fallback**:
  path-depend on the local siblings at
  `/home/talos/code/strata/language-container-rs/crates/exasol-udf-sdk` and
  `.../exasol-udf-macros` until 0.14.0 is downloadable.
- `datafusion` + `arrow`/`parquet` 58 (match the SLC's arrow 58).
- `iceberg-rust` (Iceberg REST catalog) + an object_store / opendal S3 backend for MinIO.
- Builder image `rust:1.92-bookworm`; Rust SLC from language-container-rs 0.14.0.
- Docker: `exasol/docker-db`, an Iceberg REST catalog image, `minio/minio` + `minio/mc`.

## Implementation Tasks

### Group A — Scaffolding (no dependencies)

- [ ] A.1 Create workspace `Cargo.toml` (edition 2024) and the `lakehouse-engine` `cdylib` crate skeleton; pin arrow/parquet 58, datafusion, iceberg-rust, and the SDK/macros deps (0.14.0 with documented path fallback).
- [ ] A.2 Add the Makefile with `cross-musl-udf-build` (docker run in `rust:1.92-bookworm`, `-p lakehouse-engine`, output `target/release/liblakehouse_engine.so`, persistent cargo registry volume) and `test-e2e` (gated `--features exasol-e2e`), mirroring `strata-rs`.
- [ ] A.3 Add Docker compose overlays for MinIO + Iceberg REST catalog + Exasol on a shared network (BucketFS port 2581, MinIO 9000), mirroring `strata-rs/docker-compose.exasol.yml`.

### Group B — Two entry points + scan core

- [ ] B.1 Declare both `#[exasol_udf]` entry points (adapter + scan SET UDF) in one crate and confirm one `.so` exports both symbols. [expert]
- [ ] B.2 Implement the scan spec type (file list, projection, filter, limit, catalog/storage props) and its (de)serialization across the UDF argument boundary using only SDK `Value` types. [expert]
- [ ] B.3 Implement the DataFusion scan: build `SessionContext`, configure the object_store for MinIO, register ONLY the assigned files, apply projection/filter/limit. [expert]
- [ ] B.4 Implement Arrow `RecordBatch` → SDK `Value` row conversion implementing the full `datafusion-scan/type-mapping` table — all compatible types + null, plus the JSON-string fallback (`CAST(col AS VARCHAR)` / `arrow_cast` → `Value::String`) for out-of-range Decimal128 and every incompatible Arrow type (list, struct, map, binary, etc.) — and the batch-by-batch incremental `ctx.emit` loop (drop each batch before fetching the next). [expert]
- [ ] B.5 Add scan-side error handling that surfaces unreadable-file errors without leaking credentials.

### Group C — Adapter (depends on B.2 scan-spec type)

- [ ] C.1 Implement `getCapabilities` (projection + filter + LIMIT only; no aggregation/join).
- [ ] C.2 Implement `createVirtualSchema`: resolve the Iceberg schema from the REST catalog and map each field to an Exasol SQL type using the shared `datafusion-scan/type-mapping` table (incompatible types declared as `VARCHAR(2000000)`, never an error); return the virtual-table JSON.
- [ ] C.3 Implement `pushdown`: resolve the snapshot + data-file list ONCE, capture projection/filter/limit, build the scan-driving SQL invoking the scan SET UDF with the explicit file list. [expert]
- [ ] C.4 Implement adapter-side error handling (catalog-unreachable, credential redaction).

### Group D — Tests (host unit; depend on B/C)

- [ ] D.1 Host unit tests for Arrow→`Value` conversion (all type mappings + null).
- [ ] D.2 Host unit tests for scan-spec (de)serialization round-trip across the `Value` boundary.
- [ ] D.3 Host unit tests for pushdown SQL generation (projection/filter/limit carried; untranslatable predicate omitted) and capability reporting.
- [ ] D.4 Host unit test for Iceberg-field → Exasol-type schema mapping.
- [ ] D.5 Host unit tests for the full `datafusion-scan/type-mapping` table — one test per mapping category: numeric (Int8/16/32, Int64/UInt32/64, UInt8/16), float (Float32/64), string (Utf8/LargeUtf8), date/time (Date32, Timestamp with/without tz), in-range Decimal128 (p≤36,s≤36 → DECIMAL(p,s)), out-of-range Decimal128 (p>36 or s>36 → VARCHAR via JSON), and each incompatible family (list, struct, map, binary). Assert both the declared Exasol type and the converted `Value` variant agree. [expert]

### Group E — E2E (depends on A.2, A.3, B, C, and a seeded table)

- [ ] E.1 Add an E2E test helper that seeds an Iceberg table into the REST catalog over MinIO.
- [ ] E.2 Add E2E setup: install Rust SLC + upload the `.so` to BucketFS (port 2581), set `SCRIPT_LANGUAGES`, create the adapter + scan scripts and the virtual schema. [expert]
- [ ] E.3 Add the E2E test: `SELECT <cols> ... WHERE <pred> LIMIT <n>` through the VS returns the correct projected, filtered, capped rows; and assert the suite FAILS (not skips) when Exasol is unreachable. All DSNs include `validateservercertificate=0`.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | A.1, A.2, A.3 |
| Group B | B.1, B.3, B.4, B.5 (B.2 first within group) |
| Group C | C.1, C.2, C.4 (C.3 after B.2) |
| Group D | D.1, D.2, D.3, D.4, D.5 |

Sequential dependencies:
- A.1 → B.* and C.* (crate must exist)
- B.2 → B.3, C.3 (scan-spec type is shared)
- B.*, C.* → D.* (unit tests follow the code)
- A.2, A.3, B.*, C.*, E.1 → E.2 → E.3

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| — | — | Greenfield project; no existing code to remove. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| create-virtual-schema / Adapter reports its pushdown capabilities | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `reports_projection_filter_limit_only` |
| create-virtual-schema / Create virtual schema maps the Iceberg table schema | Integration | `tests/e2e_scan_test.rs` | `create_vs_maps_iceberg_schema` |
| create-virtual-schema / Create virtual schema fails clearly when the catalog is unreachable | Integration | `tests/e2e_scan_test.rs` | `create_vs_unreachable_catalog_errors_no_secret` |
| pushdown-planning / Pushdown resolves the file list once and builds a scan-driving query | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `pushdown_resolves_files_once_builds_scan_sql` |
| pushdown-planning / Projection is pushed into the scan-driving query | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `pushdown_carries_projection` |
| pushdown-planning / Filter predicate is pushed into the scan spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `pushdown_translates_or_omits_predicate` |
| pushdown-planning / LIMIT is pushed into the scan spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `pushdown_carries_limit` |
| scan-execution / Scan registers only its assigned files and returns matching rows | Integration | `tests/e2e_scan_test.rs` | `scan_registers_only_assigned_files` |
| scan-execution / Filter predicate restricts the emitted rows | Integration | `tests/e2e_scan_test.rs` | `scan_filter_restricts_rows` |
| scan-execution / LIMIT caps the emitted rows | Integration | `tests/e2e_scan_test.rs` | `scan_limit_caps_rows` |
| scan-execution / Arrow batches are converted to Value rows and emitted incrementally | Unit | `crates/lakehouse-engine/src/scan/emit.rs` | `emits_batch_by_batch_without_materializing` |
| scan-execution / Arrow types map to the correct SDK Value variants | Unit | `crates/lakehouse-engine/src/scan/convert.rs` | `arrow_columns_map_to_value_variants` |
| scan-execution / Incompatible Arrow columns are emitted as JSON strings | Unit | `crates/lakehouse-engine/src/scan/convert.rs` | `incompatible_columns_emit_json_strings` |
| type-mapping / Compatible Arrow types map to their Exasol type | Unit | `crates/lakehouse-engine/src/types/mapping.rs` | `compatible_types_map_to_exasol_type` |
| type-mapping / In-range Decimal128 maps to a precise Exasol DECIMAL | Unit | `crates/lakehouse-engine/src/types/mapping.rs` | `decimal128_in_range_maps_to_decimal` |
| type-mapping / Out-of-range Decimal128 falls back to VARCHAR via JSON | Unit | `crates/lakehouse-engine/src/types/mapping.rs` | `decimal128_out_of_range_maps_to_varchar_json` |
| type-mapping / Incompatible Arrow types are serialized to JSON VARCHAR | Unit | `crates/lakehouse-engine/src/types/mapping.rs` | `incompatible_types_map_to_varchar_json` |
| type-mapping / A mixed-column Parquet file round-trips through schema mapping and scan | Integration | `tests/e2e_scan_test.rs` | `mixed_column_parquet_round_trips` |
| scan-execution / Scan reports a clear error when an assigned file is unreadable | Integration | `tests/e2e_scan_test.rs` | `scan_unreadable_file_errors_no_secret` |
| single-so-two-entry-points / One crate exports both the adapter and the scan entry points | Integration | `tests/two_entry_points_test.rs` | `so_exports_both_entry_symbols` |
| single-so-two-entry-points / Both scripts resolve from the same uploaded artifact | Integration | `tests/e2e_scan_test.rs` | `both_scripts_resolve_one_artifact` |
| single-so-two-entry-points / Host release build of the .so is rejected by convention | Unit | `crates/lakehouse-engine/tests/build_convention.rs` | `host_release_build_documented_unloadable` |
| e2e-harness / End-to-end projection + filter + LIMIT query returns correct rows | Integration | `tests/e2e_scan_test.rs` | `e2e_projection_filter_limit_returns_correct_rows` |
| e2e-harness / E2E suite fails when the stack is unavailable | Integration | `tests/e2e_scan_test.rs` | `e2e_fails_when_stack_unavailable` |

- **Integration test** — default for all scenarios; scenarios touching Exasol/MinIO/catalog run under `--features exasol-e2e`.
- **Unit test** — used only for pure computation (type conversion, SQL/spec string building, capability list) with no I/O.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| packaging/single-so-two-entry-points | `make cross-musl-udf-build && nm -D target/release/liblakehouse_engine.so \| grep __exa_udf_entry_` | Two entry-point symbols (adapter + scan) listed |
| packaging/e2e-harness | `docker compose up -d` then `make test-e2e` | Stack starts; E2E suite passes |
| vs-adapter/create-virtual-schema | In Exasol: `CREATE VIRTUAL SCHEMA my_vs USING ... ;` then `OPEN SCHEMA my_vs; DESCRIBE <table>;` | Table columns appear with mapped SQL types |
| vs-adapter/pushdown-planning + datafusion-scan | In Exasol: `SELECT a, b FROM my_vs.t WHERE a > 10 LIMIT 5;` | Up to 5 rows, only columns a,b, all with a>10, matching seeded data |
| datafusion-scan/type-mapping | In Exasol on a table with a list/struct column: `DESCRIBE my_vs.t;` then `SELECT <complex_col> FROM my_vs.t LIMIT 1;` | The complex column is typed `VARCHAR(2000000)`; the value comes back as a JSON string |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0; `target/release/liblakehouse_engine.so` produced |
| Test | `cargo test` | 0 failures (host unit tests) |
| Test (E2E) | `make test-e2e` | 0 failures against the running stack |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |

## Roadmap / Deferred (NOT in this plan)

Captured so later `/speq:plan` calls pick them up; do NOT design or build them now:

- **Multi-node IPROC file sharding** — partition the resolved file list across active
  Exasol nodes so no node scans another's files. The resolve-once seam (adapter passes
  an explicit file list) is built here precisely to enable this later.
- **Databricks / Unity Catalog access** — query Databricks-managed Iceberg through the
  same path.
- **Aggregation + partial-aggregation pushdown** — node-local aggregate merged by
  Exasol, to cut network transfer.
- **Out of scope project-wide (mission non-goals):** caching, result reuse,
  materialization, metadata persistence, snapshot tracking, refresh, joins, query
  rewrites.
