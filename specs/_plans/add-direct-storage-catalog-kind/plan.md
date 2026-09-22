# Plan: add-direct-storage-catalog-kind

## Summary

Add a third `CatalogKind`, `DIRECT_STORAGE`, so the engine queries a plain directory of Parquet
files on S3 or Azure Blob Storage with no catalog service. The query runs end to end through the
existing pushdown, sharding, and emit paths. Table discovery and query planning both read one
shared directory seam that lists the data files and folds their footers into one schema.

## Design

### Context

`lakehouse-engine` reaches every table through a catalog. Iceberg REST supplies a snapshot and a
manifest. Unity Catalog supplies a transaction log. Both answer "which files, and what schema".
A customer (issue #142) and issue #407 ask for the case where no catalog exists: an
object-storage prefix holding directories of Parquet files.

Two forces shape the design. First, the engine already has a format-neutral seam: `CatalogClient`
for discovery, `FormatReader` for planning. A catalog-free source must therefore arrive through
those seams rather than beside them. Second, a raw directory answers "what schema" only by reading
Parquet footers. Different files in one directory can declare one column at different types. That
second force is the design decision: whether to sample one footer, fold all of them, or refuse.

- **Goals**: one new catalog kind that discovers tables, declares columns, plans scans, and
  serves queries through the unchanged pipelines. One shared answer to "which files and what
  schema" so discovery and planning cannot disagree. Schema merging that widens only across pairs
  the engine can already cast.
- **Non-Goals**: Hive partition pruning (#408), footer-statistics file pruning (#412), Unity
  Catalog PARQUET-table routing (#409), non-Iceberg Glue tables (#410), a table-format detection
  heuristic, non-Parquet file formats, a file-count or file-size safety limit (#419), and any
  caching of a listing or a footer.

### Decision

#### Architecture

```
CREATE VIRTUAL SCHEMA                    pushdown
        │                                    │
        ▼                                    ▼
 resolve_catalog_kind ───────────────► TableScanResolver
        │                                    │
        ▼                                    ▼
 construct_catalog_client            ScanSource::DirectParquet
        │                                    │
        ▼                                    ▼
 DirectStorageCatalogClient            ParquetFormatReader
        │                                    │
        └──────────┬─────────────────────────┘
                   ▼
        parquet_directory seam
        (list *.parquet, fold footers)
                   │
                   ▼
        one admission-limited ObjectStore
```

The seam is the deep module. Two callers in different layers ask it one question. They receive the
file list with sizes, the folded Arrow schema, and the parsed per-file footers. Neither caller
carries a listing filter, a footer reader, or a merge policy.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| One shared seam, two callers | `adapter/parquet_directory.rs` | A second merge policy over one `MERGE_SCHEMA` value is the drift the seam prevents |
| Identity column binding | `LogicalField` with no field-id and no physical name | Already shipped for Delta `none` mapping; a raw directory carries no binding key |
| Widening pair set as production code | `types/widening.rs` | The fold must decide a wider type; two copies of the 13 rows would drift |
| Arrow type as a tag string on the neutral column | `ColumnSourceType::Parquet` | `lakehouse-catalog` may not declare `arrow` |
| Admission-limited object store per adapter call | `scan/object_store.rs` | One bound over the whole call rather than per table |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| `MERGE_SCHEMA` defaults to TRUE and folds every footer | Sample one footer by default | A directory whose files differ is the case the feature exists for; a silent wrong type is worse than a slower refresh |
| The fold widens only across the recorded relaxation pairs | A new pair table; `can_cast_types` as the oracle | A pair the fold invented would declare a type the scan cannot cast a file up to; `can_cast_types` also accepts narrowing |
| An unfoldable pair fails the refresh | Declare the column `VARCHAR`, drop it, take one file's type | Each alternative answers a correctness question by guessing |
| `DirectStorageCatalogClient` is declared in `lakehouse-engine` | Declare it in `lakehouse-catalog` | The catalog crate may not depend on `object_store`; the orphan rule permits a local type implementing a foreign trait |
| The neutral column carries an Arrow TAG STRING | Carry `DataType`; carry the Exasol type | `lakehouse-catalog`'s manifest forbids `arrow`; an Exasol type would move the mapping into the catalog crate |
| No source-level probe for the catalog-kind site list | Build the probe the recorded clause names | A structural invariant enforced by matching production source text is not accepted here; exhaustive matches carry the compile-time half |
| An Iceberg or Delta directory read this way returns tombstoned rows | Detect and refuse such a directory | The operator chose a kind that names no table format; a heuristic would fail a supported read on a layout it guessed wrong about |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/catalog-kind-selection | CHANGED | `specs/_plans/add-direct-storage-catalog-kind/vs-adapter/catalog-kind-selection/spec.md` |
| vs-adapter/connection-credentials-direct-storage | NEW | `specs/_plans/add-direct-storage-catalog-kind/vs-adapter/connection-credentials-direct-storage/spec.md` |
| vs-adapter/connection-credentials | CHANGED | `specs/_plans/add-direct-storage-catalog-kind/vs-adapter/connection-credentials/spec.md` |
| vs-adapter/direct-storage-properties | NEW | `specs/_plans/add-direct-storage-catalog-kind/vs-adapter/direct-storage-properties/spec.md` |
| vs-adapter/create-virtual-schema | CHANGED | `specs/_plans/add-direct-storage-catalog-kind/vs-adapter/create-virtual-schema/spec.md` |
| vs-adapter/parquet-directory-seam | NEW | `specs/_plans/add-direct-storage-catalog-kind/vs-adapter/parquet-directory-seam/spec.md` |
| vs-adapter/direct-storage-table-discovery | NEW | `specs/_plans/add-direct-storage-catalog-kind/vs-adapter/direct-storage-table-discovery/spec.md` |
| vs-adapter/direct-storage-table-planning | NEW | `specs/_plans/add-direct-storage-catalog-kind/vs-adapter/direct-storage-table-planning/spec.md` |
| vs-adapter/pushdown-format-neutral-resolution | CHANGED | `specs/_plans/add-direct-storage-catalog-kind/vs-adapter/pushdown-format-neutral-resolution/spec.md` |
| vs-adapter/delta-table-planning | CHANGED | `specs/_plans/add-direct-storage-catalog-kind/vs-adapter/delta-table-planning/spec.md` |
| vs-adapter/delta-type-mapping | CHANGED | `specs/_plans/add-direct-storage-catalog-kind/vs-adapter/delta-type-mapping/spec.md` |
| vs-adapter/catalog-crate-public-surface-extensions | CHANGED | `specs/_plans/add-direct-storage-catalog-kind/vs-adapter/catalog-crate-public-surface-extensions/spec.md` |
| datafusion-scan/type-mapping | CHANGED | `specs/_plans/add-direct-storage-catalog-kind/datafusion-scan/type-mapping/spec.md` |
| datafusion-scan/type-mapping-module-structure | CHANGED | `specs/_plans/add-direct-storage-catalog-kind/datafusion-scan/type-mapping-module-structure/spec.md` |
| datafusion-scan/type-relaxation | CHANGED | `specs/_plans/add-direct-storage-catalog-kind/datafusion-scan/type-relaxation/spec.md` |
| datafusion-scan/scan-execution-field-id-projection | CHANGED | `specs/_plans/add-direct-storage-catalog-kind/datafusion-scan/scan-execution-field-id-projection/spec.md` |
| e2e-harness/direct-storage-e2e | NEW | `specs/_plans/add-direct-storage-catalog-kind/e2e-harness/direct-storage-e2e/spec.md` |
| e2e-harness/direct-storage-e2e-properties | NEW | `specs/_plans/add-direct-storage-catalog-kind/e2e-harness/direct-storage-e2e-properties/spec.md` |
| e2e-harness/e2e-harness | CHANGED | `specs/_plans/add-direct-storage-catalog-kind/e2e-harness/e2e-harness/spec.md` |
| azure-e2e/azure-e2e-harness | CHANGED | `specs/_plans/add-direct-storage-catalog-kind/azure-e2e/azure-e2e-harness/spec.md` |

## Impact

Operators gain a third `CATALOG_KIND`. A virtual schema created with
`CATALOG_KIND = 'DIRECT_STORAGE'` serves one virtual table per first-level directory under the
CONNECTION address, optionally scoped by `NAMESPACE`. `MERGE_SCHEMA` selects whether every footer
or one footer decides the declared types.

No breaking change. `CATALOG_KIND` absent still resolves Iceberg REST. `UNITY_CATALOG` is
unchanged. Every existing virtual schema keeps its behavior with no configuration change. The
only visible change to an existing path is the unrecognized-`CATALOG_KIND` error text. It now
names three accepted spellings instead of two.

Two operator-facing cautions ship with the feature, both stated in the specs and required in the
user documentation:

- A directory that holds an Iceberg or a Delta table, read under this kind, returns raw Parquet
  rows. Deleted rows reappear. Rows can repeat. The fix is to select the catalog kind that
  supplies table-format semantics.
- Every plan reads the footers the merge mode selects. Every plan scans every file under the table
  root. A directory with many files costs a proportional plan-time read. Path-based pruning is
  #408. Footer-statistics pruning is #412.

Two gaps ship as ONE tracked exception, issue #419, opened for this plan because neither was
covered by #408, #409, #410, or #412.

- **No file-count or file-size safety limit.** Nothing refuses a table directory holding hundreds of
  thousands of files. The plan-time footer read named in the caution above is therefore unbounded at
  `CREATE VIRTUAL SCHEMA` and at every plan. This is the plan's largest operational risk.
- **The admission cap of 16 is unmeasured.** It is a deliberately conservative first guess. A
  refresh sweeping many thousands of footers tolerates far more concurrency than a per-query plan.
  Issue #419 carries the measurement that would set it.

## Dependencies

- `object_store` 0.13.2's `LimitStore`, already in the dependency tree and not yet used in this
  repo.
- No new crate, no new Docker service, no new CI job, and no new environment variable.

## Implementation Tasks

### 1. Catalog kind, CONNECTION acceptance, and direct-storage properties

- [ ] 1.1 Add `CatalogKind::DirectStorage` and the `DIRECT_STORAGE` spelling, extend
  `resolve_catalog_kind`, and rewrite the unrecognized-value error to name all three accepted
  spellings and state that absence selects Iceberg REST.
- [ ] 1.2 Add a scheme-agreement method on `StorageBackend` with no catch-all arm, accepting `s3`,
  `s3a`, and `abfss`, and rejecting `abfs` with an error naming `abfss`.
- [ ] 1.3 Widen `validate_creds` and `validate_kind_preconditions` to receive the CONNECTION
  address, and add the direct-storage arm: require a non-empty storage base path, reject
  `warehouse`, `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`, `use_sigv4`,
  and `use_vended_credentials` while accepting an explicit `false`, and check scheme agreement.
- [ ] 1.4 Add `adapter/direct_storage_properties.rs`: parse and validate `NAMESPACE`,
  `MERGE_SCHEMA` (default TRUE), and `HIVE_PARTITIONING` (parsed and validated, not acted on), and
  compose the storage base path from the CONNECTION address and `NAMESPACE` with a single `/`
  join. Reject an unparseable value rather than defaulting.
- [ ] 1.5 Add the admission-limited store builder to `scan/object_store.rs`: one `LimitStore` cap
  of 16 and a connection-retention budget derived from that same constant.
- [ ] 1.6 Unit tests in the sibling `_tests.rs` files for 1.1 to 1.5, including the
  credential-safety assertion that no rejection message carries a credential value.

### 2. Parquet directory seam and the widening pair owner

- [ ] 2.1 Add `types/widening.rs` owning the 13 recorded relaxation pairs as production code, with
  one function returning the wider of two Arrow types or no answer. Leave
  `scan/type_relaxation_tests.rs`'s concrete 17-entry `supported_relaxation_pairs` list intact as
  the pin, and assert it AGAINST that owner rather than generating it from the owner: every listed
  pair resolves to its wider member, a curated refusal set returns no answer, and
  `arrow_castability_pins_every_supported_relaxation_pair` keeps its recorded assertions. [expert]
- [ ] 2.2 Add `adapter/parquet_directory.rs` listing: recursive, `*.parquet` only, every path
  segment beginning `_` or `.` excluded, sizes carried from the listing response, deterministic
  order, and `key=value` segments split into a per-file map left unread.
- [ ] 2.3 Add the footer fold: bounded concurrent footer reads through the caller's store, pairwise
  widening in listing order, union of column sets in first-appearance order, every column
  NULLABLE, uppercase-fold name collisions rejected, and an unfoldable pair failing with the
  column, both types, and both file paths. [expert]
- [ ] 2.4 Add the merge-mode argument selecting every footer or exactly the first file's footer,
  returning the same file list either way.
- [ ] 2.5 Return the parsed per-file Parquet metadata PAIRED with the files whose footers the mode
  read and ABSENT for every other listed file, so a consumer reads presence per file rather than
  indexing positionally against the file list. Under the fold-every-file mode every listed file
  carries metadata and #412 re-reads no footer. Under the sample-one-file mode exactly one does.
- [ ] 2.6 Unit tests in `adapter/parquet_directory_tests.rs` and `types/widening_tests.rs` over
  fixture Parquet files written in the test.

### 3. Neutral catalog types and the Parquet type mapping

- [ ] 3.1 Add `TableFormat::Parquet`, the no-data-file `SkipReason` variant, and
  `ColumnSourceType::Parquet` carrying an Arrow tag string to `crates/lakehouse-catalog`.
- [ ] 3.2 Extend `crates/lakehouse-catalog/tests/catalog_public_surface.rs` to construct and
  observe all three added variants from its external vantage, adding no source-text assertion.
- [ ] 3.3 Add the third arm to `column_source_type_to_exasol`: read the tag back through the
  existing tag parser and return `arrow_to_exasol_type`'s answer, resolving an unparseable tag to
  `VARCHAR(2000000)` so the resolver stays infallible.
- [ ] 3.4 Unit tests in `types/mapping_tests.rs` pinning the third arm, the declared-type and
  Arrow-tag lockstep, and every unchanged Iceberg and Unity answer.
- [ ] 3.5 Widen the scan-spec tag vocabulary in `types/mapping.rs` to cover every Arrow type
  `compatible_exasol_type` admits, adding `arrow_type_to_tag` and `arrow_type_from_tag` entries for
  `Int8`, `Int16`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `LargeUtf8`, and `Timestamp` at every
  `TimeUnit` in both the naive and the tz-aware form, updating the doc comment that scopes the
  vocabulary to `iceberg_type_to_arrow`, and keeping every recorded tag spelling and parse answer
  byte-identical. Add the round-trip test asserting that each admitted Arrow type renders to a tag
  and parses back to itself, and that the string fallback still answers for every refused type.
  Leave the Delta reader's `byte` and `short` mapping on the `int32` tag UNCHANGED, per the
  `vs-adapter/delta-type-mapping` delta, which supersedes that mapping's recorded reason without
  changing its answer. Keep `every_natively_representable_delta_type_maps_to_its_own_arrow_tag`
  (`crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs`) passing with no
  edit to any expected tag.

### 4. Direct-storage catalog client and adapter wiring

- [ ] 4.1 Add `adapter/direct_storage.rs` declaring `DirectStorageCatalogClient` in
  `lakehouse-engine` and implementing `CatalogClient`, with a doc comment stating why the
  implementor is not in the catalog crate.
- [ ] 4.2 Implement enumeration: first-level directories are tables, a loose file under the base
  path is ignored, a nested directory contributes files, and a directory with no data file is
  reported as skipped with the new neutral reason. Implement the trait's other required method,
  `load_table`, as a clear error naming the direct-storage kind and stating that a direct-storage
  table is not loaded through the catalog trait, never a panic and never an empty or synthesized
  table, because the pushdown path resolves this kind from its composed root instead.
- [ ] 4.3 Implement column resolution through the seam, applying the string substitution for a
  nested or unrepresentable column before rendering each Arrow tag.
- [ ] 4.4 Make `construct_catalog_client` fallible, pass it the raw `NAMESPACE` property, and add
  the direct-storage arm that builds the one admission-limited store and the client.
- [ ] 4.5 Record `TABLE_MAP` with the bare original-cased directory name, reuse the shared flatten
  and collision helpers, and reject a recovered pushdown identifier that is empty or carries a path
  separator.
- [ ] 4.6 Scope the required-`NAMESPACE` rule to the catalog kinds, leaving it optional under
  direct storage.
- [ ] 4.7 Update the compile-time signature pin in `adapter/catalog_client_tests.rs` for the
  fallible construction site.
- [ ] 4.8 Unit tests in `adapter/direct_storage_tests.rs` and `adapter/adapter_tests.rs`.

### 5. Planning, scan source, and concurrent leg resolution

- [ ] 5.1 Add the third `ScanSource` variant carrying the store, the table root, and the merge
  mode, and the third `RequestSession` variant, both matched exhaustively. [expert]
- [ ] 5.2 Add `adapter/pushdown/format/parquet_format_reader.rs` resolving files and schema through
  the seam: empty deletes, empty partition values, identity binding with no synthesized field-id,
  empty partition columns and name-mapping, static storage, the composed table root, and no refused
  column.
- [ ] 5.3 Add the third arm at the one format-reader selection site, leaving the Iceberg and Delta
  arms unchanged.
- [ ] 5.4 Replace the sequential join-leg loop in `adapter/pushdown/joins/mod.rs` with concurrent
  resolution over the request's one shared session, preserving leg-index order and byte-identical
  generated SQL and scan specs. [expert]
- [ ] 5.5 Size each broadcast-join side from the seam's listing rather than from any Parquet read.
- [ ] 5.6 Unit tests in `adapter/pushdown/format/parquet_format_reader_tests.rs`,
  `adapter/pushdown/format/format_tests.rs`, `adapter/pushdown/scan_resolution_tests.rs`,
  `adapter/pushdown/joins/joins_tests.rs`, and `scan/field_id_projection_tests.rs`.

### 6. E2E coverage

- [ ] 6.1 Add `tests/common/raw_parquet.rs`, declared once in `tests/common/mod.rs`, writing one
  `RecordBatch` to one object key through the shared `local_stack_storage()` backend, creating no
  catalog table and attaching no Iceberg field-id metadata.
- [ ] 6.2 Extend the shared harness so the per-binary virtual-schema parameters include
  `CATALOG_KIND`, the namespace property, and `MERGE_SCHEMA`, omitting any parameter a binary does
  not supply.
- [ ] 6.3 Add `tests/e2e_direct_storage_test.rs` covering the fixture-shape assertion, the
  mixed-type directory, widening, the missing column, the incompatible pair, `MERGE_SCHEMA=FALSE`
  on both paths, `stale_declaration_decides_the_emitted_width`, and the Delta-directory caveat.
  Write the incompatible-pair fixture under the ISOLATED base path
  `s3://warehouse/direct_incompatible/incompatible/` and create its virtual schema over
  `s3://warehouse/direct_incompatible/`. Keep every other fixture of this binary under
  `s3://warehouse/direct/`, because that root must enumerate successfully for the other scenarios.
- [ ] 6.4 Add the discovery, `NAMESPACE`, CONNECTION-rejection, and pushdown-parity tests to the
  same binary. Pushdown parity is three tests: `projection_filter_and_limit_reach_the_scan`,
  `group_by_aggregate_matches_the_unpushed_answer`, and
  `two_table_join_matches_the_unpushed_answer_in_one_request`. Add the join's second fixture
  directory `s3://warehouse/direct/event_labels/`, holding ONE Parquet file with an `EVENT_ID`
  64-bit integer column and a `LABEL` string column, whose `EVENT_ID` values are a subset of the
  `events/` fixture's `EVENT_ID` values. Assert the join test on the returned rows against the
  equivalent unpushed join and on `EXPLAIN VIRTUAL` naming both tables in ONE pushdown request.
  Assert nothing about one shared store there: the unit scenarios
  `one_session_or_store_per_request_serves_every_leg` and
  `one_admission_limited_store_serves_the_whole_call` own that property.
- [ ] 6.5 Add `--test e2e_direct_storage_test` to the `test-e2e` recipe and extend the existing
  build-convention guard to assert the recipe names it.
- [ ] 6.6 Add the raw-Parquet-directory scenario to `tests/e2e_azure_test.rs` under the existing
  per-run container guard.
- [ ] 6.7 Add the direct-storage recipe to `docs/catalogs.md` beside the Iceberg REST and Unity
  Catalog recipes, stating the CONNECTION shape, the three properties, the Iceberg or Delta
  directory caveat with the correct `CATALOG_KIND` named as the fix, and the plan-time footer cost.
  Name the mixed timestamp-unit case as a limitation: the recorded relaxation set holds no
  timestamp-to-timestamp row, so two files declaring one column `TIMESTAMP(MICROS)` and
  `TIMESTAMP(NANOS)` fail the fold, and `MERGE_SCHEMA = 'FALSE'` is the available workaround.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Kind, CONNECTION, properties | 1.1-1.6 | — | spec deltas `vs-adapter/catalog-kind-selection`, `vs-adapter/connection-credentials-direct-storage`, `vs-adapter/connection-credentials`, `vs-adapter/direct-storage-properties`; `crates/lakehouse-engine/src/adapter/catalog_kind.rs`, `adapter/connection.rs`, `adapter/direct_storage_properties.rs`, `scan/object_store.rs` and their `_tests.rs` siblings |
| B: Directory seam and widening owner | 2.1-2.6 | — | spec deltas `vs-adapter/parquet-directory-seam`, `datafusion-scan/type-relaxation`; `crates/lakehouse-engine/src/adapter/parquet_directory.rs`, `src/types/widening.rs`, `src/scan/type_relaxation_tests.rs` and the new `_tests.rs` siblings |
| C: Neutral catalog types, type mapping, and the tag vocabulary | 3.1-3.5 | — | spec deltas `vs-adapter/catalog-crate-public-surface-extensions`, `datafusion-scan/type-mapping`, `datafusion-scan/type-mapping-module-structure`, `vs-adapter/delta-type-mapping`, plus the tag-vocabulary scenario ONLY of `datafusion-scan/scan-execution-field-id-projection` (group E owns that delta's identity-binding scenario); `crates/lakehouse-catalog/src/client.rs`, `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, `crates/lakehouse-engine/src/types/mapping.rs`, `src/types/mapping_tests.rs`, and `src/adapter/pushdown/format/delta_schema_tests.rs` as a read-only pin the widening must leave green |
| D: Discovery client and adapter wiring | 4.1-4.8 | A, B, C (consumes the properties, the seam, and the neutral types) | spec deltas `vs-adapter/direct-storage-table-discovery`, `vs-adapter/create-virtual-schema`; `crates/lakehouse-engine/src/adapter/direct_storage.rs`, `adapter/mod.rs`, `adapter/adapter_tests.rs`, `adapter/catalog_client_tests.rs`, `adapter/direct_storage_tests.rs` |
| E: Planning, scan source, join concurrency | 5.1-5.6 | A, B, C (consumes the kind, the seam, and the table-format tag) | spec deltas `vs-adapter/direct-storage-table-planning`, `vs-adapter/pushdown-format-neutral-resolution`, `vs-adapter/delta-table-planning`, the identity-binding scenario ONLY of `datafusion-scan/scan-execution-field-id-projection` (group C owns that delta's tag-vocabulary scenario); `crates/lakehouse-engine/src/adapter/pushdown/format/mod.rs`, `format/parquet_format_reader.rs`, `adapter/pushdown/scan_resolution.rs`, `adapter/pushdown/joins/mod.rs`, `src/scan/field_id_projection_tests.rs` |
| F: E2E suites and documentation | 6.1-6.7 | D, E | spec deltas `e2e-harness/direct-storage-e2e`, `e2e-harness/direct-storage-e2e-properties`, `e2e-harness/e2e-harness`, `azure-e2e/azure-e2e-harness`; `crates/lakehouse-engine/tests/common/raw_parquet.rs`, `tests/common/mod.rs`, `tests/common/e2e_harness.rs`, `tests/e2e_direct_storage_test.rs`, `tests/e2e_azure_test.rs`, `tests/build_convention.rs`, `Makefile`, `docs/catalogs.md` |

A, B, and C run in parallel. D and E run in parallel once all three finish. F runs last.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | — | This plan removes no function, test, or module |

One near-miss is worth naming so a reviewer does not look for a removal. `arrow_to_exasol_type`
(`crates/lakehouse-engine/src/types/mapping.rs`) is public API with no call site today, retained by
a recorded exemption. Task 3.3 gives it its first consumer, so the exemption stops applying and no
removal is needed.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| catalog-kind-selection: CATALOG_KIND naming direct storage resolves the direct-storage kind | Unit | `crates/lakehouse-engine/src/adapter/catalog_kind_tests.rs` | `direct_storage_spelling_resolves_case_insensitively` |
| catalog-kind-selection: The catalog kind is matched at one construction site and nowhere else | Unit | `crates/lakehouse-engine/src/adapter/catalog_client_tests.rs` | `construction_site_is_exhaustive_and_fallible_for_three_kinds` |
| catalog-kind-selection: An unrecognized CATALOG_KIND value is rejected with a clear error | Unit | `crates/lakehouse-engine/src/adapter/catalog_kind_tests.rs` | `unrecognized_kind_error_names_all_three_spellings` |
| connection-credentials-direct-storage: A direct-storage CONNECTION carries storage credentials and a storage base path | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `direct_storage_connection_needs_no_warehouse` |
| connection-credentials-direct-storage: Catalog-authentication and vending fields are rejected, not ignored | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `direct_storage_rejects_catalog_auth_and_vending_fields` |
| connection-credentials-direct-storage: The address scheme must agree with the credential shape | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `direct_storage_scheme_must_match_credential_shape` |
| connection-credentials-direct-storage: Credential vending is unreachable under the direct-storage kind | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `direct_storage_never_reaches_credential_vending` |
| connection-credentials: One storage-credential projection and one selector serve both readers | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `every_reader_derives_the_same_backend_from_one_password` |
| direct-storage-properties: NAMESPACE is optional under direct storage and scopes the table subtree | Unit | `crates/lakehouse-engine/src/adapter/direct_storage_properties_tests.rs` | `namespace_is_optional_and_joins_the_address_with_one_slash` |
| direct-storage-properties: A NAMESPACE carrying a scheme or a leading slash is rejected at create time | Unit | `crates/lakehouse-engine/src/adapter/direct_storage_properties_tests.rs` | `namespace_with_scheme_or_leading_slash_is_rejected` |
| direct-storage-properties: MERGE_SCHEMA selects one footer or every footer, on both the refresh and the plan path | Unit | `crates/lakehouse-engine/src/adapter/direct_storage_properties_tests.rs` | `merge_schema_resolves_the_same_mode_on_both_paths` |
| direct-storage-properties: An unparseable MERGE_SCHEMA or HIVE_PARTITIONING value is rejected, never defaulted | Unit | `crates/lakehouse-engine/src/adapter/direct_storage_properties_tests.rs` | `unparseable_boolean_property_is_rejected` |
| direct-storage-properties: HIVE_PARTITIONING is parsed and validated but not yet acted on | Unit | `crates/lakehouse-engine/src/adapter/direct_storage_properties_tests.rs` | `hive_partitioning_is_parsed_and_unused` |
| create-virtual-schema: Create virtual schema enumerates every table in the configured namespace | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `namespace_is_required_for_catalog_kinds_only` |
| parquet-directory-seam: One seam answers the file list and the schema for both callers | Unit | `crates/lakehouse-engine/src/adapter/parquet_directory_tests.rs` | `one_seam_returns_files_sizes_schema_and_footers` |
| parquet-directory-seam: Data files are listed recursively in a deterministic order | Unit | `crates/lakehouse-engine/src/adapter/parquet_directory_tests.rs` | `listing_is_recursive_filtered_and_deterministic` |
| parquet-directory-seam: Footers fold into one schema under the proven-castable widening pairs | Unit | `crates/lakehouse-engine/src/adapter/parquet_directory_tests.rs` | `fold_widens_only_across_supported_pairs_and_names_conflicts` |
| parquet-directory-seam: The merge mode selects every footer or exactly one | Unit | `crates/lakehouse-engine/src/adapter/parquet_directory_tests.rs` | `merge_mode_selects_every_footer_or_the_first` |
| parquet-directory-seam: The folded column set is the union of the files' column sets | Unit | `crates/lakehouse-engine/src/adapter/parquet_directory_tests.rs` | `folded_columns_are_the_nullable_union_in_first_appearance_order` |
| parquet-directory-seam: A nested or unrepresentable Parquet type folds to the JSON string declaration | Unit | `crates/lakehouse-engine/src/adapter/parquet_directory_tests.rs` | `nested_and_unrepresentable_types_fold_to_the_string_declaration` |
| parquet-directory-seam: The declaration decides the emitted width and the footer decides the structure | Integration | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `stale_declaration_decides_the_emitted_width` |
| direct-storage-table-discovery: The direct-storage client is a boxed catalog client declared in the engine crate | Unit | `crates/lakehouse-engine/src/adapter/direct_storage_tests.rs` | `client_is_reachable_as_a_boxed_catalog_client` |
| direct-storage-table-discovery: The direct-storage client is a boxed catalog client declared in the engine crate (load_table clause) | Unit | `crates/lakehouse-engine/src/adapter/direct_storage_tests.rs` | `load_table_returns_a_clear_error_naming_the_direct_storage_kind` |
| direct-storage-table-discovery: A first-level directory under the base path is a table | Unit | `crates/lakehouse-engine/src/adapter/direct_storage_tests.rs` | `first_level_directories_are_the_tables` |
| direct-storage-table-discovery: A table's columns and data files come from the one shared directory seam | Unit | `crates/lakehouse-engine/src/adapter/direct_storage_tests.rs` | `columns_and_files_come_from_the_shared_seam` |
| direct-storage-table-discovery: A first-level directory holding no data file is skipped, not failed | Unit | `crates/lakehouse-engine/src/adapter/direct_storage_tests.rs` | `directory_with_no_data_file_is_skipped_with_a_neutral_reason` |
| direct-storage-table-discovery: Table naming and the TABLE_MAP round trip reuse the shared helpers | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `table_map_records_the_bare_directory_name_and_round_trips` |
| direct-storage-table-discovery: One admission-limited object store serves every table of one adapter call | Unit | `crates/lakehouse-engine/src/adapter/direct_storage_tests.rs` | `one_admission_limited_store_serves_the_whole_call` |
| direct-storage-table-planning: The format reader is selected at the same one site for a third source | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/format_tests.rs` | `third_scan_source_selects_the_parquet_reader` |
| direct-storage-table-planning: A direct-storage table resolves its files and schema through the shared seam | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/parquet_format_reader_tests.rs` | `resolved_scan_carries_identity_bound_fields_and_no_deletes` |
| direct-storage-table-planning: Every plan-time footer is read and the resulting cost is stated | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/parquet_format_reader_tests.rs` | `plan_reads_selected_footers_and_lists_every_file` |
| direct-storage-table-planning: An Iceberg or Delta table directory read as raw Parquet ignores its table format | Integration | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `delta_directory_read_as_raw_parquet_returns_tombstoned_rows` |
| pushdown-format-neutral-resolution: One catalog session per request serves every table the request resolves | Unit | `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs` | `one_session_or_store_per_request_serves_every_leg` |
| pushdown-format-neutral-resolution: The catalog kind is matched at one added construction site and nowhere else | Unit | `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs` | `request_session_has_one_variant_per_kind` |
| pushdown-format-neutral-resolution: Capability advertisement stays blind to the catalog kind | Unit | `crates/lakehouse-engine/src/adapter/capabilities_tests.rs` | `capabilities_are_identical_under_three_kinds` |
| pushdown-format-neutral-resolution: A request's table resolutions run concurrently rather than one after another | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/joins_tests.rs` | `join_legs_resolve_concurrently_in_leg_index_order` |
| delta-table-planning: The format reader is selected at one site and refuses a mismatched pairing | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/format_tests.rs` | `delta_arm_refuses_a_parquet_tagged_table` |
| delta-type-mapping: Every Delta type Exasol represents natively maps to its own Arrow tag | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | `every_natively_representable_delta_type_maps_to_its_own_arrow_tag` |
| catalog-crate-public-surface-extensions: The Parquet table format, the Parquet column source, and the no-data-file skip reason extend the crate's public surface through an explicit reviewed edit | Integration | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `added_neutral_variants_are_reachable_from_outside_the_crate` |
| type-mapping: A Parquet-sourced column maps through the Arrow-input direction | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `parquet_source_maps_through_the_arrow_input_direction` |
| type-mapping: The scan-spec tag vocabulary covers every Arrow type the compatible-type classifier admits | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `every_classifier_admitted_arrow_type_round_trips_through_the_tag_vocabulary` |
| scan-execution-field-id-projection: Every supported primitive initial-default survives the scan-spec serialization round-trip | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `widened_primitive_tag_list_round_trips_every_encoded_initial_default` |
| type-mapping-module-structure: One arm list decides both the Exasol type string and the JSON-fallback flag | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `classifier_answers_both_functions_unchanged` |
| type-mapping-module-structure: One DECIMAL parser serves every Exasol type-string consumer | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `parquet_producer_emits_only_in_range_decimal_strings` |
| type-relaxation: A narrow physical column binds to the current wider logical type and is cast per file | Unit | `crates/lakehouse-engine/src/scan/type_relaxation_tests.rs` | `identity_bound_narrow_column_is_cast_per_file` |
| type-relaxation: The supported pair set answers a plan-time widening question from one production owner | Unit | `crates/lakehouse-engine/src/types/widening_tests.rs` | `widening_owner_answers_every_supported_pair_and_refuses_the_rest` |
| scan-execution-field-id-projection: A logical field carrying no binding key binds by its own name | Unit | `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs` | `identity_binding_spans_files_with_different_column_sets` |
| direct-storage-e2e: The direct-storage binary is wired into the suite gate | Unit | `crates/lakehouse-engine/tests/build_convention.rs` | `make_test_e2e_runs_the_direct_storage_binary` |
| direct-storage-e2e: A raw-Parquet fixture writer puts data files with no catalog | Integration | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `fixture_files_carry_the_written_physical_encoding` |
| direct-storage-e2e: A directory of mixed-type Parquet files is declared and queried end to end | Integration | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `mixed_type_directory_is_declared_and_queried` |
| direct-storage-e2e: A column typed narrowly in one file and widely in another returns every row | Integration | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `widened_column_returns_every_row` |
| direct-storage-e2e: A file whose column set is a subset returns NULL for the columns it lacks | Integration | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `missing_column_returns_null_for_its_file` |
| direct-storage-e2e: A column no widening rule folds fails the refresh naming the column and the files | Integration | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `incompatible_pair_fails_refresh_naming_column_and_files` |
| direct-storage-e2e: MERGE_SCHEMA FALSE declares the sampled footer's type on the refresh and the scan path | Integration | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `merge_schema_false_declares_the_sampled_type_on_both_paths` |
| direct-storage-e2e: A Delta table directory read as raw Parquet returns its tombstoned rows | Integration | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `delta_directory_read_as_raw_parquet_returns_tombstoned_rows` |
| direct-storage-e2e-properties: Only first-level directories holding a data file become virtual tables | Integration | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `discovery_serves_only_first_level_directories_with_data` |
| direct-storage-e2e-properties: NAMESPACE scopes discovery to a subtree of the CONNECTION address | Integration | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `namespace_scopes_discovery_to_a_subtree` |
| direct-storage-e2e-properties: A CONNECTION the direct-storage kind cannot accept is rejected at create time | Integration | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `invalid_connection_fails_create_virtual_schema` |
| direct-storage-e2e-properties: Projection, filter, and LIMIT reach the direct-storage scan | Integration | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `projection_filter_and_limit_reach_the_scan` |
| direct-storage-e2e-properties: Projection, filter, and LIMIT reach the direct-storage scan (GROUP BY clause) | Integration | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `group_by_aggregate_matches_the_unpushed_answer` |
| direct-storage-e2e-properties: Projection, filter, and LIMIT reach the direct-storage scan (two-table join clause) | Integration | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `two_table_join_matches_the_unpushed_answer_in_one_request` |
| e2e-harness: Every E2E binary provisions the scan path from one shared harness definition | Integration | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `direct_storage_binary_provisions_from_the_shared_harness` |
| azure-e2e-harness: End-to-end scan over a raw Parquet directory on ADLS returns correct rows | Integration | `crates/lakehouse-engine/tests/e2e_azure_test.rs` | `direct_storage_over_adls_returns_correct_rows` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Direct-storage discovery | `CREATE VIRTUAL SCHEMA lhds USING lhvs.ADAPTER WITH CATALOG_CONNECTION = 'DS_CONN' CATALOG_KIND = 'DIRECT_STORAGE';` then `SELECT TABLE_NAME FROM SYS.EXA_ALL_TABLES WHERE TABLE_SCHEMA = 'LHDS';` | One uppercased row per first-level directory that holds a Parquet file |
| Direct-storage properties | Repeat the statement above with `NAMESPACE = 'sub' MERGE_SCHEMA = 'FALSE'` | Only the subtree's directories appear, with column types taken from one sampled footer |
| Schema merge | `SELECT COUNT(*) FROM lhds.WIDENED;` over a directory whose files declare one column narrow and wide | The summed row count of both files, no error and no NULL |
| Unfoldable pair | `REFRESH VIRTUAL SCHEMA lhds;` over a directory holding a string and an integer under one column name | A failure naming the column, both types, and both file paths |
| Unrecognized kind | Repeat the create statement with `CATALOG_KIND = 'ICEBERG_REST'` | A rejection naming the value and all three accepted spellings |
| Pushdown | `EXPLAIN VIRTUAL SELECT a FROM lhds.EVENTS WHERE b > 1 LIMIT 5;` | Pushed SQL carrying the projection, the predicate, and the limit, and no credential value |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures, with the docker compose stack started first |
| Azure E2E | `make test-e2e-azure` | 0 failures, with the Azure variables set |
| Lint | `cargo clippy --all-targets` | 0 errors and 0 warnings |
| Format | `cargo fmt` | No changes |
