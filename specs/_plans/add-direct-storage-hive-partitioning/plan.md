# Plan: add-direct-storage-hive-partitioning

## Summary

Implements GitHub issue #408: `HIVE_PARTITIONING` (default `TRUE`) turns the `key=value` directory
segments of a `DIRECT_STORAGE` table into `VARCHAR` partition columns. Plan-time pruning on those
values drops files before their footers are read.

## Design

### Context

Issue #407 shipped the direct-storage kind. Its shared seam (`adapter/parquet_directory.rs`)
already splits `key=value` segments into an unread per-file map, and its planning reader ships
empty partition columns. `FileEntry.partition_values`, `CommonScanSpec.partition_columns`, and
`scan/partition_values.rs` already serve Delta, so the scan side needs no change. The seam reads
every listed file's footer at plan time. Pruning is what bounds that cost on a partitioned table.
Issue #412 confirms the intended order: "#408 prunes from directory path segments before any byte
is read".

- **Goals**
  - One owner of partition discovery, the key union, and the collision check for both paths.
  - Plan-time pruning that is sound and runs before any footer of a pruned file is read.
- **Non-Goals**
  - Footer-statistics pruning (#412).
  - Typed partition columns and any `ScanSpec`, `FileEntry`, or `LogicalField` change.

### Decision

#### Architecture

```
createVirtualSchema                     pushdown
DirectStorageCatalogClient              TableScanResolver ──(declared columns)──┐
        │ keep-all                              │                                │
        ▼                                       ▼                                ▼
resolve_parquet_directory(store, prefix, DirectoryOptions, keep)     ParquetFormatReader
  1. list data files (unchanged rules)                                  builds keep from
  2. parse key=value segments        (if hive_partitioning)             PartitionPredicate
  3. declare keys: union | sampled file's; fill every file's map        over filter_json
  4. drop files keep() rejects       (no footer read yet)
  5. read footers: kept files | first unfiltered file
  6. fold, dropping a column that collides with a key (uppercase fold); fail only on two keys
     colliding with each other
  7. schema = folded columns ++ partition columns (nullable Utf8)
        │                                       │
        ▼                                       ▼
CatalogColumns (unchanged mapping)      FileEntry{partition_values, encoded path},
                                        partition_columns, logical schema
                                        + declared columns absent from the fold
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Seam-owned options value (`DirectoryOptions { merge_mode, hive_partitioning }`) | `adapter/parquet_directory.rs`, derived once by `DirectStorageProperties::directory_options()` | The seam takes switch values, never property names. One derivation site replaces the two `MergeMode::for_merge_schema` calls, so the paths cannot disagree |
| Injected file-keep predicate | seam parameter, built by the planning reader | The seam stays filter-JSON-free. Pruning still runs between listing and footer reads |
| Set-valued three-valued evaluation | `adapter/pushdown/format/partition_predicate.rs` | A partition-only node is exact per file. Every other node is `{TRUE, FALSE, NULL}`. Keep iff TRUE is reachable. Sound by construction, with no separate "exact" bookkeeping |

**Quick Diagnostic** (new module `partition_predicate`, new seam parameters):

| Question | Answer |
|----------|--------|
| One-sentence responsibility | `partition_predicate`: decide from a file's partition values whether any of its rows can satisfy the filter |
| Easier to call than to reimplement | Yes: two calls (`from_filter`, `keeps`) hide the Exasol filter grammar and three-valued logic |
| Internal change forces edits elsewhere | No: the seam sees only a predicate over `BTreeMap<String, Option<String>>` |
| Doc comment states the reasoning | Required on `PartitionPredicate` (why a non-partition node counts as any truth value) and on the seam's keep parameter (why it runs before footers) |
| One owner per decision | Discovery, union, collision: seam. Translation: `partition_predicate`. Property-to-switch mapping: `directory_options()` |
| Boundaries visible | The seam names no Exasol type. The predicate module names no object store |
| Tactical shortcut with follow-up | None taken |
| Dependencies point inward | The reader depends on the seam and the predicate module. The seam depends on neither |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Prune inside the seam, before footer reads | Prune the returned file list after the fold | The issue exists to bound plan-time footer cost. Post-fold pruning reads every footer anyway |
| Fill an absent declared column from the request's ALREADY-FIXED schema (`involvedTables[].columns`, as Exasol echoes the table's own `REFRESH`-time declaration on every pushdown request), never re-derived from whichever files a query's pruning happens to keep | Accept a failing query | A pruned fold can lack a declared column. The scan rejects a projected column its logical schema does not name (`scan/raw_scan.rs` projects by name over the aliased inner select). Plan-time pruning narrows which files are read, not what an ABSENT column's declared type is. A PRESENT column's Arrow type still comes from the kept files' fold, per the recorded `vs-adapter/parquet-directory-seam` scenario "The declaration decides the emitted width and the footer decides the structure" |
| Sample-one-file keeps sampling the first UNFILTERED file | Sample the first kept file | Keeps the recorded invariant that both paths sample one file. Costs at most one extra footer read |
| Percent-encode direct-storage `FileEntry.path` segments | Leave raw keys | The scan builds `ObjectMeta` through `ListingTableUrl::parse`, which percent-decodes. A raw key holding `%2F` (the `region=a%2Fb` fixture) would address `region=a/b` |
| A partition key that folds onto a Parquet column overrides it (directory value wins) when EVERY file carrying that column also carries the key's segment. A file missing the segment while the key is declared FAILS the refresh, naming the column, the key, and that file's path | Fail the refresh for every collision | Common Hive-table practice encodes the value both ways when every file follows one layout. DuckDB's `MultiFileReader::BindOptions` overrides the value only when every file carries the segment and raises a mismatch error otherwise, which is the precedent this feature follows (decision [7]). Two DIFFERENT keys folding to the same name still fail, and there is no precedent for choosing between two directory encodings or for silently reading a stored value the override rule already claimed |

The Iceberg table spec and the Delta protocol do not govern this plan. Hive-style directory
partitioning appears in neither. The direct-storage kind implements neither format, per the recorded
`vs-adapter/direct-storage-table-planning` specification check. The new feature spec states this.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| direct-storage-hive-partitioning | NEW | `vs-adapter/direct-storage-hive-partitioning/spec.md` |
| parquet-directory-seam | CHANGED | `vs-adapter/parquet-directory-seam/spec.md` |
| direct-storage-table-planning | CHANGED | `vs-adapter/direct-storage-table-planning/spec.md` |
| direct-storage-properties | CHANGED | `vs-adapter/direct-storage-properties/spec.md` |
| direct-storage-table-discovery | CHANGED | `vs-adapter/direct-storage-table-discovery/spec.md` |
| direct-storage-e2e-properties | CHANGED | `e2e-harness/direct-storage-e2e-properties/spec.md` |

## Impact

- **Behavior change for existing virtual schemas, at the NEXT QUERY, not only after the next
  `REFRESH`.** `HIVE_PARTITIONING` defaults to `TRUE`, and the seam runs discovery on every
  pushdown request (task 1.5), so an un-refreshed schema changes as soon as it is next queried. A
  direct-storage table under `key=value` directories gains `VARCHAR` partition columns at its next
  query. A table whose key folds onto a Parquet column of the same name in EVERY file that carries
  the column silently starts reading that column from the directory value instead of the file, at
  its next query. Before the next `REFRESH`, that column still carries its previously declared,
  non-`VARCHAR` Exasol type, so `scan/emit.rs::coerce_column` casts the directory text against that
  type and fails on a value the type cannot hold, until the next `REFRESH` redeclares the column as
  `VARCHAR`. A table where some but not all files carrying that column also carry the key's segment
  starts failing at its next query that keeps the segment-less file. A query whose pruning drops
  that file still succeeds, so the failure surfaces only once a query reaches it, and it also
  surfaces at its next `CREATE`/`REFRESH VIRTUAL SCHEMA`, naming the column, the key, and one such
  file. This is a NEW
  failure: the shipped seam carries no key-collision check today, so a table under both `Year=` and
  `year=` directories only starts failing at its next query and its next `CREATE`/`REFRESH` once
  this feature ships. `HIVE_PARTITIONING = 'FALSE'` restores the previous behavior in all cases
  across the upgrade.
- A declared column absent from every file a query keeps reads NULL instead of failing the query.
- A direct-storage file whose key holds `%`, `#`, or `?` becomes scannable.
- `docs/catalogs.md` documents the property, the partition-column rules, the `MERGE_SCHEMA = 'FALSE'`
  layout precondition, and the pruning cost bound.

## Dependencies

- `percent-encoding` 2.x as a direct dependency of `crates/lakehouse-engine` (already in
  `Cargo.lock` at 2.3.2 through `url` and `object_store`), declared in `[workspace.dependencies]`.
- The implementing commit references `Closes #408`.

## Implementation Tasks

### 1. Seam: options, partition discovery, key union, keep predicate

- [ ] 1.1 Add `DirectoryOptions { merge_mode, hive_partitioning }` to `adapter/parquet_directory.rs`
  and `DirectStorageProperties::directory_options()` as its ONE derivation site. Replace every
  `merge_mode: MergeMode` carrier with `options: DirectoryOptions`: `DirectStorageCatalogClient`
  (`new`, `over_store`, field), `RequestSession::DirectStorage`, `ScanSource::DirectParquet`,
  `ParquetFormatReader`. Remove the "not yet acted on (#408)" doc comment on `hive_partitioning`.
- [ ] 1.2 Replace `partition_segments` with partition discovery gated by `hive_partitioning`: the
  `^[^/=]+=[^/]*$` segment rule, verbatim key, percent-decoded value (raw text kept when the result
  is not valid UTF-8), `__HIVE_DEFAULT_PARTITION__` and empty value to `None`, deepest repeat wins.
  Rename `ParquetFile.partition_segments` to `partition_values: BTreeMap<String, Option<String>>`.
  This pre-fill map holds a key exactly when the file's own path carries that `key=` segment,
  regardless of the decoded value, so its key SET (before task 1.3's fill) is each file's own RAW
  carried-keys set: a `key=__HIVE_DEFAULT_PARTITION__` or `key=` file carries the key, a file with no
  `key=` segment at all does not. Keep this pre-fill map (or its key set) available to task 1.4, since
  task 1.3's fill makes every file's map carry every declared key and so erases the distinction.
- [ ] 1.3 Declare the keys over the UNFILTERED listing: first-appearance union (listing order,
  shallow to deep) under `FoldEveryFile`, the first file's keys under `SampleOneFile`. Fill every
  file's map with every declared key and drop undeclared keys. Add
  `ParquetDirectory.partition_columns` and append each key to `schema` as a nullable `Utf8` field
  after the folded columns.
- [ ] 1.4 Uppercase-fold collision handling, split by kind, both checks bounded by which files'
  footers and paths the merge mode makes visible: under `FoldEveryFile` every kept file's footer and
  path, under `SampleOneFile` only the one sampled file's footer and path. Between two declared KEYS
  (e.g. `Year=` and `year=` in the same table, since 1.3 declares keys only from the paths each mode
  reads): FAILS, at declaration, naming both spellings and a file carrying each. Under
  `SampleOneFile` this can only fire when the sampled file's own path itself carries both spellings.
  Between a declared KEY and a folded Parquet COLUMN (e.g. key `k=1` and file column `K`): when
  EVERY file whose footer was read, and that carries the stored column, also carries the key's
  segment in ITS OWN RAW carried-keys set from 1.2 (not the post-fill map from 1.3, so a file under
  `key=__HIVE_DEFAULT_PARTITION__` or `key=` counts as carrying the segment while a truly
  segment-less file does not), do NOT fail. DROP the colliding column from `fold_schemas`'s output
  instead of adding it, so the key's own nullable `Utf8` partition column (step 1.3) is the only
  column of that name and the dropped column is never read or bound at scan time. When a file whose
  footer was read carries the stored column but its raw carried-keys set has NO `key=` segment,
  while the key is declared from another file, FAIL at declaration, naming the colliding column, the
  key, and the path of one such segment-less file. That file has neither a directory value nor
  permission to fall back to its own stored value. Under `SampleOneFile` both the override check and
  the segment-less-file check run against the one sampled file's own footer and raw carried-keys set
  alone: an unsampled file carrying the identical problem goes undetected, matching the one-layout
  precondition `MERGE_SCHEMA = 'FALSE'` already documents. Under `hive_partitioning = false` no key
  exists, so none of these checks run.
- [ ] 1.5 Add the file-keep predicate parameter to `resolve_parquet_directory`. Evaluate it after
  key declaration and before any footer read. Return only kept files. `FoldEveryFile` reads kept
  files' footers only. `SampleOneFile` reads the first unfiltered file's footer, kept or not.
  `DirectStorageCatalogClient::list_tables` and, until group B lands, `ParquetFormatReader` pass a
  keep-all predicate.
- [ ] 1.6 Unit tests for the seam, properties, and enumeration scenarios in the Scenario Coverage
  table. Replace `key_value_path_segments_are_split_into_a_per_file_map`. Update every call site in
  the census below.

**Call-site census for group A** (whole crate, including `tests/`):

- `resolve_parquet_directory`: `adapter/direct_storage.rs:84`,
  `adapter/pushdown/format/parquet_format_reader.rs:59`, `adapter/direct_storage_tests.rs:465`,
  `adapter/parquet_directory_tests.rs` lines 204, 273, 314, 360, 401, 427, 472, 477, 531, 538, 584,
  623, 674, 709.
- `MergeMode::for_merge_schema`: `adapter/mod.rs:610`, `adapter/pushdown/scan_resolution.rs:124`.
- `DirectStorageCatalogClient::new` / `over_store`: `adapter/mod.rs:607`,
  `adapter/direct_storage_tests.rs:40`.
- `merge_mode` carriers: `adapter/pushdown/scan_resolution.rs` lines 59, 124, 179, 187.
  `adapter/pushdown/format/mod.rs` lines 117, 173, 175. `parquet_format_reader.rs` lines 29, 38,
  44, 59. `parquet_format_reader_tests.rs:48-58`. `format/format_tests.rs:171-174`.
- `partition_segments`: `parquet_directory.rs` lines 43, 111, 144. `parquet_directory_tests.rs`
  lines 319, 327.
- No `tests/*.rs` integration file names any of these symbols today.

### 2. Planning: pruning, file entries, declared-column completion, E2E, docs

- [ ] 2.1 Add a `declared_columns: &[(String, String)]` parameter (Exasol name, Exasol type) to
  `TableScanResolver::resolve` and a `declared_columns` field to `ScanSource::DirectParquet`. Only
  the direct-storage arm reads it. Pass `&col_types` at `adapter/pushdown/mod.rs:242`. Add the
  parameter to `resolve_one_join_side` (`joins/planning.rs:354`), fed by
  `involved_table_columns(request, &leaf.table_name)` at `joins/mod.rs:210`. Update the nine
  `resolve` calls in `scan_resolution_tests.rs` (lines 42, 193, 236, 237, 282, 286, 359, 363, 446)
  and `format_tests.rs:171`.
- [ ] 2.2 Add `adapter/pushdown/format/partition_predicate.rs`. Translate the filter JSON once. Per
  file, evaluate each node to its set of reachable truth values: exact three-valued logic for
  `predicate_equal`, `predicate_notequal`, `predicate_in_constlist`, `predicate_is_null`,
  `predicate_is_not_null`, `predicate_less`, `predicate_lessequal`, `predicate_greater`,
  `predicate_greaterequal`, and `predicate_between` of a partition column (on either side of a
  comparison) against non-empty `literal_string` values (two literals for `predicate_between`),
  lifted through `predicate_and`, `predicate_or`, and `predicate_not`. Compare string values with
  Rust's native `str`/`String` ordering (byte/codepoint order). Task 2.7's range-pruning E2E test
  verifies live, against a fixture (`REGIONS`, task 2.6) whose values rank differently under byte
  order than under case-insensitive or locale order, that this matches Exasol's own `VARCHAR`
  comparison, per this project's SQL-capability verification rule. Do not assume it. A mismatch
  found by that test stops implementation and returns this plan to planning, since this task's
  range-pruning design depends on the ordering match holding. Any other node is
  `{TRUE, FALSE, NULL}`. Resolve a filter column against the file's map keys by the uppercase fold.
  Keep a file iff TRUE is reachable. [expert]
- [ ] 2.3 In `ParquetFormatReader::resolve_scan`, build the keep predicate from `filter_json`. Copy
  each file's partition values into its `FileEntry`, and percent-encode each relative path segment.
  Encode exactly the characters that do not survive `ListingTableUrl::parse` (`%`, `#`, `?`), so
  a plain path stays byte-identical. Return the seam's `partition_columns`. Append each declared
  column absent from the fold and the partition columns (uppercase fold) as a nullable
  `LogicalField` typed by
  `types::mapping::exasol_type_to_arrow`, falling back to `Utf8`, with no binding key and no nested
  descriptor. Replace the doc comment that says `filter_json` is unread.
- [ ] 2.4 Unit tests in `partition_predicate_tests.rs` (every operator including the four range
  comparisons and `predicate_between`, NULL values, AND/OR/NOT over non-partition nodes, non-string
  and empty literals, uppercase resolution) and `parquet_format_reader_tests.rs`. The path test
  asserts that `reconstruct_abs_uri` followed by `ListingTableUrl::parse` yields the listed `Path`
  for keys holding `%`, `#`, `?`, a space, and a non-ASCII character. A further
  `parquet_format_reader_tests.rs` test asserts that a predicate keeping no file resolves to zero
  files, reads zero footers, and returns without error.
- [ ] 2.5 E2E harness: add `VsProps::with_hive_partitioning`. Make `write_parquet_fixture` write its
  key verbatim with `ObjectStorePath::parse`, so `region=a%2Fb` stays literal as Spark writes it.
- [ ] 2.6 E2E fixtures in `tests/e2e_direct_storage_test.rs`. Under `BASE_DIRECT`:
  `sales/year=2026/month=09/p1.parquet` (`ID`, `AMOUNT`, `DISCOUNT`),
  `sales/year=2025/month=__HIVE_DEFAULT_PARTITION__/p2.parquet` (`ID`, `AMOUNT`),
  `sales/year=2024/month=01/p3.parquet` (`ID`, `AMOUNT`), disjoint from the other two years so a
  range predicate has something to exclude on both sides, `encoded/region=a%2Fb/p.parquet`,
  `mixed/A/p.parquet`, `mixed/year=2026/p.parquet`, and `regions/region=B/p.parquet`,
  `regions/region=a/p.parquet`, `regions/region=é/p.parquet`, each also carrying a `TIMESTAMP`
  column `TS`, whose three `REGION` values rank differently under byte order than under
  case-insensitive or locale order, so a range query proves the actual comparison order rather than
  passing under any order. Under a new isolated `s3://warehouse/direct_hive_collision/` with its own
  CONNECTION: `collision_override/k=1/p.parquet` carrying column `K` STORED as `99`, deliberately
  different from the directory's `1` so a test can tell which source it read. Under a SEPARATE
  isolated base `s3://warehouse/direct_hive_collision_missing/` with its own CONNECTION (so this
  failing fixture never shares a base, and therefore a `CREATE VIRTUAL SCHEMA` statement, with the
  passing override fixture): `collision_missing_segment/k=1/p1.parquet` (`K` STORED as `99`, under a
  `k=` segment) alongside `collision_missing_segment/p2.parquet` (no `k=` segment, `K` STORED as
  `42`), proving the `CREATE`/`REFRESH` failure for a file that lacks the colliding key's segment
  while another file of the same table declares that key. Add `VS_HIVE_OFF` over `BASE_DIRECT` with
  `HIVE_PARTITIONING = 'FALSE'`, and a second `HIVE_PARTITIONING = 'FALSE'` virtual schema over the
  `direct_hive_collision_missing` base, proving that base declares its table without error once hive
  partitioning, and so the key collision, is turned off.
- [ ] 2.7 E2E tests named in Scenario Coverage. The union test also asserts that `MIXED` under
  `VS_DIRECT_NARROW` declares no `YEAR`. The override collision test asserts `CREATE VIRTUAL SCHEMA`
  succeeds, `COLLISION_OVERRIDE` declares `K` exactly once as `VARCHAR(2000000)`, and the `k=1`
  file's row reads `K = '1'` (the directory value, never its own stored `99`). It also creates a
  `HIVE_PARTITIONING = 'FALSE'` virtual schema over the same base and asserts the `k=1` file's row
  there reads `K = '99'`, its own stored value, unaffected by any override. A second test, over its
  own isolated `direct_hive_collision_missing` base, asserts `CREATE VIRTUAL SCHEMA` over
  `COLLISION_MISSING_SEGMENT` fails with an error naming `K`, `k`, and
  `collision_missing_segment/p2.parquet`'s path (`REFRESH`'s use of the same seam is covered by the
  seam's own unit test, task 1.6, not by an E2E `REFRESH` on an already-failing schema). It also
  creates a `HIVE_PARTITIONING = 'FALSE'` virtual schema over that same `collision_missing_segment`
  base and asserts `CREATE VIRTUAL SCHEMA` succeeds, because no key exists to collide. The pruning
  test's SQL half asserts, for both the equality query and the range query over `SALES`, that
  `EXPLAIN VIRTUAL` names only the `year=2026` file, checking live that Exasol pushes both predicate
  shapes in the translated form and that the range comparison excludes `year=2025` and `year=2024`.
  A separate ordering test runs `WHERE REGION > 'Z'`, `WHERE REGION < 'z'`, and
  `WHERE REGION BETWEEN 'B' AND 'a'` over `REGIONS`, in-process against the resolved file list and
  through `EXPLAIN VIRTUAL`, and asserts the kept file set equals the value set Exasol itself
  selects natively on the same connection (for example
  `SELECT r FROM (VALUES ('B'),('a'),('é')) AS t(r) WHERE r > 'Z'`, and the matching query for each
  of the other two predicates). One variant of the ordering test adds the conjunct
  `WHERE SECOND(TS, 3) > 1` (the declined-filter shape recorded in
  `vs-adapter/pushdown-declined-filter-self-apply`) to the `REGION` range predicate, so that
  conjunct reaches Exasol's own outer `WHERE` self-apply while plan-time pruning still runs on the
  same query's `REGION` conjunct, and asserts the same row set results. A mismatch between the kept
  file set and Exasol's native ordering fails the test and, per task 2.2, stops implementation. A
  last test, `zero_matching_files_prune_to_zero_rows_without_error`, asserts live that
  `WHERE YEAR = '2099'` resolves zero files and returns zero rows without error (the zero-footer
  claim is asserted only by task 2.4's unit test, which controls the store and can count reads).
  Update the `DEEP` assertion in
  `discovery_scopes_to_first_level_directories_and_namespace_narrows_to_a_subtree` to two columns.
- [ ] 2.8 Update `docs/catalogs.md`: the `HIVE_PARTITIONING` row, one paragraph on partition
  columns (`VARCHAR`, union, the two-keys-collide-fails rule scoped to `MERGE_SCHEMA` TRUE, since
  under `'FALSE'` that check covers only the sampled file's own keys, vs. the key-overrides-column
  precedence rule, the segment-less-file-fails-refresh rule for a table where some but not all files
  carry the colliding key's segment: under `MERGE_SCHEMA` TRUE any such file fails the statement,
  under `'FALSE'` only the sampled file itself is checked and an unsampled file with the same
  problem stays undetected, the `MERGE_SCHEMA = 'FALSE'` layout precondition, and one upgrade
  sentence naming `HIVE_PARTITIONING = 'FALSE'` as the way to keep this feature's previous,
  pre-upgrade behavior across the change), and the plan-time footer cost paragraph (a pruned file's
  footer is not read, and pruning covers equality, `IN`, NULL checks, and range/`BETWEEN`
  comparisons).

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Seam and property plumbing | 1.1-1.6 | — | spec deltas `vs-adapter/parquet-directory-seam`, `vs-adapter/direct-storage-properties`, `vs-adapter/direct-storage-table-discovery`, and the discovery, union, collision, and `FALSE` scenarios of `vs-adapter/direct-storage-hive-partitioning`; `crates/lakehouse-engine/src/adapter/parquet_directory.rs`, `adapter/direct_storage.rs`, `adapter/direct_storage_properties.rs`, `adapter/mod.rs`, their `_tests.rs` siblings, and the `merge_mode` carriers in the census |
| B: Planning, pruning, E2E | 2.1-2.8 | A (consumes the seam API and edits the same carriers in `format/mod.rs`, `scan_resolution.rs`, `parquet_format_reader.rs`) | spec deltas `vs-adapter/direct-storage-table-planning`, `e2e-harness/direct-storage-e2e-properties`, and the pruning and absent-column scenarios of `vs-adapter/direct-storage-hive-partitioning` (plus all its scenarios for E2E); `crates/lakehouse-engine/src/adapter/pushdown/format/parquet_format_reader.rs`, new `format/partition_predicate.rs`, `format/mod.rs`, `pushdown/scan_resolution.rs`, `pushdown/mod.rs`, `pushdown/joins/mod.rs`, `pushdown/joins/planning.rs`, their `_tests.rs` siblings, `tests/common/raw_parquet.rs`, `tests/common/e2e_harness.rs`, `tests/e2e_direct_storage_test.rs`, `docs/catalogs.md` |

B runs after A. The groups share three carrier files, so they run in sequence rather than in
parallel.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `partition_segments` in `adapter/parquet_directory.rs` | Replaced by gated partition discovery (task 1.2) |
| Test | `key_value_path_segments_are_split_into_a_per_file_map` in `adapter/parquet_directory_tests.rs` | Replaced by the discovery unit tests |
| Field | `merge_mode` on the four carriers in the census | Replaced by `options: DirectoryOptions` |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| hive: A key=value directory segment declares a VARCHAR partition column | Integration | `tests/e2e_direct_storage_test.rs` | `hive_segments_declare_varchar_partition_columns_with_decoded_values` |
| hive: A key=value directory segment declares a VARCHAR partition column | Unit | `src/adapter/parquet_directory_tests.rs` | `directory_segments_follow_the_key_value_rule_and_decode_values` |
| hive: A table's partition columns are the union of its files' keys | Integration | `tests/e2e_direct_storage_test.rs` | `mixed_layout_unions_partition_keys_and_nulls_the_missing_key` |
| hive: A table's partition columns are the union of its files' keys | Unit | `src/adapter/parquet_directory_tests.rs` | `declared_keys_are_the_ordered_union_and_fill_every_file`, `sample_mode_declares_only_the_sampled_files_keys` |
| hive: Two partition keys that fold to the same name fail the refresh | Unit | `src/adapter/parquet_directory_tests.rs` | `two_declared_keys_folding_to_the_same_name_fail_naming_both_spellings` |
| hive: A partition key that names a Parquet column overrides it | Integration | `tests/e2e_direct_storage_test.rs` | `partition_key_colliding_with_a_parquet_column_overrides_it` |
| hive: A partition key that names a Parquet column overrides it | Unit | `src/adapter/parquet_directory_tests.rs` | `a_key_folding_onto_a_column_drops_the_column_and_keeps_the_key` |
| hive: A file missing the colliding key's segment fails the refresh | Integration | `tests/e2e_direct_storage_test.rs` | `partition_key_collision_with_a_missing_segment_fails_the_refresh` |
| hive: A file missing the colliding key's segment fails the refresh | Unit | `src/adapter/parquet_directory_tests.rs` | `a_file_missing_the_colliding_keys_segment_fails_the_fold_naming_it`, `a_sampled_files_missing_segment_fails_the_fold_under_sample_one_file` |
| hive: HIVE_PARTITIONING = FALSE reads key=value segments as plain directories | Integration | `tests/e2e_direct_storage_test.rs` | `hive_partitioning_false_declares_no_partition_columns` |
| hive: HIVE_PARTITIONING = FALSE reads key=value segments as plain directories | Unit | `src/adapter/parquet_directory_tests.rs` | `hive_partitioning_off_parses_no_segment_and_checks_no_collision` |
| hive: A predicate on partition columns prunes files before their footers are read | Integration | `tests/e2e_direct_storage_test.rs` | `partition_filter_prunes_the_resolved_file_list` |
| hive: A predicate on partition columns prunes files before their footers are read | Unit | `src/adapter/pushdown/format/partition_predicate_tests.rs`, `src/adapter/pushdown/format/parquet_format_reader_tests.rs` | `partition_nodes_evaluate_under_three_valued_logic`, `range_and_between_nodes_evaluate_under_three_valued_logic`, `a_non_partition_node_never_prunes`, `a_partition_filter_prunes_files_before_their_footers_are_read` |
| hive: A declared column absent from every kept file reads NULL | Integration | `tests/e2e_direct_storage_test.rs` | `a_column_only_pruned_files_carry_reads_null` |
| hive: A declared column absent from every kept file reads NULL | Unit | `src/adapter/pushdown/format/parquet_format_reader_tests.rs` | `a_declared_column_absent_from_kept_files_is_added_as_a_null_field` |
| hive: A predicate on partition columns prunes files before their footers are read | Integration | `tests/e2e_direct_storage_test.rs` | `range_pruning_matches_exasols_native_varchar_ordering` |
| hive: A predicate on partition columns prunes files before their footers are read | Integration | `tests/e2e_direct_storage_test.rs` | `zero_matching_files_prune_to_zero_rows_without_error` |
| hive: A predicate on partition columns prunes files before their footers are read | Unit | `src/adapter/pushdown/format/parquet_format_reader_tests.rs` | `a_predicate_keeping_no_file_reads_no_footer_and_errors_never` |
| seam: One seam answers the file list and the schema for both callers | Unit | `src/adapter/parquet_directory_tests.rs` | `one_seam_returns_files_sizes_schema_and_footers` |
| seam: Data files are listed recursively in a deterministic order | Unit | `src/adapter/parquet_directory_tests.rs` | `listing_is_recursive_filtered_and_deterministic` |
| seam: The merge mode selects every footer or exactly one | Unit | `src/adapter/parquet_directory_tests.rs` | `merge_mode_selects_every_footer_or_the_first` |
| seam: A file-keep predicate narrows the files before any footer is read | Unit | `src/adapter/parquet_directory_tests.rs` | `a_keep_predicate_narrows_files_before_any_footer_is_read` |
| planning: The format reader is selected at the same one site for a third source | Unit | `src/adapter/pushdown/format/format_tests.rs` | `third_scan_source_selects_the_parquet_reader` |
| planning: A direct-storage table resolves its files and schema through the shared seam | Unit | `src/adapter/pushdown/format/parquet_format_reader_tests.rs` | `resolved_scan_carries_identity_bound_fields_and_no_deletes`, `file_entry_paths_round_trip_to_the_listed_object` |
| planning: A direct-storage table resolves its files and schema through the shared seam | Integration | `tests/e2e_direct_storage_test.rs` | `hive_segments_declare_varchar_partition_columns_with_decoded_values` (reads the `encoded/` file) |
| planning: The kept files' footers are read at plan time and the resulting cost is stated | Unit | `src/adapter/pushdown/format/parquet_format_reader_tests.rs` | `plan_reads_selected_footers_and_lists_every_file` |
| planning: The kept files' footers are read at plan time and the resulting cost is stated | Integration | `tests/e2e_direct_storage_test.rs` | `partition_filter_prunes_the_resolved_file_list` |
| discovery: A table's columns and data files come from the one shared directory seam | Unit | `src/adapter/direct_storage_tests.rs` | `columns_and_files_come_from_the_shared_seam` (extended with a `key=value` directory) |
| properties: HIVE_PARTITIONING reaches the shared seam on both paths | Unit | `src/adapter/direct_storage_properties_tests.rs`, `src/adapter/direct_storage_tests.rs`, `src/adapter/pushdown/scan_resolution_tests.rs` | `directory_options_derive_both_switches`, `hive_partitioning_reaches_the_seam_on_enumeration`, `hive_partitioning_reaches_the_seam_on_pushdown` |
| e2e-properties: Only first-level directories holding a data file become virtual tables | Integration | `tests/e2e_direct_storage_test.rs` | `discovery_scopes_to_first_level_directories_and_namespace_narrows_to_a_subtree` |

The pruning E2E follows `e2e_scan_test.rs:3215`. It calls `format_reader(ScanSource::DirectParquet
{ .. })` in-process against live MinIO, once unfiltered, once with `YEAR = '2026'`, and once with
`YEAR > '2025'`, and asserts a strictly lower file count for both filtered runs. It then asserts
the SQL-level rows. The ordering test runs the same in-process and `EXPLAIN VIRTUAL` checks over
the `REGIONS` fixture (task 2.6), whose values rank differently under byte order than under
case-insensitive or locale order, and asserts the kept file set matches Exasol's own native
ordering on the same connection.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| direct-storage-hive-partitioning | `make test-e2e` with the stack up, then `SELECT COLUMN_NAME, COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS WHERE COLUMN_SCHEMA = 'DIRECT_LAKEHOUSE' AND COLUMN_TABLE = 'SALES'` via `exapump` | `YEAR` and `MONTH` listed as `VARCHAR(2000000) UTF8` after `ID`, `AMOUNT`, `DISCOUNT` |
| direct-storage-hive-partitioning | `SELECT ID, YEAR, MONTH FROM DIRECT_LAKEHOUSE.SALES ORDER BY ID` | Rows 1 and 2 with `2026`, `09`. Row 3 with `2025` and a NULL `MONTH`. Row 4 with `2024`, `01` |
| direct-storage-hive-partitioning | `EXPLAIN VIRTUAL SELECT ID FROM DIRECT_LAKEHOUSE.SALES WHERE YEAR = '2026'` | The pushed file list names the `year=2026` file and no `year=2025` file |
| direct-storage-hive-partitioning | `EXPLAIN VIRTUAL SELECT ID FROM DIRECT_LAKEHOUSE.SALES WHERE YEAR > '2025'` | The pushed file list names only the `year=2026` file, excluding `year=2025` and `year=2024` |
| direct-storage-hive-partitioning | `SELECT K FROM DIRECT_HIVE_COLLISION.COLLISION_OVERRIDE` | `'1'`, the `k=1` directory value, never the file's own stored `99` |
| direct-storage-hive-partitioning | `CREATE VIRTUAL SCHEMA ... CATALOG_KIND = 'DIRECT_STORAGE' ...` over `direct_hive_collision_missing` (no `NAMESPACE`) | Fails, naming `K`, `k`, and `collision_missing_segment/p2.parquet`'s path |
| parquet-directory-seam | `cargo test -p lakehouse-engine parquet_directory` | All seam tests pass |
| direct-storage-table-planning | `SELECT REGION FROM DIRECT_LAKEHOUSE.ENCODED` | One row, `a/b` |
| direct-storage-table-discovery | `cargo test -p lakehouse-engine direct_storage` | Enumeration tests pass, the partitioned fixture declaring its key |
| direct-storage-properties | `SELECT COUNT(*) FROM SYS.EXA_ALL_COLUMNS WHERE COLUMN_SCHEMA = 'DIRECT_HIVE_OFF' AND COLUMN_TABLE = 'SALES'` | `3` (no partition column) |
| direct-storage-e2e-properties | `make test-e2e` | `discovery_scopes_to_first_level_directories_and_namespace_narrows_to_a_subtree` passes with `DEEP` declaring `ID` and `Y` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` (stack started first) | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |
