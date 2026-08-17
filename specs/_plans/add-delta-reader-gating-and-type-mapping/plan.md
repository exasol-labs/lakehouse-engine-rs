# Plan: add-delta-reader-gating-and-type-mapping

## Summary

Refuse a Delta table whose reader protocol version or reader features this engine does not implement
end to end, and complete the Delta schema type mapping so every remaining Delta type either maps to an
Arrow tag or is refused per column with a named reason. Closes issue #322.

## Design

### Context

Issue #320 wired production pushdown into the Delta path, so a Delta table is now query-reachable. Two
gaps travelled with it, both recorded as scoped exceptions in `vs-adapter/delta-table-planning`:

1. **No reader-feature gating.** `delta_kernel` reads a table using `typeWidening` or `variantType`
   without error (spike #325 verified this against both fixtures), and `DeltaSnapshot::active_files`
   builds its scan `.without_row_transforms()`. A widened column is therefore read with each older data
   file's OLD physical type against the table's NEW logical type — wrong values, no error.
2. **A ten-type mapping.** `delta_type_to_arrow_tag` maps `boolean`, `integer`, `long`, `float`,
   `double`, `string`, `date`, `timestamp`, `timestamp_ntz`, and in-domain `decimal`. Every other Delta
   type refuses the whole TABLE with an error citing issue #322 — this plan — as its tracker.

The plan's shape was forced by an empirical finding about the project's own
"incompatible Arrow types → JSON `VARCHAR`" convention. Verified against `arrow-cast` 58.3's
`can_cast_types`: `Struct → Utf8` is `false` (`(Struct(_), _) => false`) and `Map → Utf8` is `false`
(it reaches `(_, Utf8) => from_type.is_primitive()`). `raw_scan` registers the logical schema as the
DataFusion table schema and DataFusion validates physical-against-logical castability at file open, so
neither type ever reaches the JSON conversion — on either table format. `Binary → Utf8` IS castable but
replaces every non-UTF-8 byte sequence with NULL. Completing the convention for struct, map, and binary
is therefore not a mapping change; it is a design problem, filed as issue #350.

- **Goals** — refuse an unsupported Delta reader protocol or reader feature at plan time before any log
  replay; map every remaining Delta type either to an Arrow tag or to a named per-column refusal; keep
  `stats_all_types` queryable on its 13 mappable columns so issue #322's E2E acceptance is met with the
  fixtures already vendored.
- **Non-Goals** — type widening (issue #349); real JSON rendering for struct, map, and binary on either
  table format (issue #350); strict JSON conformance of the existing `array` text rendering (#350);
  per-file statistics and filter-based file pruning (issue #321); any change to Iceberg behavior or to
  the `ScanSpec` wire format.

### Decision

#### Architecture

```
  DeltaFormatReader::resolve_scan
        │
        ├─ checked_table_root / effective_storage        (unchanged)
        │
        └─ read_delta_log
              │
              ├─ DeltaSnapshot::open ────────────────────────────────────┐
              │     Snapshot::builder_for(...).build()                   │
              │     ▼                                                    │  gate INSIDE the
              │   delta_protocol::ensure_readable(                        │  constructor: no path
              │       min_reader_version, reader_features)  ── refuse ────┤  can obtain an
              │     ▼                                                    │  ungated snapshot
              │   Ok(DeltaSnapshot)  ◀── gated by construction ──────────┘
              │
              ├─ build_delta_table_schema
              │     per column: classify Delta type
              │        native  → own Arrow tag        (List A)
              │        text    → "utf8" tag           (List B)
              │        refused → no LogicalField, record (name, reason)  (List C)
              │     no mappable column at all → refuse the whole table
              │
              └─ active_files                                   (unchanged)
                    ▼
              ResolvedScan { …, refused_columns }
                    ▼
  handle_pushdown / joins::planning        ── ONE gate per resolve site ──
        refuse when  refused_columns ∩ ( column nodes in request JSON
                                        ∪ final projection )  ≠ ∅
        runs BEFORE the zero-active-files early return
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Gate inside the constructor | `DeltaSnapshot::open` | A snapshot's existence proves its protocol was checked. A gate at the reader's entry point instead would leave the constructor reachable from a second caller — including a test — that then records ungated behavior as correct. |
| Default-deny allow-list | `delta_protocol::ensure_readable` | The code enumerates the five supported reader features and refuses the `_` remainder, so a `delta_kernel` upgrade adding a `TableFeature` variant refuses it rather than admitting it unreviewed. |
| Consumer-defined inputs, not the provider's type | `ensure_readable(i32, Option<&[TableFeature]>)` | `delta_kernel::actions::Protocol` has no constructor reachable from this crate, so a `&Protocol` parameter would be untestable without a real log. Taking the two values makes the gate a pure function, unit-testable per feature, and keeps the `internal-api` reach at exactly one extraction site. |
| Refused column absent from the logical schema | `build_delta_table_schema` | Defense in depth. The adapter gate supplies the clear message; the absence guarantees that a gate miss fails with a DataFusion unresolved-column error instead of emitting a silently-NULLed `binary` column. |
| Total recursive JSON walk, not a per-clause list | the adapter gate | The gate collects every `column` node in the pushdown request rather than enumerating select list, WHERE, GROUP BY, ORDER BY, aggregate arguments, and join conditions. A per-clause list silently omits every capability added after it. |
| One shared decimal predicate | `exasol_representable_catalog_decimal` | `datafusion-scan/type-mapping` already requires exactly one owner of the Exasol catalog-decimal domain; the Delta classifier reads it rather than copying it. |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Refuse struct, map, binary, and variant instead of mapping them to JSON `VARCHAR` | Complete the convention as issue #322's scope text asks | `arrow-cast` reports no `Struct → Utf8` and no `Map → Utf8` cast, so the convention is unreachable for them; `Binary → Utf8` silently NULLs non-UTF-8 bytes. Building it properly is issue #350 for BOTH formats. |
| Refusal scoped per COLUMN, not per table | Keep the shipped table-scoped refusal | `stats_all_types` carries `binary_col`, `map_col`, and `nested_struct`, so table scope leaves the fixture — and every real Delta table with one struct column — wholly unqueryable, and issue #322's "varied Delta types return expected Exasol types and values" acceptance criterion unreachable without authoring a new Spark fixture. |
| `array<E>` mapped recursively on `E` | Blanket `utf8` tag for every array | `can_cast_types` recurses: `array<integer>` is castable, `array<struct<…>>` and `array<variant>` are not. A blanket tag would push `unshredded_variant`'s `array_of_variants` into an opaque file-open cast error. |
| `byte`/`short` reuse the existing `int32` tag | Add `int8`/`int16` tags to the shared vocabulary | Exasol gives Int8, Int16, and Int32 the same `DECIMAL(precision, 0)` shape with no Exasol-visible distinction, and the physical `Int8`/`Int16` widens to logical `Int32` losslessly. A new tag would touch the cross-format classifier every reader reads, for no observable difference. |
| Gating and type mapping become two new sibling features | Grow `vs-adapter/delta-table-planning` | That feature already carries nine scenarios across replay, partitions, deletion vectors, column mapping, credentials, dispatch, and Iceberg parity. A protocol gate and a full type-surface mapping are distinct reasons to change, each with its own normative protocol citations. |
| Refused-column list on `ResolvedScan` | A new `ScanSpec` field | The scan never reads it, so a wire field would need the `ScanSpec` format-neutrality rule widened for nothing. |
| Version bounds read from `delta_kernel`'s public constants | Literals `1` and `3` | `MIN_VALID_RW_VERSION` and `MAX_VALID_READER_VERSION` are plain `pub` and need no cargo feature, so the readable range keeps one owner. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| `vs-adapter/delta-reader-feature-gating` | NEW | `vs-adapter/delta-reader-feature-gating/spec.md` |
| `vs-adapter/delta-type-mapping` | NEW | `vs-adapter/delta-type-mapping/spec.md` |
| `vs-adapter/delta-table-planning` | CHANGED | `vs-adapter/delta-table-planning/spec.md` |
| `vs-adapter/pushdown-module-structure` | CHANGED | `vs-adapter/pushdown-module-structure/spec.md` |
| `e2e-harness/unity-catalog-e2e-harness-delta-queries` | CHANGED | `e2e-harness/unity-catalog-e2e-harness-delta-queries/spec.md` |

## Impact

Two Delta behaviors change for operators, both deliberately.

**A Delta table using an unsupported reader feature stops returning rows and starts returning an
error.** Tables declaring `typeWidening`, `typeWidening-preview`, `variantType`,
`variantType-preview`, `variantShredding`, `variantShredding-preview`, `catalogManaged`,
`catalogOwned-preview`, `adaptiveMetadata-preview`, or a reader feature `delta_kernel` 0.26 does not
recognize are refused at plan time. Such a table was previously query-reachable and its results were
not trustworthy — `typeWidening` in particular read older data files with their pre-widening physical
type. This is a breaking change for anyone querying such a table, and the correct one: the prior
behavior returned wrong rows. Type widening is tracked as issue #349.

**A Delta table carrying a `binary`, `struct`, `map`, or `variant` column becomes queryable on its
other columns.** The shipped behavior refuses the whole table; this plan refuses only the requests that
read or emit such a column, including `SELECT *`. No previously-succeeding query starts failing, and
the refusal message now names the actual cause and cites issue #350 instead of the closed issue #322.
`byte`, `short`, `void`, both interval types, out-of-Exasol-domain `decimal`, and `array` of a mappable
element type become queryable where they were refused.

No Iceberg behavior changes. No `ScanSpec` wire field is added or altered, so no scan-spec golden
encoding moves. Version impact: `feat` — MINOR bump on `crates/lakehouse-engine` (0.36.0 → 0.37.0).

## Dependencies

| Dependency | Detail |
|------------|--------|
| `delta_kernel` 0.26 `internal-api` cargo feature | Already enabled. Adds `Snapshot::table_configuration`, `TableConfiguration::protocol`, `Protocol::reader_features`, `Protocol::min_reader_version`, and the `TableFeature` enum. `extract_enabled_reader_features` and `check_reader_version_range` are `pub(crate)` WITHOUT `#[internal_api]` and are NOT reachable — the gate implements the equivalent logic itself. |
| `delta_kernel::table_features::{MIN_VALID_RW_VERSION, MAX_VALID_READER_VERSION}` | Plain `pub`; no cargo feature needed. |
| Vendored Delta fixtures | `stats-all-types`, `type-widening`, `unshredded-variant`, `table-with-dv-small`, `multi-part-stats`, `basic_partitioned`, `cdf-column-mapping-{id,name}-mode` already vendored under `scripts/unity/fixtures/` and already seeded by `scripts/unity/seed.sh`. No new fixture. |
| Issue #349 | Type-widening support. Filed, out of scope, cited in the gate's refusal text. |
| Issue #350 | Real JSON rendering for struct/map/binary on both table formats. Filed, out of scope, cited in the type refusals. |

## Implementation Tasks

1. **Reader-protocol gate**
   1. Add `format/delta_protocol.rs` with a failing test that a `typeWidening-preview` reader feature is
      refused by name, then implement `ensure_readable(min_reader_version, reader_features)` — the
      default-deny allow-list of `columnMapping`, `deletionVectors`, `timestampNtz`, `v2Checkpoint`,
      `vacuumProtocolCheck`, refusing the `_` remainder and `TableFeature::Unknown(_)`.
   2. Extend the gate with the version-range check against `MIN_VALID_RW_VERSION` and
      `MAX_VALID_READER_VERSION`, ordered before the per-feature check, and the legacy-protocol
      (`reader_features == None`) pass.
   3. Make the refusal name every refused feature in one error, sorted, and cite issue #349 for
      `typeWidening` and `typeWidening-preview`.
   4. Call the gate from inside `DeltaSnapshot::open`, extracting the two values through
      `table_configuration().protocol()` — the `internal-api` reach — and add the integration test over
      a synthetic log proving the refusal precedes any schema read, partition-column read, or file
      replay. [expert]
   5. Add integration tests over the vendored fixtures: `type-widening` and `unshredded-variant`
      refused; `table-with-dv-small`, `multi-part-stats`, `stats-all-types`, `basic_partitioned`,
      `cdf-column-mapping-id-mode`, and `cdf-column-mapping-name-mode` all still resolve.

2. **Delta type classification**
   1. Add the failing unit tests asserting `can_cast_types(physical, Utf8)` for `Binary`,
      `List(Int32)`, `Interval(YearMonth)`, `Interval(DayTime)`, out-of-domain `Decimal128`, a
      POPULATED `Struct`, a `Map`, and a `List(Struct)`, pinning the three sets' membership to arrow's
      own answer.
   2. Extend `delta_type_to_arrow_tag` to map the native set — adding `byte` and `short` to the
      existing `int32` tag — and the text-rendered set: out-of-domain `decimal`, `void`,
      `interval year to month`, `interval day to second`. Read the decimal domain from the shared
      `exasol_representable_catalog_decimal` predicate.
   3. Implement the RECURSIVE `array<E>` rule: `utf8` when `E` is itself in the native or
      text-rendered set, refused when `E` is in the refused set, applied at any nesting depth. [expert]
   4. Replace `unmapped_delta_type_error` with per-type refusal reasons for `binary`, `struct`, `map`,
      and `variant` — each naming the actual cause, citing issue #350 for the first three, and citing
      no closed issue.
   5. Assert `void` reads as all-NULL end to end through the scan's missing-physical-column path, over
      a synthetic Delta log declaring a `void` column, and under `name` column mapping so the physical
      name that no data file carries is exercised too. [expert]
   6. Verify the one unverified link in the text-rendered set: that the scan's OWN field-id and
      physical-name expression adapter — not only DataFusion's default one — performs the
      physical-to-logical cast for a column whose physical Arrow type is `List(Int32)` and whose logical
      tag is `utf8`. Cover it with a scan-level integration test over a Parquet file carrying a list
      column, so a missing cast in that adapter fails here rather than in the E2E suite. [expert]

3. **Per-column refusal scoping**
   1. Add `RefusedColumn` to `format/mod.rs`, add `ResolvedScan::refused_columns`, re-export the type
      at the `pushdown` façade, update both probe `use` lists and their stated counts to 26 and 16, and
      return an empty list from the Iceberg reader.
   2. Change `build_delta_table_schema` to emit no `LogicalField` for a refused column and return the
      refused list alongside the schema, classifying the TYPE before reading the column-mapping binding
      key; thread the list through `read_delta_log` and `resolve_scan`.
   3. Refuse the whole table when no column is mappable, with the `raw_scan` empty-logical-schema
      justification in the reader's own doc comment.
   4. Implement the gate: one total recursive walk over the pushdown request JSON collecting every
      `column` node's uppercased name, unioned with the final projection, intersected with the refused
      list. [expert]
   5. Call the gate at both resolve sites — `handle_pushdown` BEFORE the zero-active-files early
      return, and `joins::planning` per resolved side — and carry the list on `ResolvedJoinSide`. [expert]
   6. Add integration tests: a mappable-only projection plans; a projection naming a refused column
      refuses; a WHERE clause on a refused column refuses while its select list names only mappable
      columns; `SELECT *` refuses; an empty-file-list table naming a refused column refuses rather than
      returning an empty result; a join leg reaching a refused column refuses.

4. **E2E coverage**
   1. Replace `unity_delta_unmappable_table_fails_the_query_loud` with the reader-feature refusal test
      over `TYPE_WIDENING` and `UNSHREDDED_VARIANT`, asserting the feature name, the #349 citation, no
      column-typed error, session survival, and no credential leak.
   2. Add the varied-types test over `STATS_ALL_TYPES`: the 13-column projection, `SELECT COUNT(*) = 4`,
      the declared Exasol type per column, non-NULL `byte_col`/`short_col`, a bracketed `array_col`
      rendering, and a captured pushdown SQL asserting the scan UDF drives the query.
   3. Add the per-column refusal test over `STATS_ALL_TYPES`: `binary_col`, `map_col`,
      `nested_struct`, `SELECT *`, and a refused column in a WHERE clause all refuse with the #350
      citation, while the 13-column projection still succeeds in the same run.

5. **Docs and fixture provenance**
   1. Update `scripts/unity/fixtures/PROVENANCE.md`'s `stats-all-types` row and its `#322 gating note`
      to state what actually shipped: `timestampNtz` is mapped rather than gated, and `array`/`map`/
      `struct`/`binary` split between the text-rendered and refused sets rather than all reaching JSON
      `VARCHAR`. Update `scripts/unity/README.md`'s two `#322` rows to match.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 2.1 |
| Group B | 1.2, 1.3, 2.2, 2.3, 2.4 |
| Group C | 1.4, 3.1 |
| Group D | 1.5, 2.5, 2.6, 3.2, 3.3 |
| Group E | 3.4 |
| Group F | 3.5, 3.6 |
| Group G | 4.1, 4.2, 4.3, 5.1 |

Sequential dependencies:
- Group A → Group B (the gate function and the castability pins exist before their cases are added)
- Group B → Group C (the gate is complete before it is wired into `DeltaSnapshot::open`; the classifier
  is complete before `ResolvedScan` carries its output)
- Group C → Group D
- Group D → Group E → Group F (the gate function exists before its call sites)
- Group F → Group G (E2E runs against the finished behavior)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `unmapped_delta_type_error` in `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema.rs` | Its single generic message and its issue-#322 citation are replaced by per-type refusal reasons citing #350. |
| Test | `struct_array_and_map_columns_are_refused_not_widened_to_json` in `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | Asserts `array` is refused; `array<integer>` is now mapped. Split into the per-type refusal tests and the recursive-array tests. |
| Test | `unity_delta_unmappable_table_fails_the_query_loud` in `crates/lakehouse-engine/tests/e2e_unity_test.rs` | Asserts every query against `STATS_ALL_TYPES` fails; 13 of its columns are now queryable. Replaced by the three E2E tests in task 4. |

No production code becomes unreachable: the gate and the classifier are additions, and
`build_delta_table_schema`'s existing body is extended rather than replaced.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| delta-reader-feature-gating: A reader feature outside the allow-list refuses the table before any log replay | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_protocol_tests.rs` | `a_reader_feature_outside_the_allow_list_is_refused_by_its_protocol_name` |
| delta-reader-feature-gating: A reader feature outside the allow-list refuses the table before any log replay | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `an_unsupported_reader_feature_is_refused_before_any_schema_or_file_read` |
| delta-reader-feature-gating: Every allow-listed reader feature keeps its table queryable | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `every_shipped_fixture_whose_reader_features_are_allow_listed_still_resolves` |
| delta-reader-feature-gating: A reader protocol version outside the readable range is refused | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_protocol_tests.rs` | `a_reader_version_outside_the_kernels_range_is_refused_before_any_feature_check` |
| delta-reader-feature-gating: A legacy-protocol table carries no explicit reader-feature list | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `a_legacy_reader_version_table_passes_the_gate_and_keeps_its_column_mapping_mode` |
| delta-reader-feature-gating: The gate runs inside snapshot construction, so no resolution path can bypass it | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `the_protocol_gate_runs_inside_snapshot_construction` |
| delta-type-mapping: Every Delta type Exasol represents natively maps to its own Arrow tag | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | `every_natively_representable_delta_type_maps_to_its_own_arrow_tag` |
| delta-type-mapping: A Delta type Exasol cannot represent natively is surfaced as a VARCHAR rendering | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | `a_type_exasol_cannot_represent_is_tagged_utf8_including_a_recursive_array` |
| delta-type-mapping: A Delta type Exasol cannot represent natively is surfaced as a VARCHAR rendering | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `a_void_column_reads_as_all_null_under_name_column_mapping` |
| delta-type-mapping: A Delta type Exasol cannot represent natively is surfaced as a VARCHAR rendering | Integration | `crates/lakehouse-engine/src/scan/raw_scan_tests.rs` | `a_list_column_tagged_utf8_is_cast_by_the_field_id_expression_adapter` |
| delta-type-mapping: A Delta type whose Arrow form cannot be rendered faithfully is refused by name | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | `binary_struct_map_and_variant_are_refused_with_their_own_reason_citing_350` |
| delta-type-mapping: A refused column refuses only the requests that read or emit it | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `a_refused_delta_column_refuses_only_the_requests_that_reference_it` |
| delta-type-mapping: A refused column refuses only the requests that read or emit it | Integration | `crates/lakehouse-engine/src/adapter/pushdown/joins/joins_tests.rs` | `a_refused_delta_column_reached_through_a_join_leg_is_refused` |
| delta-type-mapping: A Delta table with no mappable column is refused as a whole | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | `a_table_whose_every_column_is_refused_is_refused_as_a_whole` |
| delta-type-mapping: The castability claims behind the mapping are asserted, not assumed | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `arrow_castability_to_utf8_pins_the_three_delta_type_sets` |
| delta-table-planning: The Delta reader is reached from production pushdown under the Unity Catalog kind (CHANGED) | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `a_unity_catalog_pushdown_gates_the_delta_protocol_and_refuses_per_column` |
| pushdown-module-structure: The pushdown façade admits exactly one item for the Delta refused-column list | Unit | `crates/lakehouse-engine/src/adapter/pushdown_surface_probe_tests.rs` and `crates/lakehouse-engine/tests/pushdown_public_surface.rs` | compile-time `use` probes (26 and 16 items) |
| e2e: A Delta table using an unsupported reader feature fails the query loud | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_unsupported_reader_feature_fails_the_query_loud` |
| e2e: A Delta table spanning varied types returns the expected Exasol types and values | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_varied_types_return_their_expected_exasol_types_and_values` |
| e2e: A Delta column this engine cannot render refuses only the queries that name it | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_refused_column_refuses_only_the_queries_naming_it` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| `vs-adapter/delta-reader-feature-gating` | `make unity-up` then `exapump sql --dsn "$LH_DSN;validateservercertificate=0" "SELECT * FROM DELTA_E2E.TYPE_WIDENING LIMIT 1"` | Query fails with an error naming `typeWidening-preview` and citing issue #349; no row returned; the session survives a follow-up `SELECT 1 FROM DUAL`. |
| `vs-adapter/delta-type-mapping` | `exapump sql --dsn "$LH_DSN;validateservercertificate=0" "SELECT BYTE_COL, SHORT_COL, INT_COL, LONG_COL, FLOAT_COL, DOUBLE_COL, DATE_COL, TIMESTAMP_COL, TIMESTAMP_NTZ_COL, STRING_COL, DECIMAL_COL, BOOLEAN_COL, ARRAY_COL FROM DELTA_E2E.STATS_ALL_TYPES"` | 4 rows; `BYTE_COL` and `SHORT_COL` carry their real logged values, not NULL; `ARRAY_COL` carries a bracketed VARCHAR rendering of its integer elements. |
| `vs-adapter/delta-type-mapping` | `exapump sql --dsn "$LH_DSN;validateservercertificate=0" "SELECT MAP_COL FROM DELTA_E2E.STATS_ALL_TYPES"` | Query fails with an error naming `MAP_COL`, its Delta `map` type, and issue #350; the 13-column query above still succeeds afterwards on the same connection. |
| `e2e-harness/unity-catalog-e2e-harness-delta-queries` | `make test-e2e-unity` | Exit 0; the three replacement tests pass; every previously-passing Delta query scenario still passes. |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E (Unity/Delta) | `make test-e2e-unity` | 0 failures |
| E2E (Iceberg regression) | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |
