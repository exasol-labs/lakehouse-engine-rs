# Plan: add-type-relaxation

## Summary

Admit Delta type widening and Iceberg type promotion as one format-neutral read behavior — type
relaxation — by verifying the cast path this engine already has against every pair both protocols
define, allow-listing Delta's `typeWidening` reader features, and refusing by name the two Iceberg
`date` promotions a dependency gap makes unreadable. Closes issue #349.

## Design

### Context

Issue #322 refused Delta's `typeWidening` because its widening pairs were UNVERIFIED, and recorded a
justification that turns out to be wrong:

> `DeltaSnapshot::active_files` builds its kernel scan with `.without_row_transforms()`, so no
> per-file cast transform is applied. A widened column is then read with each older data file's OLD
> physical Parquet type against the table's NEW logical type — wrong values, no error.

Both halves are false. `delta_kernel` 0.26 documents `without_row_transforms()` as scoping out
*"partition column injection, column-mapping renames, and generated row ids"*, and it implements no
type-widening cast at all — its `TableFeature::TypeWidening` handling is a capability declaration and
a schema-comparison validator. There was no cast transform to discard. The cast is performed by this
engine's own chain: `register_file_list` registers the DataFusion table schema from the scan spec's
`LogicalField` list rather than from a Parquet footer, `bind_columns` renames a physical field to the
logical name that claims it without ever comparing data types, and DataFusion's
`DefaultPhysicalExprAdapter` inserts a `CastExpr` on any field inequality. That chain is installed
identically for Iceberg and Delta and is already recorded, in
`datafusion-scan/scan-execution-field-id-projection`, as delegating *"type divergence → cast"*.

So the work is verification and two named gaps, not new cast infrastructure. Three findings shaped
the plan:

1. **The vendored `type-widening` fixture is already the right shape.** Its commit 0 declares
   thirteen columns narrow and commits a physically-narrow data file; its commit 2 widens all
   thirteen in `metaData.schemaString` and commits a wide one. Both files are live. What is missing
   is the Unity Catalog registration — `seed.sh` registers 3 of the 13 columns, and a column absent
   from that list is not selectable from Exasol.
2. **Allow-listing `typeWidening` incurs a second protocol obligation.** `PROTOCOL.md` § Reader
   Requirements for Type Widening requires readers to *"validate that they support all type changes
   in the `delta.typeChanges` field … and fail when finding any unsupported type change"* — a
   per-column gate this engine does not have.
3. **Two Iceberg `date` promotions are unreadable for a reason unrelated to casting.** The Iceberg
   spec mandates inferring a manifest bound's write-time type from its byte width; `iceberg` 0.10.0
   implements that for `long`-from-4-bytes and `double`-from-4-bytes but reads `timestamp` and
   `timestamp_ns` bounds as 8 bytes unconditionally. A `date`-promoted table therefore fails inside
   manifest deserialization — for `SELECT *` as much as for a filtered query, because this engine
   loads every manifest in `ensure_supported_delete_mechanisms` before pruning ever runs — with
   `failed to convert byte slice to array`, naming neither column nor promotion. A second bounds
   decode in the same crate `unwrap()`s, so the shape has a reachable panic path, and a panic in a
   UDF makes the engine SIGKILL every sibling VM of the statement part.

- **Goals** — verify every Delta widening and Iceberg promotion pair this engine can read, end to
  end; allow-list both `typeWidening` feature names; implement the `delta.typeChanges` validation the
  protocol requires of a reader that does; refuse the two Iceberg `date` promotions by name before
  any manifest is read; put the vendored widening fixture under live assertion.
- **Non-Goals** — Iceberg `unknown` → any type, which `iceberg` 0.10.0 cannot represent at all; JSON
  rendering for struct, map, and binary (issue #350); any change to the `ScanSpec` wire format, to
  the emit path, or to the column-binding adapter; authoring a new Delta fixture, which would need a
  Spark + delta-spark one-shot job this stack does not have.

### Decision

#### Architecture

```
  PLAN TIME                                          SCAN TIME
  ─────────                                          ─────────
  Delta                                              register_file_list
   DeltaSnapshot::open                                 │ logical_schema non-empty
     └─ ensure_readable(version, readerFeatures)       ▼
          allow-list += typeWidening,             build_logical_arrow_schema
                        typeWidening-preview        (CURRENT types, never a footer)
          default-deny `_` remainder unchanged           │
     └─ build_delta_table_schema                        ▼
          per column: classify type          FieldIdExprAdapterFactory
                      validate delta.typeChanges  ├─ bind_columns  (rename only,
                      unsupported change →        │    never compares DataType)
                        RefusedColumn(name, why)  └─ DefaultPhysicalExprAdapter
                                                        │  logical != physical
  Iceberg                                               ▼
   resolve_scan                                    CastExpr(physical → logical)
     └─ refuse_date_promotion(schemas_iter)  ◀── NEW      │ inserted per FILE, into the
          field-id date in any older schema                │ physical expression tree, so
                  → timestamp/timestamp_ns now             │ filters/aggregates/joins all
          refuse: table, column, both types, issue         │ see the current type
     └─ ensure_supported_delete_mechanisms                 ▼
          (loads every manifest — the decode that    coerce_batch_to_exa_types
           fails; the gate above runs FIRST)          one generic arrow cast, unchanged
     └─ plan_files_from_table
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Verify, don't build | the whole scan half | The cast chain exists, is format-neutral, and is already recorded as delegating type divergence to `DefaultPhysicalExprAdapter`. Adding a relaxation-aware layer would duplicate a decision that already has one owner. |
| Gate on schema history, not on a decode error | `refuse_date_promotion` | The error this replaces is raised inside Avro deserialization, downstream of an `unwrap()` panic path, and carries no column name. `TableMetadata::schemas_iter` answers the same question before any object-store byte is spent. |
| Conservative refusal, stated in the doc comment | `refuse_date_promotion` | Establishing that no pre-promotion file survives requires the manifest read that fails. The cheap answer over-refuses a fully-rewritten table and says so, rather than being silently imprecise. |
| Reuse the refused-column list | `delta.typeChanges` validation | `vs-adapter/delta-type-mapping` already carries per-column refusal on `ResolvedScan` and already refuses only requests that read or emit the column. A second, table-scoped mechanism would make a readable table unreachable over a column nobody selected. |
| Exhaustive match as the tripwire | `iceberg_primitive_to_{arrow,exasol}` | Both matches already cover `PrimitiveType` with no catch-all, so an `iceberg` upgrade adding `Unknown` breaks the build instead of mapping it onto the `utf8` fallback. Pinning that with a test costs one assertion and replaces a gate that would be unreachable today. |
| Two fixture tables, not one | `create_iceberg_type_promotion_fixture.sql` | A table carrying the refused `date` promotion is refused as a whole, which would leave the readable pairs unassertable in the same table. |
| Scan-tier Parquet tests for the pairs no fixture reaches | `datafusion-scan/type-relaxation` | `byte`→`short`/`int`, `short`→`int`, and `short`→`long` have no fixture column, and authoring one needs a Spark + delta-spark job this stack lacks. A purpose-written Parquet file exercises the same cast at the same seam. |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Leave the vendored `type-widening` fixture untouched and extend `seed.sh` instead | Rewrite the fixture's Delta log to widen more columns, per the interview's premise | The premise was wrong: commit 2 already widens all thirteen columns and both data files are referenced — there is no orphan and no cosmetic-only widening. `PROVENANCE.md` also records the vendored tables as *"read fixtures — never mutated"*. Registering all thirteen columns in Unity Catalog unlocks the full coverage with no fixture byte changed. |
| Refuse Iceberg `date` → `timestamp` / `timestamp_ns` rather than support them | Support them by vendoring the missing bounds-width inference; or leave the opaque failure in place | The gap is inside `iceberg` 0.10.0's manifest decode, reached from two call sites, one of which `unwrap()`s. Vendoring a decode fix is a dependency fork for two promotion pairs; leaving it is an opaque error on every query, filtered or not. A plan-time refusal naming the column and the tracked issue is the accurately-scoped exception the project's Iceberg-spec rule requires. |
| `unknown` → any type is recorded as unreachable, with a build tripwire, and no gate is written | Write a refusal arm for `unknown`; or upgrade `iceberg` in this plan | `iceberg` 0.10.0 has no `PrimitiveType::Unknown` and no `serde` arm for the name, so such a schema fails to deserialize before any engine code runs. A gate would be unreachable from its first commit. The two exhaustive matches already fail the build if the variant appears. |
| Validate `delta.typeChanges` as a pair check, ignoring `fieldPath` | Parse the nested path grammar and validate per element | A `fieldPath` entry names a map key/value or array element; this engine refuses `map` outright and text-renders `array<E>`, so no scalar value is at risk. Parsing a path grammar to reach a column whose value never crosses as that type is cost without a consequence. |
| Encode the decimal rule as `k1 >= k2 >= 0` | `P' >= P && S' >= S`, as issue #349's summary table states | The protocol's constraint also forbids the INTEGRAL digit count shrinking, so `decimal(10,1)` → `decimal(11,3)` is illegal though both grow. The paraphrase in the issue is refuted by the specification text. |
| `date` → `timestamp_ns` covered below E2E | A `timestamp_ns` Iceberg fixture | It is refused anyway, so no read fixture is meaningful; and `datafusion-scan/scan-execution-field-id-projection` already records nanosecond timestamps as covered by unit round-trip rather than by fixture. |
| The two Iceberg tracked exceptions get their own issues | Cite #349 | #349 is this plan and closes with it; a closed issue in a shipped error reads as an unfixed gap with no owner — the same argument that moved the type refusals off #322 onto #350. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| `datafusion-scan/type-relaxation` | NEW | `datafusion-scan/type-relaxation/spec.md` |
| `vs-adapter/iceberg-type-promotion` | NEW | `vs-adapter/iceberg-type-promotion/spec.md` |
| `packaging/iceberg-type-promotion-fixture` | NEW | `packaging/iceberg-type-promotion-fixture/spec.md` |
| `vs-adapter/delta-reader-feature-gating` | CHANGED | `vs-adapter/delta-reader-feature-gating/spec.md` |
| `vs-adapter/delta-type-mapping` | CHANGED | `vs-adapter/delta-type-mapping/spec.md` |
| `e2e-harness/unity-catalog-e2e-harness-delta-queries` | CHANGED | `e2e-harness/unity-catalog-e2e-harness-delta-queries/spec.md` |

## Impact

Three behaviors change for operators.

**A Delta table using type widening becomes queryable.** Tables declaring `typeWidening` or
`typeWidening-preview` were refused at plan time; they now resolve and return their current wider
types, with each pre-widening data file cast up per file. No previously-succeeding query starts
failing. A table whose `delta.typeChanges` records a change outside the protocol's supported list is
refused per column, naming both types — the same per-column scoping already used for `binary`, `map`,
`struct`, and `variant`.

**An Iceberg table whose `date` column was promoted to `timestamp` or `timestamp_ns` is refused at
plan time instead of failing opaquely.** Such a table already failed every query with
`failed to convert byte slice to array` from inside `iceberg` 0.10.0's manifest decode, and could
reach an `unwrap()` panic. The refusal names the table, the column, both Iceberg types, and a tracked
issue. This is not a regression: no such table returned rows before.

**A column's declared Exasol type can move when a table is relaxed.** An `int` → `long` promotion
turns `DECIMAL(10,0)` into `DECIMAL(20,0)`. A virtual schema created before the change keeps
declaring the old type until `REFRESH VIRTUAL SCHEMA` runs. This is the ordinary stale-metadata
consequence of schema evolution and needs no new mechanism, but it is the case operators are most
likely to read as a bug in this change.

No `ScanSpec`, `FileEntry`, or `LogicalField` field is added or altered, so no scan-spec golden
encoding moves. No Iceberg promotion this engine already read changes its answer. Version impact:
`feat` — MINOR bump on `crates/lakehouse-engine` (0.38.0 → 0.39.0).

**The Iceberg `date` → `timestamp` refusal has no live E2E fixture; its coverage is unit-only.**
Implementation of task 6.1 found that Apache Iceberg Java never implements this promotion at any
version this stack can run — `TypeUtil.isPromotionAllowed` has no `date` case, so no conforming
Spark writer can produce the table this plan originally scoped a second fixture for. This is not a
silent scope reduction: it is the exact contingency the interview's Q4 answer pre-authorized
("fall back to unit-test coverage… rather than blocking the whole plan"), and it is recorded in
decision [14]. The refusal itself is unaffected — `refuse_date_promotion` is proven directly by unit
tests over a synthetic `TableMetadata` (task 4.3).

## Dependencies

| Dependency | Detail |
|------------|--------|
| `delta_kernel` 0.26 | `TableFeature::TypeWidening` / `TypeWideningPreview` already exist and already parse. No new cargo feature. The kernel implements no widening cast — the engine's own adapter chain performs it. |
| `iceberg` 0.10.0 | `TableMetadata::schemas_iter` and `schema_by_id` are `pub` and supply the schema history the Iceberg gate reads. `FormatVersion::V3` parses with no rejection path. `PrimitiveType` has no `Unknown` variant, and `Datum::try_from_bytes` omits the `timestamp` / `timestamp_ns` rows of the spec's bounds-width table. |
| `arrow-cast` 58.3 | `can_cast_types` is `true` for all thirteen supported pairs. `arrow::compute::cast` defaults to `safe: true`; every supported pair is a widening, so no value can overflow it. |
| Vendored `type-widening` fixture | Already vendored and seeded; needs no byte changed. `scripts/unity/seed.sh` registers 3 of its 13 columns and must register all 13. |
| `apache/spark:3.5.7` + `iceberg-spark-runtime-3.5_2.12:1.10.1` | Already pinned in `run_fixtures.sh`; authors the one new Iceberg fixture (`int`/`float`/`decimal` promotion). It cannot author a `date` → `timestamp` fixture — Iceberg Java's `TypeUtil.isPromotionAllowed` never implements that promotion at any version this stack can run; see decision [14]. |
| New tracked issue — Iceberg `date` promotion bounds-width gap | Filed by task 1.1; cited in the refusal text and in `vs-adapter/iceberg-type-promotion`. |
| New tracked issue — Iceberg `unknown` type support | Filed by task 1.1; cited as the tracked exception for the one promotion this engine cannot reach. |

## Implementation Tasks

1. **Tracked exceptions and the verification baseline**
   1. File two GitHub issues and substitute their numbers into
      `vs-adapter/iceberg-type-promotion/spec.md` and the refusal text they are cited from. Write
      both problem-focused rather than solution-prescribing. "Read Iceberg `unknown` columns once
      iceberg-rust represents the type" MUST link `apache/iceberg-rust#2581` — open enhancement "v3
      spec types — Variant, Geometry, Geography, Unknown", which adds the `Unknown` variant this
      engine's tripwire waits on — as the upstream tracking it is blocked on, and MAY cite the
      `apache/iceberg-rust#2411` v3-support epic for context. "Iceberg `date` → `timestamp` /
      `timestamp_ns` promotion unreadable: iceberg-rust omits the manifest bounds-width inference"
      MUST state that no existing `apache/iceberg-rust` issue or PR tracking this gap was found as of
      the upstream search run during planning, and stop there — this project files nothing on
      `apache/iceberg-rust`.
   2. Add `crates/lakehouse-engine/src/scan/type_relaxation_tests.rs`, declared from `scan/mod.rs`
      with `#[path = "type_relaxation_tests.rs"] mod type_relaxation;`, pinning
      `arrow::compute::can_cast_types` for all thirteen supported physical-to-logical Arrow pairs and
      asserting `long` → `double` is absent from the supported set.
   3. Extend that file with the read assertion per pair: write a Parquet file at the PHYSICAL type,
      register it under a logical schema carrying the TARGET type, run the scan, and assert the
      returned values — following `field_id_adapter_reads_renamed_column_rows` in
      `field_id_projection_tests.rs`. Cover the four pairs no fixture reaches (`byte` → `short`,
      `byte` → `int`, `short` → `int`, `short` → `long`) and a two-file case whose narrow and wide
      files are read in one query. [expert]

2. **Delta reader-feature allow-list**
   1. Add `TableFeature::TypeWidening` and `TypeWideningPreview` to `is_allow_listed`
      (`format/delta_protocol.rs`), leaving the default-deny `_` remainder untouched.
   2. Delete `describe_refused_feature` and its issue-#349 arm, inlining the plain `to_string()` the
      remaining arm produced; update `ensure_readable` accordingly.
   3. Update `delta_protocol_tests.rs`: the two tests asserting `typeWidening` refusal and the #349
      citation become tests asserting both variants PASS; the seven-feature allow-list test replaces
      the five-feature one; the refusal tests switch to a `variantType` carrier.
   4. Move `type-widening` from the refused-fixture case to the allow-listed replay list in
      `delta_replay_tests.rs`, and switch `refused_protocol_table_storage` in `pushdown_tests.rs` to
      a `variantType` carrier.

3. **Delta recorded-type-change validation**
   1. Add a `delta.typeChanges` parser reading `fromType`, `toType`, and optional `fieldPath` from a
      field's metadata, ignoring unrecognized keys — notably the superseded `tableVersion` the
      vendored fixture still carries.
   2. Implement the supported-pair predicate as the protocol's own list, with the decimal targets
      expressed as `k1 >= k2 >= 0` rather than `P' >= P && S' >= S`, and `Long` → `Double` absent.
      [expert]
   3. Wire the validation into `build_delta_table_schema` so an unsupported recorded change adds a
      `RefusedColumn` naming the column and both types, reusing the existing refused-column path; a
      `fieldPath` entry is validated by its pair alone.
   4. Add the unit tests: every supported pair accepted; `Long` → `Double` refused;
      `decimal(10,1)` → `decimal(11,3)` refused and `decimal(10,1)` → `decimal(12,3)` accepted; a
      `tableVersion` key ignored; a `fieldPath` entry validated without path parsing; a table with no
      annotation unchanged.

4. **Iceberg date-promotion refusal**
   1. Implement `refuse_date_promotion` over `TableMetadata::schemas_iter`: for each current-schema
      field whose type is `Timestamp` or `TimestampNs`, refuse when any earlier schema declares the
      same field id as `Date`, naming table, column, both types, and the tracked issue. Record the
      conservatism — no check that a pre-promotion file survives — in its own doc comment. [expert]
   2. Call it from `resolve_scan` BEFORE `ensure_supported_delete_mechanisms`, so no manifest is
      loaded and the refusal fires for an unfiltered request as well as a filtered one.
   3. Add unit tests over synthetic `TableMetadata`: a `date` → `timestamp` history refused; a
      `date` → `timestamp_ns` history refused; an unpromoted `date` column planning normally;
      `int` → `long`, `float` → `double`, and decimal-precision histories planning normally.
   4. Pin the exhaustiveness tripwire in `types/mapping_tests.rs`: assert `iceberg_primitive_to_arrow`
      and `iceberg_primitive_to_exasol` answer every `PrimitiveType` variant, so an `iceberg` upgrade
      adding `Unknown` fails the build rather than falling through to the `utf8` fallback.

5. **Delta E2E coverage**
   1. Extend `scripts/unity/seed.sh`'s `type_widening` entry from 3 columns to all 13, each at its
      WIDENED Delta type (`byte_long`/`int_long` `long`; `float_double`/`byte_double`/`short_double`/
      `int_double` `double`; `decimal_decimal_same_scale` `decimal(20,2)`;
      `decimal_decimal_greater_scale` `decimal(20,5)`; `byte_decimal` `decimal(4,1)`;
      `short_decimal` `decimal(6,1)`; `int_decimal` `decimal(11,1)`; `long_decimal` `decimal(21,1)`;
      `date_timestamp_ntz` `timestamp_ntz`). Change no fixture byte.
   2. Replace the `TYPE_WIDENING` case in `unity_delta_unsupported_reader_feature_fails_the_query_loud`
      with the `UNSHREDDED_VARIANT` case alone, dropping the now-constant `cites_349` flag and
      asserting the error does NOT cite #349.
   3. Add the widened-read E2E test over `TYPE_WIDENING`: `COUNT(*) = 2`; the ELEVEN-column
      projection for the protocol-supported columns; the declared Exasol type per column; the
      pre-widening row's real values (not NULL); the post-widening row's out-of-narrow-range values;
      `float_double`'s pre-widening value asserted as the f32's exact double expansion rather than
      `3.4`; a captured pushdown SQL proving the scan UDF drives the query; and a per-column refusal
      assertion for `byte_decimal` and `short_decimal`, which task 3.2 found derive a negative `k1`
      against the protocol's `Decimal(10+k1,k2)` base and are therefore outside the supported list
      (decision [15]).

6. **Iceberg fixture and E2E coverage**
   1. Add `scripts/spark-fixtures/create_iceberg_type_promotion_fixture.sql` authoring ONE table — a
      format-version-2 `iceberg_type_promotion` (`int`→`long`, `float`→`double`,
      `decimal(10,2)`→`decimal(20,2)`, rows written before and after each `ALTER TABLE … ALTER COLUMN
      … TYPE`) — with the header comment, ground-truth block, and drop condition
      `create_int96_timestamp_fixture.sql` models. Add its explicit `spark-sql -f` invocation line to
      `run_fixtures.sh`. No `iceberg_date_promotion` table is authored — see decision [14]: Iceberg
      Java never implements that promotion, at any version this stack can run. [expert]
   2. Add `crates/lakehouse-engine/tests/common/type_promotion_fixtures.rs` carrying the ground truth
      for `iceberg_type_promotion` in lockstep with that script, declared in `tests/common/mod.rs`
      under `exasol-e2e`; add `crates/lakehouse-engine/tests/e2e_type_relaxation_test.rs`; add that
      binary to `make test-e2e`'s `--test` list.
   3. Add the fixture-shape test asserting the pre-promotion data file's physical Parquet types are
      `INT32`, `FLOAT`, and `INT64` carrying the `DECIMAL(10,2)` logical annotation, so an
      Iceberg-side rewrite fails the suite rather than making the read test pass vacuously.
   4. Add the E2E test: `iceberg_type_promotion` returns both pre- and post-promotion rows at the
      promoted types, including a post-promotion `int_long` value outside the 32-bit range. The
      `date` → `timestamp` refusal has no E2E test — it is covered by task 4.3's unit tests over a
      synthetic `TableMetadata` alone (decision [14]).

7. **Docs and provenance**
   1. Update `scripts/unity/fixtures/PROVENANCE.md`'s `type-widening` row and its `#322 gating note`
      to state that type widening is now read rather than refused, and update the matching rows in
      `scripts/unity/README.md`.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2, 3.1, 4.4, 6.1 |
| Group B | 1.3, 2.1, 3.2, 4.1, 5.1, 6.2 |
| Group C | 2.2, 2.3, 3.3, 4.2, 6.3 |
| Group D | 2.4, 3.4, 4.3, 5.2, 5.3, 6.4, 7.1 |

Sequential dependencies:
- Group A → Group B (issue numbers exist before they are cited; the castability pins exist before
  the read assertions; the parser and the gate exist before their rules and call sites)
- Group B → Group C (the allow-list change lands before its tests are rewritten; the gate is wired
  before its tests; `seed.sh` registers the columns before the E2E asserts them)
- Group C → Group D (E2E runs against the finished behavior)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `describe_refused_feature` in `crates/lakehouse-engine/src/adapter/pushdown/format/delta_protocol.rs` | Its only special case was the `typeWidening` issue-#349 citation. With both variants allow-listed the arm is unreachable and the function collapses to `other.to_string()`. |
| Test | the `typeWidening` refusal and `#349` citation tests in `crates/lakehouse-engine/src/adapter/pushdown/format/delta_protocol_tests.rs` | Assert a refusal that no longer exists; replaced by allow-list passes and by `variantType`-carried refusal tests. |
| Test case | the `type-widening` entry in `a_vendored_fixture_declaring_a_reader_feature_outside_the_allow_list_is_refused` (`delta_replay_tests.rs`) | The fixture now replays; it moves to the allow-listed replay list in the same file. |
| Test case | the `("TYPE_WIDENING", "typeWidening-preview", true)` tuple and the `cites_349` flag in `unity_delta_unsupported_reader_feature_fails_the_query_loud` (`crates/lakehouse-engine/tests/e2e_unity_test.rs`) | The table is queryable; with one remaining case the per-case flag is a constant. |
| Test fixture choice | `refused_protocol_table_storage` in `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | Builds its refused table on `typeWidening-preview`, which no longer refuses; switches carrier to `variantType`. |

No production code becomes unreachable: the allow-list gains two variants, and the two new gates are
additions to existing resolution paths.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| type-relaxation: A narrow physical column binds to the current wider logical type and is cast per file | Integration | `crates/lakehouse-engine/src/scan/type_relaxation_tests.rs` | `a_narrow_physical_column_is_cast_to_the_current_logical_type_per_file` |
| type-relaxation: Every supported relaxation pair is proven castable rather than assumed | Unit | `crates/lakehouse-engine/src/scan/type_relaxation_tests.rs` | `arrow_castability_pins_every_supported_relaxation_pair` |
| type-relaxation: Every supported relaxation pair is proven castable rather than assumed | Integration | `crates/lakehouse-engine/src/scan/type_relaxation_tests.rs` | `every_supported_relaxation_pair_reads_its_real_values_from_a_narrow_parquet_file` |
| type-relaxation: A relaxed column crosses the emit boundary at its declared Exasol type | Unit | `crates/lakehouse-engine/src/scan/emit_tests.rs` | `a_relaxed_column_coerces_to_its_declared_exatype_without_a_relaxation_branch` |
| iceberg-type-promotion: A promotion this engine reads resolves through the shared relaxation cast | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs` | `a_readable_iceberg_promotion_plans_normally_and_carries_the_current_type` |
| iceberg-type-promotion: A promotion this engine reads resolves through the shared relaxation cast | Integration | `crates/lakehouse-engine/tests/e2e_type_relaxation_test.rs` | `iceberg_type_promotion_returns_both_layouts_at_the_promoted_types` |
| iceberg-type-promotion: A date-to-timestamp promotion is refused at plan time by name | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs` | `date_to_timestamp_promotion_is_refused_naming_table_column_both_types_and_the_issue` |
| iceberg-type-promotion: The unknown primitive type is unrepresentable, and the mapping is the tripwire | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `iceberg_primitive_mappings_are_exhaustive_so_a_new_variant_breaks_the_build` |
| delta-reader-feature-gating: A reader feature outside the allow-list refuses the table before any log replay | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_protocol_tests.rs` | `a_reader_feature_outside_the_allow_list_is_refused_with_no_per_feature_special_case` |
| delta-reader-feature-gating: Every allow-listed reader feature keeps its table queryable | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_protocol_tests.rs` | `all_seven_allow_listed_reader_features_pass_including_both_type_widening_names` |
| delta-reader-feature-gating: Every allow-listed reader feature keeps its table queryable | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `every_shipped_fixture_whose_reader_features_are_allow_listed_still_resolves` |
| delta-type-mapping: Every recorded Delta type change is validated, and an unsupported one refuses its column | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | `every_pair_the_protocol_lists_is_supported`, `a_field_carrying_an_unsupported_recorded_type_change_is_refused_naming_both_types`, `a_field_whose_recorded_type_changes_are_all_supported_plans_normally` |
| delta-type-mapping: Every recorded Delta type change is validated, and an unsupported one refuses its column | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `an_unsupported_recorded_type_change_refuses_only_its_own_column` |
| iceberg-type-promotion-fixture: Spark produces an Iceberg table whose readable promotions span the schema change | Integration | `crates/lakehouse-engine/tests/e2e_type_relaxation_test.rs` | `e2e_type_promotion_pre_promotion_data_file_is_physically_narrow` |
| iceberg-type-promotion-fixture: The new fixture and its suite are wired into the paths that actually run | Unit | `crates/lakehouse-engine/tests/build_convention.rs` | `the_type_relaxation_suite_and_fixture_are_wired_into_run_fixtures_and_make_test_e2e` |
| e2e: A Delta table using an unsupported reader feature fails the query loud (CHANGED) | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_unsupported_reader_feature_fails_the_query_loud` |
| e2e: A type-widened Delta table returns its current wider types across the widening boundary | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_type_widening_returns_the_widened_types_across_both_files` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| `vs-adapter/delta-reader-feature-gating` | `make unity-up` then `exapump sql --dsn "$LH_DSN;validateservercertificate=0" "SELECT COUNT(*) FROM DELTA_E2E.TYPE_WIDENING"` | Returns 2. The table that previously failed with a `typeWidening-preview` refusal now answers. |
| `datafusion-scan/type-relaxation` | `exapump sql --dsn "$LH_DSN;validateservercertificate=0" "SELECT BYTE_LONG, INT_LONG, LONG_DECIMAL, DATE_TIMESTAMP_NTZ FROM DELTA_E2E.TYPE_WIDENING ORDER BY INT_LONG"` | 2 rows. The pre-widening row carries 1, 2, 1.0 and its date at midnight — not NULL; the post-widening row carries 9223372036854775807 twice and 123456789012345678.9. |
| `vs-adapter/delta-type-mapping` | `exapump sql --dsn "$LH_DSN;validateservercertificate=0" "SELECT DECIMAL_DECIMAL_GREATER_SCALE FROM DELTA_E2E.TYPE_WIDENING"` | 2 rows as `DECIMAL(20,5)`; the pre-widening row's `decimal(10,2)` value rescaled, no refusal — the recorded change is on the protocol's supported list. |
| `vs-adapter/iceberg-type-promotion` | `docker compose up -d spark-iceberg-fixtures` then `exapump sql --dsn "$LH_DSN;validateservercertificate=0" "SELECT * FROM E2E_LAKEHOUSE.ICEBERG_TYPE_PROMOTION"` | Rows from both the pre- and post-promotion data files, at the promoted types, including the out-of-32-bit-range `int_long` value. |
| `vs-adapter/iceberg-type-promotion` | `cargo test a_date_promotion_is_refused_before_any_manifest_is_loaded` (no live table: Iceberg Java cannot author this promotion — decision [14]) | Test passes: refusal names the table, the promoted column, `date` and `timestamp`, and the tracked issue — not `failed to convert byte slice to array`. |
| `packaging/iceberg-type-promotion-fixture` | `docker compose logs spark-iceberg-fixtures` | The new table authored, the job exits 0, and the log shows an `ALTER TABLE … ALTER COLUMN … TYPE` between two inserts. |
| `e2e-harness/unity-catalog-e2e-harness-delta-queries` | `make test-e2e-unity` | Exit 0; the widened-read test passes; every previously-passing Delta query scenario still passes. |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E (Iceberg) | `make test-e2e` | 0 failures |
| E2E (Unity/Delta) | `make test-e2e-unity` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |
