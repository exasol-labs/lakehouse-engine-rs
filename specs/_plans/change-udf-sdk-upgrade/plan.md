# Plan: change-udf-sdk-upgrade

## Summary

Closes issue [#405](https://github.com/exasol-labs/lakehouse-engine-rs/issues/405) by moving the
`exasol-udf-sdk` and `exasol-udf-macros` pin from 0.26.1 to 0.28.1. The bump changes `ExaType` and
nothing else, but the one variant it changes, `Timestamp { precision }`, is the piece this repo was
missing to emit a timestamp at the precision its source actually carries. The plan fixes every
`ExaType` call site, splits the timestamp-precision owner into the two axes it had collapsed into
one, follows the declared precision at both emit boundaries, replaces the expression translator's
silent CAST-precision approximation with a decline, and settles the result by live measurement on
two engine versions.

The implementing commit carries `Closes #405`.

## Design

### Context

A source diff of the two published crates bounds the dependency half of this change precisely.
Between 0.26.1 and 0.28.1 the only file that changes in `exasol-udf-sdk/src` is `value.rs`, and the
only item that changes in it is the `ExaType` enum. `exasol-udf-macros` changes its version number
and one `trybuild` stderr fixture. `Value`, `ColumnInfo`, `UdfContext`, `abi.rs` and
`connect_back.rs` are byte-identical.

```
 Numeric { precision: Option<u32>, scale: Option<u32> }  ->  Numeric { precision: u32, scale: u32 }
 String  { size: Option<u32> }                           ->  String { size: u32 }
 Char    { size: Option<u32> }                           ->  Char { size: u32 }
 Timestamp                                               ->  Timestamp { precision: u32 }
 TimestampTz, Geometry, HashType,
 IntervalYearToMonth, IntervalDayToSecond                ->  removed
```

Making `Timestamp` carry its precision exposed a defect that the old type had made structurally
invisible. `iceberg_primitive_to_exasol` (`types/mapping.rs:354`) collapses all FOUR Iceberg
timestamp variants onto one `timestamp_precision.declaration()` call, and that value is resolved
once per `createVirtualSchema` request (`adapter/mod.rs:289`), not per column. Meanwhile
`iceberg_primitive_to_arrow` (`types/mapping.rs:399`, `:401`) already distinguishes them, mapping
`timestamp_ns`/`timestamptz_ns` to Arrow `Timestamp(Nanosecond, _)`, and `ScanSpec`'s logical-schema
tag vocabulary already round-trips that unit (`types/mapping.rs:436`, `:467`). So DataFusion holds
genuinely nanosecond data for a `timestamp_ns` column, the adapter declares it identically to a
microsecond one, and `target_arrow_type`'s fixed `Timestamp(Microsecond, None)` target destroys
every nanosecond digit through the strict (`safe: false`) cast in `coerce_column`
(`scan/emit.rs:188`). Nothing records that loss. Unlike the 8.x millisecond truncation, which is a
named, spec-recorded Exasol target-type trade-off, this one is a silent gap.

A second truncation site sits on the partial-aggregate path: `timestamp_to_micros`
(`scan/convert.rs:162`) divides a nanosecond array's value by 1,000 before building the
`Value::Timestamp`, and `MIN`/`MAX` over a timestamp column is pushed into that path
(`grouped_agg.rs:777` requires a numeric column for `SUM` and the statistical family only).

The recorded rationale for never targeting `TIMESTAMP(9)` is false and is deleted rather than
reworded: it blamed iceberg-rust for truncating nanoseconds via `timestamp_to_micros`, but that
function is this repo's own (`scan/convert.rs:162`) and does not exist in iceberg-rust at all.
iceberg 0.10.0 preserves nanoseconds end to end (`src/arrow/schema.rs:671-677`,
`src/arrow/int96.rs:93-106`), and the scan does not use iceberg-rust's reader in any case:
`crates/lakehouse-engine/src/scan/` carries no `iceberg::` import and reads through DataFusion's own
`ParquetSource` (`raw_scan.rs:301`, `positional_deletes.rs:542`).

A third, independent defect sits in `crates/vs-expression`. For a projected
`CAST(x AS TIMESTAMP(p))` with `p` outside `{0,3,6,9}`, `snap_timestamp_precision`
(`vs-expression/src/lib.rs:431`) renders the DataFusion side at the NEAREST member of that set, a
silent approximation rather than the requested value, while `exasol_type_from_json`
(`types/mapping.rs:658`) must declare the literal `p` verbatim, because Exasol validates a pushdown
response positionally against `selectListDataTypes`. The recorded spec papers over the mismatch with
an unverified `SHALL`: that Exasol truncates an up-snapped value back to the requested `p`. Nothing
ever measured it, and the snap contradicts that same feature's own recorded rule that the rendered
CAST target set is exactly the set whose DataFusion result matches Exasol's.

- **Goals**: one pin move, one enum's call sites fixed, a timestamp declared and emitted at the
  precision its source carries on every supported engine, a CAST precision either rendered exactly
  or declined, and four specs made true again from measurement rather than assertion.
- **Non-Goals**: no change to the `>= 2025` version rule itself, no change to the `timestamptz`
  zone-flattening trade-off, no change to `ColumnInfo`'s own optional fields (which no production
  code reads), no new Exasol type outside `TIMESTAMP(p)`, and no widening of the Exasol-dialect CAST
  renderer, which already renders every `p` in 0-9 verbatim.

### Decision

**Emit at the source precision, clamped by what the engine can emit.** The two axes that the single
`TimestampPrecision` value had conflated are separated into two types in `types/mapping.rs`, one
decision each:

- `TimestampPrecision` carries the SOURCE width, per column, three values: `Millisecond`, `Microsecond`,
  `Nanosecond`. Three, not two and not ten: these are the three sub-second Parquet timestamp
  encodings and the three sub-second Arrow `TimeUnit`s, so the vocabulary is format-neutral and a
  future direct-Parquet reader populates it unchanged. It owns `declaration()` (`TIMESTAMP`,
  `TIMESTAMP(6)`, `TIMESTAMP(9)`), `arrow_unit()`, and `from_declared_digits()`.
- `EngineTimestampSupport` carries the ENGINE ceiling, per request, resolved by
  `from_database_version`. Exasol 8.x can emit millisecond only; 2025.x and later emit 3, 6 and 9.
  It owns the version rule and the clamp, and nothing else.

A producer names a source width and clamps it: `engine.clamp(source).declaration()`. Neither
producer carries a declaration literal, and neither type knows the other's rule. The emit boundary
reads the SAME `TimestampPrecision` table in the other direction,
`TimestampPrecision::from_declared_digits(p).arrow_unit()`, so a declared `TIMESTAMP(9)` cannot
mean one width where it is declared and another where it is emitted. That single-owner property is
the point of the split: the defect this plan fixes is exactly two modules disagreeing about one
column's width.

The consequence is that on the catalog path DECLARED equals EMITTED on both engine arms, and the
"emit microseconds and trust Exasol to truncate" pattern disappears. On 8.29.13 the clamp declares
bare `TIMESTAMP` and the scan emits `Timestamp(Millisecond, None)`, so the 8.x limitation becomes a
DECLARED narrowing rather than an unmeasured reliance on the engine. That also removes the plan's
previous dependence on observing what `precision` the SLC reports for a bare `TIMESTAMP`: the
observation still happens, but it now selects an arm rather than deciding whether the probe is
vacuous.

Three live measurements already recorded in
`specs/_recorded/2026-08-19-add-timestamp-precision-versioning` bound the declaration side and are
not re-derived. `[C1]`: 2025.2.1 accepts and honors `fractionalSecondsPrecision: 9`, reported as
`TIMESTAMP(9)`; 8.29.13 silently downgrades both 6 and 9 to `TIMESTAMP(3)`. `[C2]`: 8.29.13 strips
`fractionalSecondsPrecision` from the pushdown echo entirely, so `exasol_type_from_json` reads none
there and no `TIMESTAMP(p)` EMITS clause with `p != 3` can be generated on that engine. `[C3]`:
8.29.13 rejects `TIMESTAMP(p)` for `p` outside `{3, 6}` as `0A000 Feature not supported`.

**Decline a CAST precision DataFusion cannot express, rather than approximate it.**
`render_expression`'s DataFusion-dialect TIMESTAMP arm renders `p` in `{0,3,6,9}` verbatim and
declines everything else; `snap_timestamp_precision` is deleted. "Decline" here has this codebase's
established meaning, recorded in CLAUDE.md § "Virtual Schema pushdown delegation" and ADR
`specs/_decision/045`: the DataFusion renderer returns `None`/`Err`, and the node is routed into SQL
the ADAPTER writes in the Exasol dialect, which renders `TIMESTAMP(p)` verbatim for every `p` in
0-9. Exasol never re-checks a delegated node; it executes the adapter's own SQL.

Every position a CAST node can occupy already has that route, and this plan adds none. The routing
was verified in code, not assumed:

| Position | Decline site | Route |
|----------|--------------|-------|
| Select list | `project_columns`'s `function_scalar_cast` arm sets `needs_full_fallback` on a `None` from `render_expression_safe` (`adapter/pushdown/support.rs:1221`) | piped out as `projection_widened` (`adapter/pushdown/mod.rs:187`); the `RowScan` arm returns `qualified_single_table_fallback_pushdown` (`adapter/pushdown/mod.rs:655`) |
| WHERE | `datafusion_renderable` (`adapter/pushdown/support.rs:564`) | self-applied in the wrapper's own Exasol-dialect `WHERE` (`vs-adapter/pushdown-declined-filter-self-apply`) |
| GROUP BY | `render_expression` `Err` collapses grouped-aggregate detection to `None` (`adapter/pushdown/grouped_agg.rs:189`) | falls through to `RequestShape::GroupByWrapper`, same wrapper |
| ORDER BY | none: `parse_declined_sort_key` renders a non-column sort key in the EXASOL dialect from the start (`adapter/pushdown/topn.rs:130`) | unreachable for a DataFusion-dialect decline |

The cost is stated rather than implied, because it is not local to the declined item. Declining ONE
select-list item widens the whole select list and routes the whole request to the wrapper, so Exasol
computes EVERY select-list item. What survives is the sharded parallel fan-out, the
referenced-column projection narrowing (`referenced_column_projection`,
`adapter/pushdown/joins/sql_builders.rs:1098`), and the WHERE predicate, which still travels inside
the scan spec. What is given up is the per-shard `LIMIT` and the bounded top-N: the fan-out spec is
built with `limit: None` and `order_by: Vec::new()`, and `build_qualified_single_table_fallback_sql`
(`adapter/pushdown/joins/sql_builders.rs:990`) renders the select list, GROUP BY, HAVING, ORDER BY
and LIMIT in the OUTER wrapper only. For six exotic precisions, exactness is the right side of that
trade.

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Split the one `TimestampPrecision` into a per-column source width and a per-request engine ceiling | Keep one enum and add a nanosecond arm to it | One value cannot answer two questions. The recorded defect is precisely a request-scoped value standing in for a column-scoped one, and a third variant on the same enum would have preserved that |
| Model the source width as three values, not as the ten Exasol precisions | Carry the Exasol `p` directly | Three is the real domain: Parquet encodes MILLIS/MICROS/NANOS and Arrow has the matching three sub-second units. Carrying `p` would invite arms for precisions no source produces and no Arrow unit expresses |
| Read the emit boundary's precision-to-`TimeUnit` mapping from the SAME owner as the declaration string | A local `match` in `target_arrow_type` | Two tables that must agree are the defect, not the fix. One owner makes the agreement structural |
| Floor the emit unit at `Millisecond` for a declared `p` below 3 | Map `p = 0` to Arrow `Second` | Exact, but no emit path in this repo has fed the SLC a second-unit Arrow column, and the only declaration reaching `p = 0` is the exotic projected `CAST(x AS TIMESTAMP(0))`. One Exasol-side truncation is the cheaper risk, and it is recorded |
| Decline a CAST precision outside `{0,3,6,9}` in the DataFusion dialect | Keep `snap_timestamp_precision` and verify the up-snap truncation live | The snap contradicts this feature's own recorded rule that the rendered target set equals the set whose DataFusion result matches Exasol's. Verifying the workaround would have preserved a silent approximation that a decline removes outright |
| Fix `timestamp_to_micros` in the same plan | Leave the partial-aggregate path at microsecond | It is the same defect class, reachable through `MIN`/`MAX` over a timestamp column, and leaving it would make a `TIMESTAMP(9)` declaration honest on one emit path and false on the other |
| Delete the absent-payload NUMERIC drift guard | Re-derive `precision` and `scale` from `ColumnInfo::type_name` | Re-deriving restores the replicated type-string parse that issue #399 deleted. The guard was already unreachable, because a valid Exasol NUMERIC declaration carries both values |
| Delete the five removed-variant arms rather than map them to `Utf8` through a wildcard | Add a wildcard arm so a future variant change compiles | An exhaustive match turns the next upstream change into a compile error here, which is what caught this one |

### Iceberg and Delta specification compliance

The Apache Iceberg table spec's `§ Primitive Types` defines `timestamp` and `timestamptz` as
"Timestamp, microsecond precision" and `timestamp_ns` and `timestamptz_ns` as "Timestamp,
nanosecond precision", the latter two "added in v3" per `§ Appendix E: Format version changes`. This
plan brings the declaration surface into line with that text: a nanosecond source is declared
`TIMESTAMP(9)` and emitted at the nanosecond Arrow unit, where before it was declared and emitted at
microsecond. That closes a real deviation rather than tracking one.

The residual deviation is an Exasol target-type limitation and is named as a deliberate trade-off
rather than a gap: on Exasol 8.x a UDF can emit millisecond precision only, so the engine clamp
declares every catalog timestamp column bare `TIMESTAMP` there and a nanosecond source loses six
fractional digits, exactly as a microsecond source already loses three. The `timestamptz`
zone-flattening trade-off is unchanged and independent.

The Delta Lake protocol's `§ Schema Serialization Format → Primitive Types` types both `timestamp`
and `timestamp without time zone` as "Microsecond precision", and the string `nanosecond` does not
occur in the protocol; Unity Catalog's `ColumnTypeName` domain carries only `TIMESTAMP` and
`TIMESTAMP_NTZ` in both the OSS API schema and the Databricks SDK. The Delta/Unity producer
therefore stays at microsecond BY PROTOCOL, and the spec states that rather than leaving the
asymmetry between the two producers unexplained. No Delta deviation arises and none is tracked.

One named limit of this repo's own scan configuration: `raw_scan.rs:301` sets DataFusion's
`coerce_int96 = "us"`, which forces an INT96 PHYSICAL column to microseconds whatever its logical
type. A spec-conformant Iceberg `timestamp_ns` is written as an INT64 `TIMESTAMP(NANOS)` column and
is unaffected; a legacy file storing a nanosecond-typed column as INT96 would arrive at microsecond.
Stated in the spec, not silent.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/type-mapping-timestamp-precision | CHANGED | `specs/_plans/change-udf-sdk-upgrade/datafusion-scan/type-mapping-timestamp-precision/spec.md` |
| datafusion-scan/scan-execution-value-conversion | CHANGED | `specs/_plans/change-udf-sdk-upgrade/datafusion-scan/scan-execution-value-conversion/spec.md` |
| datafusion-scan/scan-execution-partial-agg | CHANGED | `specs/_plans/change-udf-sdk-upgrade/datafusion-scan/scan-execution-partial-agg/spec.md` |
| sql-comprehension/vs-expression-translator-cast | CHANGED | `specs/_plans/change-udf-sdk-upgrade/sql-comprehension/vs-expression-translator-cast/spec.md` |

### Recorded text this plan does not amend

`e2e-harness/e2e-harness-scan-correctness` names the `ExaType` variants `Int64`, `Int32`, `Numeric`,
`Double`, `String`, `Date`, `Timestamp` and `Boolean`. All eight survive the bump under the same
names, so that clause stays true. `datafusion-scan/type-mapping-module-structure` and
`vs-adapter/pushdown-col-types-consolidation` name `ExaTypeClass` and `classify_exa_type`, which are
this repo's own string-keyed enum in `types/mapping.rs` and carry no relation to the SDK type.
`vs-adapter/pushdown-planning`'s `exasol_type_from_json` rule is unchanged: the adapter still echoes
Exasol's literal `fractionalSecondsPrecision`, because the positional `selectListDataTypes` check
leaves it no choice. `vs-adapter/pushdown-planning-capability-extensions`,
`vs-adapter/pushdown-declined-filter-self-apply` and `vs-adapter/pushdown-planning-order-by-capability`
already record every decline route this plan relies on; it adds no route and amends none of them.
`datafusion-scan/type-relaxation`'s Background states that the emit boundary casts every column
whose Arrow type differs from the target derived from the declared EMITS type, through one unguarded
cast with no per-pair match. That stays true: only which unit the target names changes, not how it
is applied, and a relaxed column still reaches the boundary carrying its current logical type.

## Impact

Breaking for operators. The SDK fingerprint is `{version}:{rustc_hash}` and the SLC checks it at UDF
load, so a 0.28.1 SLC rejects a 0.26.1 `.so` and the reverse. Any deployment MUST rebuild the `.so`
with `make cross-udf-build` and reinstall the SLC in the same window. Both steps read the version
from the single `Cargo.toml` pin with no further edit: `Makefile:129`, `bench/run.sh`,
`deploy/scripts/install.sh` and the compile-time `SLC_VERSION` in
`crates/lakehouse-engine/tests/common/e2e_harness.rs` all derive it from that line.

Query results change, deliberately, in three places.

1. An Iceberg `timestamp_ns` or `timestamptz_ns` column is declared `TIMESTAMP(9)` instead of
   `TIMESTAMP(6)` on Exasol 2025.x and later, and returns its nanosecond digits instead of silently
   losing them. A virtual schema over such a table MUST be re-created for the new declaration to
   take effect, because `createVirtualSchema` is where the column type is fixed.
2. A projected `CAST(x AS TIMESTAMP(p))` with `p` in `{1,2,4,5,7,8}` returns the value at the
   REQUESTED precision instead of a nearest-unit approximation, and its query loses per-shard
   `LIMIT` and top-N pushdown while gaining exactness. `p` in `{0,3,6,9}` is unchanged.
3. On Exasol 8.x a catalog timestamp column is now emitted at millisecond rather than emitted at
   microsecond and truncated by the engine. The returned values are the same; what changes is which
   component does the narrowing.

No Iceberg or Delta `timestamp`/`timestamptz`/`TIMESTAMP`/`TIMESTAMP_NTZ` column changes its
declaration or its values on any engine. No scan-spec wire format changes: the logical-schema tag
vocabulary already carries `timestamp_ns` and `timestamptz_ns`.

## Dependencies

| Dependency | Check |
|------------|-------|
| `exasol-udf-sdk` 0.28.1 and `exasol-udf-macros` 0.28.1 on crates.io | Confirmed published |
| SLC release `v0.28.1` with both architecture assets | Confirmed: `lc-rust-0.28.1.tar.gz` and `lc-rust-0.28.1-aarch64.tar.gz` |
| A running local Exasol Docker stack for group C | `docker compose up -d`, which `make test-e2e` does NOT start |
| An Exasol 8.x image for task 5.2's clamped arm | `exasol/docker-db:8.29.13`, already run by `.github/workflows/ci.yml:523` and selected through the `EXASOL_IMAGE` variable `Makefile:3` and `docker-compose.yml:115` both read. Each switch between major versions needs `docker compose down -v` first |
| An Iceberg REST catalog able to create a format-version 3 table, for task 5.4's optional fixture | `apache/iceberg-rest-fixture:1.10.1` (`docker-compose.yml:57`). `TableCreation::format_version` is a no-op against a REST catalog in iceberg-rust; the `format-version` table PROPERTY is the working route |

## Implementation Tasks

### 1. Move the pin and take the compiler's census

- [ ] 1.1 Set `exasol-udf-sdk` and `exasol-udf-macros` to `0.28.1` in the root `Cargo.toml`
  `[workspace.dependencies]` and update `Cargo.lock`. Change no member manifest and no other file.
  Confirm `make print-slc-version` prints `0.28.1`.
- [ ] 1.2 Run `cargo check --workspace --all-targets` and then `cargo check --workspace --all-targets
  --features exasol-e2e,unity-e2e,lakekeeper-e2e,azure-e2e,cloud-e2e`. The second command is
  mandatory: a host `cargo test` does not build the feature-gated E2E test crates, and past
  signature changes in this repo broke only at that gate. `--workspace` rather than
  `-p lakehouse-engine`, because `crates/vs-expression` also links the SDK. If the five features do
  not resolve together, run one `cargo check` per feature instead and cover all five. Record the
  complete error list. Tasks 2 and 3 list the files a full text census of every `ExaType` reference
  found. Any file the compiler names that they do not list MUST be added to this plan before the
  group proceeds.

### 2. Split the timestamp-precision owner into its two axes

- [ ] 2.1 `crates/lakehouse-engine/src/types/mapping.rs`: replace the two-state `TimestampPrecision`
  (`:277-295`) with a THREE-value source width (`Millisecond`, `Microsecond`, `Nanosecond`)
  carrying `declaration() -> &'static str` (`TIMESTAMP`, `TIMESTAMP(6)`, `TIMESTAMP(9)`),
  `arrow_unit() -> TimeUnit`, and `from_declared_digits(u32) -> Self` mapping `0..=3` to
  `Millisecond`, `4..=6` to `Microsecond` and `7..` to `Nanosecond`. Add `EngineTimestampSupport`
  carrying the moved `from_database_version` (`:298-319`, rule unchanged: leading dot-separated
  component `< 2025` takes the clamped arm, everything else including empty and unparseable takes
  the unclamped one) and `clamp(TimestampPrecision) -> TimestampPrecision`, which returns
  `Millisecond` on the clamped arm and its argument on the unclamped one. Each type's doc comment
  states which ONE decision it owns; neither restates the other's. [expert]
- [ ] 2.2 `types/mapping.rs` producers: `iceberg_primitive_to_exasol` (`:330-360`) splits the single
  `Timestamp | TimestampNs | Timestamptz | TimestamptzNs` arm into `Timestamp | Timestamptz` naming
  `Microsecond` and `TimestampNs | TimestamptzNs` naming `Nanosecond`, each rendered as
  `engine.clamp(source).declaration().to_string()`. `unity_type_name_to_exasol` (`:527-555`) names
  `Microsecond` for `"TIMESTAMP" | "TIMESTAMP_NTZ"`, with a one-line comment citing the Delta
  protocol's microsecond typing as the reason there is no nanosecond arm. Rename the threaded
  parameter through `iceberg_type_to_exasol` (`:487`) and `column_source_type_to_exasol` (`:501`).
  Neither producer may carry a `TIMESTAMP` literal.
- [ ] 2.3 `types/mapping.rs` `exasol_type_to_arrow` (`:81-164`): the `TIMESTAMP`/`TIMESTAMP(p)` arm
  resolves its `TimeUnit` through `TimestampPrecision::from_declared_digits`, parsing `p` from the
  type string and treating a bare `TIMESTAMP` as `3`. Its doc comment calls the function the single
  source of truth for the Arrow type `emit_batch` accepts, so leaving it at a fixed microsecond
  answer while `target_arrow_type` follows the precision would make that comment false. The
  `TIMESTAMP WITH LOCAL TIME ZONE` arm is unchanged.
- [ ] 2.4 `crates/lakehouse-engine/src/adapter/mod.rs`: `handle_create_virtual_schema` (`:289`) calls
  `EngineTimestampSupport::from_database_version`, and `build_listing_virtual_tables` (`:585-631`)
  takes the renamed parameter type. The single `ctx.database_version()` read stays where it is.
- [ ] 2.5 Update the tests that name the old enum: `types/mapping_tests.rs`
  (`iceberg_types_map_to_exasol_type`, `incompatible_unity_types_declared_varchar`,
  `catalog_decimal_guard_is_shared_by_both_source_kinds`,
  `database_version_leading_component_selects_the_declared_timestamp_precision`,
  `unreadable_database_version_declares_microsecond_precision`,
  `timestamp_declaration_is_version_gated_for_both_catalog_kinds`,
  `exasol_type_to_arrow_parses_timestamp_precision`),
  `adapter/adapter_tests.rs` (`iceberg_listing_is_behavior_identical_behind_the_trait`) and
  `adapter/catalog_client_tests.rs` (`catalog_kind_is_matched_only_at_the_construction_site`,
  `both_kinds_share_one_listing_pipeline`,
  `build_listing_virtual_tables_declares_timestamp_at_the_given_precision`). Rewrite
  `exasol_type_to_arrow_parses_timestamp_precision` (`types/mapping_tests.rs:469-474`), whose whole
  body asserts one fixed `Some(DataType::Timestamp(TimeUnit::Microsecond, None))` for `TIMESTAMP(0)`,
  `TIMESTAMP(6)` and `TIMESTAMP(9)`, into a per-precision assertion: `Millisecond` for
  `TIMESTAMP(0)`, `Microsecond` for `TIMESTAMP(6)`, `Nanosecond` for `TIMESTAMP(9)`, plus a bare
  `TIMESTAMP` case asserting `Millisecond`. Replace its doc comment, which states the collapsed
  answer task 2.3 removes. This test is a FAILURE, not a compile error, so task 1.2's compiler census
  cannot surface it. Add the missing
  coverage the defect exposes: a table-driven test asserting all FOUR Iceberg timestamp variants
  against both engine arms, so `timestamp_ns` and `timestamp` cannot collapse onto one declaration
  again, and a test asserting `from_declared_digits(p).arrow_unit()` for every `p` in 0-9.

### 3. Follow the declared precision at both emit boundaries

- [ ] 3.1 `crates/lakehouse-engine/src/scan/emit.rs` `target_arrow_type` (`:221-250`): delete the
  `TimestampTz`, `Geometry`, `HashType`, `IntervalYearToMonth` and `IntervalDayToSecond` arms; match
  the timestamp arm as `ExaType::Timestamp { precision }` returning
  `DataType::Timestamp(TimestampPrecision::from_declared_digits(*precision).arrow_unit(), None)`;
  leave the `Utf8` arm covering exactly `String { .. }`, `Char { .. }` and `Unsupported`. Keep the
  match exhaustive with no wildcard. Replace the arm's three-line comment with one line stating that
  the unit follows the declaration and names its owner. [expert]
- [ ] 3.2 `emit.rs` `decimal_target`: take `precision: u32, scale: u32`. Delete the `Option` zip and
  the absent-payload half of the error message. Keep the `Decimal128` range check and the
  `UdfError::User` that names the column.
- [ ] 3.3 `crates/lakehouse-engine/src/scan/convert.rs`: replace `timestamp_to_micros` (`:161-189`)
  with a conversion that produces the `chrono::NaiveDateTime` for `Value::Timestamp` directly from
  the array's own unit, losing no digit the array carries. The nanosecond branch MUST NOT divide by
  1,000, and the replacement MUST NOT route the instant through an `i64` COUNT OF NANOSECONDS, whose
  representable range is only 1677-2262. Split seconds and a sub-second remainder instead. Update
  the single call site at `:133`. [expert]
- [ ] 3.4 `crates/lakehouse-engine/src/scan/declared_columns_test_support_tests.rs`: drop `Some(...)`
  from `varchar()` and `numeric()`, and wrap the returns of `declared_size`, `declared_precision` and
  `declared_scale` in `Some(...)`, because `ColumnInfo`'s own fields stay `Option<u32>`.
- [ ] 3.5 `crates/lakehouse-engine/tests/scan_fixture/mod.rs`: the same three helper changes for
  `varchar()` and `decimal()`, plus `declared_type_name`, which loses the five removed arms and
  renders `ExaType::Timestamp { precision }` as `TIMESTAMP({precision})`. The twelve other files
  under `tests/` that name `ExaType` use only surviving variants or these helpers and need no edit.
- [ ] 3.6 `crates/lakehouse-engine/src/scan/emit_tests.rs`: update the variant sweep at `:330` for the
  ten-variant enum. Replace `exa_type_timestamp_maps_to_microsecond_target` with a test asserting the
  resolved unit for every `precision` 0 through 9 (`Millisecond` for 0-3, `Microsecond` for 4-6,
  `Nanosecond` for 7-9), and delete its `TimestampTz` half. Add a `coerce_column` test proving a
  `Timestamp(Nanosecond, None)` column declared `TIMESTAMP(9)` passes through with all nine digits,
  the regression the fixed microsecond target used to destroy. Delete the three absent-payload cases
  from `emit_stream_fails_on_numeric_with_absent_or_out_of_range_payload` and the one from
  `a_drifted_numeric_never_resolves_to_the_string_target`, keep both tests with their out-of-range
  cases, and rename both to drop "absent". Update the `Char { size }` sites at `:347` and `:484`.
- [ ] 3.7 `crates/lakehouse-engine/src/scan/partial_agg_tests.rs`: delete the "absent payload" case
  from `partial_agg_fails_on_numeric_with_absent_or_out_of_range_payload`, keep the two out-of-range
  cases, rename the test to drop "absent", and update the `ExaType::String { size }` site at `:972`.
  Add a test proving a `MIN`/`MAX` cell over a `Timestamp(Nanosecond, _)` column declared
  `TIMESTAMP(9)` converts to a `Value::Timestamp` carrying all nine digits.
- [ ] 3.8 `crates/lakehouse-engine/tests/micro_bench.rs`: update `numeric()` at `:69`, the two
  `ExaType::String { size }` sites at `:184` and `:246`, and the two `ExaType::Timestamp` sites at
  `:168` and `:257`.

### 4. Decline an inexpressible CAST precision instead of approximating it

- [ ] 4.1 `crates/vs-expression/src/lib.rs`: delete `snap_timestamp_precision` (`:418-438`) and
  change `render_cast_target`'s `Dialect::DataFusion` TIMESTAMP arm (`:531-534`) to render
  `TIMESTAMP(p)` verbatim for `p` in `{0, 3, 6, 9}` and return
  `Err(UdfError::User("unsupported CAST target type: TIMESTAMP({p}) …"))` for every other value,
  including above 9. The `Dialect::Exasol` arm and the absent-precision and `withLocalTimeZone`
  branches are untouched.
- [ ] 4.2 `crates/vs-expression/src/lib_tests.rs` `renders_cast_timestamp_precision_per_dialect`
  (`:831-889`): replace the `5 -> 6` snap assertion with a decline assertion, and extend it to all
  six declining precisions `{1,2,4,5,7,8}` plus the four rendering ones `{0,3,6,9}`. Assert the safe
  variant returns `None` for the same inputs. The Exasol-dialect half is unchanged and must stay
  green for every `p` in 0-9, because that dialect is where a declined node is computed.
- [ ] 4.3 Add a regression test in `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs`
  that a `pushdown` request whose select list carries a `function_scalar_cast` to
  `{"type":"TIMESTAMP","fractionalSecondsPrecision":2}` returns the qualified single-table wrapper
  shape (`SELECT … FROM (…) AS "LHS_T0"` carrying `CAST(… AS TIMESTAMP(2))` in the outer select
  list) rather than a plain row-scan projection. This pins the routing the decline depends on. It
  does not introduce it. Verify against the existing wrapper-shape assertions in that file rather
  than inventing a new assertion style. Runs in group C, not beside tasks 4.1 and 4.2: this file
  lives in `lakehouse-engine`, which does not compile until group A's tasks 3.1-3.8 land.

### 5. Measure the round trip on both engine legs

- [ ] 5.1 Run `docker compose down -v` so the stack starts from a clean `exa-data` volume. Bring the
  stack up with `docker compose up -d` and wait for it, then run `make test-e2e`. The `make` target
  does NOT start the stack, and a DB-backed test FAILS rather than skips without it. The target
  rebuilds the `.so` in `rust:1.94-trixie`, and the harness downloads and registers SLC 0.28.1 by
  itself, because `tests/common/e2e_harness.rs` derives `SLC_VERSION` from the SDK's own
  compile-time fingerprint. This is the `2025.1.16` unclamped arm. Confirm no fingerprint-mismatch
  error at UDF load and that `e2e_timestamp_precision_test` passes unchanged for the microsecond
  catalog columns. WARNING: a fingerprint mismatch or a timestamp emit failure here stops the plan.
  [expert]
- [ ] 5.2 On the same `2025.1.16` stack, extend
  `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs` with the THREE emit widths this
  redesign introduces, all over the existing `e2e_tsprecision` fixture
  (`crates/lakehouse-engine/tests/common/seed.rs:3506-3600`), and record what each observes.
  (p) PRECONDITION, asserted before (a), (b) and (c) assert any value: read the generated SQL for
  each width through `isolated_pushdown_statement`
  (`crates/lakehouse-engine/tests/common/e2e_harness.rs:348`) and assert the `EMITS` clause declares
  the expected literal, `TIMESTAMP(9)` for (a) and `TIMESTAMP(3)` for (b). For (c) the scan's `EMITS`
  carries the raw `ts` declaration and MUST NOT carry `TIMESTAMP(2)`, because the declined CAST is
  computed in the outer wrapper. Fail each assertion with a message naming BOTH candidate causes,
  each distinct from an SLC rejection: a stripped `fractionalSecondsPrecision` echo on `2025.1.16`,
  and a `0A000 Feature not supported` rejection of the `TIMESTAMP(p)` CAST target, the rejection
  `[C3]` records on 8.29.13.
  `[C2]` was captured on `2025.2.1`, not on the `2025.1.16` image this leg runs, so this step
  confirms the echo on the measured build rather than generalising a capture from another one.
  (a) `SELECT CAST(ts AS TIMESTAMP(9)) ...`: once step (p) has confirmed the `TIMESTAMP(9)` EMITS
  declaration on this build, the scan emits a `Timestamp(Nanosecond, None)` Arrow column. Assert
  every seeded value round-trips unchanged and `COUNT(DISTINCT) == 4`;
  (b) `SELECT CAST(ts AS TIMESTAMP(3)) ...`: `EMITS` declares `TIMESTAMP(3)`, the scan emits
  `Timestamp(Millisecond, None)`; assert `COUNT(DISTINCT) == 2`;
  (c) `SELECT CAST(ts AS TIMESTAMP(2)) ...`: the DataFusion dialect declines, so assert the returned
  values equal what Exasol computes natively for the same expression over the same literals in the
  same session, and assert through `isolated_pushdown_statement`
  (`crates/lakehouse-engine/tests/common/e2e_harness.rs:348`) that the generated SQL is the
  qualified wrapper shape rather than a plain row scan.
  Both (a) and (b) feed the SLC's strict Arrow-IPC block an Arrow unit no emit path in this repo has
  ever fed it, so they are the plan's central measurement. Classify every outcome into exactly one of
  three cases, and report the case by name.
  (1) STOP, an SLC rejection: a UDF error or a SQL error raised for a width AFTER step (p) passed for
  that width. This is the plan's only STOP condition. The design, not the implementation, then needs
  revision.
  (2) An engine limit, not a STOP: the query SUCCEEDS and raises no error, but returns fewer distinct
  values than the declared width admits. The engine accepted the declaration and silently clamped it.
  `[C1]` and `[C3]` measured that behaviour on 8.29.13, where identical DDL returned `ok` and both
  `TIMESTAMP(6)` and `TIMESTAMP(9)` reported `TIMESTAMP(3)`. Report it as a `2025.1.16` engine limit,
  record it under task 5.5, and narrow the delta clause to the width the run reached.
  (3) An engine-build difference, not a STOP: step (p) itself fails. Report it against the `2025.2.1`
  build `[C2]` was captured on.
  Guard (a) and (c) on the live engine
  version read through `live_engine_version`, because `[C3]` records that 8.29.13 rejects
  `TIMESTAMP(p)` for `p` outside `{3, 6}` as `0A000 Feature not supported` before any pushdown
  happens. [expert]
- [ ] 5.3 Runs AFTER task 5.4, which needs the `2025.1.16` stack this task replaces.
  CAUTION: run `docker compose down -v` first. The `exa-data` named volume
  (`docker-compose.yml:128-129`) holds the 2025.1.16 data directory, and an 8.29.13 container started
  over it does not come up. The reset also wipes `minio-data`, so the Spark fixture job and the
  in-process seed re-run on the next `up`. Repeat the stack bring-up and `make test-e2e` run with
  `EXASOL_IMAGE=exasol/docker-db:8.29.13` exported to BOTH the `docker compose up -d` step and the
  `make` invocation. No harness change is needed: `Makefile:3` and `docker-compose.yml:115` already
  read that variable. On 8.29.13 the engine clamp declares both seeded catalog timestamp columns
  bare `TIMESTAMP` and the scan now emits `Timestamp(Millisecond, None)`, so declared and emitted
  match and the existing `MILLISECOND` oracle arm (`COUNT(DISTINCT) == 2`,
  `retained_at(micros, 3)`) must hold unchanged. Observe once, by hand, the `precision` the SLC
  reports for the bare-`TIMESTAMP` output column, using `ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS`
  with `%udf_debug_level debug` (CLAUDE.md § Live debugging), and record it. That value now selects
  which arm `from_declared_digits` takes on this engine rather than deciding whether the probe is
  vacuous: 3 gives the exact millisecond round trip this task asserts, and 6 would mean the scan
  emits microseconds into a bare `TIMESTAMP` and Exasol narrows them, which is correct but is NOT
  what the spec clause claims. Report it instead of adjusting the clause silently. [expert]
- [ ] 5.4 Runs BETWEEN tasks 5.2 and 5.3, not in the list position printed here. The label stays,
  the same low-churn convention task 4.3 uses to name its group. This task needs task 5.1's
  `2025.1.16` stack still up: the `TIMESTAMP(9)` declaration below cannot hold once task 5.3 has
  replaced that stack with 8.29.13.
  Determine whether the pinned fixture toolchain can seed a GENUINE nanosecond SOURCE column,
  and record the answer either way. Extend `seed_timestamp_precision_probe`
  (`crates/lakehouse-engine/tests/common/seed.rs:3523`) with a `ts_ns` Iceberg `timestamp_ns` column
  carrying two values that differ only below the microsecond, creating the table with the
  `format-version` table PROPERTY set to `3` rather than `TableCreation::format_version`, which is a
  no-op against a REST catalog. Guard every assertion on the live engine version read through
  `live_engine_version` (`crates/lakehouse-engine/tests/common/timestamp_precision.rs:52`), the guard
  task 5.2(a) already uses. On the `>= 2025` arm, and on a successful seed, assert the column is
  declared `TIMESTAMP(9)` and that both values survive distinct, the only end-to-end proof that a
  real sub-microsecond digit reaches Exasol. On the clamped arm assert the clamp instead: the adapter
  declares that column bare `TIMESTAMP`, `SYS.EXA_ALL_COLUMNS` reports it as `TIMESTAMP(3)` (`[C1]`,
  and the existing `ExpectedTimestampPrecision::MILLISECOND` arm), and the two values collapse to ONE
  distinct value. The failure branch that follows covers a SEEDING failure ONLY. Its single trigger
  is an error raised by iceberg-rust or by the `apache/iceberg-rest-fixture:1.10.1` catalog while
  creating or writing the v3 table. On that error, record it exactly, revert the fixture change, and
  narrow the delta's nanosecond claims to what task 5.2 reached: the declaration, the Arrow unit, the
  SLC's acceptance of it, and value fidelity through it, but no genuinely sub-microsecond source
  value. An assertion failure on a non-2025 engine is NOT that branch, and MUST NOT narrow the delta.
  Do NOT leave the limit unstated in either outcome. [expert]
- [ ] 5.5 Record the measured behaviour in `decision-log.md` entry `[1]`: both engine versions, the
  declared type and emitted Arrow unit observed for each of the three widths, the SLC-reported
  `precision` for the bare `TIMESTAMP` from task 5.3, task 5.2(c)'s declined-CAST value against
  Exasol's native answer, and task 5.4's outcome. Then correct
  `specs/_plans/change-udf-sdk-upgrade/datafusion-scan/type-mapping-timestamp-precision/spec.md`'s
  live-engine clauses so they state exactly what was measured and no more: the engine versions, the
  widths actually exercised, and the limit of the nanosecond evidence. The delta as authored already
  scopes those clauses; correct them to the observed result if the measurement differs, and MUST NOT
  widen a SHALL past an arm the run reached. [expert]
- [ ] 5.6 Reset the stack per task 5.3's CAUTION and re-run the `2025.1.16` leg. Both engine legs
  MUST finish with 0 failures. Every assertion tasks 5.2 and 5.4 added MUST be green on the engine
  arm its own `live_engine_version` guard selects, not on both legs. Task 5.4's `TIMESTAMP(9)`
  declaration and surviving-distinct assertions run on the `>= 2025` arm only, and its clamped
  `TIMESTAMP(3)` and collapsed-distinct assertions run on the 8.29.13 arm only. A guarded assertion
  that does not run on a leg is not a gap. Every UNGUARDED assertion MUST be green on both legs.
  [expert]

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: SDK pin, timestamp-precision ownership, and both emit boundaries | 1.1-1.2, 2.1-2.5, 3.1-3.8 | none | spec deltas `datafusion-scan/type-mapping-timestamp-precision`, `datafusion-scan/scan-execution-value-conversion`, `datafusion-scan/scan-execution-partial-agg`; `Cargo.toml`, `crates/lakehouse-engine/src/types/mapping.rs`, `crates/lakehouse-engine/src/types/mapping_tests.rs`, `crates/lakehouse-engine/src/adapter/mod.rs`, `crates/lakehouse-engine/src/adapter/adapter_tests.rs`, `crates/lakehouse-engine/src/adapter/catalog_client_tests.rs`, `crates/lakehouse-engine/src/scan/emit.rs`, `crates/lakehouse-engine/src/scan/emit_tests.rs`, `crates/lakehouse-engine/src/scan/convert.rs`, `crates/lakehouse-engine/src/scan/partial_agg_tests.rs`, `crates/lakehouse-engine/src/scan/declared_columns_test_support_tests.rs`, `crates/lakehouse-engine/tests/scan_fixture/mod.rs`, `crates/lakehouse-engine/tests/micro_bench.rs` |
| B: CAST precision decline in the expression translator | 4.1-4.2 | none | spec delta `sql-comprehension/vs-expression-translator-cast`; `crates/vs-expression/src/lib.rs`, `crates/vs-expression/src/lib_tests.rs` |
| C: routing regression and live two-engine measurement | 4.3, 5.1-5.6 | A and B (needs the 0.28.1 `.so`, the matching SLC, and the declined-CAST arm) | `specs/_plans/change-udf-sdk-upgrade/datafusion-scan/type-mapping-timestamp-precision/spec.md`, spec delta `sql-comprehension/vs-expression-translator-cast`, `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs`, `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs`, `crates/lakehouse-engine/tests/common/timestamp_precision.rs`, `crates/lakehouse-engine/tests/common/seed.rs`, `docker-compose.yml`, the `test-e2e` target in `Makefile` |

Groups A and B share no file and no spec delta, so they may run concurrently. A owns the declaration
and emit surfaces in `lakehouse-engine`. B owns `crates/vs-expression` alone, tasks 4.1 and 4.2. That
crate links only `exasol_udf_sdk::error::UdfError`, which the bump leaves byte-identical, so B is
unaffected by A's pin move. Both are knowledge-complete on their own: A's
tasks all turn on one question, what width a timestamp column has and where that answer is owned,
and B's all turn on a second, what the DataFusion dialect does with a precision it cannot express.

Task 4.3 belongs to group C, not to group B. It writes a test in
`crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs`, and that crate does not compile
between task 1.1's pin move and task 3.8, because every `ExaType::Timestamp`, `ExaType::Numeric`,
`ExaType::String` and `ExaType::Char` site in it is mid-rewrite. No test in that crate can run during
group A's window, including a failing test written first. Group C runs task 4.3 ahead of task 5.1,
once group A's crate compiles and group B's decline exists.

Group C runs after both. It owns the live-engine files and the delta clauses that no amount of
reading can settle, and it carries the only tasks that can send the design back for revision. It is
kept separate from A so the mechanical parts of the bump are not priced at the expert model twice,
and separate from B because B needs neither a running engine nor a compiling `lakehouse-engine`.
Task 4.3 is the one member that needs no running engine. It sits here because its crate compiles
only after group A, and because the decline it pins is what group C's task 5.2(c) then measures live.

Group C's run order is 4.3, 5.1, 5.2, 5.4, 5.3, 5.5, 5.6. Task 5.4 runs before task 5.3 and keeps
its label, the same low-churn convention task 4.3 uses for its group. Task 5.4 asserts a nanosecond
declaration that only the `2025.1.16` stack carries, and task 5.3 replaces that stack with 8.29.13.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Match arms | `crates/lakehouse-engine/src/scan/emit.rs` `target_arrow_type` | `TimestampTz`, `Geometry`, `HashType`, `IntervalYearToMonth` and `IntervalDayToSecond` no longer exist |
| Branch | `crates/lakehouse-engine/src/scan/emit.rs` `decimal_target` | The absent-payload arm is unreachable once `precision` and `scale` are `u32` |
| Function | `crates/vs-expression/src/lib.rs` `snap_timestamp_precision` | Its only call site becomes a decline; no other caller exists |
| Match arms | `crates/lakehouse-engine/tests/scan_fixture/mod.rs` `declared_type_name` | Same five removed variants |
| Test cases | `crates/lakehouse-engine/src/scan/emit_tests.rs` `emit_stream_fails_on_numeric_with_absent_or_out_of_range_payload`, `a_drifted_numeric_never_resolves_to_the_string_target` | The absent-payload cases are no longer constructible. The tests keep their out-of-range cases |
| Test case | `crates/lakehouse-engine/src/scan/partial_agg_tests.rs` `partial_agg_fails_on_numeric_with_absent_or_out_of_range_payload` | Same reason |
| Test assertion | `crates/lakehouse-engine/src/scan/emit_tests.rs` `exa_type_timestamp_maps_to_microsecond_target` | The `TimestampTz` half asserts a removed variant, and the microsecond expectation is the defect |
| Test assertion | `crates/lakehouse-engine/src/types/mapping_tests.rs` `exasol_type_to_arrow_parses_timestamp_precision` (`:469-474`) | The single fixed `Timestamp(Microsecond, None)` expectation asserted for `TIMESTAMP(0)`, `TIMESTAMP(6)` and `TIMESTAMP(9)` is the answer task 2.3 changes. Task 2.5 replaces it with a per-precision assertion, and its doc comment goes with it |

`exasol_type_to_arrow` and its `TIMESTAMP WITH LOCAL TIME ZONE` arm stay. The function is keyed by
type string, not by `ExaType`, and `datafusion-scan/type-mapping-module-structure` records it as a
CLAUDE.md compliance surface; task 2.3 changes only which `TimeUnit` its TIMESTAMP arm resolves.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A catalog timestamp column is declared TIMESTAMP(6) on Exasol 2025.x and later | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `timestamp_declaration_is_version_gated_for_both_catalog_kinds`, `database_version_leading_component_selects_the_declared_timestamp_precision` |
| A nanosecond catalog timestamp column is declared TIMESTAMP(9) on Exasol 2025.x and later | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `iceberg_types_map_to_exasol_type` (extended to all four timestamp variants on both engine arms) |
| A nanosecond catalog timestamp column is declared TIMESTAMP(9) on Exasol 2025.x and later | Integration (live, 2025.1.16) | `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs` | `iceberg_nanosecond_timestamps_round_trip_at_the_declared_precision` (task 5.4; reduced to task 5.2's `CAST(… AS TIMESTAMP(9))` probe if the v3 fixture cannot be seeded) |
| An empty or unparseable database version declares the microsecond precision | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `unreadable_database_version_declares_microsecond_precision` |
| Iceberg timestamptz maps to plain Exasol TIMESTAMP | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `iceberg_types_map_to_exasol_type` |
| A declared TIMESTAMP(p) EMITS column maps back to the Arrow unit of that precision | Unit | `crates/lakehouse-engine/src/scan/emit_tests.rs`, `crates/lakehouse-engine/src/types/mapping_tests.rs` | `exa_type_timestamp_maps_to_the_declared_precisions_arrow_unit`, `exasol_type_to_arrow_parses_timestamp_precision` (rewritten per-precision by task 2.5) |
| A declared TIMESTAMP(p) EMITS column maps back to the Arrow unit of that precision | Integration (live, both engine legs) | `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs` | `iceberg_microsecond_timestamps_round_trip_at_the_declared_precision`, plus task 5.2's three-width extension |
| Output columns are coerced to the Arrow type the declared EMITS ExaType requires before emit_batch | Unit | `crates/lakehouse-engine/src/scan/emit_tests.rs` | `coerce_batch_to_exa_types_casts_every_declared_variant`, `emit_stream_fails_on_numeric_with_out_of_range_payload` |
| Every emitted partial-aggregate cell matches its declared output column | Unit | `crates/lakehouse-engine/src/scan/partial_agg_tests.rs` | `partial_agg_fails_on_numeric_with_out_of_range_payload`, plus task 3.7's nanosecond `MIN`/`MAX` test |
| CAST to TIMESTAMP renders the declared fractional-seconds precision per SQL dialect | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_cast_timestamp_precision_per_dialect` |
| CAST to TIMESTAMP renders the declared fractional-seconds precision per SQL dialect (routing) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | task 4.3's wrapper-shape regression test |
| CAST to TIMESTAMP renders the declared fractional-seconds precision per SQL dialect (value) | Integration (live, 2025.1.16) | `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs` | task 5.2(c) |

The declaration and coercion rules take unit tests because both are pure computation over a version
string or a declared type, with no I/O. Three claims cannot be settled that way and take integration
tests: that the SLC's strict Arrow-IPC feed accepts a millisecond and a nanosecond column at all,
that Exasol declares and returns `TIMESTAMP(9)` for a nanosecond source, and that a declined CAST
returns Exasol's own value through the wrapper. Only a running engine answers those.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| datafusion-scan/type-mapping-timestamp-precision | `docker compose down -v && docker compose up -d && make test-e2e` | `e2e_timestamp_precision_test` passes on the default `2025.1.16` image at all three emit widths. No fingerprint-mismatch error at UDF load |
| datafusion-scan/type-mapping-timestamp-precision (clamped arm) | `docker compose down -v && export EXASOL_IMAGE=exasol/docker-db:8.29.13 && docker compose up -d && make test-e2e` | `e2e_timestamp_precision_test` passes. Both catalog timestamp columns are declared bare `TIMESTAMP`, emitted at millisecond, and `COUNT(DISTINCT)` is 2 with no emit rejection |
| sql-comprehension/vs-expression-translator-cast | `cargo test -p vs-expression timestamp_precision` | 0 failures |
| datafusion-scan/scan-execution-value-conversion | `make print-slc-version` | `0.28.1` |
| datafusion-scan/scan-execution-partial-agg | `cargo test -p lakehouse-engine partial_agg` | 0 failures |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test (host) | `cargo test` | 0 failures |
| Test (E2E, 2025.x) | `docker compose down -v`, then `docker compose up -d` and `make test-e2e` | 0 failures |
| Test (E2E, 8.x) | `docker compose down -v`, then `EXASOL_IMAGE=exasol/docker-db:8.29.13` exported, `docker compose up -d` and `make test-e2e` | 0 failures |
| Compile gate (E2E crates) | `cargo check --workspace --all-targets --features exasol-e2e,unity-e2e,lakekeeper-e2e,azure-e2e,cloud-e2e` | Exit 0 |
| Lint | `cargo clippy --all-targets` | 0 errors and 0 warnings |
| Format | `cargo fmt` | No changes |
