# Plan: add-delta-file-pruning

## Summary

Translate the Exasol WHERE filter into a `delta_kernel` predicate and hand it to the Delta scan builder,
so log replay drops files on partition values and per-file min/max statistics before any Parquet byte is
read. Closes issue #321, the last deferral `vs-adapter/delta-table-planning` carries.

## Design

### Context

Issue #320 wired production pushdown into the Delta path, so projection, filter, LIMIT, GROUP BY,
ORDER BY + LIMIT, and broadcast joins all reach a Delta table and file sharding works unchanged. What it
did not deliver is pruning: `DeltaFormatReader::resolve_scan` takes `_filter_json` and drops it, so every
query reads every active file. Iceberg has pruned since `vs-adapter/pushdown-file-pruning`; Delta is the
odd path out.

Three findings shaped this plan, all verified against source rather than recalled.

1. **The kernel already prunes; nothing needs hand-rolling.** `ScanBuilder::with_predicate` drives both
   partition pruning and stats skipping through one private `DataSkippingFilter` pass, and
   `scan_metadata()` returns a selection vector `append_active_files` already honours
   (`if !selected.get(row)… { continue; }`). So the reader gains a predicate and loses nothing else —
   no stats parsing, no bound comparison, no post-filtering of the resolved list.
2. **The shipped `StatsOptions::none()` would silently defeat the whole feature.** Its doc: "**Disables
   all stats work**: no stats output, no internal data skipping (even when a predicate is set)."
   `Scan::skip_stats()` is true for exactly that construction, and `log_replay.rs` disables "both data
   column skipping and partition pruning" together on it. `ScanBuilder::new` already defaults to
   `StatsOptions::default()`, so **deleting the call is the entire configuration change** — adding a
   predicate without deleting it ships a no-op that every test would still pass.
3. **Delta's truncated string bounds are sound, and this had to be checked.** The protocol's
   "Per-file Statistics" footnote — "String columns are cut off at a fixed prefix length" — reads as
   though a truncated `maxValues` could fall below the true max, which would make range pruning drop
   real rows. It cannot: the footnote describes a writer mechanism and does not relax the normative
   "greater than or equal to all valid values". delta-spark's `truncateMaxStringAgg` appends a
   max-codepoint tie-breaker, extends the prefix when no tie-breaker is provably safe, and omits the
   stat entirely when it cannot; `parquet`'s `increment_utf8` upholds the same invariant independently.
   The decisive evidence is an asymmetry inside the kernel: it compensates for the **timestamp** half of
   that same footnote (`adjust_scalar_for_max_stat_truncation` subtracts 999 µs) and deliberately does
   not for the string half.

- **Goals** — prune Delta files at plan time on partition values and per-file statistics; ship full
  operator and literal-type parity with `iceberg_predicate.rs`; keep every result byte-identical to its
  unpruned value; add no `ScanSpec` field and no pushdown-façade item.
- **Non-Goals** — a shared format-neutral predicate IR across the three existing filter-JSON walkers;
  any change to Iceberg behavior, to the `ScanSpec` wire format, or to scan-side filter application;
  surfacing statistics to the engine or the wire; pruning on any mechanism the kernel does not already
  implement.

### Decision

#### Architecture

```
  DeltaFormatReader::resolve_scan(filter_json)          ← parameter already exists; stop dropping it
        │
        └─ read_delta_log(storage, table_root, secrets, filter_json)
              │
              ├─ DeltaSnapshot::open                                   (unchanged; protocol gate inside)
              ├─ build_delta_table_schema                              (unchanged)
              │
              ├─ delta_predicate::to_delta_predicate(filter_json, &snapshot.schema())
              │        Option<PredicateRef>          None = "no constraint" = pass all files
              │        NEVER Predicate::literal(false), NEVER an empty junction
              │
              └─ snapshot.active_files(prune)
                       scan_builder()
                         .with_predicate(prune)     ← new
                         ·                          ← .with_stats(StatsOptions::none()) DELETED
                         .without_row_transforms()
                       ▼
                    scan_metadata()  →  FilteredEngineData { data, selected }
                       ▼
                    append_active_files            (unchanged — already honours `selected`)
```

The predicate is built in `read_delta_log` because that is where the snapshot's schema first exists and
where "open, build schema, replay" already compose. `active_files` takes `Option<PredicateRef>` and
`delta_predicate` never sees a `DeltaSnapshot`, so the filter-JSON vocabulary and the kernel-replay
vocabulary stay in separate modules.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| `Option<Predicate>` means "no constraint", never "no files" | `to_delta_predicate` | The identical convention `to_iceberg_predicate` documents. Inverting it once anywhere returns an empty result set instead of a slow query. |
| Drop under AND, forfeit under OR | `fold_and` / `fold_or` | A dropped conjunct only widens the surviving file set; a dropped disjunct narrows it below what the request implies. Ported from `iceberg_predicate.rs` unchanged in semantics. |
| Delete `StatsOptions::none()` rather than replace it | `DeltaSnapshot::active_files` | `ScanBuilder`'s own default is already the mode wanted — internal skipping on, no `stats_parsed` surfaced. Naming a mode explicitly would pin a default the kernel owns. |
| IN desugars to an OR-chain of equalities | `translate_in` | `eval_pred_in` returns `None` with no override in the crate, so the kernel's native IN prunes nothing. |
| No junction constructor on an empty set | `translate_in`, `fold_or` | `Predicate::or_from([])` normalizes to literal `false`, which prunes every file. The empty case must return `None` before reaching the constructor. |
| Literal typed from the column, not from the JSON | `literal_to_scalar` | Mirrors `literal_to_datum`'s `(kind, prim)` pairing: the column's declared type is the authority, so a mismatch yields `None` instead of a coerced value. |
| Case-insensitive column resolution to the exact field name | `resolve_column` | `StructType::field` is case-sensitive and Exasol upper-cases names, so a direct lookup would silently translate nothing. |
| Translator private to `format` | `format/delta_predicate.rs` | Same rule `pushdown-module-structure` records for `delta_protocol` and `delta_schema`: reached only from inside `format`, so the frozen façade admits no item. |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| A third independent walker over the filter JSON | Extract a shared predicate IR consumed by the Iceberg, Delta, and DataFusion-render paths | The three outputs are an `iceberg::spec::Predicate`, a `delta_kernel::Predicate`, and a SQL string; their literal vocabularies and bound-soundness contracts differ, and `render_df_filter_safe` exposes no typed AST to reuse. A unifying IR would be a fourth vocabulary bridging two, sized by the union of all three — a large refactor of shipped, tested code, justified by no requirement here. Filed as a follow-up, not smuggled into this plan. |
| Translator in `adapter/pushdown/format/`, not beside `adapter/iceberg_predicate.rs` | Mirror the Iceberg translator's top-level `adapter/` placement | `iceberg_predicate` is `pub` at `adapter/` for historical reasons, predating the format seam. `delta_protocol` and `delta_schema` set the current precedent, and the recorded façade rule requires a format-internal helper stay unnamed on the façade. |
| Build the predicate in `read_delta_log` | Build it in `resolve_scan`, or pass `filter_json` into `active_files` | The schema exists only after `DeltaSnapshot::open`, which `read_delta_log` performs, so `resolve_scan` cannot. Passing JSON into `active_files` would give the kernel-replay module a second vocabulary to know. |
| Trust the writer-side string-bound invariant | Refuse to translate any comparison over a string data column | Refusing forfeits real pruning — including `multi_part_stats`'s `value` column — to defend against a writer no shipped implementation matches, and partition-column strings are exact anyway. Named as a deliberate protocol-trust trade-off in the spec rather than left unstated. |
| No stats field on `ScanSpec` | Carry per-file bounds to the scan for row-group pruning | The kernel compares bounds during replay and returns a selection vector; nothing downstream reads a bound. DataFusion already does its own row-group pruning from the Parquet footer. |
| Verify the `multi_part_stats` struct-stats path before relying on it | Assume the fixture prunes | That fixture is `writeStatsAsStruct=true` + `writeStatsAsJson=false`, exactly the shape of delta-kernel-rs issue #2541 ("ScanFile.stats is null for struct-stats-only checkpoints"). Silent degradation is keep-everything, which passes a rows-correct assertion while proving nothing. Task 1.1 gates the plan on observing a real prune. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| `vs-adapter/delta-file-pruning` | NEW | `vs-adapter/delta-file-pruning/spec.md` |
| `vs-adapter/delta-table-planning` | CHANGED | `vs-adapter/delta-table-planning/spec.md` |
| `e2e-harness/unity-catalog-e2e-harness-delta-queries` | CHANGED | `e2e-harness/unity-catalog-e2e-harness-delta-queries/spec.md` |

## Impact

**Delta queries read fewer files; results do not change.** A query whose WHERE clause targets a
partition column or falls outside a file's logged min/max bounds now opens only the files that can hold
a matching row. On the seeded fixtures a partition equality drops 4 of 6 files and a range predicate
drops 3 of 5. Nothing about the returned rows changes, because the full predicate is still evaluated
above the scan — in DataFusion when the dialect renders it, otherwise in the adapter's own outer `WHERE`.

**No breaking change, and no new failure mode.** The kernel fails open at three documented layers, so a
predicate it cannot use costs pruning rather than rows. Pruning to zero files routes to the adapter's
existing empty-result path. No `ScanSpec` field is added or altered, so every golden scan-spec encoding
for a non-pruning request stays byte-identical. No Iceberg behavior changes. Operators gain no new
property to set: pruning follows the request's filter with nothing to configure.

Version impact: `feat` — MINOR bump on `crates/lakehouse-engine` (0.37.0 → 0.38.0).

## Dependencies

| Dependency | Detail |
|------------|--------|
| `delta_kernel` 0.26 public API | `ScanBuilder::with_predicate(impl Into<Option<PredicateRef>>)`, `Snapshot::schema() -> SchemaRef`, `StructType::field`/`fields`, `StructField::data_type`, `PrimitiveType::parse_scalar`, `Predicate::{eq,lt,le,gt,ge,not,and,or,and_from,or_from,is_null,is_not_null}`, `Expression::{column,literal}`, `Scalar`. All plain `pub`; no new cargo feature. `PredicateRef = Arc<Predicate>`. |
| `internal-api` cargo feature | Already enabled and already used by `delta_replay`. This plan needs NO new accessor from it: the kernel prunes partitions from the same predicate, so the reader never asks which columns are partition columns in order to translate. |
| Vendored Delta fixtures | `basic_partitioned` and `multi-part-stats` already vendored under `scripts/unity/fixtures/` and already seeded by `scripts/unity/seed.sh`. No new fixture, no new seeding, no new Makefile target. |
| delta-kernel-rs issue #2541 | Struct-stats-only checkpoints may surface null stats. Degrades to keep-everything (safe). Gated by task 1.1; if it bites, the stats scenarios move to a synthetic JSON-stats log and the fixture limitation is recorded with a tracked issue. |
| Issue #321 | This plan. Cite `Closes #321` in the implementing commit. |

## Implementation Tasks

1. **Gate the plan's central assumption before writing the translator**
   1. Add a temporary probe test in `delta_replay_tests.rs` that opens `multi-part-stats` and
      `basic_partitioned` through `DeltaSnapshot`, builds the scan WITHOUT `StatsOptions::none()`, hands
      it a hand-built `Predicate::gt(Expression::column(["id"]), Expression::literal(3i64))` and a
      `Predicate::eq(Expression::column(["letter"]), Expression::literal("a"))`, and asserts the
      returned file counts are 2 and 2 rather than 5 and 6. This is the falsifiable check that
      `StatsOptions::default()` really enables both skipping paths on THESE fixtures, and that
      delta-kernel-rs #2541 does not silently defeat the struct-stats checkpoint. Halt and report if
      either count is unpruned. [expert]
   2. In the same probe, assert the counts are 5 and 6 when `StatsOptions::none()` is restored, pinning
      the mechanism the plan removes rather than only the behavior it adds. Keep both assertions as a
      permanent test; drop only the scaffolding.

2. **The predicate translator**
   1. Create `crates/lakehouse-engine/src/adapter/pushdown/format/delta_predicate.rs` with its
      `#[path = "delta_predicate_tests.rs"] mod tests;` sibling, declare it privately in
      `format/mod.rs`, and add a failing test that an equality on an integer column translates. Then
      implement `to_delta_predicate(filter_json: &Json, schema: &StructType) -> Option<Predicate>` for
      that one node kind, with the doc comment stating that `None` means "no constraint — pass all
      files, never no files".
   2. Implement `resolve_column`, matching a request column case-insensitively against
      `schema.fields()` and returning the exact field name plus its `PrimitiveType`; a non-primitive or
      unknown column yields `None`.
   3. Implement `literal_to_scalar(lit, prim)` over the `(literal kind, column primitive)` pairs
      `literal_to_datum` handles: `literal_bool`, `literal_exactnumeric`, `literal_double`,
      `literal_string`, `literal_date`, `literal_timestamp`, `literal_timestamp_utc`. Route date and
      both timestamp forms through `PrimitiveType::parse_scalar` so a timestamp becomes microseconds and
      a date becomes days; return `None` for an empty string, and `None` for any pair the column's type
      cannot represent. Add the decimal pairing that `iceberg_predicate` lacks, since `Scalar::Decimal`
      exists and Delta declares `decimal(p,s)` columns. [expert]
   4. Implement the five comparison node kinds (`predicate_equal`, `predicate_less`,
      `predicate_lessequal`, `predicate_greater`, `predicate_greaterequal`) with the column-on-either-side
      flip, and return `None` for `predicate_notequal` with the "not soundly prunable to a single range"
      reason in a doc comment.
   5. Implement `predicate_is_null` and `predicate_is_not_null`, and `predicate_not` via the free
      function `Predicate::not` — there is no `.negate()` method on this predicate type.
   6. Implement `fold_and` (drop `None` children, `None` when all children are `None`) and `fold_or`
      (any `None` child forfeits the whole disjunction; an empty expression list yields `None`). Assert
      in tests that neither ever constructs a literal-false predicate nor calls a junction constructor
      on an empty set. [expert]
   7. Implement `predicate_in_constlist` as an OR-chain of equalities: every element must convert or the
      node yields `None`, and an empty element set returns `None` BEFORE reaching `or_from`, because
      `or_from([])` normalizes to literal false and would prune the whole table. [expert]
   8. Implement `predicate_between` as a lower-bound AND an upper-bound comparison, keeping one bound
      when the other fails to convert and yielding `None` only when both fail.
   9. Complete `delta_predicate_tests.rs` against the shape of `iceberg_predicate_tests.rs`: one test
      per node kind, per literal type, per fold rule, plus `unknown_column_returns_none`,
      `type_mismatch_returns_none`, `notequal_returns_none`, `in_with_type_mismatch_element_returns_none`,
      `empty_in_list_returns_none_not_a_false_predicate`, `between_with_one_failing_bound_keeps_other`,
      and a case-insensitive resolution test using an upper-cased request name against a lower-cased
      Delta field.

3. **Wire the predicate into the reader**
   1. Change `DeltaSnapshot::active_files` to take `prune: Option<PredicateRef>`, pass it to
      `.with_predicate(...)`, and DELETE the `.with_stats(StatsOptions::none())` call so the builder
      keeps its own default; drop the now-unused `StatsOptions` import. Update the seven existing test
      call sites to pass `None`, and add a `replay_fixture_pruned(table, predicate)` helper beside
      `replay_fixture` so the fixture-based pruning tests read as one line each.
   2. Change `read_delta_log` to take `filter_json: Option<&Json>`, build the predicate from it and
      `snapshot.schema()` after the snapshot opens, and pass it to `active_files`. Its single production
      caller is `resolve_scan`.
   3. Change `DeltaFormatReader::resolve_scan` to forward `filter_json` instead of dropping it, and
      REPLACE its doc comment — the shipped text claims the reader carries no statistics and defers
      partition pruning to the scan side, both now false.
   4. Add integration tests in `delta_replay_tests.rs` over the vendored fixtures via
      `replay_fixture_pruned`, asserting exact surviving file sets (not just counts) for:
      `LETTER = 'a'` → 2, `LETTER IS NULL` → the default-partition file alone, `LETTER = 'z'` → 0,
      `NUMBER <= 2` → 2, `ID = 3` → 1, `ID <= 2` → 2, `ID = 99` → 0, and `"VALUE" = 'value_3'` → 1.
   5. Add integration tests for the fail-open contract: a predicate over a column no file carries stats
      for keeps every file; a predicate mixing one usable comparison with one unusable one still prunes
      by the usable one; a boolean-column equality keeps every file. Each MUST assert success, not an
      error.
   6. Add a `delta_format_reader_tests.rs` test that `resolve_scan(Some(&filter))` returns a pruned
      `ResolvedScan` whose `files` are fewer than `resolve_scan(None)`'s, and that its `logical_schema`,
      `partition_columns`, `table_root`, `name_mapping`, and `refused_columns` are unchanged by pruning.
   7. Add a test asserting the serialized scan spec for a Delta request carries no statistic field and
      is byte-identical to its pre-change encoding when the filter prunes nothing, so the
      format-neutrality rule has a guard rather than only a spec sentence.

4. **Verify the two behaviors the kernel owns but this reader depends on**
   1. Verify empirically whether the kernel maps a LOGICAL column name in the predicate to the PHYSICAL
      stat path under column mapping, using `cdf-column-mapping-name-mode` and
      `cdf-column-mapping-id-mode`: assert either that pruning works on a logical name, or that it
      degrades to keep-all. Record the observed answer in the spec; if it degrades, file the follow-up
      issue and cite it inline rather than leaving the gap unstated. Correctness is not at risk either
      way — a physical-name mismatch folds to keep-all. [expert]
   2. Verify that pruning to an EMPTY file list reaches the adapter's existing empty-result route rather
      than fanning out to zero shards or emitting a shard with an empty file list, with a pushdown-level
      test over a Delta table and a `LETTER = 'z'`-shaped filter. [expert]

5. **End-to-end coverage**
   1. Add a plan-time E2E test in `e2e_unity_test.rs` beside
      `unity_delta_planning_agrees_under_vended_and_static_credentials`, extending `resolve_delta_scan`
      to accept a filter, asserting the pruned file counts for `basic_partitioned` under
      `LETTER = 'a'` and `multi_part_stats` under `ID <= 2` against real MinIO-backed storage — the
      Delta counterpart of `e2e_range_filter_prunes_by_file_bounds`.
   2. Add a query E2E test asserting rows are unchanged under pruning: the partition predicate, the
      range predicate, an equality, a no-file-matching predicate returning zero rows, and a
      prunable-AND-unprunable mix (an equality alongside a `LIKE`) returning what both predicates
      select. Double-quote `VALUE`, which is reserved in Exasol.
   3. Extend one of those tests to capture the generated pushdown SQL with `explain_virtual_sql` and
      assert both that it drives the scan UDF and that it embeds FEWER data-file paths than the table's
      active file count, summing across shard literals because the shard count clamps to the file count.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2 |
| Group B | 2.1 |
| Group C | 2.2, 2.3 |
| Group D | 2.4, 2.5, 2.6, 2.7, 2.8 |
| Group E | 2.9, 3.1 |
| Group F | 3.2, 3.3 |
| Group G | 3.4, 3.5, 3.6, 3.7 |
| Group H | 4.1, 4.2 |
| Group I | 5.1, 5.2, 5.3 |

Sequential dependencies:
- Group A → Group B (the plan does not proceed until pruning is observed on the real fixtures)
- Group B → Group C (the module and entry point exist before its helpers)
- Group C → Group D (column resolution and literal conversion exist before the node kinds using them)
- Group D → Group E (every node kind exists before its test sweep; `active_files` rewiring is
  independent of the translator's internals and only needs the module to exist)
- Group E → Group F → Group G (the reader chain is wired before its behavior is asserted)
- Group G → Group H → Group I (E2E runs against finished behavior)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Call + import | `.with_stats(StatsOptions::none())` and the `StatsOptions` import in `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay.rs` | Disables the internal data skipping this plan exists to enable. `ScanBuilder`'s own default is the wanted mode, so the call is deleted rather than re-parameterized. |
| Doc comment | `DeltaFormatReader::resolve_scan`'s `filter_json` paragraph in `delta_format_reader.rs` | Claims the reader prunes nothing, carries no statistics, and leaves partition pruning to the scan side. All three become false. |
| Parameter name | `_filter_json` in `DeltaFormatReader::resolve_scan` | The underscore records a deliberately-ignored argument that is now used. |

No production function becomes unreachable: the translator is new, and `read_delta_log` and
`active_files` gain a parameter rather than being replaced. `append_active_files` needs no change at
all — it already honours the kernel's selection vector.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| delta-file-pruning: Equality on a partition column prunes every file in a non-matching partition | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `a_partition_equality_prunes_every_file_in_a_non_matching_partition` |
| delta-file-pruning: Equality on a partition column prunes every file in a non-matching partition | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `an_is_null_partition_predicate_resolves_the_default_partition_file_alone` |
| delta-file-pruning: Equality on a partition column prunes every file in a non-matching partition | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `a_delta_request_pruned_to_no_file_takes_the_empty_result_route` |
| delta-file-pruning: A range predicate prunes files whose min/max bounds exclude the value | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `a_range_predicate_prunes_files_whose_logged_bounds_exclude_the_value` |
| delta-file-pruning: A range predicate prunes files whose min/max bounds exclude the value | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `a_between_keeps_one_bound_when_the_other_fails_to_convert` |
| delta-file-pruning: An untranslatable conjunct disables pruning for that conjunct only | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_predicate_tests.rs` | `and_with_untranslatable_child_keeps_translatable_conjunct` |
| delta-file-pruning: An untranslatable conjunct disables pruning for that conjunct only | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_predicate_tests.rs` | `and_all_untranslatable_returns_none_not_a_true_predicate` |
| delta-file-pruning: An untranslatable conjunct disables pruning for that conjunct only | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `a_partly_untranslatable_conjunction_still_prunes_by_its_translatable_half` |
| delta-file-pruning: An untranslatable branch of an OR disables pruning entirely | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_predicate_tests.rs` | `or_with_untranslatable_child_returns_none` |
| delta-file-pruning: An untranslatable branch of an OR disables pruning entirely | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `an_or_with_an_untranslatable_branch_keeps_every_file` |
| delta-file-pruning: An IN list prunes as an OR-chain of equalities and never as an empty junction | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_predicate_tests.rs` | `in_list_translates_to_an_or_chain_of_equalities` |
| delta-file-pruning: An IN list prunes as an OR-chain of equalities and never as an empty junction | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_predicate_tests.rs` | `empty_in_list_returns_none_not_a_false_predicate` |
| delta-file-pruning: An IN list prunes as an OR-chain of equalities and never as an empty junction | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `an_in_list_prunes_to_the_union_of_its_element_files` |
| delta-file-pruning: A literal is typed from the column's Delta type or its node is dropped | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_predicate_tests.rs` | `boolean_literal_becomes_boolean_scalar`, `exactnumeric_literal_becomes_integer_scalar`, `double_literal_becomes_double_scalar`, `string_literal_becomes_string_scalar`, `date_literal_becomes_days_since_the_epoch`, `exactnumeric_literal_rescales_to_the_decimal_column_scale` |
| delta-file-pruning: A literal is typed from the column's Delta type or its node is dropped | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_predicate_tests.rs` | `timestamp_literal_becomes_microseconds_on_a_zoneless_column` |
| delta-file-pruning: A literal is typed from the column's Delta type or its node is dropped | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_predicate_tests.rs` | `resolve_column_returns_none_for_unknown_column`, `literal_the_column_type_cannot_represent_yields_no_scalar`, `notequal_returns_none`, `empty_string_literal_yields_no_scalar` |
| delta-file-pruning: A literal is typed from the column's Delta type or its node is dropped | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_predicate_tests.rs` | `resolve_column_matches_case_insensitively` |
| delta-file-pruning: Enabling the kernel's skipping surfaces no statistic to the engine or the wire | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `the_stats_disabling_option_is_what_suppresses_pruning` |
| delta-file-pruning: Enabling the kernel's skipping surfaces no statistic to the engine or the wire | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader_tests.rs` | `pruning_changes_only_the_file_list_of_the_resolved_scan` |
| delta-file-pruning: Enabling the kernel's skipping surfaces no statistic to the engine or the wire | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `a_non_pruning_delta_request_keeps_its_pre_change_field_set_and_carries_no_statistic` |
| delta-file-pruning: A predicate the kernel cannot evaluate keeps every file | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `a_predicate_over_a_statless_or_boolean_column_keeps_every_file` |
| delta-file-pruning: A predicate the kernel cannot evaluate keeps every file | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `pruning_under_column_mapping_records_its_observed_behavior` |
| delta-file-pruning: Pruning reaches every request shape and changes no result end to end | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_filters_prune_the_resolved_file_list` |
| delta-file-pruning: Pruning reaches every request shape and changes no result end to end | Integration | `crates/lakehouse-engine/src/adapter/pushdown/joins/joins_tests.rs` | `each_delta_join_leg_prunes_by_its_own_side_local_predicate` |
| delta-file-pruning: Pruning reaches every request shape and changes no result end to end | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_pruned_pushdown_sql_carries_fewer_files_and_drives_the_scan_udf` |
| delta-table-planning: A Delta table resolves its current version's active data files (CHANGED) | Integration | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `replay_reads_the_active_files_out_of_a_multi_part_checkpoint` (unchanged, passes with `None`) |
| delta-table-planning: The Delta reader is reached from production pushdown under the Unity Catalog kind (CHANGED) | Integration | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `a_unity_catalog_pushdown_prunes_the_delta_file_list_by_its_filter` |
| e2e: A query whose files were pruned returns the same rows as before pruning | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_pruned_queries_return_unchanged_rows` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| `vs-adapter/delta-file-pruning` | `make unity-up` then `exapump sql --dsn "$LH_DSN;validateservercertificate=0" "EXPLAIN VIRTUAL SELECT LETTER, NUMBER FROM UNITY_DELTA_E2E_VS.BASIC_PARTITIONED WHERE LETTER = 'a'"` | The printed scan-driving SQL embeds exactly 2 `.parquet` paths, both under `letter=a/`, against 6 for the same query without the WHERE clause. |
| `vs-adapter/delta-file-pruning` | `exapump sql --dsn "$LH_DSN;validateservercertificate=0" "EXPLAIN VIRTUAL SELECT ID FROM UNITY_DELTA_E2E_VS.MULTI_PART_STATS WHERE ID <= 2"` | The printed scan-driving SQL embeds exactly 2 `.parquet` paths against 5 unfiltered, proving statistics pruning over a struct-stats checkpoint. |
| `vs-adapter/delta-file-pruning` | `exapump sql --dsn "$LH_DSN;validateservercertificate=0" "SELECT COUNT(*) FROM UNITY_DELTA_E2E_VS.BASIC_PARTITIONED WHERE LETTER = 'z'"` | `0`, returned as a normal empty result with no error and no UDF invocation. |
| `vs-adapter/delta-table-planning` | `exapump sql --dsn "$LH_DSN;validateservercertificate=0" "SELECT LETTER, NUMBER, A_FLOAT FROM UNITY_DELTA_E2E_VS.BASIC_PARTITIONED"` | All 6 rows, one `LETTER` NULL, identical to the pre-change output — pruning is inert without a filter. |
| `e2e-harness/unity-catalog-e2e-harness-delta-queries` | `make test-e2e-unity` | Exit 0; the new pruning tests pass; every previously-passing Delta query scenario still passes. |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E (Unity/Delta) | `make test-e2e-unity` | 0 failures |
| E2E (Iceberg regression) | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |
