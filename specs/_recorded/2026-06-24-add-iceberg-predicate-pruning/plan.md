# Plan: add-iceberg-predicate-pruning

## Summary

Translate the Exasol pushdown WHERE predicate into an `iceberg::expr::Predicate` and set it on the table scan so `plan_files` prunes data files on partition values and per-file min/max bounds before any S3 I/O, while DataFusion keeps applying the full filter as the sole source of row-level correctness.

## THE Correctness Invariant

The Iceberg filter is a **pruning-only** filter: it may ONLY skip files that provably contain zero matching rows. DataFusion remains the SOLE source of row-level correctness — every per-shard scan always applies the full `ScanSpec.filter`. The Iceberg translation must therefore be **sound, not complete**: every conjunct it emits must be logically implied by the user predicate; any node it cannot translate soundly is dropped, and the scan simply prunes less. Dropping a conjunct can only ever *widen* the surviving file set, never narrow it past correctness. A wrong (over-narrow) Iceberg predicate would silently drop result rows with no backstop — that is the one failure mode this plan must make impossible.

## Design

### Context

Today the WHERE predicate is rendered to a DataFusion SQL string (`render_df_filter_safe`) and stored in `ScanSpec.filter`, giving DataFusion row-group + row-level filtering inside each scan UDF. But `resolve_file_list` → `plan_files_from_table` calls `table.scan().select_all().build()` with **no `.with_filter(...)`** — so every data file in the snapshot is planned, sharded, and opened regardless of the filter. For a partitioned table or a table with selective per-file bounds, this opens far more Parquet files (and does far more S3 I/O) than necessary.

- **Goals** — Apply a sound Iceberg pruning predicate at file-resolution time so `plan_files` skips files that cannot match; keep `iceberg-rust` types out of the shared `vs-expression` crate; change nothing about correctness, the scan UDF, sharding, capabilities, or the wrapper SQL.
- **Non-Goals** — No capability-handshake change; no residual-predicate elimination in DataFusion (the full filter stays, intentionally); no new aggregate partials; no change to multi-table identity resolution (prior plan `change-multi-table-virtual-schema`, assumed landed).

### Decision

A new module `crates/lakehouse-engine/src/adapter/iceberg_predicate.rs` translates the raw Exasol filter JSON (the same JSON the DataFusion path reads from `pushdownRequest.filter`) into an `Option<iceberg::expr::Predicate>` against the resolved Iceberg `Schema`. `handle_pushdown` threads the raw filter JSON down through `resolve_file_list` to `plan_files_from_table`, which — having the `iceberg::table::Table` in scope — reads `table.metadata().current_schema()`, builds the predicate, and applies it via `scan().with_filter(predicate)` on BOTH the signed and unsigned paths. The existing DataFusion-string filter in `ScanSpec.filter` is untouched and coexists.

Verified against iceberg-rust 0.9.1 source (`~/.cargo/registry/src/.../iceberg-0.9.1/`):
- `TableScanBuilder::with_filter(predicate: Predicate)` exists (`src/scan/mod.rs:98`) and internally calls `predicate.rewrite_not()`.
- `plan_files` binds the filter itself: `predicates.bind(schema.clone(), true)` with `case_sensitive: true` (`src/scan/mod.rs:276-277`). So the translator emits an **unbound** `Predicate`; no manual `.bind()` is needed, but the `Reference` name MUST match the Iceberg field name with exact (case-sensitive) casing or `bind` errors / mis-binds.
- Per-file min/max pruning is `InclusiveMetricsEvaluator::eval` (`src/scan/mod.rs:495`); partition pruning is the `ManifestEvaluator` — both engaged once a filter is set.
- Builders present: `Reference::new(name)` → `.equal_to / .less_than / .less_than_or_equal_to / .is_in / .is_null / .is_not_null` (`src/expr/term.rs`); `Predicate::and / .or / .negate` (`src/expr/predicate.rs:563-624`); `Datum::{bool,int,long,float,double,string,date,timestamp_*}` (`src/spec/values/datum.rs`).

#### Architecture

```
handle_pushdown (pushdown.rs)
  │  filter JSON (pushdownRequest.filter)  ── also → render_df_filter_safe → ScanSpec.filter (UNCHANGED)
  ▼
resolve_file_list(..., filter_json)            signed + unsigned paths
  ▼
plan_files_from_table(table, filter_json)
  │  schema = table.metadata().current_schema()
  │  to_iceberg_predicate(filter_json, schema)  ──► iceberg_predicate.rs
  ▼                                              Option<Predicate>  (sound-partial)
table.scan()[.with_filter(pred)].select_all().build().plan_files()
  ▼
pruned data-file list  → shards → scan-driving SQL
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Sound-partial translation (drop, never mistranslate) | `iceberg_predicate.rs` | Mirrors `render_df_filter_safe`'s conservative contract; a dropped conjunct only widens the file set |
| `Option<Predicate>` return | `to_iceberg_predicate` | `None` = "no constraint" (scan unfiltered); never fabricate `AlwaysTrue`/`AlwaysFalse` |
| Schema-driven typing & casing | `to_iceberg_predicate` | Resolve Exasol uppercased column → Iceberg field (case-insensitive lookup → exact field name) and build the `Datum` variant matching the field's primitive type |
| Apply at the file-resolution seam | `plan_files_from_table` | The `Table` (hence schema) is already in scope on both signed and unsigned paths; single choke point |

#### Sound AND/OR/NOT semantics (the correctness core)

`to_iceberg_predicate(node)` returns `Option<Predicate>` where `None` means "cannot constrain — treat as no-op":

- **Leaf comparison** (`=`, `<`, `<=`, `IN`, `IS NULL`, `IS NOT NULL`, `BETWEEN` desugared to `>= low AND <= high`, where only `<=`/`<` arrive because Exasol pre-normalises `>`→`<`): translate iff the column resolves to an Iceberg field AND the literal builds a type-matching `Datum`; else `None`.
- **AND(a, b)**: combine the `Some` children with `Predicate::and`; if one child is `None`, return the other (dropping a conjunct under AND only widens the set — sound). If both `None`, return `None`.
- **OR(a, b)**: if ANY child is `None`, the whole OR must be `None` (a row matching the untranslatable branch may live in any file — pruning on the translatable branch alone would wrongly skip files). Only when BOTH children are `Some` return `a.or(b)`.
- **NOT(a)**: if `a` is `None`, return `None` (cannot soundly negate an unknown). Else return `a_pred.negate()`. (iceberg's `with_filter` runs `rewrite_not`, and `bind` pushes negation to leaf level; `NOT` over an untranslatable child is dropped.)
- **n-ary** `predicate_and`/`predicate_or` (Exasol carries an `expressions` array): fold pairwise with the rules above (AND folds dropping `None`s; OR collapses to `None` if any element is `None`).

This is the single expert-critical correctness property; it is unit-tested directly (Task 5).

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| New `iceberg_predicate.rs` in lakehouse-engine | Extend `vs-expression` to emit iceberg predicates | Keeps `iceberg-rust` types out of the cross-project-shared `vs-expression` crate (locked decision 2) |
| Emit unbound `Predicate`; let `plan_files` bind | Bind in the translator via `predicate.bind(schema, true)` | iceberg 0.9.1 `plan_files` already binds with `case_sensitive: true`; double-binding is redundant and the unbound form is what `with_filter` expects |
| Build the predicate inside `plan_files_from_table` | Build in `handle_pushdown`, pass `Predicate` down | The Iceberg `Schema` is only assembled where the `Table` is built (both signed/unsigned); passing raw JSON + building at the seam avoids resolving the schema twice |
| Drop untranslatable conjuncts (sound-partial) | Decline pushdown / error when any node is untranslatable | DataFusion is the correctness backstop; pruning is best-effort optimisation, so less pruning is always safe |
| OR with any untranslatable branch → no constraint | Prune on the translatable branch only | Unsound: would skip files that the untranslatable branch could match — the subtle correctness trap |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning | CHANGED | `specs/_plans/add-iceberg-predicate-pruning/vs-adapter/pushdown-planning/spec.md` |
| packaging/e2e-harness | CHANGED | `specs/_plans/add-iceberg-predicate-pruning/packaging/e2e-harness/spec.md` |

## Dependencies

- Serialised AFTER `change-multi-table-virtual-schema` (assumed landed): per-pushdown table identity and the scanned Iceberg `TableIdent` are already resolved into `CatalogProps`/scan inputs by the time file resolution runs. This plan does not re-plan multi-table.
- `iceberg` 0.9.1 (workspace pin) — `expr::Predicate`, `expr::Reference`, `spec::Datum`, `TableScanBuilder::with_filter`.

## Implementation Tasks

1. **Translator module**
   - [ ] 1.1 Create `crates/lakehouse-engine/src/adapter/iceberg_predicate.rs`; register `mod iceberg_predicate;` in `adapter/mod.rs`.
   - [ ] 1.2 Implement column resolution: map an Exasol uppercased column name to its Iceberg `NestedField` via case-insensitive lookup over `Schema`, returning the exact field name + primitive type (or `None` if absent). [expert]
   - [ ] 1.3 Implement literal→`Datum` construction keyed on the resolved field's primitive type (bool/int/long/float/double/string/date/timestamp[tz]); return `None` on any type mismatch or unparsable literal. [expert]
   - [ ] 1.4 Implement `fn to_iceberg_predicate(filter_json: &Json, schema: &iceberg::spec::Schema) -> Option<Predicate>` with the sound AND/OR/NOT/leaf semantics for `predicate_equal` / `predicate_less` / `predicate_lessequal` / `predicate_in_constlist` / `predicate_is_null` / `predicate_is_not_null` / `predicate_between`, dropping every other node type (`predicate_like`, `predicate_like_regexp`, `function_scalar_*`, etc.). [expert]
2. **Thread the filter into file resolution**
   - [ ] 2.1 Add a `filter_json: Option<&Json>` parameter to `resolve_file_list` and pass it through on BOTH the signed and unsigned paths.
   - [ ] 2.2 Add a `filter_json: Option<&Json>` parameter to `plan_files_from_table`; read `table.metadata().current_schema()`, call `to_iceberg_predicate`, and apply `scan = scan.with_filter(pred)` when `Some` before `.select_all().build()`.
   - [ ] 2.3 In `handle_pushdown`, pass the raw `pushdownRequest.filter` JSON (the same value fed to `render_df_filter_safe`) into `resolve_file_list`; leave the `ScanSpec.filter` DataFusion-string path unchanged.
3. **Unit tests — translator soundness** (in `iceberg_predicate.rs` `#[cfg(test)]`)
   - [ ] 3.1 Leaf translations: `=`, `<`, `<=`, `IN`, `IS NULL`, `IS NOT NULL`, `BETWEEN` each produce the expected `Predicate`; column-vs-literal operand order handled.
   - [ ] 3.2 AND with one untranslatable child returns only the translatable conjunct. [expert]
   - [ ] 3.3 OR with one untranslatable child returns `None`. [expert]
   - [ ] 3.4 NOT of an untranslatable child returns `None`; NOT of a translatable child negates it. [expert]
   - [ ] 3.5 Unknown column / type mismatch → leaf returns `None` (no panic, no fabricated predicate).
4. **Unit tests — pushdown wiring** (in `pushdown.rs` `#[cfg(test)]`)
   - [ ] 4.1 A `LIKE`-only filter still yields a valid `ScanSpec.filter` and a `None` Iceberg predicate (mirror `pushdown_translates_or_omits_predicate`).
5. **Integration / E2E**
   - [ ] 5.1 Add a partitioned-table seed helper variant in `crates/lakehouse-engine/tests/common/seed.rs` (non-empty `UnboundPartitionSpec` + matching `PartitionKey` per data file).
   - [ ] 5.2 Add E2E `e2e_partition_filter_prunes_and_returns_correct_rows` in `tests/e2e_scan_test.rs`: seed partitioned table, run a partition-column-filtered query, assert rows match the seeded subset and equal the unpruned-equivalent result. [expert]
   - [ ] 5.3 Where observable, assert the resolved/planned file count under the predicate is less than the snapshot file count (e.g. an adapter-level integration test calling `resolve_file_list` with vs. without the filter against the seeded MinIO catalog).

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2, 1.3, 1.4 (translator) |
| Group B | 5.1 (partitioned seed helper — independent of translator) |
| Group C | 2.1, 2.2, 2.3 (wiring — depends on Group A) |
| Group D | 3.x, 4.x (unit tests — depend on Group A / Group C) |
| Group E | 5.2, 5.3 (E2E — depend on Group B + Group C) |

Sequential dependencies:
- Group A → Group C → Group D
- Group A + Group B → Group E

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| (none) | — | Purely additive: the unfiltered `plan_files_from_table` call is parameterised, not removed; no code becomes obsolete |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Filter predicate is pushed into the scan spec (CHANGED) | Integration | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `pushdown_carries_filter_and_iceberg_prune` |
| Equality on a partition column prunes data files | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_partition_filter_prunes_and_returns_correct_rows` |
| Range predicate prunes files via per-file min/max bounds | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_range_filter_prunes_by_file_bounds` |
| Untranslatable conjunct disables pruning for that conjunct only | Unit | `crates/lakehouse-engine/src/adapter/iceberg_predicate.rs` | `and_with_untranslatable_child_keeps_translatable_conjunct` |
| An untranslatable branch of an OR disables pruning entirely | Unit | `crates/lakehouse-engine/src/adapter/iceberg_predicate.rs` | `or_with_untranslatable_child_returns_none` |
| End-to-end filtered query over a partitioned table returns correct rows with file pruning | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_partition_filter_prunes_and_returns_correct_rows` |

Supporting unit tests (translator soundness, not 1:1 scenario but required by the invariant): `not_of_untranslatable_returns_none`, `leaf_equal_translates`, `between_desugars_to_range`, `unknown_column_returns_none` in `iceberg_predicate.rs`.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/pushdown-planning | `make test-e2e` then in a SQL session: `EXPLAIN VIRTUAL SELECT * FROM <vs>.<partitioned_table> WHERE part_col = '<v>';` | The scan-driving SQL lists fewer data-file paths than the full snapshot; the file list is the matching-partition subset |
| vs-adapter/pushdown-planning | `SELECT count(*) FROM <vs>.<table> WHERE col = '<v>'` and the same with a `LIKE`-only filter | Both return correct counts; correctness identical with and without prunable predicate |
| packaging/e2e-harness | `make test-e2e` | `e2e_partition_filter_prunes_and_returns_correct_rows` passes; fails (not skips) if Docker/MinIO down |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures (fails, not skips, without Docker) |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
