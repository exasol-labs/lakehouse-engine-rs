# Plan Review Findings: add-direct-storage-statistics-pruning (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 9 (Blockers: 1, Advisory: 8)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Round-1 Blocker Recheck
- Resolved: [UNSTATED_ASSUMPTION] Footer literals must equal the literal the scan compares against. Task 2.3 now converts a decimal literal only as an integer numeral that parses as `i64` or `u64`, and a timestamp literal only with no non-zero fraction digit past the sixth. The NEW spec's three-valued-logic scenario carries the matching step ("a literal SHALL convert only to the value the scan itself compares against"). Task 2.4 adds `footer_literals_convert_only_to_the_scans_value`, Task 2.5 adds the local DataFusion pin `scan_compares_pushed_literals_as_the_footer_scope_assumes`, Task 6.1 lists both keep cases, and decision [3] Rationale states the literal condition. The rule matches the code: `exact_timestamp` and `exact_numeral` in `partition_predicate.rs` stay the partition-scope rules, and DataFusion 54.1 `timeunit_coercion` coerces a nanosecond column against a microsecond literal to microseconds.
- Resolved: [UNSTATED_ASSUMPTION] Float literals convert to the value the scan parses. Task 2.3 converts DOUBLE literals with `str::parse::<f64>` and widens FLOAT bounds to DOUBLE. It adds a justified FLOAT integer exception (DataFusion `numerical_coercion` compares a FLOAT column against an `Int64` literal in FLOAT). Decision [6], the float scenario steps, the renamed unit test, and E2E case 34 (`D > 4.3`) follow. The partition scope still rejects float literals.
- Resolved: [IMPLEMENTATION_LEAKAGE] The new spec's Background stated parquet-rs internals. The deprecated-field fallback, the `ColumnOrder`/`SortOrder` mapping, the `StatisticsConverter` null-count default, and the #370 sentence are gone. The crate detail sits in decision [5] and Task 3.2. The remaining crate bullet (`nan_count`, NaN-free writer bounds) backs the float scenarios' GIVEN and THEN steps.
- Resolved: [COMPLETENESS_GAP] The scan pruning exception omitted two known deviations. The `DELTA:CHANGED` scan scenario now names a footer without `column_orders`, an unknown `ColumnOrder` union member, deprecated-only `min`/`max`, and a missing `null_count`, citing both DataFusion 54.1 and `parquet` 58.3.0 `old_format`. Decision [9] mirrors the scope.

## Premortem

Six months from now this plan failed. Three ways it could have happened:

1. A Spark job writes a directory with `TIMESTAMP_MILLIS` columns. `WHERE TS = TIMESTAMP '2025-01-01 00:00:09.000500'` returns the row holding `00:00:09.000`, because DataFusion coerces a millisecond column and a microsecond literal to milliseconds. The issue a human opens from Task 1.5 names only DECIMAL, nanosecond, and FLOAT literals, so the fix misses millisecond and second columns. No test ever read a millisecond column. Routed to the Requirement Quality blocker.
2. A filter carries one conjunct `vs-expression` cannot render, so the adapter self-applies the whole filter in Exasol over emitted values. Direct storage declares every timestamp as bare `TIMESTAMP` (milliseconds). `TS_US <= TIMESTAMP '2024-01-01 00:00:01.000'` returns a stored `00:00:01.000500` row through the emitted truncation, while the footer drops its file. Routed to the first Feasibility advisory.
3. Exasol pushes every literal against a DOUBLE column as `literal_double`, and every literal against a `DECIMAL(10,2)` column at the column's scale (`80.00`). Cases 25, 27, and 37 to 41 stop exercising their rules. Task 5.1's file assertions fail for the decimal cases, and Task 1.5 has no branch for a shape that no clause can reach. Routed to the second Feasibility advisory.

## Intent Fidelity

No objection. Axis checked: the user's follow-up asks for "all supported types for prunning / filtering, with edge cases, with as few e2e and table seeds as possible". The plan answers with one type table, `PRUNE_TYPES`, plus the separate `NAN_PROBE`, which is the interview answer "One table + NaN probe". It uses one table-driven test plus the stored-NaN test and the join test. The interview answer "Verify live, then decide" is carried out by Task 1.5's live run, the `(#TBD)` pin, and the absence of any scan change (decision [14]). The parity yardstick and the fixed float operator set of round 1 are unchanged. The missing millisecond and second timestamp columns are filed under Requirement Quality. They are an oversight that two added columns fix, not a reframing of the ask.

## Feasibility

Checked without objection: `arrow_type_to_tag` and `arrow_type_from_tag` (`types/mapping.rs`) round-trip every type the case table prunes, so Task 4.1's comparable-column rule admits each one. `DATE64`, `Decimal256`, FLOAT16, and the list column tag as `utf8` and declare `VARCHAR(2000000)` (`compatible_exasol_type`). A UTC-zoned timestamp round-trips as `timestamptz_us`, so case 54 really exercises the time-zone gate. `datafusion-common` 54.1 `ScalarValue::partial_cmp` compares floats with `total_cmp`, so case 64's `-0.0` minimum keeps `grp=a` as planned. `classify_leaf` admits FLOAT16. The existing `merge_schema_false_declares_the_narrow_sampled_type_and_refuses_a_wider_file_column` shows that `SELECT ID` succeeds over a `MERGE_SCHEMA` FALSE table whose unsampled file stores a wider column, so cases 70 and 71 survive the `W` column. Every expected ID in the case table follows from the fixture table.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: decision-log.md [3] Rationale ("Pruning is sound only when it compares in the type the scan compares in, and against the literal value the scan compares against"); plan.md § Impact ("Results do not change for any non-NaN value"); `vs-adapter/direct-storage-statistics-pruning/spec.md` § Scenario "A footer whose row-group bounds exclude the filter drops the file" (step "because the full filter still applies to the scanned rows")
- Issue: Every soundness rule assumes that the scan evaluates the filter. On the decline path of `vs-adapter/pushdown-declined-filter-self-apply`, the scan carries no filter. Exasol then applies the original filter to the emitted values, while `resolve_scan` still receives the full filter tree and prunes from it. Two direct-storage cases then differ from the scan's comparison.
  - Timestamps. Direct storage declares every timestamp column as bare `TIMESTAMP` (`ColumnSourceType::Parquet` maps through `arrow_to_exasol_type`), which is millisecond precision. A stored `00:00:01.000500` is emitted at millisecond precision. Exasol's `<=` and `=` against a millisecond literal then select it, while the footer's bound `00:00:01.000500` excludes the literal and drops the file.
  - Empty strings. Exasol reads an emitted empty string as NULL, so a self-applied `S IS NULL` selects it. The footer's present null count of zero drops the file.
- Fix: Add one sentence to decision [3] Rationale stating that the footer scope matches the scan's comparison, and that on the self-apply path Exasol compares the emitted values instead. Then choose one of two options. (a) Pass the decline outcome that `handle_pushdown` already computes into the direct-storage reader, and skip the statistics pass when the filter is self-applied. (b) Add an Open Question, `(#TBD)`, naming sub-millisecond timestamps under `<=`, `=`, `IN`, and `BETWEEN`, and empty strings under `IS NULL`, on the self-apply path, and qualify the Impact sentence to the scan path.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md Task 1.5, bullets "Literal cases 67 to 69" and "Pushed shapes"; decision-log.md [10] Rationale
- Issue: Several case rules depend on the literal kind and text that Exasol pushes, and nothing in the repo records either. The decimal rule converts only an integer numeral, so cases 37 to 41 prune only if Exasol pushes `AMOUNT <= 80` as `"80"` and not `"80.00"`. Cases 25, 27, and 69 exercise integer-literal rules only if Exasol pushes `literal_exactnumeric` against a DOUBLE-declared column. The "Pushed shapes" bullet says to replace a clause with one Exasol pushes in the intended shape. Only case 68 and the `B` cases have a branch for the case where no clause can reach that shape. The Files column is asserted only in Task 5.1, after the code exists, so a decimal-scale push surfaces there with no instruction.
- Fix: In Task 1.5, record in the "Measured scan literal comparisons" entry the pushed literal kind and text of cases 25, 27, 28, 37 to 41, and 69. Add one general branch: if no clause on a column type reaches a rule's intended literal shape, keep the case with the shape Exasol pushes, set its Files to what the Task 2.3 rules give for that shape, mark the rule unit-only in § Scenario Coverage, and record the shape in decision-log.md. If Exasol pushes a decimal literal at the column's scale, stop and ask the user whether Task 2.3 should accept a numeral whose `f64` parse rounds at scale 15 to the exact decimal, because the integer-only rule would then never prune a `DECIMAL(p,s>0)` column.

## Requirement Quality

#### [COMPLETENESS_GAP] BLOCKER
- Location: plan.md Task 1.3 (fixture table), Task 1.4 (case table), Task 1.5 ("Literal cases"), Task 1.6, Task 2.5, Task 6.1; `vs-adapter/direct-storage-statistics-pruning/spec.md` § Scenario "Integer, float, decimal, string, boolean, date, and timestamp columns prune on their footer bounds" (GIVEN) and § Scenario "A literal the scan compares inexactly keeps every file, and the scan's rows for it are pinned live" (exception step); decision-log.md [12] Decision ("one column per type that prunes")
- Issue: The type matrix omits two timestamp units that the reader admits and the footer scope prunes. The user asked to "cover all supported types for prunning / filtering, with edge cases". The test name `footer_statistics_prune_every_supported_type` and decision [12] claim the same coverage.
  - Direct storage admits `Timestamp(Millisecond, None)` and `Timestamp(Second, None)`. `arrow_type_to_tag` maps them to `timestamp_ms` and `timestamp_s`, `arrow_type_from_tag` maps them back, and both declare `TIMESTAMP`. Task 4.1's rule therefore counts both as comparable, and `exact_timestamp` converts their literals. `parquet` 58.3.0 writes `Timestamp(Second)` as INT64 with no logical annotation (`arrow/schema/mod.rs`), so its bounds depend on the embedded Arrow schema alone. No unit, integration, or E2E test of the plan reads either unit.
  - The exception step is scoped too narrowly for these units. It states that "the scan compares ... a timestamp column against a timestamp literal cut to microseconds". DataFusion 54.1 `timeunit_coercion` (`datafusion-expr-common` `type_coercion/binary.rs`) coerces a millisecond or second column and a microsecond literal to the coarser unit. The scan therefore compares such a column against the literal cut to milliseconds or seconds. `TS_MS = TIMESTAMP '2025-01-01 00:00:09.000500'` would return the row holding `00:00:09.000`, which an exact comparison does not select. This is a fourth inexact literal class. The footer scope already refuses it through `exact_timestamp(value, Millisecond)`, but the spec's `(#TBD)` exception does not name it and Task 1.5 does not measure it.
- Fix:
  1. In Task 1.3, add `TS_MS` (`Timestamp(Millisecond, None)`) and `TS_S` (`Timestamp(Second, None)`), each "as `TS_US`". Assert in the fixture-shape test that `TS_MS` is INT64 annotated `TIMESTAMP(MILLIS)`, and that `TS_S` is INT64 with no logical annotation.
  2. In Task 1.4, add one drop case per column, for example `TS_MS >= TIMESTAMP '2025-01-01 00:00:00'` (b, 9 to 16) and `TS_S < TIMESTAMP '2025-01-01 00:00:00'` (a, 1 to 8). Add one measured literal case, `TS_MS = TIMESTAMP '2025-01-01 00:00:09.000500'` (a, b; measured in Task 1.5; an exact comparison selects none).
  3. In Task 1.5, add the new literal case to "Literal cases" and record its pushed literal. Add a fallback, as for FLOAT16: if the scan cannot read a unit, remove that column and keep it unit-only.
  4. In Task 2.5, add a `Timestamp(Millisecond, None)` column to the local file, and assert what `TS_MS = TIMESTAMP '... .000500'` returns.
  5. In the spec, add both units to the GIVEN of "Integer, float, decimal, string, boolean, date, and timestamp columns prune on their footer bounds". Rewrite the exception step of "A literal the scan compares inexactly ..." so that the timestamp clause reads "a timestamp column against a timestamp literal cut to microseconds, or to the column's own unit when it is coarser". Mirror the change in Task 1.6, plan.md § Open Questions item 1, and the Task 6.1 keep list ("a timestamp literal finer than the column's unit or finer than microseconds").
  6. Update § Scenario Coverage case ranges and the § Impact query count.
- Escalation: MECHANICAL. The admitted types come from `types/mapping.rs`, and the coercion comes from the locked registry source. The user's direction already requires the coverage, and the interview answer "Verify live, then decide" already covers the new literal class.

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md Task 1.4 (keep-gate cases 52 to 57); `vs-adapter/direct-storage-statistics-pruning/spec.md` § Background (bullet "The scan compares a column in the type its logical field declares ...")
- Issue: Two admitted types reach the keep path through a rule that no E2E case exercises.
  - INT96. The fold reads an INT96 column as `Timestamp(Nanosecond, None)`, which tags as `timestamp_ns` and is therefore comparable under Task 4.1. Only the sort-order gate keeps it from pruning. Task 3.3 tests that gate on hand-built metadata only, never on a footer that parquet-rs wrote and read back. INT96 is Spark's legacy timestamp encoding. The existing `annotated_types/file1.parquet` fixture already holds `c_int96`, so a keep case costs no new seed.
  - `Decimal128` with precision 37 or 38. `logical_schema` tags it `decimal128(p,s)`, while `compatible_exasol_type` declares it `VARCHAR(2000000)`. The Background bullet "A column the engine declares `VARCHAR(2000000)` ... carries the string type in its logical field" is therefore false for it. The plan stays sound only because Exasol pushes a string literal, which the footer scope does not convert.
- Fix: Add one keep case on `DIRECT_LAKEHOUSE.ANNOTATED_TYPES`, a `C_INT96` comparison whose literal lies outside the stored values, whose Files assertion names `annotated_types/file1.parquet`. First extend the fixture-shape test to assert that `c_int96` carries statistics. If it carries none, record the gate as unit-only. Narrow the Background bullet to the types whose logical tag is `utf8`, and name `Decimal128` above precision 36 as the exception that stays sound because its literal does not convert.

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md Task 1.5, bullet "Pushed shapes" ("If no clause on `B` reaches the scan as a comparison, cases 46 and 47 expect both files"); Task 2.4; § Scenario Coverage row "Integer, float, decimal, string, boolean, date, and timestamp columns prune on their footer bounds"
- Issue: If Exasol pushes `B = TRUE` as a bare column reference, no test of the plan covers boolean bounds. Task 2.4's unit tests name no boolean case, and Task 3.3 has none either. The scenario still lists BOOLEAN as a pruning type.
- Fix: Add a boolean case to `range_facts_evaluate_under_three_valued_logic`: `B = TRUE` over bounds `[false, false]` drops, and `B <> TRUE` over `[true, true]` drops. In the Task 1.5 fallback, name that unit test as BOOLEAN's coverage row. Optionally translate a bare boolean column predicate as `= TRUE`, since the user asked for every supported type.

## Task Breakdown

No objection. Axis checked: every NEW and CHANGED scenario maps to a named test in § Scenario Coverage, and each scenario that `footer_statistics_prune_every_supported_type` covers is counted in its twelve `/// Scenario:` lines. Group A is one cluster around one new spec delta and one module set. Task 1's rows-only E2E run precedes Task 2, and Task 5.1 adds the file assertions without changing expected IDs. Task 4.2's change to `plan_reads_selected_footers_and_lists_every_file` is necessary: its one-NULL-row files under `ID = 1` would otherwise drop under `FoldEveryFile` and break its merge-mode assertion. `a_numeric_literal_prunes_no_direct_storage_file` stays green, because a numeric literal on the `utf8` partition key does not convert.

## Design Depth

No objection on structure. Axis checked: the `ColumnFacts` trait keeps Parquet types out of `partition_predicate.rs`, and `footer_statistics.rs` owns the gates. The `FooterStatistics` literal scope mirrors `vs-expression`'s rendering and DataFusion's coercion, and Task 2.5's local test is the guard that enforces agreement. No decision contradicts `delta-predicate-third-walker-defer-shared-ir`, `nested-pruning-requires-positive-proof`, `unity-parquet-schema-from-catalog-not-footer`, `timestamp-literal-arrow-cast-microsecond`, or `timestamp-emit-at-source-precision-clamped-by-engine`. `specs/architecture.md` is absent, so no architecture delta is owed.

#### [ADR_OVERPROMOTION] ADVISORY
- Location: decision-log.md [1] Alternatives (first bullet) and Consequences (second bullet)
- Issue: The round-1 advisory still applies. The entry passes the promotion gate: rule-2 criterion 4 is named, and the `speq decision-log show` search result is stated. Its Alternatives still pin a version ("`datafusion-datasource-parquet` 54.1"), which rule 6 keeps out of an ADR. Its Consequences still carry the naming choice ("keeps its name `PartitionPredicate` ... A rename would touch the second caller"), which rule 3 keeps out of an ADR.
- Fix: Drop the version number from [1]'s Alternatives. Move the naming bullet to decision [2] or to a new entry marked `Promotes to ADR: no`.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: decision-log.md [13] Decision ("`footer_statistics_prune_every_supported_type` replaces `footer_statistics_prune_the_resolved_file_list` and `unusable_statistics_keep_every_file`")
- Issue: Neither replaced test exists at HEAD. A search of `crates/` finds no match. The sentence narrates an earlier plan revision (writing-guardrails rule 8), and an implementer would look for tests to delete.
- Fix: Rewrite the sentence as a current fact: "`footer_statistics_prune_every_supported_type` is the one E2E test of statistics pruning on `PRUNE_TYPES`."

#### [PROSE_UNCLEAR] ADVISORY
- Location: plan.md § Implementation Tasks, Task 1 introduction ("The rows it observes become the expected rows of every case, and no later task changes them")
- Issue: Task 1.5 fixes the expected IDs from the fixture table and stops on any difference ("Any other difference ... stop and report it to the user"). Only cases 67 to 69 take their expected rows from the live run. The introduction says the opposite for every case.
- Fix: Rewrite the sentence as: "The planned IDs follow from the fixture table. Task 1.5 confirms them against the scan before any pruning code exists, and pins the measured rows of the literal cases. No later task changes them."

#### [PROSE_UNCLEAR] ADVISORY
- Location: `vs-adapter/direct-storage-statistics-pruning/spec.md` § Scenario "A float column prunes ordering and equality comparisons on its bounds alone", first THEN step ("the rule the scan's row-group pruning, Delta planning, and Iceberg planning apply to float bounds")
- Issue: The round-1 advisory still applies. The step reads as an identical rule. Decision [4] states that this plan is stricter, because DataFusion also prunes `<>` and simplified `NOT` forms.
- Fix: Reword the step to "the bound rule those layers apply, restricted to comparisons under an even number of `NOT`s and excluding `<>`".
