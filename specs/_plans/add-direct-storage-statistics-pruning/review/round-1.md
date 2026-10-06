# Plan Review Findings: add-direct-storage-statistics-pruning (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 11 (Blockers: 4, Advisory: 7)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Premortem

Six months from now this plan failed. Three ways it could have happened:

1. A customer filters a direct-storage `DECIMAL(10,2)` column with `WHERE AMOUNT > 1234567.89`. The scan returns the row holding `1234567.89` when that file also holds other matching rows. It drops the same row when the file holds nothing else, because statistics pruning removes the file. Results depend on file layout. Cause: the footer scope compares bounds against the exact decimal literal, but the scan compares against the literal after DataFusion parses it as `f64`. Routed to the first Feasibility blocker.
2. A pandas-written directory stores nanosecond timestamps. On Exasol 2025.x the column is declared `TIMESTAMP(9)`, and `WHERE TS > TIMESTAMP '... .000000500'` changes its row set after the release. Cause: the scan truncates the literal to microseconds, and the footer scope keeps all nine digits. Routed to the first Feasibility blocker.
3. The benchmark shows no gain on float filters such as `PRICE <= 9.99` or `D > 0.1`, which DataFusion, Delta planning, and Iceberg planning all prune. Cause: the footer scope accepts only literals that are exactly representable in binary. The reason given for that rule is contradicted by the code. Routed to the second Feasibility blocker.

## Intent Fidelity

No objection. Axis checked: the decision log carries the interview Q&A verbatim. The plan keeps #412's scope and the user's parity yardstick. It keeps the fixed operator set (`<`, `<=`, `>`, `>=`, `=`, plus `IN` and `BETWEEN` built from them) and the conservative handling of `<>` and odd-`NOT` forms. It keeps the one (#TBD) column-order and null-count exception and the #393 limitation note in the docs. References to the code at HEAD 908af7e check out with Serena: `CatalogParquetFormatReader`, `ParquetFileSource::plan`, `partition_column_types`, `list_location_files`, `ParquetFile.footer: Option<Arc<ParquetMetaData>>`, and the cited tests. The artifacts follow the current templates: the plan sections, the decision-log fields including `Architecture:`, delta anchors with the heading inside the marker, and a Background-only seam delta that carries its unmarked scenario verbatim. `speq plan validate` passes. The float-literal narrowing in the second Feasibility blocker also falls short of the user's "like Datafusion already does". It is filed under Feasibility because the code settles it.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: plan.md § Implementation Tasks 2.3; decision-log.md [3] Rationale ("Pruning is sound only when it compares in the type the scan compares in"); `vs-adapter/direct-storage-statistics-pruning/spec.md` § Scenario "Row-group statistics evaluate the filter under three-valued logic" (last AND step); plan.md § Impact ("Results do not change for any non-NaN value")
- Issue: The plan checks that the column's logical type equals its folded type. It never checks that the footer scope's literal equals the literal the scan compares against. For two comparable types they differ, so pruning drops rows the unpruned scan returns. That breaks the step "the returned rows SHALL be ... equal to the rows of the same query without statistics pruning".
  - Decimal columns. `vs-expression` renders `literal_exactnumeric` as a bare numeral (`crates/vs-expression/src/lib.rs:408-413`, `json_scalar_to_string`), and the scan runs it as SQL text (`scan/raw_scan.rs:372`). DataFusion 54.1 `parse_sql_number` parses every numeral that is not an `i64` or `u64` as `f64` (`datafusion-sql-54.1.0/src/expr/value.rs:85-101`). `parse_float_as_decimal` defaults to false, and this repo never sets it. `decimal_coercion` then compares `Decimal128(p,s)` with `Float64` in `Decimal128` at scale `max(s,15)` (`datafusion-expr-common-54.1.0/src/type_coercion/binary.rs:1062-1064, 1227-1239`). arrow-cast converts the float with `(10^scale * v).round()` in `f64` arithmetic (`arrow-cast-58.3.0/src/cast/decimal.rs:775-780`). Result: `AMOUNT > 1234567.89` on `DECIMAL(10,2)` compares against `1234567.8899999997952`. The scan returns the row holding `1234567.89`, while `literal_under`'s exact `Decimal128(123456789,10,2)` drops a file whose max is `1234567.89`. DataFusion's unwrap-cast rewrite only handles string literals for this shape, so it does not repair it (`datafusion-optimizer-54.1.0/src/simplify_expressions/unwrap_cast.rs:198-229`).
  - Nanosecond timestamps. `vs-expression` renders `literal_timestamp` as `arrow_cast('<v>', 'Timestamp(Microsecond, None)')` (`lib.rs:414-428`; accepted ADR `timestamp-literal-arrow-cast-microsecond`), which drops fraction digits 7 to 9. DataFusion coerces `Timestamp(ns)` against `Timestamp(us)` to microseconds (`timeunit_coercion`, `binary.rs:2048-2062`). Its unwrap-cast then compares the ns column against the truncated value (`casts.rs:82-99` includes `Timestamp`). The footer scope calls `exact_timestamp(value, Nanosecond)` and keeps all nine digits. `arrow_type_to_tag` round-trips `Timestamp(Nanosecond, None)`, so Task 4.1 counts such a column as comparable. Under the accepted ADR `timestamp-emit-at-source-precision-clamped-by-engine`, a ns column is declared `TIMESTAMP(9)` on Exasol 2025.x, so a nine-digit literal is reachable.
- Fix:
  1. In plan.md Task 2.3, add two rules under the `FooterStatistics` scope only. First, a literal against a decimal column converts only when it is an integer numeral that DataFusion parses as `i64` or `u64` (no `.`, no exponent). Any other literal against a decimal column leaves the node untranslatable. Second, a `literal_timestamp` converts only when its fraction has no non-zero digit past the sixth, which is the precision the scan's `arrow_cast` keeps. Any other timestamp literal leaves the node untranslatable.
  2. Add a THEN/AND step to the NEW spec's scenario "Row-group statistics evaluate the filter under three-valued logic" stating that a literal SHALL convert only to the value the scan itself compares against, and naming both rules.
  3. Add unit cases to Task 2.4: `AMOUNT > 1234567.89` over `DECIMAL(10,2)` bounds `[1234567.89, 1234567.89]` keeps the file, and a nine-digit timestamp literal against a `Timestamp(Nanosecond, None)` column keeps the file.
  4. Add one local DataFusion test that pins both scan-side behaviours, in the style of `tests/scan_parquet_pruning.rs`, so a DataFusion upgrade that changes either coercion fails a test.
  5. Add both cases to the keep-list of Task 6.1.
  6. Correct decision-log [3] Rationale to say that the literal must also equal the scan's, not only the column type.
- Escalation: MECHANICAL. The renderer, the DataFusion coercion, and the arrow cast are all in the repo and the locked registry sources. The fix conforms to the user's own yardstick ("never change today's returned rows") and needs no judgment call.

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: decision-log.md [6] Alternatives, second bullet ("nothing verifies how the scan coerces a decimal literal against a float column"); plan.md § Implementation Tasks 2.3 and 2.4 (`0.1` does not convert); `vs-adapter/direct-storage-statistics-pruning/spec.md` § Scenario "A float bound or literal is widened or rejected so it never drops a matching row" (third THEN/AND step)
- Issue: The code settles the premise behind rejecting round-to-nearest conversion. `vs-expression` puts the numeral into the scan's SQL unchanged. DataFusion parses it with `str::parse::<f64>()`, which rounds correctly (`value.rs:96`), and compares a `Float64` column against that `f64` with no further coercion. A `Float32` column is cast to `Float64`, and floats are excluded from unwrap-cast (`casts.rs:82-99`). So `str::parse::<f64>` on the same text reproduces the scan's literal exactly, and the exact-only rule only removes pruning. Under the plan as written, `D > 0.1`, `D < 2.3`, and `PRICE <= 9.99` never prune. The three layers the plan claims parity with all prune them: DataFusion row-group pruning uses the scan's `f64` literal, `delta_predicate.rs:156-158` uses `parse_f64`, and `iceberg_predicate.rs:44-47` uses `parse_f64`. The float scenario's THEN step claims "the rule the scan's row-group pruning, Delta planning, and Iceberg planning apply to float bounds", which is therefore inaccurate. The rule also falls short of the user's answer, "prune floats like Datafusion already does, and other places if any".
- Fix:
  1. In plan.md Task 2.3, under `FooterStatistics`, convert a `literal_exactnumeric` or a finite `literal_double` against a `Float64` column with `str::parse::<f64>` on the text `json_scalar_to_string` yields.
  2. For a `Float32` column, cast the row-group bounds to `Float64` (exact) and compare against the same `f64` literal, instead of narrowing the literal.
  3. Keep rejecting non-finite literals. Keep the partition scope's rejection of every float literal.
  4. Rewrite the spec step and decision-log [6] (Decision, Alternatives, Consequences) to match.
  5. Update `float_literals_convert_only_exactly_and_only_for_footer_statistics`: `0.1` converts under the footer scope and stays `Opaque` under the partition scope. Rename the test, since it is no longer "only exactly".
  6. Add an E2E probe to Task 5.2 with a non-dyadic literal, for example `D > 5.3` naming `f2.parquet` only.
- Escalation: MECHANICAL. The scan's literal path can be checked in code, and the user's recorded answer already fixes the yardstick. Conforming needs no new decision.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Impact; decision-log.md [11] Scope boundaries
- Issue: The first blocker's mechanism is a pre-existing scan correctness gap, separate from this plan. As the code reads, `AMOUNT = 1234567.89` on `DECIMAL(10,2)` matches no row in the scan, and nine-digit timestamp literals lose digits 7 to 9. The same gap breaks parity for Iceberg and Delta decimal pruning, whose translators convert decimals exactly, and for Glue Hive decimal and timestamp partition pruning in `CatalogParquetFormatReader`, which shipped in #455. CLAUDE.md requires such a claim to be verified live and forbids leaving a known gap silent.
- Fix: Add an open question to the decision log, (#TBD), describing the scan's `f64` decimal-literal path and its microsecond timestamp-literal truncation. Have Task 1 or Task 5 run one live probe (`AMOUNT = 1234567.89` over a fixture holding that value) to verify it. Leave the fix out of scope.

## Requirement Quality

#### [IMPLEMENTATION_LEAKAGE] BLOCKER
- Location: `vs-adapter/direct-storage-statistics-pruning/spec.md` § Background, bullet "`parquet` 58.3.0 (the locked version) shapes four gates" (sub-bullets 1, 2, and 4), and the bullet starting "The scan compares floats in IEEE 754 total order" (sentence "Issue #370 observed a computed NaN matching `< -1E300` ...")
- Issue: No scenario step depends on these lines.
  - Sub-bullet 1 states parquet-rs's silent fallback to deprecated `min`/`max` and the method `Statistics::is_min_max_deprecated()`.
  - Sub-bullet 2 states the crate's enum mapping (`ColumnOrder::UNDEFINED`, `SortOrder::UNDEFINED`, `ColumnOrder::UNKNOWN`).
  - Sub-bullet 4 states `StatisticsConverter`'s null-count default and `with_missing_null_counts_as_zero(false)`.
  - The #370 sentence records an earlier observation.

  The scenarios state these cases in Parquet-specification terms: deprecated `min`/`max` only, a missing null count counts as unknown, an INT96, INTERVAL, or unknown column order. The parquet.thrift quotes already in the Background ground those terms. The crate internals belong in decision-log [5], which already states the fallback.
- Fix:
  1. Delete sub-bullets 1 and 4.
  2. Replace sub-bullet 2 with the parquet.thrift text that leaves the INT96 and INTERVAL orders undefined, or delete it if the existing `ColumnOrder` quote covers it.
  3. Delete the #370 sentence.
  4. Move any crate detail the implementer needs into decision-log [5] or Task 3.2.
- Escalation: MECHANICAL. Every line can be checked against the delta file's own scenarios.

#### [COMPLETENESS_GAP] BLOCKER
- Location: `datafusion-scan/scan-execution-memory-and-credentials/spec.md` § Scenario "Scan enables Parquet row-group and page pruning so the reader skips non-matching data", last AND step; decision-log.md [9]
- Issue: The exception is limited to "a footer without `column_orders` or without `null_count`". Its own because-clause says pruning "ignore[s] `ColumnOrder`", and DataFusion 54.1 performs no deprecation check (`row_group_filter.rs:253`; `page_filter.rs` reads no column order). Two more known deviations therefore fall outside the stated scope.
  - A column whose `ColumnOrder` is a union member the reader does not know.
  - A statistic that carries only the deprecated signed-comparison `min`/`max`. parquet 58.3.0 falls back to those fields (`old_format`, `file/metadata/thrift/mod.rs:210-222`), and DataFusion uses them as type-ordered bounds even when `column_orders` is present.

  This plan's own NEW spec lists both cases as unusable under the Parquet specification. CLAUDE.md requires each known deviation to be an accurately scoped exception and never a silent gap.
- Fix: Rewrite the exception's scope clause as: "for a footer without `column_orders`, a column whose `ColumnOrder` is a union member the reader does not know, a statistic that carries only the deprecated `min`/`max` fields, or a statistic without `null_count`". Cite the parquet 58.3.0 fallback beside the existing DataFusion citation. Mirror the wider scope in decision-log [9]. Keep (#TBD).
- Escalation: MECHANICAL. The registry sources and the plan's own gate list settle it.

#### [COMPLETENESS_GAP] ADVISORY
- Location: `vs-adapter/direct-storage-table-planning/spec.md` § Scenario "The kept files' footers are read at plan time and the resulting cost is stated" (step citing "(#419)"); plan.md Task 6.1 ("Keep the #419 sentence")
- Issue: The CHANGED scenario re-records #419 as "its tracking issue" for an open exception. #419 was closed on 2026-09-22 with the comment "this should not have been split into a separate tracked-exception issue". notes/planning.md records the closure, but the decision log does not mention it.
- Fix: Ask the human whether the unbounded-file-count exception keeps citing the closed #419 or changes to (#TBD). Record the answer in decision-log.md, and keep the spec step and the Task 6.1 docs sentence consistent with it.

## Task Breakdown

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md Task 4.2; § Verification, Scenario Coverage rows for "A footer whose row-group bounds exclude the filter drops the file" and "Statistics pruning composes with every consumer of the file list"
- Issue: No planned test would fail if the reader read a footer or issued an object-store request of its own. Two steps require it not to: "MUST NOT read a footer or issue any object-storage request of its own" and "under `MERGE_SCHEMA` FALSE ... MUST NOT read a footer itself". The existing suite proves no-read by storing `NOT_PARQUET` bytes (`a_partition_filter_prunes_files_before_their_footers_are_read`).
- Fix: In `statistics_pruning_keeps_unsampled_files_and_reaches_zero_files`, store every unsampled file as `NOT_PARQUET` under `MERGE_SCHEMA` FALSE and assert that resolution succeeds and keeps them. Name the step it covers in the Scenario Coverage row.

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md Tasks 1.1 and 5.1 (extensions to `raw_parquet_fixtures_are_physically_the_types_they_declare`)
- Issue: `e2e-harness/direct-storage-e2e` requires the fixture-shape test to assert "each written object's PHYSICAL column encoding". The extensions assert row-group counts, bounds, NaN sign, and absent statistics, but no physical type for `nan_probe/probe.parquet`, `stats/f1.parquet`, or `stats/f2.parquet`.
- Fix: Add physical-type assertions to Tasks 1.1 and 5.1 (`ID` INT64; `D`, `N`, `C` DOUBLE; `QUIET` INT64) for each new object.

## Design Depth

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: plan.md Tasks 2.3, 3.2, 4.1; decision-log.md [3] and [8]
- Issue: One decision, "which comparisons match what the scan compares", is split across three modules. The reader's comparable-column rule (Task 4.1) owns column-type equality. The footer view's gates (Task 3.2) own the time-zone and FLOAT16 checks; decision [8] admits FLOAT16 and non-UTC zones repeat the reader's check. The translator's `FooterStatistics` scope (Task 2.3) owns literal conversion. The first two Feasibility blockers add more rules of this kind, and each would have to choose a home.
- Fix: Give scan-comparison eligibility one owner. Either the `FooterStatistics` translator scope, given the scan-comparable columns, decides both column and literal eligibility, or one function in `footer_statistics.rs` does. Keep the footer view's gates limited to Parquet-validity rules (column order, deprecation, NaN bounds, `min > max`, nesting). Note the owner in decision [2]'s Quick Diagnostic.

#### [ADR_OVERPROMOTION] ADVISORY
- Location: decision-log.md [1]
- Issue: The entry passes the promotion gate. Its Rationale names rule-2 criterion 4 and states the `speq decision-log show` result, and its Decision holds no signature or path. Two of its lines would carry rule-violating content into the ADR. The Alternatives bullet pins versions ("`datafusion-datasource-parquet` 54.1"), and rule 6 says an ADR never pins a version. The second Consequences bullet ("keeps its name `PartitionPredicate` ... A rename would touch the second caller") is a naming choice, which rule 3 keeps out of ADRs.
- Fix: Drop the version numbers from [1]'s Alternatives. Move the naming bullet into decision [2] or an entry marked `Promotes to ADR: no`.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: `vs-adapter/direct-storage-statistics-pruning/spec.md` § Scenario "A float column prunes ordering and equality comparisons on its bounds alone", first THEN step
- Issue: "the rule the scan's row-group pruning, Delta planning, and Iceberg planning apply to float bounds" reads as an identical rule. Decision [4] states that the plan is stricter than DataFusion, which prunes `<>` and simplified `NOT` forms. After the second Feasibility blocker is fixed, that is the only remaining difference, and the step should name it.
- Fix: Reword the step to "the bound rule those layers apply, restricted to comparisons under an even number of `NOT`s and excluding `<>`".
