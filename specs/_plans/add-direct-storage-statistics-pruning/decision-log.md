# Decision Log: add-direct-storage-statistics-pruning

## Interview

**Q:** How should footer-statistics pruning treat FLOAT/DOUBLE columns, given stored-NaN comparison semantics are unspecified (#393) and Parquet min/max exclude NaN? Options: "Unusable, cite #393 (Recommended)" or "Prune <,<=,>,>=,= on floats".
**A:** Prune <,<=,>,>=,= on floats.

**Q:** Delta planning (delta-kernel skipping) and the DataFusion scan-side row-group pruning both trust float min/max and ignore ColumnOrder. How should this plan record that? Options: "One (#TBD) exception, cite #393", "Scan-side only", or "Don't record".
**A:** One (#TBD) exception, cite #393. The float NaN part is superseded by the follow-up answer below. The column-order and null-count part stands as (#TBD).

**Orchestrator constraint (risk flag):** Float `>`/`>=` pruning is enabled only if a live measurement shows that a stored NaN row is not returned for it. Encode the decision in one place, so flipping it is a one-line change, and record the measurement as a decision-log entry. (Superseded by the follow-up answer below.)

**Q (plan review round 1, relayed by the orchestrator):** Which yardstick governs float pruning? Options: (A) row-level, "never drop a file a total-order NaN row would match", expected to leave only `=` and `IN`. (B) parity, "never change today's returned rows", the rule DataFusion's scan-side row-group pruning, Delta planning, and Iceberg planning already apply. (C) floats unusable.
**A:** "prune floats like Datafusion already does, and other places if any"

**Orchestrator summary of the same answer:** The user knows that NaN is a problem, #393 tracks it, and NaN is an edge case. Pick option B with the fixed operator set `<`, `<=`, `>`, `>=`, `=` (and `IN`/`BETWEEN` built from them). Keep `<>` and `NOT`-wrapped forms conservative. Add a limitation note to the user documentation that links #393. Record the shared behavior as consistent with the other layers under #393, not as a (#TBD) deviation.

**User direction (revision after plan review round 1):** "does the plan include extensive e2e coverage? we should try to cover all supported types for prunning / filtering, with edge cases, with as few e2e and table seeds as possible"

**Q:** The new E2E rows check would hit the suspected scan bug: `AMOUNT = 1234567.89` on a DECIMAL column probably returns no row today, because the scan parses the literal as a float (and 9-digit timestamp literals lose digits 7-9). How should the plan handle it? Options: "Verify live, then decide" (plan's first task reproduces it on Docker Exasol per CLAUDE.md; if real, a human opens an issue; E2E pins today's behavior with that issue number and pruning keeps skipping such literals; no scan change in this plan) / "Fix it in this plan" / "Leave out of E2E".
**A:** Verify live, then decide.

**Q:** How aggressive should seed consolidation be? Options: "One table, NaN inside" / "One table + NaN probe" (PRUNE_TYPES replaces STATS; NAN_PROBE stays separate so the stored-NaN characterization is isolated from the type matrix).
**A:** One table + NaN probe.

## Design Decisions

### [1] Plan-time Parquet statistics pruning uses the engine's partition predicate, not DataFusion's pruning predicate

- **Decision:** Direct-storage statistics pruning evaluates the pushed filter with the same predicate that prunes partition values. A partition value and a row group's footer bounds are both value ranges on one three-valued evaluation path. DataFusion's pruning predicate is not used at plan time.
- **Alternatives:**
  - DataFusion's `PruningPredicate`. Rejected: its `RowGroupPruningStatistics` is private in `datafusion-datasource-parquet`, `prune_by_statistics` "currently ignores ColumnOrder" and sets `missing_null_counts_as_zero: true`, and the planner has no step that turns Exasol's filter JSON into a DataFusion `Expr`.
  - A separate statistics-only walker, the "no shared mechanism" reading of #412. Rejected: it duplicates the node set and the JSON translation, and two translations of one filter can disagree about which files a node keeps.
- **Rationale:** `/speq:adr-rules` rule-2 criterion 4: the rejected DataFusion route is the first one a later reader tries, and the reasons above decide against it. `speq decision-log show` holds no ADR on plan-time Parquet footer pruning. The `vs-adapter/delta-file-pruning` Background keeps one independent walker per output format and defers a shared IR. This decision adds no walker and builds no shared IR, so it conforms.
- **Consequences:**
  - The two inputs share one meaning for every node, so partition pruning and statistics pruning cannot disagree about a filter.
- **Architecture:** no change: specs/architecture.md absent
- **Promotes to ADR:** yes

### [2] The footer view is a separate module behind a value-source trait the predicate owns

- **Decision:** The predicate evaluates its node tree over any source that answers "which orderings can this column reach against this literal, and what is known about its nulls". The partition-value map is one source. A new module, `footer_statistics.rs`, is the other: it owns every Parquet type and rule, and the predicate names none.
- **Alternatives:** Put the statistics reading inside `partition_predicate.rs`. Rejected: the predicate would depend on `parquet` types, and a Parquet rule change would edit the filter-meaning module.
- **Rationale:** Quick Diagnostic of `/speq:design-philosophy` for the new module and the new trait:
  - One-sentence summary: `footer_statistics` answers what one row group's footer proves about a column. The trait is the predicate's question to any value source.
  - Easier than reimplementing: one call hides schema derivation, about a dozen gates, the widening cast, and the float rules.
  - Internal change forces an outside edit: no. A new gate or a `parquet` upgrade touches `footer_statistics.rs` only.
  - Doc comments explain why: each public item states its soundness reason (over-approximation, NaN, column order).
  - One owner per decision: filter meaning belongs to the predicate, Parquet validity to the footer view, and the float NaN rule to one predicate function.
  - Boundary visible: the trait is the boundary. The predicate never names Parquet, and the footer view never names a JSON node.
  - Tactical shortcut with follow-up: the shared stored-NaN exposure is recorded, not fixed. #393 tracks it.
  - Business logic depends inward: the predicate depends on no storage or framework type beyond the `ScalarValue` and `DataType` it already uses. The consumer (the predicate) defines the trait in its own vocabulary.
- **Consequences:**
  - The predicate keeps its name `PartitionPredicate`, and its doc comment states the wider scope. A rename would touch the second caller and every test for no behavior.
- **Promotes to ADR:** no

### [3] The statistics pass runs after the seam, compares in the scan's type, and reads only the filter the scan evaluates

- **Decision:** The reader keeps the pre-footer partition predicate as the seam's keep closure, and that predicate reads the full filter. After the seam returns, the reader builds a second predicate in the footer-statistics scope from the statistics filter only, which is the part of the pushed filter the scan evaluates. It retains the files the footer view keeps. On the single-table path the statistics filter is the whole filter when `classify_where_filter` hands the scan a filter, and absent when the adapter applies the filter itself. On a join leg it is the leg-local conjuncts the N-scan per-leg screen carries in that leg's scan. The second predicate types a column only when its logical field's type equals its folded Arrow type. Every other column is untranslatable in that scope.
- **Alternatives:**
  - Prune on the full filter even when the adapter applies it in its own outer `WHERE`, as Iceberg manifest pruning does (`vs-adapter/pushdown-declined-filter-self-apply`). Rejected: Exasol then compares the emitted values. A sub-millisecond timestamp is emitted at the declared `TIMESTAMP(3)` precision, so `TS_US <= TIMESTAMP '2024-01-01 00:00:01.000'` selects a stored `00:00:01.000500` that the footer bound excludes. An emitted empty string reads as NULL, so `S IS NULL` selects a row whose file a present null count of zero would drop.
  - Mark the self-apply exposure as an open question and qualify the Impact to the scan path. Rejected: `handle_pushdown` already decides the self-apply outcome before it resolves the file list, so passing it costs one parameter.
  - Type every column from the folded schema alone. Rejected: a folded `Date64` or `Decimal256` column carries the string type in its logical field, so the scan compares it as text while its bounds compare as a date or a decimal.
  - Type every column from its logical field alone. Rejected: a nested column's logical type is a string, so a string literal would convert and meet leaf statistics that describe other values.
  - Prune inside the seam. Rejected: the seam is format-neutral and its other callers need no statistics.
  - Always run the pass. Rejected: an unfiltered plan would drop zero-row-group files and stop being byte-identical.
- **Rationale:** Pruning is sound only when it compares in the type the scan compares in, and against the literal value the scan compares against. An equal column type is not enough. DataFusion 54.1 parses every numeral that is not a 64-bit integer as `f64`, so the scan compares a decimal column against that float's decimal rounding. `vs-expression` renders a timestamp literal at microsecond precision, so the scan drops fraction digits 7 to 9. The footer-statistics scope therefore converts only a literal whose value equals the scan's (Task 2.3), and a local DataFusion test pins both scan-side behaviors (Task 2.5). The footer scope matches the scan's comparison. On the self-apply path of `vs-adapter/pushdown-declined-filter-self-apply`, Exasol compares the emitted values instead, so the statistics pass reads only the part of the filter the scan evaluates (Task 4.1). The per-leg screen moves into one function that both the N-scan render path and the pre-resolution statistics filter call, so one function owns which conjuncts a leg's scan carries. The pipeline is:

  ```
  handle_pushdown: classify_where_filter → scan filter or self-applied
  plan_join:       per-leg screen         → carried leg-local conjuncts
          │ statistics_filter (None when the scan evaluates no part of the filter)
          ▼
  resolve_scan(filter)
    ├─ partition predicate (pre-footer, full filter) ──keep──▶ resolve_parquet_directory (unchanged)
    ├─ ParquetDirectory { files (footer: Option<Arc<ParquetMetaData>>), schema, .. }
    ├─ plannable_schema → logical fields
    └─ statistics_filter present:
         footer-scope predicate over the columns whose logical type equals the folded type
         files.retain(footer view keeps the file)
  ```

- **Consequences:**
  - A nested column stays untranslatable, which conforms to the nested-column pruning requirement in `datafusion-scan/nested-json-rendering`. The positive proof is a test over a file of several row groups whose leaf statistics would exclude the rendered document.
  - A partition column keeps the `Utf8` type the folded schema appends for it, so it types exactly as the pre-footer pass types it.
  - `TableScanResolver::resolve` gains a parameter that only the direct-storage reader reads, as `declared_columns` already is.
- **Promotes to ADR:** no

### [4] Float columns prune by the same bound rule as the scan's row-group pruning, Delta planning, and Iceberg planning

- **Decision:** FLOAT and DOUBLE columns prune on `<`, `<=`, `>`, `>=`, and `=`, and on the `IN` and `BETWEEN` forms built from them, from the bound range alone under an even number of enclosing `NOT`s, including zero. The operator set is fixed. Every FLOAT or DOUBLE row group counts as possibly holding a NaN row. One predicate function decides that row's truth values: it adds none for the five parity comparisons outside `NOT`, and adds TRUE and FALSE for `<>` and for any comparison under an odd number of `NOT`s. `NOT (D < 5)` and `NOT (D > 0)` over `[1, 4]` therefore keep the file.
- **Alternatives:**
  - Row-level yardstick: never drop a file that a total-order NaN row would match, with a live measurement narrowing the operator set. Rejected by the user. The user's reason: NaN is a known edge case that #393 already tracks, and floats prune "like Datafusion already does, and other places if any". Under total order this yardstick leaves only `=` and `IN` prunable.
  - Treat floats as unusable (the recommended interview option). Rejected by the user.
  - Assume IEEE semantics, where a NaN matches only `<>`. Rejected because `arrow-ord` compares floats in IEEE 754 total order, and #370's computed NaN matched `< -1E300`, which a negative NaN under total order explains.
  - Prune `NOT`-wrapped forms by rewriting them to negation normal form, as DataFusion's simplifier does. Rejected because the orchestrator's summary of the user's answer asks for `<>` and `NOT`-wrapped forms to stay conservative. This is stricter than DataFusion, which prunes `<>` and simplified `NOT` forms.
  - Hard-code an operator allow-list in the footer view. Rejected because the NaN row's truth is a property of evaluation, which the predicate owns.
- **Rationale:** The yardstick is parity: no layer's stored-NaN exposure grows. The scan's row-group pruning, `delta-kernel-rs` 0.26 data skipping, and `iceberg` 0.10 `InclusiveMetricsEvaluator` all prune on float bounds that exclude NaN. The live NaN characterization (Task 1) records the scan's behavior and pins it, but its outcome never changes the operator set. The rule and the operator set live in the float scenarios of `vs-adapter/direct-storage-statistics-pruning`, so this entry is not an ADR (`/speq:adr-rules` rule 3).
- **Consequences:**
  - A file whose bounds exclude the literal of a parity comparison is dropped even if it stores a NaN row. #393 tracks that shared exposure, and `docs/catalogs.md` states it as a limitation.
  - `+0`/`-0` bounds widen per the Parquet `TYPE_ORDER` read rules, because the scan's total order separates the two zeros.
  - A NaN bound, an all-NaN row group without bounds, a `min > max` statistic, and a non-finite literal count as unknown.
  - A FLOAT column compares its bounds, widened to DOUBLE, against the scan's DOUBLE literal (entry [6]).
  - The partition-value source never produces a NaN row, so the `NOT`-polarity flag leaves partition pruning unchanged.
- **Promotes to ADR:** no

### [5] An unusable statistic means unknown, gated by the Parquet specification's normative text

- **Decision:** A statistic counts as unknown when the column order is not `TYPE_DEFINED_ORDER` with a defined sort order, when it is deprecated, absent, missing a bound, undecodable, a NaN bound, or `min > max`. A missing null count is unknown, never zero. A truncated string bound is used as a bound. A statistics failure keeps the file and never fails the query.
- **Alternatives:** Best-effort use of legacy statistics. Rejected by the issue: a statistic whose ordering is not provably valid is unusable.
- **Rationale:** `parquet.thrift` states that without `column_orders` the bounds' meaning "is undefined", that deprecated `min`/`max` use "signed comparison only", and that "readers MUST NOT assume null_count == 0". parquet-rs 58.3.0 silently falls back to the deprecated fields when `min_value` and `max_value` are both absent (`file/metadata/thrift/mod.rs`), so the gate checks `is_min_max_deprecated()` itself. The crate reads a footer without `column_orders` as `ColumnOrder::UNDEFINED`, maps INT96 and INTERVAL to `SortOrder::UNDEFINED`, and reads a union member it does not know as `ColumnOrder::UNKNOWN`. Its `StatisticsConverter` counts a missing null count as zero unless `with_missing_null_counts_as_zero(false)` is set, and it checks no column order.
- **Promotes to ADR:** no

### [6] Float literal conversion applies to footer statistics only

- **Decision:** The predicate's translator carries a literal scope. The footer-statistics scope adds float conversions that reproduce the literal the scan compares against. An exact-numeric literal or a double literal against a DOUBLE column converts with `str::parse::<f64>` on the text the scan receives. Against a FLOAT column the literal converts the same way, and the footer view widens the row-group bounds to DOUBLE. One exception applies to a FLOAT column: an integer numeral that DataFusion parses as `i64` or `u64` converts only when it is exactly representable in `f32`. A non-finite parsed value never converts. The partition-value scope keeps today's conversions, and an undeclared column there still types as `Utf8`.
- **Alternatives:**
  - Add the float conversions for both callers. Rejected because Glue Hive `double` partition columns reach `catalog_parquet_format_reader.rs` today (`partition_column_types` types a partition column from its Spark type), and `vs-adapter/partition-predicate-declared-types` requires a float partition column to keep every file.
  - Convert only a numeral whose decimal value is exactly representable in the float type. Rejected because it only removes pruning. `vs-expression` puts the numeral text into the scan's SQL unchanged (`json_scalar_to_string`). DataFusion 54.1 `parse_sql_number` parses it with `str::parse::<f64>`, which rounds to nearest, and compares a DOUBLE column against that value with no further coercion. The same parse on the same text reproduces the scan's literal. `delta_predicate.rs` and `iceberg_predicate.rs` also convert with `parse_f64`.
  - Narrow the literal to FLOAT for a FLOAT column. Rejected because DataFusion compares a FLOAT column against a non-integer literal in DOUBLE (`numerical_coercion`: `(Float64, _) => Float64`), so a narrowed literal can differ from the scan's.
  - Widen the FLOAT bounds to DOUBLE for every literal, including integer numerals. Rejected because DataFusion compares a FLOAT column against an `Int64` or `UInt64` literal in FLOAT (`numerical_coercion`: `(_, Float32) => Float32`) after rounding the literal. `F = 16777217` therefore matches a stored `16777216.0`. Only an integer exactly representable in `f32` has the same value under both coercions.
- **Rationale:** The literal rule follows entry [3]: the footer scope compares against the scan's literal value, which settles each float case in code. The second caller's partition-column behavior stays byte for byte the same. The test case "a comparison on a float column" in `partition_predicate_tests.rs` pins its keep-every-file outcome. The user's "prune floats like Datafusion already does, and other places if any" is read as the layers that already prune on float bounds. Float partition pruning in the catalog-declared Parquet reader stays out of scope.
- **Consequences:**
  - `D > 0.1`, `D < 2.3`, and `PRICE <= 9.99` prune a DOUBLE file whose bounds exclude the literal, as the scan's row-group pruning, Delta planning, and Iceberg planning do.
  - The local DataFusion test of Task 2.5 pins the scan's DOUBLE parse and its FLOAT integer coercion, so a DataFusion upgrade that changes either fails a test.
- **Promotes to ADR:** no

### [7] An all-NULL row group prunes like a NULL partition value

- **Decision:** A row group whose null count is present and equals its row count holds only NULL in that column, so a comparison on it reaches only NULL.
- **Alternatives:** Treat a row group without bounds as unknown in every case. Rejected because the predicate already gives a NULL partition value this exact meaning, and Parquet writes no bounds for an all-NULL chunk.
- **Rationale:** One meaning for "this column is NULL here" across both inputs. The rule needs a present null count, so it never assumes a missing count is zero.
- **Promotes to ADR:** no

### [8] Time-zoned timestamps and FLOAT16 columns are unusable

- **Decision:** The footer view counts a timestamp with a time zone and a FLOAT16 column as unknown.
- **Alternatives:** Convert a timestamp literal into the zoned type with DataFusion's string conversion. Rejected because nothing verifies that this conversion agrees with how the scan compares a pushed timestamp against a zoned column.
- **Rationale:** Sound, not complete. Each gate is a single type check. A non-UTC zone and FLOAT16 also fail the scan-type check of entry [3]. A UTC zone passes it, so the timestamp gate stays explicit.
- **Promotes to ADR:** no

### [9] Direct-storage float pruning shares the stored-NaN exposure of the other layers, tracked by #393

- **Decision:** The new spec states that direct-storage float pruning has the same stored-NaN exposure as the scan's DataFusion 54.1 row-group pruning, Delta planning through `delta-kernel-rs` 0.26, and Iceberg planning through `iceberg` 0.10, and cites #393. It records no (#TBD) deviation for the NaN exposure. Delta planning, Iceberg planning, and the scan's row-group pruning get no code change. The scan's own column-order, deprecated-statistics, and null-count handling is a separate deviation from the Parquet specification. It covers a footer without `column_orders`, a column whose `ColumnOrder` is a union member the reader does not know, a statistic that carries only the deprecated `min`/`max` fields, and a statistic without `null_count`. Row-group and page pruning ignore `ColumnOrder`. `parquet` 58.3.0 falls back to the deprecated `min`/`max` fields (`old_format`), and row-group pruning uses them as type-ordered bounds with no deprecation check. Row-group pruning reads a missing null count as zero (`missing_null_counts_as_zero: true`). The plan records it unconditionally as a (#TBD) exception in a `DELTA:CHANGED` copy of the recorded scan scenario "Scan enables Parquet row-group and page pruning so the reader skips non-matching data". The direct-storage spec references that exception and does not restate it. The out-of-range probes `D > 10` and `D < -10` of Task 1 measure whether the scan's row-group pruning alone drops a stored NaN row. The probe file keeps a matching row group for each probe, so the probes stay scan-alone after statistics pruning exists. If the scan drops the row, Task 1.6 appends a NaN exception bullet citing #393 to the same scan delta.
- **Alternatives:** One (#TBD) deviation for Delta planning and the scan side (the first interview answer, superseded by the user's follow-up answer). Record the scan side only, or record nothing.
- **Rationale:** User choice. The exposure is consistent across layers, and #393 already tracks it. AGENTS.md requires a known deviation to be explicit and accurately scoped. The spec scopes the exposure to a stored NaN row. Planning opens no issue, so the (#TBD) waits for a human decision on whether an issue replaces it.
- **Promotes to ADR:** no

### [10] The E2E file-count assertions run through EXPLAIN VIRTUAL

- **Decision:** The E2E tests assert the pruned file list from the pushed SQL that `EXPLAIN VIRTUAL` returns, as `partition_filter_prunes_the_resolved_file_list` in the same file does, plus a SQL-level row assertion.
- **Alternatives:** In-process `format_reader(...).resolve_scan(Some(&filter))`, the `e2e_scan_test.rs::e2e_range_filter_prunes_by_file_bounds` pattern the issue names.
- **Rationale:** `EXPLAIN VIRTUAL` exercises the same `resolve_scan` through Exasol's real pushdown request, including Exasol's own literal kinds for `D = 2`, `D = 2.0`, and `D = 2E0`. That is the live verification AGENTS.md requires. The in-process route would need direct-storage scan-source wiring that this test file does not have.
- **Promotes to ADR:** no

### [11] Scope boundaries

- **Decision:** The plan changes the direct-storage reader, the shared predicate, and one new module. It also changes the plumbing that tells the reader which part of the filter the scan evaluates: `TableScanResolver::resolve`, `handle_pushdown`, `plan_join`, and the N-scan per-leg screen in `joins/rendering.rs`, whose rendered SQL stays unchanged (entry [3]). It adds no seam change, no extra object-store request, and no footer read under `MERGE_SCHEMA` FALSE. It adds no field to `ScanSpec`, `FileEntry`, or `LogicalField`. The catalog-declared Parquet reader (Unity Parquet and Glue Hive Parquet tables) gains no statistics pruning. Delta planning, Iceberg planning, the scan's row-group pruning, #393, and #246 stay unchanged. The scan's column-order, deprecated-statistics, and missing-null-count handling is recorded, not fixed (entry [9]).
- **Alternatives:** Extend statistics pruning to the catalog-declared Parquet reader. Rejected: that reader reads no footer at plan time. `ParquetFileSource::plan` lists files through `list_parquet_files` and `list_location_files`, and each file reaches the reader with `footer: None`. `vs-adapter/unity-parquet-table-planning` requires that "the reader MUST NOT read any Parquet footer at plan time" and calls the scan "file-pruning-blind". `vs-adapter/glue-table-planning` requires "no footer read at plan time". The accepted ADR `unity-parquet-schema-from-catalog-not-footer` records the same choice. Pruning there would add a footer request per file and would need a superseding ADR.
- **Rationale:** The issue scopes #412 to direct storage, whose footers the fold already paid for. AGENTS.md requires `ScanSpec` to stay format-neutral, and pruning removes file entries only.
- **Promotes to ADR:** no


### [12] One two-file fixture, `PRUNE_TYPES`, carries the type matrix, and `NAN_PROBE` stays separate

- **Decision:** The E2E coverage of statistics pruning uses one direct-storage table, `prune_types/`, of two files under the Hive key `grp`. Each file holds two row groups of four rows. The table holds one column per type that prunes, one column per keep gate, and one column per edge statistic. Every value derives from the row's `ID`, and the two files' value ranges are disjoint in every column. `NAN_PROBE` keeps its own file, so the stored-NaN characterization stays apart from the type matrix. `PRUNE_TYPES` holds NaN only in one all-NaN row group of column `N`, whose only case is `=`. An `=` against a non-NaN literal returns no NaN row under an IEEE comparison and under the scan's total order alike. The zero column `Z` stores `+0.0` only, and the writer records that row group's bounds as `[-0.0, +0.0]`.
- **Alternatives:**
  - One table with the NaN rows inside. Rejected by the user ("One table + NaN probe").
  - Reuse `all_types_batch`. Rejected: its three fixed rows in one file cannot give two files disjoint ranges per column. The fixture reuses `encode_parquet_with`, `put_fixture_object`, and the `cast` pattern of `all_types_batch` instead.
  - One table or one file per type. Rejected: more seeds and more setup, against the user's "as few e2e and table seeds as possible".
  - Store `-0.0` in `Z`. Rejected: `parquet` 58.3.0 writes the same `[-0.0, +0.0]` bounds for a row group of `+0.0` values and for one of `-0.0` values, so a stored `-0.0` adds no bound case. It would only pin the scan's unverified total-order result for `-0.0` (plan.md § Open Questions). A footer with a `+0` minimum or a `-0` maximum, which `parquet` 58.3.0 never writes, stays in the unit test `float_bounds_widen_zero_and_reject_nan`.
- **Rationale:** User direction. Two files are the fewest that show one file dropped and one kept in a single case. Two row groups per file are the fewest that show a file kept by its second row group alone. The Hive key is needed for the case that mixes a partition column and a data column under `OR`.
- **Consequences:**
  - The widened column `W`, INT32 in one file and INT64 in the other, prunes, because the scenario "A column the footer statistics cannot describe keeps the file" casts a widenable file type's bounds to the folded type. It is an edge column, not a keep gate. A file type that neither equals nor widens to the folded type cannot reach an E2E query, because the fold refuses such a pair, so that gate stays unit-only.
  - The DATE64, `DECIMAL(50,2)`, and FLOAT16 columns are declared `VARCHAR(2000000)`, and the scan compares them as text. Where text order allows it (`DEC256 < '5'`, `F16 < '5'`), a keep case's literal excludes one file in the column's own type while the text comparison still selects rows of that file, so a wrong prune changes the rows and not only the file list.
  - A FLOAT16 column has never been read end to end. If the scan cannot read it, Task 1.5 removes the column, and the FLOAT16 gate stays unit-only.
  - The timestamp columns cover every unit the reader admits without a time zone: seconds, milliseconds, microseconds, and nanoseconds. `parquet` 58.3.0 writes the seconds column as INT64 without a timestamp annotation, so its bounds type only through the Arrow schema the file embeds.
  - The INT96 keep case reuses the existing `annotated_types/` fixture, which already holds `c_int96`, so it adds no seed.
- **Promotes to ADR:** no

### [13] One table-driven E2E test whose expected rows are fixed before any pruning code exists

- **Decision:** `footer_statistics_prune_every_supported_type` is the one table-driven E2E test of statistics pruning. It iterates 76 cases of virtual schema, `WHERE` clause, expected files, and expected IDs. Task 1 adds it with its row assertions and runs it before any pruning code exists. Task 5 adds the file assertions and changes no expected ID. Every case that keeps a file also asserts that its filter rides in the scan spec, except the self-applied case, which asserts that the scan spec carries no filter. Two cases run on the existing `DIRECT_LAKEHOUSE_NARROW` virtual schema (`MERGE_SCHEMA = 'FALSE'`) over the same files. One case queries the existing `ANNOTATED_TYPES` table for the INT96 gate.
- **Alternatives:**
  - One test per scenario, as before. Rejected: each test repeats its setup, and the user asked for as few E2E tests as possible.
  - Compare each case with Exasol's own evaluation over a native copy of the table. Rejected: the literal cases pin the scan's rows, which may differ from an exact comparison (#TBD), and a native copy adds a table write to the suite.
  - Keep the `MERGE_SCHEMA = 'FALSE'` case at integration level only. Rejected: `DIRECT_LAKEHOUSE_NARROW` already serves the same directory, so the E2E case needs no extra seed. The integration test still proves that the reader reads no unsampled footer, which no E2E query can observe.
- **Rationale:** Rows fixed against the scan without statistics pruning, and unchanged once pruning exists, prove the parity step of the scenario "A footer whose row-group bounds exclude the filter drops the file" for every case at once. The scan-spec assertion stops a self-applied filter from passing a keep case without reaching pruning. Collecting every mismatch lets the one live run of Task 1.5 report each case whose pushed shape or rows differ from the plan.
- **Promotes to ADR:** no

### [14] The scan's inexact literal comparisons are reproduced live and pinned, and the scan does not change

- **Decision:** Task 1.5 runs four literal cases on Docker Exasol before any pruning code exists: `AMOUNT = 1234567.89` on a `DECIMAL(10,2)` column, a nine-digit timestamp equality on a nanosecond column, `TS_MS = TIMESTAMP '2025-01-01 00:00:09.000500'` on a millisecond column, and `F = 16777217` on a FLOAT column. If a case returns rows other than the rows an exact comparison of the stored values selects, a human opens an issue, the E2E case pins the observed rows citing that issue (`(#TBD)` until then), and the footer scope keeps refusing such literals. If a case returns the exact rows, decision-log.md records which literals the footer scope could then accept, and Task 2.3's rules stay. This plan changes no scan code. The new spec states the suspected behavior as an exception (#TBD), and Task 1.6 rewrites it to the measured fact.
- **Alternatives:** Fix the scan in this plan. Rejected by the user. Leave the cases out of E2E. Rejected by the user.
- **Rationale:** The user's answer "Verify live, then decide", and AGENTS.md's rule that a bug is reproduced locally before it is fixed. The FLOAT case joins the live run because the case table pins its rows too, and code reading predicts the same class of difference: DataFusion compares a FLOAT column against an integer literal in FLOAT after rounding the literal (decision [6]). The millisecond case joins for the same reason: DataFusion 54.1 `timeunit_coercion` compares a millisecond or second column against a microsecond literal in the column's coarser unit, so the cast cuts `00:00:09.000500` to `00:00:09.000`.
- **Consequences:**
  - Iceberg, Delta, and Glue tables share the scan comparison, and their plan-time pruning converts decimal and timestamp literals exactly. This plan records the exception for direct storage and lists the other routes in plan.md § Open Questions for the human who opens the issue.
  - Direct storage declares every timestamp column as a bare `TIMESTAMP` (`arrow_to_exasol_type`), which is `TIMESTAMP(3)`, so Exasol may cut the case-71 and case-73 literals before it pushes them. Task 1.5 records each pushed literal. Case 71 follows the ordinary literal rule if its pushed literal holds no non-zero digit past the sixth, and case 73 if its pushed literal holds none past the third.
- **Promotes to ADR:** no

## Review Findings

### [1] [plan-review] Footer literals must equal the literal the scan compares against

- **Finding:** The plan checked that a column's logical type equals its folded type, but not that the footer scope's literal equals the scan's literal. DataFusion 54.1 parses a non-integer numeral as `f64` and compares a decimal column against its rounding at scale 15, so `AMOUNT > 1234567.89` returns a row holding `1234567.89` that exact decimal pruning would drop. `vs-expression` renders a timestamp literal at microsecond precision, so a nine-digit literal on a nanosecond column differs from the scan's.
- **Direction change:** Task 2.3 converts a literal on a decimal column only when it is an integer numeral that parses as `i64` or `u64`, and a timestamp literal only when it has no non-zero fraction digit past the sixth. An `IN` list or a `BETWEEN` translates only when every literal converts, because `vs-expression` renders both natively and DataFusion coerces their literals to one common type. The scenario "Row-group statistics evaluate the filter under three-valued logic" gains the matching steps. Task 2.4 adds `footer_literals_convert_only_to_the_scans_value`. Task 2.5 adds a local DataFusion test that pins the scan's decimal, timestamp, and float literal values. Task 6.1 lists both new keep cases. Decision [3] Rationale states the literal condition.
- **Promotes to ADR:** no

### [2] [plan-review] Float literals convert to the value the scan parses

- **Finding:** The exact-only float literal rule rested on an unverified premise about the scan's coercion. The code settles it: `vs-expression` passes the numeral text unchanged, and DataFusion parses it with `str::parse::<f64>`. The exact-only rule therefore only removed pruning for literals such as `0.1`, which the scan's row-group pruning, Delta planning, and Iceberg planning all prune.
- **Direction change:** Task 2.3 converts an exact-numeric or double literal on a DOUBLE column with `str::parse::<f64>` on the text the scan receives. On a FLOAT column the footer view widens the bounds to DOUBLE (Task 3.2). One part differs from the finding's fix. DataFusion's `numerical_coercion` compares a FLOAT column against an `Int64` or `UInt64` literal in FLOAT, so an integer numeral converts against a FLOAT column only when `f32` represents it exactly. Widening the bounds alone would let `F = 16777217` drop a file holding `16777216.0`, which the scan returns. The float-literal scenario step, decision [6], decision [4]'s FLOAT consequence, the renamed unit test `float_literals_convert_to_the_scans_double_only_for_footer_statistics`, and the E2E case `D > 4.3` (case 34 of the case table in Task 1.4) follow the new rule.
- **Promotes to ADR:** no

### [3] [plan-review] The new spec's Background stated parquet-rs internals that no scenario uses

- **Finding:** The Background bullets on the parquet 58.3.0 deprecated-field fallback, its `ColumnOrder` and `SortOrder` mapping, and `StatisticsConverter`'s null-count default had no dependent scenario step. Neither did the #370 sentence.
- **Direction change:** The three crate bullets and the #370 sentence are deleted. The `parquet.thrift` quote "INTERVAL - undefined" joins the existing `ColumnOrder` quote. The crate's `nan_count` and NaN-writer bullet stays, because the float scenarios depend on it. The crate detail moves to decision [5] Rationale and Task 3.2.
- **Promotes to ADR:** no

### [4] [plan-review] The scan pruning exception omitted two known deviations

- **Finding:** The (#TBD) exception in the CHANGED scan scenario covered only a footer without `column_orders` or without `null_count`. DataFusion 54.1 also ignores a `ColumnOrder` union member the reader does not know. It also uses deprecated-only `min`/`max` statistics as type-ordered bounds, because `parquet` 58.3.0 falls back to them (`old_format`).
- **Direction change:** The exception's scope clause in `datafusion-scan/scan-execution-memory-and-credentials` names all four cases and cites the `parquet` 58.3.0 fallback beside the DataFusion citation. It keeps (#TBD). Decision [9] states the same scope. Decision [11], Task 1.6, the Impact entry, and the direct-storage spec step that references the exception now name it "column-order, deprecated-statistics, and null-count handling".
- **Promotes to ADR:** no

### [5] [plan-review] The type matrix omitted millisecond and second timestamp columns

- **Finding:** Direct storage admits `Timestamp(Millisecond, None)` and `Timestamp(Second, None)`: `arrow_type_to_tag` maps them to `timestamp_ms` and `timestamp_s`, and the footer scope prunes both. No test read either unit, although the user asked to cover all supported types. DataFusion 54.1 `timeunit_coercion` compares a millisecond or second column against a microsecond literal in the coarser unit, a fourth inexact literal class that the `(#TBD)` exception did not name.
- **Direction change:** `PRUNE_TYPES` gains `TS_MS` and `TS_S` (Task 1.3), and the fixture-shape test asserts `TS_MS` annotated `TIMESTAMP(MILLIS)` and `TS_S` stored as INT64 without an annotation. The case table gains drop cases 52 and 53 and the measured literal case 73, `TS_MS = TIMESTAMP '2025-01-01 00:00:09.000500'`, and the later cases are renumbered. Task 1.5 records the case-73 pushed literal and removes a unit the scan cannot read. Tasks 2.4, 2.5, and 3.3 add unit coverage for both units. The spec's type scenario names all four units and adds a step for the unannotated seconds column. The exception step reads "cut to microseconds, or to the column's own unit when it is coarser". Task 1.6, plan.md § Open Questions item 1, Task 6.1, § Scenario Coverage, § Impact, and decision [14] follow. The footer scope's literal rules do not change, because `exact_timestamp` already refuses a literal finer than the column's unit.
- **Promotes to ADR:** no

### [6] [plan-review] Statistics pruning reads only the part of the filter the scan evaluates

- **Finding:** Every soundness rule assumed that the scan evaluates the filter. On the self-apply path of `vs-adapter/pushdown-declined-filter-self-apply`, Exasol compares the emitted values, while the reader still received the full filter. A sub-millisecond timestamp is emitted at `TIMESTAMP(3)` precision, and an emitted empty string reads as NULL, so a footer bound could drop a file whose emitted rows Exasol selects.
- **Direction change:** The reviewer offered two options. This plan takes the first: the reader receives a statistics filter, which is the part of the pushed filter the scan evaluates. `handle_pushdown` derives it from the `classify_where_filter` outcome it already computes before `TableScanResolver::resolve`. `plan_join` derives it per leg from the N-scan per-leg screen, which moves into one function in `joins/rendering.rs`, because the join path decides per conjunct and after resolution today. Decision [3], decision [11], the new scenario "Statistics pruning reads only the part of the filter the scan evaluates", Task 4.1, the tests of Task 4.3, E2E case 74, the `SECOND` queries of Task 5.2, Task 6.1, and § Impact follow. Plan.md § Open Questions item 5 names the same exposure for Iceberg and Delta planning, which this plan does not change.
- **Promotes to ADR:** no

## Live Measurements

All runs below used `make cross-udf-build` at the branch base, before any statistics pruning code existed, and `cargo test --features exasol-e2e --test e2e_direct_storage_test -- --test-threads=1` against the Docker image `exasol/docker-db:2025.1.16` (the default `EXASOL_IMAGE`) and the local SeaweedFS stack. Pushed literals come from the `PUSHDOWN_JSON` column of `EXPLAIN VIRTUAL`, and scan filters from its `PUSHDOWN_SQL` column.

### [M1] Measured scan literal comparisons

- **Result:** All four planned literal cases reproduced an inexact scan comparison. For each, the scan returned rows that differ from an exact comparison of the stored values.

  | Case | Clause | Pushed literal | Scan filter | Scan rows | Exact rows |
  |------|--------|----------------|-------------|-----------|------------|
  | 70 | `AMOUNT = 1234567.89` | `literal_exactnumeric` `"1234567.89"` | `("AMOUNT" = 1234567.89)` | none | 16 |
  | 71 | `TS_NS = TIMESTAMP '2025-06-01 00:00:00.123456789'` | `literal_timestamp` `"2025-06-01 00:00:00.123456789"` | `("TS_NS" = arrow_cast('2025-06-01 00:00:00.123456789', 'Timestamp(Microsecond, None)'))` | none | 16 |
  | 72 (planned) | `F = 16777217` | `literal_exactnumeric` `"16777217"` | `("F" = 16777217)` | 16 | none |
  | 73 | `TS_MS = TIMESTAMP '2025-01-01 00:00:09.000500'` | `literal_timestamp` `"2025-01-01 00:00:09.000500"` | `("TS_MS" = arrow_cast('2025-01-01 00:00:09.000500', 'Timestamp(Microsecond, None)'))` | 9 | none |

- **Outcome per class (user decisions):**
  - **Decimal (case 70): not fixed, pinned with (#TBD).** Setting `datafusion.sql_parser.parse_float_as_decimal = true` in `session_config_for_spec` made case 70 return ID 16 live, but it regressed three other results, so the setting was reverted.
    - DataFusion 54.1 prefers a decimal over a float in a comparison (`decimal_coercion`), so it cast the FLOAT column to `Decimal128(14,7)` for `F < 4.5` (case 24: "16777209.0000000 is too large to store in a Decimal128 of precision 14").
    - It cast the DOUBLE column to `Decimal128(30,15)` for `N = 0.5` (case 68) and for `D < 2.5` (`stored_nan_comparison_outcome_is_pinned`): "Cannot cast to Decimal128(30, 15). Overflowing on NaN".
    - The local scan test also showed that Exasol's `literal_double` text `1.0000000000000001e+300` fails planning ("Decimal scale -284 exceeds the minimum supported scale: -128").
    - Every other suite of the full E2E run matched the pre-flag baseline. Both runs fail `e2e_scan_test::adapter_detects_container_cpuset` only because its CPU-set precondition does not hold on this 4-core host.
    - Case 70 pins the observed rows (none), and Task 2.3's integer-only decimal rule stays unchanged.
  - **Timestamps (cases 71 and 73): not fixed, tracked by #461.** Case 71 pins no rows and case 73 pins ID 9. Task 2.3's timestamp rules stay unchanged.
  - **FLOAT (case 72): not pursued.** The case became the inequality `F <= 16777217`, so it no longer depends on the FLOAT coercion. Live it pushes `literal_exactnumeric` `"16777217"`, rides in the scan spec as `("F" <= 16777217)`, and returns IDs 1 to 16, the exact rows. Task 2.3's FLOAT rule still refuses the literal, so both files stay.
  - Note on the FLOAT coercion: the scan compares a FLOAT column against an integer literal in FLOAT after rounding the literal, so `16777217` compares as `16777216f32`, and the planned `F = 16777217` returned ID 16 where an exact comparison selects none.
- Exasol pushes the case-71 literal with all nine fraction digits and the case-73 literal with all six. It does not cut either literal to the declared `TIMESTAMP(3)`, so neither case falls back to the ordinary literal rule.
- No Task 2.3 rule is unsound. Each rule refuses all four planned literals, so the footer keeps both files, and the scan's rows come from kept files.
- The footer scope accepts none of the four planned literals.
- Pushed literal kinds of the cases Task 1.5 names:

  | Case | Clause | Pushed literal | Scan filter |
  |------|--------|----------------|-------------|
  | 25 | `F = 16777216` | `literal_exactnumeric` `"16777216"` | `("F" = 16777216)` |
  | 27 | `D = 2` | `literal_exactnumeric` `"2"` | `("D" = 2)` |
  | 28 (planned) | `D = 2E0` | `literal_exactnumeric` `"2"` | `("D" = 2)` |
  | 28 (replacement, [M3]) | `D = CAST(2 AS DOUBLE)` | `literal_double` `"2.0000000000000000e+00"` | `("D" = 2.0000000000000000e+00)` |
  | 37 | `AMOUNT <= 80` | `literal_exactnumeric` `"80"` | `("AMOUNT" <= 80)` |
  | 38 | `AMOUNT > 100` | `literal_exactnumeric` `"100"`, operands swapped | `(100 < "AMOUNT")` |
  | 39 | `AMOUNT < 5` | `literal_exactnumeric` `"5"` | `("AMOUNT" < 5)` |
  | 40 | `DEC9 >= 13` | `literal_exactnumeric` `"13"`, operands swapped | `(13 <= "DEC9")` |
  | 41 | `DEC30 < 0` | `literal_exactnumeric` `"0"` | `("DEC30" < 0)` |
  | 72 (planned) | `F = 16777217` | `literal_exactnumeric` `"16777217"` | `("F" = 16777217)` |
  | 72 (replacement) | `F <= 16777217` | `literal_exactnumeric` `"16777217"` | `("F" <= 16777217)` |

- Exasol pushes a decimal literal as a bare integer, not at the column's scale, so the integer-only decimal rule of Task 2.3 reaches `DECIMAL(p,s>0)` columns. The Task 1.5 stop for a decimal literal pushed at the column's scale does not apply.
- Exasol pushes an exact-numeric literal without trailing fraction zeros, and it pushes an exponent numeral whose value is an exact decimal as `literal_exactnumeric` (`2E0` as `"2"`, `2.5E0` as `"2.5"`, `1E-1` as `"0.1"`). It pushes `literal_double` for a DOUBLE-typed expression (`CAST(2 AS DOUBLE)` as `"2.0000000000000000e+00"`) and for a numeral outside the exact-numeric range (`1E300` as `"1.0000000000000001e+300"`). `str::parse::<f64>` reads both texts to the value they denote.

### [M2] Measured stored-NaN outcome

- **Result:** `EXPLAIN VIRTUAL` of `SELECT ID FROM NAN_PROBE WHERE D > 2.5` carries the filter in the scan spec as `(2.5 < "D")`, so the parity premise holds. `D > 10` rides as `(10 < "D")` and `D < -10` as `("D" < -10)`, and both plans name `nan_probe/probe.parquet`.

  | Filter | ID 5 (positive NaN) | ID 6 (negative NaN) |
  |--------|---------------------|---------------------|
  | `D < 2.5` | not returned | returned |
  | `D <= 2.5` | not returned | returned |
  | `D > 2.5` | returned | not returned |
  | `D >= 2.5` | returned | not returned |
  | `D = 2.5` | not returned | not returned |
  | `D <> 2.5` | returned | returned |
  | `D > 10` | not returned | not returned |
  | `D < -10` | not returned | not returned |

- In range, the scan compares in IEEE 754 total order: a positive NaN sorts above every number and a negative NaN below every number.
- Out of range, neither NaN row returns, although total order makes `+NaN > 10` and `-NaN < -10` true. Row group 1, whose bounds `[1, 4]` exclude both literals, is dropped by the scan's own row-group pruning, so that pruning alone already drops a stored NaN row that the unpruned comparison returns. Neither probe returns ID 5 or ID 6, so the float scenario's THEN step keeps parity with all three layers. Task 1.6 appends the NaN exception bullet (#393) to the scan delta.
- The outcome records behavior only. The float operator set of decision [4] does not change.
- `stored_nan_comparison_outcome_is_pinned` asserts this table (`NAN_PROBE_OUTCOMES`).

### [M3] Pushed shapes the case table replaces

- **Decision:** Three planned clauses reach the scan in a shape that no longer exercises their rule, so Task 1.5's "Pushed shapes" branch replaces them. Each replacement keeps the case's purpose and was run live.
  - Case 26 `D = 2.0` is pushed as `literal_exactnumeric` `"2"`, which carries no fraction and duplicates case 27. It becomes `D = 2.5` (`"2.5"`), IDs 5, file `a`.
  - Case 28 `D = 2E0` is pushed as `literal_exactnumeric` `"2"`. It becomes `D = CAST(2 AS DOUBLE)` (`literal_double`), IDs 4, file `a`. The `literal_double` rule therefore stays covered end to end, and § Scenario Coverage needs no unit-only mark.
  - Case 36 `D NOT IN (1.0)` is pushed as `NOT ("D" = 1)`, because Exasol rewrites a one-element `NOT IN` to a negated equality. It becomes `D NOT IN (1.0, 1.5)`, pushed as `predicate_not` over `predicate_in_constlist`, IDs 1 and 4 to 16, both files.
- Every other case reaches the scan in its planned shape: `B = TRUE` and `B <> TRUE` as `predicate_equal` and `predicate_notequal` over `literal_bool`, cases 8, 10, and 35 under `predicate_not`, case 12 in the scan spec as `((100 < "ID") OR (("ID" * 2) = 6))`, case 14 as `predicate_or`, and case 74 self-applied (`"filter":null`).
- The other Task 1.5 gates pass: the scan reads `F16` (case 57), `TS_MS` (case 52), and `TS_S` (case 53); the `c_int96` column chunk carries a min and a max; and no case other than 70 to 73 returns rows other than its planned IDs.
- Case 72 is rewritten by a user decision, not by this branch ([M1]).
- **Promotes to ADR:** no

### [M4] An all-NULL column chunk reads as deprecated statistics

- **Finding:** The first E2E run with statistics pruning kept the `grp=b` file for cases 63 (`NUL IS NOT NULL`) and 65 (`NUL > 2`), whose `NUL` column chunks are all NULL. `parquet` 58.3.0 `from_thrift_page_stats` sets `is_min_max_deprecated()` whenever `min_value` and `max_value` are both absent (`old_format = stats.min_value.is_none() && stats.max_value.is_none()`), so it reads a modern writer's boundless all-NULL chunk as deprecated, and the deprecated gate of Task 3.2 discarded its present null count.
- **Decision:** The deprecated gate applies only when the chunk holds a minimum or a maximum. A chunk without bounds has no deprecated ordering to distrust, and its null count is a generic field that the deprecated-statistics rule does not cover. Deprecated bounds that are present still count as unknown.
- **Consequences:** `a_file_is_kept_iff_one_row_group_can_be_true` pins the crate's flag on an all-NULL chunk and the pruning it now allows. Cases 63 and 65 name the `grp=a` file only, as planned.
- **Promotes to ADR:** no
