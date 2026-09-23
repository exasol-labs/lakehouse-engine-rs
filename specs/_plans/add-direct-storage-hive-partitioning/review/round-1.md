# Plan Review Findings: add-direct-storage-hive-partitioning (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 15 (Blockers: 5, Advisory: 10)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 1

## Premortem

Six months from now this plan failed badly. Three concrete reasons:

1. A Spark backfill job writes flat files into a partitioned table root. Each file stores `event_date` but sits under no `event_date=` directory. After the upgrade every backfilled row reads `EVENT_DATE` NULL, and `WHERE EVENT_DATE = '2026-03-01'` prunes those files outright. No error surfaces. The plan justified the NULL rule with a DuckDB precedent that does not exist, because DuckDB rejects that layout. Routed to Feasibility `[UNSTATED_ASSUMPTION]` (HUMAN).
2. A filter declines to Exasol self-application. Pruning drops partition files by Rust byte order while Exasol applies its own `VARCHAR` order above the scan. The live E2E passed because `'2024' < '2025' < '2026'` holds under every plausible collation, so it never tested the ordering claim. Routed to Feasibility `[UNSTATED_ASSUMPTION]`.
3. Plan #412 reads the merged library. The seam and properties specs still say the fold fails on every uppercase collision and reads every footer at pushdown. #412 then prunes on the footer statistics of the Parquet column the fold dropped, which is DuckDB's own bug duckdb#24736. Routed to Requirement Quality `[REQUIREMENT_CONFLICT]` and `[COMPLETENESS_GAP]`.

## Intent Fidelity
[no objection — axis checked: (1) range pruning is in scope: task 2.2 lists `predicate_less` through `predicate_between`, and task 2.7 runs `EXPLAIN VIRTUAL` for `YEAR = '2026'` and `YEAR > '2025'` against the live stack. The pushed SQL carries each shard's file list as a plain JSON literal (`adapter/pushdown/support.rs::build_fan_out_inner`), so the live pushdown-shape check is real. The ordering-proof gap is raised under Feasibility. (2) Directory-wins is implemented as the user decided (task 1.4) and is not re-litigated. The two-keys-collide failure stays coherent with it: a file-versus-directory conflict has a precedence rule, two directory spellings have none. (3) Percent-encoding is required, not scope creep: issue #408's E2E list names "a URL-encoded value (`region=a%2Fb`) is decoded", and the scan resolves every file path through `ListingTableUrl::parse` (`scan/raw_scan.rs:216`, `scan/positional_deletes.rs:222`), which decodes `%2F`. The fix is confined to `file_entry`. (4) The absent-column fill reads `involvedTables[].columns` through `adapter/pushdown/support.rs::column_types`, which is the stored declaration. The present-column wording is raised under Prose Quality. (5) The ADR question is raised under Design Depth, and the segment-less NULL rule under Feasibility.]

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER: a segment-less file reads NULL on a claimed precedent that does not exist
- Location: `vs-adapter/direct-storage-hive-partitioning/spec.md` § Scenario "A partition key that names a Parquet column overrides it" (third step) and § Background bullet 4. `plan.md` § Implementation Tasks 1.4, 2.6 (`collision/p2.parquet`), 2.7. `decision-log.md` [7] Decision and Rationale.
- Issue: The plan rules that a file storing `K` whose path lacks the `k=` segment "SHALL read `K` as NULL ... and MUST NOT fall back to that file's own stored value". It rests this on DuckDB. DuckDB does not support that layout. `MultiFileReader::BindOptions` (`src/common/multi_file/multi_file_reader.cpp` on current main, lines 316-337) parses the keys of the first file and throws `Hive partition mismatch between file "%s" and "%s": key "%s" not found` for any file that lacks one. The override (lines 357-359) therefore applies only when every file carries the segment. The citations establish no more. duckdb#24555 rejects a collision between the generated `filename` column and a hive key. duckdb#24752 skips footer statistics for an overridden column, and both of its repro files carry `h0=42/`. Decision [7]'s "confirms the override is whole-scan, not per file" misreads #24752. The user asked that "the partition value wins over the stored value in the parquet file". A segment-less file has no partition value, so the user's rule does not decide this case. The NULL rule discards stored data with no error, and an equality filter on `K` then prunes those files. The "exactly one source" rationale also fails over time: if the last `k=` directory is deleted, the key leaves the declaration and the same file reads its stored `42` again. This needs the requester, because each option changes the values a user gets back.
- Fix: Ask the user to choose the rule for a file that stores a column colliding with a declared key but lacks that key's segment: (a) read NULL (the current plan), (b) fail `CREATE`/`REFRESH` with an error naming the column, the key, and one such file (DuckDB's behavior for a mixed layout), or (c) read the file's stored value (needs per-file partition-or-file binding in `scan/partition_values.rs`, which conflicts with the plan's no-scan-change Non-Goal). Then rewrite the collision scenario's third step, task 1.4, the task 2.6 and 2.7 fixture assertions, and decision [7] to the chosen rule. Whatever the choice, replace decision [7]'s precedent text with what DuckDB does (override when every file carries the segment, as the `BindOptions` code quoted in duckdb#24752 shows, and a mismatch error otherwise). Delete the duckdb#24555 citation and the "whole-scan, not per file" claim.
- Escalation: HUMAN

#### [UNSTATED_ASSUMPTION] BLOCKER: the range-pruning E2E cannot falsify the ordering claim it is cited for
- Location: `plan.md` § Implementation Tasks 2.2, 2.6, 2.7 and § Verification (the pruning E2E paragraph). `decision-log.md` [3] Rationale. `vs-adapter/direct-storage-hive-partitioning/spec.md` § Scenario "A predicate on partition columns prunes files before their footers are read" (the "byte/codepoint order, matching how Exasol compares `VARCHAR` values" step).
- Issue: Task 2.2 says "task 2.7's range-pruning E2E test confirms live that this matches Exasol's own `VARCHAR` comparison". The only range fixture values are `2024`, `2025`, and `2026`: equal-length ASCII digit strings. Byte order, codepoint order, case-insensitive order, and every locale collation rank them the same way, so the test passes whatever order Exasol uses. The user required this ordering to be verified live, not assumed. The claim is load-bearing on one path. When a filter declines, `vs-adapter/pushdown-declined-filter-self-apply` evaluates it in Exasol's outer `WHERE`, so a file that Rust pruned but whose rows Exasol would keep becomes a silently missing row. On the rendered-filter path DataFusion's own Utf8 comparison applies, which is byte order.
- Fix: Add a fixture table whose partition values rank differently under byte order and under case-insensitive or locale order, for example `regions/region=B/`, `regions/region=a/`, `regions/region=é/`. In task 2.7, run `WHERE REGION > 'Z'`, `WHERE REGION < 'z'`, and `WHERE REGION BETWEEN 'B' AND 'a'`, in-process and through `EXPLAIN VIRTUAL`. Assert that the kept file set equals the value set Exasol itself selects natively on the same connection, for example `SELECT r FROM (VALUES ('B'),('a'),('é')) AS t(r) WHERE r > 'Z'`. Add one variant whose filter declines to the outer wrapper (a conjunct the DataFusion render declines). State in task 2.2 that a mismatch stops implementation and returns the plan to planning. Rewrite the spec step to state the requirement (byte order, which is DataFusion's Utf8 order and, verified live, Exasol's `VARCHAR` order) without claiming a specific test proves it.
- Escalation: MECHANICAL

#### [NFR_IGNORED] ADVISORY: upgrade behavior before `REFRESH` is misstated
- Location: `plan.md` § Impact, first bullet. § Implementation Tasks 2.8.
- Issue: The Impact bullet says the change lands "after `REFRESH VIRTUAL SCHEMA`". The pushdown path runs discovery on every query (task 1.5 passes `hive_partitioning` to the seam at pushdown), so an un-refreshed schema changes at its next query. A table with two colliding keys fails every query. A key-versus-column table reads the directory value, coerced into the stored non-`VARCHAR` type by the strict cast in `scan/emit.rs::coerce_column`, so a non-numeric directory value fails the query. A segment-less file reads NULL at once.
- Fix: Rewrite the Impact bullet to state the pre-`REFRESH` behavior for these three cases. Add the same text to the `docs/catalogs.md` paragraph in task 2.8, naming `HIVE_PARTITIONING = 'FALSE'` as the way to keep the previous behavior across the upgrade.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER: three recorded scenarios contradict the plan and carry no delta
- Location: recorded `specs/vs-adapter/parquet-directory-seam/spec.md` § Scenario "The folded column set is the union of the files' column sets" and § Scenario "Footers fold into one schema under the proven-castable widening pairs". Recorded `specs/vs-adapter/direct-storage-properties/spec.md` § Scenario "MERGE_SCHEMA selects one footer or every footer, on both the refresh and the plan path". No delta file in this plan names any of the three.
- Issue: (1) The union scenario states that "two columns whose names are equal after the declaration's uppercase fold SHALL fail the fold", and that the folded schema carries every file's columns. Task 1.4 drops a Parquet column that folds onto a declared key, and task 1.3 appends the partition columns to the seam's `schema`. (2) The widening scenario states that a column at two types with no supported pair "SHALL fail the fold". A dropped overridden column never reaches that check, so files that disagree on its type (the duckdb#24752 repro shape) no longer fail. (3) The properties scenario states that the default mode "SHALL fold EVERY data file's footer into the table's schema, at `createVirtualSchema` and at pushdown alike". The seam delta's NEW scenario reads only the kept files' footers at pushdown. After merge the library states both rules.
- Fix: In `specs/_plans/add-direct-storage-hive-partitioning/vs-adapter/parquet-directory-seam/spec.md`, add `DELTA:CHANGED` blocks for both seam scenarios. Exempt a Parquet column whose uppercase fold equals a declared partition key from the union, from the column-collision failure, and from the widening check, citing `vs-adapter/direct-storage-hive-partitioning`. State that the seam's schema ends with the partition columns. In `specs/_plans/add-direct-storage-hive-partitioning/vs-adapter/direct-storage-properties/spec.md`, add a `DELTA:CHANGED` block for the MERGE_SCHEMA scenario that says "every KEPT data file's footer" at pushdown and cites the seam's file-keep predicate.
- Escalation: MECHANICAL

#### [IMPLEMENTATION_LEAKAGE] BLOCKER: the new feature spec's Background carries rationale no step depends on
- Location: `vs-adapter/direct-storage-hive-partitioning/spec.md` § Background, bullets 1, 2, and 4.
- Issue: Bullet 4 (Hive writers encode the value both ways, DuckDB's `BindOptions` code, Polars' rejection of a suffix) states facts that no GIVEN, WHEN, or THEN step depends on. It is decision rationale, and part of it is wrong (see the Feasibility HUMAN finding). Bullet 2's third sentence ("It shares its outcome with `vs-adapter/pushdown-file-pruning` and `vs-adapter/delta-file-pruning` and none of their mechanism") has no dependent step. Neither sentence of bullet 1 ("run inside `vs-adapter/parquet-directory-seam`", "Table enumeration and query planning therefore declare the same partition columns") has a dependent step in this spec. The user asked for concise specs.
- Fix: Delete bullet 4 and keep its corrected content in `decision-log.md` [7] only. Delete bullet 2's third sentence. Delete bullet 1, or add this step to the union scenario: "*AND* table enumeration and query planning SHALL declare the same partition columns, because both obtain them from `vs-adapter/parquet-directory-seam`". Keep the Iceberg and Delta specification-check bullet.
- Escalation: MECHANICAL

#### [AMBIGUOUS_REQUIREMENT] ADVISORY: the segment rule admits a file name, and the encode set is open
- Location: `vs-adapter/direct-storage-hive-partitioning/spec.md` § Scenario "A key=value directory segment declares a VARCHAR partition column" (first AND step). `plan.md` tasks 1.2 and 2.3. `vs-adapter/direct-storage-table-planning/spec.md` § Scenario "A direct-storage table resolves its files and schema through the shared seam" (encoding step).
- Issue: The step says "a path segment below the table root SHALL be a partition segment only when it matches `^[^/=]+=[^/]*$`". A file name such as `x=1.parquet` matches that pattern. Today's `partition_segments` excludes the last segment with `split_last`, but neither the step nor task 1.2 says so. Task 2.3 encodes "at least `%`, `#`, `?`", which leaves the set open.
- Fix: Change the step to "a DIRECTORY segment below the table root (every segment except the file name)". Name the exact encode set (`%`, `#`, `?`) in task 2.3 and in the planning delta's step, and keep task 2.4's round-trip cases as the proof.

#### [COMPLETENESS_GAP] ADVISORY: the collision scenario omits the column position and the same-spelling case
- Location: `vs-adapter/direct-storage-hive-partitioning/spec.md` § Scenario "A partition key that names a Parquet column overrides it". `plan.md` tasks 1.3 and 2.6.
- Issue: The scenario does not say where `K` is declared. DuckDB keeps the file column's position (`hive_partitioning_index = idx`). Task 1.3 appends every partition column after the folded columns, so an overridden column moves to the end, while the Background says "This feature follows DuckDB". The only E2E collision fixture pairs a stored column `K` with a key `k`. The shape the user described, a writer that stores `year` and also writes `year=`, uses one spelling on both sides and has no scan-level test.
- Fix: Add a step that states the overridden column's declared position (appended with the partition columns, or kept at the file position, both of which `PartitionedScanSchema::split` supports). Add a second collision fixture whose stored column and key share one spelling (`k` and `k=`), and assert that it reads the directory value.

#### [COMPLETENESS_GAP] ADVISORY: nothing warns #412 off the dropped column's footer statistics
- Location: `vs-adapter/parquet-directory-seam/spec.md` delta § Background (the #412 bullet) and § Scenario "One seam answers the file list and the schema for both callers" (parsed-footer steps).
- Issue: The seam returns parsed footers for #412. For an overridden column, each footer still holds the statistics of the dropped Parquet column, which describe values the scan never emits. DuckDB shipped this exact defect (duckdb#24736, fixed by duckdb#24752) by resolving statistics by name for an overridden column. This plan cites that fix but records nothing for #412.
- Fix: Add an AND step to the collision scenario, or to the seam's parsed-footer step: "a consumer that prunes from footer statistics MUST NOT use the statistics of a Parquet column the fold dropped for a partition-key collision".

#### [COMPLETENESS_GAP] ADVISORY: no step covers a filter that prunes every file
- Location: `vs-adapter/direct-storage-hive-partitioning/spec.md` § Scenario "A predicate on partition columns prunes files before their footers are read". `plan.md` § Scenario Coverage.
- Issue: No step or test covers a filter that keeps no file (`WHERE YEAR = '2099'`). `handle_pushdown` returns `empty_result_sql` when `files` is empty (`adapter/pushdown/mod.rs:260`). No test pins that this path reads no footer under the fold-every-file mode and returns zero rows rather than an error.
- Fix: Add an AND step for the all-pruned case. Add one unit test in `parquet_format_reader_tests.rs` that asserts zero files, zero footer reads, and no error.

## Task Breakdown

#### [CLUSTER_INCOHERENCE] ADVISORY: two groups run a signature change over one call-site set
- Location: `plan.md` tasks 1.1 and 2.1. § Parallelization.
- Issue: Task 1.1 rewrites `ScanSource::DirectParquet`, `ParquetFormatReader`, `scan_resolution.rs`, and `format_tests.rs:171-174`. Task 2.1 edits the same variant, the same reader, `scan_resolution.rs`, and `format_tests.rs:171` again to add `declared_columns`. The project memory records graph-wide signature changes as the main cause of implementer context blowups, and this plan schedules two such passes over one census.
- Fix: Move the plumbing part of task 2.1 (the `ScanSource::DirectParquet` field, the `TableScanResolver::resolve` and `resolve_one_join_side` parameters, the nine `scan_resolution_tests.rs` call sites, and `format_tests.rs:171`) into group A beside task 1.1, with one merged census. Keep the reader's use of `declared_columns` in task 2.3.

#### [TRACEABILITY_GAP] ADVISORY: two verification rows do not match the planned fixture and code
- Location: `plan.md` § Manual Testing, row 2. § Scenario Coverage, row 2.
- Issue: The manual query `SELECT ID, YEAR, MONTH FROM DIRECT_LAKEHOUSE.SALES ORDER BY ID` expects rows for `2026` and `2025` only. Task 2.6 adds `sales/year=2024/month=01/p3.parquet`, so the query also returns that file's rows. The unit test `partition_segments_follow_the_key_value_rule_and_decode_values` is named after the function that task 1.2 removes.
- Fix: Add the `2024` and `01` rows to the expected output. Rename the unit test after its behavior, for example `directory_segments_follow_the_key_value_rule_and_decode_values`.

## Design Depth

#### [ADR_OVERPROMOTION] BLOCKER: decision [7] and its review entry both promote to an ADR
- Location: `decision-log.md` [7] and § Review Findings "[plan-review] Partition-key/Parquet-column collision was planned to fail the refresh".
- Issue: Both entries carry `Promotes to ADR: yes`. The review entry restates [7], and the `/speq:planning` gate allows "one ADR per genuinely new project-wide constraint, never one per entry or per resolved finding". Decision [7] is a data-precedence rule inside one catalog kind's partition discovery. That is a local design choice, which the gate sets to `no`. The scenario "A partition key that names a Parquet column overrides it" already records the behavior normatively. The user's brief reserves ADRs for truly architectural decisions, and the interview answer was "No ADR for this plan". The Consequences argument (a reversal would change returned values) holds for every behavioral scenario, so it does not single this one out.
- Fix: Set `Promotes to ADR: no` on decision [7] and on the review entry. Delete the ADR sentence from [7]'s Consequences line.
- Escalation: MECHANICAL

No other objection on this axis. The Quick Diagnostic table in `plan.md` § Patterns was checked: the seam stays filter-JSON-free behind an injected keep predicate, `DirectoryOptions` has one derivation site, and `partition_predicate` hides the filter grammar behind two calls.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY: the decision log reads as a changelog
- Location: `decision-log.md` § Interview (the "Superseded" note), [3] Alternatives ("Rejected on review"), [7] ("as originally planned", "unchanged from the original plan", "Rejected on review"), § Review Findings (opening paragraph and all three entries). `plan.md` § Consequences row 5 ("as originally planned") and task 1.4 ("still FAIL").
- Issue: The user asked for concise decisions, "not a changelog". These passages narrate revisions. The three `[plan-review]` entries also present the user's answers to the planner's escalated questions as plan-reviewer findings ("`[SCOPE_REDUCTION]` BLOCKER, escalated HUMAN"), although no plan review preceded this round. A round-2 recheck matches `[plan-review]` entries against round-1 blockers, so these three entries would be mismatched against this file.
- Fix: Record the three user decisions as `## Interview` question-and-answer entries. Delete the § Review Findings section and the "Superseded" note. State [3], [7], and the two `plan.md` passages as current facts, without "originally", "still", or "on review".

#### [PROSE_UNCLEAR] ADVISORY: decision [2] claims more than the design delivers for present columns
- Location: `decision-log.md` [2] Rationale. `plan.md` § Consequences row 2.
- Issue: The absent-column fix does reflect a `REFRESH`-fixed schema: `adapter/pushdown/support.rs::column_types` reads `involvedTables[].columns`, and task 2.3 types each absent column from that declaration. Present columns are different. Their Arrow types still come from the kept files' fold, so they vary with the filter. That is the recorded rule `vs-adapter/parquet-directory-seam` § "The declaration decides the emitted width and the footer decides the structure", which already names file pruning and relies on `scan/emit.rs::coerce_column` to emit at the declared width. "Plan-time pruning narrows which files are read, never what the table's schema is" states a stronger guarantee than the design gives.
- Fix: Scope that sentence in [2] and the Consequences row to absent columns. Add one sentence that cites the recorded seam scenario for present columns (logical type from the kept fold, Exasol-visible width from the declaration).

#### [PROSE_BLOAT] ADVISORY: em dashes and semicolons in governed prose
- Location: `plan.md` § Impact (line 105), tasks 1.4 (line 142), 2.2 (line 188), 2.6. `decision-log.md` [2], [3], [7], § Review Findings.
- Issue: `/speq:writing-guardrails` bans em dashes and semicolons in plan and decision-log prose. Examples: task 2.2 "(byte/codepoint order) — task 2.7's range-pruning E2E test", task 2.6 "— deliberately different from the directory's `1`", task 1.4 "(1.3); it MUST NOT fall back", Impact "overrides it); a table whose two DIFFERENT keys".
- Fix: Replace each em dash with a period, a comma, a colon, or parentheses. Split each semicolon-joined sentence into two sentences.
