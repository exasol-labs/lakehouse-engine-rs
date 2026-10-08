# Decision Log: fix-iceberg-pruning-not-over-inexact-and

## Interview

**Q:** How should Iceberg and Delta share the exactness rules?
**A:** Generic combinator module. One format-neutral module owns `Translated { predicate, exact }` plus the AND, OR, and NOT folds, generic over the predicate type. Each format keeps only its leaf translation and supplies its AND, OR, and NOT constructors. Rejected: a shared predicate tree (too big) and two copies (they drift).

**Q:** Which formats must the E2E case table run on?
**A:** Iceberg and Hive direct storage. The Iceberg fixture is written through `seed.rs`, and the same rows are written as raw Parquet through `raw_parquet.rs`. Delta is covered by host unit tests of the shared rules. A writable Delta fixture is a follow-up, marked `(#TBD)` and listed as an open question.

**Q:** How is the Iceberg fixture written?
**A:** With the iceberg-rust writer in `tests/common/seed.rs`: partitioned, several data files per partition, files with several row groups, and a file whose column holds only NULLs. The ground truth lives in the Rust harness, and the same rows are loaded into a native Exasol table as the oracle.

## Design Decisions

### [1] Every pruning translator shares one exactness rule set

- **Decision:** The Iceberg and Delta pruning translators combine translated nodes under `AND`, `OR`, `NOT`, and `BETWEEN` through one format-neutral rule set that records whether each translated node is exact, so a `NOT` negates only an exact child. Each format supplies only its leaf translation and its junction and negation constructors.
- **Alternatives:** A shared predicate tree that each format lowers from (rejected: it must unify two predicate types, two literal vocabularies, and two statistics contracts, which this fix does not need). Two copies of the rules (rejected: the copies drifted, and the Iceberg copy negated a widened child (#466)).
- **Rationale:** `/speq:adr-rules` rule-2 criteria 1 and 2: the rule set governs two format readers, and it binds every future pruning translator. `speq decision-log show` holds `sound-partial-iceberg-predicate-translation-strict-or-not-handling`. That ADR states the AND, OR, and NOT-of-untranslatable rules for Iceberg alone and leaves NOT over a partly translated child undefined, which is the gap #466 fell through. This entry supersedes it. `delta-pruning-never-construct-false-predicate` stays in force, because the shared folds never build an empty junction. `delta-kernel-prunes-adapter-only-translates` and `new-adapter-iceberg-predicate-rs-module-iceberg-rust-types-stay-out-of-vs-expression` are unaffected.
- **Consequences:**
  - A new format translator inherits the `NOT` rule by supplying leaves and constructors.
  - Direct-storage partition pruning keeps its own three-valued evaluation, because it evaluates partition values instead of translating to a library predicate (decision [4]).
- **Supersedes:** sound-partial-iceberg-predicate-translation-strict-or-not-handling
- **Architecture:** no change: the rule set lives inside the vs-adapter and format-readers components and changes no component, boundary, interface, or data flow
- **Promotes to ADR:** yes

### [2] Exactness is defined against SQL three-valued logic, and a NOT over a partly translated child gives up its constraint

- **Decision:** A translated node is exact when it is true on exactly the rows where the user predicate is true and false on exactly the rows where it is false. On a NULL row it may be either. A leaf comparison, `IN`, `IS NULL`, and `IS NOT NULL` counts as exact. Whether every literal converts exactly is out of scope (decision [3]). `AND` that dropped or holds a partly translated child is partly translated. `OR` is exact only when every branch is exact. `NOT` of an exact child is exact. `NOT` of anything else imposes no constraint.
- **Alternatives:** Translate each node into a pair of predicates, one that keeps every matching row and one that keeps only matching rows, and let `NOT` swap them; equivalently, push `NOT` down to the leaves before translating (rejected for this fix: it would also prune `NOT (k < 30 OR name LIKE 'x%')` by `k >= 30`, but it doubles the per-node state, makes the soundness argument depend on how each library treats NULL in a negated leaf, and changes more than the interview agreed. A workload that needs that pruning can request it).
- **Rationale:** Under this definition `AND`, `OR`, and `NOT` of exact children are exact, so the rule composes. A `NOT` over a partly translated child would be false on rows that the child wrongly kept, and those rows can satisfy the `NOT`. That is the #466 defect.
- **Promotes to ADR:** no

### [3] Iceberg leaf-literal conversion exactness is out of scope

- **Decision:** This plan does not change how the Iceberg translator converts a literal. Rounding a `float` literal to `f32` and truncating a `timestamp` or `timestamptz` literal to microseconds is a separate defect, tracked as a follow-up `(#TBD)`. The `pushdown-file-pruning` delta records it as a scoped exception, as AGENTS.md requires for a known deviation from the Iceberg spec that a plan does not fix.
- **Alternatives:** Fix the literal conversion in this plan (rejected by review: it is a different issue from #466, whose defect is the negation of a partly translated `AND`, `OR`, or `BETWEEN`).
- **Rationale:** The rule set in [2] treats every translated leaf as exact. A leaf whose literal was rounded or truncated is not exact, so a `NOT` over it can still skip a file that holds a matching row. The follow-up owns that case. No E2E case uses a `float`, `double`, or timestamp literal as a pruning leaf, so the case table does not depend on it.
- **Promotes to ADR:** no

### [4] Audit of the sibling translators: direct storage is sound, and Delta already marks a partial BETWEEN

- **Decision:** No code change to the direct-storage partition predicate or to the Delta leaf translation. The Delta translator moves onto the shared rule set from [1] without a behavior change.
- **Alternatives:** Move the direct-storage partition predicate onto the shared rule set (rejected: it evaluates the predicate itself against constant partition values, and its reachable-truth-value model is already sound and more precise than exactness tracking).
- **Rationale:**
  - `partition_predicate.rs` computes, per file, which SQL truth values each node can reach. An opaque node reaches TRUE and FALSE. `AND`, `OR`, and `NOT` combine the reachable sets monotonically, and `NOT` swaps TRUE and FALSE. Each node's set is therefore a superset of the truth values its rows take, so a file is pruned only when no row can be TRUE. `NOT (p = 'a' AND name LIKE 'x%')` keeps every file. Its `BETWEEN` and `IN` need every literal to convert, or they are opaque. `direct-storage/direct-storage-hive-partitioning` already states this three-valued rule, so no spec delta is needed. The E2E case table proves it live.
  - Delta's `translate_between` builds its bounds through `fold_and`, which marks the result inexact when a bound drops, and `delta_predicate_tests.rs` pins it with `not_over_a_between_that_dropped_a_bound_returns_none`. Only the Iceberg `translate_between` lacks the mark.
  - Delta protocol check: the protocol's per-file statistics rules, quoted in the `delta/delta-file-pruning` Background, are unaffected, because no Delta behavior changes.
- **Promotes to ADR:** no

### [5] The shared rule set is a private module of the adapter, beside the Iceberg translator

- **Decision:** The shared module is `crates/lakehouse-engine/src/adapter/pruning_exactness.rs`, declared as a private `mod` in `adapter/mod.rs`, with `pub(super)` items. It owns the walk over `predicate_and`, `predicate_or`, and `predicate_not`, the exactness bookkeeping, and the conjunction and disjunction folds that leaf translators reuse for `BETWEEN` and the Delta `IN` list. Each format implements a small trait with its n-ary `AND`, n-ary `OR`, and negation constructors. The fold hands the constructor a first element plus the rest, so an empty junction is unrepresentable.
- **Alternatives:** Place it in `adapter/pushdown/format/` beside `filter_json.rs` (rejected: `format` is private to `pushdown`, so `adapter::iceberg_predicate` could reach it only through a re-export on the frozen pushdown façade, which needs a `pushdown/pushdown-module-structure` delta). Move `iceberg_predicate.rs` into `format/` as well (rejected: it changes import paths in two test modules and a public module path, unrelated to the fix). Binary `and`/`or` constructors (rejected: the Delta translator builds flat n-ary junctions, and its tests pin that shape).
- **Rationale:** A private module of `adapter` is visible to `adapter::iceberg_predicate` and to `adapter::pushdown::format::delta_predicate` without touching either façade. Design check against `/speq:design-philosophy`: one sentence covers it ("decides how AND, OR, and NOT combine translated pruning nodes so the result never skips a file with a matching row"). It is deeper than its interface: the formats see one walk function, two folds, and a three-method trait, while the exactness bookkeeping and the empty-junction guard stay inside. It depends only on the filter JSON, and the format modules depend on it.
- **Promotes to ADR:** no

### [6] One fixture, one case table, a native oracle with file labels, and expected kept files per format

- **Decision:** A new binary `e2e_pruning_test.rs` provisions one Iceberg table and one direct-storage directory holding the same rows, plus a native Exasol oracle table with an extra `FILE_LABEL` column naming the data file each row lives in. Each fixture file's path carries its label. Both writers use one set of Parquet writer properties that gives several row groups per file and several data pages per row group, with a column index and an offset index. A fixture-shape test reads the written footers and asserts that shape. One case table drives two test functions, one per format. Each case lists the SQL predicate, where the predicate is applied (scan or outer `WHERE`), and, per format, an expected label set only when the predicate translates fully for that format. For each case and format, the test asserts:
  - the rows of `SELECT ID ... ORDER BY ID` equal the oracle's;
  - Sound: the labels in `EXPLAIN VIRTUAL` include `SELECT DISTINCT FILE_LABEL FROM <oracle> WHERE <predicate>`;
  - Effective, only for a case with an expected set: the labels in `EXPLAIN VIRTUAL` equal the expected labels;
  - the plan names the scan UDF exactly when it names a label, so an empty expected set means the empty-result route;
  - when the plan names a label, the placement marker matches: `"filter":"` for the scan, or the `LHS_T0` wrapper without `"filter":"` for the outer `WHERE`.
  The #466 rows also run `SELECT COUNT(*)`.
- **Alternatives:** A Rust evaluator of each predicate over the fixture rows to find the files with a matching row (rejected: it would reimplement SQL three-valued logic in the test. The oracle table answers with Exasol's own semantics). Expected sets computed by a Rust model of the Iceberg evaluators (rejected: a second copy of the evaluator semantics in test code. Hand-listed labels per case are explicit and short). An expected set for every case, including partly translated ones (rejected by review: it pins how much a partly translated predicate prunes, which is not part of the contract, so a later, more precise translation would fail the test without a defect). Multi-row-group files alone for level 3 (rejected by review: several row groups do not imply several pages, so page pruning would go unproved). A per-row-group or per-page assertion from scan telemetry (not applicable: `scan-runtime/scan-execution-telemetry` reports no row-group or page counts, so the fixture proves level 3 through the row assertion).
- **Rationale:** The issue's five filter levels are each checked: partition and statistics pruning by Sound and, for fully translated predicates, Effective; row-group and page pruning by rows from a row group and a page whose bounds would drop a matching row under a wrong evaluation, with the fixture-shape test proving those row groups and pages exist; the row filter and the declined-filter wrapper by the placement marker plus rows. The oracle is the seeded source data (`testing.md` § Correctness oracles). The expected sets follow iceberg-rust 0.10's evaluators, read in its source: `ExpressionEvaluator::not_eq` and `not_in` return true on a NULL partition value, and `InclusiveMetricsEvaluator::not_eq` and `not_in` always answer "might match". An exact `NOT` whose negation is an `OR` of a partition clause and a statistics clause, such as `NOT (K < 30 AND P = 'a')`, therefore keeps every file, and the case table records that.
- **Promotes to ADR:** no

### [7] The repro runs before any code change, on the existing typed fixture

- **Decision:** Task 1.1 reproduces #466 on the local Docker stack with `scripts/capture-pushdown-payload.sh` and the seeded `typed_distinct_probe` table: `SELECT COUNT(*) FROM {table} WHERE NOT (ID < 1000 AND C_VARCHAR LIKE 'x%')` against `SELECT COUNT(*) FROM {table} WHERE C_VARCHAR NOT LIKE 'x%'`. Every `ID` is below 1000, so both predicates select the same rows.
- **Alternatives:** Reproduce on the new fixture only (rejected: AGENTS.md requires the repro before the fix, and the fixture is new code that could itself be wrong).
- **Rationale:** The second query has no `NOT` over a conjunction, so the unfixed adapter answers it correctly. A first count of 0 with the typed empty result in `EXPLAIN VIRTUAL`, and a non-zero second count, reproduce #466 on main.
- **Promotes to ADR:** no

### [8] The Delta Background is rewritten to current-state behavior

- **Decision:** The `delta/delta-file-pruning` delta replaces the Background with 13 bullets. It keeps every rule a scenario depends on and every quote from the Delta protocol and the Iceberg spec. It drops the issue history, the stats-option change history, library API notes (`negate()`, `Expression::column`), and module placement.
- **Alternatives:** Copy the Background verbatim and edit only the two bullets this plan falsifies (rejected: the spec-conciseness rule asks a plan that touches a Background to collapse history and implementation detail, and a verbatim copy would carry about 140 lines of it forward).
- **Rationale:** The shared rule set makes two bullets false: "share NO mechanism" and "a third independent walker". A `DELTA:CHANGED` Background replaces the whole section, so the rewrite happens anyway. The reasons behind the dropped bullets stay in the ADRs that `add-delta-file-pruning` recorded (`specs/_decision/072-add-delta-file-pruning.md`) and in git history.
- **Promotes to ADR:** no

### [9] A writable Delta fixture for the case table is a follow-up

- **Decision:** The case table runs on Iceberg and direct storage only. Delta is covered by unit tests of the shared rule set and of the Delta translator. A writable Delta fixture that runs the same case table is a follow-up `(#TBD)`.
- **Alternatives:** Write the rows as a Delta table now, for example with a Spark fixture script beside the Iceberg ones (rejected in the interview: the vendored Delta fixtures are never mutated, and a new Delta write route is a separate piece of work).
- **Rationale:** The Delta translator's `NOT` behavior does not change, and the Delta library's own pruning is its contract (ADR `delta-kernel-prunes-adapter-only-translates`).
- **Promotes to ADR:** no

## Review Findings

### [1] [plan-review] Expected file sets were pinned for partly translated predicates

- **Finding:** The case table pinned an exact file set for every case, including partly translated predicates such as `NOT (K < 30 AND S LIKE 'x%')` ("all six"). For a partly translated predicate the contract is only that every file with a match survives, so the pin would fail a later, more precise translation that is still correct.
- **Direction change:** An expected label set, and the Effective assertion, now apply only to a predicate that translates fully for that format. A partly translated predicate is held to Sound only. The case table, the per-case assertions, and the worked anchors in task 1.6 and decision [6] changed accordingly. The `pushdown-file-pruning` delta adds a Background bullet that limits the contract for a partly translated predicate to soundness, rewords the `NOT` bullet to say that pruning never negates a partly translated child instead of pinning that such a `NOT` imposes no constraint, renames the first new scenario to "A NOT over a partly translated predicate keeps every file with a matching row" with a THEN that states only soundness, narrows the THEN of "A NOT over a fully translated predicate still prunes" to its example, and holds only fully translated predicates to an exact file set in the case-table scenario. The `delta-file-pruning` scenario "A NOT negates only a fully translated child" now forbids handing the Delta library the negated child instead of requiring no predicate at all.
- **Promotes to ADR:** no

### [2] [plan-review] Several row groups did not guarantee several pages

- **Finding:** The fixture set only a maximum row-group size, so page-index pruning had no file with several pages per row group and was not proved.
- **Direction change:** Task 1.2 defines one set of writer properties for both the iceberg-rust seed writer and `raw_parquet.rs`: four rows per row group, two rows per data page, a write batch size of two so the page limit is enforced, dictionary encoding off, and page-level statistics, so each file carries a column index and an offset index. It replaces the deprecated `set_max_row_group_size` with `set_max_row_group_row_count`. The suggested `K` values place a page whose bounds lie below 30 inside a row group that spans 30, holding a row the #466 case matches. A wrong evaluation on that page's min/max would therefore drop a matching row, and the row assertion fails. A new fixture-shape test (task 1.4) reads each written footer with the page index loaded and asserts the row groups, the indexes, the page count, and those page bounds.
- **Promotes to ADR:** no

### [3] [plan-review] The leaf-literal rule left out timezone-aware timestamps

- **Finding:** The exactness fix and its tests covered `Timestamp` and `TimestampNs` only, while `Timestamptz` and `TimestamptzNs` truncate to microseconds the same way.
- **Status:** Superseded by Review Finding [8]. Leaf-literal exactness left this plan's scope, so the changes below no longer apply.
- **Direction change:** Decision [3], the scenario "A literal the column type cannot hold exactly imposes no constraint", the leaf-literal Background bullet, and task 1.8 now cover all four timestamp types. Task 1.8 adds `timestamptz_literal_finer_than_the_column_unit_returns_none` and `nanosecond_timestamptz_literal_translates_at_nanosecond_precision`. Because the `timestamptz` arm matches only the node name that #242 tracks, these tests use a synthesized request.
- **Promotes to ADR:** no

### [4] [plan-review] The coverage step referenced an untracked script

- **Finding:** The former verify task (1.8) and the checklist ran a coverage script from a gitignored local path that neither the repository nor CI holds.
- **Direction change:** Task 1.11 and the checklist use tracked tooling only and drop the 90 percent figure. CI has no changed-line gate of its own: the `unit-tests` job builds `lcov-unit.info` with `cargo llvm-cov --workspace --lcov --output-path lcov-unit.info` (the same command as `make coverage`), and the `sonar` job uploads it to SonarQube Cloud, whose quality gate judges coverage on new code. Finding [6] refines this to the Sonar gate alone.
- **Promotes to ADR:** no

### [5] [plan-review] One E2E task bundled five steps

- **Finding:** Advisory. The former task 1.3 combined fixture setup with both virtual schemas, plan parsing from `EXPLAIN VIRTUAL`, the oracle comparison, the case matrix, and the run before the fix.
- **Direction change:** The work is now sequential tasks 1.2 to 1.6: the fixture definition (1.2), the two writers and the oracle (1.3), the test binary setup with the fixture-shape test (1.4), the plan-reading and oracle helpers (1.5), and the case matrix with its run before the fix (1.6). The case table still fails before the translator tasks 1.7 to 1.9 run.
- **Promotes to ADR:** no

### [6] [plan-review] Whole-file coverage was offered as evidence of changed-line coverage

- **Finding:** Task 1.11 used `cargo llvm-cov report --summary-only` to show that every changed line was covered, but that report gives whole-file percentages and cannot show whether a specific changed line ran.
- **Direction change:** The plan relies only on SonarQube Cloud's new-code quality gate for changed-line coverage, because the repository holds no tracked changed-line check and this lighter option adds no tool. Task 1.11 states that the PR's `Sonar Analysis` check must pass, fed by the `unit-tests` job's `lcov-unit.info`, and that a coverage failure there is answered with unit tests for the lines Sonar reports. The `summary-only` step and its claim are removed from task 1.11 and from the checklist, whose coverage row names the Sonar gate as the only changed-line gate.
- **Promotes to ADR:** no

### [7] [plan-review] The timestamptz tests did not exercise the offset branch

- **Finding:** The `timestamptz` tests used literals without an offset, so the branch of `has_tz_offset` and `parse_timestamptz` that handles `+HH:MM` and `Z` never ran, for either unit.
- **Status:** Superseded by Review Finding [8]. Leaf-literal exactness left this plan's scope, so the changes below no longer apply.
- **Direction change:** Task 1.8 now lists six timestamp tests with their inputs and expected values. Each `timestamptz` case runs its literal in the `+02:30` and the `Z` form, for `Timestamptz` and for `TimestamptzNs`. `2024-03-01 12:30:00.123456+02:30` and `2024-03-01 10:00:00.123456Z` give the microsecond value `1709287200123456`. The nanosecond forms with nine digits give `1709287200123456789`. Seven digits against the microsecond column, and ten against the nanosecond column, give `None` in both offset forms. Task 1.8 also states that the offset never counts as fraction digits. Decision [3] states the offset rule and records that chrono 0.4.45 accepts a space separator, `Z`, and more than nine fraction digits. The literal scenario of the `pushdown-file-pruning` delta adds the offset forms to its GIVEN and an `AND` step for the UTC instant.
- **Promotes to ADR:** no

### [8] [plan-review] Leaf-literal exactness is a separate issue

- **Finding:** The Iceberg leaf-literal work (rounding a `float` literal to `f32`, and truncating `Timestamp`, `TimestampNs`, `Timestamptz`, and `TimestamptzNs` literals to microseconds) is a different defect from #466 and does not belong in this plan.
- **Direction change:** Decision [3] now records the literal conversion as out of scope and tracked as a follow-up `(#TBD)`. Review Findings [3] and [7] are superseded. Plan changes: the Context bullet on `f32` rounding and microsecond truncation, the Impact bullet on literals the column type cannot hold, the `literal_to_datum` and timestamp parts of task 1.8, its eight literal unit tests, the extra `test_schema` fields, and the matching verification rows are removed, and Open Questions lists the follow-up. Task 1.6 states that no case uses a `float`, `double`, or timestamp literal as a pruning leaf, so `X` and `TS` appear only inside `ABS(X) > 0.1` and `SECOND(TS, 3) > 1`. Spec changes: the `pushdown-file-pruning` delta drops the leaf-literal Background bullet and the scenario "A literal the column type cannot hold exactly imposes no constraint", including its `+02:30` and `Z` examples, and adds one scoped-exception bullet naming the deviation and the follow-up, as AGENTS.md requires for an Iceberg spec deviation a plan does not fix. The `delta-file-pruning` delta held no literal-conversion text. The `BETWEEN` dropped-bound change and the shared exactness module stay.
- **Promotes to ADR:** no
