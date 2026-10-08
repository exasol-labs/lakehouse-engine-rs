# Decision Log: fix-iceberg-pruning-not-over-inexact-and

## Interview

**Q:** How should Iceberg and Delta share the exactness rules?
**A:** They do not. After review, the Iceberg translator gets the rule locally and the Delta translator is untouched (decision [1]).

**Q:** Which formats must the E2E case table run on?
**A:** Iceberg and Hive direct storage. The Iceberg fixture is written through `seed.rs`, and the same rows are written as raw Parquet through `raw_parquet.rs`. Delta joins in a follow-up (decision [6]).

**Q:** How is the Iceberg fixture written?
**A:** With the iceberg-rust writer in `tests/common/seed.rs`: partitioned, several data files per partition, files with several row groups, and a file whose column holds only NULLs. The ground truth lives in the Rust harness, and the same rows are loaded into a native Exasol table as the oracle.

## Design Decisions

### [1] The Iceberg translator negates only a fully translated child, as the Delta translator does

- **Decision:** `iceberg_predicate.rs` tracks whether each translated node is exact, meaning true on exactly the rows where the user predicate is true and false on exactly the rows where it is false (on a NULL row it may be either). A leaf comparison, `IN`, `IS NULL`, and `IS NOT NULL` is exact. `AND` that dropped or holds an inexact child is inexact. `OR` is exact only when every branch is exact. `BETWEEN` with a dropped bound is inexact. `NOT` negates an exact child and gives no constraint otherwise.
- **Alternatives:** A shared exactness module that both translators use, with a Delta refactor, a Delta spec rewrite, and a superseding ADR (rejected by review: the defect is in Iceberg's own translation, Delta already handles it, the shared part is small, and the real differences between formats, literal conversion and statistics, stay per format). Translating each node into an over- and an under-approximation and letting `NOT` swap them (rejected: it would also prune `NOT (k < 30 OR name LIKE 'x%')`, but it doubles the per-node state and widens the change beyond the bug).
- **Rationale:** A `NOT` over a partly translated child is false on rows the child wrongly kept, and those rows can satisfy the `NOT`. That is #466. The fix applies the rule Delta already follows, so behavior stays consistent without a refactor. The ADR `sound-partial-iceberg-predicate-translation-strict-or-not-handling` states the `AND`, `OR`, and `NOT` rules for an untranslatable child and does not contradict this rule, so it stays as is.
- **Promotes to ADR:** no

### [2] Iceberg leaf-literal conversion exactness is out of scope

- **Decision:** This plan does not change how the Iceberg translator converts a literal. Rounding a `float` literal to `f32` and truncating a `timestamp` or `timestamptz` literal to microseconds is a separate defect, tracked as a follow-up `(#TBD)`.
- **Alternatives:** Fix the literal conversion in this plan (rejected by review: it is a different issue from #466, whose defect is the negation of a partly translated `AND`, `OR`, or `BETWEEN`).
- **Rationale:** Decision [1] treats every translated leaf as exact. A leaf whose literal was rounded or truncated is not exact, so a `NOT` over it can still skip a file that holds a matching row. The follow-up owns that case. No E2E case uses a `float`, `double`, or timestamp literal as a pruning leaf, so the case table does not depend on it.
- **Promotes to ADR:** no

### [3] Audit of the sibling translators: direct storage and Delta are sound

- **Decision:** No change to the direct-storage partition predicate or to the Delta translator.
- **Alternatives:** Apply exactness tracking to the direct-storage partition predicate (rejected: it evaluates the predicate itself against constant partition values, and its reachable-truth-value model is already sound and more precise than exactness tracking).
- **Rationale:**
  - `partition_predicate.rs` computes, per file, which SQL truth values each node can reach. An opaque node reaches TRUE and FALSE. `AND`, `OR`, and `NOT` combine the reachable sets monotonically, and `NOT` swaps TRUE and FALSE. Each node's set is therefore a superset of the truth values its rows take, so a file is pruned only when no row can be TRUE. `NOT (p = 'a' AND name LIKE 'x%')` keeps every file. Its `BETWEEN` and `IN` need every literal to convert, or they are opaque. `direct-storage/direct-storage-hive-partitioning` already states this three-valued rule, so no spec delta is needed. The E2E case table proves it live.
  - Delta's `translate_between` builds its bounds through `fold_and`, which marks the result inexact when a bound drops, and `delta_predicate_tests.rs` pins it with `not_over_a_between_that_dropped_a_bound_returns_none`. Only the Iceberg `translate_between` lacks the mark, which decision [1] fixes.
  - Delta protocol check: no Delta behavior changes, so the per-file statistics rules quoted in the `delta/delta-file-pruning` Background are unaffected.
- **Promotes to ADR:** no

### [4] One fixture, one case table, a native oracle with file labels, and expected kept files per format

- **Decision:** A new binary `e2e_pruning_test.rs` provisions one Iceberg table and one direct-storage directory holding the same rows, plus a native Exasol oracle table with an extra `FILE_LABEL` column naming the data file each row lives in. Each fixture file's path carries its label. Both writers use one set of Parquet writer properties that gives several row groups per file and several data pages per row group, with a column index and an offset index. A fixture-shape test reads the written footers and asserts that shape. One case table, keyed by format, drives one test function per format. Each case lists the SQL predicate, where the predicate is applied (scan or outer `WHERE`), and, per format, an expected label set only when the predicate translates fully for that format. For each case and format, the test asserts:
  - the rows of `SELECT ID ... ORDER BY ID` equal the oracle's;
  - Sound: the labels in `EXPLAIN VIRTUAL` include `SELECT DISTINCT FILE_LABEL FROM <oracle> WHERE <predicate>`;
  - Effective, only for a case with an expected set: the labels in `EXPLAIN VIRTUAL` equal the expected labels;
  - the plan names the scan UDF exactly when it names a label, so an empty expected set means the empty-result route;
  - when the plan names a label, the placement marker matches: `"filter":"` for the scan, or the `LHS_T0` wrapper without `"filter":"` for the outer `WHERE`.
  The #466 rows also run `SELECT COUNT(*)`.
- **Alternatives:** A Rust evaluator of each predicate over the fixture rows to find the files with a matching row (rejected: it would reimplement SQL three-valued logic in the test. The oracle table answers with Exasol's own semantics). Expected sets computed by a Rust model of the Iceberg evaluators (rejected: a second copy of the evaluator semantics in test code. Hand-listed labels per case are explicit and short). An expected set for every case, including partly translated ones (rejected by review: it pins how much a partly translated predicate prunes, which is not part of the contract, so a later, more precise translation would fail the test without a defect). Multi-row-group files alone for level 3 (rejected by review: several row groups do not imply several pages, so page pruning would go unproved). A per-row-group or per-page assertion from scan telemetry (not applicable: `scan-runtime/scan-execution-telemetry` reports no row-group or page counts, so the fixture proves level 3 through the row assertion).
- **Rationale:** The issue's five filter levels are each checked: partition and statistics pruning by Sound and, for fully translated predicates, Effective; row-group and page pruning by rows from a row group and a page whose bounds would drop a matching row under a wrong evaluation, with the fixture-shape test proving those row groups and pages exist; the row filter and the declined-filter wrapper by the placement marker plus rows. The oracle is the seeded source data (`testing.md` § Correctness oracles). The expected sets follow iceberg-rust 0.10's evaluators, read in its source: `ExpressionEvaluator::not_eq` and `not_in` return true on a NULL partition value, and `InclusiveMetricsEvaluator::not_eq` and `not_in` always answer "might match". An exact `NOT` whose negation is an `OR` of a partition clause and a statistics clause, such as `NOT (K < 30 AND P = 'a')`, therefore keeps every file, and the case table records that.
- **Promotes to ADR:** no

### [5] The repro runs before any code change, on the existing typed fixture

- **Decision:** Task 1.1 reproduces #466 on the local Docker stack with `scripts/capture-pushdown-payload.sh` and the seeded `typed_distinct_probe` table: `SELECT COUNT(*) FROM {table} WHERE NOT (ID < 1000 AND C_VARCHAR LIKE 'x%')` against `SELECT COUNT(*) FROM {table} WHERE C_VARCHAR NOT LIKE 'x%'`. Every `ID` is below 1000, so both predicates select the same rows.
- **Alternatives:** Reproduce on the new fixture only (rejected: AGENTS.md requires the repro before the fix, and the fixture is new code that could itself be wrong).
- **Rationale:** The second query has no `NOT` over a conjunction, so the unfixed adapter answers it correctly. A first count of 0 with the typed empty result in `EXPLAIN VIRTUAL`, and a non-zero second count, reproduce #466 on main.
- **Promotes to ADR:** no

### [6] A writable Delta fixture for the case table is a follow-up

- **Decision:** The case table runs on Iceberg and direct storage in this plan and is built to become the general pruning regression suite. A writable Delta fixture that runs the same cases on all three formats is a follow-up `(#TBD)`, scoped in the plan's Open Questions.
- **Alternatives:** Write the rows as a Delta table now, for example with a Spark fixture script beside the Iceberg ones (rejected in the interview: the vendored Delta fixtures are never mutated, and a new Delta write route is a separate piece of work).
- **Rationale:** Delta's existing unit tests pin its `NOT` behavior until then, and the Delta library's own pruning is its contract (ADR `delta-kernel-prunes-adapter-only-translates`). The vendored Delta fixtures are never mutated, so live Delta cases need a new write route, which is separate work.
- **Promotes to ADR:** no

## Review Findings

### [1] [plan-review] Expected file sets were pinned for partly translated predicates

- **Finding:** The case table pinned an exact file set for partly translated predicates, so a later, more precise translation would fail the test without a defect.
- **Direction change:** An expected label set applies only to a predicate that translates fully for that format. A partly translated predicate is held to Sound only, and the `pushdown-file-pruning` delta limits its contract to soundness.
- **Promotes to ADR:** no

### [2] [plan-review] Several row groups did not guarantee several pages

- **Finding:** The fixture set only a maximum row-group size, so page-index pruning was not proved.
- **Direction change:** Task 1.2 defines one set of writer properties for both writers (four rows per row group, two per page, write batch size two, dictionary off, page-level statistics), and task 1.4 adds a fixture-shape test that reads each footer with the page index loaded.
- **Promotes to ADR:** no

### [3] [plan-review] Changed-line coverage relied on an untracked script

- **Finding:** The verify task used a gitignored script and whole-file percentages, which cannot show that a changed line ran.
- **Direction change:** The plan relies on SonarQube Cloud's new-code quality gate, fed by CI's `unit-tests` job, and answers a coverage failure there with unit tests for the lines Sonar reports.
- **Promotes to ADR:** no

### [4] [plan-review] Leaf-literal exactness is a separate issue

- **Finding:** Iceberg literal rounding and truncation is a different defect from #466.
- **Direction change:** Decision [2] records it as a follow-up `(#TBD)`, and task 1.6 keeps float and timestamp literals out of the case table.
- **Promotes to ADR:** no

### [5] [review #468] The change went beyond a bug fix

- **Finding:** The root cause is Iceberg's own translation, and Delta already negates only an exact child. The shared module, the Delta refactor, the Delta spec rewrite, and the ADR supersession change no behavior.
- **Direction change:** The plan fixes `iceberg_predicate.rs` locally (decision [1]) and drops the shared module, the Delta code and spec changes, and the ADR supersession. The `pushdown-file-pruning` spec change, including `BETWEEN` with a dropped bound, stays. The E2E case table stays and is keyed by format, and the writable Delta fixture is scoped as a follow-up (decision [6]).
- **Promotes to ADR:** no
