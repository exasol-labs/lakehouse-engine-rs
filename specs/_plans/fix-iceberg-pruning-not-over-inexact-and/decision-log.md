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

- **Decision:** A translated node is exact when it is true on exactly the rows where the user predicate is true and false on exactly the rows where it is false. On a NULL row it may be either. A leaf comparison, `IN`, `IS NULL`, and `IS NOT NULL` with an exactly converted literal is exact. `AND` that dropped or holds a partly translated child is partly translated. `OR` is exact only when every branch is exact. `NOT` of an exact child is exact. `NOT` of anything else imposes no constraint.
- **Alternatives:** Translate each node into a pair of predicates, one that keeps every matching row and one that keeps only matching rows, and let `NOT` swap them; equivalently, push `NOT` down to the leaves before translating (rejected for this fix: it would also prune `NOT (k < 30 OR name LIKE 'x%')` by `k >= 30`, but it doubles the per-node state, makes the soundness argument depend on how each library treats NULL in a negated leaf, and changes more than the interview agreed. A workload that needs that pruning can request it).
- **Rationale:** Under this definition `AND`, `OR`, and `NOT` of exact children are exact, so the rule composes. A `NOT` over a partly translated child would be false on rows that the child wrongly kept, and those rows can satisfy the `NOT`. That is the #466 defect.
- **Promotes to ADR:** no

### [3] An Iceberg leaf translates only when its literal converts without rounding

- **Decision:** The Iceberg leaf conversion drops a `float` literal that `f32` cannot hold exactly and a timestamp literal whose fraction is finer than the column's unit, instead of rounding or truncating it. A nanosecond column converts the literal at nanosecond precision.
- **Alternatives:** Keep the rounding and mark such a leaf partly translated (rejected: a rounded literal is not even sound at the top level. `x < 0.7` becomes `x < 0.699999988` in `f32`, which skips a file whose only value is `f32(0.7)`. Whether that row matches depends on the precision the row filter compares in, DataFusion's coercion in the scan or Exasol's `DOUBLE` on the declined-filter path, and a pruning rule must not depend on that). Leave the leaf conversion out of this plan and record a scoped exception (rejected: AGENTS.md requires a plan that touches pruning to fix each deviation from the Iceberg spec's inclusive projection, and the fix is a few lines).
- **Rationale:** The exactness rule in [2] assumes exact leaves, so a rounded leaf under `NOT` reintroduces #466: `NOT (x = 0.7)` becomes `x != f32(0.7)` and skips a file whose only value is `f32(0.7)`, although those rows satisfy the `NOT` under a `DOUBLE` comparison. iceberg-rust 0.10's `Datum::timestamp_from_str` truncates sub-microsecond digits (`timestamp_micros()`). The Delta translator already drops an inexact `float` literal (`double_literal_not_exactly_representable_as_float_yields_no_scalar`), and the direct-storage partition predicate drops any literal it cannot convert exactly (`file-planning/partition-predicate-declared-types`).
- **Consequences:** An Iceberg `float` comparison against a decimal literal such as `0.7` prunes no file. The E2E case table has no `float` column, because the leaf rule is pure literal conversion that unit tests prove (`specs/testing.md` § Coverage rule).
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

- **Decision:** A new binary `e2e_pruning_test.rs` provisions one Iceberg table and one direct-storage directory holding the same rows, plus a native Exasol oracle table with an extra `FILE_LABEL` column naming the data file each row lives in. Each fixture file's path carries its label. One case table drives two test functions, one per format. Each case lists the SQL predicate, the labels of the files Iceberg keeps, the labels of the files direct storage keeps, and where the predicate is applied (scan or outer `WHERE`). For each case and format, the test asserts:
  - the rows of `SELECT ID ... ORDER BY ID` equal the oracle's;
  - Sound: the labels in `EXPLAIN VIRTUAL` include `SELECT DISTINCT FILE_LABEL FROM <oracle> WHERE <predicate>`;
  - Effective: the labels in `EXPLAIN VIRTUAL` equal the case's expected labels;
  - an empty expected set means the plan names no scan UDF, and a non-empty one means it does;
  - for a non-empty set, the placement marker matches: `"filter":"` for the scan, or the `LHS_T0` wrapper without `"filter":"` for the outer `WHERE`.
  The #466 rows also run `SELECT COUNT(*)`.
- **Alternatives:** A Rust evaluator of each predicate over the fixture rows to find the files with a matching row (rejected: it would reimplement SQL three-valued logic in the test. The oracle table answers with Exasol's own semantics). Expected sets computed by a Rust model of the Iceberg evaluators (rejected: a second copy of the evaluator semantics in test code. Hand-listed labels per case are explicit and short). Effective checks only for exact cases (rejected: listing the expected set for every case also pins the documented precision of partly translated cases, at no extra cost). A per-row-group assertion from scan telemetry (not applicable: `scan-runtime/scan-execution-telemetry` reports no row-group counts, so the multi-row-group files prove level 3 through the row assertion alone).
- **Rationale:** The issue's five filter levels are each checked: partition and statistics pruning by Sound and Effective, row-group pruning by rows from multi-row-group files whose per-group bounds would drop a matching row under a wrong negation, the row filter and the declined-filter wrapper by the placement marker plus rows. The oracle is the seeded source data (`testing.md` § Correctness oracles). The expected sets follow iceberg-rust 0.10's evaluators, read in its source: `ExpressionEvaluator::not_eq` and `not_in` return true on a NULL partition value, and `InclusiveMetricsEvaluator::not_eq` and `not_in` always answer "might match". An exact `NOT` whose negation is an `OR` of a partition clause and a statistics clause, such as `NOT (K < 30 AND P = 'a')`, therefore keeps every file, and the case table records that.
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
