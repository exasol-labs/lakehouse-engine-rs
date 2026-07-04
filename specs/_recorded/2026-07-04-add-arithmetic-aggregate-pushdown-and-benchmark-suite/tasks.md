# Tasks: add-arithmetic-aggregate-pushdown-and-benchmark-suite

## Group A — Two-column arithmetic aggregate pushdown (item 4)
- [x] 1.1 Live root-cause capture: EXPLAIN VIRTUAL for NQ1 shape against test1; record exact `function_scalar` names for `*`,`+`,`-`,`/` and SUM's declared DECIMAL product type. Write into decision-log.
- [x] 1.2 Advertise FN_ADD/FN_SUB/FN_MULT/FN_FLOAT_DIV in capabilities.rs; extend capability tests.
- [x] 1.3 Reconcile vs-expression arithmetic operator-name matching with verified live names; add rendering unit tests. [expert]
- [x] 1.4 Verify/adjust expression-argument SUM type derivation for two-column DECIMAL product in pushdown.rs; add plan-shape coverage. [expert]
- [x] 1.5 End-to-end integration test: NQ1-shape query over MinIO-backed Iceberg table, merged result equals single-node eval; untranslatable-argument fallback still works.
- [x] 1.6 Fix regression: `scan/mod.rs::build_scan_sql` (raw-scan path) treats every `spec.projection` entry as a bare column identifier and quotes the whole string, which breaks when an entry is a rendered scalar-expression fragment (e.g. `("SCORE" * 2)`) rather than a plain identifier — newly exposed by advertising arithmetic capabilities broadly (task 1.2), causing `e2e_capability_test.rs::e2e_selectlist_expression_pushdown` to fail with `No field named "(""SCORE"" * 2)"`. Fix so expression-fragment projection entries are selected verbatim (like `spec.filter` and `agg_arg_sql` already do), not re-quoted as identifiers. Confirm the full `make test-e2e` suite passes afterward. [expert]

## Group B — Five new benchmark queries (item 5)
- [x] 2.1 Add NQ1-NQ5 to bench/run.sh (Exasol dialect) + pushdown_check for NQ1 (aggregates/arg_expr) and NQ2 (LIKE/IN).
- [x] 2.2 Add NQ1-NQ5 to bench/trino_compare.sh and bench/athena_compare.sh (Presto dialect).
- [x] 2.3 Add NQ1-NQ5 to deploy/scripts/spark_queries.py (Spark SQL dialect).
- [x] 2.4 Update bench/README.md sync note; run ./bench/run.sh selftest.

## Group C — Parallelism-factor sweep (item 3, evidence-gated)
- [x] 3.1 Add BENCH_PARALLELISM_FACTOR sweep (8/16/24) to bench/sweep.sh measured on Q2/Q3/Q5/Q9b against test1.
- [x] 3.2 Analyze evidence, record finding in decision-log either way; apply default change only if it's a real, repeatable, non-regressing improvement.

## Phase 4: Code Review
- [x] 4.1 Review all changed files (code-reviewer agent) — 3 LOW nits, none actioned (reviewer: "not worth churning")

## Phase 5: Verification
- [x] 5.1 Build: make cross-musl-udf-build (already current from task 1.6's make test-e2e run)
- [x] 5.2 Test: cargo test --workspace — 441 passed, 2 ignored, 0 failed
- [x] 5.3 Lint: cargo clippy --workspace --all-targets — no issues
- [x] 5.4 Format: cargo fmt --check — clean
- [x] 5.5 Scenario coverage audit — all plan scenarios covered (see verification-report.md)
- [x] 5.6 Manual verification (bench runs against test1) — NQ1-NQ5 wired + selftest OK; full competitive bench run is a follow-up operational step (task 4/5 of the outer orchestration), not blocking plan completion

## Phase 6: Verification Report
- [x] 6.1 Generate verification-report.md
