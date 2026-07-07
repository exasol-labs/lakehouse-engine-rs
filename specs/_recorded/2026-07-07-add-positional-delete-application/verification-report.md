# Verification Report: add-positional-delete-application

## Bottom Line

**PASS.** All implementation tasks (Groups A–D) complete; all verification gates green. The engine
now applies Iceberg merge-on-read Parquet positional deletes on read (both `file` and `partition`
granularity), keeping DataFusion's `ParquetSource` (projection/filter/LIMIT pushdown, row-group/page
pruning, streaming, and the `FieldIdExprAdapter` all preserved), and fails loud at plan time on
every delete mechanism it cannot apply (equality, Puffin/v3 deletion vector, ORC/Avro). Ready for
`/speq:record`.

## Verification Gates

| Gate | Command | Result |
|------|---------|--------|
| Build | `make cross-musl-udf-build` | ✅ exit 0 — `.so` built in `rust:1.94-bookworm`, fingerprint matched, loaded by Exasol |
| Host tests | `cargo test` | ✅ 403 lib + all no-container integration, 0 failures |
| Lint | `cargo clippy --all-targets` | ✅ 0 warnings |
| Format | `cargo fmt --check` | ✅ no changes |
| E2E | `make test-e2e` | ✅ 67 passed, 0 failed (e2e_positional_deletes 11, e2e_scan 43, e2e_capability 7, e2e_count_distinct 6) |

## Scenario Coverage Audit

Every scenario in the plan's `## Verification > Scenario Coverage` table has a corresponding passing
test:

- **Scan-level (no container)** — `tests/scan_positional_deletes.rs`: file granularity, partition
  granularity (file_path filter), multi-delete-file union, fully-deleted file, compose with
  pushdown/pruning, backstop rejection, delete-free non-regression, vended credentials — 8/8 pass.
- **Plan-shape GATE** — `tests/scan_plan_shape.rs::raw_plan_lean_and_prunes_with_access_plan`:
  asserts the lean single-partition shape (no repartition/coalesce) AND row-group pruning WITH a base
  `ParquetAccessPlan` attached (8 of 10 row groups pruned by statistics). **Gate PASSED → unified
  provider path confirmed safe; no conditional `ListingTable` fallback needed.**
- **Reconstitution / no-HEAD** — `tests/scan_two_arg.rs`, `tests/scan_no_head_test.rs`, and
  `scan/spec.rs` unit tests: delete entries reconstitute; legacy `(path,size)` entries reconstitute
  with empty deletes; no HEAD for data or delete files; footer parsed once via a single range GET.
- **Adapter unit** — `adapter/pushdown.rs` `#[cfg(test)]`: deletes preserved into scan spec (file +
  partition); relative/absolute path encoding; content type carried; fail-loud on
  equality/DV/ORC/Avro at plan time.
- **E2E matrix** — `tests/e2e_positional_deletes_test.rs`: file & partition granularity post-delete
  correctness, multi-partition-spanning delete, fan-out invariance (deterministic same-shard vs
  split-shard placement via parallelism factor), deletes × projection/filter/LIMIT, deletes ×
  single/grouped aggregation, unsupported-delete fail-loud, delete-free non-regression, and
  suite-fails-when-stack-unavailable — 11/11 pass against a live Exasol + Spark + MinIO + Iceberg-REST
  stack.

## Code Review

Independent review found **no correctness defects**. The fail-loud gate is airtight (runs before file
planning; total over the `(content, format)` matrix; manifest-level detection catches Puffin DVs that
`plan_files` would drop). Boundary discipline preserved (no Arrow across the `.so` seam; streaming
intact). Vendored `build_deletes_row_selection` is faithfully attributed and pinned to upstream's own
oracle test. One low-severity **efficiency** finding — a per-row downcast + allocation in the
delete-scan loop — was **applied** (hoisted to a per-batch downcast, borrowing each `file_path` cell
in place). One readability observation (the `Data → EqualityDeletes` sentinel) was left as-is per the
reviewer's recommendation (not worth churning the wire enum).

## Known Limitations (documented, tracked upstream)

- **Delete-file pruning breadth** — iceberg-rust 0.10.0-rc.2's `DeleteFileIndex` associates every
  partition-scoped position-delete file with every data file in the partition (it does not yet gate
  by `referenced_data_file`; upstream PR apache/iceberg-rust#2532, pre-work for #340). This is a
  pruning/observability gap, **not a correctness bug** — `positional_deletes.rs` filters applied
  deletes by `file_path` per data file at scan time, so results are exact. The
  `fixture_spark_file_granularity_delete_table` shape assertion is honestly relaxed to the achievable
  invariant with a drop condition to tighten once #2532 ships.
- **Equality deletes / v3 deletion vectors / ORC / Avro** — out of scope by design; rejected loud at
  plan time. Deferred under #11. The E2E fail-loud path is exercised against a real Spark-authored
  format-v3 Puffin deletion-vector table (`mor_dv_unsupported`).
- **Native position-delete writer** — fixtures are authored via Apache Spark because iceberg-rust has
  no native position-delete writer yet (apache/iceberg-rust#340); drop condition noted on the Spark
  fixtures.

## Follow-up (out of scope, flagged for a separate issue)

A pre-existing scan-pushdown defect unrelated to this feature: a pushed-down `WHERE` filter combined
with a bare `LIMIT` (no `ORDER BY`) returns a shifted row window. Reproduced with zero positional
deletes and identically on the pre-existing `ListingTable` path — predates this work. No existing test
covers bare filter+limit-without-ORDER-BY. Worth a follow-up issue.
