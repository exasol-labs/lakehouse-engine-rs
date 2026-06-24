# Verification Report: add-iceberg-predicate-pruning

## Bottom Line

**PASS.** The adapter now translates the WHERE predicate into a sound, pruning-only
`iceberg::expr::Predicate` applied at file-resolution time (both signed and unsigned paths), so
`plan_files` skips data files on partition values and per-file min/max bounds before S3 I/O.
DataFusion still applies the full `ScanSpec.filter` as the sole row-level correctness backstop.
All host unit tests, both E2E suites, clippy, and fmt are green.

## Evidence

### Automated checks (Verification > Checklist)

| Step | Command | Result |
|------|---------|--------|
| Build (UDF `.so`) | `make cross-musl-udf-build` (via `make test-e2e`) | Exit 0 — built in `rust:1.92-bookworm` |
| Unit tests | `cargo test -p lakehouse-engine --lib` | 218 passed, 0 failed |
| E2E | `make test-e2e` | `MAKE_EXIT=0`; `e2e_capability_test` 7/7, `e2e_scan_test` 27/27 |
| Lint | `cargo clippy -p lakehouse-engine --all-targets` | 0 warnings |
| Format | `cargo fmt --check` | No changes |

### Scenario coverage

| Scenario | Test | Status |
|----------|------|--------|
| Filter predicate pushed into the scan spec (+ Iceberg prune predicate) | `pushdown_carries_filter_and_iceberg_prune` | ✅ unit |
| Equality on a partition column prunes data files | `e2e_partition_filter_prunes_and_returns_correct_rows` + `e2e_range_filter_prunes_by_file_bounds` (file-count) | ✅ E2E |
| Range predicate prunes files via per-file min/max bounds | `e2e_range_filter_prunes_by_file_bounds` (id<=5 → 1 file) | ✅ E2E |
| Untranslatable conjunct disables pruning for that conjunct only | `and_with_untranslatable_child_keeps_translatable_conjunct` | ✅ unit |
| Untranslatable branch of an OR disables pruning entirely | `or_with_untranslatable_child_returns_none` | ✅ unit |
| End-to-end filtered query over a partitioned table returns correct rows | `e2e_partition_filter_prunes_and_returns_correct_rows` | ✅ E2E |

Supporting soundness unit tests: `not_of_untranslatable_returns_none`, `leaf_equal_translates`,
`between_desugars_to_range`, `unknown_column_returns_none`, `has_tz_offset_detects_explicit_zones`,
`like_filter_yields_df_string_and_no_iceberg_predicate`.

### The correctness invariant

The translator is **sound-not-complete**: AND drops untranslatable conjuncts (only widens the file
set), OR returns `None` if any branch is untranslatable, NOT of an untranslatable child returns
`None`, `predicate_notequal` and partial-`IN` are dropped, and type mismatches yield `None` (never a
coerced Datum). Verified by the code review and the unit suite. DataFusion remains the row-level
backstop, so pruning can never change the result set.

### Code review

Phase 4 review confirmed the soundness core (OR/AND/NOT/IN/operand-flip/casing/typing) correct — no
BLOCKER. Two SHOULD-FIX addressed: (1) `has_tz_offset` now detects negative UTC offsets (was
appending `+00:00` to `-05:00` timestamps and silently losing pruning) with a new unit test;
(2) the file-count test doc-comment was de-confused and the range assertion tightened to `== 1`.

## Deviations / notes

- **Seed bug found and fixed during E2E:** the partitioned `regions` seed initially built the
  `UnboundPartitionSpec` via `add_partition_field`, which leaves `field-id: null`; the REST catalog
  rejected `create_table` with HTTP 500 (`Cannot parse to an integer value: field-id`). Fixed by
  assigning an explicit partition field-id (1000, the Iceberg convention) via a
  `UnboundPartitionField` + `add_partition_fields`. Both pruning E2E tests pass.
- Purely additive: no dead code removed; the unfiltered `plan_files_from_table` call was
  parameterised, not removed; `ScanSpec.filter` (DataFusion path) is unchanged and coexists.
