# Tasks: add-iceberg-predicate-pruning

## Group A — translator module (+ soundness unit tests)
- [x] 1.1 Create `adapter/iceberg_predicate.rs`; register `mod iceberg_predicate;`
- [x] 1.2 Column resolution: Exasol uppercase name → Iceberg NestedField (case-insensitive → exact field name + primitive type) [expert]
- [x] 1.3 literal→Datum keyed on field primitive type; None on mismatch/unparsable [expert]
- [x] 1.4 `to_iceberg_predicate(filter_json, schema) -> Option<Predicate>` with sound AND/OR/NOT/leaf semantics [expert]
- [x] 3.1 Leaf translations (=, <, <=, IN, IS NULL, IS NOT NULL, BETWEEN); operand order
- [x] 3.2 AND with one untranslatable child → only translatable conjunct [expert]
- [x] 3.3 OR with one untranslatable child → None [expert]
- [x] 3.4 NOT of untranslatable → None; NOT of translatable → negated [expert]
- [x] 3.5 Unknown column / type mismatch → None (no panic)

## Group B — partitioned seed helper (independent)
- [x] 5.1 Partitioned-table seed helper in tests/common/seed.rs (non-empty UnboundPartitionSpec + PartitionKey per file)

## Group C — wiring (depends A)
- [x] 2.1 `filter_json: Option<&Json>` param on `resolve_file_list` (signed + unsigned)
- [x] 2.2 `filter_json` param on `plan_files_from_table`; read current_schema, apply `scan.with_filter(pred)` when Some
- [x] 2.3 `handle_pushdown` passes raw `pushdownRequest.filter` into `resolve_file_list`; ScanSpec.filter unchanged
- [x] 4.1 Unit: LIKE-only filter → valid ScanSpec.filter + None Iceberg predicate
- [x] pushdown_carries_filter_and_iceberg_prune (named scenario test)

## Group E — E2E (depends B + C)
- [x] 5.2 E2E `e2e_partition_filter_prunes_and_returns_correct_rows` [expert]
- [x] 5.3 Assert resolved file count with predicate < snapshot file count (+ `e2e_range_filter_prunes_by_file_bounds`)

## Review + Verify
- [x] R.1 Code review
- [x] V.1 cargo test + clippy + fmt
- [x] V.2 make test-e2e
