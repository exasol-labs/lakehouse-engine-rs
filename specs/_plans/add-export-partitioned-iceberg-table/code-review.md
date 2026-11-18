# Code Review Findings: add-export-partitioned-iceberg-table

## Summary
- Files reviewed: 29
- Total findings: 7

Evidence: `cargo test --workspace --lib export` passes (68 tests, exit 0). The E2E suite was not rerun for this review.

The known deviation (`partition_key` survives as `null_key` in `fanout.rs`, with its test moved along, because iceberg-rust's `RecordBatchPartitionSplitter` refuses a spec with no non-void field) is justified by the comment in `PartitionBuffer::new`. It is not a finding.

## Findings

### docs/export.md

#### [DOC_CODE_MISMATCH] The write-out policy is described as "most buffered rows", but the code picks by bytes
- Location: "Partitioned tables" section, last paragraph ("writes the partition with the most buffered rows into a data file"). The same wording is in the spec delta `specs/_plans/add-export-partitioned-iceberg-table/export/export-partitioned-iceberg-table/spec.md` § Background ("it writes the partition with the most buffered rows").
- Issue: `PartitionBuffer::push` (`fanout.rs`) passes `get_array_memory_size()` totals to `write_out`, which writes the partition with the most Arrow bytes, not the most rows. A partition of a few wide rows goes out before one of many narrow rows. The docs also say "When the buffered rows reach the budget", but the code writes out only when the total exceeds the budget (`total > budget`).
- Fix: In `docs/export.md`, replace "When the buffered rows reach the budget, the writer writes the partition with the most buffered rows into a data file." with "When the buffered rows exceed the budget, the writer writes the partition that buffers the most bytes into a data file." In the spec delta's § Background bullet, replace "When the budget is reached, it writes the partition with the most buffered rows into a data file" with "When the buffered rows exceed the budget, it writes the partition that buffers the most bytes into a data file".

### crates/lakehouse-engine/src/export/fanout.rs

#### [DUPLICATED_DECISION] The per-instance overhead is restated instead of reusing the scan's constant
- Location: line 12, `const OVERHEAD_BYTES: u64 = 200 * MIB;`, whose doc reads "as the scan's memory pool assumes"
- Issue: The scan's 200 MB overhead is owned by `crate::scan::spec::DEFAULT_INSTANCE_OVERHEAD_MB` (`pub(crate)`), and `scan/object_store.rs` multiplies it by `1024 * 1024`. The export copies the number, so the two drift apart when one of them changes, even though the doc comment says they must agree.
- Fix: Replace `const OVERHEAD_BYTES: u64 = 200 * MIB;` and its doc line with `const OVERHEAD_BYTES: u64 = crate::scan::spec::DEFAULT_INSTANCE_OVERHEAD_MB * MIB;`, and keep a one-line doc: `/// The engine's own share of the per-instance limit, the same as the scan's.`

### crates/lakehouse-engine/src/export/iceberg_rest.rs

#### [STALE_COMMENT] `refuse_unappendable` doc still says it refuses only a `void` field named like a column
- Location: lines 118-119, the doc comment of `refuse_unappendable`
- Issue: The filter now refuses every field named like a column unless the field is that column's own identity. That includes a `bucket` field, or an identity field of another column, under a column's name. The doc comment says "a `void` field named like a column", so it understates the rule the code applies.
- Fix: Replace the doc comment's second clause with: "and a partition field named like a column unless it is that column's identity, which iceberg-rust cannot commit to."

### crates/lakehouse-engine/src/export/iceberg_rest_tests.rs

#### [UNTESTED_BRANCH] An identity field named like a column but sourced from another column is not tested
- Location: `a_void_partition_field_named_like_a_column_is_refused_naming_it`
- Issue: The new filter has two refusing conditions: `field.transform != Transform::Identity` and `column.id != field.source_id`. The test covers `void` (refused) and an identity of its own column (accepted). No case makes the second condition decide, so if `|| column.id != field.source_id` were dropped, no test would fail.
- Fix: In that test, make the `spec` closure also take the first field's `source-id`. Then add `assert!(refused_with("identity", 2).is_some(), "an identity field of another column named like c0");`, where the field `c0` with transform `identity` and `source-id` 2 is refused. Keep the existing two assertions with `source-id` 1.

### crates/lakehouse-engine/src/export/partition_by_tests.rs

#### [OPAQUE_TEST_EXPECTATION] The expected types of each transform are built by slicing indices
- Location: lines 175-191, `each_transform_takes_only_its_source_types`
- Issue: The expected sets come from `types[..2].iter().chain(&types[4..6])`, `types[6..]`, `types[7..]`, and the nested `numbers_and_text.iter().copied().chain(&types[6..])`. A reader must count positions in `types` to see which types `bucket` or `hour` takes, and reordering `types` silently changes every expectation. `specs/testing.md` asks that a test read as the behavior it proves.
- Fix: Replace `numbers_and_text` and the `cases` array with literal lists: `(Transform::Identity, types.to_vec())`, `(Transform::Bucket(4), vec!["DECIMAL(18,0)", "DECIMAL(12,2)", "VARCHAR(10) UTF8", "CHAR(3) UTF8", "DATE", "TIMESTAMP(3)"])`, `(Transform::Truncate(4), vec!["DECIMAL(18,0)", "DECIMAL(12,2)", "VARCHAR(10) UTF8", "CHAR(3) UTF8"])`, `(Transform::Year | Month | Day, vec!["DATE", "TIMESTAMP(3)"])` (three entries), and `(Transform::Hour, vec!["TIMESTAMP(3)"])`. Type `cases` as `[(Transform, Vec<&str>); 7]` and collect `accepted` as `Vec<&str>`.

### crates/lakehouse-e2e/tests/e2e_local/export_partition.rs

#### [MAGIC_NUMBER] Fixture positions and counts are hard-coded instead of read from the ground-truth constants
- Location: line 407 (`[0, 6, 13]` in `test_partition_by_creates_partitioned_table`) and line 382 (`assert_eq!(exported_rows(...), 4)` in `test_append_writes_under_current_spec`)
- Issue: `[0, 6, 13]` are the positions of `s_id`, `s_b_bucket`, and `s_t_trunc` in `BY_VALUE_SPEC`, and `4` is the size of `EVOLVED_CURRENT_RIDS`. Both duplicate facts that `export_partition_fixtures.rs` owns. A fixture edit that reorders the spec or adds rows breaks these tests with a misleading misplacement or count error, not at the fixture.
- Fix: At line 407, replace `[0, 6, 13].iter().map(|&i| value[i].clone())` with `["s_id", "s_b_bucket", "s_t_trunc"].iter().map(|name| value[BY_VALUE_SPEC.iter().position(|(field, _)| field == name).unwrap()].clone())`. At line 382, replace `4` with `EVOLVED_CURRENT_RIDS.clone().count() as i64`.

#### [UNCLEAR_COMMENT] The many-partitions sizing comment ends in an unexplained number
- Location: lines 595-596, the comment in `test_many_partitions_export_completes`
- Issue: "while the image's 1 GiB temp volume holds 160" gives no unit and no subject, so a reader cannot tell what 160 measures or why it matters for the test's sizing.
- Fix: Replace the comment with: `// Narrow numeric rows: Arrow holds about 1,100 bytes per row of 40 DECIMAL columns, over` / `// 600 MB in all, above the 512 MiB budget, while Exasol stores them in about 160 MB of the` / `// image's 1 GiB temp volume.` If the measured figure is not 160 MB, use the measured one.
