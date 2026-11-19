# Code Review Findings: fix-export-parquet-write-properties

## Summary
- Files reviewed: 10
- Total findings: 4

## Findings

### crates/lakehouse-engine/src/export/iceberg_sink.rs

#### [UNREFUSED_UNUSABLE_PROPERTY] A content-defined chunking mask out of range fails at the first write, not at open
- Location: `refused`, the content-defined chunking branch
- Issue: `refused` covers the two parquet-rs panics (`min_chunk_size == 0`, `max_chunk_size <= min_chunk_size`), but parquet-rs 59.3.2 also rejects, with an error rather than a panic, a mask whose bit count `floor(log2(((min + max) / 2 - min) / 8)) - norm_level` falls outside `1..=63` (`ContentDefinedChunker::calculate_mask`, reached from `ArrowWriter::try_new`). Examples: `content-defined-chunking.enabled = true` with `norm-level = 40`, or `min-chunk-size = 1` and `max-chunk-size = 2`. `open` accepts these, and the export fails at the first write with `writing the data files of table ... to storage failed`, which names no property. That breaks the spec's "error that names the table, the property, and its value", and `docs/export.md` says the content-defined chunking keys follow the table.
- Fix: In `refused`, after the `min`/`max` checks, read `table.cdc_norm_level()` (return `None` on `Err`). Compute `target = ((min + max) / 2 - min) / 8` and `bits = (if target > 0 { target.ilog2() as i32 } else { 0 }) - norm`. If `bits` is not in `1..=63`, return `Some((TableProperties::PROPERTY_PARQUET_CDC_NORM_LEVEL, "the chunk sizes and the normalization level leave no valid chunking mask"))`. Add one case to `an_unusable_write_property_is_named_with_its_value` in `iceberg_sink_tests.rs` with `enabled = true` and `norm-level = 40`, expecting `write.parquet.content-defined-chunking.norm-level = '40'`.

#### [STRING_MATCHED_ERROR] The failing key is found by matching iceberg-rust's error text, not by the accessors the plan names
- Location: `data_file_settings`, the `map_err` closure and `WRITE_KEYS`
- Issue: Plan step 2 says the sink "calls each `TableProperties` accessor that `from_table_properties` uses, in turn, and names the key of the first that fails". The code instead searches the error chain for `Invalid value for {key}:` and `compression codec`, and names `write.parquet.compression-level` for any other text. This ties the key to iceberg-rust's message wording. Any unrelated `from_table_properties` error, including one a future iceberg-rust adds, is reported as `write.parquet.compression-level = ''`.
- Fix: In `data_file_settings`, replace `WRITE_KEYS` and the string search with an ordered array of `(key, fn(&TableProperties) -> iceberg::Result<()>)` pairs, one per accessor `from_table_properties` and `write_target_file_size_bytes` call: `parquet_compression_codec`, `parquet_row_group_size_bytes`, `parquet_page_size_bytes`, `parquet_page_row_limit`, `parquet_dict_size_bytes`, `write_target_file_size_bytes`, `cdc_enabled`, and, only when `cdc_enabled()` is `Ok(true)`, `cdc_min_chunk_size`, `cdc_max_chunk_size`, and `cdc_norm_level`. Each closure is `|t| t.<accessor>().map(drop)`. For `parquet_compression_codec`, name `PROPERTY_PARQUET_COMPRESSION_LEVEL` when the error chain mentions `compression-level` (both keys parse in that one accessor), else `PROPERTY_PARQUET_COMPRESSION_CODEC`. Name the first key whose accessor fails. If every accessor succeeds and `from_table_properties` still fails, name `PROPERTY_PARQUET_COMPRESSION_LEVEL`, the only remaining cause (the level range check in `parquet_compression`). Keep the existing unit cases passing unchanged.

#### [MISLEADING_COMMENT] `WRITE_KEYS` doc contradicts its contents
- Location: the doc comment on `WRITE_KEYS`
- Issue: The comment says "other than the content-defined chunking switch", but the array includes `TableProperties::PROPERTY_PARQUET_CDC_ENABLED`.
- Fix: Delete `WRITE_KEYS` together with its comment when you apply [STRING_MATCHED_ERROR]. If you keep it, change the comment to `/// The keys \`data_file_settings\` reads.`

### crates/lakehouse-engine/src/export/fanout.rs

#### [PLAN_DEVIATION] `PartitionBuffer::new` keeps a comment for a branch that no longer exists and lacks the planned `debug_assert`
- Location: `PartitionBuffer::new`, the line `// iceberg-rust's splitter refuses a spec without a field other than \`void\`.`
- Issue: Plan step 3 says `PartitionBuffer::new` refuses an unpartitioned spec by `debug_assert`. The code has no assertion, and the comment explained the removed `Split::Unpartitioned` branch. It now stands over an unconditional call.
- Fix: In `PartitionBuffer::new`, replace that comment with `debug_assert!(!spec.is_unpartitioned(), "the sink streams an unpartitioned table's rows");`.
