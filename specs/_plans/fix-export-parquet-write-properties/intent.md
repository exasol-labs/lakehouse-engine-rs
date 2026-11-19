# Intent: fix-export-parquet-write-properties

## Interview

- Q: An append target has an unusable write property (codec `lzo`, a non-numeric size)? → A: Fail the export, naming the table, the property, and its value.
- Q: Cap `write.parquet.row-group-size-bytes` by the memory limit? → A: No, follow the property; the docs state the memory cost.
- Q: An append target has `write.format.default = 'orc'` or `'avro'`? → A: Write Parquet anyway; the docs state it.
- Q: Where do the new scenarios go? → A: Into the existing export features, no new feature.

## Directions

- [D1] An unusable write property fails each writer when it opens the table, before it writes any data, not in the export hook before any source row is read; the hook checks only whether the table exists, and the column check already runs at the same point.
- [D2] The E2E tests set write properties through the REST catalog's `set-properties` update on an exported table, not through a new Spark fixture; any catalog client sets them the same way, and the Spark fixture cannot be rerun per test.
- [D3] A row group may exceed `write.parquet.row-group-size-bytes` by up to a tenth, asserted on the footer's compressed size: parquet-rs estimates how many rows fit, and a measured 1 MiB row group came out 2.7% over.
- [D4] The partition buffer budget stays as it is and does not count the row group in progress; before this change row groups had no byte cap at all, so writer memory does not grow.
- [D5] "Holds no rows beyond the row group in progress" is a design rule in the architecture Constraints, not a scenario: no E2E observable separates streaming from buffering, and a fake-server unit test is not allowed. The rolling at `write.target-file-size-bytes` is the tested scenario.
