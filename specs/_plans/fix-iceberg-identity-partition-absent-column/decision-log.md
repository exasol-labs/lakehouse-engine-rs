# Decision Log: fix-iceberg-identity-partition-absent-column

## Interview

- Q: Reproduce before planning or as first implement task? → A: Before planning (done, see Repro)
- Q: Identity-partition column physically present in the file: which value wins? → A: File column wins (spec text, rule 1 only for absent fields)
- Q: Null in the manifest partition struct for an absent column: present? → A: Present, emit NULL (planner confirms against Iceberg Java)
- Q: Split into several plans? → A: No, one plan (issue #99)

## Repro

Run against the local Docker Exasol on 2026-10-28, before any fix. Fixture: Spark writes a Hive-partitioned Parquet directory (`region=eu`, `region=us`, files without the `region` column), `add_files` registers it into an Iceberg table partitioned by `identity(region)`, then Spark appends two native rows (`apac`, `eu`). Spark's own read returns eu 3, us 2, apac 1. The Spark SQL used is in the Appendix.

| Query through the VS | Engine result | Correct |
|---|---|---|
| `SELECT id, region ... ORDER BY id` | ids 1-4: `region` NULL; id 5 `apac`; id 6 `eu` | 1,2 `eu`; 3,4 `us`; 5 `apac`; 6 `eu` |
| `GROUP BY region` | apac 1, eu 1, NULL 4 | apac 1, eu 3, us 2 |
| `WHERE region = 'eu'` | id 6 only | ids 1, 2, 6 |

No error is raised. The bug is confirmed as a silent wrong result.

### Appendix: repro Spark SQL

```sql
CREATE TABLE rest_catalog.e2e_lakehouse.ident_part_absent (id INT, amount INT, region STRING)
USING iceberg PARTITIONED BY (region) TBLPROPERTIES ('format-version'='2');
CREATE TABLE spark_catalog.default.ident_part_src (id INT, amount INT, region STRING)
USING parquet PARTITIONED BY (region) LOCATION 's3://warehouse/e2e_lakehouse/ident_part_src';
INSERT INTO spark_catalog.default.ident_part_src VALUES (1,10,'eu'),(2,20,'eu'),(3,30,'us'),(4,40,'us');
CALL rest_catalog.system.add_files(table => 'e2e_lakehouse.ident_part_absent',
  source_table => '`parquet`.`s3://warehouse/e2e_lakehouse/ident_part_src`');
INSERT INTO rest_catalog.e2e_lakehouse.ident_part_absent VALUES (5,50,'apac'),(6,60,'eu');
```

It runs like the INT96 fixture: the same `SPARK_CONF`, plus `hadoop-aws` and `spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem`.

## Design Decisions

### [1] An identity partition value fills only an absent field, ahead of the initial-default

- **Decision:** The scan resolves a field absent from a data file in the Iceberg "Column Projection" order: the `identity` partition value of the file's own spec, then the `initial-default` for a required or nullable field, then NULL for a nullable field or a clean error for a required one. A field the file holds always reads from the file. A null partition value is present and reads as NULL. `write-default` is never read.
- **Rationale:** The spec applies rule (1) to "field ids which are not present in a data file" when "the partition value is present in the `partition` struct". For a file whose physical values equal its partition value, the rejected alternatives return the same rows. Criterion 4: it rejects Iceberg Java's precedence, and a reader who compares results with Spark needs the reason. ADR digest: `implement-iceberg-column-projection-rule-3-for-all-absent-fields` states rule (3) and the required-absent error with no rule (1), so this decision supersedes it.
- **Alternatives:** Iceberg Java's precedence, where the constant wins even when the file holds the column (`ParquetValueReaders.java`, `idToConstant.containsKey(id)` checked before the file reader): rejected, the spec text limits rule (1) to absent fields. iceberg-rust's null handling, where a null value falls through to the `initial-default` (`record_batch_transformer.rs`, `constants_map`): rejected, Iceberg Java treats null as present ("containsKey is used because the constant may be null"), as the interview decided.
- **Supersedes:** implement-iceberg-column-projection-rule-3-for-all-absent-fields
- **Architecture:** no change: the rule is internal to datafusion-scan and format-readers
- **Promotes to ADR:** yes

### [2] Per-file identity values travel in `partition_values` and reach the field-id adapter as DataFusion partition literals

- **Decision:** The Iceberg reader writes each file's `identity` values into `FileEntry.partition_values` and leaves `partition_columns` empty. The scan registers each `partition_values` key that `partition_columns` does not list as a hidden DataFusion partition column, and projects the column through one expression over the file column and the hidden column. The field-id adapter, which knows per file whether the field is bound, reduces that expression to the file column or the per-file literal.
- **Rationale:** This design has the fewest moving parts: it reuses the wire field, the `partition_scalar` conversion, DataFusion's per-file partition literal, and the adapter's per-file binding, and adds one scan expression. DataFusion 55 substitutes partition literals before it calls `PhysicalExprAdapterFactory::create(logical, physical)`, which receives no file identity (`datafusion-datasource-parquet` `opener/mod.rs:776-801`, `datafusion-physical-expr-adapter` `schema_rewriter.rs:175`). The value must therefore arrive as a partition literal, and the presence check must stay in the adapter. DataFusion 55.2 skips the adapter for a file whose physical schema equals the logical file schema when no predicate exists (`opener/mod.rs:1009-1017`), so the expression's own evaluation, which returns its file-column child, covers that file. This conforms to `partition-columns-via-datafusion-native-mechanism`, because the value still enters through DataFusion's native partition mechanism.
- **Alternatives:** Listing the `identity` columns in `partition_columns` reuses the Delta path unchanged, but the log value then always wins and the file column is never read. A new `FileEntry` field duplicates the per-file map, against the AGENTS.md rule to widen an existing field first. One sub-scan per distinct partition-value tuple, joined by a union, lets each adapter factory hold constants, but plan size and concurrently opened partitions grow with the number of distinct partition values in a shard.
- **Consequences:** The fallback expression type owns the hidden column's three-state encoding, both building the per-file value and reducing itself. The adapter supplies only whether the file binds the field, so the scan modules share no encoding.
- **Promotes to ADR:** no

## Review Findings
