# Decisions: fix-iceberg-identity-partition-absent-column

## ADR: A file's identity partition value wins over its column, ahead of the initial-default

**ID:** a-files-identity-partition-value-wins-over-its-column-ahead-of-the-initial-default
**Plan:** `fix-iceberg-identity-partition-absent-column`
**Status:** Accepted
**Supersedes:** implement-iceberg-column-projection-rule-3-for-all-absent-fields

### Context

The Iceberg table spec applies its "Column Projection" rule (1) to field ids "not present in a data file" when the partition value is present in the `partition` struct. A metadata-only import registers data files without the partition column, so reading NULL for them returns wrong rows.

### Decision

A file whose own spec has an `identity` field on a column reads that partition value for the column, through the partition-value mechanism Delta and Hive tables already use, where the file's value wins over the data file. Other files resolve the column through binding, then the `initial-default`, then NULL for a nullable field or a clean error for a required one. A null partition value is present and reads as NULL. `write-default` is never read.

This departs from the letter of rule (1) only for a file that holds the column under its field id with values other than its partition value, which the spec forbids: "Partition values for a data file must be the same for all records stored in the data file". It is a scoped exception.

### Options Considered

| Option | Verdict |
|--------|---------|
| Iceberg Java precedence, where the partition constant wins even when the file holds the column | Chosen: it reuses the existing partition-value scan, and a conforming file returns the same rows as under the spec text |
| Spec-text precedence, where a column held under its field id wins, through a per-file fallback expression | Rejected: a second partition mechanism inside the column-binding adapter, for files the spec forbids |
| iceberg-rust precedence, where name mapping binds a column before rule (1) applies | Rejected: the spec lists rule (1) ahead of rule (2), and a name-mapped column carries no field id |
| iceberg-rust null handling, where a null partition value falls through to the `initial-default` | Rejected: Iceberg Java treats a null partition value as present |
