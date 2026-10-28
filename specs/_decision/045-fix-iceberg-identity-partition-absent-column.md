# Decisions: fix-iceberg-identity-partition-absent-column

## ADR: An identity partition value fills only an absent field, ahead of the initial-default

**ID:** an-identity-partition-value-fills-only-an-absent-field-ahead-of-the-initial-default
**Plan:** `fix-iceberg-identity-partition-absent-column`
**Status:** Accepted
**Supersedes:** implement-iceberg-column-projection-rule-3-for-all-absent-fields

### Context

The Iceberg table spec applies its "Column Projection" rule (1) to field ids "not present in a data file" when the partition value is present in the `partition` struct. A metadata-only import registers data files without the partition column, so reading NULL for them returns wrong rows.

### Decision

The scan resolves a field absent from a data file in the Iceberg "Column Projection" order: the `identity` partition value of the file's own spec, then the `initial-default`, then NULL for a nullable field or a clean error for a required one. A field the file holds always reads from the file. A null partition value is present and reads as NULL. `write-default` is never read.

### Options Considered

| Option | Verdict |
|--------|---------|
| Iceberg Java precedence, where the partition constant wins even when the file holds the column | Rejected: the spec text limits rule (1) to absent fields |
| iceberg-rust null handling, where a null partition value falls through to the `initial-default` | Rejected: Iceberg Java treats a null partition value as present |
