# Decisions: add-column-source-notes

## ADR: A table's declared schema is decided at CREATE and persisted per column in adapterNotes

**ID:** table-declared-schema-persisted-per-column-in-adapter-notes
**Plan:** add-column-source-notes
**Status:** Accepted

### Context

Each format reader derives a table's schema again at every pushdown, and the derived shape can differ from the one Exasol declared at CREATE. The Exasol `dataType` loses the nested structure, the binding key, and the source type.

### Decision

`createVirtualSchema`, `refresh`, and `setProperties` record each virtual column's declared source shape in that column's `adapterNotes`. Every pushdown takes the table's logical schema, partition columns, and refused columns from those notes, and no format reader derives a schema at pushdown.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep per-reader derivation at pushdown | Rejected: each reader re-derives the shape differently per query |
| Persist the shape in schema-level `adapterNotes` | Rejected: the value grows with every column of every table and is capped at 2,000,000 characters for the whole schema |
| Re-derive the shape from the Exasol `dataType` at pushdown | Rejected: the Exasol type loses the nested structure, the binding key, and the source type |

### Consequences

- The declared schema goes stale exactly when the Exasol `dataType` does, and `REFRESH` heals both.
- A source column added after the last `REFRESH` stays invisible, a widened type fails at scan time, and `MERGE_SCHEMA` and `HIVE_PARTITIONING` take effect at CREATE, REFRESH, or SET only.
- Pushdown loads per query only what is not the schema: snapshot, files, deletes, name mapping, credentials, and partitions.

## ADR: The catalog stays the Unity Parquet schema authority, and the declaration classifies `type_json` at CREATE

**ID:** unity-parquet-catalog-schema-classified-at-create
**Plan:** add-column-source-notes
**Status:** Accepted

### Context

The earlier decision has the Unity Parquet reader classify each column's `type_json` at pushdown. That is per-reader derivation, which the per-column declaration replaces.

### Decision

A Unity Parquet table's logical schema comes from the catalog's declared columns, and its partition columns come from each column's `partition_index`. `createVirtualSchema` classifies each column's `type_json` and records the result in the column notes. The reader takes the declaration from the notes, the parquet-directory seam supplies only the file list, and no footer is read.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the classification in the reader at pushdown | Rejected: it is the per-reader derivation the column notes remove |
