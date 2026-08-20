# Feature: Pushdown File Resolution

Resolves a table's identity and current file state exactly once per pushdown, before any
SQL is built. Recovers the target table from the Exasol involved-table name via the
persisted `TABLE_MAP` and hands it to the format reader that owns its table format, which
returns the active data-file list, each file's byte size, the logical schema, the table
root, and — where the format has them — each file's associated delete references, all at one
resolve-once seam so the scan UDF never discovers files, delete files, or sizes itself. That
orchestration has no Delta counterpart feature because it never needed one: a Delta table
reaches it by the same route an Iceberg one does
(`vs-adapter/pushdown-format-neutral-resolution`). This feature owns the ICEBERG reader's
half of the seam — the multi-level `TableIdent` build, the Iceberg snapshot and
`current_schema()` read that produce the field-id-carrying logical schema, and the
merge-on-read positional-delete resolution; the Delta reader's half is owned by
`vs-adapter/delta-table-planning`. A `loadTable` response that carries no table `location`
is rejected here, before the vended/static storage split, so every path depending on a table
root — including each join side — fails identically rather than resolving an empty root. See
`vs-adapter/pushdown-planning` for how the resolved table identity, file list, byte sizes,
delete-file references, and logical schema feed the scan-driving SQL.

## Background

* **This delta corrects the feature's own framing and is issue #324. It changes no behavior.** The recorded Purpose described the resolve-once orchestration as an Iceberg-only path. It never was one after `vs-adapter/pushdown-format-neutral-resolution` shipped: the `TABLE_MAP` lookup, the resolve-once seam, and the `ScanSpec` build are one code path both formats reach. What is Iceberg-specific is the READER behind that seam, and this feature keeps owning it.
* **The distinction matters because the two halves have different owners.** A reader looking for the Delta counterpart of this feature will not find one and should not: the counterpart of the ICEBERG READER is `vs-adapter/delta-table-planning`, and the counterpart of the ORCHESTRATION is the orchestration itself.
* **The table root carried into the common scan spec is a neutral field.** Both format readers populate it, so the clause naming it is renamed here for the same reason it is renamed in `vs-adapter/pushdown-planning-file-encoding` and `datafusion-scan/scan-execution`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Pushdown resolves the file list once and builds a scan-driving query

* *GIVEN* a virtual schema over a namespace whose tables are backed by MinIO
* *AND* a query that projects a subset of columns from one of those tables
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL determine the target Iceberg table from the schema-metadata mapping, resolve that table's Iceberg snapshot, data-file list, and each file's byte size exactly once, and at that same seam extract the table's current Iceberg schema (from `current_schema()`) into a logical schema carrying, per column, its `field_id`, current name, Arrow type, and nullability
* *AND* the adapter SHALL return a JSON response of type `pushdown` containing SQL that invokes the `LAKEHOUSE_SCAN` SCALAR EMIT UDF, carrying the logical schema AND the table root in the shard-invariant common spec spliced ONCE as the scalar scan's first-argument literal, and the resolved data-file list flowed through the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor as the per-shard argument, where each per-shard entry carries the file path together with its resolved byte size
* *AND* the outer scalar scan select MUST NOT be wrapped in a `SELECT * FROM (...)` materialization boundary
* *AND* the adapter MUST NOT require the scan UDF to discover files itself, and MUST NOT require the scan UDF to re-fetch any file's size
* *AND* the resolve-once orchestration this scenario describes — `TABLE_MAP` lookup, one resolve, one `ScanSpec` build — SHALL be reached identically by every table format, with the format reader supplying the file list, byte sizes, logical schema, and table root (`vs-adapter/pushdown-format-neutral-resolution`); only the reader behind the seam is format-specific
<!-- /DELTA:CHANGED -->
