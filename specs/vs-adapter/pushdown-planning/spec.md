# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it resolves
the Iceberg data-file list once, captures the requested projection, filter, LIMIT, and
any supported aggregate, extracts the table's current Iceberg schema for field-id-based
projection, and emits the SQL that drives the DataFusion scan. Cluster fan-out is
separated from the scan: a nested `LAKEHOUSE_DISTRIBUTE_FILES` LUA SET distributor
subquery (`GROUP BY shard_key`) spreads each shard's per-file list across nodes, and an
outer ungrouped `LAKEHOUSE_SCAN` SCALAR EMIT UDF scans each distributed file list
node-locally and streams the rows. The scan-driving SQL splices the shard-invariant parts
(projection, filter, LIMIT, logical schema, credentials, and the Iceberg table root) once
as the scalar scan UDF's first-argument common literal and flows each shard's per-file
subset through the distributor as the second argument. A single-shard plan short-circuits
the distributor and calls the scalar scan directly on the file-list literal. See
`vs-adapter/pushdown-planning-file-encoding` for the table-root-once and relative/absolute
path encoding rules. See `vs-adapter/pushdown-planning-nested-aggregate-fallback` for the
guard against composed requests (e.g. an outer aggregate over an inner grouped-aggregate
sub-select) that don't map onto the source table's own columns. This feature also extends
the resolve-once seam to associate each data file's positional-delete files and carry them
minimally in the per-shard argument. Single-group aggregate pushdown (capability
advertisement, partial-aggregate scan-spec translation, wrapper merge SQL, and AVG
sum/count decomposition) is covered separately in
`vs-adapter/pushdown-planning-single-group-agg`.

## Background

* The data-file list, each file's byte size (from the Iceberg manifest), and the current Iceberg schema are resolved exactly once per pushdown, in the planning layer; the scan UDF never discovers files itself.
* The logical schema carried into the common scan-spec argument identifies each column by its Iceberg field-id, current name, Arrow type, and nullability.
* The scan-driving SQL invokes the `LAKEHOUSE_SCAN` SCALAR EMIT UDF over a nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor subquery; the shard-invariant common spec (projection, filter, LIMIT, aggregates, group keys, logical schema, EMITS types, credentials, tuning knobs, and the Iceberg table root) is spliced once as the scalar scan's first argument and each shard's file subset flows through the distributor as the second argument.
* The outer scalar scan select is never wrapped in a `SELECT * FROM (...)` materialization boundary.
* Each per-shard file entry carries both the file path and its byte size, so the scan UDF never re-discovers a size the adapter already resolved.
* Credentials MUST NOT appear in any returned SQL string or error message, and MUST NOT be repeated per shard.
* The data-file list, each file's byte size, and each file's associated positional-delete files are resolved exactly once, at the same seam; the scan UDF never discovers files or delete files.
* Delete support keeps the wire surface minimal — per-file delete references only, with no serialized Iceberg schema and no bound predicate added to the spec.
* The `LAKEHOUSE_SCAN` and `LAKEHOUSE_DISTRIBUTE_FILES` UDF names in the scan-driving SQL are schema-qualified from the schema of the running adapter script, read from the UDF handshake via `ctx.script_schema()`; there is no VS property that supplies this schema. The scan and distributor scripts are co-deployed in the adapter script's schema, so this single source qualifies both.
* The common spec's `projection` field carries the pushed-down projected columns ONLY for the row-scan and join dispatch paths. An aggregate or GROUP BY request leaves `projection` empty, because the aggregate scan-dispatch path derives its physical projection from the `aggregates`/`group_keys` fields rather than from `projection` (see `vs-adapter/pushdown-planning-single-group-agg` and `vs-adapter/pushdown-planning-grouped-agg`).
* See `vs-adapter/pushdown-planning-single-group-agg` for single-group aggregate pushdown (capability advertisement, partial-aggregate translation, wrapper merge SQL, and AVG decomposition).

## Scenarios

### Scenario: Pushdown derives the scanned Iceberg table from the involved virtual table

* *GIVEN* a virtual schema created over a namespace containing multiple Iceberg tables, whose `adapterNotes` carry the `TABLE_MAP` recorded at create time
* *AND* a `pushdown` request whose `involvedTables[0].name` is the Exasol (uppercased, `__`-flattened) name of one of those tables
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL read `TABLE_MAP` back from `schemaMetadataInfo.adapterNotes` and look up the involved virtual table name to recover its original-cased fully-qualified Iceberg identifier
* *AND* the adapter SHALL resolve the data-file list and build the scan-driving SQL for exactly that one Iceberg table, carrying its identifier in the per-shard `CatalogProps.table`
* *AND* a `pushdown` request whose involved virtual table name is absent from `TABLE_MAP` SHALL fail with an error naming the unknown virtual table, never silently scanning a different or stale table

### Scenario: Pushdown resolves the file list once and builds a scan-driving query

* *GIVEN* a virtual schema over a namespace whose tables are backed by MinIO
* *AND* a query that projects a subset of columns from one of those tables
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL determine the target Iceberg table from the schema-metadata mapping, resolve that table's Iceberg snapshot, data-file list, and each file's byte size exactly once, and at that same seam extract the table's current Iceberg schema (from `current_schema()`) into a logical schema carrying, per column, its `field_id`, current name, Arrow type, and nullability
* *AND* the adapter SHALL return a JSON response of type `pushdown` containing SQL that invokes the `LAKEHOUSE_SCAN` SCALAR EMIT UDF, carrying the logical schema AND the Iceberg table root in the shard-invariant common spec spliced ONCE as the scalar scan's first-argument literal, and the resolved data-file list flowed through the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor as the per-shard argument, where each per-shard entry carries the file path together with its resolved byte size
* *AND* the outer scalar scan select MUST NOT be wrapped in a `SELECT * FROM (...)` materialization boundary
* *AND* the adapter MUST NOT require the scan UDF to discover files itself, and MUST NOT require the scan UDF to re-fetch any file's size

### Scenario: Projection is pushed into the scan-driving query

* *GIVEN* a row-scan or inner-join `pushdown` request that selects only some of the table's columns and carries NO aggregate and NO GROUP BY
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the generated scan-driving SQL SHALL carry only the projected columns to the UDF, in the shard-invariant common spec spliced once as the scalar scan UDF's first-argument literal shared by all shards
* *AND* the projected column names SHALL be the current Iceberg logical names carried in the common spec's logical schema, so the UDF's registered table exposes them and the field-id adapter maps each to the correct physical column per file
* *AND* the scalar scan UDF's declared EMITS column list SHALL match the projected items in order and type, named POSITIONALLY: a bare-column item SHALL keep its real (quoted) source-column name so an outer `ORDER BY` over a projected column still resolves, while an expression or literal item SHALL be named by a positional-unique synthetic EMITS identifier rather than its rendered SQL text, so two structurally identical expression or literal items never collapse into one column and never collide into a duplicate EMITS name Exasol rejects
* *AND* the guarantee in this scenario SHALL govern ONLY the row-scan and join paths; an aggregate or GROUP BY request instead leaves the `projection` field empty (see `vs-adapter/pushdown-planning-single-group-agg` and `vs-adapter/pushdown-planning-grouped-agg`), so an empty `projection` on an aggregate scan spec is expected, not a lost projection

### Scenario: Filter predicate is pushed into the scan spec

* *GIVEN* a query with a WHERE predicate over a supported column and operator
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL translate the predicate into the shard-invariant common spec passed to the UDF, omitting (never mistranslating) any node it cannot render
* *AND* the adapter SHALL ALSO translate the soundly-translatable conjuncts into an `iceberg::expr::Predicate` applied to the Iceberg table scan as a file-pruning filter, dropping any node it cannot translate soundly rather than skipping a file that could match
* *AND* the DataFusion scan SHALL always apply the full common-spec filter, so the Iceberg pruning filter only narrows which files are opened and never changes the result set

### Scenario: LIMIT is pushed into the scan spec

* *GIVEN* a query with a LIMIT clause and NO `order_by` that governs which rows are selected
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the shard-invariant common spec spliced into the scalar scan UDF SHALL carry the row limit
* *AND* because the common spec is shared by every shard, each row-scan shard invocation SHALL observe the same limit
* *AND* the generated SQL SHALL attach the `LIMIT` DIRECTLY to the outer ungrouped scalar scan select (over the distributor subquery, or the from-less single-shard select) as a correctness backstop, with no `SELECT * FROM (...)` wrapper
* *AND* when the request DOES carry an `order_by`, the per-shard row limit SHALL be governed by ordered top-N (pushed only alongside the matching per-shard `ORDER BY`), never as a bare per-shard `LIMIT` ahead of a global sort

### Scenario: Pushdown resolves multi-level namespace identifiers into the iceberg TableIdent

* *GIVEN* a `TABLE_MAP` entry whose value is a multi-level Iceberg identifier such as `prod.finance.orders`
* *WHEN* the adapter resolves that identifier to load the table from the catalog
* *THEN* the adapter SHALL split the identifier into all namespace segments and the trailing table name, building the iceberg `TableIdent` from a multi-segment `NamespaceIdent` rather than treating only the first segment as the namespace
* *AND* both the SigV4-signed and the unsigned catalog paths SHALL build the identifier the same way so multi-level namespaces load correctly under either path

### Scenario: Positional-delete file references are carried in the per-shard files argument

* *GIVEN* a virtual schema over an Iceberg merge-on-read table backed by MinIO, where `plan_files` associates each data file with its applicable Parquet positional-delete files (at `file` or `partition` granularity)
* *WHEN* Exasol sends the corresponding pushdown request
* *THEN* the adapter SHALL resolve the data-file list, each file's byte size, and each file's associated positional-delete files exactly once, at the same resolve-once seam, and MUST NOT require the scan UDF to discover delete files itself
* *AND* the adapter SHALL carry each data file's associated positional-delete file references (path, byte size, delete content type) in the per-shard files argument alongside the data-file entry, keeping the wire surface minimal — no serialized Iceberg schema and no bound predicate are added for delete support
* *AND* the shard-invariant common spec (logical schema, projection, filter, LIMIT, credentials, table root) SHALL be unchanged by delete support, so a delete-free table produces a byte-identical common spec to before this feature

### Scenario: Scan-driving UDF invocations are schema-qualified from the running adapter script's schema

* *GIVEN* a virtual schema whose adapter script, `LAKEHOUSE_SCAN` scan script, and `LAKEHOUSE_DISTRIBUTE_FILES` distributor are all deployed in one Exasol schema
* *AND* a `CREATE VIRTUAL SCHEMA` statement that carries NO `SCAN_SCHEMA` property
* *WHEN* Exasol sends a `pushdown` request and the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL qualify the `LAKEHOUSE_SCAN` and `LAKEHOUSE_DISTRIBUTE_FILES` UDF names with the schema reported by the running adapter script's UDF handshake (`ctx.script_schema()`), and MUST NOT read any VS property to obtain that schema
* *AND* because those scripts are co-deployed in the adapter script's schema, the qualified names SHALL resolve when the scan-driving SQL executes outside the adapter script's own schema context
* *AND* when `ctx.script_schema()` reports an empty schema, the adapter SHALL emit the `LAKEHOUSE_SCAN` and `LAKEHOUSE_DISTRIBUTE_FILES` UDF names unqualified, relying on the session's current schema to resolve them
