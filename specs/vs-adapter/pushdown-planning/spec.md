# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it resolves the Iceberg data-file list once, captures the requested projection, filter, LIMIT, and any supported aggregate, extracts the table's current Iceberg schema for field-id-based projection, and emits the SQL that drives the DataFusion scan SET UDF — sharded across cluster nodes — over exactly those files. The scan-driving SQL passes the shard-invariant parts (projection, filter, LIMIT, logical schema, credentials, and the Iceberg table root) once as the UDF's common argument and each shard's per-file `(path, size)` subset as the per-shard argument. See `vs-adapter/pushdown-planning-file-encoding` for the table-root-once and relative/absolute path encoding rules. See `vs-adapter/pushdown-planning-nested-aggregate-fallback` for the guard against composed requests (e.g. an outer aggregate over an inner grouped-aggregate sub-select) that don't map onto the source table's own columns.

## Background

* The data-file list, each file's byte size (from the Iceberg manifest), and the current Iceberg schema are resolved exactly once per pushdown, in the planning layer; the scan UDF never discovers files itself.
* The logical schema carried into the common scan-spec argument identifies each column by its Iceberg field-id, current name, Arrow type, and nullability.
* The scan-driving SQL serializes the shard-invariant common spec once (projection, filter, LIMIT, aggregates, group keys, logical schema, EMITS types, credentials, tuning knobs, and the Iceberg table root) and carries only each shard's per-file `(path, size)` subset per shard.
* Each per-shard file entry carries both the file path and its byte size, so the scan UDF never re-discovers a size the adapter already resolved.
* Credentials MUST NOT appear in any returned SQL string or error message, and MUST NOT be repeated per shard.

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
* *AND* the adapter SHALL return a JSON response of type `pushdown` containing SQL that invokes the scan SET UDF, carrying the logical schema AND the Iceberg table root in the shard-invariant common spec argument (each serialized once) and the resolved data-file list as the per-shard argument, where each per-shard entry carries the file path together with its resolved byte size
* *AND* the adapter MUST NOT require the scan UDF to discover files itself, and MUST NOT require the scan UDF to re-fetch any file's size

### Scenario: Projection is pushed into the scan-driving query

* *GIVEN* a query that selects only some of the table's columns
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the generated scan-driving SQL SHALL carry only the projected columns to the UDF, in the shard-invariant common spec argument shared by all shards
* *AND* the projected column names SHALL be the current Iceberg logical names carried in the common spec's logical schema, so the UDF's registered table exposes them and the field-id adapter maps each to the correct physical column per file
* *AND* the UDF's declared EMITS column list SHALL match the projected columns in order and type

### Scenario: Filter predicate is pushed into the scan spec

* *GIVEN* a query with a WHERE predicate over a supported column and operator
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL translate the predicate into the shard-invariant common spec passed to the UDF, omitting (never mistranslating) any node it cannot render
* *AND* the adapter SHALL ALSO translate the soundly-translatable conjuncts into an `iceberg::expr::Predicate` applied to the Iceberg table scan as a file-pruning filter, dropping any node it cannot translate soundly rather than skipping a file that could match
* *AND* the DataFusion scan SHALL always apply the full common-spec filter, so the Iceberg pruning filter only narrows which files are opened and never changes the result set

### Scenario: LIMIT is pushed into the scan spec

* *GIVEN* a query with a LIMIT clause
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the shard-invariant common spec passed to the UDF SHALL carry the row limit
* *AND* because the common spec is shared by every shard, each row-scan shard invocation SHALL observe the same limit
* *AND* the generated SQL MAY also retain the LIMIT at the Exasol level as a correctness backstop

### Scenario: Adapter advertises aggregate pushdown for supported functions

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the capabilities list SHALL include single-group aggregate pushdown for `COUNT`/`COUNT(*)`/`SUM`/`MIN`/`MAX`/`AVG`, `AGGREGATE_GROUP_BY_COLUMN`/`AGGREGATE_GROUP_BY_EXPRESSION`/`AGGREGATE_GROUP_BY_TUPLE`/`AGGREGATE_HAVING`, the decomposable statistical aggregates `FN_AGG_STDDEV`/`FN_AGG_STDDEV_POP`/`FN_AGG_STDDEV_SAMP`/`FN_AGG_VARIANCE`/`FN_AGG_VAR_POP`/`FN_AGG_VAR_SAMP`, and (still) column projection, scalar select-list expressions, filter predicates, and LIMIT
* *AND* the adapter SHALL advertise `AGGREGATE_GROUP_BY_TUPLE` only because the grouped-aggregate detection and scan-driving SQL builder handle an arbitrary number of group keys (see `vs-adapter/pushdown-planning-grouped-agg`), so a GROUP BY over two or more keys is pushed down as node-local partial aggregation rather than falling back to a raw row scan that Exasol aggregates itself
* *AND* the capabilities list MUST NOT include `FN_AGG_COUNT_DISTINCT` (or any other `*_DISTINCT` aggregate), `FN_AGG_MEDIAN`, `FN_AGG_APPROXIMATE_COUNT_DISTINCT`, `FN_AGG_GROUP_CONCAT*`/`FN_AGG_LISTAGG`, or join pushdown

### Scenario: Aggregate query is translated into a partial-aggregate scan spec

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
* *AND* a query whose select list is one or more supported aggregate functions over the whole table
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL recognise the request as an aggregate query and resolve the data-file list exactly once
* *AND* the adapter SHALL build a scan spec carrying, for each requested aggregate, its function kind and target column (the wildcard for `COUNT(*)`), plus any pushed-down filter so the partial aggregate covers filtered rows only
* *AND* the adapter MUST NOT push down an aggregate the scan UDF cannot compute, falling back to row scanning for that query instead

### Scenario: Aggregate wrapper SQL merges per-shard partial results

* *GIVEN* an aggregate pushdown over a file list partitioned into one or more shards
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the generated SQL SHALL drive the scan SET UDF to emit one partial-aggregate row per shard
* *AND* the SQL SHALL wrap those partial rows in an outer aggregation that merges them into the final result: `SUM` over per-shard partial counts for `COUNT`, `SUM` over partial sums for `SUM`, `MIN`/`MAX` over partial extrema for `MIN`/`MAX`
* *AND* the merged result SHALL equal the result of the same aggregate evaluated over all rows on a single node

### Scenario: AVG is pushed down as a sum/count pair and divided in the wrapper

* *GIVEN* a query selecting `AVG(col)` over the table
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the scan spec SHALL instruct the scan UDF to emit a partial `SUM(col)` and a partial `COUNT(col)` pair rather than a per-shard average
* *AND* the wrapper SQL SHALL compute the final average as `SUM(partial_sum) / SUM(partial_count)`
* *AND* the wrapper SQL SHALL yield NULL when the total partial count is zero, never dividing by zero

### Scenario: Pushdown resolves multi-level namespace identifiers into the iceberg TableIdent

* *GIVEN* a `TABLE_MAP` entry whose value is a multi-level Iceberg identifier such as `prod.finance.orders`
* *WHEN* the adapter resolves that identifier to load the table from the catalog
* *THEN* the adapter SHALL split the identifier into all namespace segments and the trailing table name, building the iceberg `TableIdent` from a multi-segment `NamespaceIdent` rather than treating only the first segment as the namespace
* *AND* both the SigV4-signed and the unsigned catalog paths SHALL build the identifier the same way so multi-level namespaces load correctly under either path
