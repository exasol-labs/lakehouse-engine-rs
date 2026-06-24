# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan for a single involved table: it derives the scanned Iceberg table from `involvedTables[0].name` via the create-time `TABLE_MAP`, resolves that table's Iceberg data-file list once (signing catalog requests with AWS SigV4 and applying vended S3 credentials when enabled), captures projection, filter, LIMIT, and any supported aggregate, and emits the SQL that drives the DataFusion scan SET UDF over exactly those files.

## Background

Each pushdown request concerns exactly one virtual table — Exasol issues a separate single-table pushdown per table, including for JOINs (which are not advertised; Exasol joins the per-table result sets itself). The `TABLE_MAP` recorded in `schemaMetadata.adapterNotes` at create time is handed back in `schemaMetadataInfo.adapterNotes` and maps each Exasol table name to its original-cased Iceberg identifier.

* The adapter receives a `pushdown` request carrying the projection, filter, and
  aggregate specification from Exasol.
* Catalog and storage credentials are resolved from the CONNECTION object, not plain
  properties. See `vs-adapter/connection-credentials`.
* The adapter resolves the Iceberg snapshot and file list exactly once per query.
* The shard count G is `CLUSTER_NODES × PARALLELISM_FACTOR` capped at 300 and clamped
  to the file count, per the `parallelism/work-unit-sharding` feature; the scan-driving
  SQL groups on `shard_key`, never on `IPROC()`.
* Credentials MUST NOT appear in any returned SQL or error message.
* A predicate or group-key expression the adapter cannot translate is omitted from the
  scan spec; Exasol keeps it as a correctness backstop.
* SigV4 signing and credential vending scenarios are in
  `vs-adapter/pushdown-planning-cloud-credentials`.

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
* *THEN* the adapter SHALL determine the target Iceberg table from `involvedTables[0].name` via the `TABLE_MAP` and resolve that table's Iceberg snapshot and data-file list exactly once
* *AND* the adapter SHALL return a JSON response of type `pushdown` containing SQL that invokes the scan SET UDF and passes the resolved data-file list as an explicit argument
* *AND* the adapter MUST NOT require the scan UDF to discover files itself

### Scenario: Projection is pushed into the scan-driving query

* *GIVEN* a query that selects only some of the table's columns
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the generated scan-driving SQL SHALL carry only the projected columns to the UDF
* *AND* the UDF's declared EMITS column list SHALL match the projected columns in order and type

### Scenario: Filter predicate is pushed into the scan spec

* *GIVEN* a query with a WHERE predicate over a supported column and operator
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL translate the predicate into the scan spec passed to the UDF
* *AND* a predicate the adapter cannot translate SHALL be omitted from the scan spec rather than produce an incorrect result

### Scenario: LIMIT is pushed into the scan spec

* *GIVEN* a query with a LIMIT clause
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the scan spec passed to the UDF SHALL carry the row limit
* *AND* the generated SQL MAY also retain the LIMIT at the Exasol level as a correctness backstop

### Scenario: Adapter advertises aggregate pushdown for supported functions

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the capabilities list SHALL include single-group aggregate pushdown for `COUNT`/`COUNT(*)`/`SUM`/`MIN`/`MAX`/`AVG`, `AGGREGATE_GROUP_BY_COLUMN`/`AGGREGATE_GROUP_BY_EXPRESSION`/`AGGREGATE_HAVING`, the decomposable statistical aggregates `FN_AGG_STDDEV`/`FN_AGG_STDDEV_POP`/`FN_AGG_STDDEV_SAMP`/`FN_AGG_VARIANCE`/`FN_AGG_VAR_POP`/`FN_AGG_VAR_SAMP`, and (still) column projection, scalar select-list expressions, filter predicates, and LIMIT
* *AND* the capabilities list MUST NOT include `AGGREGATE_GROUP_BY_TUPLE`, `FN_AGG_COUNT_DISTINCT` (or any other `*_DISTINCT` aggregate), `FN_AGG_MEDIAN`, `FN_AGG_APPROXIMATE_COUNT_DISTINCT`, `FN_AGG_GROUP_CONCAT*`/`FN_AGG_LISTAGG`, or join pushdown

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
