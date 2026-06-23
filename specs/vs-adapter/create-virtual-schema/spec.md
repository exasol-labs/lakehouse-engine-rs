# Feature: Create Virtual Schema

Lets an Exasol user register an Iceberg table (resolved through an Iceberg REST
catalog over S3-compatible storage) as a queryable virtual schema, so the table's
columns appear to Exasol with correctly mapped SQL types, and records — in the
response `adapterNotes` — both the cluster's active node count (via `NPROC()`) and
a parallelism factor, so later pushdowns can size the oversubscribed work-unit
shard count.

## Background

* The adapter holds no state between requests; cluster information is resolved per
  request and returned in the `createVirtualSchema` response's `adapterNotes`
  (stringified JSON), which Exasol persists and round-trips back at pushdown time.
  The adapter MUST NOT use `schemaMetadata.properties` for this purpose, as Exasol
  2025.2.1 silently drops adapter-returned properties. The `adapterNotes` channel is
  queryable via `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`.
* `NPROC()` is obtained over a read-only connect-back session and recorded as
  `CLUSTER_NODES`.
* The parallelism factor is supplied as a VS/connection property and recorded
  alongside `CLUSTER_NODES`.
* Credentials MUST NOT appear in any error message or returned property.
* The Iceberg REST catalog and the S3-compatible object store (MinIO) are reachable
  from the adapter via connection properties supplied at `CREATE VIRTUAL SCHEMA` time.
* The adapter is the Rust ADAPTER SCRIPT entry point of a single `.so`; it speaks the
  Exasol virtual-schema JSON protocol (request in, JSON response out).
* Schema mapping (C.2) MUST use the same mapping as the scan, defined in the
  `datafusion-scan/type-mapping` feature. Columns whose Arrow type Exasol cannot
  represent (list, struct, map, binary, out-of-range decimal, and the other incompatible
  types) are declared as `VARCHAR(2000000)` — they MUST NOT cause `createVirtualSchema`
  to error.

## Scenarios

### Scenario: Adapter reports its pushdown capabilities

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the adapter SHALL return a JSON response of type `getCapabilities` whose list includes projection (`SELECTLIST_PROJECTION`), scalar select-list expressions (`SELECTLIST_EXPRESSIONS`), filter predicates (`FILTER_EXPRESSIONS`), `LIMIT`, the comparison predicates `FN_PRED_EQUAL`/`FN_PRED_NOTEQUAL`/`FN_PRED_LESS`/`FN_PRED_LESSEQUAL`, the matching predicates `FN_PRED_LIKE`/`FN_PRED_LIKE_ESCAPE`/`FN_PRED_REGEXP_LIKE`, the literal capabilities `LITERAL_BOOL`/`LITERAL_DATE`/`LITERAL_DOUBLE`/`LITERAL_EXACTNUMERIC`/`LITERAL_NULL`/`LITERAL_STRING`/`LITERAL_TIMESTAMP`/`LITERAL_TIMESTAMP_UTC`, the supported math/string/date/conditional scalar-function capabilities enumerated in `vs-adapter/pushdown-planning`, and `AGGREGATE_HAVING` plus the decomposable statistical aggregates `FN_AGG_STDDEV`/`FN_AGG_STDDEV_POP`/`FN_AGG_STDDEV_SAMP`/`FN_AGG_VARIANCE`/`FN_AGG_VAR_POP`/`FN_AGG_VAR_SAMP`
* *AND* the capabilities list MUST NOT include `FN_PRED_GREATER` or `FN_PRED_GREATEREQUAL` (those names do not exist in the Exasol capability vocabulary — Exasol normalises `a > b` to `b < a` and `a >= b` to `b <= a` before it reaches the adapter — so advertising them is misleading dead capability), nor any of `ORDER_BY_COLUMN`/`ORDER_BY_EXPRESSION`, `JOIN*`, geospatial (`FN_ST_*`), Exasol-only session functions (`FN_CURRENT_USER`/`FN_SYS_GUID`/`FN_CURRENT_SCHEMA`), `LITERAL_INTERVAL`, `AGGREGATE_GROUP_BY_TUPLE`, any `*_DISTINCT` aggregate, `FN_AGG_MEDIAN`, `FN_AGG_APPROXIMATE_COUNT_DISTINCT`, or any `FN_AGG_GROUP_CONCAT*`/`FN_AGG_LISTAGG`
* *AND* every advertised capability name MUST be one the adapter can either translate via the VS expression translator or decompose into a correct partial/merge plan, so the advertised set never claims behaviour the engine cannot execute correctly

### Scenario: Create virtual schema maps the Iceberg table schema

* *GIVEN* an Iceberg table exists in the REST catalog backed by MinIO
* *AND* the catalog and storage connection properties are supplied to the adapter
* *WHEN* Exasol sends a `createVirtualSchema` request naming that table
* *THEN* the adapter SHALL resolve the table's current Iceberg schema from the catalog
* *AND* the adapter SHALL return a JSON response describing one virtual table whose columns map each Iceberg field to an Exasol SQL type per the `datafusion-scan/type-mapping` table, declaring any Exasol-incompatible type as `VARCHAR(2000000)` rather than failing
* *AND* the adapter MUST NOT persist any catalog metadata between requests

### Scenario: Create virtual schema fails clearly when the catalog is unreachable

* *GIVEN* the supplied Iceberg REST catalog endpoint cannot be reached
* *WHEN* Exasol sends a `createVirtualSchema` request
* *THEN* the adapter SHALL return an error describing that the catalog could not be reached
* *AND* the error message MUST NOT contain storage access keys or secret keys

### Scenario: Adapter records the cluster node count in the virtual-schema adapterNotes

* *GIVEN* an Exasol session that has installed the VS adapter script
* *AND* the catalog and storage connection properties are supplied to the adapter
* *WHEN* Exasol sends a `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL open a connect-back session to Exasol and run `SELECT NPROC()` to obtain the count of active cluster nodes
* *AND* the adapter SHALL return the resolved node count as a positive-integer `CLUSTER_NODES` entry inside the `createVirtualSchema` response's `adapterNotes` (stringified JSON), which Exasol persists and which is queryable via `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`
* *AND* the adapter MUST NOT persist the node count anywhere other than that returned `adapterNotes`

### Scenario: Cluster node count defaults to one when it cannot be determined

* *GIVEN* the VS adapter cannot open a connect-back session or `SELECT NPROC()` fails
* *WHEN* Exasol sends a `createVirtualSchema` request
* *THEN* the adapter SHALL write `CLUSTER_NODES: 1` into the `adapterNotes` of the `createVirtualSchema` response
* *AND* the adapter SHALL still return a successful `createVirtualSchema` response describing the mapped table
* *AND* the resulting single-shard behaviour MUST be identical to the pre-sharding single-node execution path

### Scenario: Adapter records the parallelism factor in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that supplies a `PARALLELISM_FACTOR` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the supplied parallelism factor in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES`
* *AND* the adapter SHALL default the parallelism factor to a sensible value (8) when the property is absent or not a positive integer
* *AND* the adapter MUST NOT persist the parallelism factor anywhere other than that returned `adapterNotes`

### Scenario: Recorded node count and parallelism factor drive later work-unit sharding

* *GIVEN* a `createVirtualSchema` request for which `NPROC()` resolves the active node count
* *WHEN* the adapter returns the `createVirtualSchema` response
* *THEN* the `adapterNotes` SHALL carry both the resolved `CLUSTER_NODES` node count and the `PARALLELISM_FACTOR`
* *AND* both values SHALL be round-tripped back to the adapter at pushdown time so the shard count G can be computed as `CLUSTER_NODES × PARALLELISM_FACTOR` capped at 300
