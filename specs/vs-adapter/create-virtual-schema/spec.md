# Feature: Create Virtual Schema

Lets an Exasol user register every Iceberg table in a configured namespace (resolved through an Iceberg REST catalog over S3-compatible storage, including AWS Glue with SigV4-signed requests) as queryable virtual tables, so each table's columns appear to Exasol with correctly mapped SQL types, and records — in the response adapterNotes — the cluster's active node count, per-node core count, parallelism factor, DataFusion threading and memory-budget controls, and the Exasol-name to Iceberg-identifier map so later pushdowns can size sharding and recover the scanned table.

## Background

The catalog endpoint and storage credentials are supplied through the Exasol CONNECTION object named by the `CATALOG_CONNECTION` property; the namespace to expose is supplied by the `ICEBERG_NAMESPACE` property. The adapter holds no state between requests other than the values it returns in `schemaMetadata.adapterNotes`, which Exasol persists.

* Catalog endpoint and storage credentials are supplied through a CONNECTION object
  named by `CATALOG_CONNECTION`. The adapter resolves credentials via `ctx.connection`
  and never persists catalog metadata between requests.
* Credentials MUST NOT appear in any returned response or error message.
* The adapter holds no state between requests; cluster information is resolved per
  request and returned in the `createVirtualSchema` response's `adapterNotes`
  (stringified JSON), which Exasol persists and round-trips back at pushdown time.
  The adapter MUST NOT use `schemaMetadata.properties` for this purpose, as Exasol
  2025.2.1 silently drops adapter-returned properties. The `adapterNotes` channel is
  queryable via `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`.
* The adapter is the Rust ADAPTER SCRIPT entry point of a single `.so`; it speaks the
  Exasol virtual-schema JSON protocol (request in, JSON response out).
* Schema mapping MUST use the same mapping as the scan, defined in the
  `datafusion-scan/type-mapping` feature. Columns whose Arrow type Exasol cannot
  represent (list, struct, map, binary, out-of-range decimal, and the other incompatible
  types) are declared as `VARCHAR(2000000)` — they MUST NOT cause `createVirtualSchema`
  to error.
* Cluster configuration and the Exasol-name to Iceberg-identifier map are recorded in
  `adapterNotes` per `vs-adapter/create-virtual-schema-adapter-notes`.

## Scenarios

### Scenario: Adapter reports its pushdown capabilities

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the adapter SHALL return a JSON response of type `getCapabilities` whose list includes projection (`SELECTLIST_PROJECTION`), scalar select-list expressions (`SELECTLIST_EXPRESSIONS`), filter predicates (`FILTER_EXPRESSIONS`), `LIMIT`, the comparison predicates `FN_PRED_EQUAL`/`FN_PRED_NOTEQUAL`/`FN_PRED_LESS`/`FN_PRED_LESSEQUAL`, the matching predicates `FN_PRED_LIKE`/`FN_PRED_LIKE_ESCAPE`/`FN_PRED_REGEXP_LIKE`, the literal capabilities `LITERAL_BOOL`/`LITERAL_DATE`/`LITERAL_DOUBLE`/`LITERAL_EXACTNUMERIC`/`LITERAL_NULL`/`LITERAL_STRING`/`LITERAL_TIMESTAMP`/`LITERAL_TIMESTAMP_UTC`, the supported math/string/date/conditional scalar-function capabilities enumerated in `vs-adapter/pushdown-planning`, and `AGGREGATE_HAVING` plus the decomposable statistical aggregates `FN_AGG_STDDEV`/`FN_AGG_STDDEV_POP`/`FN_AGG_STDDEV_SAMP`/`FN_AGG_VARIANCE`/`FN_AGG_VAR_POP`/`FN_AGG_VAR_SAMP`
* *AND* the capabilities list MUST NOT include `FN_PRED_GREATER` or `FN_PRED_GREATEREQUAL` (those names do not exist in the Exasol capability vocabulary — Exasol normalises `a > b` to `b < a` and `a >= b` to `b <= a` before it reaches the adapter — so advertising them is misleading dead capability), nor any of `ORDER_BY_COLUMN`/`ORDER_BY_EXPRESSION`, `JOIN*`, geospatial (`FN_ST_*`), Exasol-only session functions (`FN_CURRENT_USER`/`FN_SYS_GUID`/`FN_CURRENT_SCHEMA`), `LITERAL_INTERVAL`, `AGGREGATE_GROUP_BY_TUPLE`, any `*_DISTINCT` aggregate, `FN_AGG_MEDIAN`, `FN_AGG_APPROXIMATE_COUNT_DISTINCT`, or any `FN_AGG_GROUP_CONCAT*`/`FN_AGG_LISTAGG`
* *AND* every advertised capability name MUST be one the adapter can either translate via the VS expression translator or decompose into a correct partial/merge plan, so the advertised set never claims behaviour the engine cannot execute correctly

### Scenario: Create virtual schema enumerates every table in the configured namespace

* *GIVEN* an Iceberg REST catalog reachable through the CONNECTION named by `CATALOG_CONNECTION`
* *AND* a `createVirtualSchema` request that supplies an `ICEBERG_NAMESPACE` property naming an Iceberg namespace (one or more dot-separated levels, e.g. `finance` or `prod.finance`)
* *WHEN* Exasol sends the `createVirtualSchema` request
* *THEN* the adapter SHALL list every table contained in that namespace and in each of its descendant namespaces, resolving credentials via `CATALOG_CONNECTION` and SigV4-signing the catalog requests when enabled
* *AND* the adapter SHALL return a JSON response describing one virtual table per discovered Iceberg table, whose Exasol name is the namespace segments below the configured namespace plus the table name joined with `__` and uppercased, mapping each Iceberg field to an Exasol SQL type per the type-mapping table and declaring any incompatible type as VARCHAR rather than failing
* *AND* the adapter MUST NOT persist any catalog metadata between requests other than the table-name map recorded in `adapterNotes`

### Scenario: Create virtual schema fails clearly when the catalog is unreachable

* *GIVEN* the Iceberg REST catalog endpoint resolved from the CONNECTION cannot be reached
* *WHEN* Exasol sends a `createVirtualSchema` request
* *THEN* the adapter SHALL return an error describing that the catalog could not be reached or the namespace could not be listed
* *AND* the error message MUST NOT contain storage access keys, secret keys, session tokens, or any SigV4 signing key

### Scenario: Create virtual schema records the Exasol-name to Iceberg-identifier map in adapterNotes

* *GIVEN* a `createVirtualSchema` request that enumerates one or more tables in the configured namespace
* *WHEN* the adapter builds the `createVirtualSchema` response
* *THEN* the adapter SHALL record, inside the response's `schemaMetadata.adapterNotes` (a stringified JSON object), a `TABLE_MAP` entry mapping each uppercased `__`-flattened Exasol table name to its original-cased fully-qualified Iceberg identifier (dot-joined namespace segments plus table name)
* *AND* the adapter SHALL preserve every other pre-existing `adapterNotes` entry (`CLUSTER_NODES`, `NR_OF_CORES`, `PARALLELISM_FACTOR`, and the DataFusion threading and memory-budget entries) when writing `TABLE_MAP`
* *AND* the recorded map SHALL round-trip back to the adapter at pushdown time so a pushdown can recover the exact Iceberg identifier from the Exasol table name without re-listing the catalog
* *AND* the adapter MUST NOT persist the map anywhere other than the returned `adapterNotes`

### Scenario: Multi-level Iceberg namespaces flatten deterministically into Exasol table names

* *GIVEN* a configured namespace `prod.finance` containing an Iceberg table `orders` and a child namespace `prod.finance.eu` containing a table `orders`
* *WHEN* Exasol sends the `createVirtualSchema` request naming namespace `prod.finance`
* *THEN* the adapter SHALL name the first virtual table `ORDERS` and the second `EU__ORDERS`, flattening only the namespace segments below the configured namespace using `__` and uppercasing the result
* *AND* the adapter SHALL apply the same flatten function when building the `TABLE_MAP` so the Exasol name maps back to the correct original-cased Iceberg identifier
* *AND* when two distinct Iceberg identifiers flatten to the same Exasol name (a `__` collision) the adapter SHALL return an error naming the colliding Exasol table name rather than silently dropping or overwriting a table
