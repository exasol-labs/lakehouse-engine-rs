# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan. The plan captures the requested projection, filter and LIMIT, and emits the SQL that drives the DataFusion scan over the table identity, file list and logical schema resolved by `file-planning/pushdown-planning-file-resolution`.

## Background

* The scan-driving SQL invokes the `LAKEHOUSE_SCAN` scalar EMIT UDF over a nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor subquery. The shard-invariant common spec is spliced once as the scan's first argument, and each shard's file subset flows through the distributor as the second argument. A single-shard plan calls the scalar scan directly. See `parallelism/work-unit-sharding`.
* The outer scalar scan select is never wrapped in a `SELECT * FROM (...)` materialization boundary. That boundary exists only on the declined-filter path.
* The common spec carries a CONNECTION name or sealed envelope, never a plaintext credential: see `storage-access/scan-spec-credential-reference`.
* The adapter schema-qualifies the UDF names from `ctx.script_schema()`. No VS property supplies the schema.
* Each pushdown builds one pushdown context that holds the CONNECTION, the qualified UDF names, the node count, the tuning values of `vs-properties/tuning-resolution`, and the Exasol version from `ctx.database_version()`. Every planning path reads these values from it.
* The adapter reads the node count from `UdfContext::node_count()` per pushdown, not from `adapterNotes`. It captures the value in `dispatch` before the tokio runtime is entered and passes it to planning as a plain integer. `node_count()` returns `0` only without a live handshake, and `0` maps to a node count of `1`.
* The common spec's `projection` field carries the projected columns only for the row-scan and join paths. A single-table aggregate or GROUP BY request leaves it empty: see `pushdown-aggregates/pushdown-planning-single-group-agg` and `pushdown-grouped-agg/pushdown-planning-grouped-agg`.
* Exasol does not re-apply a predicate whose capability the adapter advertised, so the adapter's own SQL applies any predicate it cannot translate. See `pushdown/pushdown-declined-filter-self-apply`. An absent filter is omitted and the wrapper-free scan is unchanged. A declined filter routes to the qualified single-table wrapper, which applies the predicate in its `WHERE`.
* Which expressions the scan computes is decided by `pushdown-types/pushdown-planning-scan-text-conversion`.
* Exasol validates the pushed query's result positionally, and the request's `selectList` states the columns it expects in one of three forms. An absent or JSON-null `selectList` is `SELECT *`, one column per declared column. An empty `selectList` needs no column and accepts exactly one, whose value Exasol ignores. Any other `selectList` expects one column per item.
* A scan below an Exasol wrapper emits only columns that a clause or a join condition of the request names. When the wrapper needs none of them, the scan emits one column the reader does not refuse, never the whole row. Only `SELECT *` emits every declared column.
* The plan-time file-pruning predicate has a per-format owner. For Iceberg it is `iceberg::expr::Predicate`, owned by `file-planning/pushdown-file-pruning`. For Delta it is the Delta stats predicate, owned by `delta/delta-file-pruning`. Scenario clauses naming `iceberg::expr::Predicate` describe the Iceberg arm.

## Scenarios

### Scenario: Projection is pushed into the scan-driving query

* *GIVEN* a row-scan or inner-join `pushdown` request that selects only some of the table's columns and carries NO aggregate and NO GROUP BY
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the generated scan-driving SQL SHALL carry only the projected columns to the UDF, in the shard-invariant common spec spliced once as the scalar scan UDF's first-argument literal shared by all shards
* *AND* the projected column names SHALL be the current Iceberg logical names carried in the common spec's logical schema, so the UDF's registered table exposes them and the field-id adapter maps each to the correct physical column per file
* *AND* the scalar scan UDF's declared EMITS column list SHALL match the projected items in order and type, named POSITIONALLY: a bare-column item SHALL keep its real (quoted) source-column name so an outer `ORDER BY` over a projected column still resolves, while an expression or literal item SHALL be named by a positional-unique synthetic EMITS identifier rather than its rendered SQL text, so two structurally identical expression or literal items never collapse into one column and never collide into a duplicate EMITS name Exasol rejects
* *AND* the guarantee in this scenario SHALL govern ONLY the row-scan and join paths; a single-table aggregate or GROUP BY request instead leaves the `projection` field empty (see `pushdown-aggregates/pushdown-planning-single-group-agg` and `pushdown-grouped-agg/pushdown-planning-grouped-agg`), so an empty `projection` on an aggregate scan spec is expected, not a lost projection

### Scenario: Filter predicate is pushed into the scan spec

* *GIVEN* a query with a WHERE predicate over a supported column and operator
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL translate the predicate into the shard-invariant common spec passed to the UDF, and SHALL translate a filter that does not render whole one top-level `AND` conjunct at a time
* *AND* a conjunct the adapter cannot render for DataFusion SHALL be self-applied in the qualified wrapper's `WHERE` rather than omitted, while the rendered conjuncts stay in the scan, per `pushdown/pushdown-declined-filter-self-apply`
* *AND* the scan SHALL compute only the conjuncts that `pushdown-types/pushdown-planning-scan-text-conversion` allows, because DataFusion performs no implicit conversion to text
* *AND* the adapter SHALL ALSO prune files by the conjuncts it can translate soundly (`file-planning/pruning-predicate-translation`), ignoring any other node rather than skipping a file that could match
* *AND* the scan SHALL always apply its whole scan filter, so pruning only narrows which files are opened and never changes the result set

### Scenario: A declined WHERE filter routes the single-table request to the qualified wrapper

* *GIVEN* a single-table `pushdown` request carrying a non-null `filter` with at least one top-level conjunct that the DataFusion-bound render declines, of any dispatch shape, row scan, top-N, single-group aggregate, grouped aggregate, or `COUNT(DISTINCT)`
* *WHEN* the adapter builds the pushdown SQL
* *THEN* the adapter SHALL return the qualified single-table wrapper for every dispatch shape, and the wrapper's raw fan-out SHALL carry the filter's rendered conjuncts as its scan filter
* *AND* file pruning SHALL still use every conjunct of the request's filter, declined or not, and remove only files that provably cannot match
* *AND* the wrapper's returned column count, order, and declared types SHALL equal what the request's `selectList` declares: the full base row for an absent or JSON-null `selectList`, and one column for an empty one, so the route never trips Exasol's positional `04000` validation
* *AND* a request whose filter renders, or which carries no filter, SHALL take its existing dispatch shape with the emitted SQL byte-identical to its pre-change output

### Scenario: LIMIT is pushed into the scan spec

* *GIVEN* a query with a LIMIT clause and NO `order_by` that governs which rows are selected
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the shard-invariant common spec spliced into the scalar scan UDF SHALL carry the row limit
* *AND* because the common spec is shared by every shard, each row-scan shard invocation SHALL observe the same limit
* *AND* the generated SQL SHALL attach the `LIMIT` DIRECTLY to the outer ungrouped scalar scan select (over the distributor subquery, or the from-less single-shard select) as a correctness backstop, with no `SELECT * FROM (...)` wrapper
* *AND* when the request DOES carry an `order_by`, the per-shard row limit SHALL be governed by ordered top-N (pushed only alongside the matching per-shard `ORDER BY`), never as a bare per-shard `LIMIT` ahead of a global sort

### Scenario: Scan-driving UDF invocations are schema-qualified from the running adapter script's schema

* *GIVEN* a virtual schema whose adapter script, `LAKEHOUSE_SCAN` scan script, and `LAKEHOUSE_DISTRIBUTE_FILES` distributor are all deployed in one Exasol schema
* *WHEN* Exasol sends a `pushdown` request and the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL qualify the `LAKEHOUSE_SCAN` and `LAKEHOUSE_DISTRIBUTE_FILES` UDF names with the schema reported by the running adapter script's UDF handshake (`ctx.script_schema()`), and MUST NOT read any VS property to obtain that schema
* *AND* because those scripts are co-deployed in the adapter script's schema, the qualified names SHALL resolve when the scan-driving SQL executes outside the adapter script's own schema context
* *AND* when `ctx.script_schema()` reports an empty schema, the adapter SHALL emit the `LAKEHOUSE_SCAN` and `LAKEHOUSE_DISTRIBUTE_FILES` UDF names unqualified, relying on the session's current schema to resolve them

### Scenario: Projected CAST expression preserves the declared TIMESTAMP fractional-seconds precision in its EMITS type

* *GIVEN* a row-scan `pushdown` request whose select list carries an expression item, for example `CAST(c_ts AS TIMESTAMP(6))`, whose parallel `selectListDataTypes` entry is `{"type":"TIMESTAMP","fractionalSecondsPrecision":6}`
* *WHEN* the adapter derives the scalar scan UDF's EMITS column type for that item
* *THEN* the derived EMITS type SHALL be `TIMESTAMP(6)`, reading the precision from the `fractionalSecondsPrecision` field, Exasol's data-type field for a TIMESTAMP's fractional-seconds precision; a `precision` or `scale` key on a TIMESTAMP `dataType` MUST NOT set its precision
* *AND* when `fractionalSecondsPrecision` is absent the derived EMITS type SHALL be bare `TIMESTAMP`, equivalent to Exasol's default `TIMESTAMP(3)`
* *AND* a `withLocalTimeZone: true` dataType SHALL be `TIMESTAMP(p) WITH LOCAL TIME ZONE`, which the scan cannot emit, so the item routes per `pushdown-capabilities/pushdown-planning-capability-extensions`
* *AND* the EMITS type and the CAST target the adapter renders for the same item (`sql-comprehension/vs-expression-translator-cast`) SHALL carry the same precision, because Exasol's `EXPLAIN VIRTUAL` type check (`Data type mismatch ... Expected TIMESTAMP(6), but got TIMESTAMP(3)`, SQL error 04000) compares the outer query's expected column type against the EMITS-declared type; this scenario governs the pushed-down CAST expression's declared target type, not a raw column's `createVirtualSchema` declaration, which `scan-types/type-mapping` governs

### Scenario: Pushdown reads the cluster node count from the UDF handshake

* *GIVEN* a virtual schema over an Iceberg table whose persisted `adapterNotes` carry no node-count entry
* *AND* a live UDF handshake on the running adapter script reporting `UdfContext::node_count()` as N where N is at least 1
* *WHEN* Exasol sends a `pushdown` request against that virtual schema
* *THEN* the adapter SHALL use N as the node count in the shard count `G = node_count × PARALLELISM_FACTOR` (see `parallelism/work-unit-sharding`)
* *AND* the adapter MUST NOT read the node count from `schemaMetadataInfo.adapterNotes`, and MUST NOT open a connect-back session or issue `SELECT NPROC()` to obtain it

### Scenario: Pushdown node count falls back to one when the handshake reports none

* *GIVEN* a `pushdown` request whose context reports `UdfContext::node_count()` as `0` (no live handshake node count)
* *WHEN* the adapter resolves the node count for that request
* *THEN* the adapter SHALL use a node count of `1`
* *AND* the resulting shard count `G` SHALL be `min(1 × PARALLELISM_FACTOR, 300, file_count)` per `parallelism/work-unit-sharding`
* *AND* the adapter SHALL still return a successful `pushdown` response

*Credential-reference invariant:* the common scan-spec literal carries a connection REFERENCE or sealed envelope, never a plaintext credential. See `storage-access/scan-spec-credential-reference` for the full contract.
