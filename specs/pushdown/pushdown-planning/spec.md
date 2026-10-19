# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan. The plan captures the requested projection, filter and LIMIT, and emits the SQL that drives the DataFusion scan over the table identity, file list and logical schema resolved by `file-planning/pushdown-planning-file-resolution`.

## Background

* The scan-driving SQL invokes the `LAKEHOUSE_SCAN` scalar EMIT UDF over a nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor subquery. The shard-invariant common spec is spliced once as the scan's first argument, and each shard's file subset flows through the distributor as the second argument. A single-shard plan calls the scalar scan directly. See `parallelism/work-unit-sharding`.
* The outer scalar scan select is never wrapped in a `SELECT * FROM (...)` materialization boundary. That boundary exists only on the declined-filter path.
* The common spec carries a CONNECTION name or sealed envelope, never a plaintext credential: see `storage-access/scan-spec-credential-reference`.
* The adapter schema-qualifies the UDF names from `ctx.script_schema()`. No VS property supplies the schema.
* The adapter reads the node count from `UdfContext::node_count()` per pushdown, not from `adapterNotes`. It captures the value in `dispatch` before the tokio runtime is entered and passes it to planning as a plain integer. `node_count()` returns `0` only without a live handshake, and `0` maps to a node count of `1`.
* The common spec's `projection` field carries the projected columns only for the row-scan and join paths. An aggregate or GROUP BY request leaves it empty: see `pushdown-aggregates/pushdown-planning-single-group-agg` and `pushdown-grouped-agg/pushdown-planning-grouped-agg`.
* Exasol does not re-apply a predicate whose capability the adapter advertised, so the adapter's own SQL applies any predicate it cannot translate. See `pushdown/pushdown-declined-filter-self-apply`. An absent or trivially-true filter is omitted and the wrapper-free scan is unchanged. A declined filter routes to the qualified single-table wrapper, which applies the predicate in its `WHERE`.
* The type-aware LIKE rule is in `pushdown-types/pushdown-planning-like-type-coercion`.
* The plan-time file-pruning predicate has a per-format owner. For Iceberg it is `iceberg::expr::Predicate`, owned by `file-planning/pushdown-file-pruning`. For Delta it is the Delta stats predicate, owned by `delta/delta-file-pruning`. Scenario clauses naming `iceberg::expr::Predicate` describe the Iceberg arm.

## Scenarios

### Scenario: Projection is pushed into the scan-driving query

* *GIVEN* a row-scan or inner-join `pushdown` request that selects only some of the table's columns and carries NO aggregate and NO GROUP BY
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the generated scan-driving SQL SHALL carry only the projected columns to the UDF, in the shard-invariant common spec spliced once as the scalar scan UDF's first-argument literal shared by all shards
* *AND* the projected column names SHALL be the current Iceberg logical names carried in the common spec's logical schema, so the UDF's registered table exposes them and the field-id adapter maps each to the correct physical column per file
* *AND* the scalar scan UDF's declared EMITS column list SHALL match the projected items in order and type, named POSITIONALLY: a bare-column item SHALL keep its real (quoted) source-column name so an outer `ORDER BY` over a projected column still resolves, while an expression or literal item SHALL be named by a positional-unique synthetic EMITS identifier rather than its rendered SQL text, so two structurally identical expression or literal items never collapse into one column and never collide into a duplicate EMITS name Exasol rejects
* *AND* the guarantee in this scenario SHALL govern ONLY the row-scan and join paths; an aggregate or GROUP BY request instead leaves the `projection` field empty (see `pushdown-aggregates/pushdown-planning-single-group-agg` and `pushdown-grouped-agg/pushdown-planning-grouped-agg`), so an empty `projection` on an aggregate scan spec is expected, not a lost projection

### Scenario: Filter predicate is pushed into the scan spec

* *GIVEN* a query with a WHERE predicate over a supported column and operator
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL translate the predicate into the shard-invariant common spec passed to the UDF, and the translation SHALL be ALL-OR-NOTHING over the whole top-level filter
* *AND* a filter the adapter cannot render for DataFusion SHALL be self-applied in the qualified wrapper's `WHERE` rather than omitted, per `pushdown/pushdown-declined-filter-self-apply`
* *AND* before translating a `predicate_like` whose subject is a bare `column` node, the adapter SHALL apply the type-aware LIKE rule (see `pushdown-types/pushdown-planning-like-type-coercion`), because DataFusion performs no implicit non-string-to-VARCHAR coercion and would hard-fail the scan on a LIKE over a non-string column
* *AND* the adapter SHALL ALSO translate the soundly-translatable conjuncts into an `iceberg::expr::Predicate` applied to the Iceberg table scan as a file-pruning filter, dropping any node it cannot translate soundly rather than skipping a file that could match
* *AND* the DataFusion scan SHALL always apply the full common-spec filter, so the Iceberg pruning filter only narrows which files are opened and never changes the result set

### Scenario: A declined WHERE filter routes the single-table request to the qualified wrapper

* *GIVEN* a single-table `pushdown` request carrying a non-null `filter` that the DataFusion-bound render declines, of any dispatch shape, row scan, top-N, single-group aggregate, grouped aggregate, or `COUNT(DISTINCT)`
* *WHEN* the pushdown dispatcher selects the SQL shape
* *THEN* the dispatcher SHALL route the request to the qualified single-table wrapper BEFORE the routing classifier runs, so one route serves every dispatch shape
* *AND* the request's ORIGINAL filter tree SHALL still be forwarded to Iceberg-level file pruning unchanged, because pruning reads the un-rewritten tree and only ever removes files that provably cannot match
* *AND* the wrapper's returned column count, order, and declared types SHALL equal what the request's `selectList` declares, and an absent, JSON-null, or empty `selectList` SHALL return the FULL base row rather than only the columns the declined predicate references, so the route never trips Exasol's positional `04000` validation
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
* *AND* a `CREATE VIRTUAL SCHEMA` statement that carries NO `SCAN_SCHEMA` property
* *WHEN* Exasol sends a `pushdown` request and the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL qualify the `LAKEHOUSE_SCAN` and `LAKEHOUSE_DISTRIBUTE_FILES` UDF names with the schema reported by the running adapter script's UDF handshake (`ctx.script_schema()`), and MUST NOT read any VS property to obtain that schema
* *AND* because those scripts are co-deployed in the adapter script's schema, the qualified names SHALL resolve when the scan-driving SQL executes outside the adapter script's own schema context
* *AND* when `ctx.script_schema()` reports an empty schema, the adapter SHALL emit the `LAKEHOUSE_SCAN` and `LAKEHOUSE_DISTRIBUTE_FILES` UDF names unqualified, relying on the session's current schema to resolve them

### Scenario: Projected CAST expression preserves the declared TIMESTAMP fractional-seconds precision in its EMITS type

* *GIVEN* a row-scan `pushdown` request whose select list carries an expression item, for example `CAST(c_ts AS TIMESTAMP(6))`, whose parallel `selectListDataTypes` entry is `{"type":"TIMESTAMP","fractionalSecondsPrecision":6}`
* *WHEN* the adapter derives the scalar scan UDF's EMITS column type for that item via `exasol_type_from_json`
* *THEN* the derived EMITS type SHALL be `TIMESTAMP(6)`, reading the precision from the `fractionalSecondsPrecision` field, Exasol's documented data-type field for a TIMESTAMP's fractional-seconds precision (default 3), verified against Exasol's virtual-schema data-type API and the reference fixture `pushdown_request_alltypes.json`; the `DECIMAL` arm's `precision`/`scale` keys MUST NOT be read for a TIMESTAMP
* *AND* when `fractionalSecondsPrecision` is absent the derived EMITS type SHALL be bare `TIMESTAMP`, equivalent to Exasol's default `TIMESTAMP(3)`, preserving the current behavior asserted by `exasol_type_from_json_reads_with_local_time_zone_flag`
* *AND* a `withLocalTimeZone: true` timestamp dataType SHALL still map to `TIMESTAMP WITH LOCAL TIME ZONE` and SHALL take precedence over any precision rendering, leaving the WLTZ branch unchanged
* *AND* this EMITS-precision derivation and the vs-expression CAST-render precision fix (`sql-comprehension/vs-expression-translator-cast`) SHALL ship together, because Exasol's `EXPLAIN VIRTUAL` type check (`Data type mismatch ... Expected TIMESTAMP(6), but got TIMESTAMP(3)`, SQL error 04000) compares the outer query's expected column type against the EMITS-declared type, and fixing only one of the two collapse points still fails the check; this scenario governs the pushed-down CAST *expression's* declared target type and MUST NOT be conflated with `scan-types/type-mapping`'s "Iceberg timestamptz maps to plain Exasol TIMESTAMP" scenario, which governs a raw column's `createVirtualSchema` schema declaration (always bare `TIMESTAMP`)

### Scenario: Pushdown reads the cluster node count from the UDF handshake

* *GIVEN* a virtual schema over an Iceberg table whose persisted `adapterNotes` carry NO `CLUSTER_NODES` entry
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
