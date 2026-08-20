# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it captures
the requested projection, filter, LIMIT, and any supported aggregate, and emits the SQL
that drives the DataFusion scan over the table identity, file list, byte sizes,
delete-file references, and logical schema resolved once by
`vs-adapter/pushdown-planning-file-resolution`. Cluster fan-out is separated from the
scan: a nested `LAKEHOUSE_DISTRIBUTE_FILES` LUA SET distributor subquery (`GROUP BY
shard_key`) spreads each shard's per-file list across nodes, and an outer ungrouped
`LAKEHOUSE_SCAN` SCALAR EMIT UDF scans each distributed file list node-locally and streams
the rows. The scan-driving SQL splices the shard-invariant parts (projection, filter,
LIMIT, logical schema, credentials, and the table root) once as the scalar scan
UDF's first-argument common literal and flows each shard's per-file subset through the
distributor as the second argument. A single-shard plan short-circuits the distributor and
calls the scalar scan directly on the file-list literal. See
`vs-adapter/pushdown-planning-file-encoding` for the table-root-once and relative/absolute
path encoding rules. See `vs-adapter/pushdown-planning-nested-aggregate-fallback` for the
guard against composed requests (e.g. an outer aggregate over an inner grouped-aggregate
sub-select) that don't map onto the source table's own columns. Single-group aggregate
pushdown (capability advertisement, partial-aggregate scan-spec translation, wrapper merge
SQL, and AVG sum/count decomposition) is covered separately in
`vs-adapter/pushdown-planning-single-group-agg`.

## Background

* The scan-driving SQL invokes the `LAKEHOUSE_SCAN` SCALAR EMIT UDF over a nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor subquery; the shard-invariant common spec (projection, filter, LIMIT, aggregates, group keys, logical schema, EMITS types, credentials, tuning knobs, and the table root) is spliced once as the scalar scan's first argument and each shard's file subset flows through the distributor as the second argument.
* The outer scalar scan select is never wrapped in a `SELECT * FROM (...)` materialization boundary.
* Credentials MUST NOT appear in any returned SQL string or error message, and MUST NOT be repeated per shard.
* The `LAKEHOUSE_SCAN` and `LAKEHOUSE_DISTRIBUTE_FILES` UDF names in the scan-driving SQL are schema-qualified from the schema of the running adapter script, read from the UDF handshake via `ctx.script_schema()`; there is no VS property that supplies this schema. The scan and distributor scripts are co-deployed in the adapter script's schema, so this single source qualifies both.
* The cluster node count that sizes the shard fan-out is read per pushdown from the adapter script's own UDF handshake via `UdfContext::node_count()`. It is NOT read from `schemaMetadataInfo.adapterNotes`. Every VS request type reaches the adapter through the same single-call script invocation, so the handshake carries the node count on a `pushdown` request exactly as it does on a `createVirtualSchema` request; the request type lives in the JSON payload, not in the handshake.
* `node_count()` is a synchronous handshake read and MUST be captured in `dispatch` before the tokio runtime is entered, alongside `ctx.script_schema()` and the resolved CONNECTION credentials. The value is then threaded into the pushdown planning path as a plain integer, so the async planning code performs no ambient context read of its own.
* `node_count()` returns `0` only on a context carrying no live handshake metadata (a stub, a test double, or a broken handshake), so `0` maps to a node count of `1` and any live cluster (single-node included) reports `≥ 1`.
* The common spec's `projection` field carries the pushed-down projected columns ONLY for the row-scan and join dispatch paths. An aggregate or GROUP BY request leaves `projection` empty, because the aggregate scan-dispatch path derives its physical projection from the `aggregates`/`group_keys` fields rather than from `projection` (see `vs-adapter/pushdown-planning-single-group-agg` and `vs-adapter/pushdown-planning-grouped-agg`).
* See `vs-adapter/pushdown-planning-single-group-agg` for single-group aggregate pushdown (capability advertisement, partial-aggregate translation, wrapper merge SQL, and AVG decomposition).
* This delta SUPERSEDES the preceding Background bullet "A predicate node the adapter cannot faithfully translate is OMITTED from the scan spec; Exasol keeps and evaluates the predicate itself as a correctness backstop." That claim is FALSE and is corrected, not merely superseded: a predicate the adapter cannot faithfully translate MUST be applied by the adapter's own returned SQL — see `vs-adapter/pushdown-declined-filter-self-apply` and ADR `specs/_decision/045` for why nothing else applies it. Omitting it returns extra unfiltered rows, verified live.
* The single-table path distinguishes an ABSENT filter from a DECLINED one. An absent or trivially-true filter is omitted and the wrapper-free fast scan is unchanged; a declined filter routes the request to the qualified single-table wrapper, which applies the predicate in its own `WHERE`.
* The wrapper-free outer scalar scan select remains the shape for every request whose filter renders. The `SELECT * FROM (…)` boundary the wrapper introduces exists only on the decline path.
* See `vs-adapter/pushdown-planning-like-type-coercion` for the type-aware LIKE/REGEXP_LIKE rule that dispatches on the subject column's Exasol type before rendering the filter.
* When a query aliases a table in its `FROM` clause (`FROM customer c`), Exasol stamps a `tableAlias` on every `column` node in the pushdown request — including nodes the user wrote unqualified. The single scan relation exposes only bare column names, so an alias-qualified reference does not resolve; the single-table push therefore strips the alias before rendering (see `vs-adapter/pushdown-planning-alias-stripping`). The `crates/vs-expression` translator itself always honors a present `tableAlias` (`sql-comprehension/vs-expression-translator`); stripping is the single-table caller's responsibility.
* The plan-time file-pruning predicate this feature dispatches to has a per-format owner: for the Iceberg reader it is `iceberg::expr::Predicate`, owned by `vs-adapter/pushdown-file-pruning`; for the Delta reader it is the Delta stats predicate, owned by `vs-adapter/delta-file-pruning`. A reader of this feature's `## Scenarios` clauses naming `iceberg::expr::Predicate` should read those clauses as describing the Iceberg arm of the rule, not the whole rule.

## Scenarios

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
* *THEN* the adapter SHALL translate the predicate into the shard-invariant common spec passed to the UDF, and the translation SHALL be ALL-OR-NOTHING over the whole top-level filter — REPLACING the recorded "omitting (never mistranslating) any node it cannot render", which sanctioned dropping one node while keeping the rest of the tree
* *AND* a filter the adapter cannot render for DataFusion SHALL be self-applied in the qualified wrapper's `WHERE` rather than omitted, per `vs-adapter/pushdown-declined-filter-self-apply`
* *AND* before translating a `predicate_like` or `predicate_like_regexp` whose subject is a bare `column` node, the adapter SHALL apply the type-aware LIKE rule (see `vs-adapter/pushdown-planning-like-type-coercion`), because DataFusion performs no implicit non-string-to-VARCHAR coercion and would hard-fail the scan on a LIKE over a non-string column
* *AND* the adapter SHALL ALSO translate the soundly-translatable conjuncts into an `iceberg::expr::Predicate` applied to the Iceberg table scan as a file-pruning filter, dropping any node it cannot translate soundly rather than skipping a file that could match
* *AND* the DataFusion scan SHALL always apply the full common-spec filter, so the Iceberg pruning filter only narrows which files are opened and never changes the result set

### Scenario: A declined WHERE filter routes the single-table request to the qualified wrapper

* *GIVEN* a single-table `pushdown` request carrying a non-null `filter` that the DataFusion-bound render declines, of any dispatch shape — row scan, top-N, single-group aggregate, grouped aggregate, or `COUNT(DISTINCT)`
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

* *GIVEN* a row-scan `pushdown` request whose select list carries an expression item — for example `CAST(c_ts AS TIMESTAMP(6))` — whose parallel `selectListDataTypes` entry is `{"type":"TIMESTAMP","fractionalSecondsPrecision":6}`
* *WHEN* the adapter derives the scalar scan UDF's EMITS column type for that item via `exasol_type_from_json`
* *THEN* the derived EMITS type SHALL be `TIMESTAMP(6)`, reading the precision from the `fractionalSecondsPrecision` field — Exasol's documented data-type field for a TIMESTAMP's fractional-seconds precision (default 3), verified against Exasol's virtual-schema data-type API and the reference fixture `pushdown_request_alltypes.json`; the `DECIMAL` arm's `precision`/`scale` keys MUST NOT be read for a TIMESTAMP
* *AND* when `fractionalSecondsPrecision` is absent the derived EMITS type SHALL be bare `TIMESTAMP`, equivalent to Exasol's default `TIMESTAMP(3)`, preserving the current behavior asserted by `exasol_type_from_json_reads_with_local_time_zone_flag`
* *AND* a `withLocalTimeZone: true` timestamp dataType SHALL still map to `TIMESTAMP WITH LOCAL TIME ZONE` and SHALL take precedence over any precision rendering, leaving the WLTZ branch unchanged
* *AND* this EMITS-precision derivation and the vs-expression CAST-render precision fix (`sql-comprehension/vs-expression-translator-cast`) SHALL ship together, because Exasol's `EXPLAIN VIRTUAL` type check (`Data type mismatch ... Expected TIMESTAMP(6), but got TIMESTAMP(3)`, SQL error 04000) compares the outer query's expected column type against the EMITS-declared type, and fixing only one of the two collapse points still fails the check; this scenario governs the pushed-down CAST *expression's* declared target type and MUST NOT be conflated with `datafusion-scan/type-mapping`'s "Iceberg timestamptz maps to plain Exasol TIMESTAMP" scenario, which governs a raw column's `createVirtualSchema` schema declaration (always bare `TIMESTAMP`)

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
