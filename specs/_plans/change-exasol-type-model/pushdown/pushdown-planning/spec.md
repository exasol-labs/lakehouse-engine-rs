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

<!-- DELTA:NEW -->
### Scenario: A pushdown request with a missing or invalid Exasol type fails the query

* *GIVEN* a `pushdown` request that lacks `selectListDataTypes`, or whose `selectListDataTypes` entry, involved-table column, or CAST node carries a `dataType` that is incomplete, names a type Exasol does not define, or lies outside Exasol's limits
* *AND* Exasol's limits are DECIMAL precision 1 to 36 with scale 0 to the precision, CHAR size 1 to 2,000, VARCHAR size 1 to 2,000,000, TIMESTAMP precision 0 to 9, INTERVAL leading precision 1 to 9 and fraction 0 to 9, and HASHTYPE size 1 to 1,024 bytes
* *WHEN* the adapter plans the pushdown
* *THEN* the adapter SHALL fail the query with an error that names the missing `selectListDataTypes` or the offending `dataType` object
* *AND* the adapter MUST NOT substitute a default other than the three that "Every request of a supported Exasol version passes the strict type check" names, cap or clamp a value, or declare `VARCHAR(2000000)` in place of the type
* *AND* a request whose `selectList` holds one or more items SHALL fail the query when `selectListDataTypes` holds a different number of entries
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Every request of a supported Exasol version passes the strict type check

* *GIVEN* Exasol 8.29.x or later, as verified on 8.29.13 and 2025.1.16
* *WHEN* a query over a virtual table sends the adapter a `pushdown` request
* *THEN* the request SHALL carry `selectListDataTypes` with one complete `dataType` per select-list item, and the adapter SHALL plan it without a type error
* *AND* a TIMESTAMP `dataType` without `fractionalSecondsPrecision` SHALL be valid and mean precision 3, because Exasol 8.x omits the field, and an absent `withLocalTimeZone` SHALL mean `false`
* *AND* a CHAR or VARCHAR `dataType` without `characterSet` SHALL be valid and mean UTF8, because Exasol sends the field only for ASCII
* *AND* an absent or JSON-null `selectList`, which Exasol sends for `SELECT *` with one `selectListDataTypes` entry per table column, SHALL skip the count check
* *AND* an empty `selectList` with an empty `selectListDataTypes`, which Exasol sends for `SELECT RANDOM() FROM <vs>.<table>`, SHALL pass the count check
* *AND* the query SHALL return as many rows as native Exasol returns for it
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Projected CAST expression preserves the declared TIMESTAMP fractional-seconds precision in its EMITS type

* *GIVEN* a row-scan `pushdown` request whose select list carries an expression item, for example `CAST(c_ts AS TIMESTAMP(6))`, whose parallel `selectListDataTypes` entry is `{"type":"TIMESTAMP","fractionalSecondsPrecision":6}`
* *WHEN* the adapter derives the scalar scan UDF's EMITS column type for that item
* *THEN* the derived EMITS type SHALL be `TIMESTAMP(6)`, reading the precision from the `fractionalSecondsPrecision` field, Exasol's documented data-type field for a TIMESTAMP's fractional-seconds precision (default 3), verified against Exasol's virtual-schema data-type API; a `precision` or `scale` key on a TIMESTAMP `dataType` MUST NOT set its precision
* *AND* when `fractionalSecondsPrecision` is absent the derived EMITS type SHALL be bare `TIMESTAMP`, equivalent to Exasol's default `TIMESTAMP(3)`
* *AND* a `withLocalTimeZone: true` dataType SHALL be `TIMESTAMP(p) WITH LOCAL TIME ZONE`, which the scan cannot emit, so the item routes per `pushdown-capabilities/pushdown-planning-capability-extensions`
* *AND* the EMITS type and the CAST target the adapter renders for the same item (`sql-comprehension/vs-expression-translator-cast`) SHALL carry the same precision, because Exasol's `EXPLAIN VIRTUAL` type check (`Data type mismatch ... Expected TIMESTAMP(6), but got TIMESTAMP(3)`, SQL error 04000) compares the outer query's expected column type against the EMITS-declared type; this scenario governs the pushed-down CAST expression's declared target type, not a raw column's `createVirtualSchema` declaration, which `scan-types/type-mapping` governs
<!-- /DELTA:CHANGED -->
