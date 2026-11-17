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

<!-- DELTA:CHANGED -->
### Scenario: Filter predicate is pushed into the scan spec

* *GIVEN* a query with a WHERE predicate over a supported column and operator
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL translate the predicate into the shard-invariant common spec passed to the UDF, and SHALL translate a filter that does not render whole one top-level `AND` conjunct at a time
* *AND* a conjunct the adapter cannot render for DataFusion SHALL be self-applied in the qualified wrapper's `WHERE` rather than omitted, while the rendered conjuncts stay in the scan, per `pushdown/pushdown-declined-filter-self-apply`
* *AND* the scan SHALL compute only the conjuncts that `pushdown-types/pushdown-planning-scan-text-conversion` allows, because DataFusion performs no implicit conversion to text
* *AND* the adapter SHALL ALSO prune files by the conjuncts it can translate soundly (`file-planning/pruning-predicate-translation`), ignoring any other node rather than skipping a file that could match
* *AND* the scan SHALL always apply its whole scan filter, so pruning only narrows which files are opened and never changes the result set
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A declined WHERE filter routes the single-table request to the qualified wrapper

* *GIVEN* a single-table `pushdown` request carrying a non-null `filter` with at least one top-level conjunct that the DataFusion-bound render declines, of any dispatch shape, row scan, top-N, single-group aggregate, grouped aggregate, or `COUNT(DISTINCT)`
* *WHEN* the adapter builds the pushdown SQL
* *THEN* the adapter SHALL return the qualified single-table wrapper for every dispatch shape, and the wrapper's raw fan-out SHALL carry the filter's rendered conjuncts as its scan filter
* *AND* file pruning SHALL still use every conjunct of the request's filter, declined or not, and remove only files that provably cannot match
* *AND* the wrapper's returned column count, order, and declared types SHALL equal what the request's `selectList` declares: the full base row for an absent or JSON-null `selectList`, and one column for an empty one, so the route never trips Exasol's positional `04000` validation
* *AND* a request whose filter renders, or which carries no filter, SHALL take its existing dispatch shape with the emitted SQL byte-identical to its pre-change output
<!-- /DELTA:CHANGED -->
