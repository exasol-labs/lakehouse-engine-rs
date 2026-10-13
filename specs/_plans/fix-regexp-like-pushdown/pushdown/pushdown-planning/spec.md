# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it captures
the requested projection, filter, LIMIT, and any supported aggregate, and emits the SQL
that drives the DataFusion scan over the table identity, file list, byte sizes,
delete-file references, and logical schema resolved once by
`file-planning/pushdown-planning-file-resolution`. Cluster fan-out is separated from the
scan: a nested `LAKEHOUSE_DISTRIBUTE_FILES` LUA SET distributor subquery (`GROUP BY
shard_key`) spreads each shard's per-file list across nodes, and an outer ungrouped
`LAKEHOUSE_SCAN` SCALAR EMIT UDF scans each distributed file list node-locally and streams
the rows. The scan-driving SQL splices the shard-invariant parts (projection, filter,
LIMIT, logical schema, a storage-credential REFERENCE, and the table root) once as the scalar scan
UDF's first-argument common literal and flows each shard's per-file subset through the
distributor as the second argument. A single-shard plan short-circuits the distributor and
calls the scalar scan directly on the file-list literal. See
`file-planning/pushdown-planning-file-encoding` for the table-root-once and relative/absolute
path encoding rules. See `pushdown-aggregates/pushdown-planning-nested-aggregate-fallback` for the
guard against composed requests (e.g. an outer aggregate over an inner grouped-aggregate
sub-select) that don't map onto the source table's own columns. Single-group aggregate
pushdown (capability advertisement, partial-aggregate scan-spec translation, wrapper merge
SQL, and AVG sum/count decomposition) is covered separately in
`pushdown-aggregates/pushdown-planning-single-group-agg`.

<!-- DELTA:CHANGED -->
## Background

* The scan-driving SQL invokes the `LAKEHOUSE_SCAN` SCALAR EMIT UDF over a nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor subquery; the shard-invariant common spec (projection, filter, LIMIT, aggregates, group keys, logical schema, EMITS types, a storage-credential reference (or, under vending, a sealed envelope), tuning knobs, and the table root) is spliced once as the scalar scan's first argument and each shard's file subset flows through the distributor as the second argument.
* The outer scalar scan select is never wrapped in a `SELECT * FROM (...)` materialization boundary.
* Credential reference: the common spec carries a CONNECTION name or sealed envelope, never a plaintext credential — see `storage-access/scan-spec-credential-reference`.
* The `LAKEHOUSE_SCAN` and `LAKEHOUSE_DISTRIBUTE_FILES` UDF names in the scan-driving SQL are schema-qualified from the schema of the running adapter script, read from the UDF handshake via `ctx.script_schema()`; there is no VS property that supplies this schema. The scan and distributor scripts are co-deployed in the adapter script's schema, so this single source qualifies both.
* The cluster node count that sizes the shard fan-out is read per pushdown from the adapter script's own UDF handshake via `UdfContext::node_count()`. It is NOT read from `schemaMetadataInfo.adapterNotes`. Every VS request type reaches the adapter through the same single-call script invocation, so the handshake carries the node count on a `pushdown` request exactly as it does on a `createVirtualSchema` request; the request type lives in the JSON payload, not in the handshake.
* `node_count()` is a synchronous handshake read and MUST be captured in `dispatch` before the tokio runtime is entered, alongside `ctx.script_schema()` and the resolved CONNECTION credentials. The value is then threaded into the pushdown planning path as a plain integer, so the async planning code performs no ambient context read of its own.
* `node_count()` returns `0` only on a context carrying no live handshake metadata (a stub, a test double, or a broken handshake), so `0` maps to a node count of `1` and any live cluster (single-node included) reports `≥ 1`.
* The common spec's `projection` field carries the pushed-down projected columns ONLY for the row-scan and join dispatch paths. An aggregate or GROUP BY request leaves `projection` empty, because the aggregate scan-dispatch path derives its physical projection from the `aggregates`/`group_keys` fields rather than from `projection` (see `pushdown-aggregates/pushdown-planning-single-group-agg` and `pushdown-grouped-agg/pushdown-planning-grouped-agg`).
* See `pushdown-aggregates/pushdown-planning-single-group-agg` for single-group aggregate pushdown (capability advertisement, partial-aggregate translation, wrapper merge SQL, and AVG decomposition).
* A predicate node the adapter cannot faithfully translate MUST be applied by the adapter's own returned SQL, because Exasol does not re-apply a predicate whose capability the adapter advertised — see `pushdown/pushdown-declined-filter-self-apply` and ADR `specs/_decision/045`. Omitting it returns extra unfiltered rows, verified live.
* The single-table path distinguishes an ABSENT filter from a DECLINED one. An absent or trivially-true filter is omitted and the wrapper-free fast scan is unchanged; a declined filter routes the request to the qualified single-table wrapper, which applies the predicate in its own `WHERE`.
* The wrapper-free outer scalar scan select remains the shape for every request whose filter renders. The `SELECT * FROM (…)` boundary the wrapper introduces exists only on the decline path.
* See `pushdown-types/pushdown-planning-like-type-coercion` for the type-aware LIKE rule that dispatches on the subject column's Exasol type before rendering the filter.
* When a query aliases a table in its `FROM` clause (`FROM customer c`), Exasol stamps a `tableAlias` on every `column` node in the pushdown request — including nodes the user wrote unqualified. The single scan relation exposes only bare column names, so an alias-qualified reference does not resolve; the single-table push therefore strips the alias before rendering (see `pushdown/pushdown-planning-alias-stripping`). The `crates/vs-expression` translator itself always honors a present `tableAlias` (`sql-comprehension/vs-expression-translator`); stripping is the single-table caller's responsibility.
* The plan-time file-pruning predicate this feature dispatches to has a per-format owner: for the Iceberg reader it is `iceberg::expr::Predicate`, owned by `file-planning/pushdown-file-pruning`; for the Delta reader it is the Delta stats predicate, owned by `delta/delta-file-pruning`. A reader of this feature's `## Scenarios` clauses naming `iceberg::expr::Predicate` should read those clauses as describing the Iceberg arm of the rule, not the whole rule.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Filter predicate is pushed into the scan spec

* *GIVEN* a query with a WHERE predicate over a supported column and operator
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL translate the predicate into the shard-invariant common spec passed to the UDF, and the translation SHALL be ALL-OR-NOTHING over the whole top-level filter
* *AND* a filter the adapter cannot render for DataFusion SHALL be self-applied in the qualified wrapper's `WHERE` rather than omitted, per `pushdown/pushdown-declined-filter-self-apply`
* *AND* before translating a `predicate_like` whose subject is a bare `column` node, the adapter SHALL apply the type-aware LIKE rule (see `pushdown-types/pushdown-planning-like-type-coercion`), because DataFusion performs no implicit non-string-to-VARCHAR coercion and would hard-fail the scan on a LIKE over a non-string column
* *AND* the adapter SHALL ALSO translate the soundly-translatable conjuncts into an `iceberg::expr::Predicate` applied to the Iceberg table scan as a file-pruning filter, dropping any node it cannot translate soundly rather than skipping a file that could match
* *AND* the DataFusion scan SHALL always apply the full common-spec filter, so the Iceberg pruning filter only narrows which files are opened and never changes the result set
<!-- /DELTA:CHANGED -->
