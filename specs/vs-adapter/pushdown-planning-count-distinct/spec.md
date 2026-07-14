# Feature: Pushdown Planning — Single-Group COUNT(DISTINCT)

Extends single-group aggregate pushdown (`vs-adapter/pushdown-planning`) so a
`COUNT(DISTINCT col)` over the whole table (no GROUP BY) is decomposed into a
shard-associative partial/merge plan instead of forcing a full raw row-scan fallback.
Each shard computes its LOCAL distinct value set inside DataFusion and emits it as one
VARCHAR partial value; a dedicated scalar merge UDF unions the per-shard sets and returns
the final distinct count. Execution is bounded by an explicit per-shard cap so a
high-cardinality column fails cleanly rather than exhausting memory or overflowing the
wire value.

## Background

* Only the single-group (no GROUP BY) `COUNT(DISTINCT col)` case is in scope. A
  `COUNT(DISTINCT ...)` that appears inside a GROUP BY request MUST still cause the grouped
  detection to decline (fall back to row scanning); it is explicitly out of scope.
* The distinct-set wire form is a JSON array string carried as one VARCHAR partial value
  per shard, consistent with the project's "incompatible Arrow type → VARCHAR via JSON"
  convention; no Arrow type crosses the `.so` boundary.
* `COUNT(DISTINCT col)` excludes NULLs, matching single-node SQL semantics: the per-shard
  local distinct set MUST NOT contain a NULL element.
* The merge is performed by an ordinary scalar function call mixed into the same outer
  wrapper SELECT that merges SUM/MIN/MAX/COUNT partials — the adapter MUST NOT generate
  bespoke SQL string-splitting or `CONNECT BY` hierarchical rewrites (an explicit non-goal).
* Execution is bounded: a per-shard distinct set that exceeds the safety cap MUST produce a
  clean bounded-resource error, never an OOM crash or a silently truncated (wrong) value.
* Credentials MUST NOT appear in any returned SQL or error message.
* The scalar merge UDF `LAKEHOUSE_DISTINCT_MERGE_COUNT` is declared `RETURNS DECIMAL(20,0)`;
  under SDK-0.21.0 it MUST produce its result through the RETURNS path, not `ctx.emit`.

## Scenarios

### Scenario: Single-group COUNT(DISTINCT) is decomposed into per-shard local distinct sets

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
* *AND* a query whose select list includes `COUNT(DISTINCT col)` over the whole table with no GROUP BY, e.g. `SELECT COUNT(DISTINCT L_SHIPMODE) FROM {vs_table}`
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL recognise the distinct aggregate, resolve the file list once, and build a scan spec instructing each shard to compute the LOCAL distinct value set of that column inside DataFusion
* *AND* each shard SHALL emit that local distinct set as exactly one VARCHAR partial value encoded as a JSON array, excluding NULLs, preserving the one-row-per-shard partial wire shape
* *AND* no Arrow type SHALL cross the `.so` boundary

### Scenario: Scalar merge UDF unions per-shard distinct sets into the final count

* *GIVEN* an aggregate pushdown over a file list partitioned into one or more shards, each emitting its local distinct-set JSON array for a `COUNT(DISTINCT col)`
* *WHEN* the adapter builds the outer wrapper SQL
* *THEN* the outer wrapper SHALL feed the per-shard distinct-set partial values into a dedicated scalar merge UDF via an ordinary scalar function call (fed the concatenation of the per-shard JSON arrays), mixed into the same merge SELECT as the SUM/MIN/MAX partials
* *AND* the scalar merge UDF SHALL parse the per-shard arrays, union their elements into a single distinct set, and return that set's cardinality
* *AND* the merged distinct count SHALL equal `COUNT(DISTINCT col)` evaluated over all rows on a single node, including when the same value appears in more than one shard (deduplicated across shards) and when the column contains NULLs (NULLs never counted)
* *AND* an empty table SHALL yield a distinct count of zero

### Scenario: High-cardinality COUNT(DISTINCT) fails cleanly under the safety cap

* *GIVEN* a `COUNT(DISTINCT col)` pushdown over a column whose per-shard local distinct set exceeds the configured safety cap (a maximum distinct-element count and a maximum serialized-byte size, the latter kept safely below the `VARCHAR(2000000)` wire limit)
* *WHEN* a shard computes its local distinct set and the cap is exceeded
* *THEN* the scan UDF SHALL abort that shard with a clean bounded-resource error naming the offending column and the cap that was exceeded
* *AND* the UDF MUST NOT emit a truncated distinct set (which would yield a wrong count) and MUST NOT continue accumulating until the process runs out of memory
* *AND* the error message MUST NOT contain any credential value

### Scenario: Multiple COUNT(DISTINCT) columns in one query merge independently

* *GIVEN* a query selecting several `COUNT(DISTINCT col)` aggregates over different columns in one select list (optionally alongside SUM/MIN/MAX/COUNT and expression-argument aggregates), e.g. Q9b
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL assign each distinct aggregate its own per-shard VARCHAR partial column and its own scalar-merge call in the outer wrapper, so each `COUNT(DISTINCT)` is merged independently
* *AND* each merged distinct count SHALL equal the corresponding `COUNT(DISTINCT col)` evaluated over all rows on a single node

### Scenario: Distinct-merge UDF returns its count via the RETURNS path without emitting

* *GIVEN* the `LAKEHOUSE_DISTINCT_MERGE_COUNT` scalar UDF declared `RETURNS DECIMAL(20,0)` in its DDL, fed the concatenation of the per-shard distinct-set JSON arrays for one `COUNT(DISTINCT col)`
* *WHEN* the merge UDF computes the global distinct cardinality for its one input value
* *THEN* the UDF SHALL produce its result through the SDK-0.21.0 RETURNS path as `Ok(Some(count))` and MUST NOT call `ctx.emit`, which the SDK-0.21.0 runtime rejects in RETURNS (`ExactlyOnce`) context
* *AND* a SQL NULL input (a `LISTAGG` over zero shard rows) SHALL return `Ok(Some(0))`, a distinct count of zero
* *AND* the merged distinct count SHALL remain byte-identical to the value the prior EMITS-based implementation produced, so the conformance fix changes the output mechanism, not the result
