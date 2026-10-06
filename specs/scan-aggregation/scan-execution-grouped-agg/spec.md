# Feature: DataFusion Scan Execution — Grouped Aggregates and Memory Safety

Extends `datafusion-scan/scan-execution` with the bounded `RuntimeEnv` memory pool
(sized from the per-instance UDF memory limit with a spill backstop) and grouped
partial aggregate execution (one partial row per distinct user group per shard).

## Background

* The per-instance memory limit is read from `ctx.memory_limit()` (bytes; `0` =
  unbounded/unknown sentinel). Exasol enforces the same per-process heap limit via
  `setrlimit(RLIMIT_RSS)` and stalls additional concurrent VMs once usage reaches 80%
  of it, so a pool sized under the limit lets the engine self-manage concurrency.
* The spill backstop directory is `/tmp`; the UDF probes at runtime whether `/tmp` is
  real disk with free space (tmpfs detection via `/proc/mounts` plus `statvfs`
  free-space check). Any `/tmp` spill is transient per-invocation scratch — NOT
  persistent state.
* A grouped scan spec carries `group_keys` (rendered DataFusion SQL fragments) and
  partial-aggregate instructions. The scan emits one partial row per distinct user
  group per shard; the adapter's outer wrapper SQL re-groups and merges the partials.
* The grouped partial-aggregate path registers the full logical table schema and builds
  its DataFusion query from the `group_keys` and `aggregates` fields. It does not consult
  the scan spec's `projection` field; DataFusion's own projection pushdown derives the
  physical column set from the grouped partial-aggregate query text.
* The `group_keys` list MAY contain two or more rendered fragments; the DataFusion
  GROUP BY is built over all of them, so the per-shard partial key space is the
  product of the distinct values of every group key.
* The bounded memory pool and `/tmp` spill backstop apply uniformly regardless of the
  number of group keys.
* LIMIT is never applied inside a grouped partial scan.

## Scenarios

### Scenario: DataFusion memory pool is sized from the per-instance memory limit

* *GIVEN* a scan UDF invocation whose `ctx.memory_limit()` reports a non-zero per-instance limit in bytes
* *WHEN* the UDF builds its DataFusion `RuntimeEnv`
* *THEN* the UDF SHALL size the `MemoryPool` to a fraction (~0.6) of the reported per-instance limit
* *AND* the UDF MUST NOT size the pool to the full reported limit, leaving headroom below the engine's 80% stall threshold

### Scenario: Default memory budget is used when the limit is unknown

* *GIVEN* a scan UDF invocation whose `ctx.memory_limit()` returns the `0` sentinel (no limit reported / accessor unavailable)
* *WHEN* the UDF builds its DataFusion `RuntimeEnv`
* *THEN* the UDF SHALL size the `MemoryPool` to a conservative default budget (e.g., 1024 MB)
* *AND* the UDF MUST NOT run with an unbounded memory pool

### Scenario: Spill to disk is enabled when /tmp is real disk with free space

* *GIVEN* a scan UDF invocation whose `/tmp` probe reports a real (non-tmpfs) filesystem with sufficient free space
* *WHEN* the UDF builds its DataFusion `RuntimeEnv`
* *THEN* the UDF SHALL configure a `FairSpillPool` together with a `DiskManager` rooted at `/tmp`
* *AND* a memory-intensive grouped or sorted scan SHALL complete by spilling to disk rather than failing, at any group cardinality

### Scenario: Clean ResourcesExhausted error when no spill disk is available

* *GIVEN* a scan UDF invocation whose `/tmp` probe reports tmpfs or insufficient free space
* *WHEN* a scan exceeds the bounded memory pool
* *THEN* the UDF SHALL configure a `GreedyMemoryPool` with no spill
* *AND* an over-budget scan SHALL return a clean DataFusion `ResourcesExhausted` error rather than OOM-crashing the UDF process

### Scenario: Grouped partial aggregate computes per-group partial results per shard

* *GIVEN* a scan spec carrying group-key expressions and partial-aggregate instructions
* *AND* the files assigned to this shard contain rows belonging to multiple distinct groups
* *WHEN* the scan UDF runs for that spec
* *THEN* the UDF SHALL register only its assigned files, apply any pushed-down filter, and execute a DataFusion GROUP BY query using the rendered group-key expressions and partial aggregate functions
* *AND* the UDF SHALL emit one partial-aggregate row per distinct group found in the shard, carrying the group-key column values followed by the partial aggregate values
* *AND* a shard with no matching rows SHALL emit zero rows (not a NULL row)

### Scenario: Grouped partial row layout matches wrapper SQL column contract

* *GIVEN* a scan spec with N group-key expressions and M aggregate plans
* *WHEN* the scan UDF emits grouped partial rows
* *THEN* the first N columns of each emitted row SHALL carry the group-key values in spec order
* *AND* the remaining columns SHALL carry the partial aggregate values in the same PARTIAL_* naming convention used by the single-group path
* *AND* the EMITS declaration in the scan-driving SQL MUST match this layout exactly

### Scenario: LIMIT is not applied inside the grouped partial scan

* *GIVEN* a scan spec for a grouped aggregate (group_keys is non-empty)
* *WHEN* the scan UDF builds its DataFusion query
* *THEN* the UDF MUST NOT apply any LIMIT to the per-group partial scan
* *AND* the limit field in the scan spec for a grouped query SHALL be None (enforced by the adapter)

### Scenario: Group-key expressions are pushed into the DataFusion GROUP BY clause verbatim

* *GIVEN* a scan spec carrying a group_keys list of rendered DataFusion SQL fragments (e.g., `["\"REGION\"", "YEAR(\"ORDER_DATE\")"]`)
* *WHEN* the scan UDF builds the DataFusion SQL for the partial aggregate
* *THEN* each group-key fragment SHALL appear verbatim in the DataFusion GROUP BY clause
* *AND* each group-key fragment SHALL also appear in the SELECT list so its value is emitted as a group-key column

### Scenario: High-cardinality multi-key grouped scan completes under the bounded memory pool

* *GIVEN* a grouped scan spec whose `group_keys` list has two or more elements and whose shard's assigned files contain many distinct multi-key group combinations (a larger key space than any single key alone)
* *WHEN* the scan UDF runs the per-shard DataFusion GROUP BY over all group keys
* *THEN* the scan SHALL emit one partial row per distinct multi-key group observed in the shard, with each group key's value in its own `GK_{i}` column
* *AND* when `/tmp` is real disk with free space the grouped scan SHALL complete by spilling rather than failing, at any multi-key group cardinality
* *AND* when no spill disk is available the scan SHALL return a clean `ResourcesExhausted` error rather than OOM-crashing the VM

### Scenario: Grouped partial aggregate physically reads only the group-key and aggregate columns

* *GIVEN* a grouped partial-aggregate scan over a multi-column Parquet file that groups on one column and aggregates another (e.g. `GROUP BY region` with `SUM(score)` over a table of `id`, `region`, `score`, `name`)
* *AND* a scan spec whose `projection` field is empty
* *WHEN* the scan UDF builds the DataFusion physical plan for the grouped partial aggregate
* *THEN* the physical Parquet scan SHALL project ONLY the group-key and aggregate-referenced columns, never the full column set
* *AND* the empty `projection` field SHALL NOT cause a full-column read, because DataFusion derives the physical projection from the grouped partial-aggregate query text rather than from the scan spec's `projection` field
