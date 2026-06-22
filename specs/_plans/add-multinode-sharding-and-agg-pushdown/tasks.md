# Tasks: add-multinode-sharding-and-agg-pushdown

## Group A: Cluster-node capture + partition module
- [x] 1.1 Connect-back `SELECT NPROC()` in createVirtualSchema path [expert]
- [x] 1.2 Default CLUSTER_NODES to 1 on connect-back/query failure; never fail create [expert]
- [x] 1.3 Emit CLUSTER_NODES in createVirtualSchema response props; redact credentials on connect-back error [expert]
- [x] 2.1 Pure partition_files(files, n) -> Vec<Vec<String>> (balanced, disjoint, cap at file_count, no empty shard) [expert]
- [x] 2.2 Unit tests: disjointness, coverage, balance, fewer-files-than-nodes, n=1, empty input

## Group B: IPROC fan-out SQL + aggregate detection/ScanSpec
- [x] 3.1 Build derived-VALUES + GROUP BY IPROC(), shard_key scan-driving SQL (one UDF call per shard) [expert]
- [x] 3.2 Collapse to existing single-invocation query shape when one shard [expert]
- [x] 3.3 Unit tests: per-shard spec literals, IPROC grouping presence, single-shard equivalence
- [x] 4.1 Extend ScanSpec with optional aggregates plan (kind: Count|CountCol|Sum|Min|Max|Avg, column) [expert]
- [x] 4.2 Detect supported single-group aggregate select-list; produce plan or fall back (GROUP BY/DISTINCT/HAVING) [expert]
- [x] 4.3 Advertise aggregate capabilities in adapter::capabilities; update test asserting NO agg caps
- [x] 4.4 Unit tests for detection -> plan translation and fallback

## Group C: UDF partial emit + merge wrapper SQL
- [x] 5.1 When spec carries agg plan, run partial aggregate in DataFusion, emit one partial row per shard [expert]
- [x] 5.2 COUNT summable, SUM summable, MIN/MAX re-min/max-able; empty shard -> count 0, NULL sum/min/max [expert]
- [x] 5.3 AVG as (partial_sum, partial_count) pair with NULL-excluding count; never per-shard average [expert]
- [x] 5.4 Unit/integration tests for each partial form, including empty-shard row
- [x] 6.1 Wrap shard fan-out in outer merge: SUM(count), SUM(sum), MIN(min), MAX(max) [expert]
- [x] 6.2 AVG wrapper: SUM(partial_sum)/SUM(partial_count) with count=0 -> NULL guard [expert]
- [x] 6.3 Unit tests: merge SQL shape, AVG division/zero-guard

## Group D: E2E + dead code
- [x] 7.1 E2E: createVirtualSchema stores CLUSTER_NODES property >= 1
- [x] 7.2 E2E: each aggregate (COUNT(*), COUNT(col), SUM, MIN, MAX, AVG) returns correct merged value, with/without WHERE
- [x] 7.3 E2E: multi-shard row query returns same row set as single-shard path
- [x] 8.1 Remove capabilities test asserting aggregates absent; update create-VS test expectations to include CLUSTER_NODES

## Code Review Fixes
- [x] R.1 BLOCKER: SUM/MIN/MAX EMITS type must match column type (DATE/TIMESTAMP/DECIMAL), not always DOUBLE; or fall back to row scan for non-numeric agg targets [expert]
- [x] R.2 SHOULD-FIX: multi-shard row-scan SQL must append outer LIMIT (correctness backstop invariant) [expert]
- [x] R.3 SHOULD-FIX: rename stale test reports_projection_filter_limit_only; fix stale "Group C extension point" doc comment; drop dense inline section comments in handle_pushdown

## Verification
- [x] V.1 make cross-musl-udf-build -> exit 0
- [x] V.2 cargo test -> 0 failures
- [x] V.3 cargo clippy --all-targets -> 0 warnings
- [x] V.4 cargo fmt -> no changes
