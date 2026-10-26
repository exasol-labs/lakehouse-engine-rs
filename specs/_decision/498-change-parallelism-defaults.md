# Decisions: change-parallelism-defaults

## ADR: One scan shard per node, with DataFusion threads on every core

**ID:** one-scan-shard-per-node-with-threads-on-every-core
**Plan:** change-parallelism-defaults
**Status:** Accepted

### Context

The scan is a scalar UDF, so Exasol runs one scan VM per node and feeds it that node's shards one after another. The VM pool that multiplexes SET groups across cores does not apply to it.

### Decision

`PARALLELISM_FACTOR` defaults to 1, so each node gets one shard. In `AUTO` mode each scan instance runs one DataFusion thread per core and twice as many target partitions, independent of the factor.

### Options Considered

| Option | Verdict |
|--------|---------|
| Oversubscribed shards, threads = cores / factor | Rejected: the extra shards run serially, so each adds startup cost and the factor divides away all but one core |
| A SET-based scan that revives the VM pool | Rejected: raw-row output would materialize in temp DB instead of streaming |

### Consequences

- Per-node CPU parallelism and memory both come from one instance per node.
- A table with very uneven file sizes can leave one node as the long pole. A factor of 2 hedges against it.
