# Decisions: change-broadcast-join-collect-left

## ADR: The broadcast join builds one hash table from the whole dimension side

**ID:** broadcast-join-builds-one-hash-table
**Plan:** change-broadcast-join-collect-left
**Status:** Accepted

### Context

The hash join passes the dimension side's join keys to the fact scan as a runtime filter. Parquet statistics can skip a fact row group only when that filter is a plain predicate over the join key.

### Decision

The broadcast join scan collects the whole dimension side into one hash table (DataFusion `CollectLeft` mode). The fact scan then receives plain key bounds and a key set, and skips the row groups outside them.

### Options Considered

| Option | Verdict |
|--------|---------|
| One hash table per hash partition, DataFusion's default above one target partition | Rejected: the fact scan receives one filter per partition, which skips no row group and adds row-level filtering cost |

### Consequences

- One thread builds the hash table. With a 272 MB dimension side the query still ran faster than with the partitioned build.
- A fact table clustered by the join key gains the most: a selective join skipped 248 of 256 row groups and ran 6.5 times faster.
