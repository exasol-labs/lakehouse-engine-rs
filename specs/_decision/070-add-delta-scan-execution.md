# Decisions: add-delta-scan-execution

## ADR: Delta deletion vectors are decoded by `delta_kernel`, not hand-decoded

**ID:** delta-deletion-vectors-decoded-by-delta-kernel
**Plan:** `add-delta-scan-execution`
**Status:** Accepted

### Context

Delta deletion vectors store a roaring bitmap in a binary format with three storage types. The workspace already depends on `delta_kernel` and `roaring`, and the kernel's decoder handles all three types.

### Decision

The scan decodes deletion vectors with the `delta_kernel` decoder, which returns the same `roaring` bitmap type the Iceberg positional-delete path already uses.

### Options Considered

| Option | Verdict |
|--------|---------|
| Hand-decode against the protocol with `roaring` | Rejected: re-derives framing, magic, CRC-32, and the Z85 inline form that the kernel already validates |
| Use the kernel's full scan pipeline | Rejected: see the next ADR |

### Consequences

A divergence between a hand-rolled decoder and the kernel would cause silent wrong rows. Using the kernel decoder removes that risk and adds no dependency.

## ADR: The deletion-vector decoder is fed pre-fetched bytes, never a live storage client

**ID:** deletion-vector-decoder-fed-prefetched-bytes
**Plan:** `add-delta-scan-execution`
**Status:** Accepted

### Context

The kernel's deletion-vector decoder needs a `StorageHandler` to fetch sidecar bytes. DataFusion is the engine's only execution engine, and a second one inside the UDF competes for the bounded memory pool.

### Decision

The scan fetches each sidecar on its own limiter-bounded asynchronous path, the same one used for Iceberg delete files. It gives the decoder a read-only in-memory `StorageHandler` adapter serving those bytes. Every other adapter operation returns a clean error and never panics, because a panic in a UDF is an abnormal VM exit that makes the engine SIGKILL every sibling VM of the statement part.

### Options Considered

| Option | Verdict |
|--------|---------|
| Build a kernel `DefaultEngine` and use its object-store handler | Rejected: the decoder is synchronous and the default handler starts a second Tokio runtime inside a memory-bounded UDF |
| Construct the kernel's `ObjectStoreStorageHandler` directly | Rejected: its constructor is `pub(crate)` |

### Consequences

A sidecar shared across data files is read once per shard, and object-store I/O stays on the shared connection-budget limiter.

## ADR: Partition columns are materialized through DataFusion's native partition-column mechanism

**ID:** partition-columns-via-datafusion-native-mechanism
**Plan:** `add-delta-scan-execution`
**Status:** Accepted

### Context

Delta does not store partition column values in data files, so the scan must materialize them from the log's partition values. The mechanism must compose with the delete pipeline's `ParquetAccessPlan` and with filtering and grouping on a partition column.

### Decision

The scan splits the logical schema into file fields and partition fields and uses DataFusion's native `table_partition_cols`, with each file's partition values set from its file entry. The table schema keeps declared order, and projection indices are remapped to the order the config uses.

### Options Considered

| Option | Verdict |
|--------|---------|
| Extend the field-id expression adapter's absent-column defaults with partition literals | Rejected: the factory is built once per scan and sees no file identity |
| Rewrite the emitted batch after the scan | Rejected: a filter or GROUP BY on a partition column would see NULLs, and Exasol re-applies nothing it delegated |
| Append partition columns and restore order with a `ProjectionExec` | Rejected: the index remap alone suffices |

### Consequences

A partition column works as a predicate target and group key with no extra plan node.

## ADR: One per-request scan-source resolver, matching the catalog kind at a single site

**ID:** one-per-request-scan-source-resolver
**Plan:** `add-delta-scan-execution`
**Status:** Accepted

### Context

The single-table pushdown path and every join leg must resolve a table's scan against an Iceberg REST or Unity Catalog session without a per-operation `CatalogKind` fork.

### Decision

A per-request `TableScanResolver` holds the request's one catalog session and matches `CatalogKind` exhaustively at one site. The single-table path and every join leg call it. This site replaces the pushdown refusal in the list of production sites permitted to name a `CatalogKind` variant, so the site count is unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| A `CatalogKind` branch at each call site | Rejected: the per-operation fork the one-construction-site rule forbids |
| A session per resolved table | Rejected: doubles a two-leg join's catalog authentication round-trips |
| Extend `construct_catalog_client` | Rejected: it returns a boxed `CatalogClient`, but the Unity Delta source needs the concrete session |

### Consequences

Keeping the resolver out of the pushdown facade shrinks the frozen surface by exactly one item.

## ADR: The Unity table identity is recovered by splitting the recorded dotted identifier

**ID:** unity-table-identity-recovered-by-splitting-dotted-identifier
**Plan:** `add-delta-scan-execution`
**Status:** Accepted

### Context

The resolver must recover a table's namespace segments and name for Unity Catalog, as it does for Iceberg, from the identifier `TABLE_MAP` records at create time.

### Decision

The resolver splits the recorded dotted identifier into namespace segments and table name for both kinds, and fails naming the identifier when no table name results. The Unity loader re-joins the segments, so the split round-trips losslessly.

### Options Considered

| Option | Verdict |
|--------|---------|
| Re-encode `TABLE_MAP` with explicit namespace segments | Rejected: changes a create-time wire format and forces a REFRESH story for existing virtual schemas |

### Consequences

The rule against re-splitting a joined identifier still applies to any future catalog kind that addresses tables by a different string, and the rejected option becomes right only then.
