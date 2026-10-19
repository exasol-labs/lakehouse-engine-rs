# Decisions: add-delta-scan-execution

## ADR: Delta deletion vectors are decoded by `delta_kernel`, not hand-decoded

**ID:** delta-deletion-vectors-decoded-by-delta-kernel
**Plan:** `add-delta-scan-execution`
**Status:** Accepted

### Context

Delta deletion vectors store a roaring bitmap in a binary format with three storage types. The Delta kernel library, already a dependency, decodes all three.

### Decision

The scan decodes deletion vectors with the kernel decoder, which returns the same bitmap type the Iceberg positional-delete path uses.

### Options Considered

| Option | Verdict |
|--------|---------|
| Hand-decode against the protocol with `roaring` | Rejected: re-derives framing, checksum, and inline encoding that the kernel already validates |
| Use the kernel's full scan pipeline | Rejected: see the next ADR |

### Consequences

- No hand-rolled decoder can diverge from the kernel and return silently wrong rows.

## ADR: The deletion-vector decoder is fed pre-fetched bytes, never a live storage client

**ID:** deletion-vector-decoder-fed-prefetched-bytes
**Plan:** `add-delta-scan-execution`
**Status:** Accepted

### Context

The kernel's deletion-vector decoder needs a storage handler to fetch sidecar bytes. DataFusion is the engine's only execution engine, and a second one inside the UDF competes for the bounded memory pool.

### Decision

The scan fetches each sidecar on the connection-limited asynchronous path that Iceberg delete files use. It gives the decoder a read-only in-memory storage handler serving those bytes. Every other handler operation returns a clean error and never panics, because a panic in a UDF is an abnormal VM exit that makes the engine kill every sibling VM.

### Options Considered

| Option | Verdict |
|--------|---------|
| Build a kernel `DefaultEngine` and use its object-store handler | Rejected: the decoder is synchronous and the default handler starts a second async runtime inside a memory-bounded UDF |
| Construct the kernel's `ObjectStoreStorageHandler` directly | Rejected: its constructor is not public |

### Consequences

- A sidecar shared across data files is read once per shard.
- Object-store I/O stays within the shared connection budget.

## ADR: Partition columns are materialized through DataFusion's native partition-column mechanism

**ID:** partition-columns-via-datafusion-native-mechanism
**Plan:** `add-delta-scan-execution`
**Status:** Accepted

### Context

Delta does not store partition column values in data files, so the scan must materialize them from the log's partition values. The mechanism must work with deletion filtering and with filtering and grouping on a partition column.

### Decision

The scan splits the logical schema into file fields and partition fields and uses DataFusion's native partition-column support, setting each file's partition values from its file entry. Projection indices are remapped so the declared column order is kept.

### Options Considered

| Option | Verdict |
|--------|---------|
| Extend the field-id expression adapter's absent-column defaults with partition literals | Rejected: the factory is built once per scan and does not see which file it serves |
| Rewrite the emitted batch after the scan | Rejected: a filter or GROUP BY on a partition column would see NULLs, and Exasol does not re-apply delegated filters |
| Append partition columns and restore order with a `ProjectionExec` | Rejected: the index remap alone restores order |

### Consequences

- A partition column works as a predicate target and group key with no extra plan node.

## ADR: One per-request scan-source resolver, matching the catalog kind at a single site

**ID:** one-per-request-scan-source-resolver
**Plan:** `add-delta-scan-execution`
**Status:** Accepted

### Context

The single-table path and every join leg must resolve a table's scan against an Iceberg REST or Unity Catalog session without forking on catalog kind per operation.

### Decision

A per-request scan resolver holds the request's one catalog session and matches the catalog kind exhaustively at one site. The single-table path and every join leg call it.

### Options Considered

| Option | Verdict |
|--------|---------|
| A `CatalogKind` branch at each call site | Rejected: this is the per-operation fork the single-site rule forbids |
| A session per resolved table | Rejected: doubles a two-leg join's catalog authentication round-trips |
| Extend `construct_catalog_client` | Rejected: it returns an abstract client, but the Unity Delta source needs the concrete session |

### Consequences

- The resolver stays out of the pushdown facade, which keeps that public surface smaller.

