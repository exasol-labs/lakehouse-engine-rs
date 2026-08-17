# Decisions: add-delta-scan-execution

## ADR: Delta deletion vectors are decoded by `delta_kernel`, not hand-decoded

**ID:** delta-deletion-vectors-decoded-by-delta-kernel
**Plan:** `add-delta-scan-execution`
**Status:** Accepted

### Context

Delta deletion vectors need decoding a roaring-bitmap binary. The workspace already depends on
`delta_kernel` 0.26 (used only for log-replay metadata) and `roaring` 0.11 (used by the Iceberg
positional-delete path). The investigation carried out against the vendored `delta_kernel` 0.26.0
source found `DeletionVectorDescriptor::read` plain `pub`, gated behind no feature flag, and it
handles all three storage types including the Z85 inline form.

### Decision

Decode through `delta_kernel::actions::deletion_vector::DeletionVectorDescriptor::read`, which
returns `roaring::RoaringTreemap`. `Cargo.lock` resolves exactly one `roaring` entry (0.11.4)
shared by `delta_kernel`, `iceberg`, and `lakehouse-engine`, so the returned bitmap is the same type
the Iceberg positional-delete path already feeds to `build_deletes_row_selection`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Decode through `delta_kernel`'s own `DeletionVectorDescriptor::read` | ✓ Chosen — the kernel's decoder validates the version byte, declared size, magic, and CRC-32; re-implementing that is new surface for zero gain |
| Hand-decode against the protocol with `roaring` | ✗ Rejected — re-derives container framing (version byte, `dataSize`, CRC-32), the portable magic, and the Z85 inline variant the workspace already carries a validated implementation of |
| Use the kernel's full scan pipeline | ✗ Rejected outright — see the pre-fetched-bytes ADR below |

### Consequences

A divergence between a hand-rolled decoder and the kernel's would be a silent wrong-rows bug; using
the kernel's own decoder removes that risk entirely and adds no new dependency.

## ADR: The deletion-vector decoder is fed pre-fetched bytes, never a live storage client

**ID:** deletion-vector-decoder-fed-prefetched-bytes
**Plan:** `add-delta-scan-execution`
**Status:** Accepted

### Context

`delta_kernel`'s deletion-vector decoder depends on a `StorageHandler` to fetch a sidecar's bytes.
DataFusion is this engine's only execution engine, and a UDF that hosts a second one competes for
the bounded memory pool the mission's self-throttling model depends on.

### Decision

The scan fetches each deletion-vector sidecar on its OWN asynchronous, limiter-bounded path — the
same path that already reads Iceberg delete files — and satisfies the kernel decoder's
`StorageHandler` dependency with a read-only in-memory adapter that serves those bytes. Every other
operation on the adapter (list, put, head, copy, delete) returns a clean error and never panics.

### Options Considered

| Option | Verdict |
|--------|---------|
| Pre-fetch bytes via the shared limiter; feed a read-only in-memory `StorageHandler` adapter | ✓ Chosen — preserves the read-once-per-shard property for a sidecar shared across data files, and keeps object-store I/O on the shared connection-budget limiter |
| Build a `delta_kernel` `DefaultEngine` inside the scan and use its object-store-backed storage handler | ✗ Rejected — the kernel's decoder is synchronous and its default handler drives its own Tokio background executor, starting a second runtime inside a memory-bounded UDF |
| Construct the kernel's `ObjectStoreStorageHandler` directly | ✗ Rejected — impossible: its constructor is `pub(crate)` |

### Consequences

`unimplemented!()` on the adapter's unused methods is forbidden — a panic inside a UDF is an
abnormal VM exit that makes the engine SIGKILL every sibling VM of the statement part, so every
unused method returns a clean error instead.

## ADR: Partition columns are materialized through DataFusion's native partition-column mechanism

**ID:** partition-columns-via-datafusion-native-mechanism
**Plan:** `add-delta-scan-execution`
**Status:** Accepted

### Context

Delta never writes a partition column's value into the data file, so the scan must materialize it
from the log-replayed `partitionValues` at read time. The scan side needed a mechanism that
composes with the delete pipeline's `ParquetAccessPlan` and with pushdown filtering/grouping on a
partition column, rather than reintroducing NULLs a filter or GROUP BY could see.

### Decision

Split the logical schema into file fields and partition fields at registration, pass the partition
fields as `table_partition_cols` on the `FileScanConfig`, and populate each `PartitionedFile`'s
`partition_values` from that file's `FileEntry`. Keep `TableProvider::schema()` in declared order
and remap the incoming projection indices into the `file ++ partition` order the config uses.

### Options Considered

| Option | Verdict |
|--------|---------|
| DataFusion's native `table_partition_cols` / `PartitionedFile.partition_values` mechanism | ✓ Chosen — per-file by construction, composes with the delete pipeline's `ParquetAccessPlan`, and lets DataFusion prune a file on its partition values for free |
| Extend `FieldIdExprAdapterFactory`'s absent-column default map with partition literals | ✗ Rejected — the factory is built once per scan and receives schemas, not file identity, so it cannot carry a per-file constant |
| Rewrite the emitted `RecordBatch` after the scan | ✗ Rejected — a filter or GROUP BY over a partition column would run against NULLs before the rewrite, and Exasol re-applies nothing it delegated, so this returns wrong rows |
| Append partition columns to the output and restore declared order with a `ProjectionExec` | ✗ Rejected — `FileScanConfig` applies projection indices in the order given, so the remap alone suffices with no extra plan node |

### Consequences

A partition column becomes usable as a predicate target and a group key with no extra plan node,
and neither of the two touched call sites is currently used elsewhere in this repo, so nothing
regresses.

## ADR: The partition-materialization feature is named format-neutrally

**ID:** partition-materialization-feature-named-format-neutrally
**Plan:** `add-delta-scan-execution`
**Status:** Accepted

### Context

An earlier, stale scaffold directory named the feature `scan-execution-delta-partition-values`. The
recorded `delta-table-planning` contract already states that the per-file partition-value map is
the SAME field an Iceberg identity-transform partition value (issue #99) and a future Hive-style
partition value would populate.

### Decision

The new spec is `datafusion-scan/scan-execution-partition-values`, not a Delta-named feature, and
its scenarios dispatch on whether `partition_columns` and `partition_values` are populated — never
on the table format.

### Options Considered

| Option | Verdict |
|--------|---------|
| `datafusion-scan/scan-execution-partition-values`, format-neutral | ✓ Chosen — matches the shared field ownership already recorded in `delta-table-planning` |
| `scan-execution-delta-partition-values`, mirroring the stale scaffold | ✗ Rejected — a Delta-named scan feature would invite a second, format-named home for one decision |

### Consequences

The deletion-vector feature keeps its Delta name for the opposite reason: the container framing and
the Z85 payload are the Delta protocol's, and an Iceberg Puffin deletion vector is a genuinely
different mechanism that stays refused.

## ADR: One per-request scan-source resolver, matching the catalog kind at a single site

**ID:** one-per-request-scan-source-resolver
**Plan:** `add-delta-scan-execution`
**Status:** Accepted

### Context

`handle_pushdown`'s single-table path and every join leg each needed to resolve a table's scan
against either an Iceberg REST catalog session or a Unity Catalog session, without reintroducing a
per-operation `CatalogKind` fork.

### Decision

Introduce a per-request `TableScanResolver` that holds the request's ONE catalog session (Iceberg
or Unity), matches `CatalogKind` exhaustively at exactly ONE site, and answers
`resolve(table_identifier, filter_json) -> ResolvedScan`. `handle_pushdown`'s single-table path and
every join leg call it. That construction site REPLACES the pushdown refusal in the recorded list
of production sites permitted to name a `CatalogKind` variant, leaving the permitted-site count
unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| One per-request `TableScanResolver`, matching `CatalogKind` at one site | ✓ Chosen — callers learn one format-neutral fact about a table instead of knowing about sessions, kinds, and formats |
| Add a `CatalogKind` branch at each of the two call sites | ✗ Rejected — exactly the per-operation fork the recorded one-construction-site rule exists to prevent |
| Build a session per resolved table | ✗ Rejected — would regress the recorded resolution economy, doubling a two-leg join's catalog authentication round-trips |
| Extend the existing `construct_catalog_client` to serve pushdown | ✗ Rejected — it returns a boxed `CatalogClient`, while `ScanSource::UnityDelta` needs the concrete `&UnityCatalogSession` |

### Consequences

Keeping the resolver out of the pushdown façade keeps the frozen surface shrinking by exactly one
item rather than churning.

## ADR: The Unity table identity is recovered by splitting the recorded dotted identifier

**ID:** unity-table-identity-recovered-by-splitting-dotted-identifier
**Plan:** `add-delta-scan-execution`
**Status:** Accepted

### Context

The per-request scan-source resolver needs to recover `CatalogTableIdent`'s namespace segments and
table name for the Unity Catalog kind, the same way it already does for the Iceberg kind, from the
identifier `TABLE_MAP` records at create time.

### Decision

Recover the namespace segments and table name by splitting the recorded dotted identifier, for the
Unity Catalog kind as well as the Iceberg one. Fail with an error naming the identifier when the
split yields no table name.

### Options Considered

| Option | Verdict |
|--------|---------|
| Split the recorded dotted identifier at pushdown time | ✓ Chosen — the Unity Catalog loader re-joins the segments into the catalog's own dotted full name before it issues the request, so the split round-trips losslessly |
| Re-encode `TABLE_MAP` to carry explicit namespace segments, with a backward-compatible read of the legacy dotted form | ✗ Rejected for this plan — changes a create-time wire format, forces a REFRESH story for existing virtual schemas, and touches the createVirtualSchema adapter-notes contract, none of which this plan's issue covers |

### Consequences

`CatalogTableIdent`'s doc rule against re-splitting a joined identifier still applies to any future
catalog kind that addresses a table by something other than that same joined string — the rejected
alternative becomes the right move only then.

## ADR: Two Delta gaps are named as scoped exceptions rather than closed here

**ID:** two-delta-gaps-named-as-scoped-exceptions
**Plan:** `add-delta-scan-execution`
**Status:** Accepted

### Context

Making the Delta path query-reachable changes the risk profile of every already-recorded Delta gap
from unreachable to reachable. This project's rule requires a known deviation to be either fixed in
the plan or recorded as an explicit, accurately-scoped tracked exception — never left as a silent
gap.

### Decision

Record in the spec deltas that (a) a Delta table declaring an unimplemented reader feature is now
query-reachable and ungated, bounded by issue #322 rather than by a refusal, and (b) filter-based
Delta file pruning remains issue #321, so a filter narrows rows without narrowing files.

### Options Considered

| Option | Verdict |
|--------|---------|
| Name both gaps as scoped exceptions in the spec deltas | ✓ Chosen — keeps the shift from unreachable to reachable visible rather than silent |
| Add reader-feature gating in this plan | ✗ Rejected — the recorded `delta-table-planning` contract states a gate added here would refuse the very deletion-vector and column-mapping fixtures this plan must read, and gating is #322's scope |
| Say nothing | ✗ Rejected — violates this project's rule that a known deviation must be fixed or explicitly tracked |

### Consequences

Issues #321 and #322 carry forward as the accountable trackers for filter-based file pruning and
reader-feature gating respectively, rather than this plan silently widening their blast radius.
