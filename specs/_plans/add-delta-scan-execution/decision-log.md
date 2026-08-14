# Decision Log: add-delta-scan-execution

## Interview

**Q:** Join/aggregate scope under Delta. `handle_pushdown` currently calls the Iceberg-only
`resolve_file_list` for every request shape (scan, join, aggregate). Issue #320's title only covers
single-table scan. How should the plan handle join and aggregate pushdown against a Delta-backed
(Unity Catalog) table?

**A:** The format-specific part is only the file resolution (Delta log replay → `ScanSpec`). That is
what `FormatReader::resolve_scan` does. Once it returns a format-neutral `ResolvedScan`, the
downstream pipeline — plain scan, join, or aggregate — operates on format-neutral types. That is the
whole point of #342's neutralization. So concretely: #320 removes the Unity Catalog refusal from
`handle_pushdown` and routes through the `FormatReader` / `ScanSource` dispatch for every request
shape, not just scan. The `FormatReader` does not know or care whether a join or aggregate sits
above — it produces the file list and schema. Join pushdown (broadcast equi-join) works on `ScanSpec`
+ `JoinSpec` — format-neutral. It should just work once the Delta path returns a valid
`ResolvedScan`. Aggregate pushdown (partial/merge decomposition) likewise operates on format-neutral
types. #321 (pushdown parity) is about plan-time optimizations specific to the Delta log: partition
pruning, stats-based file pruning. That is a different concern — it makes Delta pushdown efficient,
not reachable. The plan should treat the refusal removal as a single dispatch change that opens all
request shapes to the Delta path at once, not a per-shape gate. If a specific shape breaks in E2E
(for example a join against a Delta table hits something unexpected), that is a bug to fix in #320,
not a reason to scope it down to scan-only.

**Q:** Deletion-vector decode approach. Delta deletion vectors need decoding a roaring-bitmap binary.
The workspace already depends on `delta_kernel` 0.26 (used only for log-replay metadata) and
`roaring` 0.11 (used by the Iceberg positional-delete path). Should the plan prefer reusing
`delta_kernel`'s own DV-reading utilities if it exposes them, or go straight to hand-decoding via the
`roaring` crate?

**A:** Prefer `delta_kernel`'s DV utilities if they are exposed — but verify before planning on it.
The kernel already reads and applies DVs correctly (proven in spike #317: 10 raw rows → 8), so the
decoding logic exists. The question is whether it is accessible as a standalone utility or only as
part of the kernel's internal scan pipeline.

**Q:** Test call-site migration depth for the `IcebergFormatReader` collapse. ADR 4 of the recorded
`018-add-delta-table-planning` decision log lists `resolve_file_list`'s direct callers as two
production sites plus three test files. Should collapsing the delegator also migrate those test call
sites?

**A:** Full collapse. Migrate the test call sites too, and delete `resolve_file_list` as a distinct
public function. Fully close the ADR-4 debt: one code path for production and tests alike.

## Design Decisions

### [1] Delta deletion vectors are decoded by `delta_kernel`, not hand-decoded

- **Decision:** Decode through `delta_kernel::actions::deletion_vector::DeletionVectorDescriptor::read`, which returns `roaring::RoaringTreemap`. The investigation Q2 asked for was carried out against the vendored `delta_kernel` 0.26.0 source: the item is plain `pub`, gated behind no feature flag (`internal-api` is needed only for the action structs, which this path never touches), and handles all three storage types including the Z85 inline form. `Cargo.lock` resolves exactly one `roaring` entry (0.11.4) shared by `delta_kernel`, `iceberg`, and `lakehouse-engine`, so the returned bitmap is the same type the Iceberg positional-delete path already feeds to `build_deletes_row_selection`.
- **Alternatives:** Hand-decode against the protocol with `roaring` — rejected: it re-derives the container framing (big-endian version byte, `dataSize`, CRC-32), the little-endian portable magic `1681511377`, and the Z85 inline variant that the workspace already carries a validated implementation of. Use the kernel's full scan pipeline — rejected outright, see decision [2].
- **Rationale:** The kernel's decoder validates the version byte, the declared size, the magic, and the CRC-32 and refuses a native-serialized inline bitmap with a clear error. Re-implementing that is new surface for zero gain, and a divergence between our decoder and the kernel's would be a silent wrong-rows bug.
- **Promotes to ADR:** yes

### [2] The deletion-vector decoder is fed pre-fetched bytes, never a live storage client

- **Decision:** The scan fetches each deletion-vector sidecar on its OWN asynchronous, limiter-bounded path — the same path that already reads Iceberg delete files — and satisfies the kernel decoder's `StorageHandler` dependency with a read-only in-memory adapter that serves those bytes. Every other operation on the adapter (list, put, head, copy, delete) returns a clean error and never panics.
- **Alternatives:** Build a `delta_kernel` `DefaultEngine` inside the scan and use its object-store-backed storage handler — rejected: the kernel's decoder is synchronous and its default handler drives its own Tokio background executor, so this would start a second runtime inside a memory-bounded UDF and move object-store I/O off the shared connection-budget limiter. Construct the kernel's `ObjectStoreStorageHandler` directly — impossible: its constructor is `pub(crate)`.
- **Rationale:** DataFusion is this engine's only execution engine; a UDF that hosts a second one competes for the bounded memory pool the mission's self-throttling model depends on. Pre-fetching also inverts the kernel's per-descriptor whole-file fetch into one fetch per distinct sidecar, which is what preserves the recorded read-once-per-shard property for a sidecar shared across data files. `unimplemented!()` on the unused methods is forbidden: a panic inside a UDF is an abnormal VM exit that makes the engine SIGKILL every sibling VM of the statement part.
- **Promotes to ADR:** yes

### [3] A deletion-vector sidecar is fetched whole, once per distinct resolved path

- **Decision:** Fetch the entire `.bin` object rather than the byte range the descriptor's `offset` and `sizeInBytes` describe, and memoize the fetched bytes per shard keyed on the resolved absolute path. Issue no object-store `HEAD`.
- **Alternatives:** Range-GET `[offset, offset + 4 + sizeInBytes + 4)` — rejected: the decoder reads the container's format-version byte at file position 0 and then seeks to `offset`, so a range starting at `offset` fails validation. Key the memo on the descriptor's derived unique id instead of the path — rejected: that dedups identical descriptors but still refetches the same sidecar once per offset.
- **Rationale:** Delta sidecars are small — the seeded `table-with-dv-small` fixture's file is 45 bytes for a 36-byte deletion vector — and the descriptor carries no sidecar size, so nothing is gained by ranging and a `HEAD` would be a round-trip for a value the scan never needs.
- **Promotes to ADR:** no

### [4] Partition columns are materialized through DataFusion's native partition-column mechanism

- **Decision:** Split the logical schema into file fields and partition fields at registration, pass the partition fields as `table_partition_cols` on the `FileScanConfig`, and populate each `PartitionedFile.partition_values` from that file's `FileEntry`. Keep `TableProvider::schema()` in declared order and remap the incoming projection indices into the `file ++ partition` order the config uses.
- **Alternatives:** Extend `FieldIdExprAdapterFactory`'s per-file absent-column default map with partition literals — rejected: the factory is built once per scan and receives schemas, not file identity, so it cannot carry a per-file constant. Rewrite the emitted `RecordBatch` after the scan — rejected: a filter or a GROUP BY over a partition column would then run against NULLs, and Exasol re-applies nothing it delegated, so that returns wrong rows rather than a slow plan. Append partition columns to the output and restore declared order with a `ProjectionExec` — rejected: `FileScanConfig` applies projection indices in the order given, so the remap alone suffices with no extra plan node.
- **Rationale:** The native mechanism is per-file by construction, composes with the `ParquetAccessPlan` the delete pipeline attaches to the same `PartitionedFile`, keeps the expr adapter looking only at real file fields, and lets DataFusion prune a file on its partition values for free. Neither of the two sites it touches is currently used by this repo, so nothing regresses.
- **Promotes to ADR:** yes

### [5] The partition-materialization feature is named format-neutrally

- **Decision:** The new spec is `datafusion-scan/scan-execution-partition-values`, not a Delta-named feature, and its scenarios dispatch on whether `partition_columns` and `partition_values` are populated — never on the table format.
- **Alternatives:** Name it `scan-execution-delta-partition-values`, mirroring the stale scaffold directory left by an earlier attempt — rejected.
- **Rationale:** The recorded `delta-table-planning` contract already states that the per-file partition-value map is the SAME field an Iceberg identity-transform partition value (issue #99) and a future Hive-style partition value would populate. A Delta-named scan feature would invite a second, format-named home for one decision. The deletion-vector feature keeps its Delta name for the opposite reason: the container framing and the Z85 payload are the Delta protocol's, and an Iceberg Puffin deletion vector is a genuinely different mechanism that stays refused.
- **Promotes to ADR:** yes

### [6] One per-request scan-source resolver, matching the catalog kind at a single site

- **Decision:** Introduce a per-request `TableScanResolver` that holds the request's ONE catalog session (Iceberg or Unity), matches `CatalogKind` exhaustively at exactly ONE site, and answers `resolve(table_identifier, filter_json) -> ResolvedScan`. `handle_pushdown`'s single-table path and every join leg call it. That construction site REPLACES the pushdown refusal in the recorded list of production sites permitted to name a `CatalogKind` variant, leaving the permitted-site count unchanged.
- **Alternatives:** Add a `CatalogKind` branch at each of the two call sites — rejected: that is exactly the per-operation fork the recorded one-construction-site rule exists to prevent. Build a session per resolved table — rejected: it would regress the recorded resolution economy, making a two-leg join perform twice the catalog authentication round-trips of a single-table scan. Extend the existing `construct_catalog_client` to serve pushdown — rejected: it returns a boxed `CatalogClient`, while `ScanSource::UnityDelta` needs the concrete `&UnityCatalogSession`.
- **Rationale:** The callers stop knowing about sessions, kinds, and formats entirely; they learn one thing about a table and it is format-neutral. Keeping the resolver out of the pushdown façade keeps the frozen surface shrinking by exactly one item rather than churning.
- **Promotes to ADR:** yes

### [7] The Unity table identity is recovered by splitting the recorded dotted identifier

- **Decision:** Recover `CatalogTableIdent`'s namespace segments and table name by splitting the identifier `TABLE_MAP` recorded at create time, for the Unity Catalog kind as well as the Iceberg one. Fail with an error naming the identifier when the split yields no table name.
- **Alternatives:** Re-encode `TABLE_MAP` to carry explicit namespace segments, with a backward-compatible read of the legacy dotted form — rejected for this plan: it changes a create-time wire format, forces a REFRESH story for existing virtual schemas, and touches the createVirtualSchema adapter-notes contract, none of which issue #320 covers.
- **Rationale:** `CatalogTableIdent`'s doc rule against re-splitting a joined identifier guards against ambiguity, and no ambiguity arises here: the Unity Catalog loader re-joins the segments into the catalog's own dotted full name before it issues the request, so the split round-trips losslessly and cannot resolve a different table. The rejected alternative is the right move if a catalog kind ever addresses a table by anything other than that same joined string.
- **Promotes to ADR:** yes

### [8] `JoinSpec` gains its own `partition_columns`

- **Decision:** Add `partition_columns: Vec<String>` to `JoinSpec`, serde-defaulted and omitted from the wire when empty, so the broadcast side carries its own partition columns alongside the file list, logical schema, name mapping, table root, and storage it already carries.
- **Alternatives:** Reuse `CommonScanSpec.partition_columns` for both sides — rejected: the two sides are different tables and may partition differently, so one field would materialize the fact side's columns on the broadcast side. Refuse a broadcast join whose broadcast side is partitioned — rejected: the interview ruled that a shape which breaks is a bug to fix here, not a reason to narrow scope.
- **Rationale:** It widens an existing format-neutral concept per this project's `ScanSpec` neutrality rule rather than adding a format-specific block, and it is symmetric with every other per-side field `JoinSpec` already carries. Omitting it when empty keeps every Iceberg join spec byte-identical.
- **Promotes to ADR:** no

### [9] `resolve_file_list` is deleted, and the façade shrinks by exactly one item

- **Decision:** Move `resolve_file_list`'s body into `IcebergFormatReader::resolve_scan`, delete the free function, and migrate all five test call sites onto the format-reader seam. The in-crate pushdown surface probe drops from 26 items to 25 and the external probe from 16 to 15; the new resolver is NOT added to the façade. Supersedes the recorded decision "`IcebergFormatReader` is a deliberately thin delegator, with the collapse scheduled for #320".
- **Alternatives:** Keep `resolve_file_list` `pub` and deprecated — rejected: two resolution paths for one format is the information leak the seam exists to prevent, and the superseded decision accepted the delegator only until this issue removed its direct callers. Migrate only the production call sites — rejected by the interview's full-collapse answer.
- **Rationale:** The superseded decision named this collapse as a scheduled follow-up rather than an open-ended one, and this is the scheduled point. Routing the test callers through `format_reader` costs nothing: it and `ScanSource` are already on both probes, so the collapse adds no façade item.
- **Promotes to ADR:** no

### [10] Column mapping needs verification, not implementation

- **Decision:** Plan no new column-binding work. The generalized binding adapter already dispatches with documented precedence — embedded Parquet field id, then declared physical name, then the table-level name mapping, then identity — which covers Delta's `id`, `name`, and `none` modes. This plan's column-mapping scope is E2E coverage over the `cm_id_mode` and `cm_name_mode` fixtures alone.
- **Alternatives:** Rename the adapter's types to drop their field-id-specific names — rejected as churn outside this issue's scope; the types are already generalized in behavior and are covered by recorded scenarios under their current names.
- **Rationale:** Issue #320's column-mapping scope item says the generalized adapter "handles Id-mode and Name-mode binding" and that this issue "verifies the Delta paths work end-to-end". Planning implementation work for behavior that already exists would produce a task with no failing test to drive it.
- **Promotes to ADR:** no

### [11] The delete-application module keeps its Iceberg-flavored name

- **Decision:** Delta deletion-vector decoding lands in a new `scan/deletion_vectors.rs` sibling; the shared `PositionalDeleteScanTable` and the two-phase pipeline stay in `scan/positional_deletes.rs` under its current name.
- **Alternatives:** Rename the module to a format-neutral name — rejected for this plan.
- **Rationale:** The module's name is now narrower than its contents, but the provider it owns is named in several recorded scenarios and re-exported through the scan façade, so a rename spends real churn on a naming improvement this issue did not ask for. The behavior stays neutral — the pipeline dispatches on the delete mechanism's variant, never on the table format — which is what the recorded backstop scenario actually requires.
- **Promotes to ADR:** no

### [12] Two Delta gaps are named as scoped exceptions rather than closed here

- **Decision:** Record in the spec deltas that (a) a Delta table declaring an unimplemented reader feature is now query-reachable and ungated, bounded by issue #322 rather than by a refusal, and (b) filter-based Delta file pruning remains issue #321, so a filter narrows rows without narrowing files. Additionally, task 5.1 verifies whether a percent-encoded Delta `add.path` survives log replay and file registration, and opens a tracked issue if it does not.
- **Alternatives:** Add reader-feature gating in this plan — rejected: the recorded `delta-table-planning` contract states a gate added there would refuse the very deletion-vector and column-mapping fixtures this plan must read, and gating is #322's scope. Say nothing — rejected under this project's rule that a known deviation is either fixed in the plan or recorded as an explicit, accurately-scoped tracked exception.
- **Rationale:** Making the Delta path query-reachable changes the risk profile of every already-recorded Delta gap from unreachable to reachable. Naming that shift in the spec is what keeps it from becoming a silent gap.
- **Promotes to ADR:** yes

### [13] No spec delta for the column-binding or scan-module-structure features

- **Decision:** Author no delta against `datafusion-scan/scan-execution-field-id-projection` or `datafusion-scan/scan-module-structure`.
- **Alternatives:** Add a "verified end to end for Delta" scenario to the binding feature — rejected as spec noise: the E2E assertion belongs to the E2E harness feature that runs it, and the binding behavior is unchanged. Add a scenario admitting the new `scan/deletion_vectors.rs` submodule — rejected: that feature's recorded rule already requires every functional scan submodule to carry its own sibling `_tests.rs`, and its façade scenarios pin a refactor-time baseline rather than freezing the module set.
- **Rationale:** A delta that restates unchanged behavior costs review attention and adds a merge target for no requirement. `vs-adapter/pushdown-module-structure` does get a delta, because its façade IS explicitly frozen and this plan removes an item from it.
- **Promotes to ADR:** no

## Review Findings
