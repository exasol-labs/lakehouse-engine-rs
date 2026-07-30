# Plan: refactor-pushdown-node-count

## Summary

Read the cluster node count at pushdown from `UdfContext::node_count()` instead of from the `CLUSTER_NODES` entry the adapter currently persists into `schemaMetadata.adapterNotes` at `createVirtualSchema`. Shard count `G` stays identical for a given cluster size; the create-time write goes away and `build_adapter_notes` actively removes any inherited key ([#184](https://github.com/exasol-labs/lakehouse-engine-rs/issues/184)).

## Context

`adapterNotes` exists to carry values a pushdown cannot recompute. `TABLE_MAP` qualifies: it is built by enumerating the catalog namespace at create time, and re-enumerating it on every pushdown would cost a catalog round-trip. `CLUSTER_NODES` does not qualify. It is UDF handshake metadata that arrives free with every single-call invocation of the adapter script, `pushdown` included.

The persisted note therefore buys nothing and costs two things. It duplicates one decision (where the node count comes from) across two code sites that agree only through an untyped JSON string key, which is textbook back-door information leakage. And it freezes a live cluster property into schema metadata, so the shard fan-out keeps sizing itself against the cluster as it was at `CREATE VIRTUAL SCHEMA` time until an operator runs `REFRESH`.

The reason the pushdown path reads the note rather than the context is mechanical, not architectural. `handle_pushdown_request` is `async` and receives no `ctx`, because `node_count()` (like `script_schema()` and `connection()`) is a synchronous handshake read that must happen before the tokio runtime is entered. `dispatch` already establishes exactly that pattern for `script_schema` and the CONNECTION credentials. The note read was the path of least resistance around a boundary the file already knows how to cross.

- **Goals** — one owner for the node count; no persisted derived cluster state, including none inherited from a pre-refactor schema; `G` byte-identical for a fixed cluster size; the `CLUSTER_NODES` write path deleted and the key actively removed from any notes the adapter rewrites.
- **Non-Goals** — passing `ctx` into `handle_pushdown_request` (that would push an ambient read into async planning code); touching `NR_OF_CORES`, which is a per-node `available_parallelism()` value and not handshake metadata; touching `TABLE_MAP` or any other note; changing `shard_count`'s arithmetic, the 300 cap, or the file-count clamp.

## Design

### The premise holds by construction of the runtime

The refactor rests on one claim: `ctx.node_count()` returns the cluster size at pushdown, not only at `createVirtualSchema`. Reading the SLC runtime settles it at the code level.

Every VS request reaches the adapter through the same mechanism. `exa-udf-runtime` decodes one `UdfMeta` from the handshake, and for a single-call script (`SC_FN_*`, which includes the virtual-schema adapter call) `single_call.rs` builds `HandshakeMeta::from(meta)` and hands it to `SingleCallContext`, whose `node_count()` returns `self.handshake.node_count`. The request type (`createVirtualSchema`, `pushdown`, `refresh`) is a field inside the JSON payload, not part of the handshake. One script, one invocation kind, one metadata source; the request type cannot vary it.

That makes the status quo the load-bearing evidence. `CLUSTER_NODES` is already sourced from this exact call, and the E2E test `create_vs_records_cluster_nodes_property` asserts it comes back `≥ 1` against a live database. If `node_count()` were wrong at pushdown it would be equally wrong at create time, and today's sharding would already be wrong. This refactor changes *when* the value is read, not *what* is read.

What code inspection cannot settle is whether Exasol populates `numberOfNodes` as `4` rather than `1` on a real four-node cluster. That is a property of the database, not of this change, and it is unverified today for both the old and the new path. The `0 => 1` guard makes a wrong value degrade silently (`G` collapses to `parallelism_factor`) instead of failing, so the plan makes the four-node staging check a mandatory gate under Manual Testing rather than an assumption.

### Architecture

```
                 dispatch(ctx, request)                  [sync: handshake reads live here]
                          │
   createVirtualSchema ───┤─── pushdown
                          │      │
   resolve_nr_of_cores(props)    ├─ resolve_connection_config(ctx, props)   (existing)
   (no ctx, no node count)       ├─ ctx.script_schema()                     (existing)
            │                    └─ cluster_nodes_from_context(ctx)         (NEW)
   build_adapter_notes(...)             │
   NR_OF_CORES, PARALLELISM_FACTOR,     │  rt.block_on(...)
   TABLE_MAP, ... and                   │
   notes.remove(CLUSTER_NODES)  (NEW)   ▼
                              handle_pushdown_request(..., cluster_nodes: usize)
                                        │   (no adapter_note(NOTE_CLUSTER_NODES) read)
                                        ▼
                              handle_pushdown(..., cluster_nodes, parallelism_factor, ...)
                                        │
                              shard_count(node_count, parallelism_factor, file_count)
                                        │   unchanged: ×, cap 300, clamp [1, file_count]
```

The new `cluster_nodes_from_context(ctx) -> usize` owns the whole node-count decision: read the handshake, apply the `0 => 1` floor, widen to `usize`. It joins the two sibling captures already in `dispatch`'s pushdown arm.

### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Synchronous handshake capture before `rt.block_on` | `dispatch` pushdown arm | `node_count()` may block on the UDF host; the arm already does this for `script_schema` and the CONNECTION config |
| Dependency injection of ambient state | `handle_pushdown_request(..., cluster_nodes: usize)` | Async planning code keeps reading no ambient state; the entry point resolves it, matching the existing `script_schema: &str` parameter |
| Single owner per decision | `cluster_nodes_from_context` | Replaces a writer/reader pair coupled through the `"CLUSTER_NODES"` string with one function |
| Delete the parameter, not just the value | `resolve_cluster_nodes` → `resolve_nr_of_cores(props)` | Once the note is gone the create-time node count has no consumer; keeping it would leave dead code and a false `ctx` dependency |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Capture `node_count()` in `dispatch`, pass `usize` down | Pass `&mut dyn UdfContext` into `handle_pushdown_request` | A sync handshake read inside the tokio runtime may block the host, and it would make async planning code depend on the delivery mechanism |
| Reduce `resolve_cluster_nodes` to `resolve_nr_of_cores(props)` | Keep the `(u32, u32)` tuple and ignore the node count | Nothing at create time consumes the node count once the note is dropped; the function also loses a `ctx` parameter it never needed for cores |
| Delete the `CLUSTER_NODES` write and actively remove an inherited key | Keep the note as a diagnostic, written but unread; or drop only the write and let the key persist unread on pre-refactor schemas | A written-never-read note is state the mission forbids, and it would re-invite the same coupling. `build_adapter_notes` merges into the notes Exasol round-trips, so dropping only the write leaves the key on every pre-existing schema forever; the `NOTE_CLUSTER_NODES` constant therefore survives as the removal key. That survival is a tombstone, not a permanent fixture: [#287](https://github.com/exasol-labs/lakehouse-engine-rs/issues/287) tracks deleting the constant and the `remove` call once every deployed virtual schema has refreshed on this version or later |
| No adapterNotes fallback when `node_count()` is `0` | Fall back to a persisted `CLUSTER_NODES` when present | Restores the coupling the change removes, adds an untestable path, and the `0 => 1` floor already matches the pre-refactor absent-note behaviour exactly |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning | CHANGED | `vs-adapter/pushdown-planning/spec.md` |
| vs-adapter/create-virtual-schema-adapter-notes | CHANGED | `vs-adapter/create-virtual-schema-adapter-notes/spec.md` |
| vs-adapter/create-virtual-schema-adapter-notes-resources | CHANGED | `vs-adapter/create-virtual-schema-adapter-notes-resources/spec.md` |
| parallelism/work-unit-sharding | CHANGED | `parallelism/work-unit-sharding/spec.md` |
| vs-adapter/create-virtual-schema | CHANGED | `vs-adapter/create-virtual-schema/spec.md` |
| vs-adapter/refresh-and-set-properties | CHANGED | `vs-adapter/refresh-and-set-properties/spec.md` |

The last two features each carry one scenario edit, and `create-virtual-schema` also revises its Feature description (see § Record Notes). Both enumerated `CLUSTER_NODES` among the notes the adapter preserves. The adapter now removes it, so leaving the clauses would make the library both mandate and forbid the key on a `refresh` response. Each edit deletes the key from the enumeration and names it as the one exception to preservation.

Several delta files also revise their feature's `## Background` and, where the node count is named in the summary line, the Feature description. Those edits sit outside the `DELTA:*` scenario markers, so § Record Notes below lists each one by file and anchor for the recorder to apply from a checklist.

## Impact

No change to query results, generated SQL, or shard count for a cluster of fixed size. Three consequences an operator can observe:

1. **`SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES` no longer reports the node count.** `CLUSTER_NODES` disappears from the JSON. `SELECT NPROC()` reports the same number directly, so no diagnostic capability is lost.
2. **The shard fan-out now tracks cluster resizes without a `REFRESH`.** Previously `G` used the node count frozen at `CREATE VIRTUAL SCHEMA` time, so a cluster that grew from 1 to 4 nodes kept planning single-node fan-outs until an operator refreshed the schema. `G` is now computed from the live handshake on every pushdown. This is a behaviour change only on a resized cluster, and it moves toward the mission's stateless rule (resolve metadata per query, persist none).
3. **No migration.** Virtual schemas created before this change keep a stale `CLUSTER_NODES` key in their persisted notes until the schema is next rewritten. Nothing reads it, and because `build_adapter_notes` merges into the notes Exasol round-trips, the removal has to be active: `notes.remove(NOTE_CLUSTER_NODES)` is what makes the next `REFRESH` or `ALTER VIRTUAL SCHEMA SET` drop it. No re-create, no DDL, no version gate.

## Migration

| Current | New |
|---------|-----|
| `adapterNotes` of existing virtual schemas carry `CLUSTER_NODES` | Key is never read, and `build_adapter_notes` removes it from the notes it rewrites, so the next `REFRESH` / `setProperties` drops it; no operator action required |
| `handle_pushdown` doc comment: `cluster_nodes` read from the `CLUSTER_NODES` adapterNotes entry | Doc comment: captured from `ctx.node_count()` in `dispatch` |
| `build_adapter_notes` doc comment lists `CLUSTER_NODES` among the keys it carries | Doc comment: states the key is removed rather than written, and that the merge preserves every other pre-existing key |
| Channel-rationale comment block above `NOTE_CLUSTER_NODES` describes it as the write channel for the resolved node count | Rewritten in place: the constant survives only as the key removed from inherited notes |
| `specs/mission.md:75` (Domain Glossary, IPROC / NPROC row): node count "captured once at `createVirtualSchema`" | Reworded to say the shard-count node count is read from `UdfContext::node_count()` per pushdown request; the `NPROC()` / `IPROC()` clauses stay unchanged |
| Rollback to a pre-refactor `.so` after a `REFRESH` | The old adapter reads no `CLUSTER_NODES` and shards as `1 × PARALLELISM_FACTOR` until an `ALTER VIRTUAL SCHEMA ... REFRESH` re-writes the note; results are unaffected, fan-out shrinks |

## Implementation Tasks

- [ ] 1.1 Add `cluster_nodes_from_context(ctx: &dyn UdfContext) -> usize` in `crates/lakehouse-engine/src/adapter/mod.rs`, applying the `0 => 1` floor. Write its two unit tests first — `cluster_nodes_from_context_defaults_to_one_when_node_count_zero` (`0 => 1`) and `cluster_nodes_from_context_passes_through_reported_node_count` (`N => N` for `N = 4`) — reusing the existing `StubCtx` and `NoopCtx` test doubles.
- [ ] 1.2 Capture the node count in `dispatch`'s pushdown arm next to `ctx.script_schema()`, add a `cluster_nodes: usize` parameter to `handle_pushdown_request`, and delete its `adapter_note(request, NOTE_CLUSTER_NODES)` read. Update the arm's explanatory comment, which currently cites `resolve_cluster_nodes`, and rewrite the comment block at `adapter/mod.rs:370-372` so it names only `PARALLELISM_FACTOR` and the other note-carried values, not `CLUSTER_NODES`.
- [ ] 1.3 Stop writing the note, start removing it, and reconcile the tests in the same step — the arity change breaks the test target, so the production edit and the test repair are one unit of work. Production side, in `crates/lakehouse-engine/src/adapter/mod.rs`: drop the `cluster_nodes` parameter from `build_adapter_notes`, replace its `NOTE_CLUSTER_NODES` insert with `notes.remove(NOTE_CLUSTER_NODES);` so a key inherited from a pre-refactor schema is dropped rather than preserved by the merge, keep the `NOTE_CLUSTER_NODES` constant as that removal key, reduce `resolve_cluster_nodes(ctx, props) -> (u32, u32)` to `resolve_nr_of_cores(props) -> u32` (dropping the now-unused `ctx` parameter), update `build_adapter_notes`'s doc comment (it currently lists `CLUSTER_NODES` among the keys it carries), and rewrite the channel-rationale comment above the constant so it describes a removal key rather than a write channel — stating that the constant and the `remove` call exist solely to evict a key persisted by adapter versions before this change, and that both can be deleted once every deployed virtual schema has been refreshed at least once on this version or later. The arity change makes the compiler enumerate all 19 `build_adapter_notes` call sites (1 production, 18 test). Test side, same file: delete `create_response_carries_cluster_nodes_property` and `adapter_notes_cluster_nodes_round_trips`; retarget the `adapter_note` probe key in `adapter_note_absent_or_unparseable_yields_none` to `NOTE_PARALLELISM_FACTOR`; in `build_adapter_notes_merges_existing` (`adapter/mod.rs:1473`), keep the fixture's inherited `CLUSTER_NODES` entry, replace the `parsed[NOTE_CLUSTER_NODES] == Some("3")` assertion (lines 1502-1506) with an assertion that the key is absent, and update the doc comment to state merge-not-clobber for every key except `CLUSTER_NODES`, which is removed; rename `adapter_notes_carry_cluster_nodes_and_parallelism_factor` to `adapter_notes_carry_parallelism_factor` and drop its `CLUSTER_NODES` assertion; delete `cluster_nodes_defaults_to_one_when_node_count_zero` and `cluster_nodes_passes_through_reported_node_count`, superseded by task 1.1's two tests; update all three `resolve_cluster_nodes` test call sites to the new `resolve_nr_of_cores` signature: `nr_of_cores_from_available_parallelism_when_unavailable` (`adapter/mod.rs:1702`), `nr_of_cores_property_overrides_auto_detect` (`:2194`, also deleting its `assert_eq!(nodes, 3u32, ...)`), and `nr_of_cores_property_falls_back_to_auto_detect` (`:2245`, also deleting its `assert_eq!(nodes, 1u32)`); invert the surviving `CLUSTER_NODES` assertions in `table_map_merges_with_existing_notes` (asserts `"5"` today) and `create_vs_records_table_map_in_adapter_notes` (asserts an inherited `"3"` survives today) to assert the key is absent; add `adapter_notes_omit_cluster_nodes`, `refresh_notes_drop_inherited_cluster_nodes` (a `refresh` request whose incoming `adapterNotes` carry `CLUSTER_NODES` yields notes without it, with every other inherited key preserved), and `pushdown_ignores_persisted_cluster_nodes_note` (a `pushdown` request whose `adapterNotes` carry `CLUSTER_NODES` = "9", called with `cluster_nodes = 1`, asserting the shard count derived is `1 × PARALLELISM_FACTOR`).
- [ ] 1.4 Update the IPROC / NPROC row of the Domain Glossary at `specs/mission.md:75`: replace "captured once at `createVirtualSchema` from `UdfContext::node_count()` (the UDF handshake)" with a statement that the shard-count node count is read from `UdfContext::node_count()` per pushdown request. Leave that row's `NPROC()` and `IPROC()` clauses, and every other mission section, unchanged.
- [ ] 1.5 Replace the E2E test `create_vs_records_cluster_nodes_property` in `crates/lakehouse-engine/tests/e2e_scan_test.rs` with `create_vs_omits_cluster_nodes_from_adapter_notes` (asserts `ADAPTER_NOTES` parses, carries `PARALLELISM_FACTOR` and `NR_OF_CORES`, and carries no `CLUSTER_NODES` key).
- [ ] 1.6 Add the E2E test `pushdown_shards_from_handshake_node_count_without_note` in `crates/lakehouse-engine/tests/e2e_scan_test.rs`: run `EXPLAIN VIRTUAL` over a multi-file scan against the note-free virtual schema and assert the generated SQL still carries the `LAKEHOUSE_DISTRIBUTE_FILES` fan-out with `AS shards(shard_key, files) GROUP BY shard_key)`, following the assertion style already used at `e2e_scan_test.rs:1088`.
- [ ] 1.7 Update the `cluster_nodes` doc comment on `handle_pushdown` in `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` to name `ctx.node_count()` captured in `dispatch` as the source.
- [ ] 1.8 Run the verification checklist below: `cargo test`, `cargo clippy --all-targets`, `cargo fmt`, and `make test-e2e`.

No task is tagged `[expert]`. The sync-to-async threading crosses a boundary the same function already crosses twice for `script_schema` and the CONNECTION config, moves a copied `u32`, and introduces no concurrency, ordering, or shared state. Task 1.3 is the largest, but its size is compiler-enumerated breadth over one file rather than reasoning depth.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1 |
| Group B | 1.2 |
| Group C | 1.3 |
| Group D | 1.4, 1.5, 1.6, 1.7 |
| Group E | 1.8 |

Sequential dependencies:

- Group A → Group B (1.2 calls the helper 1.1 adds)
- Group B → Group C (1.3 may only stop writing the note after 1.2 has removed its last reader)
- Group C → Group D (the E2E tests and the doc-comment fix follow the signature changes)
- Group D → Group E (verification runs last)

Groups A through C are single tasks by necessity, not by choice: each is the smallest step that leaves `cargo test` compiling, so a failure localizes to one step. That is why 1.3 carries both the `build_adapter_notes` arity change and the test reconciliation — the 18 test call sites and the two `CLUSTER_NODES` assertions in the table-map tests break the test target the moment the parameter goes, so splitting them would leave an intermediate state where the per-task gate cannot run at all. After 1.2 the note is written but unread, which is a valid intermediate state.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Statement | `NOTE_CLUSTER_NODES` insert in `build_adapter_notes`, `adapter/mod.rs:678-681` | Replaced by `notes.remove(NOTE_CLUSTER_NODES)`; the constant itself survives as that removal key |
| Function parameter | `cluster_nodes` on `build_adapter_notes`, `adapter/mod.rs:664` | The note it fed is gone |
| Function parameter | `ctx` on `resolve_cluster_nodes`, `adapter/mod.rs:961` | Only the node count used it; the core count is props-and-host derived |
| Return value | node-count half of `resolve_cluster_nodes`'s `(u32, u32)` | No create-time consumer; `resolve_parallelism_factor`, `resolve_df_threading`, and `resolve_s3_max_connections` all derive from `nr_of_cores` |
| Statement | `adapter_note(request, NOTE_CLUSTER_NODES)` read, `adapter/mod.rs:373-376` | Replaced by the `dispatch` capture |
| Test | `create_response_carries_cluster_nodes_property`, `adapter/mod.rs:1359` | Asserts a note the response no longer carries |
| Test | `adapter_notes_cluster_nodes_round_trips`, `adapter/mod.rs:1414` | The round-trip it asserts no longer exists |
| Test | `create_vs_records_cluster_nodes_property`, `tests/e2e_scan_test.rs:1211` | Asserts `ADAPTER_NOTES` carries `CLUSTER_NODES`; replaced by `create_vs_omits_cluster_nodes_from_adapter_notes` |
| Test | `cluster_nodes_defaults_to_one_when_node_count_zero`, `adapter/mod.rs:1332` | Superseded by task 1.1's `cluster_nodes_from_context_defaults_to_one_when_node_count_zero` |
| Test | `cluster_nodes_passes_through_reported_node_count`, `adapter/mod.rs:1345` | Superseded by task 1.1's `cluster_nodes_from_context_passes_through_reported_node_count` |

## Record Notes

`/speq:spec-merge`'s marker table defines actions for scenarios only, so every edit a delta makes outside a `### Scenario:` block is listed here by file and anchor. `recorder-agent` applies this checklist; it MUST NOT infer these edits from the `DELTA:*` markers that wrap them.

| Delta file | Anchor | Edit |
|------------|--------|------|
| `vs-adapter/create-virtual-schema-adapter-notes/spec.md` | Feature description (paragraph under the `# Feature:` heading) | Replace the recorded description; the node count is named as excluded, not recorded |
| `vs-adapter/create-virtual-schema-adapter-notes/spec.md` | `## Background` bullets 1-5 (recorded numbering) | Replace all five with the marker's six bullets verbatim; the node-count bullet becomes two, the connect-back bullet drops the node count from its subject, and the parallelism-factor bullet drops `CLUSTER_NODES` from its sibling list |
| `vs-adapter/create-virtual-schema-adapter-notes-resources/spec.md` | `## Background` bullet 4 ("The parallelism factor is supplied...") | Drop `CLUSTER_NODES` from the sibling-key phrasing |
| `vs-adapter/create-virtual-schema-adapter-notes-resources/spec.md` | `## Background` bullet 7 (the closing `See ...` cross-reference) | Add why the node count is deliberately not recorded here |
| `vs-adapter/pushdown-planning/spec.md` | `## Background`, insert after the `ctx.script_schema()` bullet | Append three new bullets (handshake source, `dispatch`-before-runtime capture, `0` only without live handshake) |
| `parallelism/work-unit-sharding/spec.md` | `## Background` bullet 1 | Replace the `adapterNotes`-source bullet with the per-pushdown handshake bullet |
| `vs-adapter/create-virtual-schema/spec.md` | Feature description (paragraph under the `# Feature:` heading) | Drop "the cluster's active node count" from the recorded-values list |

No other file's Background, Feature description, or prose changes. The `vs-adapter/create-virtual-schema` and `vs-adapter/refresh-and-set-properties` deltas quote a two-bullet `## Background` excerpt verbatim from their recorded specs to satisfy the spec-structure validator; neither excerpt is an edit, and `refresh-and-set-properties` changes nothing outside its one scenario.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Pushdown reads the cluster node count from the UDF handshake | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `cluster_nodes_from_context_passes_through_reported_node_count`, `pushdown_ignores_persisted_cluster_nodes_note` |
| Pushdown reads the cluster node count from the UDF handshake | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `pushdown_shards_from_handshake_node_count_without_note` |
| Pushdown node count falls back to one when the handshake reports none | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `cluster_nodes_from_context_defaults_to_one_when_node_count_zero` |
| createVirtualSchema adapterNotes omit the cluster node count | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `adapter_notes_omit_cluster_nodes`, `refresh_notes_drop_inherited_cluster_nodes`, `build_adapter_notes_merges_existing` |
| createVirtualSchema adapterNotes omit the cluster node count | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `create_vs_omits_cluster_nodes_from_adapter_notes` |
| Adapter records the per-node core count in the virtual-schema adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `adapter_notes_records_nr_of_cores` |
| Recorded parallelism factor drives later work-unit sharding | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `adapter_notes_carry_parallelism_factor`, `pushdown_ignores_persisted_cluster_nodes_note` |
| Adapter records the parallelism factor in the virtual-schema adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `create_vs_records_parallelism_factor` |
| Adapter records the DataFusion target partition count in the virtual-schema adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `df_target_partitions_uses_supplied_value` |
| Adapter records the DataFusion threads-per-UDF count in the virtual-schema adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `df_threads_per_udf_uses_supplied_value` |
| Adapter records the memory-pool fraction in the virtual-schema adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `memory_budget_params_round_trip_through_adapter_notes` |
| Adapter records the instance-overhead megabytes in the virtual-schema adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `memory_budget_params_round_trip_through_adapter_notes` |
| Shard count oversubscribes the cluster and is capped at the round-robin threshold | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `shard_count_oversubscribes_and_caps_at_300`, `shard_count_clamped_to_file_count_no_empty_shards` |
| Shard count oversubscribes the cluster and is capped at the round-robin threshold | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `multi_shard_row_query_matches_single_shard` |
| Create virtual schema records the Exasol-name to Iceberg-identifier map in adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `create_vs_records_table_map_in_adapter_notes`, `table_map_merges_with_existing_notes` |
| Refresh rebuilds the table map and preserves other adapter notes | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `refresh_rebuilds_table_map_preserves_notes`, `refresh_notes_drop_inherited_cluster_nodes` |

The `0 => 1` fallback and `shard_count`'s arithmetic are pure computation over integers with no I/O, so unit tests are the correct instrument for them. Every scenario whose behaviour reaches the database also carries an integration test. The six scenarios whose deltas edit only a sibling-key enumeration appear here because that enumeration sits inside a `SHALL` clause; their behaviour and their existing tests are untouched. The last two rows are the exception: the preserve-other-notes clause changes meaning under this refactor, so `create_vs_records_table_map_in_adapter_notes`, `table_map_merges_with_existing_notes`, and the new `refresh_notes_drop_inherited_cluster_nodes` are what prove the inherited key is dropped and every other key survives.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/create-virtual-schema-adapter-notes | `SELECT ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = 'LAKEHOUSE_VS';` | Valid JSON with no `CLUSTER_NODES` key; `NR_OF_CORES`, `PARALLELISM_FACTOR`, and `TABLE_MAP` all present |
| vs-adapter/refresh-and-set-properties (legacy-key removal) | Against a virtual schema created with the pre-refactor adapter (its `ADAPTER_NOTES` still showing `CLUSTER_NODES`), deploy the new `.so`, run `ALTER VIRTUAL SCHEMA LAKEHOUSE_VS REFRESH;`, then re-run the `ADAPTER_NOTES` query above | `CLUSTER_NODES` is gone and `TABLE_MAP`, `NR_OF_CORES`, and `PARALLELISM_FACTOR` are still present. A surviving `CLUSTER_NODES` means the merge preserved the inherited key and § Migration row 1 is wrong |
| vs-adapter/pushdown-planning | `EXPLAIN VIRTUAL SELECT * FROM LAKEHOUSE_VS.LINEITEM;` | Generated SQL contains `LAKEHOUSE_DISTRIBUTE_FILES(files) FROM (VALUES` and `AS shards(shard_key, files) GROUP BY shard_key)` |
| parallelism/work-unit-sharding (GATE: four-node staging) | On the four-node staging cluster: `SELECT NPROC();` then `EXPLAIN VIRTUAL SELECT * FROM LAKEHOUSE_VS.LINEITEM;` and count the `(shard_key, files)` rows in the distributor's `VALUES` list | `NPROC()` reports 4, and the row count equals `min(4 × PARALLELISM_FACTOR, 300, file_count)`. A count matching `min(1 × PARALLELISM_FACTOR, 300, file_count)` means `node_count()` did not report the cluster size at pushdown and the refactor MUST NOT ship |
| parallelism/work-unit-sharding | `SELECT COUNT(*) FROM LAKEHOUSE_VS.LINEITEM;` on the same staging cluster | Row count matches the pre-change value, confirming the fan-out change altered no result |

The four-node staging gate closes the open item the issue author flagged. It is the only check that distinguishes a correctly read four-node count from the `0 => 1` floor, because a single-node Docker container reports `1` under either path. The legacy-key row is the only check that a pre-refactor persisted note actually disappears, because no automated test in this repository can create a schema with the old adapter. Run both before the PR leaves draft.

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Test (unit) | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
| Build (UDF `.so`) | `make cross-musl-udf-build` | Exit 0 |
| Test (E2E, Docker Exasol) | `make test-e2e` | 0 failures |
