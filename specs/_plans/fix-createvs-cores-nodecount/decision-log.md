# Decision Log: fix-createvs-cores-nodecount

Date: 2026-07-01

Tracking: GitHub issue https://github.com/exasol-labs/lakehouse-engine-rs/issues/32

## Interview

**Q:** Is `exasol-udf-sdk`/`exasol-udf-macros` 0.20.0 (which adds `UdfContext::node_count() -> u32`, from `language-container-rs` commit `7d90182`) available to depend on?
**A:** Yes — verified published on crates.io (`cargo search exasol-udf-sdk` → `0.20.0`; `exasol-udf-macros = "0.20.0"`). Treat as a normal version bump in `crates/lakehouse-engine/Cargo.toml` and root `Cargo.toml` (currently pinned to `0.19.1`); no publish-blocking precondition.

**Q:** Should the fix remove the connect-back session and `CONNECTION_NAME` property entirely (node count via `ctx.node_count()`, cores via `std::thread::available_parallelism()`), or keep connect-back as a defensive fallback?
**A:** Remove entirely. Drop `resolve_cluster_nodes`'s connect-back path, the `CONNECTION_NAME` VS property, and the `PARAM_VALUE` query. Grep-verified that `CONNECTION_NAME` / `ctx.connect_back()` in this crate (`adapter/mod.rs`) is used ONLY for this node/core resolution — nowhere else — so removal is clean with no other blast radius. `CATALOG_CONNECTION` (credentials, `adapter/connection.rs`) is a fully separate mechanism and must NOT be touched.

**Q:** Should `available_parallelism()`-in-adapter-UDF returning the true per-node core count (not sandbox/cgroup-limited) be verified on a live cluster as part of implementation?
**A:** No — trust the existing ADR-023 precedent (the scan UDF's DataFusion `target_partitions` already defaults to the full host core count via `available_parallelism()` inside a UDF on this codebase's target clusters). No live-cluster verification task.

## Design Decisions

### [1] Source cluster node count from the UDF handshake, not a connect-back query

- **Decision:** Read the active node count from `UdfContext::node_count()` (SDK 0.20.0) in-process, mapping the neutral `0` (no live handshake) to a `CLUSTER_NODES` default of `1`.
- **Alternatives:** Keep `SELECT NPROC()` over a connect-back session (the current approach for the node count); keep connect-back purely as a defensive fallback behind `node_count()`.
- **Rationale:** The handshake already carries the node count, so no SQL session, auth, or transaction is needed and there is no query that can fail-and-discard. It was the shared-closure `?` on the sibling `PARAM_VALUE` query that discarded the valid node count in the first place; removing the session removes the failure class entirely. A defensive fallback would re-introduce the exact fragile SQL path being deleted.
- **Promotes to ADR:** yes

### [2] Source per-node core count from `available_parallelism()`, no live-cluster verification

- **Decision:** Read the per-node core count from `std::thread::available_parallelism()` on the executing node when the `NR_OF_CORES` override is absent/invalid; treat an unavailable reading as `0` ("unknown"). Do not add a live-cluster verification task.
- **Alternatives:** The bogus `SELECT PARAM_VALUE('NR_OF_CORES')` connect-back query (never worked — not a real Exasol function); add a task to verify on a live cluster that `available_parallelism()` reports the true host core count rather than a cgroup/sandbox limit.
- **Rationale:** `PARAM_VALUE` was the root cause of issue #32. `available_parallelism()` is already trusted for the scan UDF's DataFusion `target_partitions` under ADR-023 on the same target clusters, so the source is proven; a separate verification task is redundant. The `0`-means-unknown sentinel preserves the downstream parallelism-factor floor-of-8 contract unchanged.
- **Promotes to ADR:** yes

### [3] Remove `CONNECTION_NAME` as a supported VS property (breaking, but zero blast radius)

- **Decision:** Delete the `PROP_CONNECTION_NAME` constant and stop reading the property; document its removal in the adapter-notes spec as a REMOVED scenario.
- **Alternatives:** Retain the property as an accepted-but-ignored no-op for backward compatibility.
- **Rationale:** Grep confirmed the property is single-purpose in this crate (topology discovery only) and unreferenced by any other spec feature; `CATALOG_CONNECTION` credentials are a separate mechanism. Silently ignoring a set property is acceptable (existing VS instances do not error), but keeping the constant/plumbing would be dead code. This is a simplification aligned with the mission's "no separate query stack to operate."
- **Promotes to ADR:** no

### [4] Preserve `parse_nr_of_cores_override` and the override-wins precedence unchanged

- **Decision:** Keep `parse_nr_of_cores_override` exactly as-is and keep the `NR_OF_CORES` property taking priority over auto-detection; only the auto-detect source changes (connect-back `PARAM_VALUE` → `available_parallelism()`).
- **Alternatives:** Fold override parsing into the rewritten function.
- **Rationale:** The override contract and its tests are unaffected by this fix; minimising the change surface keeps the fix focused and the exact override-value assertions stable.
- **Promotes to ADR:** no

### [5] Scope Background-wording fix in `parallelism/work-unit-sharding`

- **Decision:** Apply a small CHANGED delta to the one `work-unit-sharding` Background bullet that attributes the node count to `NPROC()`, updating it to `UdfContext::node_count()`; no scenario behaviour changes there.
- **Alternatives:** Leave the downstream Background wording stale.
- **Rationale:** The node-count provenance is referenced narratively in that feature's Background; keeping specs internally consistent avoids a future reader re-deriving the removed `NPROC()` path. The shard-count math and sharding behaviour are unchanged.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
