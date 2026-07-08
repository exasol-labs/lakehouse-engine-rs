# Decision Log: add-group-by-and-sql-comprehension

Date: 2026-06-22

## Interview

**Q:** Which feature should be the next version target — GROUP BY pushdown, Databricks/Unity access, or a benchmark harness?
**A:** GROUP BY pushdown.

**Q:** Which forms of GROUP BY key should be pushed down — bare columns only, or also expressions? And how should we handle the expression translation?
**A:** Columns and expressions; add a polyglot Rust crate for SQL comprehension.

**Q:** What should happen when a GROUP BY query would produce a very high number of distinct groups (high cardinality)?
**A:** Add a guard; this is not a PoC; it shall be a usable engine.

**Q:** How should the new SQL-comprehension crate relate to the existing `predicate.rs` walker — new standalone parser, generalise the JSON-AST walker, or something else?
**A:** Generalise the JSON-AST walker (recommended approach). No new parser dependency; extend the proven serde_json walk to cover scalar functions, arithmetic, and CAST for group keys and predicates.

**Q:** Should the coverage of filter/expression pushdown stay as-is, or should we also broaden it beyond comparison predicates?
**A:** Also broaden filter/expression coverage (IN, BETWEEN, LIKE, IS NULL, AND/OR nesting, scalar functions in filters — noting that predicate.rs already has most of these; the new crate will confirm and extend coverage).

**Q:** Should the mission.md be updated to reflect the shift from PoC to usable engine?
**A:** Update mission.md in this plan (recommended). Drop "PoC only", state the usable-engine intent, record the safety mechanism as a real requirement.

### Revision interview (corrected Exasol parallelization & memory model)

**Q:** A corrected reading of the Exasol engine internals shows groups drive UDF *invocations* (not OS processes), parallel instances on a node = a VM pool sized to `NR_OF_CORES`, and `GROUP BY IPROC()` yields one group per node (capping parallelism at node count). How should the scan fan out instead?
**A:** Oversubscribe: shard count `G = node_count × parallelism_factor`, capped at 300 (stay in Exasol's round-robin distribution regime; above 300 it hash-partitions), clamped ≥1 and ≤ file_count. `parallelism_factor` is a VS property (default 8). Keep the NPROC node-count capture (it feeds G). Assign files with the existing balanced split, passing G instead of node_count.

**Q:** What is the driving SQL?
**A:** Replace `GROUP BY IPROC(), shard_key` with `GROUP BY shard_key` — each shard is its own group, so Exasol distributes groups across nodes AND multiplexes them onto each node's core pool. The file→shard assignment is the balanced split.

**Q:** The file-count cardinality guard (`GROUP_BY_CARDINALITY_LIMIT`) was the prior memory-safety mechanism. Should it stay?
**A:** No — remove it entirely. Instead the scan UDF reads `ctx.memory_limit()` and sizes the DataFusion `MemoryPool` to ~0.6 of it (fallback 1024 MB when the limit is `0`/unknown). Spill backstop: probe whether `/tmp` is real disk with free space (`/proc/mounts` tmpfs check + `statvfs`); real disk → `FairSpillPool` + `DiskManager` rooted at `/tmp` (completes at any cardinality); tmpfs/too-small → `GreedyMemoryPool` returning a clean `ResourcesExhausted` instead of OOM-crashing.

**Q:** Where does `ctx.memory_limit()` come from?
**A:** From the sibling-repo plan `language-container-rs:add-memory-limit-metadata` (already planned), which adds `UdfContext::memory_limit() -> u64` (bytes; `0` = unbounded/unknown). This plan depends on it landing first.

**Q:** Should the parallelization behavior be documented for future work?
**A:** Yes — add a section to `CLAUDE.md` documenting groups→invocations (not processes), the per-node VM pool = `NR_OF_CORES`, why `GROUP BY IPROC()` caps at node count, the oversubscribed `GROUP BY shard_key` with G capped 300 (round-robin ≤300, hash above), the per-instance memory limit from metadata, and the engine's 80% concurrency stall — with the [redacted] citations.

## Design Decisions

### [1] New workspace crate `crates/vs-expression` for expression translation

- **Decision:** Create a standalone workspace crate (`crates/vs-expression`) containing the full serde_json expression-node walker, generalised from `adapter/predicate.rs`. The crate has no lakehouse-engine internals in its API; its only dependencies are `serde_json` and `exasol-udf-sdk` (for `UdfError`). Delete `adapter/predicate.rs` and replace all its callers with `vs_expression::render_df_filter_safe`.
- **Alternatives:** (a) Keep the walker as an inline module in lakehouse-engine but extend it in place — rejected: blocks reuse by the sibling project and keeps a tight coupling between expression logic and engine internals. (b) Introduce a SQL-parser dependency (sqlparser-rs) as the IR — rejected: user explicitly declined; overweight for the narrow translation job; the serde_json walker is already proven.
- **Rationale:** Standalone crate creates a clean, testable, reusable translation layer. The API surface (three functions: raising, safe, filter-safe) is small and stable. Long-term monorepo convergence with the sibling project becomes trivial.
- **Promotes to ADR:** yes

### [2] GROUP BY group-key values emitted as plain columns, wrapper groups on column refs

- **Decision:** The scan UDF emits computed group-key values as plain output columns (not re-rendered expressions). The EMITS declaration names them `GK_0`, `GK_1`, ... and the outer wrapper SQL groups on `GK_0`, `GK_1`, ... matching the physical column positions.
- **Alternatives:** Re-render the same GROUP BY expression in the outer wrapper SQL (risky: any mismatch between the UDF-side rendering and the wrapper-side rendering produces wrong grouping). Emit group keys by column name only (breaks expression group keys that produce a computed value without a stable column name).
- **Rationale:** Emitting the computed value eliminates expression-rendering consistency risk between the partial scan and the merge. The wrapper just groups on output column positions — a simple, correct pattern.
- **Promotes to ADR:** yes

### [3] SUPERSEDED — File-count heuristic cardinality guard

- **Status:** Superseded by decision [8] (metadata-sized memory pool + spill backstop). The file-count guard and `GROUP_BY_CARDINALITY_LIMIT` are removed entirely. Retained here only to record the rejected approach.
- **Why superseded:** The corrected Exasol memory model exposes a real per-instance memory limit in UDF metadata, so the engine can be bounded directly rather than guessing distinct-group counts from a file-count heuristic with no statistical basis. A bounded pool + spill gives correctness at any cardinality (spill) or a clean failure (hardcap); the engine also self-throttles concurrency at 80% of the per-process heap.
- **Promotes to ADR:** no

### [7] SUPERSEDED — Reuse the `GROUP BY IPROC()` fan-out (shipped ADR-007)

- **Status:** Superseded by decision [9] (oversubscribed `GROUP BY shard_key`). Retained to record why the shipped mechanism is wrong.
- **Why superseded:** Verified in [redacted]/script-languages: groups drive UDF *invocations*, not OS processes; parallel instances on a node are a VM pool sized to `NR_OF_CORES` (`[redacted]`, `[redacted]`); `GROUP BY IPROC()` (`[redacted]`) yields exactly one group per node → caps parallelism at the node count and leaves a node's other cores idle.
- **Promotes to ADR:** no

### [8] Memory safety via metadata-sized DataFusion pool + spill-or-hardcap (REPLACES planned ADR-011)

- **Decision:** The scan UDF reads `ctx.memory_limit()` (bytes) and sizes the DataFusion `RuntimeEnv` `MemoryPool` to ~0.6 of it, leaving headroom below the engine's 80% concurrency-stall threshold; falls back to a conservative 1024 MB default when the limit is `0`/unknown. A `/tmp` probe (tmpfs detection via `/proc/mounts` + `statvfs` free space) selects the pool: real disk → `FairSpillPool` + `DiskManager` rooted at `/tmp` (completes at any cardinality); tmpfs/too-small → `GreedyMemoryPool` returning a clean `ResourcesExhausted` instead of OOM-crashing. The `/tmp` spill is transient per-invocation scratch, never persistent state.
- **Alternatives:** (a) File-count cardinality guard (decision [3]) — rejected: heuristic with no statistical basis. (b) Per-shard emitted-group cap with UDF abort — rejected: produces partial results, adds abort logic. (c) Unbounded pool — rejected: OOM-crashes the UDF process at high cardinality.
- **Rationale:** Sizing to the real per-instance limit lets the engine self-manage concurrency; spill gives correctness at any cardinality; the hardcap path fails cleanly when no spill disk exists. Layered with oversubscribed sharding (smaller per-instance footprint) and the engine's 80% stall, this is a robust, statistics-free safety model.
- **Promotes to ADR:** yes

### [9] Oversubscribed `GROUP BY shard_key` work-unit sharding (SUPERSEDES ADR-007)

- **Decision:** Replace `GROUP BY IPROC()` node-sharding with oversubscribed work-unit sharding: shard count `G = node_count × parallelism_factor` (`parallelism_factor` a VS property, default 8), capped at 300 and clamped to `[1, file_count]`. The scan-driving SQL groups on `shard_key` (each shard its own group). The `parallelism/iproc-sharding` feature is renamed to `parallelism/work-unit-sharding`; the balanced `partition_files` split is reused with G instead of node_count; NPROC node-count capture (ADR-006) is kept to feed G.
- **Alternatives:** (a) `GROUP BY IPROC()` (shipped, decision [7]) — rejected: caps parallelism at node count. (b) `GROUP BY IPROC(), shard_key` — rejected: redundant; `shard_key` alone lets Exasol distribute groups across nodes and multiplex onto each node's core pool. (c) Uncapped G — rejected: above `max_dynamic_group_count` (default 300) Exasol hash-partitions groups (unbalanced) instead of round-robin (`[redacted]`).
- **Rationale:** Oversubscription uses each node's full `NR_OF_CORES` VM pool; the 300 cap keeps the group set in the round-robin (balanced) distribution regime. Two-level grouping composes cleanly: inner `shard_key` parallelizes, DataFusion does the user GROUP BY inside each instance, the outer wrapper merges partials by the user group keys.
- **Promotes to ADR:** yes

### [10] Cross-repo dependency on `language-container-rs:add-memory-limit-metadata`

- **Decision:** The memory-pool sizing (decision [8]) depends on the sibling-repo plan `language-container-rs:add-memory-limit-metadata` landing first, which adds `UdfContext::memory_limit() -> u64` (bytes; `0` = unbounded/unknown sentinel). Until it lands, `ctx.memory_limit()` is unavailable: the pool-sizing task is blocked and downstream scan tasks operate against the `0`-sentinel default-budget path only. Do not reimplement the metadata read locally.
- **Alternatives:** Reimplement the proto deserialization + accessor in this repo — rejected: duplicates SDK responsibility across repos and risks drift in the wire-protocol decoding.
- **Rationale:** The accessor is handshake-metadata plumbing that belongs in the SDK (`language-container-rs`); a single source avoids divergent decodings. Recording the dependency makes the build-order constraint explicit for the implementer.
- **Promotes to ADR:** yes

### [4] Advertise both `AGGREGATE_GROUP_BY_COLUMN` and `AGGREGATE_GROUP_BY_EXPRESSION`

- **Decision:** Add both capability strings to `capabilities.rs`. Do NOT add `AGGREGATE_GROUP_BY_TUPLE` (requires supporting arbitrary tuple expressions in GROUP BY, beyond scope).
- **Alternatives:** Column-only first (`AGGREGATE_GROUP_BY_COLUMN`) — rejected: user confirmed expression group keys are in scope for this version; not advertising the capability would mean Exasol never sends expression GROUP BY for pushdown.
- **Rationale:** Both capabilities required to receive the full range of GROUP BY pushdown requests from Exasol that the implementation supports.
- **Promotes to ADR:** no

### [5] LIMIT excluded from per-shard grouped scan; applies only in outer wrapper

- **Decision:** When the scan spec has `group_keys` set, the adapter MUST NOT populate `limit` in the spec, and the UDF MUST NOT apply LIMIT to its per-group partial scan. LIMIT goes only in the outer wrapper SQL.
- **Alternatives:** Apply LIMIT per shard — rejected: would silently drop groups before the merge, producing incorrect GROUP BY results when the LIMIT triggers.
- **Rationale:** Standard correctness requirement for grouped aggregates with LIMIT: the limit must apply after all groups are merged.
- **Promotes to ADR:** no

### [6] Mission reframe: drop "PoC only", state usable-engine intent

- **Decision:** Edit `specs/mission.md` to remove the business constraint "PoC only — feasibility and measurement, not production hardening" and replace it with "Usable engine — correctness and safety guards are first-class requirements; the engine is designed to be operated, not just measured." Add the bounded-execution mechanism (metadata-sized memory pool + spill + oversubscribed sharding, per decisions [8] and [9]) to Core Capabilities; update Capability 3, the architecture diagram, and the glossary to work-unit sharding. Keep non-goals unchanged (caching, joins, HAVING, COUNT(DISTINCT)).
- **Alternatives:** Leave mission.md unchanged — rejected: user explicitly requested the update; leaving PoC language in the mission would create contradiction with the cardinality guard requirement.
- **Rationale:** Mission document is the ground truth for what the engine is. The user's statement "this is not a PoC; it shall be a usable engine" is a mission-level declaration.
- **Promotes to ADR:** no

## Review Findings

<!-- Significant code-review findings that changed implementation direction. -->
<!-- Populated by speq-implement after code review. -->
