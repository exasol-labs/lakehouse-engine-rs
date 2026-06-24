# Decision Log: change-memory-pool-sizing

Date: 2026-06-24

## Interview

**Q:** What formula?
**A:** `fraction × (ctx.memory_limit() − overhead_bytes)` — subtract a fixed container overhead from the per-instance limit before applying the fraction. Both become configurable.

**Q:** What should be configurable?
**A:** Both: `MEMORY_POOL_FRACTION` — fraction of the net per-instance budget handed to DataFusion (default 0.6); `INSTANCE_OVERHEAD_MB` — container + binary overhead to subtract before computing the pool (default ~200 MB).

**Q:** Scope?
**A:** Additive: keep the existing `ctx.memory_limit()` path, just subtract `overhead_bytes` first. Low blast radius — one formula tweak plus two new VS properties.

## Design Decisions

### [1] Subtract a constant overhead before applying the fraction

- **Decision:** New positive-limit formula is `budget = max(fraction × (limit − overhead), MIN_POOL_FLOOR)`; the overhead is subtracted as an absolute byte count, not as a proportion.
- **Alternatives:** Lower the fraction (e.g. 0.5) to implicitly account for overhead. Rejected — overhead is roughly constant in absolute bytes (binary + shared libs + allocator arenas + stack), not proportional to the RSS limit, so a constant subtraction models reality and stays correct as the limit scales up or down.
- **Rationale:** `ctx.memory_limit()` is the `RLIMIT_RSS` cap, which counts the ~150 MB Rust SLC container overhead resident before DataFusion's allocator runs. Treating the whole limit as DataFusion-available systematically over-allocates.
- **Promotes to ADR:** yes

### [2] Default `INSTANCE_OVERHEAD_MB` = 200, not 150

- **Decision:** Default the overhead to 200 MB.
- **Alternatives:** 150 MB (the user's empirical estimate) or 256 MB. The 150 MB figure is the floor of the estimate with no margin; 256 MB would shrink the pool more than necessary.
- **Rationale:** Actual RSS overhead from shared libs, allocator arenas, and stacks at startup is hard to bound and varies; under-sizing risks OOM, which is strictly worse than a marginally smaller pool. 200 MB adds a ~50 MB cushion over the estimate at negligible pool cost.
- **Promotes to ADR:** yes

### [3] Floor the final pool budget at 256 MiB

- **Decision:** After computing `fraction × net`, clamp the result up to `MIN_POOL_FLOOR_BYTES = 256 MiB`.
- **Alternatives:** Clamp `net` to a floor instead of the final budget; return an error when `overhead ≥ limit`.
- **Rationale:** Flooring the final budget is the single simplest guard covering every degenerate case (overhead ≥ limit, tiny limit, mis-set overhead) and guarantees a usable session context. Erroring would needlessly fail scans on small-limit nodes.
- **Promotes to ADR:** no

### [4] Carry fraction + overhead in the ScanSpec, not read from UDF metadata

- **Decision:** The fraction and overhead are VS properties, recorded in `adapterNotes`, round-tripped at pushdown, and carried in each per-shard `ScanSpec`; only the limit itself is read at runtime via `ctx.memory_limit()`.
- **Alternatives:** Read them from UDF metadata in the scan UDF.
- **Rationale:** Fraction and overhead are operator policy set at the VS layer, not engine-reported facts. This reuses the existing `DATAFUSION_TARGET_PARTITIONS` / `DATAFUSION_THREADS_PER_UDF` round-trip plumbing verbatim — no new transport channel.
- **Promotes to ADR:** no

### [5] Keep the 1 GiB unknown-limit fallback unchanged

- **Decision:** When `ctx.memory_limit() == 0`, return `DEFAULT_BUDGET_BYTES` (1 GiB) ignoring fraction and overhead.
- **Alternatives:** Apply the floor/fraction logic to the fallback too.
- **Rationale:** The fallback already encodes a conservative known-good budget; fraction and overhead are meaningless without a real reported limit.
- **Promotes to ADR:** no

### [6] Add new property scenarios to create-virtual-schema rather than a new feature

- **Decision:** Add the two property-recording scenarios to `vs-adapter/create-virtual-schema` (taking it to 13 scenarios) rather than splitting off a new feature.
- **Alternatives:** Create a `scan-execution-memory-budget` feature for the new round-trip scenarios.
- **Rationale:** The new scenarios are structurally identical to the existing five adapterNotes-recording scenarios (same feature, same VS-property→adapterNotes pattern). Splitting them off would fragment the single adapterNotes story across two features.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
