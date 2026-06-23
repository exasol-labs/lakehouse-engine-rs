# Follow-up plan prompts (deferred from add-glue-catalog-sigv4-connection)

These two items were split out of the Glue/SigV4/vended-credentials plan as a
follow-up. The SDK bump + `ctx.memory_limit()` wiring already ships in
`add-glue-catalog-sigv4-connection`; these build on top of it.

---

**1. Accurate error for `ResourcesExhausted`.**
Today every stream error funnels through `redact_storage_error` (`scan/emit.rs:30,48`),
so a DataFusion `ResourcesExhausted` (from `GreedyMemoryPool` in `SpillMode::NoDisk`,
`runtime.rs:128`) surfaces as `"scan failed: assigned data could not be read: …"` — a
storage-oriented message that misleads. Give `ResourcesExhausted` its own branch with an
accurate message (e.g. *"scan exceeded its memory budget of N MB; /tmp is not real disk so
the query cannot spill"*), keeping credential redaction for genuine storage errors. Add a
test asserting `ResourcesExhausted` propagates out as a clean, accurately-labeled `UdfError`
(paths: streaming `emit_stream` and the single-row `.collect()` partial-aggregate at
`mod.rs:120`). Currently only `runtime.rs:142` proves the pool errors at budget+1 — nothing
covers end-to-end propagation.

**2. (Research/design) Can clever chunking/splitting prevent `ResourcesExhausted` in the first place?**
Investigate whether per-instance memory pressure can be reduced *before* it trips the pool,
so high-cardinality / large-aggregate queries complete without spill or failure. Consider:
finer work-unit sharding (smaller `G`-shard file assignments → smaller per-instance footprint,
within the `G = node_count × parallelism_factor`, capped 300, balanced ≤300 round-robin
constraints from CLAUDE.md); batch-size / target-partition tuning on the DataFusion side;
streaming-friendlier aggregate plans; or pushing more reduction earlier. Deliverable: a
recommendation on which levers are worth implementing vs. relying on spill, with the
trade-offs (extra scan passes, shard-count caps, metadata overhead). This is design-first —
don't commit to an implementation until the analysis lands.

**Out of scope:** changing the partial/merge aggregate *decomposition itself* — i.e. do NOT
redefine which aggregates are decomposable, the partial-column contract (`partial_emits_items`
/ scan partial SQL), or the merge formulas (`merge_select_items` / `cast_merge_items`, incl.
the `(count, sum, sum_sq)` sufficient-statistics scheme). Tune work-unit sizing and DataFusion
batch config around the existing algebra. If item 2 concludes the only fix is re-sharding by
group key or multi-level/tree aggregation (which WOULD change the decomposition), spin that
out as a separate follow-up plan rather than folding it in.

Verify with host unit tests + `make test-e2e`.
