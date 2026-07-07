# Backlog

Follow-up work not yet scheduled into a plan. Each entry records the finding and
proposed direction so it can be promoted into a `/speq:plan` without re-deriving
the analysis.

---

## BL-001: JOIN pushdown

**Raised by:** `add-capability-alignment` (2026-06-22)
**Status:** Open — deliberately out of scope; `JOIN*` capabilities not advertised

Today a query joining two VS tables is not pushed down: Exasol issues a separate
row-scan pushdown per table and joins the results in its core engine (correct, no
pushdown benefit). Pushing the join into DataFusion requires both join inputs in
one scan invocation, which the single-table file-sharding model does not provide.

### Phase 1 — broadcast inner equi-join (feasible)

Resolve the small side fully, replicate it into every scan invocation, join each
fact-table shard against the full copy in node-local DataFusion. Correct with no
cross-shard exchange. Advertise only `JOIN`, `JOIN_TYPE_INNER`, `JOIN_CONDITION_EQUI`;
add a small-side size threshold and a replicate-into-scan-spec mechanism. Covers the
star-schema case and reuses the existing `vs-expression` translator for the condition.

### Phase 2 — shuffle/partitioned join (hard; gate on benchmarks)

Large/large joins need both tables hash-partitioned on the join key so matching keys
co-locate per shard — a sharding-model redesign with no cross-shard exchange today,
and files can't be assigned to key-shards without reading them first. Exasol already
joins large/large well, so the pushdown win is mostly Iceberg pruning. Investigate
only if benchmarks justify it.

---

## BL-002: Adopt lc-rs `feat/add-emit-transfer-spikes` once released

**Raised by:** performance investigation vs Trino (2026-07-06), see
[`docs/performance.md`](../docs/performance.md#bottleneck-analysis-2026-07-06)
**Status:** Open — blocked on an external, unreleased dependency

`language-container-rs` branch `feat/add-emit-transfer-spikes` (local, uncommitted-turned-commit
`8387a1f` at the time of this writing, not released to crates.io) pre-sizes the emit/ingest `Vec`
buffers in `EmitBuffer`/`encode_slice`/`decode_string_block` and replaces `chrono`/`Decimal`
`Display`-based DATE/TIMESTAMP/DECIMAL string formatting with hand-rolled fixed-format byte
parsers, verified byte-identical to the paths they replace by unit test. `lineitem`'s 3 DATE + 4
DECIMAL columns are exactly the string-block-encoded types this targets, and every wide-lineitem
TPC-H query (Q2, Q3, Q5, Q9b, NQ1) pays that encoding cost. ABI version is unchanged (still 6), so
this is expected to be a drop-in `exasol-udf-sdk`/SLC version bump with no wire-format break, once
released.

Not adoptable now: `exasol-udf-sdk`/SLC are pinned to the released 0.20.2; this branch only exists
locally on the machine that authored it. Revisit when it ships — bump the SDK/SLC pin
(`crates/lakehouse-engine/Cargo.toml`, `Makefile`'s `SLC_VERSION`, `bench/.env`'s
`BENCH_SLC_VERSION`) and re-run `bench/compare_all.sh` to quantify the query-level delta.
