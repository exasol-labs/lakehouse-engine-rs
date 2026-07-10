# Decision Log: change-scan-fanout-to-scalar-emit

Date: 2026-07-09

## Interview

**Q:** The issue makes `LAKEHOUSE_SCAN` a SCALAR EMIT UDF and removes the SET scan script,
but its SQL examples only show the raw-row path. Every other wrapper currently drives the
SET scan (partial-agg merge, grouped-agg, count-distinct, top-n, broadcast join, N-scan join
fallback). What scope should this plan cover?
**A:** Convert all paths now. Restructure EVERY wrapper onto the distributor + scalar-scan
shape in THIS plan. Because `LAKEHOUSE_SCAN` can no longer be a SET UDF, every wrapper that
drove the SET scan must move to the nested-distributor + scalar-scan shape. The existing
merge/group/join logic moves to the OUTER (ungrouped) query over the scalar scan's emitted
partial rows. The scalar scan fires once per shard (`DISTRIBUTE_FILES` emits one row per
shard) → one partial-agg / partial-distinct / partial-top-n row per shard, exactly as the SET
path did → merge semantics are preserved. Single-shard short-circuit applies uniformly across
all wrappers where the plan resolves to one shard.

**Q:** What is the disposition of #94 (the earlier related issue)?
**A:** #94 is already closed. Do not treat it as open. Reference `Closes #97` in commits.

## Design Decisions

### [1] Split fan-out is the sole scan path, applied unconditionally

- **Decision:** The nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor + scalar `LAKEHOUSE_SCAN`
  is the ONLY scan path, with no flag, VS property, or planner mode branch.
- **Alternatives:** Keep the spike's temporary local A/B flag; or a per-query mode switch.
- **Rationale:** The temp-DB-RAM materialization is universal to raw-row emit under
  `GROUP BY shard_key`; a mode branch is dead weight and a divergence risk. The spike's flag
  was diagnostic scaffolding, not a shipping surface.
- **Promotes to ADR:** yes

### [2] `LAKEHOUSE_SCAN` itself becomes SCALAR — one scan entry point, not two

- **Decision:** Convert the existing `LAKEHOUSE_SCAN` symbol's DDL script type SET → SCALAR;
  do NOT add a second `LAKEHOUSE_SCAN_SCALAR` entry point.
- **Alternatives:** #94's separate `LAKEHOUSE_SCAN_SCALAR` script alongside the SET scan.
- **Rationale:** The compiled scan logic is identical; only the DDL type and the run loop
  change. A single `.so` symbol keeps packaging and fingerprinting unchanged.
- **Promotes to ADR:** yes

### [3] The file distributor is a LUA SET script, not a Rust `.so` entry point

- **Decision:** `LAKEHOUSE_DISTRIBUTE_FILES` is a pure LUA SET passthrough created by its own
  DDL, schema-qualified like the scan/merge scripts; it carries no scan logic and no data.
- **Alternatives:** A third Rust entry point in the `.so`.
- **Rationale:** A passthrough re-emit needs no Rust; keeping it out of the `.so` leaves the
  build/fingerprint surface untouched and makes the fan-out a plain DDL artifact.
- **Promotes to ADR:** yes

### [4] SCALAR batching requires a `while ctx.next()` scan loop with once-per-batch runtime

- **Decision:** `run_scan` loops over the whole scalar input batch, building the DataFusion
  runtime once from the first row's thread config and tearing it down deterministically once.
- **Alternatives:** Keep the single-`ctx.next()` read (drops all rows past the first — the
  spike observed 108M / 210M rows returned); or build/tear down a runtime per row.
- **Rationale:** Exasol batches multiple input rows into one SCALAR `run()` call; scanning only
  the first row silently loses data. Per-row runtime build/teardown would race object_store's
  detached hyper tasks and waste setup cost. Once-per-batch is correct and cheap.
- **Promotes to ADR:** yes

### [5] Drop the `SELECT * FROM (...)` scan wrapper

- **Decision:** Attach `ORDER BY`/`LIMIT` and the merge/group-by directly to the outer
  ungrouped scalar select; no `SELECT * FROM (...)` in any wrapper.
- **Alternatives:** Keep the wrapper for uniformity.
- **Rationale:** With `GROUP BY` nested in the distributor, the outer scalar select is already
  an ungrouped projection; the wrapper was the un-flattenable materialization boundary the
  whole change removes. (Through the normal VS path Exasol re-wraps under the user's own
  projection anyway, so this mainly removes redundant nesting; the streaming win is on the
  VS-bypass / CTAS path.)
- **Promotes to ADR:** no

### [6] N-scan join fallback renders `INNER JOIN … ON` with greedy-attach and per-leg filter pushdown

- **Decision:** Render the fallback FROM as a left-to-right `INNER JOIN … ON` chain, attaching
  each condition to the earliest join point where all its tables are in scope (by the SET of
  `tableName`s, never by column name; empty join point → `ON 1=1`), and push each side's
  side-local WHERE conjuncts into that leg while leaving cross-table/OR-spanning/untagged
  residual conjuncts in the outer WHERE.
- **Alternatives:** Keep the comma cross-join + flat WHERE (already correct, order-agnostic).
- **Rationale:** The `INNER JOIN … ON` shape is what Exasol's optimizer plans best and keeps
  each leg's scan lean; attaching by `tableName` set stays correct under shared column names.
- **Promotes to ADR:** yes

### [7] Result-equivalence invariants are the correctness guardrail for every converted path

- **Decision:** Every converted wrapper's spec carries an explicit invariant that the returned
  result equals (as an order-independent multiset where ordering is not requested) single-node
  evaluation — union for raw, SUM/MIN/MAX/AVG-pair for aggregate, re-group for grouped,
  merge-sort for top-n, reconstructed join for the fallback.
- **Alternatives:** Rely on E2E parity tests alone.
- **Rationale:** The refactor moves merge logic across query levels; making equivalence an
  explicit per-scenario invariant prevents a silent regression (e.g. dropped shard rows).
- **Promotes to ADR:** no

## Review Findings

Code review (Phase 4): correctness verdict **PASS** — batch loop (no row dropped, once-per-batch
runtime/teardown), fan-out primitive (`common` spliced once, only `files` fans out), result-equivalence
across all wrappers, N-scan greedy-attach + WHERE split (scope by tableName set), and SQL quoting all
traced clean. 0 must-fix; 4 nice-to-have:

- **[fixed] Stale doc comments** — 4 sites still said "SET UDF"/"SET SCRIPT" after the SET→SCALAR
  conversion (`scan/mod.rs`, `pushdown.rs`). Updated to "SCALAR EMIT UDF".
- **[fixed] Stale arg-count comment** — `build_scan_driving_sql` comment said "8 args" (now 10) and
  carried a `ponytail:` work marker. Corrected/removed.
- **[fixed] Latent panic** — `clamp(1, last_join_point)` in the join-chain builder would panic if ever
  called with a single leg (`clamp` min>max). Unreachable today (N≥2 fallback); hardened defensively.
- **[deferred] `ScanUdfNames` struct** — three adjacent same-typed `&str` UDF-name params are a
  transposition footgun. Not applied: the author explicitly rejected a params struct, there is no
  actual bug (tests green), and bundling touches every builder signature — needless regression risk
  immediately before recording. Recorded here as a known future cleanup.

## E2E blocker found and fixed (2026-07-09/10) — one-line call-site bug, NOT a design flaw

**Resolution: fixed in `build_fan_out_inner`.** The plan's design (decisions [1]/[2]) is sound; the
scalar scan driven by the LUA-SET distributor relation IS supported by Exasol — verified against the
deployed, working spike on the staging cluster (`DBX` VS, `LAKEHOUSE_SCAN_SCALAR` + `LAKEHOUSE_DISTRIBUTE_FILES`).

**Symptom.** First `make test-e2e` failed 7/8 capability tests: `Adapter generated invalid pushdown
query ... The script has a static return argument definition. Dynamic return arguments are not
supported in this case` (SQL state 04000/42000).

**Root cause (verified by a minimal live repro, not assumed).** `build_fan_out_inner` rendered the
multi-shard distributor call WITH a query-side `EMITS (files VARCHAR(2000000))` clause. But
`LAKEHOUSE_DISTRIBUTE_FILES` is a LUA SET script with a STATIC `EMITS ("FILES" VARCHAR(2000000))`
definition — and Exasol rejects a query-side `EMITS` on a statically-defined script. Minimal repro
on the local stack:
- `SELECT LHVS.LAKEHOUSE_DISTRIBUTE_FILES(files) EMITS (files VARCHAR(2000000)) FROM (VALUES ...) GROUP BY shard_key` → the exact error.
- `SELECT LHVS.LAKEHOUSE_DISTRIBUTE_FILES(files) FROM (VALUES ...) GROUP BY shard_key` → returns rows.

The scalar `LAKEHOUSE_SCAN` was never the problem: its entry point has a null (dynamic) output
schema, so its query-side `EMITS(...)` is legitimate; staging drives the identical scalar shape from
a relation with no trouble. The confusing part of the initial diagnosis — that the passing test
(`e2e_selectlist_expression_pushdown`) took the single-shard short-circuit (which omits the
distributor entirely, so it never hit the bad clause) while the failing tests took the multi-shard
path (which did) — matches this root cause exactly.

**Fix.** Drop the query-side `EMITS (...)` from the distributor call in `build_fan_out_inner`
(the scan's own `EMITS(...)` stays). Matches the working staging pushdown verbatim. Unit tests that
asserted the old shape updated (`pushdown.rs`, `scan_plan_shape.rs`); a negative assertion pins that
the distributor call carries no query-side EMITS. Spec deltas needed no change (they describe the
distributor semantically, never asserting a query-side EMITS).
