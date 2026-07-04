# Decision Log: add-topn-pushdown

Date: 2026-07-04

## Interview

No dedicated end-user interview was conducted for this plan. It is a follow-on fix
discovered live while benchmarking the sibling plan (`add-arithmetic-aggregate-pushdown-and-benchmark-suite`)
this session, under the standing user directive captured in that plan's interview:

**Q (carried over):** "Win on all queries" vs mission.md's join-pushdown non-goal — how to reconcile?
**A (carried over):** Tune/optimize the Exasol-side query path where legitimately possible WITHOUT building join pushdown or changing the file-sharding architecture. The join-shaped Q2/Q3/Q5 losses were explicitly accepted as out of scope; non-join losses that are legitimately fixable are in scope.

**Derived scope for this plan (no new user Q&A):** NQ4 (`ORDER BY … LIMIT`, single table, no join, no GROUP BY) is a non-join loss (12.03s vs Trino 4.71s) that is legitimately fixable via a partial/merge top-N without touching joins or sharding — so it fits the standing directive and was called out as worth fixing rather than accepted as structural.

## Design Decisions

### [1] Fix the top-N loss by advertising ORDER_BY_COLUMN + a partial/merge top-N, not by touching sharding or joins

- **Decision:** Advertise `ORDER_BY_COLUMN` and push `ORDER BY <bare projected col(s)> LIMIT n` down as a per-shard bounded top-N (each shard `ORDER BY … LIMIT n`) merged by an Exasol-side outer `ORDER BY … LIMIT n`, reusing the SHAPE of the existing aggregate partial/merge machinery.
- **Alternatives:** (a) Leave it as a raw scan and accept the loss — rejected: it is a non-join, single-table query squarely within the standing "optimize where legitimately possible" directive. (b) Change the file-sharding to co-locate top rows — rejected: violates the sharding-architecture non-goal. (c) Build a general ORDER BY pushdown (expression keys, offset, ordered aggregates) — rejected as over-scope; column top-N covers the target and the common shape.
- **Rationale:** The blocker is purely that `ORDER_BY_COLUMN` is unadvertised, so Exasol never delegates the ordering and raw-emits the whole table. Advertising it + a bounded-sort/merge path is the smallest sound change and mirrors the just-shipped aggregate partial/merge shape.
- **Promotes to ADR:** yes

### [2] Whether this is pure optimization or also a latent correctness fix is gated on live capture (A1)

- **Decision:** Make A1 (live `EXPLAIN VIRTUAL` of the NQ4 shape, reading whether Exasol pushes a bare `limit` for an ORDER BY query today) a hard gate that decides the plan framing before coding.
- **Alternatives:** Infer from the captured NQ4 result being correct-but-slow that today is safe, and skip verification — rejected: the sibling plan's methodology is live-capture-first; the code read (`extract_limit` reads `pushdownRequest.limit.numElements`; `build_row_scan_sql` pushes that limit per-shard) shows that IF Exasol ever pushed a bare `LIMIT` alongside an unpushable ORDER BY, today's code WOULD wrongly truncate — so the "safe today" conclusion must be verified, not assumed.
- **Rationale:** The strong evidence (NQ4 rows are correctly, fully descending) says Exasol withholds the limit today (safe-but-slow), making this pure optimization — but the cost of being wrong is a silent correctness bug, so it is verified live. Either way the plan adds the defensive invariant (decision [4]).
- **Promotes to ADR:** yes

### [3] Advertise ORDER_BY_COLUMN only; ORDER_BY_EXPRESSION and LIMIT_WITH_OFFSET stay absent

- **Decision:** Advertise only `ORDER_BY_COLUMN`. Keep `ORDER_BY_EXPRESSION` and `LIMIT_WITH_OFFSET` unadvertised so Exasol never pushes an expression sort key or an OFFSET the adapter has no path for.
- **Alternatives:** Advertise expression ordering / offset too — rejected: they add rendering + bounded-sort-with-skip complexity with no evidenced need; leaving them unadvertised makes the unsupported shapes structurally impossible in the request rather than something the adapter must defensively decline at runtime.
- **Rationale:** Column sort keys cover NQ4 and the common top-N shape; the capability surface stays exactly as wide as the backing path, matching the codebase's "advertise only what the translator/planner backs" discipline.
- **Promotes to ADR:** yes

### [4] Never push a bare per-shard LIMIT ahead of a global sort — the anti-wrong-truncation invariant

- **Decision:** The per-shard row limit is emitted ONLY alongside the matching per-shard `ORDER BY` (the matched top-N shape). For any ORDER-BY-carrying request the adapter does not match as a top-N, the per-shard limit is withheld and row selection is left to the Exasol-side ordering.
- **Alternatives:** Keep pushing the per-shard limit whenever a `limit` is present (today's row-scan behavior) — rejected: once `ORDER_BY_COLUMN` is advertised, Exasol will send `order_by` + `limit` together, and a bare per-shard limit ahead of the sort would let each shard return an arbitrary (not top-ranked) subset → silent wrong truncation.
- **Rationale:** This invariant is what makes advertising the capability safe across every shape, not just the optimized one. It is asserted directly as a spec scenario and a unit test.
- **Promotes to ADR:** yes

### [5] Returned SQL is self-contained (own outer ORDER BY … LIMIT), not dependent on an Exasol re-sort backstop

- **Decision:** The matched top-N returns an outer `SELECT <proj> FROM (<fan-out>) ORDER BY <keys> LIMIT n`, fully specifying the final ordering itself.
- **Alternatives:** Rely on Exasol re-applying the pushed ORDER BY at the top level as a backstop (the model documented for `LIMIT`/`HAVING`) — kept as the safety net for the UNmatched decline shapes, but NOT relied on for the matched path.
- **Rationale:** Removing the matched path's dependence on "does Exasol re-sort?" eliminates a whole class of risk; the outer wrapper already exists for the fan-out so adding the ordering is cheap. A2 still verifies the backstop for the decline shapes.
- **Promotes to ADR:** yes

### [6] Sort keys must be bare columns present in the projection (MVP); multi-key is free, unprojected keys deferred

- **Decision:** Match a sort key only when it is a bare column reference that also appears in the query's projection. Multiple sort keys are handled by the same comma-list rendering. Unprojected sort keys and expression keys decline to the pre-existing plan.
- **Alternatives:** Emit unprojected sort keys as extra trailing EMITS columns dropped by the outer SELECT — a clean generalization, deferred to keep the MVP's outer merge sort on already-emitted columns with zero extra machinery. NQ4's sort key (`L_EXTENDEDPRICE`) is projected, so the restriction does not block the target.
- **Rationale:** The projected-key restriction is the minimal provably-simple version; multi-key falls out of list rendering for free, so it is included unless an edge cost appears in implementation (then drop to single-key and decline multi-key — still an acceptable v1).
- **Promotes to ADR:** no

### [7] Direction + NULL placement must be rendered identically per-shard and in the merge

- **Decision:** Render an explicit `ASC`/`DESC` and `NULLS FIRST`/`NULLS LAST` on BOTH the per-shard `ORDER BY` (scan UDF) and the outer merge `ORDER BY` (adapter), using the direction/NULL semantics captured live in A2, rather than relying on DataFusion's or Exasol's default NULL ordering.
- **Alternatives:** Render only direction and let NULL ordering default on each side — rejected: if the scan UDF's default NULL placement differs from Exasol's, the per-shard cut and the merge disagree on ranking and the top-N silently diverges from single-node results near NULLs.
- **Rationale:** The distributed top-N is exact only if both sorts induce the same ranking; explicit NULL placement is the one subtle correctness detail and is called out as an [expert] task with a dedicated NULL-placement test.
- **Promotes to ADR:** yes

### [8] Decline the top-N shape when a sort key column needs the JSON-fallback VARCHAR cast (B3b)

- **Decision:** `detect_topn` resolves each sort key column's Arrow type from its `LogicalField.arrow_type` tag (via `types::mapping::arrow_type_from_tag`) and returns `None` (declining the whole ordered-top-N shape, falling back to the safe raw-scan path) whenever `types::mapping::needs_json_fallback` is true for that type. A sort key column absent from the resolved `logical_schema` also declines defensively. `logical_schema: &[LogicalField]` was threaded into `detect_topn` (it is already resolved in `handle_pushdown` before the call).
- **Why (the gap):** B4's implementer flagged, while reviewing their own per-shard ORDER BY rendering, that `build_scan_sql` emits `CAST(col AS VARCHAR)` (a JSON string) in the SELECT list for a JSON-fallback-typed column, but the per-shard `ORDER BY col` binds against the FROM-clause row source — the REAL native value, before that cast. Exasol's outer merge (decision [5]) sees only the emitted JSON string, so it re-ranks lexicographically. Per-shard and merge would disagree on ranking and silently corrupt the global top-N (each shard's LOCAL top-N stays correct; the GLOBAL merge picks the wrong rows). This was not in the original plan (decisions [1]–[7] assumed sort keys sort and emit the same representation) and is a ship-blocking correctness gap for any future ordered-top-N over a List/Struct/Decimal256/Binary/etc. column.
- **Not hit by NQ4:** `L_EXTENDEDPRICE` is a plain in-range DECIMAL (not a fallback type), so NQ4's ranking is unaffected and C1 can proceed — but B5 (E2E) and C1 (live NQ4) are gated on this fix landing first for correctness.
- **Alternatives:** (a) Emit the sort key uncast and cast only a duplicate projection column — rejected: changes the emit contract and needs extra trailing columns dropped by the outer SELECT (the deferred generalization from decision [6]), disproportionate to a shape no evidenced query hits. (b) Sort the merge on the pre-cast value — impossible: Exasol only ever receives the emitted representation.
- **Known scope note:** the logical-schema tag vocabulary (`arrow_type_to_tag`/`arrow_type_from_tag`) collapses List/Struct/Binary/Time/out-of-range-Decimal and all non-primitive types to `utf8` at `build_logical_schema` time, so the ONLY JSON-fallback type reachable through `arrow_type_from_tag` today is an out-of-range `decimal128(p>36,…)` (the unit test uses `decimal128(40,6)` as the representative trigger). The guard is nonetheless the correct seam: it is evaluated on the same type info the scan path keys its own `needs_json_fallback` cast decision on, and it becomes fully load-bearing the moment the tag vocabulary is enriched to preserve richer types. Declining conservatively is always correctness-safe (worst case: a rare shape falls back to the raw scan).
- **Promotes to ADR:** yes

## Gating Investigation Results (Group A, 2026-07-04)

### A1 — Live-verified: today's behavior is PURE OPTIMIZATION, not a latent bugfix

**Live capture** (`EXPLAIN VIRTUAL SELECT L_ORDERKEY, L_EXTENDEDPRICE FROM TPCH.LINEITEM ORDER BY
L_EXTENDEDPRICE DESC LIMIT 20;` against test1, `TPCH` VS, current staged `.so` which does NOT
advertise any `ORDER_BY*` capability):

- `getCapabilities` response DOES include `"LIMIT"` (and does not include any `ORDER_BY*`).
- The `pushdownRequest` for the NQ4 shape contains ONLY `from`, `selectList`,
  `selectListDataTypes`, `type` — **no `limit` key and no `orderBy` key at all**. Exasol withholds
  the `limit` element entirely once the query also carries an ORDER BY it cannot delegate (even
  though `LIMIT` is advertised and would normally be sent for a plain `LIMIT`-only query).
- Consequently the returned pushdown SQL is a bare, un-limited fan-out:
  `SELECT * FROM (SELECT "LHVS".LAKEHOUSE_SCAN(...) EMITS (...) FROM (VALUES ...) AS
  shards(shard_key, files) GROUP BY shard_key)` — no outer `LIMIT`, and the embedded common-spec
  JSON literal has no `"limit"` key at all.

**Code read** (`crates/lakehouse-engine/src/adapter/pushdown.rs`, current `main`, no `order_by`
code exists yet):
- `extract_limit` (line ~2795) reads `pushdownRequest.limit.numElements` unconditionally — returns
  `None` whenever the `limit` key is absent, which the live capture shows is exactly what happens
  for an ORDER-BY-carrying query today.
- The row-scan branch (`handle_pushdown`, ~line 2112) sets `spec_template.limit = limit`
  unconditionally (no ORDER-BY-awareness) and this flows into the COMMON blob
  (`to_common_json()`) shared by every shard — i.e. **today's code has no defensive check at all**;
  if Exasol ever did send a bare `limit` alongside an unpushed `order_by`, this row-scan branch
  would push that limit to EVERY shard via the common spec (read by the scan UDF's
  `build_scan_sql` as a per-shard `LIMIT`), which — combined with no shard-level ordering — would
  silently truncate to an arbitrary (not top-ranked) subset per shard. `build_row_scan_sql` also
  applies the same limit again as an outer `SELECT * FROM (fan-out) LIMIT n` (arbitrary post-union
  rows, still no ordering).

**Conclusion:** the live capture proves Exasol structurally withholds `limit` whenever the
accompanying `order_by` can't also be delegated (LIMIT capability alone is not enough to trigger a
`limit` push when a sort is present) — so **today's code path can never exercise the theoretical
per-shard-truncation danger described above**: the request itself never contains the ingredients
(`limit` + unpushed `order_by`) that would trigger it. This plan is **pure optimization**, not a
bugfix. The plan's decision [4] (never push a bare per-shard LIMIT ahead of a global sort) remains
adopted anyway as a structural invariant for the NEW code path once `ORDER_BY_COLUMN` is
advertised — because advertising the capability is exactly what will start putting `order_by` +
`limit` together in requests B3 must handle, and B3's own detection/shape-matching becomes the only
thing standing between "matched top-N" and "unmatched ORDER BY + LIMIT" once that door is opened.

### A2 — Exact `order_by` request field shape (from Exasol's public Virtual Schema API doc)

A scratch `.so` deploy to test1 was judged too heavy/risky for this step (test1 is a shared live
cluster used concurrently by other benchmark/E2E work; overwriting the staged production `.so` at
the shared BucketFS path to observe one request shape was not worth the disruption risk). Used the
documented fallback instead: `exasol/virtual-schema-common-java`
(`doc/development/api/virtual_schema_api.md`, fetched raw from GitHub `main`), which documents the
exact JSON wire shape Exasol's core sends (this is generated by the Exasol DB core itself, not a
Java-library artifact — the Java library's docs mirror the core protocol verbatim, same as the
`selectList`/`having`/`filter` shapes already confirmed live in A1 above).

Verbatim shape (`pushdownRequest.orderBy`, sibling to `pushdownRequest.limit`):

```json
"orderBy": [
    {
        "type": "order_by_element",
        "expression": { "type": "column", "columnNr": 1, "name": "USER_ID", "tableName": "CLICKS" },
        "isAscending": true,
        "nullsLast": true
    }
],
"limit": { "numElements": 10 }
```

Field semantics, pinned for B3's parser:
- `orderBy` is a top-level array sibling of `from`/`selectList`/`filter`/`limit` inside
  `pushdownRequest`, present only when the adapter advertises an `ORDER_BY_*` capability.
- Each element has `"type": "order_by_element"` and three fields:
  - `expression` — a standard expression node, IDENTICAL shape to nodes already parsed elsewhere in
    this codebase (`selectList`/`filter`/`having`/`groupBy` column nodes are
    `{"type":"column","columnNr":N,"name":"COL","tableName":"TBL"}` — confirmed live in A1's
    capture). Advertising `ORDER_BY_COLUMN` only (not `ORDER_BY_EXPRESSION`, per decision [3])
    means Exasol will only ever send a bare `"type":"column"` node here — B3 can reuse whatever
    helper already extracts a bare column reference from a `selectList`/`groupBy` node.
  - `isAscending` (boolean) — `true` = `ASC`, `false` = `DESC`. Maps directly to the plan's
    `SortKey.ascending`.
  - `nullsLast` (boolean) — `true` = `NULLS LAST`, `false` = `NULLS FIRST`. Maps directly to the
    plan's `SortKey.nulls_last`.
- `limit` co-occurs as `{"numElements": N}` — same field A1 confirmed `extract_limit` already reads
  (`pushdownRequest.limit.numElements`); no parser change needed for the limit side, only the new
  `orderBy` array.
- The doc separately lists `ORDER_BY_COLUMN` and `ORDER_BY_EXPRESSION` as distinct capabilities
  (confirming the plan's choice to advertise only the former is a real, enforced distinction on
  Exasol's side — advertising only `ORDER_BY_COLUMN` structurally prevents Exasol from ever sending
  an `expression`-typed sort key) and separately lists `LIMIT_WITH_OFFSET` (an optional `offset`
  companion to `limit`, left unadvertised per decision [3] — no offset field name needed).
- The doc does not show an explicit statement of a top-level ORDER BY "backstop" re-application
  when `orderBy`/`limit` ARE pushed (Exasol's own doc frames pushdown as delegating that clause,
  not double-applying it); A1's live evidence that Exasol withholds `limit` entirely when it can't
  ALSO push `orderBy` is the operative backstop-relevant fact for the UNmatched-shape case, and it
  is confirmed from this codebase's own live cluster, not inferred from the doc.

Source: `https://github.com/exasol/virtual-schema-common-java/blob/main/doc/development/api/virtual_schema_api.md` (fetched 2026-07-04).

### A3 — DataFusion folds `ORDER BY <col> LIMIT n` into a bounded TopK; guard-test reconciliation

**Insertion point** (`crates/lakehouse-engine/src/scan/mod.rs`, `build_scan_sql`, current `main`):
the `ORDER BY` clause must be inserted between the WHERE-append block (ends at the line closing `if
let Some(filter) = &spec.filter { ... }`, i.e. right after `sql.push_str(filter);` and its closing
brace) and the LIMIT-append block (`// Append LIMIT clause.` / `if let Some(limit) = spec.limit
{ ... }`). Rendered clause order must be `... FROM (...) WHERE ... ORDER BY <keys> LIMIT n` — SQL
requires ORDER BY before LIMIT, matching the plan's stated intent.

**TopK folding — confirmed from DataFusion 54.0.0 source** (`datafusion-physical-plan-54.0.0/src/sorts/sort.rs`,
pulled from this repo's own cached cargo registry volume, `lakehouse-engine-rs-udf-cargo-registry`):
`SortExec` carries a single `fetch: Option<usize>` field; `build_raw_scan_physical_plan` /
`build_scan_sql` route the built SQL string through the REAL DataFusion pipeline
(`ctx.sql(&sql).await` → `df.create_physical_plan().await`), so DataFusion's own physical
optimizer automatically folds a `Sort` immediately followed by a `Limit` into a fetch-bounded
`SortExec` — no extra crate code is needed beyond emitting `ORDER BY <keys> LIMIT n` in the SQL
text (the existing LIMIT-append code already does the LIMIT half). This IS the bounded TopK path
(bounded memory, not a full materialize-then-sort), confirmed structurally, not just by name.

**Guard-test reconciliation — the literal-substring check needs a different assertion for the new
path, but is NOT broken by this feature today.** `SortExec`'s `DisplayAs` impl
(`impl DisplayAs for SortExec`, same file) renders:
- `fetch: None` (full/global sort) → `"SortExec: expr=[...], preserve_partitioning=[...]"`
- `fetch: Some(n)` (bounded top-K) → `"SortExec: TopK(fetch={n}), expr=[...], ..."`

Both forms contain the literal substring `"SortExec"`. The existing guard test
(`crates/lakehouse-engine/tests/scan_plan_shape.rs::raw_scan_plan_has_no_repartition_stage`)
asserts `!rendered.contains("SortExec")` over `single_partition_spec()`, which has no `order_by`
field today and will default to `None`/absent after B1 adds it — so that spec never emits an
`ORDER BY` clause, the plan never contains a `SortExec` of either form, and **the existing test is
unaffected by this feature** (it exercises a spec shape this plan does not touch).
However, the NEW scenario this plan introduces
(`order_by_spec_emits_bounded_topk_not_global_sort`, per the plan's Scenario Coverage table) MUST
NOT reuse the blanket `!rendered.contains("SortExec")` assertion — that would be a false failure
against the plan's OWN intentional output. B4/B5's new test must instead assert the plan display
contains the bounded form specifically (`rendered.contains("TopK(fetch=")`) AND does not contain
the unbounded form (`!rendered.contains("SortExec: expr=[")`, i.e. no `SortExec` display lacking
`TopK(fetch=`) — that is the correct, non-conflicting way to prove "bounded TopK, never a global
sort" for the order-by-carrying path while leaving the pre-existing no-`order_by` guard test intact
and passing as-is.

## Review Findings

<!-- Populated by speq-implement after code review. -->

### B5 — critical regression discovered live during E2E testing (2026-07-04)

**Finding:** Advertising `ORDER_BY_COLUMN` (B2) breaks two PRE-EXISTING, previously-passing
GROUP BY tests: `test_high_cardinality_group_by_spill` and
`test_high_cardinality_multi_key_group_by_spill` (both in `e2e_scan_test.rs`, both
`... GROUP BY id [, mod_key] ORDER BY id [, mod_key]` with NO `LIMIT`). Confirmed via a live
A/B experiment: temporarily removing `ORDER_BY_COLUMN` from `capabilities.rs`'s `CAPABILITIES`
list, rebuilding the `.so`, and re-running ONLY these two tests made both pass; restoring
`ORDER_BY_COLUMN` and rebuilding reproduces the failure deterministically (ids returned in
scan/shard order, not sorted — e.g. position 0 is `7` instead of `1`).

**Root cause (read from code, not yet fixed):** `build_grouped_aggregate_scan_sql`
(`crates/lakehouse-engine/src/adapter/pushdown.rs`) never renders any `ORDER BY` clause in its
returned SQL — the ordered-top-N feature (B3/B4) is scoped to the pure row-scan path only
(`topn` is computed only when `aggregates.is_none()`, ~line 2149), and the grouped/aggregate
branch's only order-by-awareness is withholding `grouped_limit` when `has_order_by` (decision
[4], line ~2098) — it does nothing about the ordering itself. Before B2, Exasol never sent an
`orderBy` in the pushdown request (no `ORDER_BY_COLUMN` capability), so Exasol unconditionally
applied its own final `ORDER BY` on top of the returned (grouped, unsorted) rows — correct by
accident of the capability being entirely absent. After B2, once `ORDER_BY_COLUMN` is
advertised, Exasol pushes `orderBy` (with no `limit`, since these two tests have none) for
these GROUP BY queries too, and — unlike the pure row-scan "no LIMIT" case, which a separate
live probe confirmed IS safe (Exasol keeps its own top-level sort when only `orderBy` with no
`limit` is delegated) — the grouped/aggregate path's returned SQL apparently is trusted as final
for ordering purposes even without a LIMIT, so no backstop sort happens for GROUP BY queries.
The exact Exasol-side rule that distinguishes the two cases was not further reverse-engineered
(would require another live capture cycle) — flagging as a confirmed, reproducible defect with a
precise repro rather than a fully diagnosed fix.

**A SEPARATE, likely more severe defect was also found (not covered by a committed regression
test, to avoid shipping a known-broken assertion)**: `ORDER BY <unprojected column> LIMIT n`
over the plain row-scan path (e.g. `SELECT id FROM events ORDER BY score DESC LIMIT 5`, where
`score` drives the sort but is not selected) returns COMPLETELY WRONG results today — all 20
rows, unsorted — because `detect_topn` correctly declines the shape (per decision [6], sort keys
must be projected) and withholds the LIMIT (decision [4]), but Exasol does NOT re-apply either
the `ORDER BY` or the `LIMIT` once it has delegated both together in one `orderBy`+`limit`
pushdown request — it fully trusts the returned SQL. This contradicts decision [4]'s documented
assumption ("Exasol re-applies both clauses") and A1's now-STALE verification (A1 was captured
BEFORE `ORDER_BY_COLUMN` was advertised, so it could not have observed this interaction). Live
evidence: `EXPLAIN VIRTUAL` for this query shows Exasol's `pushdownRequest` DOES carry both
`orderBy` (for `SCORE`) and `limit: {numElements: 5}` even though `SCORE` is not in the
`selectList` — contradicting the implicit assumption that Exasol would only push `orderBy` for
a column already being projected.

**Scope decision for B5:** B5's own two new tests (`ordered_topn_pushes_down_matches_single_node`,
`order_by_without_limit_falls_back_correctly`) were written to exercise a MATCHED shape and a
CONFIRMED-SAFE decline shape (ORDER BY with no LIMIT, over a row-scan — verified safe by a live
probe distinct from the GROUP BY case above), so both pass. The task's suggested "ORDER BY over
an unprojected column" decline shape was tried FIRST and is the one that surfaced the
LIMIT-withheld wrong-results defect above; it was not used as the shipped regression test because
it would need to assert an admittedly-wrong result to pass, which is not an acceptable regression
test. Both defects (grouped ORDER BY regression; unprojected-column+LIMIT wrong results) are
correctness gaps in the already-landed B2/B3/B3b code, not in B5's own tests, and need an
[expert] fix — most likely either (a) don't advertise `ORDER_BY_COLUMN` unconditionally, only
when the adapter can guarantee it always resolves correctly for every shape Exasol might send it
alongside, or (b) render a defensive backstop `ORDER BY`/`LIMIT` in BOTH the grouped/aggregate
wrapper and the row-scan wrapper whenever `order_by` is present in the request but not matched by
`detect_topn` / not applicable to the grouped path, so the adapter's own SQL is always correct
independent of Exasol's undocumented reapplication behavior — before this plan can be considered
safe to ship (blocks Phase 5 `make test-e2e` — 0 failures).

**Verification commands used:** `cargo test --features exasol-e2e --test e2e_scan_test --
--test-threads=1` (full file, twice — once per `.so` variant); the A/B experiment additionally
ran only the two named GROUP BY tests per variant. `.so` rebuilt via
`make cross-musl-udf-build` (never on host) both times.

## C1 — Live NQ4 re-run against test1 (2026-07-04, post-Group B)

**Setup:** `.so` was already current for the working tree (`make: Nothing to be done for
'cross-musl-udf-build'` — mtime check confirmed no rebuild needed) with all of Group B's changes
(B1–B6). `./bench/run.sh` (target `remote`, `bench/.env`) re-installed SLC 0.20.1 and
re-uploaded the `.so` to test1's BucketFS unconditionally (its default path, `BENCH_SKIP_UPLOAD`
unset) — no fingerprint mismatch encountered, confirming the SLC is still in lockstep with the
`.so`'s `exasol-udf-sdk` version.

### Pushdown shape (`EXPLAIN VIRTUAL`) — confirmed BOTH per-shard and outer ORDER BY + LIMIT

Live `EXPLAIN VIRTUAL SELECT L_ORDERKEY, L_EXTENDEDPRICE FROM TPCH.LINEITEM ORDER BY
L_EXTENDEDPRICE DESC LIMIT 20` against test1:

- `getCapabilities` now advertises `ORDER_BY_COLUMN` (and no `ORDER_BY_EXPRESSION`/
  `LIMIT_WITH_OFFSET`/join capabilities), matching B2.
- `pushdownRequest` carries both `orderBy` (`L_EXTENDEDPRICE`, `isAscending:false`,
  `nullsLast:false`) and `limit:{numElements:20}` together — the live shape A2 predicted.
- The embedded PER-SHARD common scan spec (inside the `LAKEHOUSE_SCAN(...)` JSON literal) carries
  `"limit":20,"order_by":[{"column":"L_EXTENDEDPRICE","ascending":false,"nulls_last":false}]` —
  every shard runs the identical bounded sort, not a bare unlimited raw scan.
- The OUTER merge SQL renders its own final ordering, self-contained per decision [5]:
  `...GROUP BY shard_key) ORDER BY "L_EXTENDEDPRICE" DESC NULLS FIRST LIMIT 20`.
- This is NOT the old "bare, un-limited fan-out" shape A1 captured pre-feature (no `limit`, no
  `order_by` key at all, no outer `ORDER BY`) — confirms the optimized path is live and active.

### Correctness — IDENTICAL top-20 result set, one benign tie-order difference

Diffed the new run's NQ4 output against the pre-optimization baseline
(`bench/reports/bench-report-20260704-122600.txt`, captured pre-Group-B) row by row:

- All 20 `(L_ORDERKEY, L_EXTENDEDPRICE)` pairs are IDENTICAL as a set (`diff` of both lists
  sorted lexicographically shows zero differences) — same top row
  (`L_ORDERKEY=151324423, L_EXTENDEDPRICE=104949.00`), same full 20-row set.
- Position-by-position diff shows exactly one difference: positions 18/19 (both
  `L_EXTENDEDPRICE=104898.00`, a true tie) have `L_ORDERKEY` 4633346 and 176418949 swapped
  relative to each other. This is NOT a correctness bug: the query's `ORDER BY L_EXTENDEDPRICE
  DESC` has no secondary tiebreak column, so SQL does not guarantee a stable order among
  exactly-tied rows, and the new per-shard-bounded-sort-then-merge execution shape can legitimately
  resolve a tie differently than the old single full sort did while still producing the exact
  same top-20 VALUE set. Confirmed correct.
- New report: `bench/reports/bench-report-20260704-153151.txt`.

### Timing — confirmed real speedup, now also faster than the Trino comparison point

| Run | NQ4 elapsed |
|---|---|
| Pre-optimization baseline (raw unlimited scan, Exasol-side sort) | 12.03 s |
| Post-topn-pushdown (this run, live test1) | **2.13 s** |
| Trino (2-node), captured previously, not re-measured | 4.71 s |

Speedup vs the 12.03 s baseline: **~5.65×** (12.03 / 2.13). The task only required confirming a
real speedup vs the baseline (not beating Trino), but as a bonus the new number (2.13 s) is also
faster than the previously-captured Trino comparison point (4.71 s) — NQ4 flips from lakehouse-
engine-rs's largest competitive loss to a lakehouse-engine-rs win, without touching joins or the
file-sharding architecture (consistent with the plan's non-goals).

### `pushdown_check` added

Added an NQ4 `pushdown_check` to `bench/run.sh` (mirrors the NQ1/NQ2/Q9b convention), asserting
the `EXPLAIN VIRTUAL` output for the NQ4 shape contains `order_by` (the common-spec key) AND
`LIMIT` (the outer merge clause) — i.e. never regresses to the bare 2-column unlimited raw scan.
Verified passing live: `PUSHDOWN: NQ4 top-N (ORDER BY + LIMIT) pushdown` → `OK pushed: order_by`,
`OK pushed: LIMIT`.

**C1 complete.** All of Group C's manual-testing acceptance criteria (plan `§ Verification →
Manual Testing`) are met against the live test1 cluster. Full remaining checklist (Phase 4 code
review; Phase 5 build/test/e2e/lint/format) is unblocked to proceed next.
