# Decision Log: add-broadcast-join-topn

## Interview

**Q (Offset scope):** The flat-scan TopN pattern (`vs-adapter/pushdown-planning-topn`) declines its per-shard bounded path whenever OFFSET is non-zero, because a per-shard offset does not compose. Should the broadcast-join per-shard TopK follow that precedent, or also attempt the non-zero-offset case now?
**A:** Zero-offset only (Recommended). A non-zero-offset ORDER BY+LIMIT broadcast join keeps its current behavior unchanged: every shard computes its full local join, and the outer `wrap_declined_order_by` wrapper applies ORDER BY/LIMIT/OFFSET over the merged result. Per-shard top-(limit+offset) truncation is out of scope for this plan.

**Q (Merge rendering):** The Ordered broadcast-join path wraps the fan-out in an outer SELECT (`wrap_declined_order_by`) that applies the global ORDER BY/LIMIT/OFFSET over the merged rows. Once each shard runs its own TopK, should that wrapper stay as it is, or should the zero-offset case attach ORDER BY/LIMIT directly onto the fan-out select, as the flat-scan bounded top-N path does?
**A:** Keep the existing wrapper unchanged (Recommended). Only the per-shard TopK inside the JoinSpec/UDF is added. The wrapper keeps doing the final global sort and limit, now over at most G × limit merged rows. The merge-rendering branch of `build_broadcast_join_sql` is not reworked, and the wrapper's rendering, tests, and spec scenario stay as they are.

## Design Decisions

### [1] The per-shard ordering rides in the join block as `post_join_order_by: Vec<SortKey>`

- **Decision:** Add `JoinSpec::post_join_order_by: Vec<SortKey>` beside `post_join_limit`, with `#[serde(default, skip_serializing_if = "Vec::is_empty")]`. The adapter sets it only together with `post_join_limit`.
- **Alternatives:** Reuse `CommonScanSpec::order_by`: rejected. That field orders one side's scan before any join, and the fallback leg builder shares the fan-out helper, so the field would carry two meanings. A new join-specific sort-key type: rejected. `SortKey` already models a column, a direction, and a NULL placement, and it is format-neutral.
- **Rationale:** This is the ordering counterpart of ADR `join-post-limit-lives-in-join-block`, and a corollary of it, so it gets no ADR of its own. The same type-level separation applies: a spec without a join block has no field that can carry a post-join ordering.
- **Consequences:** Every `JoinSpec { .. }` struct literal gains the field: one production site and 14 test sites. A spec serialized without the key deserializes with an empty ordering. A plan that sets no ordering keeps its encoding.
- **Promotes to ADR:** no

### [2] A join shard ranks each sort key by the value it emits, and the join scan owns that rule

- **Decision:** A per-shard sort whose output a merge re-ranks ranks the value the merge sees, and the module that renders the emitted value owns that rule. For the join scan, `build_join_sql` renders each post-join sort key over the key column's emitted expression, the same `render_join_select_item` output the select list uses, wrapped in `nullif(<expr>, '')` for a string key and in `CASE WHEN isnan(<expr>) THEN NULL ELSE <expr> END` for a `Float32`/`Float64` key. Direction and NULL placement render through `SortKey::render_ordered`, the shared direction/NULL-placement seam.
- **Alternatives:**
  - Render the keys with `render_order_by_clause`, as the raw scan does: rejected. That ranks the native value. The Exasol-side wrapper ranks the emitted value: text for a column that the JSON rendering or the `CAST(... AS VARCHAR)` fallback covers, and NULL for an emitted empty string. Either divergence lets a shard's cut drop a row that the global top-N selects, which returns wrong rows with no error.
  - A plan-time guard that withholds the per-shard bound for a JSON-fallback key, as `detect_topn` does: rejected. `arrow_type_to_tag` maps nested and binary types to `utf8`, so the adapter cannot see which keys the scan renders as JSON. The guard also leaves the empty-string divergence open.
  - Withhold the per-shard bound for every string key: rejected. A string column is a common sort key, and the rule in the Decision serves it correctly.
- **Rationale:** The distributed top-N argument holds only when each shard's order refines the wrapper's order over the emitted rows. Only the join scan knows how each column is emitted: it owns `render_join_select_item` and sees the registered Arrow types. Exasol's VARCHAR domain has no empty string (`'' IS NULL` is TRUE, captured live, `sql-comprehension/vs-expression-translator-concat`). DataFusion ranks a native `NaN` as a number, while the `emit_batch` path emits it as a silent NULL (`sql-comprehension/vs-expression-translator-float-div`, issue #246).
- **Consequences:**
  - The rule assumes that DataFusion's comparison of two strings agrees with Exasol's VARCHAR comparison. The flat-scan top-N path makes the same assumption. This plan does not verify it.
  - If a fix of `#246` changes the emitted value, this rule changes with it.
  - Every future per-shard sort follows this rule, including a fix of the flat-scan path (decision [6]).
- **Promotes to ADR:** yes

### [3] The per-shard bound applies only to a zero offset with a LIMIT

- **Decision:** An `Ordered` window with `limit = Some(n)` and `offset = 0` carries the per-shard ordering and cap. An `Ordered` window with a non-zero offset, or with no limit, carries neither. Its SQL is unchanged.
- **Alternatives:** A per-shard top-(n + m) bound for a non-zero offset `m`: the same distributed top-N argument makes it correct, but the interview scoped it out. A per-shard sort with no cap: rejected, because it bounds nothing and the wrapper re-sorts every row.
- **Rationale:** Interview answer "Offset scope". It matches `detect_topn`, which bounds shards only for a zero offset.
- **Promotes to ADR:** no

### [4] The wrapper is unchanged, and an ordered fan-out carries no merge LIMIT

- **Decision:** `build_broadcast_join_sql` separates the join-block bounds from the fan-out merge limit. The merge limit it passes to `build_scan_driving_sql` is `Some(n)` only for `BareLimit(n)` and stays `None` for every `Ordered` window. The wrapper stays the only global `ORDER BY` and window.
- **Alternatives:** Attach the merge `ORDER BY … LIMIT n` directly onto the fan-out: rejected in the interview. Reuse one cap value for both the join block and the fan-out limit, as the `BareLimit` arm does: rejected. For an ordered window it renders `LIMIT n` on the fan-out, which cuts the merged rows to an arbitrary `n` before the wrapper sorts them.
- **Rationale:** Interview answer "Merge rendering", plus the correctness of the merge.
- **Promotes to ADR:** no

### [5] No Iceberg or Delta normative section governs query-result ordering

- **Decision:** The spec deltas carry no Iceberg or Delta citation. This entry records the compliance check that CLAUDE.md requires.
- **Alternatives:** none
- **Rationale:** Iceberg table spec, § Sorting: "Users can sort their data within partitions by columns to gain performance. The information on how the data is sorted can be declared per data or delete file, by a **sort order**." The same section states: "Writers should use this default sort order to sort the data on write, but are not required to if the default order is prohibitively expensive, as it would be for streaming writes." A sort order is therefore writer-side file metadata. Delta protocol, § Clustered Table: "The Clustered Table feature facilitates the physical clustering of rows that share similar values on a predefined set of clustering columns." Its only requirements section is § Writer Requirements for Clustered Table. Neither document states a reader requirement on the order of returned rows. The per-shard top-N reads no sort order id, manifest, snapshot, field id, or log action. It sorts joined rows that the scan already read, and it behaves identically on Iceberg and Delta sides.
- **Promotes to ADR:** no

### [6] The flat-scan top-N path shares the ranking divergence and stays out of scope

- **Decision:** This plan does not change the raw scan's per-shard `ORDER BY`. The planner reports the finding to the orchestrator and recommends a GitHub issue for it.
- **Alternatives:** Fix the raw scan in this plan: rejected. It is a separate feature (`vs-adapter/pushdown-planning-topn`, `datafusion-scan/scan-execution-plan-shape`), and CLAUDE.md requires a bug to be reproduced live before it is fixed.
- **Rationale:** `build_scan_sql` (`crates/lakehouse-engine/src/scan/raw_scan.rs`) renders its per-shard `ORDER BY` with `render_order_by_clause` over the bare column. By inference from the code, not reproduced live, both divergences of decision [2] apply there. An empty-string key diverges under `ASC NULLS LAST` and `DESC NULLS FIRST`. A nested-type key diverges because the tag collapse hides it from `detect_topn`'s JSON-fallback guard.
- **Promotes to ADR:** no

### [7] Touched Background sections state current behavior only

- **Decision:** The two `DELTA:CHANGED` Background blocks drop the "this delta", "superseded", and "withdrawn" narrative and keep one current-state bullet per rule. They keep the evidence verbatim: DataFusion source line pins, Iceberg normative quotes, and tracked issues `#294`, `#303`, `#304`, and `#307`. `vs-adapter/pushdown-planning-join-fallback` gets no delta. Its Background cites `(#309)` beside a statement that stays true ("the ordering as an outer wrapper over the broadcast fan-out"), and a delta would have to reproduce its 117-line Background to change one parenthetical.
- **Alternatives:** Reproduce the old Background bullets verbatim: rejected by the standing spec-conciseness review preference.
- **Rationale:** A reader who opens only the permanent spec needs the current rules, not the history of the plans that produced them.
- **Promotes to ADR:** no

### [8] Rows tied at the shard cut need no special handling

- **Decision:** Neither the shard's TopK nor the wrapper breaks ties. A shard keeps any subset of its rows tied at its local `n`-th key. The result requirement is the single-node sort-key sequence, with every returned row a row of the join.
- **Alternatives:** Append a tiebreaker key, such as a fact-row identity, to every shard's ordering: rejected. It changes no sort-key value in the result, and a single-node `ORDER BY … LIMIT n` also picks among tied rows arbitrarily. Withhold the per-shard bound when a key can tie: rejected. Any key without a uniqueness guarantee can tie, so this withholds the bound from nearly every query.
- **Rationale:** This answers the brief's question whether a DataFusion TopK needs special handling for ties at the shard boundary. Let `k` be the global `n`-th key, and let `a` count the rows ranked strictly ahead of it. Every row ahead of `k` survives its shard's cut. Each shard keeps either all of its rows tied at `k` or at least `n − a` of them. The merged rows therefore contain a valid single-node answer, which the wrapper selects.
- **Consequences:** The planner scenario requires the single-node sort-key sequence, and the scan scenario requires that no omitted joined row outrank an emitted row. Task 2.8 asserts a tie that straddles two fact files.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] A stored NaN sort key ranks differently on the shard and in Exasol

- **Finding:** The ranking rule adjusted only string keys. The `emit_batch` path emits a stored `NaN` as NULL (`#246`), and DataFusion ranks a native `NaN` as a number. A shard's cut could then drop rows that the wrapper's global top-N selects, and decision [2] called this an untested area.
- **Direction change:** A `Float32` or `Float64` key ranks `CASE WHEN isnan(<expr>) THEN NULL ELSE <expr> END`. The rule sits in the `datafusion-scan/scan-execution-join` Background ranking bullet, a new AND step of its emitted-value scenario, `plan.md` § Architecture, task 1.4, and decision [2]. Task 1.5 and § Scenario Coverage add `build_join_sql_ranks_a_nan_float_key_as_null`.
- **Promotes to ADR:** no

### [plan-review] Ties at the shard cut were unaddressed, and the result requirement was untestable under ties

- **Finding:** No artifact answered whether TopK needs tie handling. The Background proof and two THEN steps assumed a unique global top-n, so they had no pass/fail reading when rows tie at the cut.
- **Direction change:** Decision [8] answers the question: no tie handling. The `vs-adapter/pushdown-planning-join` Background proof covers ties. Its zero-offset scenario requires the single-node sort-key sequence, and the `datafusion-scan/scan-execution-join` THEN requires that no omitted joined row outrank an emitted row. Task 2.8 adds a tie case that straddles two fact files.
- **Promotes to ADR:** no
