# Decision Log: fix-211-decimal-string-trailing-zeros

## Interview

Headless mode — no live interview. The orchestrator supplied a fully-specified intent (issue #211 fix, fresh-verified against the live Docker Exasol stack this session; see `STATUS.md`) that stands in for the interview. Key inputs treated as authoritative:

**Q:** What is the bug and where does it manifest?
**A:** Exasol trims trailing scale zeros in DECIMAL→string conversion; the pushed-down DataFusion path does not. Confirmed live for explicit `CAST(c_decimal_a AS VARCHAR)`, implicit `CONCAT`/`||`, and implicit `LENGTH` over a DECIMAL column — the last drives a silent aggregate COUNT divergence.

**Q:** Where should the fix live?
**A:** Follow the #207 precedent exactly — type detection in the adapter (`support.rs`, using `extract_all_column_types`), formatting primitive in `vs-expression` with no type awareness.

**Q:** What reusable primitive is required?
**A:** A single crate-visible, unit-testable `format_decimal_exasol_style` in `vs-expression`, next to `render_cast_target`, that issue #210 can import. Verified expression: `regexp_replace(regexp_replace(CAST(<expr> AS VARCHAR), '(\.[0-9]*[1-9])0+$', '\1'), '\.0+$', '')` (DataFusion 54 POSIX backreferences).

**Q:** What scope?
**A:** Explicit select-list `CAST(<DECIMAL column> AS VARCHAR/CHAR)` and select-list `CONCAT`/`LENGTH` over a bare DECIMAL column — the only two confirmed to silently coerce. WHERE/HAVING/GROUP-BY, computed-expression arguments, and every other string function are out of scope; record as a named tracked exception.

**Q:** What about the emit boundary?
**A:** Verify `scan/emit.rs` coerces the `Utf8View` produced by `CAST(... AS VARCHAR)` for a projected expression column; make it part of this fix if it does not.

## Design Decisions

### [1] Type-aware trim decision lives in the adapter; vs-expression stays type-blind

- **Decision:** The adapter (`project_columns` guard) resolves column types and decides where to inject the trim; `vs-expression` gains only a pure primitive and a synthetic node it renders without inspecting types.
- **Alternatives:** Add column-type awareness inside `vs-expression`'s CAST/string-function arms — rejected: no column-type context on the wire, and the crate is stateless and sibling-shared.
- **Rationale:** Directly applies the accepted #207 ADR `like-guard-in-adapter-not-vs-expression`, which named #211 as its deferred follow-up. Extends that precedent to the projection path; supersedes nothing.
- **Promotes to ADR:** yes

### [2] Inject an adapter-synthesized `decimal_to_varchar_exasol` node

- **Decision:** The adapter rewrites a bare-DECIMAL-column stringification point into a one-argument `decimal_to_varchar_exasol` JSON node; `vs-expression` renders it by rendering the argument then applying `format_decimal_exasol_style`.
- **Alternatives:** (a) Post-process the rendered SQL string to find and wrap the column — rejected as fragile and unable to target a nested `CONCAT` argument. (b) A generic raw-SQL passthrough node — rejected as a broader, less self-documenting surface.
- **Rationale:** A synthetic node keeps nesting correct inside `CONCAT`, mirrors #207's `function_scalar_cast` injection for DATE, and is reused verbatim by #210 (which wraps DECIMAL arguments for its own string functions) with zero new `vs-expression` code.
- **Promotes to ADR:** yes

### [3] `regexp_replace`-based trim expression

- **Decision:** Reproduce Exasol formatting with two chained `regexp_replace` calls over `CAST(<expr> AS VARCHAR)`.
- **Alternatives:** Re-format Arrow-side at the emit boundary; add a bespoke DataFusion UDF.
- **Rationale:** The stringified value may be consumed inside the scan (e.g. `LENGTH` in a filter), so emit-side reformatting cannot fix it in general; `regexp_replace` needs no new UDF and was verified against DataFusion 54 for every repro and edge case.
- **Promotes to ADR:** no

### [4] Scope to single-table select-list AND WHERE-clause bare-DECIMAL-column CAST/CONCAT/LENGTH; defer the rest to #223

- **Decision:** Fix the single-table select-list projection AND the single-table WHERE-clause filter for a bare DECIMAL column, using one shared recursive rewriter. This directly fixes issue #211's headline `LENGTH(c_acctbal)>5` COUNT-divergence repro (a WHERE filter), earning a true `Closes #211`. Deferred to #223: a computed-expression (non-bare-column) argument, the broadcast-join per-leg filter path, and a GROUP-BY key absent from the select list. (Superseded the initial round-1 scope, which fixed the select-list path only — see Review Findings [1].)
- **Alternatives:** Select-list only, deferring WHERE — rejected: it would leave issue #211's only real-data-quantified symptom live while claiming `Closes #211`. Fix computed args and the join per-leg filter now too — rejected: a computed argument's result type is not carried on the wire, and the join per-leg filter is a separate render surface (the same single-table-vs-join split #207/#215 drew). Fold into #210 — rejected: #210 is a disjoint hard-fail failure mode, not silent-wrong formatting.
- **Rationale:** The WHERE path is exactly one production filter site (`handle_pushdown`, mod.rs:188, where `like_subject_type_guard` is already composed) reusing the same rewriter and node — bounded and low-risk. The residual slices follow the repo's tracked-exception convention (cited issue #223, never a silent gap) and the #215/#219 precedent.
- **Promotes to ADR:** no

### [7] Unify the projection and filter rewrite in one recursive tree walk

- **Decision:** A single `rewrite_decimal_stringifications(node, col_types)` recurses through any expression/predicate tree, wrapping a bare DECIMAL column only where it is directly stringified by CAST/CONCAT/LENGTH; both `project_columns` (per select-list item) and the filter chain call it.
- **Alternatives:** Two separate guards (a select-list guard and a filter guard) — rejected: they would duplicate the nested-CONCAT descent and the stringifier dispatch, doubling the correctness surface.
- **Rationale:** Nested `CONCAT` (`a||b||c` → `CONCAT(a,CONCAT(b,c))`) and the WHERE path both reduce to "descend the tree, rewrite at stringifier nodes only"; one walk covers both and both blocker fixes.
- **Promotes to ADR:** no

### [5] Leave `scan/emit.rs` unchanged

- **Decision:** No emit-boundary code change. `target_arrow_type` already coerces `Utf8View` → `Utf8` for a VARCHAR-declared column (`emit.rs:176,182`), which covers the `Utf8View` that `CAST(... AS VARCHAR)` produces under DataFusion 54; only a focused regression test is added.
- **Alternatives:** Add new coercion handling — rejected: verified already present.
- **Rationale:** The intent flagged this as a verification item; verification shows the path is covered, so the fix stays additive.
- **Promotes to ADR:** no

### [6] Leave `pushdown-planning-like-type-coercion` cross-reference unchanged

- **Decision:** Do not edit the LIKE feature's "tracked in issue #211" note.
- **Alternatives:** Update it to point the residual LIKE-over-decimal filter case at #223.
- **Rationale:** The LIKE-over-decimal filter case remains genuinely declined and unfixed by this plan (it is a filter path), so the existing umbrella reference stays accurate; editing it is out-of-scope churn.
- **Promotes to ADR:** no

## Review Findings

### [1] [plan-review] Nested CONCAT is not reached by a top-node-only guard

- **Finding:** [COMPLETENESS_GAP] `a||b||c` renders as nested `CONCAT(a, CONCAT(b, c))` (confirmed live in STATUS.md for `id||'-'||c_decimal_a`), so the DECIMAL column is a direct argument only of the INNER CONCAT. A guard inspecting only the top CONCAT node's direct arguments never reaches it.
- **Direction change:** Replaced the flat select-list guard with a recursive rewriter (`rewrite_decimal_stringifications`, decision [7]) that descends through nested CONCAT (and the CAST/LENGTH argument position), wrapping a bare DECIMAL column wherever it is directly stringified, and explicitly NOT wrapping a DECIMAL column in a non-stringifying context (arithmetic, comparison). Task 3 and the CONCAT scenario now state this recursion and its boundary; a dedicated non-stringifying-context scenario and test were added.
- **Promotes to ADR:** no

### [2] [plan-review] project_columns dispatch drops the rewritten CAST node into the full-row fallback

- **Finding:** [HIDDEN_DEPENDENCY] The explicit-CAST rewrite replaces the whole `function_scalar_cast` node with a top-level `decimal_to_varchar_exasol` node, but `project_columns`'s `item_type` match (support.rs ~663-673) does not list that type, so the rewritten item falls into the `_ =>` arm and triggers `needs_full_fallback` — the trim node never reaches rendering.
- **Direction change:** Task 4 now explicitly adds `decimal_to_varchar_exasol` to the recognized-scalar `item_type` match arm so a rewritten item routes through `render_expression_safe`. The explicit-CAST scenario gained an *AND* clause asserting this routing (not the full-row fallback), and its test (`selectlist_decimal_cast_rewritten_and_routed`) asserts it.
- **Promotes to ADR:** no

### [3] [plan-review] "Closes #211" while the headline WHERE-clause symptom stays broken

- **Finding:** [SCOPE_REDUCTION] The round-1 plan claimed `Closes #211` while deferring issue #211's only real-data-quantified symptom — the WHERE-clause `LENGTH(c_acctbal)>5` COUNT divergence (729176 vs 742505) — to #223. The deferral mechanism was convention-compliant, but closing #211 with its headline repro live is not acceptable.
- **Direction change:** Took the scope-extension path (the orchestrator's preferred option). Investigation confirmed the single-table WHERE filter renders at exactly one production site (`handle_pushdown`, mod.rs:188; mod.rs:707 and topn.rs:204 are inside `#[cfg(test)]` modules; grouped_agg.rs has no `render_df_filter_safe` call), where `like_subject_type_guard` is already composed. Task 5 wires the same recursive rewriter there, on the DataFusion filter tree only (Iceberg-pruning tree untouched), reusing the node and primitive with no new `vs-expression` code — a bounded, low-risk extension. The plan keeps `Closes #211`. Issue #223 was re-scoped and its title/body updated to the residual slices (computed-expression arguments, broadcast-join per-leg filter, GROUP-BY-only keys); the join SELECT-list is structurally covered because it reuses `project_columns`. Added filter-path scenarios (WHERE trim, non-stringifying filter context), tests, and an E2E assertion of the headline COUNT shape.
- **Promotes to ADR:** no

### [4] [plan-review] Advisory findings folded in

- **Finding:** Four non-blocking advisories: (1) add NULL / scale-0 integer cases to the `format_decimal_exasol_style` test; (2) the "join benefits automatically" claim was untested; (3) `[expert]` over-tagged Tasks 1-2; (4) Task 5 was listed as a true parallel peer.
- **Direction change:** (1) Task 1 and the format scenario now cover NULL→NULL, scale-0 integer (`100`, `-7`), and negative-with-trailing-zero. (2) The adapter-spec Background now states the join SELECT-list is structurally covered by the shared `project_columns` but this plan adds no join-specific test, and the join per-leg FILTER path is out of scope (#223). (3) `[expert]` dropped from Tasks 1-2 (now the primitive and node arm); it remains on Tasks 3-5 (the recursive rewriter and its two wirings), where the real correctness complexity lives. (4) The rewrite wirings (Tasks 4-5) form Group D after the rewriter (Group C); tests (Task 6) follow in Group E, sequential — not parallel peers of the rewriter.
- **Promotes to ADR:** no
