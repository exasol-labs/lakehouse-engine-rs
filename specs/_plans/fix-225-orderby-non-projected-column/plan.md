# Plan: fix-225-orderby-non-projected-column

## Summary

Make `ORDER BY <col>` push down correctly when `<col>` is not a bare select-list item, by
appending the missing sort-key column to the per-shard scan as a HIDDEN extra EMITS column
and having the declined-`ORDER BY` wrapper name only the ORIGINAL select-list items instead
of `SELECT *` (issue #225, same root cause as issue #189). This replaces the current
full-base-row widening, which trades one fatal error for another: `sqlCode 04000 "Expected
number of columns is 1 but pushdown query has 10"`.

## Context

`crates/lakehouse-engine/src/adapter/pushdown/mod.rs::build_dispatch_sql` has two coupled
pieces that together produce the bug:

1. **The "Declined-ORDER-BY projection guard (issue #190)" block** (lines 511-537). When any
   pushed sort key is not a `ProjectionItem::Column` in `proj_cols`, it REPLACES
   `proj_cols`/`proj_types` with the FULL BASE ROW — every column in `col_types`.
2. **The row-scan DECLINE wrapper** (lines 630-646; the `declined_order_by` boolean it
   branches on is computed at lines 627-629). It wraps the fan-out as
   `SELECT * FROM ({sql}) ORDER BY <keys> [LIMIT n]`, guarded by `if keys.is_empty() { sql }`
   because `render_order_by_clause(&[])` returns an empty string (`scan/spec.rs:194-199`,
   whose doc states "callers must guard on that before emitting a bare `ORDER BY`").

Combined, a query whose select list has 1 item returns 10 columns (the base row of
`typed_distinct_probe`). Exasol validates a returned pushdown query's column count
POSITIONALLY against the original `selectList` and never re-projects a declined pushdown,
so it rejects the query outright. The same file documents this rule repeatedly elsewhere —
see the `RequestShape::GroupByWrapper` arm's comment ("Exasol expects the pushdown query to
return exactly the selectList columns... a raw full-row scan... SQL state 04000") and the
`SingleGroupAgg` distinct-decline comment.

Both failure modes for this shape are verified live against the local Exasol + MinIO +
Iceberg Docker stack via `scripts/capture-pushdown-payload.sh` / `e2e_capture_pushdown`:

| Query | Failure |
|---|---|
| `SELECT c_varchar FROM typed_distinct_probe WHERE id=1 ORDER BY id` | `sqlCode 04000: Expected number of columns is 1 but pushdown query has 10` |
| `SELECT id\|\|'-'\|\|c_decimal_a FROM typed_distinct_probe WHERE id<=3 ORDER BY id` | `sqlCode 04000: Expected number of columns is 1 but pushdown query has 10` |

Before the #190 guard existed, the same shape failed the other way — `object <COL> not
found`, the wrapper's outer `ORDER BY` referencing a column the narrowed scan never emitted.
That is exactly the error text in issue #189 (`object C_CUSTKEY not found`) and in #225's own
report (`object ID not found`). One root cause, two error texts depending on whether the
widening guard is present. Issue #189's own suggested fix names the same mechanism this plan
adopts — "Include ORDER-BY-referenced columns in the scan projection" — though this plan does
NOT rely on #189's accompanying premise that Exasol drops the extra columns from the final
output; it drops them explicitly itself, via the wrapper's named column list.

### Two projection widenings, only one of which this plan touches

`extract_projection` / `project_columns` (`support.rs:640-752`) already has its OWN
full-base-row fallback, via `needs_full_fallback`: an untranslatable select-list item, an
unknown or aggregate node, or a declared EMITS type Exasol rejects makes it return
`full_row()` instead of one item per select-list item. That fallback runs BEFORE
`build_dispatch_sql` and is mandated by a recorded sibling scenario ("Projected constant
whose declared EMITS type Exasol rejects declines to the full base row").

So `proj_cols.len()` is the length of the adapter's DERIVED PROJECTION, which is NOT
reliably the select-list arity. For `SELECT <untranslatable expr> FROM t ORDER BY id` the
derived projection is already the full base row before this fix's code runs, the sort-key
extension is inert (`id` is present in that widened set), and the wrapper renders every base
column where Exasol expects one — still `04000`. That composed shape is a PRE-EXISTING gap
this fix neither causes nor repairs; every rule in this plan is therefore stated against the
derived projection, never against the raw select list. Task 4.2 files it as a tracked
exception rather than leaving it silent.

- **Goals** — a pushed-down `SELECT <items> FROM t ORDER BY <col>` where `<col>` is absent
  from the adapter's derived projection returns the correct rows, in the correct order, with
  that derived projection's exact column count and order.
- **Non-Goals** —
  - Making these shapes ELIGIBLE for the bounded top-N optimization. The matched-top-N
    rendering path emits `proj_cols` directly as the final visible EMITS with no wrapping
    SELECT; extending it to hide columns is a separate, riskier change and is not needed to
    fix this bug. `detect_topn` must keep seeing only the pre-extension `proj_cols`.
  - Bounding the shapes that change paths. Reordering the extension past `detect_topn` means
    a shape whose sort key is outside the derived projection now DECLINES where it previously
    matched a bounded top-N over the widened projection. Nothing regresses: that widened
    match returned every base column where Exasol expected the select list's, i.e. it also
    failed `04000` — it was buggy-but-matching, never working. The trade is a correct answer
    for an unbounded per-shard scan on those shapes. A bounded variant (per-shard sort over
    visible + hidden columns, hidden columns stripped by an outer select) is possible future
    work; task 4.2's issue notes it.
  - The pre-existing declined-path ordering gap for a JSON-fallback-typed sort key: on the
    declined path the outer `ORDER BY` binds against the EMITTED column, which for a
    fallback-typed column (reachable today only as an out-of-range `decimal128(p>36,s)`) is
    a `CAST(col AS VARCHAR)` JSON string, so it orders lexicographically rather than
    natively. That is true of today's full-base-row widening too and is unchanged here.
    Task 4.1 files a tracked issue and substitutes its number for the `(#TBD-JSONSORT)`
    citation the spec delta already carries, so the SHALL is never unconditionally false.
  - The composed pre-existing arity gap described above: a select-list item that trips
    `extract_projection`'s OWN full-base-row fallback, combined with an `ORDER BY` on a
    column outside even that widened set. Untouched by this fix and still `04000`; task 4.2
    files it and substitutes its number for the spec delta's `(#TBD-FULLROWARITY)` citation.
  - Grouped / aggregate / join ORDER BY paths. `build_grouped_order_by_clause` already
    resolves grouped sort keys against grouped output columns and hard-errors on an
    unresolvable one; joins are routed before `build_dispatch_sql`.
- **Iceberg spec compliance** — NOT APPLICABLE, explicitly rather than silently skipped.
  Per `CLAUDE.md`, any plan touching scanning, pushdown, or schema/type handling must be
  checked against the Apache Iceberg table spec. This change is pure Exasol-SQL-shape
  plumbing in the adapter's pushdown-response construction: no Iceberg file resolution,
  snapshot handling, field-id projection, delete application, or type mapping is touched.
  The appended hidden column reuses the already-resolved `involvedTables[0].columns` type
  map. No Iceberg-spec section governs it and there is no deviation to record.

## Design

### Decision

Split the two responsibilities the current guard conflates. The SCAN's emitted-column set
and the QUERY's visible column set are different sets; make that explicit instead of forcing
them equal by widening.

1. **Extend, don't widen.** Append only the unprojected sort-key columns, resolved by name
   from `col_types`, AFTER the original `proj_cols`/`proj_types`. Original positions and
   therefore original `emits_ident` values are untouched.
2. **Name the visible columns explicitly.** The declined wrapper becomes
   `SELECT <emits_ident(item, i) for i in 0..visible_count> FROM (<fan-out>) ORDER BY <keys>
   [LIMIT n]`. `visible_count` is `proj_cols.len()` captured BEFORE the extension — the
   DERIVED projection's length, not the select-list arity (see "Two projection widenings"
   above). Both existing guards survive: no keys → return the SQL unwrapped;
   `visible_count == 0` → keep `SELECT *`.
3. **Reorder the guard past `detect_topn`.** The extension runs only on the declined path,
   after top-N detection, so `detect_topn` sees the pre-extension projection exactly as
   today. It must still run BEFORE `spec_template` is built (`mod.rs:565`), so the common
   blob's `projection` / `emit_exa_types` and the EMITS clause all carry the appended column.

#### Architecture

```
build_dispatch_sql (row-scan path)
  ├─ detect_topn(request, pushdown_req, &proj_cols /* ORIGINAL */, &logical_schema)
  │     └─ Some(keys) → matched top-N: unchanged, no extension, no wrapper
  └─ None → declined ORDER BY
        ├─ visible_count = proj_cols.len()
        ├─ topn::extend_projection_with_sort_keys(&mut proj_cols, &mut proj_types,
        │                                          &keys, &col_types)
        │     appends  ProjectionItem::Column(<key>)  +  its Exasol type
        ├─ build_scan_driving_sql(...)  → EMITS ("SCORE" …, "ID" …)   ← ID is hidden
        └─ topn::wrap_declined_order_by(&sql, &proj_cols, visible_count, &keys, limit)
              → SELECT "SCORE" FROM (…) ORDER BY "ID" ASC NULLS LAST
```

Worked example for `SELECT id || '-' || name FROM EVENTS WHERE id <= 3 ORDER BY id`:

```
SELECT "_LH_PROJ_0"
FROM (SELECT LHVS.LAKEHOUSE_SCAN('{… "projection":[{"expr":"…"},"ID"] …}', files)
      EMITS ("_LH_PROJ_0" VARCHAR(2000000), "ID" DECIMAL(20,0)) FROM (…))
ORDER BY "ID" ASC NULLS LAST
```

One visible column, matching the one-item select list; `"ID"` reachable by the outer
`ORDER BY` and dropped from the result.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Append-only projection extension | `topn::extend_projection_with_sort_keys` | Appending preserves every original index, so `emits_ident`'s positional `_LH_PROJ_{index}` and `raw_scan`'s matching `AS _LH_PROJ_{i}` alias stay aligned by construction |
| Explicit visible select list via the shared `emits_ident` seam | `topn::wrap_declined_order_by` | Inner EMITS and outer select list render through ONE identifier function, so they cannot drift |
| Dedupe by resolved column name | `extend_projection_with_sort_keys` | A column named by two sort keys, or already bare-projected, must be appended at most once — a repeated EMITS identifier is a duplicate-column error |
| Single source of truth for the wrapper | `topn.rs`, called by the dispatcher AND by `topn.rs`'s own `plan_scan_sql` test helper | That helper currently DUPLICATES the wrapping logic (`topn.rs:256-275`); a second copy would silently drift from the fixed dispatcher |
| Home the new helpers in `topn.rs` | not `support.rs` | `topn.rs` already owns `parse_order_by_keys` / `detect_topn` / the ORDER BY concern; `support.rs` is already 3474 lines of cross-cutting helpers |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Hidden extra EMITS columns + explicit outer select list | Keep widening but add an outer explicit select list only | The explicit list alone fixes arity but still scans and transports every base column for a 1-column query — a large, needless per-shard payload and network cost |
| Hidden extra EMITS columns + explicit outer select list | Decline the pushdown entirely (return `Err`) for this shape | A hard decline is a user-visible failure for a common shape (`SELECT a FROM t ORDER BY b`); the wrapper is correct AND succeeds |
| Hidden extra EMITS columns + explicit outer select list | Drop the pushed `orderBy` and let Exasol sort | Exasol does NOT re-apply a delegated `orderBy` once `ORDER_BY_COLUMN` is advertised — verified live and documented in `e2e_scan_test.rs::order_by_without_limit_falls_back_correctly` — so this returns silently unordered rows |
| Extension AFTER `detect_topn` | Keep it before, as the #190 guard does | Running before would let a widened/extended projection make an ineligible shape match the bounded top-N, whose rendering path emits `proj_cols` as the FINAL visible EMITS with no wrapper — the hidden column would leak into the result and break arity again |
| Unresolvable sort key: skip the append, still render the `ORDER BY` | Hard-error; or drop the wrapper | Unreachable in practice (`col_types` is the full table column list and every pushed sort key is a real column); preserving today's shape adds no new machinery for a case that cannot occur |
| `visible_count == 0` keeps `SELECT *` | Always emit the explicit list | `SELECT  FROM (…)` is not valid SQL; an empty row-scan projection is itself already impossible, so this is a one-line structural guard, not a new code path |
| Empty parsed sort keys returns the SQL unwrapped | Render the wrapper anyway | `render_order_by_clause(&[])` yields `""`, so wrapping would emit a bare `ORDER BY ` — invalid SQL. This guard EXISTS today (`mod.rs:632-633`) and must be carried into the extracted helper, not dropped; a non-empty `orderBy` can still parse to zero keys because `parse_sort_key_element` filters non-column elements and elements missing `isAscending`/`nullsLast` |
| Rules stated against the DERIVED projection | Stated against the select list | `extract_projection`'s own `needs_full_fallback` widening can already make the two differ; a select-list-arity claim would be unconditionally false for that pre-existing shape |
| Helpers in `topn.rs` | `support.rs`; or inline in `mod.rs` | Cohesion with the existing ORDER BY submodule; inline duplication in `mod.rs` is what created the stale test mirror |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning-capability-extensions | CHANGED | `vs-adapter/pushdown-planning-capability-extensions/spec.md` |
| vs-adapter/pushdown-planning-topn | CHANGED | `vs-adapter/pushdown-planning-topn/spec.md` |

The `pushdown-planning-topn` delta is a WORDING correction only — it fixes the decline
scenario's stale "relying on Exasol to apply the ordering it retains" clause (Exasol does
not), pins the top-N eligibility input to the pre-extension derived projection, and records
the path-change trade-off in its Background. No change to the matched top-N code path is
planned or permitted by this plan.

`vs-adapter/pushdown-module-structure` needs NO delta: its scenarios govern the façade,
byte-identical behavior across the refactor, and per-submodule test co-location. Adding two
`pub(super)` helpers to `topn.rs` and having its test module call them instead of duplicating
the logic is consistent with those scenarios, not a change to them.

## Implementation Tasks

1. **Core fix — hidden sort-key columns and an explicit wrapper select list**
   1. Add two `pub(super)` helpers to `crates/lakehouse-engine/src/adapter/pushdown/topn.rs`:
      `extend_projection_with_sort_keys(&mut Vec<ProjectionItem>, &mut Vec<String>, &[SortKey],
      &[(String, String)])`, which appends each sort-key column absent from the projection as a
      `ProjectionItem::Column` plus its `col_types` Exasol type — deduping against existing
      `ProjectionItem::Column` entries AND against columns already appended in this call, and
      skipping any key unresolvable from `col_types`; and
      `wrap_declined_order_by(&str, &[ProjectionItem], usize /* visible_count */, &[SortKey],
      Option<u64>) -> String`, which renders `SELECT <emits_ident(item, i) for i in
      0..visible_count> FROM ({sql}) ORDER BY {render_order_by_clause(keys)} [LIMIT n]`.
      `emits_ident` is `pub(super)` in `support.rs` and is reused verbatim by both the inner
      EMITS clause and this outer list. `wrap_declined_order_by` MUST carry BOTH guards:
      it returns `sql` UNCHANGED when `keys` is empty — preserving today's
      `if keys.is_empty() { sql }` guard at `mod.rs:632-633`, without which
      `render_order_by_clause(&[]) == ""` would emit an invalid bare `ORDER BY ` — and it
      falls back to `SELECT *` when `visible_count == 0`. [expert]
   2. Rewire `build_dispatch_sql` in `crates/lakehouse-engine/src/adapter/pushdown/mod.rs`:
      DELETE the "Declined-ORDER-BY projection guard (issue #190)" block (lines 511-537);
      leave `detect_topn`'s call site reading the pre-extension `proj_cols` untouched; compute
      the declined-`ORDER BY` decision and `parse_order_by_keys` BEFORE `spec_template` is
      built (the boolean at lines 627-629 currently reads `spec_template.common.order_by`,
      which equals the already-computed `order_by`); capture
      `visible_count = proj_cols.len()`; call `extend_projection_with_sort_keys` only on the
      declined path AND strictly BEFORE the `spec_template` literal at line 565, so the common
      blob's `projection` and `emit_exa_types` carry the appended column consistently with the
      EMITS clause built from the same vectors — extending after line 565 would produce an
      EMITS clause containing the hidden column but a scan-spec projection without it, and the
      scan would not emit it; and replace the `SELECT * FROM ({sql}) ORDER BY …` block (lines
      630-646) with `wrap_declined_order_by`. Define `visible_count` in a comment as "the
      number of items `extract_projection` already derived, NOT necessarily the raw
      select-list arity — a separate pre-existing fallback in `extract_projection` /
      `project_columns` (`needs_full_fallback`) can already have widened `proj_cols` to the
      full base row for an untranslatable or EMITS-rejected item, independent of this fix".
      Update the surrounding comments to describe the hidden-column contract and to cite
      #225/#189 instead of the removed #190 widening rationale. `effective_limit` stays `None`
      on the declined path (the limit lands only on the wrapper) — the anti-wrong-truncation
      invariant, decision [4], is unchanged. [expert]

2. **Unit tests — dispatcher and top-N submodule**
   1. Replace `declined_order_by_on_unprojected_column_projects_full_row`
      (`pushdown/mod.rs`) with `declined_order_by_appends_unprojected_sort_key_as_hidden_column`:
      same literal-only select list + `ORDER BY "NAME"` request, now asserting the scan spec's
      `"projection":[{"expr":"1"},"NAME"]` shape, `EMITS ("_LH_PROJ_0" DECIMAL(1,0), "NAME"
      VARCHAR(2000000))`, an outer `SELECT "_LH_PROJ_0" FROM (`, an outer `ORDER BY "NAME"`,
      and the ABSENCE of `REGION` / `AMOUNT` / `"ID"` (no full-row widening). This test MUST
      fail on pre-fix code.
   2. Add `declined_order_by_wrapper_selects_only_original_select_list` (`pushdown/mod.rs`)
      for the bare-column shape — project `NAME`, `ORDER BY "ID"`, no `LIMIT` — asserting one
      visible column in the outer select list, `"ID"` present in EMITS but not in the outer
      select list, and no `SELECT *`.
   3. Add `declined_order_by_dedupes_repeated_and_projected_sort_keys` (`pushdown/mod.rs`):
      an `orderBy` naming `NAME` twice plus `ID` once, over a projection that already carries
      bare `NAME`, asserting `NAME` is NOT appended and `ID` is appended exactly once.
   4. Update `declined_order_by_all_keys_projected_leaves_projection_untouched`
      (`pushdown/mod.rs`) to keep asserting the inert case: projection untouched, no widening,
      and a matched bounded top-N still forming with `ORDER BY "NAME"` + `LIMIT 5` and NO
      wrapping outer `SELECT … FROM (` around the fan-out.
   5. Rewrite `topn.rs`'s `plan_scan_sql` test helper to call
      `extend_projection_with_sort_keys` + `wrap_declined_order_by` instead of duplicating the
      wrapping logic, and update
      `order_by_present_without_topn_match_withholds_per_shard_limit` to assert the new shape:
      the outer `ORDER BY "L_EXTENDEDPRICE" DESC NULLS LAST LIMIT 20` still renders, the
      visible select list is `"L_ORDERKEY"` only, `"L_EXTENDEDPRICE"` appears in EMITS as the
      hidden column, and the per-shard common blob still carries no `"limit"` and no
      `order_by`. [expert]
   6. Add `declined_order_by_extension_runs_after_topn_detection` (`pushdown/mod.rs`) — the
      test that pins decision [2], the plan's most load-bearing invariant. It MUST be able to
      FAIL on a mis-ordered implementation, so the fixture must make `detect_topn` capable of
      matching if the extension ran too early: via `guard_dispatch_sql`, a literal-only select
      list, `orderBy` on `NAME` (ascending, nulls last), `limit = Some(5)`, and a POPULATED
      `logical_schema` carrying `NAME` as `utf8` (a non-JSON-fallback type). Assert the common
      blob carries NEITHER a `"limit"` NOR an `"order_by"` key, and that the outer
      `SELECT "_LH_PROJ_0" FROM (` wrapper IS present. An implementation that extends before
      `detect_topn` would match the top-N, push per-shard `order_by` + `limit`, and emit no
      wrapper — failing all three assertions.
      Do NOT rely on a `detect_topn`-only assertion over the pre-extension projection: that
      holds regardless of call order and is already covered by `topn.rs:370-379`. Likewise
      note that tasks 2.1 and 2.2 canNOT pin this invariant — 2.1 forces `detect_topn` to
      decline via an empty `logical_schema` and 2.2 via an absent `LIMIT`, both order-blind.
      [expert]
   7. Add `declined_order_by_unparseable_sort_key_emits_no_wrapper` (`pushdown/mod.rs`): a
      request whose `orderBy` holds only elements `parse_sort_key_element` rejects (e.g. an
      expression sort key, or a bare column missing `isAscending`), so `has_order_by` is true
      but `parse_order_by_keys` yields zero keys. Assert the returned SQL contains NEITHER
      `ORDER BY` NOR a wrapping outer `SELECT … FROM (` — pinning the empty-keys guard that
      `wrap_declined_order_by` must carry over from `mod.rs:632-633`.

3. **E2E regression tests — live Exasol stack**
   1. Add `e2e_order_by_unprojected_column_bare_projection` to
      `crates/lakehouse-engine/tests/e2e_capability_test.rs` (EVENTS via `vs_table()`,
      following that file's `setup_e2e` / `exa_conn` / `query_columns` conventions). Run issue
      #225's literal repro `SELECT score FROM <t> WHERE id = 1 ORDER BY id` and assert 1
      column, 1 row, `score = 5.0`. Then run `SELECT name FROM <t> WHERE id <= 5 ORDER BY id
      DESC` and assert 1 column, 5 rows, names in `event-05 … event-01` order — proving the
      hidden sort column actually drives the ordering and is dropped from the result. This
      test MUST fail (`sqlCode 04000`) on pre-fix code.
   2. Add `e2e_order_by_column_referenced_only_in_projected_expression` to the same file:
      `SELECT id || '-' || name FROM <t> WHERE id <= 3 ORDER BY id` — `FN_CONCAT` is
      advertised (`adapter/capabilities.rs:87`) and the translator renders `CONCAT`
      (`vs-expression/src/lib.rs:632`), so this pushes down as ONE `Expr` select-list item
      whose sort column `id` is not bare-projected: exactly #225's computed-projection shape.
      Assert 1 column, 3 rows, values `1-event-01`, `2-event-02`, `3-event-03` in that order.
      This test MUST fail (`sqlCode 04000`) on pre-fix code.
      **Pre-check first:** the brief's live repro of this shape failed at Exasol's ARITY
      validation, which runs BEFORE the scan executes, so DataFusion never evaluated the
      concat — its coercion of `concat(Int64/Decimal128, Utf8)` at the pinned DataFusion
      version is therefore UNVALIDATED, and this project has already shipped one
      type-coercion pushdown bug of exactly that shape (`a6e829e`, LIKE over a non-string
      column). Confirm the rendered fragment executes with a cheap `scan_plan_shape`-level
      or `capture-pushdown-payload.sh` check before relying on it. If the concat is rejected,
      switch the repro to `CAST(id AS VARCHAR) || '-' || name` — still ONE `Expr` item, `id`
      still referenced only inside the expression, so the shape under test is unchanged.
   3. Add an `EXPLAIN VIRTUAL` plan-shape assertion (via `explain_virtual_sql`, the helper
      `e2e_scan_test.rs`'s top-N tests use) to task 3.1's test: the pushed SQL MUST contain
      the hidden sort column in the EMITS clause and MUST NOT contain `SELECT * FROM (`.
      Scope the negative "no full-row widening" assertion to the `EMITS (` clause or the
      scan-spec `"projection":[...]` JSON array — NOT to the whole SQL string. A whole-string
      `!contains("EVENT_DATE")` would pass today for the wrong reason: the common blob's
      `logical_schema` carries the LOWERCASE Iceberg field names (`event_date`, `event_ts` —
      `seed.rs:541-545`), so the uppercase spelling is absent by casing accident rather than
      because the projection excludes the column.

4. **Tracked exceptions and cross-issue verification**
   1. File a GitHub issue for the pre-existing declined-path ordering gap on a
      JSON-fallback-typed sort key (the outer `ORDER BY` binds the emitted
      `CAST(col AS VARCHAR)` JSON string, so it orders lexicographically, not natively),
      then REPLACE the `(#TBD-JSONSORT)` placeholder in the
      `pushdown-planning-capability-extensions` spec delta with its real number. Unchanged
      by this fix; reachable today only for an out-of-range `decimal128(p>36,s)` column.
      Until substituted, the delta's exception clause is what keeps that scenario's
      single-node-equality SHALL from being unconditionally false.
   2. File a GitHub issue for the composed pre-existing arity gap — a select-list item that
      trips `extract_projection`'s OWN `needs_full_fallback` full-base-row widening, combined
      with an `ORDER BY` on a column outside even that widened set, which still returns more
      columns than the select list and still fails `04000` — then REPLACE the
      `(#TBD-FULLROWARITY)` placeholder in the same delta with its real number. Note in the
      issue that a bounded-top-N variant for hidden-sort-key shapes (Non-Goals) is the natural
      companion follow-up.
   3. Verify #189 against the fixed build using the shape-equivalent LOCAL query
      `SELECT c_name FROM <VS>.DIM_CUSTOMER WHERE c_custkey <= 5 ORDER BY c_custkey`. #189's
      literal repro (`SELECT c_acctbal FROM CUSTOMER ...`) is NOT reproducible on this stack —
      the seeded `dim_customer` has only `C_CUSTKEY` and `C_NAME` (`seed.rs:996-997`) and
      there is no `CUSTOMER`/`c_acctbal` table; the literal repro needs the remote Glue TPC-H
      cluster and is out of scope here. Record the result; whether to close #189 with an
      explanatory comment is a PR-stage decision, not a planning-stage one.

5. **Verification** — run the checklist below; capture both repros end to end via
   `scripts/capture-pushdown-payload.sh`. Confirm no `#TBD-` placeholder survives in any spec
   delta before recording.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1 → 1.2 (sequential; `topn.rs` then `mod.rs`) |
| Group B | 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7 |
| Group C | 3.1, 3.2, 3.3 |
| Group D | 4.1, 4.2, 4.3, 5 |

Sequential dependencies:

- Group A → Group B and Group A → Group C. Every test in B and C asserts the post-fix SQL
  shape, so both must run after the fix lands. Task 1.1 must precede 1.2 because 1.2 calls
  the helpers 1.1 introduces.
- Groups B and C run concurrently: B touches `pushdown/mod.rs` + `pushdown/topn.rs` test
  modules, C touches `tests/e2e_capability_test.rs`. No shared files.
- Group D runs last: 4.3 needs the fixed build, and 5's manual repro needs the fix plus a
  rebuilt `.so`. 4.1 and 4.2 have no code dependency but must complete before `speq record`,
  since they substitute the spec deltas' `#TBD-` placeholders.
- Task 3.2's coercion pre-check should run at the START of Group C: if the concat is rejected
  the repro SQL changes, and discovering that late costs a rebuild + full E2E cycle.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Code block | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` lines 511-537 — the "Declined-ORDER-BY projection guard (issue #190)" full-base-row widening | Replaced by the append-only sort-key extension; the widening is the direct cause of the `04000` arity rejection |
| Code block | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` lines 630-646 — the inline `SELECT * FROM ({sql}) ORDER BY …` wrapper construction (the `declined_order_by` boolean at 627-629 MOVES earlier rather than being deleted) | Replaced by `topn::wrap_declined_order_by`; the `SELECT *` is the second half of the arity bug. Its `if keys.is_empty() { sql }` guard MOVES into the helper (task 1.1) — it is NOT removed |
| Duplicated logic | `crates/lakehouse-engine/src/adapter/pushdown/topn.rs` lines 256-275 — `plan_scan_sql`'s hand-copied mirror of the dispatcher's decline wrapping | Replaced by a call to the shared helper; a second copy would drift from the fixed dispatcher and keep asserting a shape the real path no longer produces |
| Test | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs::declined_order_by_on_unprojected_column_projects_full_row` | Asserts the removed full-base-row widening (`"projection":["REGION","NAME","AMOUNT","ID"]`); it encodes the bug and is replaced by task 2.1 |
| Spec scenario | `specs/vs-adapter/pushdown-planning-capability-extensions/spec.md` — "Projected literal with an ORDER BY on an unprojected column declines to the full base row" | Normatively requires the removed widening; retired via `DELTA:REMOVED` and replaced by two `DELTA:NEW` scenarios ("ORDER BY on a column outside the derived projection emits the sort key as a hidden scan column" and "Hidden sort-key columns are appended at most once and never invented") |

NOT removed, despite touching the same subject: the sibling scenario "Projected constant whose
declared EMITS type Exasol rejects declines to the full base row" stays exactly as recorded. It
governs `extract_projection`'s own `needs_full_fallback` widening, a DIFFERENT widening this
fix does not touch (see "Two projection widenings" in Context). The delta's new Background
paragraph reconciles the two so the recorded feature does not read as self-contradictory.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| ORDER BY on a column outside the derived projection emits the sort key as a hidden scan column | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_order_by_unprojected_column_bare_projection` |
| ORDER BY on a column outside the derived projection emits the sort key as a hidden scan column | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_order_by_column_referenced_only_in_projected_expression` |
| ORDER BY on a column outside the derived projection emits the sort key as a hidden scan column (literal-only select list; append + explicit wrapper list) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `declined_order_by_appends_unprojected_sort_key_as_hidden_column` |
| ORDER BY on a column outside the derived projection emits the sort key as a hidden scan column (visible column count equals the derived projection) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `declined_order_by_wrapper_selects_only_original_select_list` |
| Hidden sort-key columns are appended at most once and never invented (dedupe across keys) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `declined_order_by_dedupes_repeated_and_projected_sort_keys` |
| Hidden sort-key columns are appended at most once and never invented (already-projected key is inert) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `declined_order_by_all_keys_projected_leaves_projection_untouched` (existing, updated) |
| Hidden sort-key columns are appended at most once and never invented (no parsed sort key → no wrapper, no bare `ORDER BY`) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `declined_order_by_unparseable_sort_key_emits_no_wrapper` |
| An ORDER BY the adapter cannot bound as a top-N remains correctness-safe | Unit | `crates/lakehouse-engine/src/adapter/pushdown/topn.rs` | `order_by_present_without_topn_match_withholds_per_shard_limit` |
| An ORDER BY the adapter cannot bound as a top-N remains correctness-safe (Exasol does not re-sort; the adapter renders the ordering) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `order_by_without_limit_falls_back_correctly` (existing, must still pass unchanged) |
| Unsupported ordered-query shapes decline the ordered-top-N path (eligibility reads the PRE-EXTENSION derived projection) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `declined_order_by_extension_runs_after_topn_detection` |
| Unsupported ordered-query shapes decline the ordered-top-N path (matched top-N undisturbed) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `declined_order_by_all_keys_projected_leaves_projection_untouched` (existing, updated) |
| Unsupported ordered-query shapes decline the ordered-top-N path (matched top-N still pushes down) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `ordered_topn_pushes_down_matches_single_node` (existing, must still pass unchanged) |

The two E2E tests in task 3 are the PRIMARY regression proof: only a live Exasol round-trip
exercises the positional column-count validation that produces the `04000` rejection. The
unit tests pin the generated SQL shape (a pure string computation over an in-memory request)
and are the minimum bar for the append/dedupe/explicit-list rules.

Two coverage rows carry a stronger obligation than "a test exists", because each pins an
invariant that is silent when violated:

- `declined_order_by_extension_runs_after_topn_detection` MUST fail on an implementation that
  extends the projection BEFORE `detect_topn`. Verify that by construction: the fixture gives
  `detect_topn` everything it needs to match (a `LIMIT`, a single table, a populated
  `logical_schema` with a native-typed sort key), so only the call ORDER decides the outcome.
  A test that would pass either way does not discharge this row.
- `declined_order_by_unparseable_sort_key_emits_no_wrapper` MUST fail on an implementation
  whose `wrap_declined_order_by` drops the empty-keys guard, which would emit a bare
  `ORDER BY ` and produce invalid SQL rather than a wrong result — a shape no arity assertion
  elsewhere in this plan would catch.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Bare-column unprojected sort key | `scripts/capture-pushdown-payload.sh 'SELECT c_varchar FROM {table} WHERE id=1 ORDER BY id'` | No `sqlCode 04000`. The echoed scan-spec JSON shows `"projection":["C_VARCHAR","ID"]`; the generated SQL's EMITS carries `"C_VARCHAR" VARCHAR(2000000), "ID" DECIMAL(20,0)` and the outer query reads `SELECT "C_VARCHAR" FROM (…) ORDER BY "ID" …`. The `SELECT` RETURNS exactly ONE column and one row |
| Sort column referenced only inside a projected expression | `scripts/capture-pushdown-payload.sh 'SELECT id\|\|'"'"'-'"'"'\|\|c_decimal_a FROM {table} WHERE id<=3 ORDER BY id'` | No `sqlCode 04000`. EMITS carries `"_LH_PROJ_0" …, "ID" DECIMAL(20,0)`; the outer query reads `SELECT "_LH_PROJ_0" FROM (…) ORDER BY "ID" …`. The `SELECT` RETURNS exactly ONE column, three rows, ascending by `id` |
| Matched bounded top-N unaffected | `scripts/capture-pushdown-payload.sh 'SELECT id, c_double FROM {table} ORDER BY c_double DESC LIMIT 5'` | The scan spec still carries `"order_by":[…]` and a per-shard `"limit"`; the SQL carries `GROUP BY shard_key) ORDER BY` with NO wrapping `SELECT … FROM (` around the fan-out — the matched path is untouched |
| No full-base-row widening | `scripts/capture-pushdown-payload.sh 'SELECT c_varchar FROM {table} WHERE id=1 ORDER BY id'` | The echoed scan-spec `"projection"` names exactly 2 of the table's 10 columns; `C_DECIMAL_A` / `C_DECIMAL_B` / `C_TS` / `C_BOOL` / `C_PRICE` / `C_QTY` / `C_DATE` / `C_DOUBLE` are ABSENT |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Test (host unit) | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt --check` | No changes |
| Build (UDF `.so`) | `make cross-musl-udf-build` | Exit 0 |
| Test (E2E, live stack) | `make test-e2e` | 0 failures, including the two new `e2e_capability_test` tests |
| Spec validation | `speq plan validate fix-225-orderby-non-projected-column` | pass |
| No unresolved placeholders | `grep -rn '#TBD-' specs/_plans/fix-225-orderby-non-projected-column/vs-adapter/` | No matches (tasks 4.1/4.2 substituted the real issue numbers; scoped to the spec deltas — plan.md/decision-log.md prose legitimately keeps the placeholder names for history) |

Commit and PR conventions for this change: the commit message MUST reference `Closes #225`;
the PR title MUST be `fix(pushdown): <short description>` (Conventional Commits, matching
recent merged PRs such as "fix(vs-expression): make LIKE pushdown type-aware for non-string
columns"); the PR base is `main`.
