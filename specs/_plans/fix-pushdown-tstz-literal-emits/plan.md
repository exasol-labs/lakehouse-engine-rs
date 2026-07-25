# Plan: fix-pushdown-tstz-literal-emits

## Summary

Fix issue #218 by routing a select-list item the scan UDF cannot emit to the existing
qualified single-table wrapper, on both the normal and the zero-file path. Today such a
request answers with the full base row, which Exasol rejects as an invalid pushdown
response — the query fails outright. The same routing fixes a projected `SYSTIMESTAMP`
shipping the UTC wall clock (#238). Reaching the literal case also requires correcting the
translator's TSTZ literal node name, which never matched Exasol's wire format.

## Design

### Context

Issue #218 and this plan's own round-1 draft both rested on the same false premise: that
a declined select-list item "falls back to the full base row and Exasol post-processes
the select list — results are correct, only unaccelerated". It does not. Exasol validates
the pushdown response POSITIONALLY against the request's `selectList`, so a full-row
response to an N-item select list is rejected and the user's query FAILS.

Verified on the live E2E container (Exasol 2025.2.1, `SESSIONTIMEZONE = EUROPE/BERLIN`,
against the deployed `MY_LAKEHOUSE` virtual schema):

| Query | Result today | Fixed here |
|---|---|---|
| `SELECT CURRENT_TIMESTAMP FROM <vs_t> WHERE ID = 1` | `04000` "Expected number of columns is 1 but pushdown query has 5" | yes |
| `SELECT CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE) FROM <vs_t> WHERE ID = 1` | same `04000` | yes — Exasol constant-folds this to a bare `literal_timestamputc` node, so it is the LITERAL path, not `FN_CAST` |
| `SELECT CAST(EVENT_TS AS TIMESTAMP WITH LOCAL TIME ZONE) FROM <vs_t> WHERE ID = 1` | same `04000` | NO — `FN_CAST` over a COLUMN is excluded (see "Excluded" below); it stops emitting the invalid full row and instead hard-fails with a named adapter error |
| `SELECT ID, CURRENT_TIMESTAMP …` / `SELECT CURRENT_TIMESTAMP, ID …` | `04000` "expected 2 … has 5" — position-independent | yes |
| `SELECT CURRENT_TIMESTAMP FROM <vs_t> WHERE ID = 999999` (all files pruned) | same `04000` — the zero-file short-circuit never routes | yes |
| `SELECT SYSTIMESTAMP FROM <vs_t> WHERE ID = 1` | succeeds, returns `16:32:33.665` where native returns `18:32:34.061` — a silent 2-hour error (#238) | yes |
| `SELECT LOCALTIMESTAMP FROM <vs_t> WHERE ID = 1` | succeeds, correct — `FN_LOCALTIMESTAMP` is NOT advertised, so Exasol evaluates it itself | n/a (control) |

Two request payloads captured live settle the shapes this plan depends on:

| Captured payload | Finding |
|---|---|
| `SELECT SYSTIMESTAMP FROM <vs_t> WHERE ID = 1` | select-list node `{"name":"SYSTIMESTAMP","type":"function_scalar"}`; `selectListDataTypes` = `{"type":"TIMESTAMP","fractionalSecondsPrecision":3}` — plain TIMESTAMP, so the EMITS-type gate alone cannot see it. Pushed spec: `"projection":[{"expr":"now()"}]`, `EMITS ("_LH_PROJ_0" TIMESTAMP)` — the UDF ships the UTC instant (#238) |
| `SELECT CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP) FROM <vs_t> WHERE ID = 1` | select-list node is a BARE `{"type":"literal_timestamp","value":"2024-03-01 10:00:00.000"}` — Exasol CONSTANT-FOLDS a cast of a timestamp literal; no `function_scalar_cast` node reaches the adapter |
| `WHERE EVENT_TS > CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)` | node type is **`literal_timestamputc`** — NOT `literal_timestamp_utc`, the name `crates/vs-expression/src/lib.rs:301` matches. `value` is `2024-03-01 09:00:00.000`, UTC-normalized. Exasol's own `filter_expr_string_for_debug` names the repair: `CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 09:00:00.000', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP(3) WITH LOCAL TIME ZONE)` |

The last row is a defect this plan discovered and must fix to fix #218 at all: the translator's
timestamp-utc arm matches a node type Exasol never sends, so EVERY TSTZ literal is unrenderable.
Proof it is unmatched today: the pushed scan spec for that predicate carries NO `filter` field and
scans both data files, where a plain-timestamp predicate pushes `"filter":"(\"ID\" = 1)"`.

**Excluded from the fixed set.** `FN_CAST` to TSTZ over a COLUMN stays unfixed.
`render_cast_target` (`crates/vs-expression/src/lib.rs:174-178`) returns `Err` for
`withLocalTimeZone: true` in BOTH dialects, before any dialect match, and
`specs/sql-comprehension/vs-expression-translator-cast/spec.md` states that decline normatively.
This plan does not touch it. Such an item IS still routed away from the invalid full-row response
(its declared type is TSTZ, so the classifier catches it), and the wrapper's renderer then fails
loud and clean — `sql_builders.rs:201-207` returns "join pushdown declined: a select-list item
could not be rendered for the qualified N-scan join; this is a hard error, not a native re-plan".
A named adapter error replaces a misleading `04000`: an improvement, not a fix, and not a new
defect — it is the same underlying `04000` #218 already covers, so no separate issue is opened.

The repo's own recorded spec already names this failure mode. The literal-projection
scenario in `specs/vs-adapter/pushdown-planning-capability-extensions/spec.md` cites
"the column-count mismatch Exasol rejects" for issues #190/#205, and
`specs/vs-adapter/pushdown-planning-grouped-agg/spec.md:132` states it normatively. The
row-scan path is simply the one decline path that never adopted the repair.

The repair mechanism therefore already exists, is already specified, and is already
shipped for two other decline shapes: `qualified_single_table_fallback_pushdown`
(`joins/sql_builders.rs:849`), whose own doc contract is "the result column count and
per-column types match Exasol's positional `selectListDataTypes` validation, so this
never emits the `04000`-triggering bare row scan".

The `LOCALTIMESTAMP` row above is the decisive control: an expression Exasol evaluates
itself returns the correct session-local value. The fix makes the adapter reach that same
outcome deliberately — via the wrapper rather than by withdrawing capabilities, so filter
pushdown and Iceberg file pruning for those functions are preserved.

- **Goals** — make every affected query succeed with the value Exasol computes natively;
  fix #218 and #238; keep the streaming row-scan happy path unchanged; keep the pushed
  DataFusion scan filter byte-identical; record every adjacent defect found while verifying
  as an accurately-scoped tracked exception.
- **Non-Goals** — no change to the raw-scan happy path, the sharding shape, or
  `exasol_type_from_json`'s type table. NOT fixed here: the filter-side now-family
  divergence (#239), the `CHAR(n)` positional type mismatch (#240), the join-side instance
  of this same routing gap (#231), the DataFusion-side half of the `literal_timestamputc`
  node-name defect (#242), and `FN_CAST` to TSTZ over a column.

### Decision

Classify a select-list item the scan UDF cannot emit, and route the whole request to the
qualified single-table wrapper instead of the full base row. Two disjoint reasons make an
item non-emittable:

1. **Declared EMITS type invalid** — `TIMESTAMP WITH LOCAL TIME ZONE`, which Exasol
   rejects as a UDF EMITS output type (sqlCode 22002). Reaches the adapter as a
   `literal_timestamputc` item (including a constant-folded `CAST(TIMESTAMP '…' AS TSTZ)`)
   and as an `FN_CURRENT_TIMESTAMP` item.
2. **Session-context-dependent value** — `SYSTIMESTAMP`, `CURRENT_TIMESTAMP`,
   `CURRENT_DATE`, `SYSDATE`. The scan UDF cannot evaluate these correctly at ANY declared
   type: it has no access to the caller's `SESSIONTIMEZONE`, and connect-back opens an
   independent session. `SYSTIMESTAMP` is declared plain `TIMESTAMP(3)`, so the type gate
   alone lets it through and it ships the UTC instant (#238).

The classifier is a pure predicate over the request — `select_list_requires_exasol_wrapper`
— NOT a comparison of `selectList` length against `proj_cols.len()`. It is deliberately
reason-based so it can never fire on the three arms where the full base row IS the correct
answer (absent `selectList`, empty `selectList`, non-array `selectList`), and it ignores
bare `column` items so a TSTZ-typed base column keeps today's behavior exactly.
`project_columns` keeps its current signature and behavior, which is what leaves the two
join call sites untouched (see #231 below).

#### Architecture

```
pushdown request
  └─ select_list_requires_exasol_wrapper(pushdown_req)   ← new pure predicate (support.rs)
       │
       ├─ false → UNCHANGED on both paths:
       │            files > 0 → build_row_scan_sql (streaming, no wrapper)
       │            files = 0 → empty_pushdown_sql (typed full-row NULLs)
       │
       └─ true  → routed on BOTH paths:
                    files > 0 → qualified_single_table_fallback_pushdown  (mod.rs, RowScan arm)
                                = SELECT <items rendered in Exasol dialect>
                                  FROM (<sharded fan-out, referenced cols only>) AS "LHS_T0"
                    files = 0 → empty_select_list_typed_sql  (file_resolution.rs, RowScan arm)
                                = SELECT CAST(NULL AS <declared type>), … FROM DUAL WHERE 1=0
```

Both paths must route, because `handle_pushdown` short-circuits to `empty_result_sql`
(`mod.rs:219-221`) BEFORE `build_dispatch_sql` runs whenever file resolution prunes to zero
files. The empty path already has the right mechanism: its `GroupByWrapper` arm calls
`empty_select_list_typed_sql` (`file_resolution.rs:709`) for exactly this reason, with the
documented intent "so the empty and non-empty column shapes never diverge (never a full-row
`04000` mismatch)". The `RowScan` arm is the one that never adopted it.

Verified end-to-end at the SQL level against the deployed scan UDF, by wrapping the exact
pushdown SQL `EXPLAIN VIRTUAL` produced:

| Hand-run wrapper shape | Result |
|---|---|
| `SELECT CURRENT_TIMESTAMP FROM (<scan>) AS "LHS_T0"` | `2026-07-25 18:38:16.819` — session-local, correct |
| `SELECT CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 09:00:00','UTC',SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE) FROM (<scan>) AS "LHS_T0"` | `2024-03-01 10:00:00` — identical to the native TSTZ value |
| `SELECT "LHS_T0"."ID", CURRENT_TIMESTAMP, "LHS_T0"."ID" FROM (<scan>) AS "LHS_T0"` | correct, arbitrary interleaving — which a flat sibling select list cannot express |
| both expressions via `CREATE VIEW` + `SYS.EXA_ALL_COLUMNS` | `TIMESTAMP(3) WITH LOCAL TIME ZONE` — matches Exasol's declared type, so the positional type check passes |

#### Patterns

| Pattern | Where | Why |
|---|---|---|
| One decline shape for every non-emittable request | `mod.rs` row-scan path reusing `qualified_single_table_fallback_pushdown` | Three other decline paths already use it; the row-scan path diverging is the defect |
| One predicate shared by the non-empty and empty paths | `support.rs` predicate called from `build_dispatch_sql` and `empty_result_sql` | Mirrors the existing `classify_request_shape` design (`mod.rs:324-332`): both paths route from ONE decision so their column shapes cannot drift |
| Exasol-side evaluation of session context | `vs-expression` Exasol dialect | The only correct place: the value depends on the caller's session, which the UDF cannot observe |
| Symbolic `SESSIONTIMEZONE` | rendered wrapper SQL | Zero adapter knowledge of the session's zone; Exasol resolves it in the caller's session |
| Fail loud, never silently wrong | wrapper renderer's existing hard error | An unrenderable item is a clear error, not a wrong value |
| Dialect-asymmetric wire-name acceptance | `vs-expression` timestamp-utc literal arm | The Exasol dialect accepts `literal_timestamputc`; the DataFusion dialect keeps declining it. Accepting it there too would start pushing TSTZ predicates into DataFusion, whose coercion against a naive `timestamp_us` column is unverified — a correctness risk this plan does not take (#242) |

### Consequences

| Decision | Alternatives Considered | Rationale |
|---|---|---|
| Route to the qualified single-table wrapper | Keep declining to the full base row | Verified `04000` hard failure at every item position — not a correct-but-slow path. The round-1 "permanent design boundary" conclusion rested on this false premise |
| Route to the wrapper | Append the item as a flat SIBLING scalar expression next to `LAKEHOUSE_SCAN(...) EMITS (...)` | Verified to work for a SCALAR emitter, but cannot express two of the required shapes: an EMITS call expands to a contiguous column block, so an item between two scan columns cannot be positioned; and `SELECT CURRENT_TIMESTAMP FROM t` needs exactly one output column while the scan must still emit at least one to drive the rows |
| Route to the wrapper | Substitute plain `TIMESTAMP` for the declared TSTZ EMITS type | Value-lossy (returns the UTC wall clock) AND rejected outright by Exasol's positional type check |
| Route to the wrapper | Withdraw `FN_CURRENT_TIMESTAMP`/`FN_SYSTIMESTAMP`/`LITERAL_TIMESTAMP_UTC` so Exasol never delegates the item | Verified to work (the `LOCALTIMESTAMP` control case), and cheap, but capabilities are global, not per-clause: it would also kill `WHERE ts < CURRENT_TIMESTAMP` predicate pushdown and the Iceberg timestamptz-literal file pruning `iceberg_predicate.rs` depends on. It also cannot cover `FN_CAST` to TSTZ, which is not separately withdrawable |
| Wrapper is acceptable despite the `SELECT * FROM (...)` `MUST NOT` | Treat the `MUST NOT` as prohibiting all wrapping | Those `MUST NOT`s are textually scoped to the literal star form and govern the streaming HAPPY path, which this plan leaves untouched. Three recorded specs already emit non-star decline wrappers. The round-1 draft overclaimed them as a blanket prohibition and cited an archived, non-promoted decision that in fact says the opposite |
| Track #239, #240, #231, #242 rather than fix them | Fold them in | Different code paths and different failure classes; folding them in would blur five defects into one PR. Each is filed with a live repro and cited inline in the spec deltas |
| Keep `is_valid_emits_output_type`'s name and exact-match semantics | Rename, or convert to a substituting `emits_output_type(&str) -> String` | Its verdict is unchanged; only the caller's response to a `false` verdict changes |
| A reason-based predicate over the request | #229's arity check (`selectList.len() != proj_cols.len()`) | Narrower and safer: an arity check also fires on the absent / empty / non-array `selectList` arms, where the full base row is the CORRECT response. A reason-based predicate cannot regress those |
| Leave `project_columns`' signature untouched | Return a `SelectListPlan` enum from it | Changing it forces both join call sites (`joins/rendering.rs:36`, `joins/mod.rs:138`) to handle a new variant, which is exactly the join-side change #231 owns. Leaving it alone keeps the join path byte-identical and keeps #231's description accurate |
| Accept `literal_timestamputc` in the Exasol dialect only | Accept it in both dialects | Both-dialect acceptance starts pushing TSTZ predicates into the DataFusion scan filter, comparing `Timestamp(Microsecond, Some("UTC"))` against a naive `timestamp_us` column. That coercion is unverified and a wrong result is worse than today's correct-but-unpruned scan. Tracked as #242 |
| Exclude `FN_CAST` to TSTZ over a column | Add an Exasol-dialect arm to `render_cast_target` | It would be ~3 lines, but it contradicts a recorded normative contract (`vs-expression-translator-cast/spec.md`: the WLTZ decline happens in both dialects, before precision handling) and so needs its own spec delta and its own verification. Out of scope for #218/#238; the item still stops emitting an invalid response and now fails with a named error |

## Features

| Feature | Status | Spec |
|---|---|---|
| vs-adapter/pushdown-planning-capability-extensions | CHANGED | `vs-adapter/pushdown-planning-capability-extensions/spec.md` |
| sql-comprehension/vs-expression-translator-date-fns | CHANGED | `sql-comprehension/vs-expression-translator-date-fns/spec.md` |
| sql-comprehension/vs-expression-translator-literals | CHANGED | `sql-comprehension/vs-expression-translator-literals/spec.md` |

## Requirements

| Requirement | Details |
|---|---|
| No `04000` for a routed request | `SELECT CURRENT_TIMESTAMP FROM <vs_t>`, `SELECT SYSTIMESTAMP FROM <vs_t>`, and a projected TSTZ literal MUST all succeed |
| No `04000` when all files are pruned | The same three queries under an all-pruning predicate MUST also succeed, returning zero rows |
| Value equals native | Each routed item's value MUST equal Exasol's native value for the same expression in the same session. A projected `CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)` MUST return `2024-03-01 10:00:00` |
| Happy path untouched | The streaming row-scan SQL for an all-emittable select list MUST be byte-identical to today's; no new wrapper, no `SELECT *` |
| Scan filter untouched | The pushed DataFusion `ScanSpec.filter` MUST be byte-identical to today's for every request, including one carrying a TSTZ literal predicate (#242 stays open) |
| Join path untouched | `project_columns`' signature and behavior MUST NOT change, so both join call sites keep today's behavior (#231 stays open) |
| Exasol dialect purity | No `arrow_cast`, `now()`, or `current_date()` may appear in wrapper SQL |
| Guard cannot go vacuous | The E2E scenario MUST pin the session time zone and fail loudly on a zero UTC offset before asserting any value |
| No silent gaps | #231, #239, #240, and #242 MUST each be cited inline in the spec deltas as accurately-scoped tracked exceptions |

## Dependencies

**No crate, SDK, or version change.** The `.so` must be rebuilt and redeployed for the E2E
tests because the adapter entry point changes.

**Deliberate overlap with the in-flight PR #229.** PR #229 (`fix/210-string-functions-type-blind`,
base `fix/212-timestamp-precision-collapse`, OPEN and not merged) already adds an equivalent
reroute to `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` (commit `e41e2b0`): in the
`RequestShape::RowScan` path it routes to `qualified_single_table_fallback_pushdown` when
`selectList` length does not match `proj_cols.len()`. That commit is NOT on `main`.

This plan's PR must be based on `main`, so it cannot build on #229's unmerged branch and MUST
carry its own reroute — otherwise #218's `04000` stays unfixed on this branch's own diff and
the E2E scenarios here cannot pass. The duplication is deliberate, not an oversight. The two
triggers differ: #229 fires on any arity mismatch; this plan fires only on a classified
non-emittable item (see the Decision section for why the narrower trigger is safer). When
#229 merges, in either order relative to this PR, a human must reconcile the overlap in
`mod.rs` — expect a small merge conflict and a follow-up dedup to a single trigger. Task 10
states this in the PR body as a known, expected follow-up rather than a defect.

Issue #231 tracks the same routing gap on the broadcast-join path and is NOT addressed here.

## Implementation Tasks

1. Re-verify every load-bearing finding against the live E2E Exasol container before
   changing any file, using the EXACT SQL below, and abandon this plan's direction if any
   check fails. [expert]
   - (a) Session zone is non-UTC: `SELECT SESSIONTIMEZONE;` → expect a non-UTC zone
     (`EUROPE/BERLIN`).
   - (b) TSTZ display value versus UTC representation:
     `SELECT CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE) AS DISP, CONVERT_TZ(TIMESTAMP '2024-03-01 10:00:00', SESSIONTIMEZONE, 'UTC') AS UTC_REPR;`
     → expect `DISP = 2024-03-01 10:00:00`, `UTC_REPR = 2024-03-01 09:00:00`.
     NOTE: `CAST(<tstz> AS TIMESTAMP)` returns the SESSION-LOCAL value `10:00:00`, not
     `09:00:00` — it is NOT the check to use.
   - (c) `CURRENT_TIMESTAMP` is typed TSTZ: create a view over `SELECT CURRENT_TIMESTAMP`
     and read `COLUMN_TYPE` from `SYS.EXA_ALL_COLUMNS` → expect
     `TIMESTAMP(3) WITH LOCAL TIME ZONE`.
   - (d) THE load-bearing check — the decline is a hard failure, not a slow path. Run
     `SELECT CURRENT_TIMESTAMP FROM <vs_t> WHERE ID = 1;`,
     `SELECT CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE) FROM <vs_t> WHERE ID = 1;`,
     and `SELECT ID, CURRENT_TIMESTAMP FROM <vs_t> WHERE ID = 1;` → each expect SQL state
     `04000` "Expected number of columns is N but pushdown query has 5". If any SUCCEEDS,
     stop: the whole fix rationale is wrong.
   - (e) `(#238)`: `SELECT SYSTIMESTAMP FROM <vs_t> WHERE ID = 1;` versus
     `SELECT SYSTIMESTAMP;` → expect the VS value to be behind the native value by the
     session's UTC offset. Control: `SELECT LOCALTIMESTAMP FROM <vs_t> WHERE ID = 1;` →
     expect it to MATCH native (unadvertised, so Exasol evaluates it).
   - (f) Capture the REAL `pushdown` request payload rather than inferring it, per the
     repo's own convention. Run
     `EXPLAIN VIRTUAL SELECT COUNT(*) FROM <vs_t> WHERE EVENT_TS > CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE);`
     and read the select node's literal → expect node type `literal_timestamputc` with
     `value` `2024-03-01 09:00:00.000`, and `filter_expr_string_for_debug` carrying
     `CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 09:00:00.000', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP(3) WITH LOCAL TIME ZONE)`.
     This confirms the value is UTC-NORMALIZED (not session-local). If it carries `10:00:00`
     instead, the Exasol-dialect literal rendering in task 3 must drop the `CONVERT_TZ` and
     cast the value directly.
   - (f2) THE NODE-NAME check. From the same output confirm the node type is
     `literal_timestamputc` — NOT `literal_timestamp_utc`, the string
     `crates/vs-expression/src/lib.rs:301` matches. Confirm the arm is unmatched today by
     reading the pushed `LAKEHOUSE_SCAN` spec from the same `EXPLAIN VIRTUAL`: expect NO
     `filter` field at all and every data file listed (no pruning), where a plain-timestamp
     predicate pushes `"filter":"(\"ID\" = 1)"`. If the node type IS `literal_timestamp_utc`,
     skip the wire-alias half of task 3 — the arm already matches.
   - (f3) SELECT-LIST-side verification of (f), as a targeted extension of that already-verified
     filter-side finding — not a re-derivation. `EXPLAIN VIRTUAL` cannot read the payload for a
     projected TSTZ item directly, because the adapter's invalid full-row response makes
     `EXPLAIN VIRTUAL` itself fail with `04000`. Use the emittable plain-TIMESTAMP analogue
     instead: `EXPLAIN VIRTUAL SELECT CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP) FROM <vs_t> WHERE ID = 1;`
     → expect the select-list node to be a BARE `{"type":"literal_timestamp","value":"2024-03-01 10:00:00.000"}`
     with NO `function_scalar_cast` wrapper, proving Exasol constant-folds a cast of a timestamp
     literal into a bare literal node in the SELECT LIST. Combined with (f), this establishes that
     a projected `CAST(TIMESTAMP '…' AS TIMESTAMP WITH LOCAL TIME ZONE)` arrives as a bare
     `literal_timestamputc` node carrying the UTC-normalized value — the LITERAL path, not
     `FN_CAST`. After task 5 lands, re-confirm the projected value end-to-end via task 8's
     assertion. If the folded node does NOT appear (a `function_scalar_cast` reaches the adapter
     instead), that shape is in the EXCLUDED set: task 8 then asserts the named adapter error
     instead of `04000`, and moves its value assertion to a directly projected
     `literal_timestamputc` item.
   - (h) The zero-file path: `SELECT CURRENT_TIMESTAMP FROM <vs_t> WHERE ID = 999999;` (a
     predicate that prunes every file) → expect the SAME `04000` today, proving the
     `empty_result_sql` short-circuit at `mod.rs:219-221` also needs routing (task 6).
   - (g) The wrapper shape itself: take the pushdown SQL from
     `EXPLAIN VIRTUAL SELECT ID FROM <vs_t> WHERE ID = 1;` and hand-run
     `SELECT CURRENT_TIMESTAMP FROM (<that SQL>) AS "LHS_T0";` and
     `SELECT CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 09:00:00','UTC',SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE) FROM (<that SQL>) AS "LHS_T0";`
     → expect the session-local now, and exactly `2024-03-01 10:00:00`. Declare both via
     `CREATE VIEW` and confirm `SYS.EXA_ALL_COLUMNS` reports
     `TIMESTAMP(3) WITH LOCAL TIME ZONE`.
2. Add the Exasol-dialect arm for the now-family in `crates/vs-expression/src/lib.rs`:
   `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `SYSDATE`, `SYSTIMESTAMP` each render as
   themselves under `Dialect::Exasol`, leaving the DataFusion arm unchanged. Add unit
   tests asserting both dialects and asserting no `now()`/`current_date()` leaks into an
   Exasol-dialect render.
3. Add the Exasol-dialect arms for the two timestamp literals in
   `crates/vs-expression/src/lib.rs` (the arms at lines 291 and 301), and accept the real
   wire node name. [expert]
   - `literal_timestamp` → `TIMESTAMP '<value>'` under `Dialect::Exasol`.
   - `literal_timestamputc` / `literal_timestamp_utc` →
     `CAST(CONVERT_TZ(TIMESTAMP '<value>', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)`
     under `Dialect::Exasol`, carrying the declared fractional-seconds precision as
     `TIMESTAMP(p) WITH LOCAL TIME ZONE` when the request declares one (Exasol's own debug
     rendering uses `TIMESTAMP(3) WITH LOCAL TIME ZONE`), and treating `<value>` as the UTC
     representation.
   - Accept the wire name `literal_timestamputc` as a match alias ALONGSIDE the existing
     `literal_timestamp_utc`, in the EXASOL dialect ONLY. Under `Dialect::DataFusion` the wire
     name MUST keep returning the same unmatched/`None` outcome as today, so the pushed
     `ScanSpec.filter` is byte-identical for every request. Comment the asymmetry at the arm
     with the reason and the issue number: accepting it in the DataFusion dialect would start
     pushing TSTZ predicates into DataFusion, whose coercion against a naive `timestamp_us`
     column is unverified (#242).
   - Keep the existing quote-escaping (`quote_literal`) on both arms and leave both
     DataFusion `arrow_cast` renderings untouched.
   - Unit tests: each Exasol-dialect form renders exactly as above; `arrow_cast` never appears
     in an Exasol-dialect render; the wire name `literal_timestamputc` renders in the Exasol
     dialect AND still yields `None` from `render_expression_safe` (locking #242's deliberate
     deferral so a later change cannot silently widen the scan filter).
4. Add the non-emittable classifier in
   `crates/lakehouse-engine/src/adapter/pushdown/support.rs` as a PURE predicate over the
   request — `select_list_requires_exasol_wrapper(pushdown_req: &Json) -> bool`. It does NOT
   change `project_columns`. [expert]
   - Return `false` unless `selectList` is a non-empty JSON array, so the absent, empty, and
     non-array arms keep returning the full base row, which is correct for them.
   - Return `true` when any item that is NOT a bare `column` node is non-emittable by either
     reason: (a) its parallel `selectListDataTypes` entry maps via `exasol_type_from_json` to a
     type that fails the existing `is_valid_emits_output_type` (today: exactly
     `TIMESTAMP WITH LOCAL TIME ZONE`); or (b) its node tree contains, at any nesting depth, a
     `function_scalar` whose `name` is `CURRENT_TIMESTAMP`, `SYSTIMESTAMP`, `CURRENT_DATE`, or
     `SYSDATE`.
   - Skip bare `column` items deliberately: `project_columns`' `column` arm types them from
     `involvedTables`, never from `selectListDataTypes`, and never consults
     `is_valid_emits_output_type`. Including them would change behavior for a TSTZ-typed base
     column, which is outside this plan's scope.
   - `SYSTIMESTAMP` is why reason (b) exists: its declared type is plain `TIMESTAMP(3)`
     (verified in task 1's captured payload), so reason (a) alone cannot see it (#238).
   - Document at the predicate why the scan cannot evaluate either class, and why the trigger
     is reason-based rather than an arity comparison, citing this plan's decision log.
5. Route the predicate in `crates/lakehouse-engine/src/adapter/pushdown/mod.rs`'s
   `RequestShape::RowScan` arm of `build_dispatch_sql` to
   `qualified_single_table_fallback_pushdown`, passing the same arguments the `GroupByWrapper`
   and multi-`COUNT(DISTINCT)` guards already pass, and returning before the declined-ORDER-BY
   widening and `detect_topn` run. [expert]
6. Route the SAME predicate in `empty_result_sql`'s `RequestShape::RowScan` arm
   (`crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs:700`), mirroring the
   pattern its `GroupByWrapper` arm already uses one line above: when the predicate fires,
   return `empty_select_list_typed_sql(pushdown_req)` with the existing
   `.unwrap_or_else(|| empty_pushdown_sql(proj_cols, proj_types))` fallback for an
   absent/empty `selectListDataTypes`; otherwise return `empty_pushdown_sql` exactly as today.
   This is required because `handle_pushdown` short-circuits here at `mod.rs:219-221` before
   `build_dispatch_sql` runs whenever file resolution prunes to zero files.
   `CAST(NULL AS TIMESTAMP WITH LOCAL TIME ZONE)` is valid Exasol SQL (verified live), and
   `exasol_type_from_json` already emits that type string, so `empty_select_list_typed_sql`
   needs no change.
7. Add and update unit tests. [expert]
   - New, in `support.rs`: the predicate fires for a TSTZ-declared `literal_timestamputc` item;
     for a TSTZ-declared scalar-expression item; and for a `SYSTIMESTAMP` `function_scalar`
     item declared plain `TIMESTAMP(3)`. It does NOT fire for an all-emittable select list, for
     an absent `selectList`, for an empty `selectList`, or for a bare `column` item.
   - New, in `mod.rs`: an all-emittable select list still produces the unchanged streaming
     row-scan SQL with no wrapper; a routed request produces the `AS "LHS_T0"` wrapper.
   - New, in `file_resolution.rs`: `empty_result_sql` on a `RowScan` request whose select list
     holds a TSTZ-declared item returns a select-list-typed empty shape whose column count
     equals the `selectList` length (NOT the full-row `empty_pushdown_sql` shape), and an
     all-emittable `RowScan` request still returns `empty_pushdown_sql` byte-identically.
   - Update `selectlist_tstz_literal_falls_back_to_full_row` (`support.rs:3286`): change its
     node type from the synthetic `literal_timestamp_utc` to the real wire name
     `literal_timestamputc`, and split its render assertion — `render_expression_safe`
     (DataFusion) MUST return `None` for it (#242), while the Exasol-dialect renderer MUST
     return the `CONVERT_TZ` form. Keep its `project_columns` full-row assertion unchanged and
     rename it to say so (`project_columns` still widens; the routing now happens above it),
     then assert the new predicate fires for the same request.
   - Leave `selectlist_plain_timestamp_literal_rendered_as_expr` (`support.rs:3327`) untouched:
     it locks `is_valid_emits_output_type`'s exact-match boundary, which does not change.
8. Add the E2E scenario to `crates/lakehouse-engine/tests/e2e_capability_test.rs`
   following the existing `setup_e2e` / `vs_table` / `conn.query_columns` /
   `assert_select_pushed_down` harness pattern. [expert]
   - Set the session zone explicitly (`ALTER SESSION SET TIME_ZONE = 'EUROPE/BERLIN'`),
     then read `SESSIONTIMEZONE` and its UTC offset and PANIC if the offset is zero,
     before asserting any value.
   - Assert `SELECT CURRENT_TIMESTAMP FROM <vs_t> WHERE id = 1` and
     `SELECT SYSTIMESTAMP FROM <vs_t> WHERE id = 1` both succeed, and that each value is
     within 60 seconds of the same session's native value.
   - Assert each value's deviation from native is strictly less than the session's UTC
     offset in seconds, computed from `SESSIONTIMEZONE` — the sharp assertion that fails
     if the UTC instant is ever emitted, independent of tolerance width.
   - Assert the projected TSTZ LITERAL by exact value, not by tolerance:
     `SELECT CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE) FROM <vs_t> WHERE id = 1`
     MUST return `2024-03-01 10:00:00`. This is the value-fidelity assertion for the literal
     path; the two now-family queries above cannot provide it because their value moves. Assert
     it equals the native
     `CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 09:00:00','UTC',SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)`
     in the same session rather than hardcoding the string alone, so the assertion stays valid
     if the harness's pinned zone ever changes.
   - Assert the zero-file path: the same three queries under an all-pruning predicate
     (`WHERE id = 999999`, or whatever value the fixture guarantees prunes every file) MUST
     each succeed and return zero rows, never `04000`. If the fixture cannot be shown to prune
     to zero files, keep the assertion as "succeeds and returns zero rows" — which holds either
     way — and rely on task 7's `empty_result_sql` unit test as the deterministic guard.
   - Assert `EXPLAIN VIRTUAL` for the two now-family queries shows the `AS "LHS_T0"` wrapper
     and no `_LH_PROJ_0` identifier.
9. Correct the stale comment in `crates/lakehouse-engine/tests/common/int96_fixtures.rs`
   that calls `#118` open and claims timestamptz cannot reach the scan-emit path: `#118`
   is CLOSED and `types/mapping.rs` maps Iceberg `Timestamptz` to plain `TIMESTAMP`, so
   the column path does reach scan-emit.
10. Post the verified findings to GitHub. Comment on `#218` via
    `ghbrk gh issue comment 218 --repo exasol-labs/lakehouse-engine-rs` stating that its
    "Impact: Low — correctness is preserved" premise is disproven (the query fails with
    `04000`; live repro included), that the fix routes the item to the qualified wrapper,
    and that the desired outcome is met by Exasol-side evaluation rather than by a
    narrowed positional EMITS projection. Keep `#218` open until the implementing PR closes
    it. The PR body MUST:
    - close `#218` and `#238` (`Closes #218`, `Closes #238`);
    - reference `#231`, `#239`, `#240`, and `#242` as deliberately unfixed tracked
      exceptions, each with its one-line scope reason;
    - state the PR #229 overlap explicitly as a known, expected follow-up, not a defect:
      this PR is based on `main`, which lacks #229's arity-mismatch reroute, so it carries
      its own narrower reason-based reroute in `mod.rs`; whichever PR merges second will
      need a small manual reconciliation to a single trigger.
11. Run the verification checklist below.

## Parallelization

| Parallel Group | Tasks |
|---|---|
| Group A | Task 1 |
| Group B | Task 2, Task 3, Task 9 |
| Group C | Task 4 |
| Group D | Task 5, Task 6 |
| Group E | Task 7, Task 8, Task 10 |
| Group F | Task 11 |

Sequential dependencies:
- Group A → everything (the findings gate every edit; a failed re-verification abandons
  the direction)
- Group B → Group C: tasks 2 and 3 are independent of each other (different arms) and task 9
  is a different file, but the wrapper cannot render a routed select list until both Exasol
  dialect arms exist
- Group C → Group D: tasks 5 and 6 both consume task 4's predicate. They edit DIFFERENT files
  (`mod.rs`, `file_resolution.rs`) and neither changes the other's signature, so they are
  genuinely concurrent
- Group D → Group E → Group F

## Dead Code Removal

| Type | Location | Reason |
|---|---|---|
| None | — | `is_valid_emits_output_type` and both call sites stay; `project_columns`' full-row fallback stays (it is still correct for the absent/empty/non-array arms and still reached by the join callers); the pre-existing decline unit test is corrected and extended, not deleted |
| Not removed here | `iceberg_predicate.rs:93` `literal_timestamp_utc` pruning arm | Dead for real traffic because of the same wire-name defect, but repairing it changes file pruning and needs its own verification — tracked as #242, not touched here |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|---|---|---|---|
| Projected constant whose declared EMITS type Exasol rejects routes to the qualified wrapper (literal branch) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `selectlist_tstz_literal_routes_to_qualified_wrapper` |
| Projected constant whose declared EMITS type Exasol rejects routes to the qualified wrapper (scalar-expression branch) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `selectlist_tstz_scalar_expr_routes_to_qualified_wrapper` |
| Projected constant whose declared EMITS type Exasol rejects routes to the qualified wrapper (session-dependent item at a valid declared type) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `selectlist_systimestamp_routes_to_qualified_wrapper_despite_plain_timestamp_type` |
| Projected constant whose declared EMITS type Exasol rejects routes to the qualified wrapper (predicate does not fire where the full base row is correct) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `wrapper_predicate_ignores_absent_empty_and_column_select_lists` |
| Projected constant whose declared EMITS type Exasol rejects routes to the qualified wrapper (zero-file path routes identically) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs` | `empty_result_row_scan_routes_non_emittable_select_list_to_typed_shape` |
| Projected literal or constant select-list item is pushed down as a positional projection | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `selectlist_plain_timestamp_literal_rendered_as_expr` (existing, unchanged) |
| Projected literal or constant select-list item is pushed down as a positional projection (happy path emits no wrapper) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `emittable_selectlist_emits_streaming_row_scan_without_wrapper` |
| CURRENT_DATE and CURRENT_TIMESTAMP translate per dialect | Unit | `crates/vs-expression/src/lib.rs` | `now_family_renders_exasol_names_in_exasol_dialect` |
| Literal nodes translate to SQL literal forms | Unit | `crates/vs-expression/src/lib.rs` | `timestamp_literals_render_exasol_dialect_without_arrow_cast` |
| Literal nodes translate to SQL literal forms (wire node name accepted in the Exasol dialect only) | Unit | `crates/vs-expression/src/lib.rs` | `literal_timestamputc_wire_name_renders_exasol_only` |
| Routed session-dependent projection returns the value Exasol computes natively | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_session_dependent_projection_routes_to_wrapper_and_matches_native` |
| Routed session-dependent projection returns the value Exasol computes natively (exact TSTZ literal value, and the zero-file path) | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_projected_tstz_literal_matches_native_and_pruned_scan_succeeds` |

Unit tests cover the routing decision and both dialect renderings because
`project_columns` and `render_expression` are pure computation over a JSON request with no
I/O. The value-fidelity scenario is an E2E integration test because only the live database
exercises the session-timezone conversion and the positional pushdown validation the
scenario constrains.

### Manual Testing

| Feature | Command | Expected Output |
|---|---|---|
| vs-adapter/pushdown-planning-capability-extensions | `exapump sql --dsn "exasol://sys:exasol@localhost:28563?validateservercertificate=0" "SELECT CURRENT_TIMESTAMP FROM <vs_schema>.EVENTS WHERE ID = 1;"` | One row; no `04000`; timestamp matches the wall clock in `SESSIONTIMEZONE`, NOT the UTC wall clock |
| vs-adapter/pushdown-planning-capability-extensions | `exapump sql --dsn "exasol://sys:exasol@localhost:28563?validateservercertificate=0" "SELECT SYSTIMESTAMP FROM <vs_schema>.EVENTS WHERE ID = 1;"` | One row matching native `SELECT SYSTIMESTAMP;` — no UTC-offset shift (#238) |
| sql-comprehension/vs-expression-translator-literals | `exapump sql --dsn "exasol://sys:exasol@localhost:28563?validateservercertificate=0" "EXPLAIN VIRTUAL SELECT CURRENT_TIMESTAMP FROM <vs_schema>.EVENTS WHERE ID = 1;"` | Pushed SQL is a qualified wrapper (`FROM (…) AS "LHS_T0"`) whose outer select list carries `CURRENT_TIMESTAMP`; no `_LH_PROJ_0`, no `arrow_cast`, no `now()` |
| sql-comprehension/vs-expression-translator-literals | `exapump sql --dsn "exasol://sys:exasol@localhost:28563?validateservercertificate=0" "SELECT CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE) FROM <vs_schema>.EVENTS WHERE ID = 1;"` | Exactly `2024-03-01 10:00:00` — the session-local value, identical to native; no `04000` |
| vs-adapter/pushdown-planning-capability-extensions | `exapump sql --dsn "exasol://sys:exasol@localhost:28563?validateservercertificate=0" "SELECT CURRENT_TIMESTAMP FROM <vs_schema>.EVENTS WHERE ID = 999999;"` | Zero rows; no `04000` — the pruned-to-zero-files path routes too |
| vs-adapter/pushdown-planning-capability-extensions | `exapump sql --dsn "exasol://sys:exasol@localhost:28563?validateservercertificate=0" "EXPLAIN VIRTUAL SELECT COUNT(*) FROM <vs_schema>.EVENTS WHERE EVENT_TS > CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE);"` | Pushed scan spec still carries NO `filter` field and still lists every data file — byte-identical to before this change, confirming #242 is untouched |

### Checklist

| Step | Command | Expected |
|---|---|---|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
| Spec validation | `speq plan validate fix-pushdown-tstz-literal-emits` | pass |
