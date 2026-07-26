# Decision Log: fix-225-orderby-non-projected-column

## Interview

This plan was authored in HEADLESS mode — no live human was available. The exchanges below
are the brief the orchestrator passed in, recorded as Q&A so the reasoning behind each
constraint stays traceable.

**Q:** What exactly fails, and where is the root cause?
**A:** Issue #225. A pushed-down query with `ORDER BY <col>` fails when `<col>` is not a
top-level bare select-list item, even when it is referenced inside another projected
expression. Root cause is in
`crates/lakehouse-engine/src/adapter/pushdown/mod.rs::build_dispatch_sql`: the
"Declined-ORDER-BY projection guard (issue #190)" block (lines 511-537) widens
`proj_cols`/`proj_types` to the FULL BASE ROW when a sort key is unprojected, and the later
decline block (lines 627-646) wraps the scan in `SELECT * FROM (...) ORDER BY <key>`. Exasol
validates the returned pushdown SQL's column count POSITIONALLY against the original select
list, so a 10-column widened row for a 1-item select list is rejected with
`sqlCode 04000 "Expected number of columns is 1 but pushdown query has 10"`. This was already
verified live against the local Exasol Docker stack via
`scripts/capture-pushdown-payload.sh` / `e2e_capture_pushdown`; do not re-derive it, but do
read the code to confirm the exact shape.

**Q:** What is the proper fix, as opposed to a patch around the specific repro?
**A:** Instead of widening the ENTIRE projection, extend only the SCAN's emitted-column set
with the missing ORDER BY sort-key column(s) as extra HIDDEN columns — resolved by name from
`col_types`, appended AFTER the original `proj_cols`/`proj_types` so original positions and
`emits_ident` indices are unchanged — then have the declined-`ORDER BY` wrapper SELECT
explicitly only the ORIGINAL select-list columns by their `emits_ident` identifiers rather
than `SELECT *`. Example:
`SELECT "SCORE" FROM (SELECT ... EMITS ("SCORE" ..., "ID" ...) ...) ORDER BY "ID"`.

**Q:** What must the fix NOT change?
**A:** `detect_topn`'s eligibility check must keep seeing only the ORIGINAL `proj_cols`. The
bounded top-N optimization path (`vs-adapter/pushdown-planning-topn`) is explicitly OUT OF
SCOPE — only the already-existing declined/unoptimized fallback path's correctness is being
fixed. Do not try to make these newly-fixed shapes eligible for the bounded top-N; that would
touch the matched-topn rendering path, which emits `proj_cols` directly as the final visible
EMITS with no wrapping SELECT, and is a separate, riskier change.

**Q:** How should an unresolvable sort-key column be handled?
**A:** A sort-key column that cannot be resolved from `col_types` (defensive — should never
happen, since every ORDER BY column is a real table column) should be left unresolved exactly
as today's pre-existing defensive decline shape. Do not add new defensive machinery beyond
what already exists for this edge case.

**Q:** Which existing artifacts encode the bug and must be corrected, not preserved?
**A:** The unit tests `declined_order_by_on_unprojected_column_projects_full_row` and
`declined_order_by_all_keys_projected_leaves_projection_untouched` in `pushdown/mod.rs`
assert the buggy full-base-row widening. And
`specs/vs-adapter/pushdown-planning-capability-extensions/spec.md`'s last scenario
("Projected literal with an ORDER BY on an unprojected column declines to the full base row")
documents the buggy behavior and needs a corrective delta.

**Q:** Is there a related open issue?
**A:** Check, do not assume: issue #189 (open, unassigned, "ORDER BY on a non-projected
column generates invalid pushdown (column not found)", repro
`SELECT c_acctbal FROM CUSTOMER WHERE c_custkey <= 5 ORDER BY c_custkey`) looks like the same
root cause and repro shape. Note it in the plan as an implement-time verification, not a
planning-stage resolution. Whether to close #189 with an explanatory comment is a PR-stage
decision.

**Q:** What regression coverage is required?
**A:** Two shapes, both of which must fail on pre-fix code and pass after: (1) the
bare-column case — a SELECT projecting one column and ordering by a DIFFERENT, unprojected
column; (2) the computed-projection case — a SELECT whose only select-list item is an
expression referencing the ORDER BY column without bare-projecting it. Prefer a live E2E
addition (extending `e2e_capability_test.rs` or a similar existing E2E file, following its
conventions) as the PRIMARY coverage, PLUS updating the two existing unit tests to assert
the NEW correct behavior as the minimum unit bar. Map both explicitly to test tasks.

**Q:** Does the Iceberg-spec compliance rule in `CLAUDE.md` apply?
**A:** No — the fix touches no Iceberg scanning, schema, or type handling; it is pure
Exasol-SQL-shape plumbing in the adapter's pushdown-response construction. Note this
explicitly in the plan as "no deviation, not applicable" rather than silently skipping it.

**Q:** Does the adjacent `pushdown-planning-topn` spec also need a delta?
**A:** Check whether ITS wording needs one given the `SELECT *` → explicit-column-list
change, versus only the more specific "Projected literal with an ORDER BY..." scenario
needing one.

**Q:** What are the commit and PR conventions?
**A:** PR title MUST be `fix(pushdown): <short description>` (Conventional Commits, matching
recent merged PRs); the commit message must reference `Closes #225`; PR base is `main`. Do
NOT create or checkout any branch — plan in place on the current worktree/branch.

## Design Decisions

### [1] Extend the scan's emitted columns; never widen the visible projection

- **Decision:** On the declined-`ORDER BY` path, append each unprojected bare sort-key column
  (resolved by name from `col_types`) to `proj_cols`/`proj_types` AFTER every original item,
  and have the wrapper name only the original items explicitly via `emits_ident`. The scan's
  emitted-column set and the query's visible column set become two different sets instead of
  being forced equal by widening.
- **Alternatives:**
  - Keep the widening and add an explicit outer select list. Rejected: fixes arity but still
    scans and transports every base column for a 1-column query.
  - Decline the pushdown entirely (`Err`) for this shape. Rejected: a hard, user-visible
    failure for a very common shape (`SELECT a FROM t ORDER BY b`), which is what #189
    reports as high-impact.
  - Drop the pushed `orderBy` and let Exasol sort. Rejected: Exasol does NOT re-apply a
    delegated `orderBy` once `ORDER_BY_COLUMN` is advertised — verified live and documented
    in `e2e_scan_test.rs::order_by_without_limit_falls_back_correctly` — so this returns
    silently unordered rows, the worst possible outcome.
- **Rationale:** Append-only preserves every original select-list index, so `emits_ident`'s
  positional `_LH_PROJ_{index}` and `raw_scan`'s matching `AS _LH_PROJ_{i}` alias remain
  aligned by construction rather than by a second hand-maintained rule. The explicit visible
  list makes the returned arity equal the select list's by construction. This is also
  precisely the fix issue #189's own report suggests.
- **Promotes to ADR:** yes

### [2] Run the projection extension AFTER `detect_topn`, not before

- **Decision:** Reverse the #190 guard's deliberate ordering. `detect_topn` is called on the
  ORIGINAL `proj_cols`; the extension runs only once the shape is known to be declined.
- **Alternatives:** Keep the extension before `detect_topn`, as the #190 guard does — its
  comment argues that a widened projection lets a bounded top-N match, "a strictly better,
  equally well-formed outcome".
- **Rationale:** That argument holds for widening (every column visible, so a matched top-N's
  wrapper-less EMITS is still well-formed) but NOT for hidden columns. The matched-top-N path
  emits `proj_cols` directly as the FINAL visible EMITS with no wrapping SELECT, so a hidden
  column reaching it would leak into the result and reintroduce the arity mismatch. Ordering
  is therefore load-bearing for correctness, not stylistic — hence the explicit normative
  clause in the `pushdown-planning-topn` delta and a dedicated pinning test.
- **REFINED by review finding [3].** The originally-planned pinning test could not actually
  fail on a mis-ordered implementation. The obligation is now discharged by
  `declined_order_by_extension_runs_after_topn_detection`, a dispatcher-level test fixtured so
  `detect_topn` COULD match if the extension ran early. A second ordering constraint surfaced
  while resolving advisory [4].6: the extension must run BEFORE `spec_template`
  (`mod.rs:565`), or the EMITS clause would carry the hidden column while the scan-spec
  projection would not. So the extension is pinned on BOTH sides — after `detect_topn`, before
  `spec_template`.
- **Promotes to ADR:** yes

### [3] Delta the `pushdown-planning-topn` spec too, as a wording-only correction

- **Decision:** Author a second spec delta for
  `vs-adapter/pushdown-planning-topn`'s "Unsupported ordered-query shapes decline the
  ordered-top-N path" scenario: replace its stale "relying on Exasol to apply the ordering it
  retains" clause with the self-contained-wrapper description, and add the normative clause
  pinning top-N eligibility to the derived projection as it stands before any declined-path
  sort-key extension. No top-N CODE path changes.
- **Alternatives:** Delta only `pushdown-planning-capability-extensions`, on the reading that
  the brief said `pushdown-planning-topn` is "NOT to be modified".
- **Rationale:** The brief's "not to be modified" is a CODE constraint on the bounded top-N
  path, and the brief explicitly invited the wording check. The clause in question is
  factually false today — once `ORDER_BY_COLUMN` is advertised Exasol does not re-apply a
  delegated `orderBy`, which is exactly why the B6 self-contained wrapper exists — and
  leaving a knowingly-false normative sentence in the library is the kind of silent gap this
  project's rules forbid. The delta touches one scenario's THEN clauses and adds one, with no
  behavioral claim about the matched path.
- **Promotes to ADR:** no

### [4] Rename the buggy capability-extensions scenario via REMOVED + NEW, not CHANGED

- **Decision:** Retire "Projected literal with an ORDER BY on an unprojected column declines
  to the full base row" with `DELTA:REMOVED` (quoting its full retired body, which the
  `speq plan validate` step-structure check requires) and introduce two `DELTA:NEW`
  scenarios in its place: "ORDER BY on a column outside the derived projection emits the sort
  key as a hidden scan column" (the core rule; retitled per review finding [2] from
  "...outside the select list...") and "Hidden sort-key columns are appended at most once and
  never invented" (the dedupe and defensive rules).
- **Alternatives:** A single `DELTA:CHANGED` block keeping the old title; or one combined NEW
  scenario carrying all seven THEN/AND clauses.
- **Rationale:** The old title asserts the removed behavior ("declines to the full base row"),
  so keeping it would leave a self-contradicting heading. `DELTA:CHANGED` is matched by
  scenario title during the record merge, so a rename inside a CHANGED block risks leaving the
  old scenario in place alongside the new one. REMOVED + NEW states the retirement explicitly.
  The replacement is also broadened past the literal-only select list to cover all three
  reachable shapes (different bare column, column referenced only inside a projected
  expression, literal-only select list), since the root cause is shape-independent. Splitting
  the core rule from the dedupe/defensive rules keeps each scenario within the validator's
  recommended step budget and gives the two concerns independent test mappings.
- **Promotes to ADR:** no

### [5] Home the new helpers in `topn.rs` and delete the duplicated test mirror

- **Decision:** Add `extend_projection_with_sort_keys` and `wrap_declined_order_by` as
  `pub(super)` in `crates/lakehouse-engine/src/adapter/pushdown/topn.rs`, and rewrite that
  file's `plan_scan_sql` test helper (lines 256-275) to call `wrap_declined_order_by` instead
  of hand-copying the dispatcher's wrapping logic.
- **Alternatives:** `support.rs` (already 3474 lines of cross-cutting helpers); or inline in
  `mod.rs` (the status quo, which is what produced the stale mirror).
- **Rationale:** `topn.rs` already owns `parse_order_by_keys`, `detect_topn`, and the ORDER BY
  concern generally, so the declined-`ORDER BY` rendering belongs beside them.
  `vs-adapter/pushdown-module-structure`'s own scenario requires a shared helper to live in
  one place rather than be duplicated; the current mirror already violates that in spirit, and
  after this fix it would silently drift and keep asserting a shape the real dispatcher no
  longer produces.
- **Promotes to ADR:** no

### [6] Defensive edges: unresolvable sort key and an empty visible projection

- **Decision:** A sort key unresolvable from `col_types` is skipped by the extension and
  otherwise left exactly as today (the `ORDER BY` clause still renders it). When
  `visible_count == 0` the wrapper keeps `SELECT *`.
- **Alternatives:** Hard-error on an unresolvable key; or drop the wrapper entirely; or always
  emit the explicit list.
- **Rationale:** `col_types` is the full `involvedTables[0].columns` list and every pushed sort
  key is a real table column, so the unresolvable case is unreachable — per the brief, add no
  new machinery for it. `SELECT  FROM (…)` is not valid SQL, and an empty row-scan projection
  is itself already impossible, so the `visible_count == 0` fallback is a one-line structural
  guard, not a new code path.
- **Promotes to ADR:** no

### [7] E2E regression tests go in `e2e_capability_test.rs` against EVENTS, not `typed_distinct_probe`

- **Decision:** Add both regression tests to
  `crates/lakehouse-engine/tests/e2e_capability_test.rs`, using the EVENTS seed table via
  `vs_table()`: `SELECT score FROM <t> WHERE id = 1 ORDER BY id` (issue #225's own literal
  repro) and `SELECT id || '-' || name FROM <t> WHERE id <= 3 ORDER BY id`.
- **Alternatives:**
  - Use `typed_distinct_probe`, the table the brief's two verified repros ran against.
    Rejected as the primary home: only `e2e_count_distinct_test.rs` (topically wrong) and
    `e2e_capture_pushdown.rs` (a debugging tool, not a regression suite) seed it, and adding
    a second seed to `e2e_capability_test.rs`'s setup would slow every test in that binary.
  - Put them in `e2e_count_distinct_test.rs` to reuse its `typed_distinct_probe` seed.
    Rejected: that file's subject is COUNT(DISTINCT).
- **Rationale:** `e2e_capability_test.rs` already owns the capability-extensions feature's
  scenarios, including the #190 literal-projection regressions, and is already in the
  `make test-e2e` target. EVENTS reproduces both shapes faithfully: #225's own primary repro
  is stated on EVENTS (`SELECT score FROM EVENTS WHERE id=1 ORDER BY id`), and `FN_CONCAT` is
  advertised (`adapter/capabilities.rs:87`) with a translator arm
  (`vs-expression/src/lib.rs:632`), so `id || '-' || name` pushes down as ONE `Expr`
  select-list item — exactly the computed-projection shape, with `id` referenced only inside
  it.
- **Promotes to ADR:** no

### [8] Scope out the JSON-fallback-typed declined sort key, but file it rather than leave it silent

- **Decision:** Do not fix, in this plan, the pre-existing ordering gap where a declined
  `ORDER BY` on a JSON-fallback-typed column sorts the emitted `CAST(col AS VARCHAR)` JSON
  string lexicographically instead of the native value. Task 4.1 files a tracked issue and
  cites it inline in the capability-extensions spec Background before recording.
- **Alternatives:** Fix it in this plan (e.g. decline the wrapper for a fallback-typed sort
  key); or say nothing.
- **Rationale:** The gap is unchanged by this fix — today's full-base-row widening emits the
  same cast column and the same lexicographic outer sort — so it is not a regression, and
  bundling it would widen a targeted correctness fix. `detect_topn` already declines
  fallback-typed sort keys for exactly this reason, so the declined path is where the
  representation mismatch lands. It is reachable today only via an out-of-range
  `decimal128(p>36,s)` column. Saying nothing would leave an unnamed correctness gap, which
  this project's rules forbid.
- **SUPERSEDED IN PART by review finding [4].4.** This entry originally kept the exception out
  of the spec delta entirely, to avoid shipping a placeholder citation. `plan-reviewer` showed
  that leaves the scenario's single-node-equality SHALL unconditionally false for a
  fallback-typed sort key — a worse defect than a placeholder. The exception is now stated
  inline in the scenario with a `(#TBD-JSONSORT)` citation that task 4.1 substitutes, and a
  checklist row greps for any surviving `#TBD-` marker before recording.
- **Promotes to ADR:** no

### [9] No `pushdown-module-structure` delta

- **Decision:** Author no delta for `vs-adapter/pushdown-module-structure`.
- **Alternatives:** Delta it because the fix adds two `pub(super)` helpers to `topn.rs` and
  changes a test helper.
- **Rationale:** That feature's scenarios govern the preserved public façade, byte-identical
  behavior across the refactor itself, per-submodule test co-location, and the shared-base /
  shared-helper / single-classifier consolidations. Adding two `pub(super)` helpers to the
  submodule that already owns the concern, and replacing a duplicated test mirror with a call
  to a shared helper, is consistent with every one of those scenarios — it does not change
  what any of them require.
- **Promotes to ADR:** no

## Review Findings

### [1] [plan-review] `wrap_declined_order_by` dropped the existing empty-keys guard

- **Finding:** `plan-reviewer` (round 1, BLOCKER, COMPLETENESS_GAP) flagged that task 1.1
  specified `wrap_declined_order_by` as rendering unconditionally apart from a
  `visible_count == 0` fallback, silently discarding the existing `if keys.is_empty() { sql }`
  guard at `mod.rs:632-633`. Verified: `render_order_by_clause(&[])` returns `""`
  (`scan/spec.rs:194-199`, whose own doc says "callers must guard on that before emitting a
  bare `ORDER BY`"), and a non-empty `orderBy` CAN parse to zero keys because
  `parse_sort_key_element` filters out non-column elements and elements missing
  `isAscending`/`nullsLast` while `order_by_present` only checks array non-emptiness. The
  planned helper would have emitted `… ORDER BY ` — invalid SQL — for that shape.
- **Direction change:** Task 1.1 now requires BOTH guards explicitly, with the empty-keys one
  named as carried over rather than newly invented. Added a Consequences row, a Dead Code
  Removal note that the guard MOVES rather than being deleted, a spec-delta AND clause in
  "Hidden sort-key columns are appended at most once and never invented", and a new unit test
  (task 2.7, `declined_order_by_unparseable_sort_key_emits_no_wrapper`) with an explicit
  must-fail-on-a-dropped-guard obligation in the coverage notes.
- **Promotes to ADR:** no

### [2] [plan-review] The arity guarantee was overstated and contradicted a recorded sibling scenario

- **Finding:** `plan-reviewer` (round 1, BLOCKER, REQUIREMENT_CONFLICT) flagged that
  `visible_count = proj_cols.len()` is not reliably the select-list arity. Verified:
  `extract_projection` / `project_columns` (`support.rs:640-752`) has its OWN pre-existing
  full-base-row fallback via `needs_full_fallback`, triggered by an untranslatable item, an
  unknown/aggregate node, or a declared EMITS type Exasol rejects — and that fallback is
  MANDATED by the recorded sibling scenario "Projected constant whose declared EMITS type
  Exasol rejects declines to the full base row". For `SELECT <untranslatable expr> FROM t
  ORDER BY id` the projection is already the full base row before this fix runs, the extension
  is inert, and the wrapper still returns every base column → still `04000`. The plan's prose
  conflated "projection item" with "select-list item", so after `speq record` the feature would
  have read as self-contradictory.
- **Direction change:** Introduced the term DERIVED PROJECTION and restated every rule against
  it instead of the select list — in the spec delta's Background, all three affected scenarios,
  and the plan's Goals, Design, and Consequences. Added a Context subsection ("Two projection
  widenings, only one of which this plan touches") distinguishing the two widenings, a
  Dead Code Removal note that the sibling scenario stays as recorded and is reconciled rather
  than contradicted, a Non-Goals entry naming the composed gap, and task 4.2 to file it as a
  tracked exception substituting the spec delta's `(#TBD-FULLROWARITY)` citation.
- **Promotes to ADR:** yes

### [3] [plan-review] No test could detect a mis-ordered implementation

- **Finding:** `plan-reviewer` (round 1, BLOCKER, TRACEABILITY_GAP) flagged that decision [2]
  calls "extend AFTER `detect_topn`" load-bearing for correctness, yet nothing in the plan
  would fail if an implementation extended first. Verified: the original task 2.6 only asserted
  `detect_topn` over the pre-extension projection returns `None`, which is true regardless of
  call order and already covered at `topn.rs:370-379`; task 2.1's fixture forces a decline via
  an empty `logical_schema` (`mod.rs:1274`) and task 2.2's via an absent `LIMIT` — both
  order-blind. A future implementation could reintroduce the exact `04000` bug with a green
  `cargo test`.
- **Direction change:** Replaced task 2.6 with `declined_order_by_extension_runs_after_topn_
  detection` at the DISPATCHER level via `guard_dispatch_sql`, fixtured so `detect_topn` COULD
  match if the extension ran early (literal-only select list, `ORDER BY "NAME"`,
  `limit = Some(5)`, populated `logical_schema` with `NAME` as `utf8`), asserting no `"limit"`
  and no `"order_by"` in the common blob plus the presence of the outer
  `SELECT "_LH_PROJ_0" FROM (` wrapper. Recorded the must-fail-on-mis-ordering obligation in
  the coverage notes, and documented in the task why the `detect_topn`-only assertion and
  tasks 2.1/2.2 cannot discharge it. Also tagged `[expert]`.
- **Promotes to ADR:** yes

### [4] [plan-review] Advisory findings folded in

- **Finding:** Six ADVISORY findings, all verified against the code before acting.
- **Direction change:**
  1. Path-change trade-off (previously-matching-but-broken top-N shapes now decline to an
     unbounded per-shard scan) named in Non-Goals and in the topn delta's Background, with the
     bounded variant flagged as follow-up in task 4.2's issue.
  2. Task 3.2's `id || '-' || name` coercion risk: added a pre-check obligation plus the
     `CAST(id AS VARCHAR) || '-' || name` fallback, and a Parallelization note to run the
     pre-check at the START of Group C. The reviewer's reasoning holds — the brief's live repro
     failed at ARITY validation, which precedes execution, so DataFusion never evaluated the
     concat and its coercion is genuinely unvalidated.
  3. Task 4.3 (was 4.2) restated to the shape-equivalent LOCAL query
     `SELECT c_name FROM <VS>.DIM_CUSTOMER WHERE c_custkey <= 5 ORDER BY c_custkey`; verified
     `dim_customer` carries only `C_CUSTKEY`/`C_NAME` (`seed.rs:996-997`), so #189's literal
     `c_acctbal`/`CUSTOMER` repro needs the remote Glue cluster and is out of scope.
  4. The JSON-fallback exception is now stated INLINE in the spec delta scenario with a
     `(#TBD-JSONSORT)` citation, reversing design decision [8]'s "keep placeholders out of the
     spec": the reviewer is right that the single-node-equality SHALL is otherwise
     unconditionally false, and a false SHALL is worse than a placeholder. Task 4.1 substitutes
     the number and a checklist row greps for surviving `#TBD-` markers.
  5. Task 3.3's negative assertion scoped to the `EMITS (` clause / `"projection":[...]` array.
     Verified the reviewer's casing claim: EVENTS Iceberg fields are lowercase
     (`seed.rs:541-545`), so a whole-string `!contains("EVENT_DATE")` would pass by accident.
  6. Prose: trimmed the #189 quote to the mechanism only, noting this plan does NOT rely on
     its "Exasol drops them from the final output" premise (the wrapper drops them itself);
     unified the line citations as 627-629 (boolean) and 630-646 (wrapper); added the
     "extension must run BEFORE `spec_template` at `mod.rs:565`" requirement to task 1.2 so
     the common blob's `projection`/`emit_exa_types` and the EMITS clause stay consistent.
- **Promotes to ADR:** no

### [4] [plan-review] Round 2 confirmed all three BLOCKERs resolved; two ADVISORY nits fixed inline

- **Finding:** `plan-reviewer` (round 2) re-walked the actual current plan/decision-log/spec-delta
  text and the real code against each round-1 BLOCKER — including manually tracing task 2.6's
  fixture through `detect_topn` under the mis-ordered-implementation hypothesis — and confirmed
  all three genuinely resolved (not merely reworded). It raised two ADVISORY nits: (a) the
  Checklist's `#TBD-` grep row (`grep -rn '#TBD-' specs/_plans/.../`) can never pass, since
  task 4.1/4.2's own task text and this decision log legitimately keep the placeholder names as
  history; (b) the `pushdown-planning-topn` delta's Background (and this decision log's entry
  [3]) still said "the ORIGINAL select list" in one place, one sentence after a correctly-worded
  "derived projection" clause — the last remnant of BLOCKER [2]'s conflation, in text the record
  merge would carry into the permanent feature.
- **Direction change:** Fixed both directly (no third review round needed, per both findings
  being ADVISORY and one-line): scoped the Checklist grep to
  `specs/_plans/.../vs-adapter/` (the spec deltas only) with a note that plan.md/decision-log.md
  prose keeps the placeholder names on purpose; replaced "the ORIGINAL select list" with "the
  derived projection as it stands before any declined-path sort-key extension" in the topn
  delta's Background and in this decision log's entry [3].
- **Promotes to ADR:** no
