# Decision Log: fix-pushdown-tstz-literal-emits

## Interview

Headless mode: the orchestrator supplied a pre-resolved brief in place of a live interview. The
exchanges below paraphrase that brief and record the planner's disposition of each item.

**Q:** Fix issue #218 — a projected `TIMESTAMP WITH LOCAL TIME ZONE` constant declines to the
full-base-row fallback instead of pushing down as a narrowed positional projection. Land a real fix.
**A (orchestrator):** Emitting such an item under EMITS type plain `TIMESTAMP` is value-safe.
Evidence offered: Exasol stores/exchanges TSTZ as a UTC instant; `literal_timestamp_utc` already
renders a UTC instant; the declared EMITS type only affects `coerce_batch_to_exa_types`, which casts
the timezone metadata and not the microsecond value; and the same reasoning already shipped for
Iceberg timestamptz scan COLUMNS in decision `007-fix-timestamptz-mapping`.

**Q:** Is that reasoning sound? The brief authorized declining instead if re-verification "finds a
hole in this reasoning", and asked that any hole be stated explicitly with reasoning.
**A (planner):** The proposed EMITS-type substitution has a hole, verified against the live E2E
Exasol container. Each premise is individually true, but they do not compose to value safety for a
projected OUTPUT value: Exasol returns a TSTZ value converted into the session time zone, while
plain `TIMESTAMP` is returned verbatim. See design decision [1].

**Q:** Use the proposed fix shape — replace `is_valid_emits_output_type`'s gate with an EMITS-type
substitution in both gated branches of `project_columns`?
**A (planner):** Rejected. The gate's verdict stays; what changes is the adapter's RESPONSE to a
`false` verdict. See design decisions [1] and [2].

**Q:** Update the tracked-exception scenario in
`specs/vs-adapter/pushdown-planning-capability-extensions/spec.md` so it no longer reads as an open
tracked exception.
**A (planner):** Done, as a real fix: the item is routed to the qualified single-table wrapper and
Exasol evaluates it in the caller's session. See design decision [3].

**Q:** Update the stale unit test `selectlist_tstz_literal_falls_back_to_full_row` and the comment
on `selectlist_plain_timestamp_literal_rendered_as_expr` that references
`is_valid_emits_output_type`'s exact-match boundary, since that function's semantics will change.
**A (planner):** The function's semantics do NOT change, so
`selectlist_plain_timestamp_literal_rendered_as_expr` and its comment stay correct and untouched.
`selectlist_tstz_literal_falls_back_to_full_row` IS stale — it asserts the invalid full-row response
— and is retargeted to assert wrapper routing. See design decision [4].

**Q:** The PR title is decided: `fix(vs-adapter): push down a projected TIMESTAMP WITH LOCAL TIME
ZONE constant instead of declining to the full base row`.
**A (planner):** The type is right but the mechanism named is wrong. See design decision [5].

## Design Decisions

### [1] Reject the EMITS-type substitution: it is both value-lossy and rejected by Exasol

- **Decision:** Do not substitute plain `TIMESTAMP` for a declared `TIMESTAMP WITH LOCAL TIME ZONE`
  EMITS type.
- **Alternatives:** Substitute the type as the brief proposed. Rejected on verified evidence
  gathered against the live E2E container (Exasol 2025.2.1, `SESSIONTIMEZONE = EUROPE/BERLIN`):
  `CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)` displays
  `2024-03-01 10:00:00`, while its UTC representation — the value the UDF would emit — is
  `2024-03-01 09:00:00`. The error is one hour under CET, two under CEST, silent, and
  session-dependent. Independently, Exasol validates the pushdown response's per-column types
  positionally against `selectListDataTypes` and rejects a substituted type outright, so the
  substitution would not even reach the wrong-value stage.
- **Rationale:** Every premise in the brief was individually correct; the composition was not. The
  brief's decisive analogy — decision `007-fix-timestamptz-mapping` — differs in exactly the load-
  bearing respect: there the VS DECLARES the Iceberg column plain `TIMESTAMP` at
  `createVirtualSchema`, so Exasol makes no localization promise and the UTC wall clock IS the
  contract. Here Exasol has already inferred TSTZ for the select-list item from its own expression
  analysis, independent of the adapter's schema, so a localization promise exists.
- **Promotes to ADR:** yes

### [2] Route a non-emittable select-list item to the qualified single-table wrapper

- **Decision:** When `project_columns` finds a select-list item the scan UDF cannot emit, route the
  whole request to `qualified_single_table_fallback_pushdown` — the shape the grouped-aggregate and
  multi-`COUNT(DISTINCT)` declines already use — instead of responding with the full base row.
- **Alternatives:**
  (a) Keep declining to the full base row. Rejected: it is an INVALID pushdown response, not a slow
  one. Exasol validates the response positionally against the request's `selectList` and rejects a
  count mismatch with SQL state `04000`. Live-verified for both gated branches, for a TSTZ `FN_CAST`
  over a column, and at every item position.
  (b) Append the item as a flat SIBLING scalar expression beside `LAKEHOUSE_SCAN(...) EMITS (...)`.
  Verified to work for a SCALAR emitter, but it cannot express two required shapes: an EMITS call
  expands to a CONTIGUOUS column block, so an item sitting between two scan columns cannot be
  positioned; and `SELECT CURRENT_TIMESTAMP FROM t` must return exactly one column while the scan
  must still emit at least one column to drive the rows.
  (c) Withdraw `FN_CURRENT_TIMESTAMP`, `FN_SYSTIMESTAMP`, and `LITERAL_TIMESTAMP_UTC` so Exasol
  never delegates the item. Verified to work — the `LOCALTIMESTAMP` control case returns the correct
  value precisely because `FN_LOCALTIMESTAMP` is unadvertised — and by far the cheapest. Rejected
  because capabilities are global, not per-clause: it would also remove `WHERE ts < CURRENT_TIMESTAMP`
  predicate pushdown and the Iceberg timestamptz-literal file pruning `iceberg_predicate.rs`
  depends on, and it cannot cover `FN_CAST` to TSTZ, which is not separately withdrawable.
  (d) Carry the value as a `VARCHAR` bearing a UTC offset. Rejected: Exasol's `VARCHAR` → TSTZ
  conversion honors only `NLS_TIMESTAMP_FORMAT` and rejects the offset suffix (SQL state 22018),
  and an offset-free string is read as session-local, losing the instant.
  (e) Read `SESSIONTIMEZONE` in the adapter over connect-back and compensate. Rejected: connect-back
  opens an INDEPENDENT session and cannot observe the user session's zone.
- **Rationale:** The mechanism already exists, is already specified normatively for two other
  decline shapes, and its documented contract is exactly the requirement — "the result column count
  and per-column types match Exasol's positional `selectListDataTypes` validation, so this never
  emits the `04000`-triggering bare row scan". The row-scan path is the one decline path that never
  adopted it. Verified end-to-end at the SQL level against the deployed scan UDF: the wrapper
  returns the session-local value, reproduces a TSTZ literal exactly, supports arbitrary column
  interleaving, and yields column type `TIMESTAMP(3) WITH LOCAL TIME ZONE`.
- **Promotes to ADR:** yes

### [3] Fix #218 rather than close it as a permanent design boundary

- **Decision:** Rewrite the spec scenario as a real fix — the item routes to the qualified wrapper
  and Exasol evaluates it — and let the implementing PR close `#218` with `Closes #218`.
- **Alternatives:** Close `#218` as a permanent, deliberate design boundary (this plan's round-1
  conclusion). Rejected: it rested on the premise that the decline yields a correct-but-unaccelerated
  result. That premise is false — the query FAILS — so there is nothing defensible to record as a
  boundary. Leaving the exception open was also rejected: a verified fix exists.
- **Rationale:** CLAUDE.md requires a known deviation to be either fixed or recorded as an
  accurately-scoped tracked exception. A fix is available at moderate cost and reuses a shipped
  mechanism, so the deviation is fixed. The residual defects found while verifying are what get
  tracked, each filed with a live repro and cited inline in the spec deltas: `#239` (filter-side
  now-family divergence), `#240` (`CHAR(n)` positional type mismatch), `#242` (the
  `literal_timestamputc` wire-name defect's DataFusion half and the dead Iceberg `timestamptz`
  pruning arm), and `#231` (the same routing gap on the broadcast-join path, filed independently of
  this plan). `FN_CAST` to TSTZ over a column is excluded as a scope boundary rather than filed as a
  new issue: it is the same `#218` `04000`, and after this plan it fails with a named adapter error
  instead.
- **Promotes to ADR:** yes

### [4] Keep the gate's name and semantics; correct its test and add the missing coverage

- **Decision:** Keep `is_valid_emits_output_type` and its exact-match `!=` semantics. Keep
  `selectlist_tstz_literal_falls_back_to_full_row`'s `project_columns` full-row assertion, which
  stays TRUE because `project_columns` is unchanged; correct the node type it uses from the
  synthetic `literal_timestamp_utc` to the real wire name `literal_timestamputc`, split its render
  assertion per dialect, and extend it to assert the new routing predicate fires. Leave
  `selectlist_plain_timestamp_literal_rendered_as_expr` untouched. Add coverage for the
  scalar-expression branch, for a session-dependent item at a valid declared type, and for the
  three arms where the predicate must NOT fire.
- **Alternatives:** Convert the guard to a substituting `emits_output_type(&str) -> String`
  (rejected with decision [1]); delete the decline test (rejected — the shape still needs a test);
  retarget the test to assert wrapper routing from `project_columns` (rejected once decision [8]
  moved the routing OUT of `project_columns` — the widening it asserts still happens).
- **Rationale:** The guard's verdict is unchanged; only the adapter's response to a `false` verdict
  changes, and that response now lives above `project_columns`. The test was additionally passing on
  a payload shape that never occurs on the wire, which hid the node-name defect in decision [9].
- **Promotes to ADR:** no

### [5] Replace the pre-decided PR title

- **Decision:** The pre-decided title names a mechanism the fix does not use. Propose
  `fix(vs-adapter): route a non-emittable projected select-list item to the qualified wrapper instead
  of the invalid full base row`.
- **Alternatives:** Keep the original `fix(...)` wording (rejected: it promises a narrowed positional
  EMITS projection, which is impossible for a TSTZ item); use `docs(...)` (rejected with decision
  [3] — a real defect is fixed and behavior changes).
- **Rationale:** CLAUDE.md requires the title to describe the change's target state. Task 10 carries
  the corresponding action: the PR body closes `#218` and `#238` and references `#239` and `#240`.
- **Promotes to ADR:** no

### [6] Do not narrow the routing to admit a timezone-independent TSTZ-declared NULL

- **Decision:** Keep the classifier uniform; do not add a special case letting a TSTZ-declared
  `literal_null` stay on the positional-projection path.
- **Alternatives:** Admit it, since NULL carries no instant and is provably timezone-safe.
- **Rationale:** Speculative, and now moot: the wrapper handles a TSTZ-declared NULL correctly with
  no special case, so the exception would buy only the avoidance of one wrapper. The observed
  issue-#205 `literal_null` payload declares `BOOLEAN`, never TSTZ, so the shape has never been seen
  on the wire.
- **Promotes to ADR:** no

### [7] Fix the projected SYSTIMESTAMP defect here; track the filter-side instance separately

- **Decision:** Fix the adjacent defect found while verifying — `SELECT SYSTIMESTAMP FROM <vs_table>`
  returns the UTC wall clock — inside this plan, by the same routing, since it is the same failure
  mode from the same cause. Filed as `#238`, closed by the implementing PR. Do NOT fix the
  filter-side instance of the same rendering arm; file it as `#239` and cite it inline.
- **Alternatives:**
  (a) Only note the defect in the plan (this plan's round-1 disposition). Rejected: a live
  wrong-value defect must not be a note. It is now filed, cited inline, and fixed.
  (b) File it but fix it in a separate plan. Rejected: it needs no additional mechanism — the same
  classifier and the same routing cover it, and separating them would ship a plan that fixes the
  TSTZ half of a two-half defect.
  (c) Fix the filter side too. Rejected: a pushed filter is executed inside DataFusion by design, so
  there is no Exasol-side seam. Repairing it means withdrawing four capabilities (losing predicate
  pushdown and its file pruning) or passing the session's evaluated instant into the scan spec —
  a different design question.
- **Rationale:** Verified live: `SELECT SYSTIMESTAMP FROM <vs_t>` returned `16:32:33.665` against a
  native `18:32:34.061` — a silent 2-hour error under CEST, invisible in a UTC session. Exasol types
  `SYSTIMESTAMP` plain `TIMESTAMP(3)`, so the EMITS-type gate alone cannot see it; the classifier
  needs the session-dependence test. `LOCALTIMESTAMP` is NOT affected — verified correct, because
  `FN_LOCALTIMESTAMP` is not advertised. `CURRENT_DATE`/`SYSDATE` are the same class (a UTC-versus-
  session-local date across local midnight) and are covered by the same test.
- **Promotes to ADR:** no

### [8] Route from a reason-based predicate above `project_columns`, not from inside it

- **Decision:** Add a pure predicate `select_list_requires_exasol_wrapper(pushdown_req)` in
  `support.rs` and call it from `build_dispatch_sql`'s `RequestShape::RowScan` arm and from
  `empty_result_sql`'s `RowScan` arm. Leave `project_columns`' signature and behavior unchanged.
- **Alternatives:**
  (a) Change `project_columns` to return a `SelectListPlan` enum (this plan's round-2 draft,
  tasks 5-6). Rejected: it forces both join call sites (`joins/rendering.rs:36`,
  `joins/mod.rs:138`) to handle the new variant, which is exactly the join-side change `#231`
  owns. Collapsing the variant back to a full row at those sites is dead ceremony; handling it
  properly is out of scope. Leaving the signature alone keeps the join path byte-identical.
  (b) PR #229's trigger — compare `selectList` length against `proj_cols.len()`. Rejected as
  broader than needed: an arity comparison also fires on the absent, empty, and non-array
  `selectList` arms, where the full base row IS the correct response.
- **Rationale:** A pure predicate over the request is callable from BOTH the resolved-file
  dispatcher and the zero-file short-circuit, which decision [10] showed is mandatory. That
  mirrors the design already documented at `mod.rs:324-332` for `classify_request_shape`: both
  paths route from one decision so their column shapes cannot drift. The predicate deliberately
  ignores bare `column` items, whose EMITS type comes from `involvedTables` and never reaches
  `is_valid_emits_output_type`.
- **Promotes to ADR:** yes

### [9] Fix the TSTZ literal's wire node name, in the Exasol dialect only

- **Decision:** Accept `literal_timestamputc` — Exasol's actual wire node type — alongside the
  existing `literal_timestamp_utc` in `vs-expression`'s timestamp-utc arm, under
  `Dialect::Exasol` ONLY. The DataFusion dialect keeps declining the wire name exactly as today.
- **Alternatives:**
  (a) Accept it in both dialects. Rejected for this plan: it would immediately start pushing TSTZ
  literal predicates into the DataFusion scan filter as
  `arrow_cast('<utc>+00:00', 'Timestamp(Microsecond, Some("UTC"))')`, compared against a naive
  `timestamp_us` column. That coercion is unverified, and a wrong coercion converts a
  currently-correct-but-unpruned query into a silently wrong one. Filed as `#242` with the
  verification it needs.
  (b) Leave the name alone. Rejected: it leaves `#218`'s headline repro unfixed. A projected TSTZ
  literal must render in the wrapper or the routing has nothing to render.
- **Rationale:** Discovered while capturing the payload for task 1(f): the request carries
  `{"type":"literal_timestamputc","value":"2024-03-01 09:00:00.000"}`, while
  `crates/vs-expression/src/lib.rs:301` matches `literal_timestamp_utc`. Confirmed unmatched by
  its effect, not by reading alone — the pushed scan spec for that predicate carries NO `filter`
  field and lists every data file, where a plain-timestamp predicate pushes
  `"filter":"(\"ID\" = 1)"`. The existing unit test passed only because its fixture used the
  synthetic name. `#242` also covers the identically misspelled `iceberg_predicate.rs:93`
  `timestamptz` range-pruning arm, which is dead for the same reason; repairing that changes file
  pruning and needs its own verification.
- **Promotes to ADR:** no

### [10] Route the zero-file path too

- **Decision:** Extend `empty_result_sql`'s `RowScan` arm with the same routing, emitting
  `empty_select_list_typed_sql` when the predicate fires.
- **Alternatives:** Fix only the dispatcher. Rejected: `handle_pushdown` short-circuits to
  `empty_result_sql` at `mod.rs:219-221` BEFORE `build_dispatch_sql` runs whenever file resolution
  prunes to zero files, so a dispatcher-only fix leaves
  `SELECT CURRENT_TIMESTAMP FROM <vs_table> WHERE ID = 999999` failing with the same `04000`.
- **Rationale:** The mechanism already exists one line above: the `GroupByWrapper` arm calls
  `empty_select_list_typed_sql` (`file_resolution.rs:709`) for exactly this reason, documented as
  "so the empty and non-empty column shapes never diverge (never a full-row `04000` mismatch)".
  The `RowScan` arm is the one that never adopted it. `CAST(NULL AS TIMESTAMP WITH LOCAL TIME ZONE)`
  is valid Exasol SQL and `exasol_type_from_json` already emits that type string, so the helper
  needs no change.
- **Promotes to ADR:** no

### [11] Duplicate PR #229's reroute deliberately rather than depend on its branch

- **Decision:** Implement this plan's own reroute in `mod.rs` even though PR #229 already adds an
  equivalent one, and record the duplication as an expected follow-up in the PR body.
- **Alternatives:**
  (a) Branch from `fix/210-string-functions-type-blind` and build on #229. Rejected: this
  deliverable requires PR base `main`, and depending on an unmerged external branch is not
  acceptable.
  (b) Skip the reroute and rely on #229 landing first. Rejected: `main` does not have #229's
  commit `e41e2b0`, so skipping it leaves `#218`'s `04000` unfixed on this branch's own diff and
  makes this plan's E2E scenarios unpassable.
- **Rationale:** The duplication is a consequence of the base-branch constraint, not an oversight.
  The two triggers differ — #229 fires on any arity mismatch, this plan on a classified
  non-emittable item (decision [8]) — so whichever merges second needs a small manual
  reconciliation to a single trigger in `mod.rs`. Naming that in the PR body up front makes it a
  planned follow-up rather than a surprise conflict. `#231` tracks the same gap on the join path
  and is untouched here.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] "No fix exists" was unproven — an unexplored alternative led to a verified real fix

- **Finding:** `plan-reviewer` confirmed the round-1 value-lossy physics but found the "no fix
  exists" conclusion UNPROVEN, and named a specific unexplored alternative: because the scan's
  emitting entry point is a SCALAR script, Exasol accepts sibling select-list items beside an
  emitting call, so the adapter could render a TSTZ item as an Exasol-side sibling expression. The
  reviewer had verified this against raw SQL only, not against the real VS `pushdown` response path,
  and required that path to be verified before concluding either way.
- **Direction change:** Path (A) — a real fix. Verifying against the real pushdown path did not just
  test the reviewer's alternative; it overturned the plan's central premise. `EXPLAIN VIRTUAL` and a
  plain query through the deployed VS show that `SELECT CURRENT_TIMESTAMP FROM <vs_t> WHERE ID = 1`
  FAILS today with SQL state `04000` "Expected number of columns is 1 but pushdown query has 5" —
  Exasol validates the pushdown response positionally against the request's `selectList`, so the
  full-base-row decline is an INVALID response, not a correct-but-unaccelerated one. Reproduced for
  the literal branch, the scalar branch, a TSTZ `FN_CAST` over a column, and every item position.
  The repo's own recorded specs already say so (`pushdown-planning-capability-extensions` cites the
  mismatch for #190/#205; `pushdown-planning-grouped-agg/spec.md:132` states it normatively), which
  makes the round-1 claim — and issue #218's own "Impact: Low, correctness is preserved" — wrong.
  The reviewer's sibling shape was then evaluated and rejected on merits: an EMITS call expands to a
  CONTIGUOUS column block, so it cannot position an item between two scan columns, and
  `SELECT CURRENT_TIMESTAMP FROM t` needs exactly one output column while the scan must still emit
  one to drive rows. The chosen fix instead reuses the already-shipped, already-specified qualified
  single-table wrapper, verified end-to-end at the SQL level against the deployed scan UDF (correct
  session-local value; exact TSTZ literal reproduction; arbitrary interleaving; column type
  `TIMESTAMP(3) WITH LOCAL TIME ZONE`). See design decisions [2] and [3].
- **Promotes to ADR:** yes

### [plan-review] The `MUST NOT` prohibition was overclaimed

- **Finding:** Round-1 `decision-log` [2] and the spec delta asserted a normative prohibition on
  "either of the two available repair mechanisms", resting on two `MUST NOT`s that are textually
  scoped to `SELECT * FROM (...)`. Three recorded specs already emit non-star wrappers, and the
  archived `decision [5]` cited as authority says the opposite of the use it was put to and was
  never promoted to an ADR.
- **Direction change:** The overclaim is gone. The `MUST NOT`s are now described accurately: they
  govern the streaming row-scan HAPPY path and are scoped to the literal star form, so they do not
  reach a non-star decline wrapper — which `pushdown-planning-grouped-agg`,
  `pushdown-planning-count-distinct`, and `pushdown-planning-single-group-agg` already emit. The
  archived decision is no longer cited. The happy path stays wrapper-free, which is what those
  `MUST NOT`s actually protect. Rejections of the remaining alternatives now rest on merits:
  positional impossibility for the sibling shape, and global-capability collateral damage for
  capability withdrawal.
- **Promotes to ADR:** no

### [plan-review] No task closed or commented on #218

- **Finding:** No task posted findings to `#218` or disposed of it, risking a silently closed issue
  whose author never learned why.
- **Direction change:** Task 10 comments on `#218` with the disproven "Impact: Low" premise and the
  live `04000` repro, states the fix, and leaves the issue open until the implementing PR closes it
  with `Closes #218`. The PR body also closes `#238` and references `#239` and `#240`.
- **Promotes to ADR:** no

### [plan-review] The adjacent SYSTIMESTAMP defect is now filed, fixed, and cited

- **Finding:** Round-1 decision [7] merely noted that `SYSTIMESTAMP`/`LOCALTIMESTAMP` pass the EMITS
  gate and render to a UTC instant, with no issue and no task — a silent gap. The reviewer also
  required narrowing the claim that the decline is "the only reason CURRENT_TIMESTAMP returns the
  correct value".
- **Direction change:** Verified live and split by function. `SELECT SYSTIMESTAMP FROM <vs_t>`
  returns `16:32:33.665` against a native `18:32:34.061` — filed as `#238`, cited inline in the
  capability-extensions delta, and FIXED by this plan's classifier. `LOCALTIMESTAMP` is NOT affected
  (verified correct; `FN_LOCALTIMESTAMP` is unadvertised) — so the reviewer's premise was half right
  and the plan now states which half. The overclaim it targeted is gone entirely: the decline is not
  why `CURRENT_TIMESTAMP` returns the correct value — `CURRENT_TIMESTAMP` does not return a value at
  all today, it errors. The filter-side instance of the same rendering arm is filed as `#239` and
  cited as a deliberately unfixed tracked exception. See design decision [7].
- **Promotes to ADR:** no

### [plan-review] The real pushdown payload is now captured, not inferred

- **Finding:** Task 1 never captured the real Exasol `pushdown` request JSON, leaving two
  load-bearing premises unverified: that Exasol delegates a projected `CURRENT_TIMESTAMP` at all
  rather than constant-folding it, and that the TSTZ literal node's `value` carries the
  UTC-normalized wall clock.
- **Direction change:** Both premises verified against the real payload, and task 1(f) now carries
  the capture method as a normative step. (a) Exasol DOES delegate it: the `04000` message "Expected
  number of columns is 1" proves a one-item select list containing the expression reached the
  adapter; the contrast case `SELECT SYSTIMESTAMP, LOCALTIMESTAMP, CURRENT_DATE FROM <vs_t>` pushed
  `projection:["ID"]` with an EMPTY select list, showing what non-delegation looks like. (b) The
  value IS UTC-normalized: Exasol's own `filter_expr_string_for_debug` in `EXPLAIN VIRTUAL` renders
  the request for `EVENT_TS > CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME
  ZONE)` around `TIMESTAMP '2024-03-01 09:00:00.000'` — one hour earlier. That same debug string
  also names the canonical repair, `CAST(CONVERT_TZ(TIMESTAMP '<utc>', 'UTC', SESSIONTIMEZONE) AS …)`,
  which the Exasol-dialect literal arm now adopts and which was verified value-exact against the
  native TSTZ value.
- **Promotes to ADR:** no

### [plan-review] Task 1's checks now carry exact SQL and expected output

- **Finding:** Task 1(b) said only that the TSTZ value "displays `10:00:00` while its UTC
  representation is `09:00:00`", and the obvious method — `CAST(<tstz> AS TIMESTAMP)` — returns
  `10:00:00`, the opposite answer, which would have aborted the plan for the wrong reason.
- **Direction change:** All seven task-1 checks now give exact SQL and expected output, including the
  explicit warning that `CAST(<tstz> AS TIMESTAMP)` yields the session-local value and is NOT the
  check to use; only `CONVERT_TZ(<tstz>, SESSIONTIMEZONE, 'UTC')` yields `09:00:00`. Check (d) is
  marked as THE load-bearing one, with the instruction to stop if any of its three queries succeeds.
- **Promotes to ADR:** no

### [plan-review] The E2E regression guard can no longer go vacuous

- **Finding:** The scenario required a non-zero `SESSIONTIMEZONE` offset as a GIVEN, but nothing in
  the E2E setup pins the session zone, so a UTC-defaulting container would make the value assertion
  pass whether or not the defect existed.
- **Direction change:** The scenario now states pinning as a normative step, not a precondition: the
  test sets `ALTER SESSION SET TIME_ZONE = 'EUROPE/BERLIN'`, reads `SESSIONTIMEZONE`, and FAILS
  LOUDLY if the resulting UTC offset is zero, BEFORE any value assertion. Folding in ADVISORY 8, the
  tolerance tightens from 300 s to 60 s and a sharper assertion is added: each value's deviation from
  native must be strictly smaller than the session's UTC offset in seconds, computed from
  `SESSIONTIMEZONE` rather than relying on tolerance width.
- **Promotes to ADR:** no

### [plan-review] Parallelization corrected, and every actionable decision now has a task

- **Finding:** ADVISORY 11 — round-1 Group B listed tasks 2, 3, 4, 5 as parallel although they all
  edit `support.rs` in sequence. ADVISORY 12 — decisions [5] (PR title) and [7] (new issue) had no
  corresponding tasks. ADVISORY 14 — the Summary was a single 60-word sentence.
- **Direction change:** The Parallelization table now has seven groups, with tasks 4, 5, and 6
  strictly sequential (4 and 5 edit the same file; 6 depends on 5's signature) and only tasks 2, 3,
  and 9 genuinely concurrent. Task 10 carries decisions [5] and [7]: it comments on `#218`, and the
  three issues decision [7] requires (`#238`, `#239`, `#240`) are filed with live repros and cited
  inline in the spec deltas. The Summary is now four short sentences.
- **Promotes to ADR:** no

### [plan-review] The PR #229 overlap is now named, and the reroute is deliberately duplicated

- **Finding:** Round 2 found that open PR #229 (`fix/210-string-functions-type-blind`, commit
  `e41e2b0`) already adds nearly this plan's reroute to `mod.rs`'s `RequestShape::RowScan` path,
  and that this plan's Dependencies section said "None".
- **Direction change:** The overlap is documented rather than avoided. This PR's base must be
  `main`, which lacks `e41e2b0`, so it carries its own reroute — skipping it would leave `#218`'s
  `04000` unfixed on this branch's own diff. The Dependencies section now names PR #229 and issue
  #231, states which trigger each side uses, and states that whichever PR merges second needs a
  small manual reconciliation in `mod.rs`. Task 10 puts that in the PR body as a known follow-up,
  not a defect. The trigger here is deliberately NARROWER than #229's: a reason-based predicate
  over the classified select list, not an arity comparison, so it cannot fire on the absent, empty,
  or non-array `selectList` arms where the full base row is correct. See design decisions [8] and
  [11].
- **Promotes to ADR:** no

### [plan-review] The zero-file path was unrouted and still returned `04000`

- **Finding:** `handle_pushdown` short-circuits to `empty_result_sql` at `mod.rs:219-221` before
  `build_dispatch_sql` runs, and that function's `RowScan` arm calls
  `empty_pushdown_sql(proj_cols, proj_types)` with the widened full row. Confirmed live:
  `SELECT CURRENT_TIMESTAMP FROM MY_LAKEHOUSE.EVENTS WHERE ID = 999999` still fails with `04000`.
  A dispatcher-only fix would have shipped a half-fix.
- **Direction change:** Task 6 now routes that arm through the same predicate, emitting
  `empty_select_list_typed_sql` — the helper its own `GroupByWrapper` arm already uses one line
  above (`file_resolution.rs:709`) for exactly this reason. `CAST(NULL AS TIMESTAMP WITH LOCAL TIME ZONE)`
  was verified valid Exasol SQL, and `exasol_type_from_json` already emits that type string, so the
  helper needs no change. A `file_resolution.rs` unit test is the deterministic guard and task 8
  adds an all-pruning E2E case. The capability-extensions delta states the zero-file requirement
  normatively. See design decision [10].
- **Promotes to ADR:** no

### [plan-review] `FN_CAST` to TSTZ is excluded, and a deeper node-name defect was found underneath

- **Finding:** `render_cast_target` returns `Err` for `withLocalTimeZone: true` in BOTH dialects
  before any dialect match (`crates/vs-expression/src/lib.rs:174-178`), and
  `specs/sql-comprehension/vs-expression-translator-cast/spec.md` records that decline normatively,
  so `CAST(<col> AS TSTZ)` never reaches this plan's classifier by the route the plan claimed.
- **Direction change:** Two separate corrections. (1) `FN_CAST` to TSTZ over a COLUMN is now
  explicitly EXCLUDED from the fixed set, removed from the Decision section's "reaches the adapter
  via" list, and marked "NO" in the evidence table. `render_cast_target` and the cast spec are
  untouched, so no fourth spec delta is needed. Such an item IS still routed away from the invalid
  full-row response, because its declared type is TSTZ and the classifier reads
  `selectListDataTypes`; the wrapper's renderer then fails loud and clean —
  `joins/sql_builders.rs:201-207` returns "a select-list item could not be rendered … this is a
  hard error, not a native re-plan". A named adapter error replacing a misleading column-count
  `04000` is an acceptable improvement, is not claimed as a fix, and is the same underlying `04000`
  `#218` already covers, so no redundant issue was opened. (2) Verifying the CONSTANT case exposed
  a distinct defect: Exasol constant-folds `CAST(TIMESTAMP '…' AS TSTZ)` to a BARE literal node
  whose wire type is `literal_timestamputc`, while the translator matches
  `literal_timestamp_utc` — so the arm never matched real traffic. That is fixed here, in the
  Exasol dialect only, because `#218`'s headline repro depends on it. The DataFusion half and the
  identically misspelled Iceberg pruning arm are filed as `#242`. See design decisions [9] and
  [11].
- **Promotes to ADR:** no

### [plan-review] The join-side instance of this gap is deferred to #231, not fixed

- **Finding:** `extract_join_projection` (`joins/rendering.rs:36`) and the empty-side
  short-circuit (`joins/mod.rs:138`) both call `project_columns`, but the plan routed only the
  single-table caller. A broadcast join with a non-emittable select-list item hits the identical
  `04000`.
- **Direction change:** Deliberately not fixed here. Issue `#231` already documents the identical
  mechanism for the broadcast-join path and proposes its fix shape (fall through to
  `build_n_scan_join_sql` on a projection/`selectList` length mismatch), filed independently of
  #229. It is now cited inline in the capability-extensions delta with the `(#NNN)` tracked-exception
  pattern and in the plan's Non-Goals and Dependencies. No duplicate issue was opened. Design
  decision [8] reinforces the boundary structurally: leaving `project_columns`' signature unchanged
  is what keeps both join call sites byte-identical, so the deferral cannot rot into a
  half-migrated path.
- **Promotes to ADR:** no

### [plan-review] The TSTZ literal now has an exact value assertion, and the payload check covers the select list

- **Finding:** The E2E scenario asserted only the two moving now-family values, whose tolerance-based
  checks cannot prove value fidelity for a projected TSTZ literal. Task 1(f) also verified the
  UTC-normalized literal value on the FILTER side only, never as a projected item.
- **Direction change:** Task 8 and the E2E scenario now assert
  `SELECT CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE) FROM <vs_t> WHERE id = 1`
  returns exactly `2024-03-01 10:00:00`, compared against the same session's native
  `CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 09:00:00','UTC',SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)`
  rather than a hardcoded string alone. Task 1 gains sub-checks (f2) and (f3): (f2) pins the wire
  node name and proves the arm is unmatched today from the pushed scan spec; (f3) extends the
  already-verified filter-side finding to the select-list node. `EXPLAIN VIRTUAL` cannot read the
  payload for a projected TSTZ item — the invalid full-row response makes `EXPLAIN VIRTUAL` itself
  fail — so (f3) uses the emittable plain-TIMESTAMP analogue, which showed a bare
  `{"type":"literal_timestamp","value":"2024-03-01 10:00:00.000"}` select-list node with no
  `function_scalar_cast` wrapper. Combined with (f), that establishes the projected constant as a
  bare `literal_timestamputc` node carrying the UTC value: the literal path, which this plan fixes.
  (f3) states the fallback disposition if the folded node does not appear, so the outcome is
  pre-decided either way.
- **Promotes to ADR:** no
