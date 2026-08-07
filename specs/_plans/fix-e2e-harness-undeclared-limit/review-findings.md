# Code Review Findings: fix-e2e-harness-undeclared-limit

## Summary
- Files reviewed: 8 (Makefile, `common/exasol_ws.rs`, `e2e_capture_pushdown.rs`, `e2e_count_distinct_test.rs`, `e2e_join_test.rs`, `e2e_lakekeeper_test.rs`, `e2e_harness_row_cap_test.rs`, `docs/debugging-pushdown.md`) plus both spec deltas
- Total findings: 7 (standard: 6, expert: 1)

Verified clean, no findings needed: the default flip (`connect_inner` → `0`), the deletion of
`unbounded_result_sets` and all six call sites (`grep` over the whole repo returns zero surviving
references outside plan artifacts and the not-yet-merged permanent lakekeeper spec, which has a
`DELTA:CHANGED` filed for it), the deletion of the stale `e2e_join_test.rs:113-117` comment, the
`Makefile` registration of `e2e_harness_row_cap_test`, `DEFAULT_FETCH_NUM_BYTES` naming the former
magic number with a genuinely non-obvious soft-budget rationale, and the mid-implementation
correction of the disproven pushdown-`limit` premise — the corrected `capped_result_sets` doc
comment, `declared_cap_truncates_delivered_result_set_not_pushdown_request`, and the
`e2e-harness/e2e-harness` Background bullet and scenario all agree with `injection-surface.md`'s
measurement and with each other. `undeclared_cap_pushes_no_limit`'s `contains("\"limit\"")` needle
was checked against the existing convention (`e2e_scan_test.rs:1171`, `:3666` match the same
unescaped form on raw `explain_virtual_sql` output), so it is a real assertion, not a vacuous one.

## Addendum (second correction, post-dates this review)

This review's "Verified clean" summary above states that the mid-implementation correction of the
pushdown-`limit` premise — the corrected `capped_result_sets` doc comment,
`declared_cap_truncates_delivered_result_set_not_pushdown_request`, and the `e2e-harness/e2e-harness`
Background bullet and scenario — "all agree with `injection-surface.md`'s measurement and with each
other." That was accurate at the time this review ran: everything it checked was internally
consistent. It is **no longer accurate as a statement of the underlying fact**, not merely stale —
a direct capture of the REAL adapter request (not `EXPLAIN VIRTUAL`, which this review's cited
measurement relied on exclusively) subsequently confirmed a declared cap DOES reach the adapter as
a pushdown `limit`, including for the broadcast-join shape. See `decision-log.md`'s second
correction entry for the full evidence and what changed as a result: `capped_result_sets`'s doc
comment, the `e2e_join_test.rs` comment, `declared_cap_truncates_delivered_result_set_not_pushdown_request`
(renamed again, to `declared_cap_truncates_returned_row_count`), and both spec deltas were all
corrected a second time.

This entry is left in place rather than edited, per this project's paper-trail convention — the
finding it records (internal consistency at review time) is still true; only the premise it was
checked against has since changed. The test name in the SHRINKABLE finding below
(`crates/lakehouse-engine/tests/e2e_harness_row_cap_test.rs`, "Unexplained backslash-stripping
before the `limit` check") also refers to the pre-second-correction test name and the
`capped_plan`/scan-spec comparison it made, both of which no longer exist in that form — that
finding's own fix was applied, then superseded by the later restructure, not left unresolved.

## Standard fixes

### crates/lakehouse-engine/tests/common/exasol_ws.rs

#### [MISSING_DESIGN_INTENT] `capped_result_sets` states the measured fact but no longer says when to declare a cap
- Location: lines 144-146
- Issue: `plan.md:105-107` designates this doc comment as the single documented home for the cap
  decision, and specifies two halves: the mechanism, *and* "the method is for tests whose assertion
  is about that capped plan". The mid-implementation correction rewrote the mechanism half
  correctly (a cap truncates delivery, it does not reach the adapter) but dropped the
  when-to-use-it half entirely. What is left is a purely negative statement — the method changes
  nothing an adapter can see — which leaves a reader at a call site with no answer to the only
  question the opt-in inversion was meant to make legible: why is this cap here? Both present-day
  callers have a stated purpose (decision-log `[2]`): `e2e_capture_pushdown`'s reproducible
  capped-versus-uncapped comparison, and an assertion about result-set truncation itself.
- Fix: In `crates/lakehouse-engine/tests/common/exasol_ws.rs`, extend `capped_result_sets`'s doc
  comment with one sentence stating when a call site should declare a cap: for a test whose
  assertion is about result-set truncation at row-delivery time, and for
  `e2e_capture_pushdown`'s `CAPTURE_RESULT_SET_MAX_ROWS` capped-versus-uncapped comparison — and
  that a test asserting pushdown or plan shape needs no cap, because a declared cap does not change
  either. Keep the existing measured-behavior sentences and the `docs/debugging-pushdown.md`
  cross-reference unchanged.

#### [SHRINKABLE] One of three panic paths closes the result-set handle; the other two do not
- Location: line 230
- Issue: `fetch_result_columns_with_num_bytes` calls `self.close_result_set(handle)` immediately
  before the zero-rows `panic!` (line 230), but the two sibling panic paths in the same loop — the
  missing-`responseData.numRows` `unwrap_or_else` (lines 223-228) and the missing-`responseData.data`
  `unwrap_or_else` (lines 238-243) — panic without closing. The close does no work on any of them:
  `impl Drop for ExaConn` (lines 315-319) closes the WebSocket, which ends the session and releases
  the result set, and a panic in a `#[test]` fn is terminal. So line 230 is an unexplained
  asymmetry that reads as if the zero-rows case needed cleanup the others do not.
- Fix: In `crates/lakehouse-engine/tests/common/exasol_ws.rs`, delete the
  `self.close_result_set(handle);` call on line 230 so all three panic paths in
  `fetch_result_columns_with_num_bytes` behave alike. Leave the successful-completion
  `close_result_set` call after the loop (line 259) in place.

### crates/lakehouse-engine/tests/e2e_capture_pushdown.rs

#### [OUTDATED_COMMENT] Module doc still claims the binary is driven entirely by `CAPTURE_SQL`
- Location: lines 14-15
- Issue: the module doc reads "Driven entirely by the `CAPTURE_SQL` env var so future issues on this
  stack … can reuse it without editing this file." The diff adds a second input,
  `CAPTURE_RESULT_SET_MAX_ROWS` (lines 51-56), so the claim is now false. Decision-log `[8]` chose
  the env-var seam precisely *because* that sentence promises a no-edit reuse contract; leaving the
  sentence enumerating one variable makes the new knob invisible to the next reader of this file.
- Fix: In `crates/lakehouse-engine/tests/e2e_capture_pushdown.rs`, update the module doc's
  "Driven entirely by the `CAPTURE_SQL` env var" sentence to name both env vars — required
  `CAPTURE_SQL`, and optional `CAPTURE_RESULT_SET_MAX_ROWS` (unset means no declared row cap) —
  keeping the existing reuse-without-editing rationale and the `docs/debugging-pushdown.md` pointer.

#### [BROAD_CATCH] A malformed `CAPTURE_RESULT_SET_MAX_ROWS` silently produces an uncapped capture
- Location: lines 51-56
- Issue: `Err(_) => conn` catches every `std::env::VarError`, not just `NotPresent`. A
  `CAPTURE_RESULT_SET_MAX_ROWS` whose value is not valid Unicode yields `VarError::NotUnicode` and
  is silently treated as "no cap declared" — the operator asked for a capped capture and gets an
  uncapped one with no diagnostic. That is the same operator-invisible cap decision this plan exists
  to remove, reintroduced on the error path. The sibling failure is loud but contextless: the parse
  panic message `"CAPTURE_RESULT_SET_MAX_ROWS must be a u32"` names the constraint but not the
  offending value, so an operator debugging a shell-quoting mistake cannot see what was received.
- Fix: In `crates/lakehouse-engine/tests/e2e_capture_pushdown.rs`, replace the
  `match std::env::var("CAPTURE_RESULT_SET_MAX_ROWS")` arms so only
  `Err(std::env::VarError::NotPresent)` means "declare no cap", and any other `Err` panics naming
  the variable and the error. Change the parse failure to `unwrap_or_else` with a message that
  includes the received value, e.g. `panic!("CAPTURE_RESULT_SET_MAX_ROWS must be a u32, got {n:?}")`.

### crates/lakehouse-engine/tests/e2e_harness_row_cap_test.rs

#### [SHRINKABLE] Unexplained backslash-stripping before the `limit` check, inconsistent with the sibling test
- Location: lines 116-121
- Issue: `declared_cap_truncates_delivered_result_set_not_pushdown_request` builds
  `let capped_plan_unescaped = capped_plan.replace('\\', "");` before asserting the plan carries no
  `"limit"`. `undeclared_cap_pushes_no_limit` forty lines above (line 75) checks the same needle
  against the raw `explain_virtual_sql` output with no unescaping, and so do the two pre-existing
  assertions of this kind in the repo (`e2e_scan_test.rs:1171`, `:3666`). The extra step is provably
  redundant here: this same test asserts `capped_plan == uncapped_plan` (line 122), and
  `undeclared_cap_pushes_no_limit` matches the unescaped form successfully on the identical
  statement shape, so both variants of the plan text already contain real double quotes. What is
  left is an unexplained transformation on the review path — exactly the kind of non-obvious step
  that must be either removed or justified, and here it should be removed.
- Fix: In `crates/lakehouse-engine/tests/e2e_harness_row_cap_test.rs`, delete the
  `capped_plan_unescaped` binding and assert `!capped_plan.contains("\"limit\"")` directly, matching
  `undeclared_cap_pushes_no_limit` at line 75. Keep the assertion's failure message unchanged.

### docs/debugging-pushdown.md

#### [OUTDATED_COMMENT] The broadcast-join row states a flat conclusion the cited evidence explicitly bounds
- Location: lines 66-81 (headline and the `broadcast-eligible inner equi-join` table row, line 81)
- Issue: the operator-facing table row reads "No — the broadcast join plan is unaffected; a declared
  cap does not disqualify broadcast pushdown", stated as established fact. The evidence artifact
  this section cites bounds that exact claim: `injection-surface.md` § "Observation boundary" states
  that the capture reads the adapter exchange out of `EXPLAIN VIRTUAL`, that `resultSetMaxRows` is
  an attribute of the `EXPLAIN VIRTUAL` wrapper rather than the inner `SELECT`, and that "this
  measurement cannot exclude that on shape 7 alone, because a broadcast join and the two-scan
  fallback return the same six rows". Controls c3/c6 bound it for the row-scan and aggregate shapes
  but not for the join. The `e2e-harness/lakekeeper-e2e-harness` spec delta hedges correctly ("was
  never shown to reach the adapter as a pushdown `limit` or to suppress broadcast eligibility");
  this doc, which decision-log `[5]` designates as the permanent home for the measurement, does not.
  Because #307 is precisely about a `limit` suppressing broadcast pushdown, an operator reading this
  row would treat an unbounded conclusion as settled.
- Fix: In `docs/debugging-pushdown.md`, reword the `broadcast-eligible inner equi-join` row (line 81)
  to the hedged form used in the lakekeeper spec delta — a declared cap was never *shown* to reach
  the adapter or suppress broadcast eligibility, with the broadcast block still emitted and no
  `LHS_T0` wrapper at caps of `5` and `10000`. Then add one sentence after the controls paragraph
  (after line 87) stating the observation boundary: the capture observes the adapter exchange
  through `EXPLAIN VIRTUAL`, where `resultSetMaxRows` applies to the wrapper statement, so a `limit`
  reaching only a directly-executed statement's pushdown request is excluded by result-value
  controls for the row-scan and aggregate shapes but not by the echo for the join shape.

## Expert fixes

### crates/lakehouse-engine/tests/common/exasol_ws.rs

#### [TACTICAL_SHORTCUT] The inline-`data` branch still returns a prefix, so "read to completion" holds on only one of two paths
- Location: lines 203-214 (and the doc comment at lines 189-197)
- Issue: `fetch_result_columns_with_num_bytes` returns the inline `data` array before it ever reads
  `numRows` (line 214 is reached only when `data` is absent). The handle path was made rigorous —
  it loops, panics on a zero-row response with rows outstanding, and asserts every column's
  accumulated length equals `numRows` — while the inline path performs no completeness check at
  all. Three artifacts promise otherwise:
  - this method's own doc comment (lines 193-197): "Every way of reading short — a truncated read, a
    response that carries no rows while rows remain, a response whose payload is missing or changes
    shape mid-read — panics naming the outstanding count";
  - the `e2e-harness/e2e-harness` spec delta's third scenario, normatively: "the helper SHALL issue
    successive `fetch` requests until the rows it has accumulated reach the count the result-set
    metadata reports in `numRows`" and "SHALL return exactly that row count";
  - `plan.md` § Design → Patterns, "Read to completion … Correctness no longer depends on an
    upstream cap bounding the result".
  This was raised during planning and not carried into the implementation: `review/round-1.md:174-187`
  ([COMPLETENESS_GAP] ADVISORY) names the branch order, the `numRowsInMessage < numRows` shape, and
  the fix, and `tasks.md` task 2.2 was never extended to cover it — so the shortcut also has no
  follow-up recorded anywhere. Note the scope honestly: `injection-surface.md` § "Rows-per-fetch-response
  measurement" records that the 30,000-row scan "came back as a `resultSetHandle` (not inlined)", and
  `harness_reads_high_cardinality_result_set_to_completion`'s `responses >= 2` assertion would fail
  outright if this server ever attached inline `data` to a handle-backed result set, so this is not a
  live wrong-result bug on Exasol 2025.2.1. It is the identical defect class decision-log `[4]` used to
  justify Phase 2 in the first place — the reader's correctness resting on an upstream property of the
  server rather than on the reader — left standing on the branch the fix did not touch, behind a doc
  comment and a normative spec clause that both claim it was closed.
- Fix: In `crates/lakehouse-engine/tests/common/exasol_ws.rs`, restructure
  `fetch_result_columns_with_num_bytes` so no path can return a prefix. Read
  `let advertised = result_set["numRows"].as_u64();` before branching. When
  `result_set["resultSetHandle"]` is present, stop returning early on inline `data`: seed `cols` and
  `rows_read` from the inline array when one is present and then enter the existing loop, so a
  partial inline chunk alongside a handle is the first chunk rather than the whole answer. When no
  handle is present, keep returning the inline columns but first assert every column's length equals
  `advertised`, in a message naming both the advertised and the accumulated count — and guard that
  assert on `advertised.is_some()` so DDL and other responses that omit `numRows` keep working
  across the twelve existing `fetch_result_columns` call sites (`e2e_scan_test.rs`,
  `e2e_capability_test.rs`, `e2e_int96_timestamp_test.rs`, `e2e_count_distinct_test.rs`,
  `common/e2e_harness.rs`; re-derive the list with Serena's `find_referencing_symbols` rather than
  trusting this list). A failing-test-first cycle is not available for the inline-plus-handle shape —
  this server never produces it, which is why the gap is invisible today — so treat the added
  assertion as the guard and verify no call site regressed by running `make test-e2e` and
  `make test-e2e-lakekeeper` to a zero exit code, checking exit codes rather than output tails.
  Leave the method's doc comment's completeness claim as written; the restructure is what makes it
  true.
