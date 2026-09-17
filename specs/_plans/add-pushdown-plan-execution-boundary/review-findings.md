# Code Review Findings: add-pushdown-plan-execution-boundary

## Summary
- Files reviewed: 4
- Total findings: 4 (standard: 4, expert: 0)

Scope note: reviewed against `notes/A.md` (live evidence), the recorded spec delta
`e2e-harness/e2e-harness/spec.md`, and `crates/lakehouse-engine/src/adapter/pushdown/testdata/dispatch_golden/single_group_row_scan.sql`.
The script-name, denial-wording, DBA-view and privilege-count assertions in the new test are all
grounded in `notes/A.md` §1–§4, and the `docs/install.md` re-install statement is grounded in §5.
The four findings below are the cases where the code or prose outruns that evidence, or where a
layout assumption is encoded without a name or a guard.

## Standard fixes

### crates/lakehouse-engine/tests/common/e2e_harness.rs

#### [CONTEXTLESS_ERROR] `isolated_pushdown_statement` panics without naming the query or the observed layout
- Location: lines 350-357 (`isolated_pushdown_statement`), specifically line 353
- Issue: the function indexes `conn.fetch_result_columns(result_set)[1][0]` directly. The whole
  purpose of the helper is to encode one assumption — that `EXPLAIN VIRTUAL` returns exactly 1 row
  of 4 cells with `PUSHDOWN_SQL` second (`notes/A.md` §1) — yet nothing checks it. If a future
  Exasol version, a multi-table query, or a `NULL` cell breaks that layout, the failure surfaces as
  a bare `index out of bounds` panic that names neither `EXPLAIN VIRTUAL`, the query that produced
  it, nor the expected layout, so the reader of a red E2E run cannot tell a layout change from a
  test bug. `.expect("PUSHDOWN_SQL cell is a string")` states the constraint but neither the query
  nor the value observed. Every neighbouring helper in this file panics with context instead:
  `current_user` (lines 308-315) prints `{cols:?}`, `parse_numeric` (lines 361-365) and `parse_int`
  (lines 369-373) print the offending value, and `fetch_result_columns_with_num_bytes` in
  `common/exasol_ws.rs` panics naming the outstanding row count.
- Fix: In `crates/lakehouse-engine/tests/common/e2e_harness.rs`, rewrite the body of
  `isolated_pushdown_statement` to bind `let cols = conn.fetch_result_columns(result_set);` and,
  before indexing, assert the layout with a message carrying `query_sql` and the observed shape:
  assert `cols.len() == 4` and `cols[0].len() == 1`, panicking with the column count, the row count
  and `query_sql` when either fails. Then replace `.expect("PUSHDOWN_SQL cell is a string")` with
  `.unwrap_or_else(|| panic!("EXPLAIN VIRTUAL PUSHDOWN_SQL cell was not a string for:\n{query_sql}\ngot: {:?}", cols[PUSHDOWN_SQL_COLUMN][0]))`,
  using the constant introduced by the `[MAGIC_NUMBER]` fix below.

#### [MAGIC_NUMBER] The `PUSHDOWN_SQL` cell position is a bare literal `1`
- Location: line 353 (`conn.fetch_result_columns(result_set)[1][0]`), doc comment lines 346-349
- Issue: the column index `1` is the single piece of knowledge this helper exists to own (plan task
  1.3: "This helper is the single place that encodes which `EXPLAIN VIRTUAL` cell carries the
  adapter-generated statement"), and it is written as an unnamed literal. The doc comment carries
  the meaning instead, and does so ambiguously: it says "cell 1 (`PUSHDOWN_SQL`)" and "cell 2" on a
  0-based scale it never states, so "cell 1" reads as the first cell (`PUSHDOWN_ID`) until the
  reader cross-checks the parenthetical column list. The comment also never states what the function
  returns — it is four lines of layout rationale with no statement of the promise.
- Fix: In `crates/lakehouse-engine/tests/common/e2e_harness.rs`, add a module-scope constant beside
  `isolated_pushdown_statement`: `const PUSHDOWN_SQL_COLUMN: usize = 1;` and index with it in place
  of the literal `1`. Then replace the four-line doc comment with a single line that leads with the
  promise and names the cell by name rather than by ordinal, for example:
  `/// Returns the `PUSHDOWN_SQL` cell of `EXPLAIN VIRTUAL`'s single 4-cell row — the complete,
  directly submittable adapter-generated statement, unlike `explain_virtual_sql`'s joined blob.`

### crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs

#### [DUPLICATE_TEST] The filter assertion re-checks the projection and leaves the filter itself unasserted
- Location: lines 261-264
- Issue: `assert!(isolated.contains("filter") && isolated.contains("SCORE"), "isolated statement
  must carry the filter: {isolated}")`. The second conjunct is already guaranteed by the projection
  loop at lines 255-260, which asserts `"SCORE"` (quoted) is present, so it can never independently
  fail and contributes no coverage. What remains is the bare substring `filter`, which proves only
  that the JSON key exists. The assertion's message, plan task 1.5 ("carries ... the filter") and
  spec clause "SHALL carry the table root, the projection, the filter ... as plaintext literals"
  all claim more than the check delivers: a plan whose predicate was dropped down to a different
  column or bound still passes. The golden fixture
  `crates/lakehouse-engine/src/adapter/pushdown/testdata/dispatch_golden/single_group_row_scan.sql`
  shows the serialized shape is `"filter":"(\"REGION\" = ''EU'')"`, so the key and the escaped
  column reference are both assertable literals.
- Fix: In `crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs`, in
  `the_reader_cannot_execute_the_pushdown_plan_it_captured`, replace the single filter assertion at
  lines 261-264 with two assertions: `assert!(isolated.contains(r#""filter":"#), "isolated
  statement must carry a filter key: {isolated}")` and `assert!(isolated.contains(r#"\"SCORE\""#),
  "isolated statement's filter must name the filtered column: {isolated}")`. Drop the
  `isolated.contains("SCORE")` conjunct entirely. Re-run `cargo test --features exasol-e2e --test
  e2e_credential_exposure_test -- --test-threads=1` against the local stack and confirm all four
  tests pass; if the live filter rendering escapes differently from the golden fixture, record the
  observed `"filter"` value in `specs/_plans/add-pushdown-plan-execution-boundary/notes/A.md` and
  assert the observed form instead.

### docs/security.md

#### [OUTDATED_COMMENT] The "or `LAKEHOUSE_DISTRIBUTE_FILES`" claim is unverified and contradicts the script's role
- Location: line 26
- Issue: "Granting `EXECUTE ON SCRIPT` on `LAKEHOUSE_SCAN` **or** `LAKEHOUSE_DISTRIBUTE_FILES`, or
  `EXECUTE ANY SCRIPT`, to a querying user makes every adapter-side predicate advisory for that user
  once they also hold connection access". The disjunction asserts that distributor-execute alone is
  sufficient to defeat an adapter-injected predicate. `LAKEHOUSE_DISTRIBUTE_FILES` is the LUA SET
  script whose whole body re-emits its input file list (`deploy/scripts/install.sh:1362`); it
  resolves no CONNECTION and reads no storage. `docs/security.md` line 11 states only the adapter
  and scan scripts resolve the CONNECTION. A user holding distributor-execute alone still fails the
  `EXECUTE ON SCRIPT` check on `LAKEHOUSE_SCAN`, which is precisely the denial the new test asserts.
  This sufficiency claim is not among the five facts verified live in `notes/A.md`: §4 established
  that all three grants are NECESSARY for the VS owner, which is a different proposition from either
  one being SUFFICIENT for a reader. A security document that names a harmless grant as dangerous
  and states it as verified fact undermines the grants it correctly flags. Project rule
  (CLAUDE.md § Verification discipline) forbids asserting a privilege reachability claim that was
  not run against the live instance.
- Fix: Against the running local stack from task 2.1, probe the claim: as SYS, `GRANT EXECUTE ON
  SCRIPT LHVS.LAKEHOUSE_DISTRIBUTE_FILES TO CREDEXP_READER`, have `CREDEXP_READER` submit the
  isolated `PUSHDOWN_SQL` statement captured per `notes/A.md` §1, record the outcome verbatim, then
  `REVOKE` it and re-confirm the reader's ordinary VS query returns 17 rows. Append the probe and
  its outcome to `specs/_plans/add-pushdown-plan-execution-boundary/notes/A.md` as a sixth section.
  Then rewrite `docs/security.md` line 26 to state only what that probe supports — name
  `LAKEHOUSE_SCAN` (and `EXECUTE ANY SCRIPT`) as the grants that make an adapter-side predicate
  advisory, and state `LAKEHOUSE_DISTRIBUTE_FILES`'s actual role in the plan rather than listing it
  as an independently sufficient grant.

## Expert fixes
[none]
