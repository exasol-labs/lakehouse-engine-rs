# Plan Review Findings: add-pushdown-plan-execution-boundary (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 8 (Blockers: 2, Advisory: 6)
- Intent Fidelity blockers: 0

## Premortem

Three failure stories, each routed into the taxonomy below.

1. **The guard never guarded.** The implementer runs task 1.6, finds that the captured string is not
   a submittable statement, and settles for `denied["status"] == "error"`. The boundary assertion
   becomes a tautology that passes on a syntax error. Routed to Feasibility BLOCKER 1.
2. **The install note breaks the install.** A non-DBA owner follows the new `docs/install.md` note,
   issues the three `EXECUTE ON SCRIPT` grants, re-runs the installer, and `CREATE VIRTUAL SCHEMA`
   fails, because `CREATE OR REPLACE SCRIPT` dropped the grants and the note never said so. Routed
   to Requirement Quality BLOCKER 2.
3. **The ADR misleads the next feature.** The recorded ADR names `EXECUTE ON SCRIPT` as the single
   enforcement boundary. The later per-user authorization work treats the script-scoped CONNECTION
   grant as a detail, and a role-wide `GRANT ACCESS ON CONNECTION` reopens the path the ADR claimed
   to close. Routed to Requirement Quality ADVISORY 1.

## Intent Fidelity

no objection — axis checked. The three deliverables in issue #402 § Proposed change map one to one
onto R1 (`docs/security.md`), R2 (`docs/install.md`), and the spec delta plus task 1.3. The sibling
scenario serves the issue's intent: the issue asks to "extend the existing least-privilege scenario
in `specs/e2e-harness/e2e-harness/spec.md`", and the delta lands in that same feature, with the test
in the same binary and the same provisioning, so no coverage is deferred. The broader
absent-privilege assertion (decision [3]) closes the regression path the issue itself names in
§ Regression risk, so it is not `[SCOPE_CREEP]`. The declined `install.sh` change and the declined
`docs/index.md` edit both match interview answer 2 and are recorded as decisions [4] and [6], so
neither is a silent `[SCOPE_REDUCTION]`. The no-version-bump call matches the plan's docs-and-test
scope.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER

- Location: plan.md § Implementation Tasks tasks 1.3, 1.4, 1.6; `e2e-harness/e2e-harness/spec.md`
  scenario `WHEN` clause; decision-log.md § Design Decisions [8]
- Issue: the plan assumes `explain_virtual_sql` returns a submittable SQL statement. It does not.
  `EXPLAIN VIRTUAL` returns the adapter-generated SQL **and** a further cell carrying the echoed
  adapter exchange (`getCapabilities` plus the `pushdown` request and response) as a JSON array, and
  `explain_virtual_sql` (`crates/lakehouse-engine/tests/common/e2e_harness.rs:335-344`) flattens
  every cell of every column into one blob. Four independent places in the repository state this:
  the helper's own doc comment ("the generated scan-driving plan plus Exasol's echoed pushdown
  request"), `crates/lakehouse-engine/tests/e2e_capability_test.rs:3023-3033` ("`EXPLAIN VIRTUAL`
  returns the adapter-generated SQL *and* a column carrying the echoed adapter exchange ... as a
  JSON array. `explain_virtual_sql` flattens all of that into one blob"),
  `crates/lakehouse-engine/tests/e2e_non_ascii_identifier_test.rs:106-108`, and
  `crates/lakehouse-engine/tests/e2e_scan_test.rs:1166-1172`.
  `crates/lakehouse-engine/tests/e2e_capability_test.rs:455-457` adds that the space join can split
  a single JSON token across a cell boundary, so the generated SQL is not reliably one whole cell
  either. Submitting the blob verbatim therefore produces a syntax fault, which is exactly the cause
  the spec clause forbids ("rather than an unresolved object, a syntax fault, or a missing CONNECTION
  grant"). Decision [8] rules out multi-statement shapes over the space join but never checks
  whether the single-statement shape yields a submittable string, so the one mitigation the plan
  records does not reach the defect.
- Fix: Add a task before the current 1.3 that isolates the adapter-generated statement. Name the
  extraction concretely: add a harness helper beside `explain_virtual_sql` in
  `crates/lakehouse-engine/tests/common/e2e_harness.rs` that returns only the generated pushdown
  statement for a single-table query, selected by cell rather than by flattening, and state in the
  task that the cell layout of `EXPLAIN VIRTUAL` (column names, cell count, which cell carries the
  statement) is read from the running container in task 1.2 before the helper is written. Rewrite
  tasks 1.3, 1.4, and 1.6 to consume that helper rather than `explain_virtual_sql`. Rewrite the
  scenario `WHEN` clause in `e2e-harness/e2e-harness/spec.md` so it says the reader captures the
  single adapter-generated pushdown statement, isolated from the echoed adapter exchange that
  `EXPLAIN VIRTUAL` returns alongside it, and submits that statement verbatim. Replace
  decision-log.md [8]'s rationale with the real constraint: `EXPLAIN VIRTUAL` returns the echoed
  exchange next to the generated SQL, so the statement must be isolated per cell, and a single-table
  shape keeps that isolation to exactly one statement. Add the new helper to plan.md
  § Parallelization § Knowledge and to plan.md § Impact, which currently claims the change touches
  no code at all.

## Requirement Quality

#### [COMPLETENESS_GAP] BLOCKER

- Location: plan.md § Requirements R2 and R3; plan.md § Implementation Tasks tasks 1.2 and 1.9
- Issue: R2 publishes an operator-facing privilege requirement that no step verifies, and omits the
  one fact that makes the grant break in practice. R2 states "a non-DBA VS owner needs `EXECUTE ON
  SCRIPT` for all three scripts". The fixture at
  `crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs:92-100` proves that granting all
  three is sufficient. It proves nothing about which of the three is necessary. CLAUDE.md
  § Verification discipline is absolute on this point: "A claimed SQL capability gap or limitation
  MUST be verified against a live Exasol system" and "No assumptions about SQL capabilities, syntax,
  or pushdown reachability without checking them against a running Exasol instance." Task 1.2's
  live-verification list covers the denial wording and the DBA view spellings only, so R3 does not
  cover R2. Separately, `docs/security.md` § Privilege model already records that "`CREATE OR REPLACE
  CONNECTION` and `CREATE OR REPLACE SCRIPT` both drop the grant", and
  `deploy/scripts/install.sh:1570-1572` re-issues the two connection grants for that reason. An
  `EXECUTE ON SCRIPT` object grant on a replaced script is dropped the same way, so an operator who
  follows the new note and then re-runs the installer loses the grants with no warning. R2 does not
  state that, and `docs/install.md:175-176` is the exact place an operator reads before
  `CREATE VIRTUAL SCHEMA`.
- Fix: Extend task 1.2 with a fourth live check: revoke each of the three `EXECUTE ON SCRIPT` grants
  from `CREDEXP_OWNER` in turn against the running container, record which revocation breaks
  `CREATE VIRTUAL SCHEMA` and which breaks a reader query, and re-grant after each probe. Then
  rewrite R2 to state only the grant set that check confirms. Add to R2 that re-running the installer
  replaces the scripts and drops these `EXECUTE ON SCRIPT` grants, so an operator MUST re-issue them,
  matching the sentence `docs/security.md` § Privilege model already carries for the connection
  grants. Add that clause to task 1.9's scope.

#### [COMPLETENESS_GAP] ADVISORY

- Location: plan.md § Requirements R1; decision-log.md § Design Decisions [1]; plan.md § Design
  § Decision
- Issue: R1 and the promoted ADR name `EXECUTE ON SCRIPT` as the single enforcement boundary
  ("`EXECUTE ON SCRIPT` on `LAKEHOUSE_SCAN` and `LAKEHOUSE_DISTRIBUTE_FILES` is the enforcement
  boundary for every adapter-side pushdown decision"), and R1 mandates the consequence that granting
  it "to a querying user makes every adapter-side predicate advisory". A second, independent gate
  exists and this repository already tests it. The script-scoped `GRANT ACCESS ON CONNECTION ... FOR
  SCRIPT` grant is held by the owner, not the reader, and
  `revoking_the_owners_scan_grant_denies_the_reader_without_leaking_the_credential`
  (`crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs:185-227`) proves that removing it
  denies the scan. A reader granted `EXECUTE ON SCRIPT` but no connection access still cannot run a
  hand-edited plan against the data, so R1's consequence holds only when the grantee also holds
  connection access. The installer's role-based model
  (`deploy/scripts/install.sh:1570-1572` grants connection access to `$role`) is what makes the
  issue's regression path real, and that is worth naming rather than flattening. The plan's own spec
  clause already distinguishes the two gates ("rather than ... a missing CONNECTION grant"), so the
  artifacts disagree with each other. The ADR outlives this plan and issue #402 states that future
  per-user authorization work builds on it.
- Fix: In plan.md § Requirements R1, add one clause stating that script execution needs both
  `EXECUTE ON SCRIPT` and the script-scoped `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` that
  `docs/security.md` § Privilege model already documents, and qualify the advisory-predicate
  consequence as holding when the grantee holds both. In decision-log.md [1], change "is the
  enforcement boundary" to name `EXECUTE ON SCRIPT` as the boundary this plan states and guards, and
  the script-scoped connection grant as the second independent gate, so the ADR records both.

#### [COMPLETENESS_GAP] ADVISORY

- Location: plan.md § Requirements R1
- Issue: R1's exposure list is "table root, bucket layout, file names, and byte sizes". The plan text
  also carries the catalog CONNECTION name, the `allow_http` flag, and, for a vended-credential
  query, the sealed AES-256-GCM envelope that `docs/security.md` § Sealed vended-credential envelope
  (#378) describes. A section that tells an operator what a reader can read from the plan, placed
  immediately above the sealed-envelope section, invites the question of whether the ciphertext is in
  the plan too. Leaving it out of the list makes the new section read as exhaustive when it is not.
- Fix: In plan.md § Requirements R1, extend the exposure list with the CONNECTION name, and add one
  sentence stating that a sealed vended-credential envelope travels as ciphertext in the plan and is
  readable but not openable without the CONNECTION password, cross-referencing the existing
  `## Sealed vended-credential envelope (#378)` section instead of restating it.

## Task Breakdown

#### [TRACEABILITY_GAP] ADVISORY

- Location: plan.md § Design § Context, § Implementation Tasks task 1.5, § Dead Code Removal;
  plan.md § Parallelization; decision-log.md § Design Decisions [2] and [7]
- Issue: four cross-references point at the wrong place, and an implementer navigates by them.
  (a) `crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs:80-89` is cited three times as
  the script-execute grant loop. Lines 80-89 hold `DROP CONNECTION`, `CREATE USER CREDEXP_OWNER`, and
  the start of the `CREATE SESSION` / `CREATE CONNECTION` / `CREATE VIRTUAL SCHEMA` system-privilege
  loop. The script-execute grant loop is lines 92-100. Task 1.5 tells the implementer that "someone
  adds the reader to the grant loop at `e2e_credential_exposure_test.rs:80-89`", which names the
  wrong loop. (b) decision-log.md [7] says "Task 1.1 reads the exact Exasol error text", and
  § Parallelization says "the test verifies live in task 1.1". The live verification is task 1.2.
  Task 1.1 builds the `.so` and brings the stack up. (c) decision-log.md [2] justifies the sibling
  scenario partly on "the recorded scenario already carries five `THEN` clauses". The recorded
  scenario at `specs/e2e-harness/e2e-harness/spec.md:81-88` carries one `THEN` and three `AND`
  clauses, so the count is four.
- Fix: In plan.md § Design § Context, task 1.5, and § Dead Code Removal, change every
  `e2e_credential_exposure_test.rs:80-89` citation to `e2e_credential_exposure_test.rs:92-100`. In
  decision-log.md [7] and plan.md § Parallelization, change "task 1.1" to "task 1.2". In
  decision-log.md [2], change "five `THEN` clauses" to "four `THEN`-family clauses".

## Design Depth

no objection on module depth, boundaries, or dependency direction — axis checked: the plan adds no
production module, interface, or boundary, and decision [5] keeps the documentation obligation in
plan.md § Requirements rather than inventing a documentation domain for two paragraphs, which
`speq domain list` confirms does not exist. One leakage finding follows.

#### [INFORMATION_LEAKAGE] ADVISORY

- Location: plan.md § Requirements R2; plan.md § Impact
- Issue: R2 makes `docs/install.md` assert the content of another file: it "states that the installer
  does not issue it". Nothing enforces that agreement. Interview answer 2 declined every
  `deploy/scripts/install.sh` change, so the two files are deliberately unlinked. The day
  `install.sh` gains a `GRANT EXECUTE`, the documented claim silently becomes false, and it becomes
  false in the exact direction that matters, because the claim is what tells an operator to issue the
  grant by hand. The plan's § Verification § Manual Testing greps `docs/install.md` only, so no check
  binds the claim to the installer.
- Fix: Add a row to plan.md § Verification § Manual Testing that greps
  `deploy/scripts/install.sh` for `GRANT EXECUTE` and expects no match, naming
  `docs/install.md` as the statement it guards. Alternatively, rewrite R2 to state the requirement
  without asserting the installer's content ("issue these grants before `CREATE VIRTUAL SCHEMA`"),
  and record the reason in decision-log.md [4]. Choose one and keep the plan self-consistent.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY

- Location: `e2e-harness/e2e-harness/spec.md` § Background, the `DELTA:NEW` block lines 14-20
- Issue: seven Background bullets support one scenario, and three of them restate the decision log
  rather than constrain the scenario. Bullet 4 repeats decision [1] and the `WHEN` clause's
  schema-qualification, bullet 5 repeats decision [8], and bullet 6 repeats decision [7] and R3.
  Bullet 2 restates the scenario's own purpose in four sentences. The user's standing instruction is
  to keep specs concise and not a changelog. Bullet 1 follows the recorded `#359` precedent at
  `specs/e2e-harness/e2e-harness/spec.md:31`, so it stays. The rest belongs in the decision log,
  which already holds it.
- Fix: In `e2e-harness/e2e-harness/spec.md` § Background, keep bullet 1 and bullet 3 (the asserted
  absent privilege, which constrains the scenario), compress bullet 2 into one sentence naming why
  the denial matters, and delete bullets 4, 5, and 6, whose content decision-log.md [1], [8], and [7]
  already carry. Target four bullets.

#### [PROSE_BLOAT] ADVISORY

- Location: plan.md § Design § Context, the Goals and Non-Goals bullets (lines 34 and 37)
- Issue: both bullets use an em dash, which `/speq:writing-guardrails` bans in governed prose. The
  table cells at lines 136 and 149 are exempt. Two further prose sentences join two ideas with
  "which": "It exists only as a fixture arrangement at ..., which grants the three script-execute
  privileges to the owner and not to the reader" in § Design § Context, and "An unqualified name
  would fail to RESOLVE instead, which would prove nothing about privileges" in the spec Background
  bullet slated for deletion above.
- Fix: In plan.md § Design § Context, replace the em dash after `**Goals**` and `**Non-Goals**` with
  a colon. Split the "which grants the three script-execute privileges" sentence into two sentences.
