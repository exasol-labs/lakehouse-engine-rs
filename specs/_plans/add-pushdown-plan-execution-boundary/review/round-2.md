# Plan Review Findings: add-pushdown-plan-execution-boundary (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 4 (Blockers: 1, Advisory: 3)
- Intent Fidelity blockers: 0

## Round-1 Blocker Recheck

- Resolved: [UNSTATED_ASSUMPTION] `explain_virtual_sql` returns a flattened blob, not a submittable
  statement. New task 1.3 (plan.md:140) adds a helper beside `explain_virtual_sql` that "returns only
  the adapter-generated pushdown statement for a single-table query" and selects it "by cell, using
  the layout read in 1.2, rather than by flattening every cell". The cell-selection logic is not
  asserted: task 1.2 (plan.md:139) commits to reading "the column names, the cell count, and which
  cell carries the adapter-generated statement" from the running container, and to recording "the
  observed cell layout ... verbatim in the task notes before writing any assertion". The helper is
  written after that read, not before it. The tautology path is closed on three independent clauses:
  task 1.5 asserts the isolated statement is executable-shaped before any denial assertion, task 1.7
  asserts `denied["exception"]["text"]` names insufficient privilege to call the script using the
  wording captured in 1.2, and spec.md:29 forbids "an unresolved object, a syntax fault, or a missing
  CONNECTION grant" as the denial cause. Tasks 1.4, 1.5, and 1.7 all consume the isolated statement
  rather than `explain_virtual_sql`. The spec `WHEN` clause (spec.md:27) now names the isolation step.
  Decision [8]'s rationale (decision-log.md:157-160) states the real constraint. The helper appears in
  plan.md § Parallelization § Knowledge (plan.md:154) and in plan.md § Impact (plan.md:107-110), which
  no longer claims the change touches no code.

- Resolved: [COMPLETENESS_GAP] R2 published an unverified grant set and omitted the grant-drop on
  re-install. Task 1.2 gained the revocation probe: "revoke each of the three grants from
  `CREDEXP_OWNER` in turn, record whether that revocation breaks `CREATE VIRTUAL SCHEMA` or a reader
  query, and re-grant before the next probe". R2 (plan.md:123) now pre-guesses no grant set. It names
  "only the scripts task 1.2's revocation probe confirms are necessary". R3 (plan.md:124) covers all
  four live checks and states "R2 states only the grant set that last check confirms". The re-issue
  clause reached R2, task 1.10 (plan.md:147), plan.md § Impact (plan.md:104-105), and the § Manual
  Testing R2 row (plan.md:186). One part of this fix carries a new unverified claim of its own, raised
  as the Requirement Quality BLOCKER below. That is a new defect in the fix, not a failure to apply it.

Round-1 advisories, all confirmed applied: ADVISORY 1 (R1 at plan.md:122 and decision [1] at
decision-log.md:25-37 now name both gates), ADVISORY 2 (R1 carries the CONNECTION name and the
sealed-envelope cross-reference), ADVISORY 3 (all three `e2e_credential_exposure_test.rs:92-100`
citations at plan.md:28, 143, and 169 match the real grant loop at lines 92-100; decision [7] and
§ Parallelization say task 1.2; decision [2] and plan.md:90 say "four `THEN`-family clauses", which
matches the recorded scenario's one `THEN` plus three `AND` at `specs/e2e-harness/e2e-harness/spec.md`),
ADVISORY 4 (option B applied: R2 asserts nothing about `deploy/scripts/install.sh`, decision [4]
records why, no grep row was added), ADVISORY 5 (§ Background is four bullets), ADVISORY 6 (both em
dashes are colons at plan.md:43 and 46, the "which grants" sentence is split at plan.md:28-29, and the
only remaining em dashes are two table cells and the pre-existing recorded feature description).

## Premortem

Two failure stories for the revised plan.

1. **The operator re-issues grants that never dropped.** `docs/install.md` tells a non-DBA owner that
   re-running the installer drops the `EXECUTE ON SCRIPT` grants. Nobody checked whether Exasol drops
   an `EXECUTE` object privilege on a `CREATE OR REPLACE`d script. The repository documents that
   behavior only for `GRANT ACCESS ON CONNECTION`. Routed to Requirement Quality BLOCKER.
2. **The necessity probe answers a question it never asked.** Task 1.2 revokes each grant and watches
   for a break in `CREATE VIRTUAL SCHEMA`. The virtual schema already exists from task 1.1, so nothing
   re-invokes the adapter script. The probe records "no break", R2 names too few scripts, and the
   documented grant set is short by one. Routed to Feasibility ADVISORY.

## Intent Fidelity

no objection — axis checked. The three deliverables in issue #402 § Proposed change still map one to
one onto R1, R2, and the spec delta plus tasks 1.4-1.8. The revision added only live-verification work
(task 1.2's fourth check) and one test helper (task 1.3), both traceable to round-1 BLOCKERs and to
CLAUDE.md § Verification discipline, so neither is `[SCOPE_CREEP]`. Interview answer 2 stays honoured:
`grep -n GRANT deploy/scripts/install.sh` confirms the installer issues only two
`GRANT ACCESS ON CONNECTION` statements and one role grant, and decision [4] leaves the file untouched
including its comments. Interview answer 1 is honoured in kind: decision [1] is the single
`Promotes to ADR: yes` entry. Its length is raised as a Prose Quality advisory, not as a drift finding.

## Feasibility

#### [HIDDEN_DEPENDENCY] ADVISORY

- Location: plan.md § Implementation Tasks task 1.2, fourth live check
- Issue: the revocation probe cannot observe its own `CREATE VIRTUAL SCHEMA` criterion as written.
  Task 1.2 says "record whether that revocation breaks `CREATE VIRTUAL SCHEMA` or a reader query".
  Task 1.1 already ran `e2e_credential_exposure_test`, so `CREDEXP_VS` exists before the probe starts.
  `create_virtual_schema_with_password` runs once per test binary behind
  `SETUP_DONE: OnceLock<()>` (`crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs:20` and
  `:55`), so no probe step re-invokes `LAKEHOUSE_ADAPTER` through a fresh `CREATE VIRTUAL SCHEMA`.
  Revoking `EXECUTE ON SCRIPT` on `LAKEHOUSE_ADAPTER` with the schema already created therefore shows
  up, if at all, only through the per-query pushdown call, which is the other criterion. The probe can
  report the adapter grant as unnecessary and R2 then documents a grant set short by one script. The
  probe also has no stated restore step for the schema itself, only for the grants.
- Fix: In plan.md § Implementation Tasks task 1.2, fourth check, state the probe procedure
  explicitly: for each of the three grants, revoke it from `CREDEXP_OWNER`, then run BOTH criteria
  before re-granting. First `DROP VIRTUAL SCHEMA CREDEXP_VS CASCADE` and re-create it as
  `CREDEXP_OWNER` with the same properties the harness uses. Second run the reader's
  projection-and-filter query. Re-grant, re-create the schema if the drop left it absent, and confirm
  the reader query returns `SEED_ROWS_SCORE_GT_15` rows before moving to the next grant.

## Requirement Quality

#### [UNSTATED_ASSUMPTION] BLOCKER

- Location: plan.md § Requirements R2 and R3; plan.md § Impact; plan.md § Verification § Manual
  Testing, Requirement R2 row; plan.md § Implementation Tasks task 1.10; decision-log.md § Review
  Findings, second `[plan-review]` entry
- Issue: the fix for round-1 BLOCKER 2 removed one unverified operator-facing privilege claim and
  added another. R2 now states that `docs/install.md` "adds that `CREATE OR REPLACE SCRIPT` drops an
  `EXECUTE ON SCRIPT` grant, so re-running the installer replaces the scripts and the operator MUST
  re-issue the grants, mirroring the sentence `docs/security.md` § Privilege model already carries for
  the connection grants". The mirrored sentence does not cover this privilege. `docs/security.md:18`
  sits under a `## Privilege model` section whose only subject is
  `GRANT ACCESS ON CONNECTION <conn> FOR SCRIPT` (`docs/security.md:11-16`), and its "the grant" is
  that grant. `deploy/scripts/install.sh:1581` scopes the same statement the same way:
  "CREATE OR REPLACE drops ACCESS grants." Nothing in this repository records what `CREATE OR REPLACE
  SCRIPT` does to an `EXECUTE ON SCRIPT` object privilege, and the E2E fixture cannot show it either,
  because `create_schema_and_scripts` runs at
  `crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs:75` and the grant loop runs after it
  at lines 92-100, never the reverse. Task 1.2's four live checks do not include this one. R3 therefore
  states something false about the plan itself: "Every asserted and documented privilege fact is
  verified live first" followed by a list of four facts that omits this fifth one. CLAUDE.md
  § Verification discipline is the same rule that made round-1 BLOCKER 2 a BLOCKER: "A claimed SQL
  capability gap or limitation MUST be verified against a live Exasol system" and "No assumptions about
  SQL capabilities, syntax, or pushdown reachability without checking them against a running Exasol
  instance." The claim is load-bearing for an operator, because it is the sentence that tells a
  non-DBA owner to re-run three grants after every upgrade.
- Fix: Add a fifth live check to plan.md § Implementation Tasks task 1.2: re-issue
  `CREATE OR REPLACE RUST SCALAR SCRIPT` for `SCHEMA_NAME.LAKEHOUSE_SCAN` against the running
  container, the same statement form
  `crates/lakehouse-engine/tests/common/e2e_harness.rs:174` and `deploy/scripts/install.sh:1354` both
  use, then query the DBA object-privilege view verified in check three for `CREDEXP_OWNER`'s
  `EXECUTE` privilege on that script, and record whether the grant survived. Restore the grant if it
  did not. In plan.md § Requirements R3, change "four facts" to "five facts" and add the grant-survival
  check to the list. In plan.md § Requirements R2, replace the assertion that `CREATE OR REPLACE
  SCRIPT` drops the grant with the outcome that check records, and delete the
  "mirroring the sentence `docs/security.md` § Privilege model already carries" clause, because that
  sentence is scoped to connection grants. Align plan.md § Impact, task 1.10, and the § Manual Testing
  Requirement R2 row with the same wording. Extend the second `[plan-review]` entry in
  decision-log.md § Review Findings with the grant-survival check, so the revision record matches the
  tasks.

## Task Breakdown

no objection — axis checked. Every spec delta clause has an implementing task: spec.md:26 maps to task
1.6, spec.md:27 to tasks 1.3 and 1.4, spec.md:28 to task 1.5, spec.md:29 to tasks 1.7 and 1.8,
spec.md:30 to task 1.8, and spec.md:31 to the `fail not skip` stack waits in `setup_e2e`. R1 maps to
task 1.9, R2 to task 1.10, and R3 to task 1.2. The renumbering is internally consistent: every
cross-reference to the helper task says 1.3, every reference to the live-verification task says 1.2,
and § Verification § Checklist is reached by task 1.11. The single group stays coherent, because all
eleven tasks share the spec delta and the two test files named in § Parallelization § Knowledge, and
the plan states the consolidation reason rather than splitting the documentation tasks off.
`speq plan validate add-pushdown-plan-execution-boundary` returns rc=0 with the one pre-existing
4-AND-step recommendation. `4 passed; 0 failed` in § Manual Testing matches the three `#[test]`
functions the file holds today at lines 121, 149, and 185, plus the new one.

## Design Depth

#### [INFORMATION_LEAKAGE] ADVISORY

- Location: plan.md § Implementation Tasks task 1.3; plan.md § Impact
- Issue: task 1.3 makes the `EXPLAIN VIRTUAL` result-set shape a decision that three modules encode
  independently, and names no owner. `explain_virtual_sql`
  (`crates/lakehouse-engine/tests/common/e2e_harness.rs:335-344`) encodes it as "flatten every cell".
  `explain_virtual_pushdown_request` (`crates/lakehouse-engine/tests/e2e_capability_test.rs:3025`)
  encodes it a second time as "flatten, then find the `pushdownRequest` object". The new helper
  encodes it a third time as "select the statement cell". All three read the same undocumented
  column-and-cell layout of one Exasol statement, and nothing enforces agreement between them. The day
  Exasol changes that layout, three call sites break and only one of them says which cell it wanted.
  This is the back-door leakage class: the plan is the right moment to name the owning module, because
  it is the change that adds the third reader.
- Fix: In plan.md § Implementation Tasks task 1.3, add one sentence naming the new helper the single
  place that encodes which `EXPLAIN VIRTUAL` cell carries the adapter-generated statement, and require
  its doc comment to state the observed layout recorded in task 1.2 as the reason for the selection
  rather than only the helper's purpose. In plan.md § Impact, state that
  `explain_virtual_pushdown_request` in `crates/lakehouse-engine/tests/e2e_capability_test.rs` keeps
  its own flattening path unchanged in this plan, so the duplication is a named and accepted
  trade-off rather than an oversight.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY

- Location: decision-log.md § Design Decisions [1], the `Decision` field (lines 27-37)
- Issue: the only ADR entry in this plan carries ten sentences in one paragraph. The interview answer
  asked for "One tight ADR entry, no essay", and `/speq:writing-guardrails` caps a descriptive
  paragraph at six sentences. Four of the ten sentences restate material the plan already holds
  elsewhere. "The plan text is not a secret" repeats plan.md:59. "`EXPLAIN VIRTUAL` may hand any reader
  the complete plan, including `table_root`, the projection, the filter, the `limit`, and the file list
  as plaintext literals" repeats plan.md:15-18. "What makes an adapter-injected predicate enforced
  rather than advisory is that the reader cannot CALL the scripts" repeats plan.md:26. The `Rationale`
  field then restates the second-gate point a fourth time. One sentence also joins two ideas with
  "and": "`docs/security.md` § Privilege model already documents it, and
  `revoking_the_owners_scan_grant_denies_the_reader_without_leaking_the_credential` already guards it."
- Fix: In decision-log.md § Design Decisions [1], cut the `Decision` field to six sentences. Keep the
  two-gate statement, the `GRANT SELECT ON SCHEMA <vs>` sentence, and the closing obligation on future
  per-user authorization work. Delete the three sentences that repeat plan.md:15-18, plan.md:26, and
  plan.md:59. Split the "already documents it, and ... already guards it" sentence into two sentences.
