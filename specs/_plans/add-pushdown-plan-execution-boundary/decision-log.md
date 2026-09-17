# Decision Log: add-pushdown-plan-execution-boundary

## Interview

**Q:** Issue #402 frames "EXECUTE ON SCRIPT is separable from SELECT on the VS" as the load-bearing
boundary all future per-user authorization work will build on. Given your instruction to promote to
ADR only genuine architectural decisions, should this boundary get a formal ADR entry in
decision-log.md?

**A:** Yes, promote to ADR. It is explicitly called out as foundational for future OPA and
per-user-identity work, so it is a real architectural constraint rather than an implementation
detail. One tight ADR entry, no essay.

**Q:** The issue's regression risk section flags that someone could later add a convenience
`GRANT EXECUTE ON SCRIPT ... TO <role>` to install.sh, silently breaking this guarantee. The issue's
own proposed-change list (docs + spec + test) does not include a code-level guardrail for that.
Should the plan add one?

**A:** No, docs and test only, as the issue proposes. Stick to the issue's stated three-item scope:
`docs/security.md`, `docs/install.md`, spec plus E2E test. No `install.sh` change, not even a
comment.

## Design Decisions

### [1] Plan visibility and plan execution are separate privileges, guarded by `EXECUTE ON SCRIPT` and the script-scoped connection grant

- **Decision:** `EXECUTE ON SCRIPT` on `LAKEHOUSE_SCAN` and `LAKEHOUSE_DISTRIBUTE_FILES` is the
  boundary this plan states and guards, held by the virtual schema owner alone. The script-scoped
  `GRANT ACCESS ON CONNECTION ... FOR SCRIPT`, also held by the owner, is a second independent gate,
  so running a script against real data needs both grants. `docs/security.md` § Privilege model
  already documents that second gate.
  `revoking_the_owners_scan_grant_denies_the_reader_without_leaking_the_credential` already guards
  it. `GRANT SELECT ON SCHEMA <vs>` confers no script-execute privilege. Any future per-user
  authorization work MUST preserve both gates rather than assume plan secrecy.
- **Alternatives:**
  - Make the plan text unreadable, by obfuscating or encrypting it. Rejected: `EXPLAIN VIRTUAL` is
    the supported pushdown-debugging surface (`docs/debugging-pushdown.md`), and plan secrecy is not
    what makes a predicate enforced.
  - Grant `EXECUTE ON SCRIPT` to querying users for convenience. Rejected: for a grantee that also
    holds script-scoped connection access, it turns every adapter-side predicate advisory, because
    such a user can then run a hand-edited plan.
  - Leave the boundary implicit in the test fixture. Rejected: the fixture withholds the grant from
    the reader but no test reads that fact, so adding a reader to the grant loop breaks nothing.
- **Rationale:** The boundary is the reason adapter-side pushdown is safe at all, and issue #402
  names the exact way it gets lost: an operator closing the undocumented non-DBA owner gap with a
  role-wide `GRANT EXECUTE`. A stated and asserted boundary is also the precondition for OPA-based
  row-level security and per-user catalog identity, both of which would build on it.
- **Consequences:** Granting `EXECUTE ON SCRIPT`, or `EXECUTE ANY SCRIPT`, to a querying user is a
  privilege-model change rather than a convenience. The plan still exposes the table root, the
  bucket layout, file names, byte sizes, and the catalog CONNECTION name to any reader. A sealed
  vended-credential envelope travels in the plan as ciphertext, readable but not openable without
  the CONNECTION password. That exposure is accepted and stated in `docs/security.md` rather than
  left for a reader to discover.
- **Promotes to ADR:** yes

### [2] One new sibling scenario rather than an amendment of the recorded credential-absence scenario

- **Decision:** Add "A least-privilege reader cannot execute the pushdown plan it can read" as a new
  scenario in `e2e-harness/e2e-harness`. Leave the recorded scenario "A least-privilege user queries
  the VS and recovers no credential from the plan" byte-identical.
- **Alternatives:** Amend the recorded scenario with a further `THEN` clause, which is the issue's
  own wording. Rejected: the recorded scenario's subject is what the plan CONTAINS, and its `WHEN`
  is "the reader runs a query and `EXPLAIN VIRTUAL`". Plan execution needs a different `WHEN` and a
  different `GIVEN` clause asserting the absent script-execute privilege, so hosting it would mean
  rewriting a recorded scenario's `GIVEN` and `WHEN` rather than adding to it.
- **Rationale:** The recorded scenario already carries four `THEN`-family clauses and maps to three
  tests.
  A fifth clause about a different property would make the scenario harder to read and would not be
  separately findable. This library holds eight scenarios in this feature already, so a sibling
  scenario is the established shape. Nothing the issue asks for is dropped: the new scenario sits in
  the same feature spec, shares the same provisioning, and its test lands in the same file.
- **Promotes to ADR:** no

### [3] Assert the reader's absent script-execute privilege, not only the denial

- **Decision:** The scenario asserts two facts, not one. It asserts that no `EXECUTE` object
  privilege on the three scripts and no `EXECUTE ANY SCRIPT` system privilege reaches the reader,
  directly or through any role it holds, read from the DBA object-privilege, system-privilege, and
  role-privilege views. It then asserts the denial.
- **Alternatives:** Assert only that submitting the captured plan fails. Rejected: a denial can
  arise from a cause other than the privilege check, and a fixture that later grants the reader
  `EXECUTE` would turn the test green-but-meaningless rather than red.
- **Rationale:** Issue #402 names the regression path as someone adding a reader to a grant loop.
  Only an explicit absence assertion fails on that change. It mirrors the recorded scenario's
  existing "no connection privilege, asserted absent" clause, so the shape is already established in
  this test file.
- **Promotes to ADR:** no

### [4] No `deploy/scripts/install.sh` guardrail

- **Decision:** Leave `deploy/scripts/install.sh` untouched, including its comments.
- **Alternatives:** Add a warning comment beside the two connection grants, or a check that fails on
  a role-wide script-execute grant. Both declined in the interview.
- **Rationale:** The installer issues no `GRANT EXECUTE` today, so there is no existing statement to
  correct. The regression risk is addressed by stating the requirement in `docs/install.md`, which
  is what makes a future installer change a considered one. Because the two files stay deliberately
  unlinked, `docs/install.md` states the operator's obligation ("issue these grants before `CREATE
  VIRTUAL SCHEMA`") and asserts nothing about `deploy/scripts/install.sh`'s content. A documented
  claim about another file's content would silently become false the day that file gains a
  `GRANT EXECUTE`, and it would become false in the direction that matters.
- **Promotes to ADR:** no

### [5] The two documentation changes are traced through the plan's Requirements table

- **Decision:** `docs/security.md` and `docs/install.md` changes are specified in plan.md
  § Requirements as R1 and R2, with one implementing task each.
- **Alternatives:** Encode the documentation obligation as a clause in the `e2e-harness/e2e-harness`
  spec delta. Rejected: that feature governs the E2E harness, and a documentation obligation in it
  would be unverifiable by the harness and misplaced.
- **Rationale:** This spec library has no documentation domain, and creating one for two paragraphs
  would be disproportionate. The Requirements table is the plan template's stated home for
  requirements not captured in a feature spec, and it keeps every task traceable.
- **Promotes to ADR:** no

### [6] `docs/index.md` is left unchanged

- **Decision:** Do not amend the `docs/index.md:30` summary of `docs/security.md`.
- **Alternatives:** Extend the summary from "what a `SELECT`-only Virtual Schema user can and cannot
  read" to also name what such a user cannot execute.
- **Rationale:** The existing summary stays accurate after this change, so the edit would be an
  improvement rather than a correction. The brief scopes the change to the three files the issue
  names, so the edit is left out deliberately rather than overlooked.
- **Promotes to ADR:** no

### [7] The denial wording and the privilege-view spellings are verified live before assertion

- **Decision:** Task 1.2 reads the exact Exasol error text and the applicable DBA privilege views
  from the running container, and records the observed error text, before any assertion is written.
- **Alternatives:** Assert the error text issue #402 quotes ("insufficient privileges for calling
  script") and the view names recalled from Exasol documentation. Rejected by CLAUDE.md
  § Verification discipline, which forbids asserting a claimed SQL or privilege behavior from
  documentation, memory, or a capability registry.
- **Rationale:** The issue's verification was manual and its quoted wording is paraphrased in the
  issue text. An error-text assertion built on a paraphrase fails on the first run or, worse, passes
  on a substring that means something else.
- **Promotes to ADR:** no

### [8] The scenario uses a single-table query shape

- **Decision:** The scenario captures the plan of a single-table projection-and-filter query only,
  and isolates the adapter-generated statement by cell through a new harness helper rather than
  through `explain_virtual_sql`.
- **Alternatives:**
  - Submit `explain_virtual_sql`'s output directly. Rejected: `EXPLAIN VIRTUAL` returns the echoed
    adapter exchange (`getCapabilities` plus the `pushdown` request and response) next to the
    generated SQL, and `explain_virtual_sql`
    (`crates/lakehouse-engine/tests/common/e2e_harness.rs:335-344`) space-joins every cell of every
    column into one blob. That join can also split a JSON token across a cell boundary
    (`crates/lakehouse-engine/tests/e2e_capability_test.rs:455-457`). Submitting the blob yields a
    syntax fault, which is the exact cause the scenario's `THEN` clause forbids.
  - Use a join or a grouped-aggregate shape. Rejected for this scenario: a shape yielding more than
    one pushdown statement needs more than one cell isolated and submitted, which tests the harness
    helper rather than the privilege boundary.
- **Rationale:** The statement must be isolated per cell, because `EXPLAIN VIRTUAL` returns the
  echoed exchange alongside it. A single-table shape keeps that isolation to exactly one statement.
  One statement is enough to prove the boundary, and it reuses the query the sibling tests already
  issue.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] `explain_virtual_sql` returns a flattened blob, not a submittable statement

- **Finding:** round 1 `[UNSTATED_ASSUMPTION]` BLOCKER. The plan assumed `explain_virtual_sql`
  returns a submittable statement. It space-joins the adapter-generated SQL with the echoed adapter
  exchange, and that join can split a JSON token across a cell boundary. Submitting its output
  yields a syntax fault, so the denial assertion would have been a tautology.
- **Direction change:** New task 1.3 adds a harness helper beside `explain_virtual_sql` that returns
  only the adapter-generated statement, selected by cell. Task 1.2 reads the `EXPLAIN VIRTUAL` cell
  layout live before that helper is written. Tasks 1.4, 1.5, and 1.7 consume the helper. The
  scenario `WHEN` clause, decision [8], § Parallelization § Knowledge, and § Impact now state the
  isolation step.
- **Promotes to ADR:** no

### [plan-review] R2 published an unverified grant set and omitted the grant-drop on re-install

- **Finding:** round 1 `[COMPLETENESS_GAP]` BLOCKER. R2 claimed a non-DBA owner needs
  `EXECUTE ON SCRIPT` for all three scripts. The fixture proves that set sufficient, never
  necessary, which CLAUDE.md § Verification discipline forbids. R2 also said nothing about what
  happens to those grants when the installer replaces the scripts, so an operator who re-runs it
  could lose them without warning.
- **Direction change:** Task 1.2 gains a revocation probe: revoke each of the three grants from
  `CREDEXP_OWNER` in turn, record which revocation breaks `CREATE VIRTUAL SCHEMA` or a reader query,
  re-grant between probes. R2 now names only the confirmed set. R2 and task 1.10 state the
  re-install outcome conditionally, on the grant-survival check added by the round-2 finding below.
- **Promotes to ADR:** no

### [plan-review] The re-install clause asserted an unverified script-privilege behavior

- **Finding:** round 2 `[UNSTATED_ASSUMPTION]` BLOCKER. The round-1 fix replaced one unverified
  operator-facing claim with another: R2 asserted that `CREATE OR REPLACE SCRIPT` drops an
  `EXECUTE ON SCRIPT` grant, citing `docs/security.md` § Privilege model as a mirror. That section's
  grant-drop sentence is scoped to `GRANT ACCESS ON CONNECTION` (`docs/security.md:11-18`), and
  nothing in this repository records the behavior for a script privilege. R3 then claimed every
  documented privilege fact was verified live while listing four checks that omitted this one.
- **Direction change:** Task 1.2 gains a fifth live check: re-issue
  `CREATE OR REPLACE RUST SCALAR SCRIPT` for `LAKEHOUSE_SCAN` in the form the harness and the
  installer both use, query the object-privilege view for `CREDEXP_OWNER`'s `EXECUTE` privilege on
  it, record whether the grant survived, restore it if not. R2, R3, task 1.10, § Impact, and the
  § Manual Testing R2 row now state the re-install outcome as whatever that check records, and the
  mirror claim is gone. R3 reads five facts.
- **Promotes to ADR:** no
