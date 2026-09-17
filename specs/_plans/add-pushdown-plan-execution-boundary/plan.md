# Plan: add-pushdown-plan-execution-boundary

Closes [#402](https://github.com/exasol-labs/lakehouse-engine-rs/issues/402).

## Summary

State in the operator docs that the pushdown plan is readable by any virtual-schema reader but
executable only by a principal holding `EXECUTE ON SCRIPT`. Guard that split with one E2E scenario
asserting a reader who isolates the adapter-generated statement and resubmits it verbatim is denied.

## Design

### Context

Every adapter-side pushdown decision reaches the engine as SQL text. `EXPLAIN VIRTUAL` returns that
text in full to any user holding `SELECT` on the virtual schema. The plan carries `table_root`, the
projection, the filter, the `limit`, and the per-shard file list as plaintext literals
(`crates/lakehouse-engine/src/adapter/pushdown/testdata/dispatch_golden/single_group_row_scan.sql`).
A reader can therefore read the exact filter the adapter injected on its behalf.

What stops that reader from editing and rerunning the plan is a separate privilege.
`crates/lakehouse-engine/src/adapter/pushdown/mod.rs:275-279` schema-qualifies `LAKEHOUSE_SCAN` and
`LAKEHOUSE_DISTRIBUTE_FILES`, so the captured statement is self-contained and resolvable. Calling
either script needs `EXECUTE ON SCRIPT`, which `GRANT SELECT ON SCHEMA <vs>` does not confer.

That privilege split is the reason an adapter-injected predicate is enforced rather than advisory.
Today it appears in no document and in no assertion. It exists only as a fixture arrangement at
`crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs:92-100`. That loop grants the three
script-execute privileges to the owner and not to the reader. No test fails if a reader joins that
loop. `deploy/scripts/install.sh` issues no `GRANT EXECUTE` at all, so a non-DBA VS owner needs a
grant that no document names. An operator closing that undocumented gap with a role-wide
`GRANT EXECUTE ON SCRIPT` would silently make every pushed predicate advisory.

Capturing the plan needs one new test helper. `EXPLAIN VIRTUAL` returns the adapter-generated SQL
next to a further cell carrying the echoed adapter exchange (`getCapabilities` plus the `pushdown`
request and response) as a JSON array. `explain_virtual_sql`
(`crates/lakehouse-engine/tests/common/e2e_harness.rs:335-344`) space-joins every cell of every
column into one blob. That join can split a JSON token across a cell boundary
(`crates/lakehouse-engine/tests/e2e_capability_test.rs:455-457`). Submitting the blob verbatim
produces a syntax fault, not a privilege denial, so the scenario needs the generated statement
isolated by cell.

- **Goals**: State the read/execute split in `docs/security.md`. Name the owner's missing
  `EXECUTE ON SCRIPT` requirement in `docs/install.md`. Turn the one-off live verification in issue
  #402 into a regression guard. Name what the plan does still expose to a reader.
- **Non-Goals**: No production code change. No `deploy/scripts/install.sh` change, not even a
  comment. No per-user authorization, row-level security, or per-user catalog identity. No change to
  the plan's content, verbosity, or readability.

### Decision

Treat plan visibility and plan execution as two privileges, and guard the second one.
`EXECUTE ON SCRIPT` on the scan and distributor scripts is the boundary this plan states and
guards, held by the virtual schema owner alone. A second independent gate exists: the script-scoped
`GRANT ACCESS ON CONNECTION ... FOR SCRIPT` that `docs/security.md` § Privilege model already
documents and
`revoking_the_owners_scan_grant_denies_the_reader_without_leaking_the_credential`
(`crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs:185-227`) already guards. Plan text
is not a secret. Plan execution is the guarded act.

#### Architecture

```
reader (CREATE SESSION + SELECT ON SCHEMA <vs>)
  │
  ├─ SELECT ... FROM <vs>.<table> ──▶ engine invokes the scripts as the VS OWNER ──▶ rows
  │
  ├─ EXPLAIN VIRTUAL ─────────────▶ full plan text                      ALLOWED (not a secret)
  │                                    │
  │                                    ▼ resubmit verbatim, or hand-edited
  └────────────────────────── EXECUTE ON SCRIPT check ──────────────────▶ DENIED

owner (EXECUTE ON SCRIPT on adapter + scan + distributor) ───────────────▶ ALLOWED
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Assert the absence of a privilege, not only its arrangement | `e2e_credential_exposure_test.rs` | A fixture that merely withholds a grant breaks no test when a later grant is added |
| Positive denial assertion over documentation of intent | new E2E scenario | The issue's live verification was manual and one-off, so only a test keeps it true |
| Statement of an accepted exposure beside the guarded one | `docs/security.md` | Table root, bucket layout, file names, byte sizes, and the CONNECTION name stay readable, and an operator must know that |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| `EXECUTE ON SCRIPT` is the boundary this plan states and guards, alongside the script-scoped connection grant as a second independent gate | Leave it implicit in the test fixture | A fixture arrangement no test reads is not a guarantee, and issue #402 names the exact way it gets lost |
| Plan text stays readable | Obfuscate or encrypt the plan text | `EXPLAIN VIRTUAL` is the supported pushdown-debugging surface (`docs/debugging-pushdown.md`), and plan secrecy is not what makes a predicate enforced |
| One new sibling scenario | Amend the recorded credential-absence scenario | The recorded scenario asserts plan CONTENT and already carries four THEN-family clauses. Plan EXECUTION needs a different WHEN, so amending it would rewrite a recorded clause for no gain |
| No installer guardrail | Add a warning comment or a check to `deploy/scripts/install.sh` | Declined in the interview. The installer issues no `GRANT EXECUTE` today, so there is nothing to guard against in it yet |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| e2e-harness/e2e-harness | CHANGED | `specs/_plans/add-pushdown-plan-execution-boundary/e2e-harness/e2e-harness/spec.md` |

## Impact

Operators gain two statements they cannot read anywhere today. `docs/security.md` states that
granting `EXECUTE ON SCRIPT` on the scan or distributor script, or `EXECUTE ANY SCRIPT`, to a
querying user makes every adapter-side predicate advisory. `docs/install.md` names the
`EXECUTE ON SCRIPT` grants a non-DBA VS owner needs, and states whether re-running the installer
drops them, per task 1.2's grant-survival check.

No breaking change. No behavior change. No production code change. Test code changes: one new test
plus one new helper beside `explain_virtual_sql` in
`crates/lakehouse-engine/tests/common/e2e_harness.rs` that isolates the adapter-generated statement
from the echoed adapter exchange. `explain_virtual_pushdown_request` in
`crates/lakehouse-engine/tests/e2e_capability_test.rs` keeps its own separate flattening path
unchanged in this plan. That duplication is an accepted trade-off rather than an oversight: it
extracts the echoed wire payload, not the generated statement, so folding the two together would
widen the new helper's contract for no assertion this plan adds.

**No version bump.** This plan ships documentation and one E2E test. Nothing in the shipped `.so`,
the scripts, or the installer changes, so the release version stays as it is.

## Requirements

The two documentation changes carry no feature spec, because this library has no documentation
domain. They are traced here instead.

| Requirement | Details |
|-------------|---------|
| R1: `docs/security.md` states the visibility/execution split | A new section states: the plan is readable by any VS reader and is not a secret; safety comes from `EXECUTE ON SCRIPT` being separable from `SELECT` on the virtual schema; running a script against real data needs BOTH `EXECUTE ON SCRIPT` AND the script-scoped `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` that `## Privilege model` already documents, so the two grants are two independent gates; granting `EXECUTE ON SCRIPT` on `LAKEHOUSE_SCAN` or `LAKEHOUSE_DISTRIBUTE_FILES`, or `EXECUTE ANY SCRIPT`, to a querying user makes every adapter-side predicate advisory when that user also holds connection access, because that user can then run a hand-edited plan. It names what the plan still exposes to a reader: table root, bucket layout, file names, byte sizes, and the catalog CONNECTION name. It adds one sentence stating that a sealed vended-credential envelope travels in the plan as ciphertext, readable but not openable without the CONNECTION password, cross-referencing `## Sealed vended-credential envelope (#378)` rather than restating it. |
| R2: `docs/install.md` names the owner's `EXECUTE ON SCRIPT` grants | The existing non-DBA note beside `CREATE VIRTUAL SCHEMA` gains the `EXECUTE ON SCRIPT` requirement, naming only the scripts task 1.2's revocation probe confirms are necessary, and states that the operator issues these grants before `CREATE VIRTUAL SCHEMA`. It states the re-install outcome that task 1.2's grant-survival check records: if `CREATE OR REPLACE SCRIPT` drops the grant, the note MUST tell the operator to re-issue these grants after re-running the installer; if the grant survives, the note MUST say so, so the operator does not re-issue grants that never dropped. `docs/security.md` § Privilege model's existing grant-drop sentence is scoped to `GRANT ACCESS ON CONNECTION` and is evidence for neither outcome. |
| R3: Every asserted and documented privilege fact is verified live first | Task 1.2 reads five facts from the running container, per CLAUDE.md § Verification discipline: the `EXPLAIN VIRTUAL` cell layout, the exact Exasol denial text, the DBA privilege-view spellings, which of the three `EXECUTE ON SCRIPT` grants are necessary, and whether an `EXECUTE ON SCRIPT` grant survives `CREATE OR REPLACE SCRIPT`. R2 states only the grant set the fourth check confirms and only the re-install outcome the fifth check records. |

## Dependencies

The new scenario needs the local Exasol Docker stack with MinIO and the Iceberg REST catalog, the
same stack every other `test-e2e` binary needs. It also needs a `.so` built by `make
cross-udf-build` before the first run, because `setup_e2e()` uploads a pre-built artifact.
`e2e_credential_exposure_test` is already listed in the `test-e2e` make target (`Makefile:82`), so
no Makefile change is needed.

## Implementation Tasks

### 1. Plan-execution boundary: documentation and E2E guard

- [ ] 1.1 Run `make cross-udf-build`, bring up the local stack, and run `e2e_credential_exposure_test` once so the SLC, the `.so`, the scripts, and the two users exist. The `.so` must be built before any live check, because `setup_e2e()` uploads a pre-built artifact rather than building one.
- [ ] 1.2 Verify live, against that running container, the five facts the plan depends on. Record the observed cell layout, the observed error text, and each probe outcome verbatim in the task notes before writing any assertion or documentation sentence.
    - First, the cell layout of `EXPLAIN VIRTUAL` for a single-table projection-and-filter query: the column names, the cell count, and which cell carries the adapter-generated statement.
    - Second, the exact error text Exasol returns when `READER_USER` submits that isolated statement.
    - Third, the DBA views that report a user's `EXECUTE` object privilege on a script, the `EXECUTE ANY SCRIPT` system privilege, and the roles a user holds.
    - Fourth, which `EXECUTE ON SCRIPT` grants are NECESSARY. For each of the three grants: revoke it from `CREDEXP_OWNER`, then run BOTH criteria before re-granting. First `DROP VIRTUAL SCHEMA CREDEXP_VS CASCADE` and re-create it as `CREDEXP_OWNER` with the same `VsProps` the harness setup uses, then re-issue `GRANT SELECT ON SCHEMA CREDEXP_VS TO CREDEXP_READER`, which the `CASCADE` drops. Second run the reader's projection-and-filter query. Re-grant, re-create the schema if the drop left it absent, and confirm the reader query returns `SEED_ROWS_SCORE_GT_15` rows before moving to the next grant. The re-create is what makes the first criterion observable: `CREDEXP_VS` already exists from task 1.1, so a probe that leaves it standing never re-invokes `LAKEHOUSE_ADAPTER` and would report the adapter grant as unnecessary.
    - Fifth, whether `CREATE OR REPLACE SCRIPT` drops an `EXECUTE ON SCRIPT` grant. Re-issue `CREATE OR REPLACE RUST SCALAR SCRIPT` for `SCHEMA_NAME.LAKEHOUSE_SCAN`, in the statement form `crates/lakehouse-engine/tests/common/e2e_harness.rs:174` and `deploy/scripts/install.sh:1354` both use, then query the object-privilege view from the third check for `CREDEXP_OWNER`'s `EXECUTE` privilege on that script. Record whether the grant survived, and restore it if it did not. No file in this repository records this behavior for a script privilege: `docs/security.md:18` scopes its grant-drop sentence to `GRANT ACCESS ON CONNECTION`, and the E2E fixture creates the scripts before it grants, never the reverse.
- [ ] 1.3 Add a helper beside `explain_virtual_sql` in `crates/lakehouse-engine/tests/common/e2e_harness.rs` that returns only the adapter-generated pushdown statement for a single-table query. Select the statement by cell, using the layout read in 1.2, rather than by flattening every cell. `explain_virtual_sql` space-joins the generated SQL with the echoed adapter exchange and can split a JSON token across a cell boundary, so its output is not a submittable statement. This helper is the single place that encodes which `EXPLAIN VIRTUAL` cell carries the adapter-generated statement, so its doc comment MUST state the cell layout recorded in 1.2 as the reason for that selection rather than only the helper's purpose.
- [ ] 1.4 Add `the_reader_cannot_execute_the_pushdown_plan_it_captured` to `crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs`. Take the serial guard and call `setup_e2e()` like every sibling test. Capture the isolated statement of the existing single-table projection-and-filter query with the 1.3 helper against `reader_conn()`.
- [ ] 1.5 Assert the isolated statement is executable-shaped before asserting the denial: it names the scan and distributor scripts qualified by `SCHEMA_NAME`, and carries the table root, the projection columns, the filter, and a `VALUES` file list. Without this, a denial could come from an unresolved name rather than from a privilege check.
- [ ] 1.6 Assert the reader's absent script-execute authority from the views verified in 1.2: zero rows for the reader's `EXECUTE` privilege on `ADAPTER_SCRIPT_NAME`, `SCAN_SCRIPT_NAME`, and `DISTRIBUTOR_SCRIPT_NAME`, zero rows for `EXECUTE ANY SCRIPT`, and zero roles held by the reader, so a privilege reaching it through a role is covered too. This is the clause that fails when someone adds the reader to the grant loop at `e2e_credential_exposure_test.rs:92-100`.
- [ ] 1.7 Submit the isolated statement as the reader through `reader_conn().try_execute(...)`. Assert `denied["status"] == "error"`, and that `denied["exception"]["text"]` names insufficient privilege to call the script, using the wording captured in 1.2. Follow the assertion shape of `revoking_the_owners_scan_grant_denies_the_reader_without_leaking_the_credential`.
- [ ] 1.8 Assert the denial error carries neither `access_key` nor `secret_key` VALUE, reading them from `local_stack_connection_password()` as the sibling tests do. Then assert the reader's ordinary virtual-schema query still returns `SEED_ROWS_SCORE_GT_15` rows, so the denial is scoped to direct script invocation.
- [ ] 1.9 Add the plan visibility versus plan execution section to `docs/security.md` per § Requirements R1, placed after `## Privilege model`. Amend the existing `EXPLAIN VIRTUAL` line under `## Privilege model` so it points to the new section instead of framing the plan only as a credential-leak surface.
- [ ] 1.10 Amend the non-DBA note in `docs/install.md` § "Point the VS at your data" per § Requirements R2. Name only the scripts the 1.2 revocation probe confirmed necessary. State the re-install outcome the 1.2 grant-survival check recorded: tell the operator to re-issue these grants after re-running the installer if that check found them dropped, or state that they survive if it found them intact.
- [ ] 1.11 Run the gates in § Verification § Checklist, including the feature-gated clippy pass, because CI's clippy job does not enable `exasol-e2e` and therefore never lints the new test.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Plan-execution boundary | 1.1-1.11 | — | spec delta `e2e-harness/e2e-harness`; `crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs`, `crates/lakehouse-engine/tests/common/e2e_harness.rs` (`explain_virtual_sql` plus the new single-statement isolation helper added in task 1.3), `docs/security.md`, `docs/install.md` |

One group. Splitting the two documentation tasks into a second group was rejected: both documents
state the same privilege boundary the test verifies live in task 1.2, so a second agent would
re-derive the knowledge the first one just established. Overlapping knowledge across groups is a
consolidation signal rather than a parallelism opportunity. No task is tagged `[expert]`: the test
follows the `try_execute` denial pattern already present in the same file, and the documentation
tasks are prose.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | — | No production code, test, or module becomes obsolete. The plan adds one test, one test helper, and two documentation statements |

The three script-execute grants at `e2e_credential_exposure_test.rs:92-100` are RETAINED unchanged.
They remain the fixture the new assertion reads, rather than a detail it replaces.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A least-privilege reader cannot execute the pushdown plan it can read | Integration | `crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs` | `the_reader_cannot_execute_the_pushdown_plan_it_captured` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| e2e-harness/e2e-harness (new scenario) | `docker compose up -d --wait minio exasol iceberg-rest && cargo test --features exasol-e2e --test e2e_credential_exposure_test -- --test-threads=1 --nocapture` | `4 passed; 0 failed`, including `the_reader_cannot_execute_the_pushdown_plan_it_captured` |
| Requirement R1 | `grep -n "EXECUTE ON SCRIPT\|EXECUTE ANY SCRIPT\|FOR SCRIPT\|#378" docs/security.md` | The new section names the object grant, the system privilege, and the script-scoped connection grant as two independent gates. It names table root, bucket layout, file names, byte sizes, and the CONNECTION name as readable, and cross-references the sealed-envelope section |
| Requirement R2 | `grep -n "EXECUTE ON SCRIPT\|re-running the installer" docs/install.md` | The non-DBA note names the grants the 1.2 revocation probe confirmed necessary, tells the operator to issue them before `CREATE VIRTUAL SCHEMA`, and states the re-install outcome the 1.2 grant-survival check recorded |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e > /tmp/e2e.log 2>&1; echo "rc=$?"` then read `/tmp/e2e.log` | `rc=0`, 0 failures. Read the file rather than piping to `tail`, because the suite output is truncated otherwise |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` | 0 warnings |
| Lint (E2E feature) | `cargo clippy --workspace --all-targets --features exasol-e2e -- -D warnings` | 0 warnings. CI's clippy job omits this feature, so the new test is otherwise unlinted |
| Format | `cargo fmt --all -- --check` | No changes |
