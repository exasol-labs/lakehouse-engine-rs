# Decision Log: change-install-version-smoke-test

## Interview

**Q:** The old fingerprint smoke test worked on any engine `.so` build. The new version-based smoke
test needs `LAKEHOUSE_VERSION()`. If someone runs `install.sh` with an explicit
`--lakehouse-version` pinned to a release older than v0.45.0 (the first release containing
`LAKEHOUSE_VERSION`), that `.so` will not have the entry point. How should the script handle that?
**A:** "we don't want to support older versions. If someone passes a version older than v0.45.0
then ... bad luck." No guard and no minimum-version check. `create_engine_scripts` creates the
`LAKEHOUSE_VERSION` script unconditionally. If the `.so` lacks the entry point, invoking it fails
and the generic error path reports a plain failure. No version-floor check, no warning, and no
special-cased error message. The limitation is accepted and undocumented, not a tracked exception.

**Q:** Should the new version-based smoke test also keep a scan-UDF call as a secondary or fallback
check?
**A:** "We want also to remove references to the scan udf invocation to check the installation."
Full replacement, not addition. No `LAKEHOUSE_SCAN` invocation remains anywhere in the smoke-test
path. `classify_fingerprint_response` and `smoke_test_sql` are removed outright.

**Q:** (clarification folded into the same exchange) The four verdict names in the issue are `pass`,
`version-mismatch`, `fingerprint-mismatch`, and `other-error`. Does the new verdict function still
detect the SLC fingerprint-mismatch error text?
**A:** Yes. The SLC checks the fingerprint on loading ANY Rust UDF, so the failure is orthogonal to
which entry point the query invokes. It reaches the `LAKEHOUSE_VERSION()` call and stays a distinct
classification, even though the scan call is gone.

## Design Decisions

### [1] The install verification records against `packaging/version-udf`, not a new feature

- **Decision:** Add the installer's script-creation and verification scenarios to
  `specs/packaging/version-udf/spec.md` as a CHANGED delta.
- **Alternatives:** A new `packaging/install-version-verification` feature. A delta on
  `packaging/single-so-two-entry-points`.
- **Rationale:** The reported-version-equals-release-version contract needs one owner. A separate
  feature would leave two features independently assuming that contract, which is back-door
  leakage. The recorded plan for the version UDF (`_recorded/029-add-lakehouse-version-udf`) had
  already written both halves into this one feature before the installer half was split out.
  `single-so-two-entry-points` states what the `.so` exports and already assumes a created version
  script without stating who creates it, so it is the wrong owner.
- **Promotes to ADR:** no

### [2] A second dedicated extractor rather than one consolidated extractor

- **Decision:** Add `extract_version_value` alongside `extract_query_value` instead of merging the
  two. Record the bound in the spec Background: the duplication stays at two callers, and a third
  caller consolidates them into one extractor that takes the header literal.
- **Alternatives:** Parameterize `extract_query_value` with the header literal and a digit-skip
  flag. Replace both with one extractor whose footer skip matches the `* row in set` suffix instead
  of the `[0-9]*` glob.
- **Rationale:** The consolidated form is the better design and would also fix a latent defect in
  the SCRIPT_LANGUAGES read. That read's own recorded reasoning (`install.sh`, the empty-value
  guard) states that a mis-parse there drops every registered language on the following
  `ALTER SYSTEM SET SCRIPT_LANGUAGES`. Refactoring that path is a higher-risk change than this
  issue asks for. The flag variant is a configuration parameter standing in for a decision the
  function declined to make. `extract_query_value` is already special-purpose despite its general
  name, because it skips its one caller's `SYSTEM_VALUE` header, so a second special-purpose
  extractor matches the existing shape. No follow-up issue is filed: the consolidation trigger is a
  recorded spec constraint, not scheduled work.
- **Promotes to ADR:** no

### [3] The verification query carries a fixed projection alias

- **Decision:** Emit `SELECT <schema>.LAKEHOUSE_VERSION() AS LAKEHOUSE_ENGINE_VERSION`.
- **Alternatives:** An unaliased `SELECT <schema>.LAKEHOUSE_VERSION()`.
- **Rationale:** The extractor must skip the column-header line, which requires knowing that
  header. Without an alias the header is a database-generated name the script cannot predict. The
  alias also gives the test suite's exapump stub a discriminator that no DDL statement contains:
  the version DDL holds `LAKEHOUSE_VERSION()` followed by `RETURNS`, never followed by
  `AS LAKEHOUSE_ENGINE_VERSION`. That matters because the stub's smoke-test branch precedes its
  `*"CREATE "*` branch.
- **Promotes to ADR:** no

### [4] The fingerprint check precedes the return-code check in the verdict order

- **Decision:** `classify_version_smoke` tests for `Fingerprint mismatch` in the output first, then
  a non-zero return code, then a version difference, then passes.
- **Alternatives:** Test the return code first and treat every non-zero exit as `other-error`.
- **Rationale:** A fingerprint mismatch exits non-zero, so a return-code-first order would
  reclassify it as `other-error` and lose its SLC-alignment remediation message. The order is
  load-bearing, not incidental.
- **Promotes to ADR:** no

### [5] The documentation obligation is a spec scenario with a real test

- **Decision:** Add a third scenario covering the install documentation, and assert it from
  `deploy/scripts/tests/install.test.sh` by reading `$REPO_ROOT/docs/install.md`.
- **Alternatives:** Leave the doc updates as tasks with no spec clause. Assert them from
  `crates/lakehouse-engine/tests/build_convention.rs`.
- **Rationale:** Doc-only tasks with no delta are a traceability gap, and this repo already specs
  documentation claims (`single-so-two-entry-points` requires the build docs to state that a host
  release build is unloadable). The install test suite is the right owner because it already
  reaches outside `install.sh` to assert repo-level installer wiring through `make -n install-slc`,
  and because it runs in the same CI job as the script change. `build_convention.rs` is scoped to
  build conventions.
- **Promotes to ADR:** no

### [6] No engine crate version bump

- **Decision:** Leave `crates/lakehouse-engine/Cargo.toml`'s `version` field untouched. State the
  instruction in the plan's Impact section so the implement step does not apply its default
  feat-to-minor bump.
- **Alternatives:** Bump the minor version, per the default Conventional Commits rule for a `feat`.
- **Rationale:** This plan changes no code compiled into the `.so`. CI deliberately excludes
  `install.sh` from the release assets, with an in-file comment stating that users fetch it from the
  contents API on `main` and that `?ref=<tag>` covers pinning. A bump would publish a release whose
  only delta is the version string. Project precedent: PR #292 reverted exactly such a bump.
- **Promotes to ADR:** no

### [7] The reverted prior implementation is cited as reference, not as authority

- **Decision:** Name commits `dac42c5` and `5b3cbed` in the plan's Dependencies section, and keep
  the spec delta self-sufficient so the plan stands if those commits become unreachable.
- **Alternatives:** Treat the reverted diff as the specification. Omit it entirely and let the
  implementer re-derive the change.
- **Rationale:** The reverted diff carries the exact skip-pattern set and verdict order, including
  the header-glob fix its own review round produced, which saves re-deriving two subtle details.
  The commits live only on a local branch whose remote was deleted after PR #390 merged, so the
  plan must not depend on them. The spec delta states every load-bearing clause directly.
- **Promotes to ADR:** no

### [8] Credential redaction in the verdict path is out of scope (considered and rejected)

- **Decision:** The plan states no credential requirement anywhere in the verification path.
  `run_smoke_test` surfaces the captured output unchanged. This plan adds no `redact_credentials`
  function and leaves `test_external_failure_actionable` untouched.
- **Alternatives:** A normative clause requiring that no aborting message carry a credential,
  enforced by a `redact_credentials` helper and asserted from `test_external_failure_actionable`.
- **Rationale:** Issue #388 lists four scope items and names no credential requirement. The
  clarifying interview raised none. Initial planning proposed the clause on its own initiative.
  Round-1 review then flagged the self-introduced clause as normative but unenforced, which is what
  motivated building the `redact_credentials` mechanism to satisfy it. The user rejected the whole
  direction as out of scope once it surfaced. `install.sh:995` and `install.sh:1006` stay the
  pre-existing unaddressed instance of the same echo-output pattern, untouched by this plan, exactly
  as before this plan existed. The plan stays scoped to replacing the fingerprint smoke test with a
  version check.
- **Promotes to ADR:** no

### [9] The CI job comment is in scope, and the spec clause covers it

- **Decision:** Update the `install-script-e2e` job comment in `.github/workflows/ci.yml`, and
  extend the documentation scenario's closing clause to bind CI job descriptions as well as the
  documentation.
- **Alternatives:** Leave the comment. Update it as an untraced task.
- **Rationale:** That comment states the job proves a "fingerprint smoke test", which this plan
  removes. A comment describing removed behavior is a false claim about project behavior. Widening
  the spec clause keeps the task traceable instead of leaving it as an untraced extra. The change
  touches comment text only, so no job or step behavior changes.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] The credential clause was unenforced and its test could not fail

- **Finding:** `plan-reviewer` (round 1, Feasibility `[UNSTATED_ASSUMPTION]`) found the
  credential-non-leak clause normative but unimplemented and untestable. `run_smoke_test` surfaced
  `$out` unfiltered, `run_sql` puts a credential-bearing DSN on exapump's argv
  (`install.sh:1168`, `install.sh:1169`), and task 2.1's `other-error` stub mode echoed a fixed
  string that carries no credential by construction. Decision [8] also claimed a new exposure
  surface that `install.sh:995` and `install.sh:1006` already show is pre-existing.
- **Direction change (superseded by the Reversed note below):** The round-1 revision added a
  `redact_credentials` task, routed task 1.5's `other-error` output through it, changed task 2.1's
  `other-error` stub mode to echo the stub's own argv, and added a task extending
  `test_external_failure_actionable`. It corrected decision [8]'s rationale and added a Patterns
  row. None of that remains in the plan. The task numbers that entry used are reused by other
  tasks now.
- **Reversed:** This entire direction is reversed by explicit user decision. The credential clause
  was never part of issue #388 or the interview, so the finding above resolved a self-introduced
  requirement. The spec clause, `redact_credentials`, the Patterns row, the stub argv echo, and the
  `test_external_failure_actionable` extension are all removed. Decision [8] now records the
  proposal as considered and rejected.
- **Promotes to ADR:** no

### [plan-review] The empty-reported-version clause had no test

- **Finding:** `plan-reviewer` (round 1, Requirement Quality `[COMPLETENESS_GAP]`) found that no
  planned case produces an empty reported version. Task 1.5 implements the clause as
  `${reported:-<empty>}`, and tasks 2.3 and 2.4 enumerated no case that yields return code 0 with
  an empty extraction.
- **Direction change:** Task 2.1 gains a fifth `EXAPUMP_SMOKE_MODE` value, `empty-version`, which
  prints the banner, the header, and the footer with no value line and exits 0. Task 2.3 gains a
  fifth case asserting a non-zero exit and the literal `<empty>` in the failure message.
- **Promotes to ADR:** no

### [plan-review] No test ran the verification on the Exasol Personal `--deployment` path

- **Finding:** `plan-reviewer` (round 1, Task Breakdown `[TRACEABILITY_GAP]`) found the plan
  claiming end-to-end Personal-path coverage that does not exist. `deployment_local_ssh_transport`
  asserts only connection resolution, and every Personal-path test calls `deploy_personal_local` or
  `resolve_deployment_transport` directly. `run_smoke_test` is called from `main()` at
  `install.sh:1529`, after that branch, so no test reached it on that path.
- **Direction change:** Took the coverage half of the fix rather than the spec-narrowing half,
  because the harness supports a full run. `DEPLOYMENT_ROOT` derives from `$HOME`
  (`install.sh:43`), the existing `write_local_deployment_fixture` already writes the descriptor,
  the secrets file and the node key, and the ssh, scp, exapump and curl stubs answer every call the
  path makes. Added task 2.7, `test_version_smoke_runs_on_the_deployment_path`, renumbered the
  runner-list task to 2.8, added a Scenario Coverage row, and replaced the false coverage claim
  under the table. `spec.md:79-80` stays unchanged.
- **Promotes to ADR:** no

### [plan-review] The documentation scenario's CI half had no test

- **Finding:** `plan-reviewer` (round 1, Task Breakdown `[TRACEABILITY_GAP]`) found that decision
  [9] widened the documentation clause to cover CI job descriptions, while task 2.6, the only test
  mapped to that scenario, reads `docs/install.md` alone. Nothing asserted anything about
  `.github/workflows/ci.yml:424` or `:444`.
- **Direction change:** Took the coverage half of the fix. Task 2.6 now also reads
  `.github/workflows/ci.yml` and asserts the file carries no `fingerprint smoke test` text and
  names `LAKEHOUSE_VERSION`. The assertion pins the exact phrase, because `ci.yml` lines 33, 74 and
  95 use `fingerprint` for the unrelated SDK and build-cache fingerprints. Task 3.4 stays in the
  traced task list, and `spec.md:119-120` stays unchanged.
- **Promotes to ADR:** no
