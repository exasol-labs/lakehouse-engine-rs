# Plan Review Findings: change-install-version-smoke-test (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 12 (Blockers: 4, Advisory: 8)
- Intent Fidelity blockers: 0

## Premortem

Three failure stories, each routed into the taxonomy below.

1. An operator's failed install prints the DSN password into a terminal or a CI log. The
   `other-error` verdict echoes exapump's captured output verbatim, and `run_sql` passes a
   credential-bearing DSN on exapump's argv. The spec forbids the leak, the implementation does not
   prevent it, and the planned test cannot detect it. Routed to Feasibility
   `[UNSTATED_ASSUMPTION]`.
2. An Exasol Personal install ships with a broken verification path for a release cycle. The spec
   binds the verification to the `--deployment` run, no test exercises `main()` on that path, and
   the plan asserts a test that does not cover it. Routed to Task Breakdown `[TRACEABILITY_GAP]`.
3. A later edit to the projection alias silently breaks every install. The literal
   `LAKEHOUSE_ENGINE_VERSION` lives in three independent places, and a change to one makes the
   extractor return the column header as the reported version. Routed to Design Depth
   `[INFORMATION_LEAKAGE]`.

## Intent Fidelity

No objection, axis checked. All four issue scope items map to tasks: item 1 to task 1.1, item 2 to
tasks 1.2 through 1.5, item 3 to tasks 2.1 through 2.8, item 4 to tasks 3.1 through 3.3. Both
"not in scope" items hold: no task touches what `install-script-e2e` downloads, and no task touches
the `.so` build or packaging. The two user decisions are operationalized as directed: no
version-floor check appears anywhere (`spec.md:23-26`, `spec.md:59-60`), and no `LAKEHOUSE_SCAN`
call survives in the verification path (`spec.md:73-74`, plan.md § Dead Code Removal). The three
additions beyond the issue's scope each trace to a spec clause rather than to gold-plating, so no
`[SCOPE_CREEP]` finding stands. Two of them carry verification defects, raised under Task Breakdown
instead.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: spec.md § "The install verification passes only on an exact version match", lines 99-101; plan.md § Implementation Tasks 1.5 and 2.7; decision-log.md § [8]
- Issue: the credential clause is normative but unenforced, and its planned test cannot fail. The
  clause states "the surfaced database error text MUST NOT reveal the configured profile password,
  DSN password, or SaaS token". Task 1.5 surfaces `$out` unfiltered (`err "version smoke test
  failed: $out"` in the reverted reference implementation), and `run_sql` passes a credential-bearing
  connection string on exapump's argv in two of three modes: `install.sh:1168` (`exapump sql -d
  "$ARG_DSN"`) and `install.sh:1169` (`exapump sql -d "$HOST_DSN"`, built at `install.sh:1491` as
  `exasol://$enc_user:$enc_password@...`). Nothing in the plan redacts that value. Task 2.1's
  `other-error` stub mode echoes a fixed `Error: connection to database lost`, which carries no
  credential by construction, so task 2.7's `SECRETPAT123` / `SECRETPW456` assertions pass whatever
  the script does. Decision-log [8] also over-claims: `install.sh:995` and `install.sh:1006` already
  echo captured exapump output into an `err` message, so echoing exapump output is not a new
  exposure class.
- Fix: In plan.md § Implementation Tasks, add a task under item 1 that redacts before echoing:
  `run_smoke_test` MUST replace every occurrence of the resolved profile password, DSN password, and
  SaaS PAT in `$out` with a fixed placeholder before passing it to `err`. In task 2.1, change the
  `other-error` stub mode to echo its own argv (which in host mode carries the percent-encoded
  `SECRETPW456`) so task 2.7's assertion can fail. In decision-log.md § [8], delete the sentence
  claiming a new exposure surface and cite `install.sh:995` and `install.sh:1006` as the existing
  sites of the same class.

Otherwise no objection on this axis. The load-bearing dependency claims check out: `v0.45.0` is the
latest published release, its tag resolves to `cb1da8b` which is this checkout's `HEAD` and the
merge commit of PR #390, and `crates/lakehouse-engine/Cargo.toml` reads `version = "0.45.0"`, so the
downloaded `.so` does export the version entry point. Both `v0.45.0` and `v0.44.1` publish
`lakehouse-engine.tar.gz`, so the plan's manual-test rows are runnable. The SQL shape is
live-verified rather than assumed: `crates/lakehouse-engine/tests/common/e2e_harness.rs:180-183`
creates the script with `RETURNS VARCHAR(100)` and
`crates/lakehouse-engine/tests/e2e_version_udf_test.rs:36` calls it as `SELECT
{SCHEMA}.{VERSION_SCRIPT}() AS REPORTED_VERSION` with no `FROM` clause, the exact shape task 1.2
emits. The no-bump decision is verified against `.github/workflows/ci.yml:1096-1100`, whose comment
states install.sh is deliberately not a release asset. The prior-art claim is verified: `dac42c5`
implements the change and `5b3cbed` reverts the install.sh, install.test.sh, and docs/install.md
halves, both reachable on local branch `feat/add-lakehouse-version-udf`, and the reverted
`extract_version_value` already carries the `LAKEHOUSE_ENGINE_VERSION*` glob suffix task 1.3
requires. The extractor's skip set is sound against real exapump output rather than only against the
stub, because the sibling `extract_query_value` proves the banner / header / value / footer shape on
every live install.

## Requirement Quality

#### [COMPLETENESS_GAP] BLOCKER
- Location: spec.md § "The install verification passes only on an exact version match", lines 91-93; plan.md § Implementation Tasks 2.3 and 2.4; plan.md § Scenario Coverage
- Issue: the empty-reported-version clause has no test. The spec singles it out normatively:
  "rendering an empty reported value as a visible placeholder rather than as nothing". Task 1.5
  implements it as `${reported:-<empty>}`. No planned case produces it. Task 2.3 enumerates four
  cases (pass, version-mismatch at `9.9.9`, fingerprint-mismatch, other-error) and task 2.4
  enumerates three extraction cases (bare value, trailing-whitespace header, banner and footer
  around the value); none yields return code 0 with an empty extraction. The Scenario Coverage table
  nonetheless maps the whole scenario to `test_version_smoke_pass_and_fail`.
- Fix: In plan.md § Implementation Tasks 2.1, add a fifth `EXAPUMP_SMOKE_MODE` value
  `empty-version` that prints the banner, the `LAKEHOUSE_ENGINE_VERSION` header, and the
  `1 row in set` footer with no value line, and exits 0. In task 2.3, add a fifth case asserting
  that the run exits non-zero and that the failure message contains the literal `<empty>`.

#### [REQUIREMENT_CONFLICT] ADVISORY
- Location: spec.md § Background, lines 10-11
- Issue: the delta's own manifest mis-counts its Background additions. It states "This delta ADDS
  FOUR scenarios and TWO Background bullets". The recorded library spec
  (`specs/packaging/version-udf/spec.md`) carries two Background bullets, the entry-point bullet and
  the version-owner bullet, both reproduced at spec.md:27-29. The delta adds four further
  substantive bullets: lines 17-20 (feature ownership), 23-26 (no version floor), 30-33 (tag
  derivation), and 35-36 (the two-extractor bound). The scenario count of four is correct; the
  Background count of two is wrong by half. The Background bullets carry no DELTA markers, so this
  sentence is the only signal the recorder has.
- Fix: In spec.md:10, change "TWO Background bullets" to "FOUR Background bullets", and name them by
  their opening words so the recorder can reconcile each against the library.

#### [COMPLETENESS_GAP] ADVISORY
- Location: spec.md § "The install script creates the version script with the other deployment scripts", lines 57-58
- Issue: one Gherkin clause carries two separate requirements, and the second has no test. The
  clause reads "every statement SHALL stay idempotent, so a re-run over a prior install replaces the
  scripts in place, and a failure of any statement SHALL abort the install naming that statement".
  Task 2.2 asserts `CREATE OR REPLACE` for the idempotency half. Nothing asserts the abort half. The
  stub already carries an `EXAPUMP_DDL_FAIL` branch at `install.test.sh:165`, and no test in the
  suite sets that variable, so the branch is unexercised today and stays unexercised under this
  plan.
- Fix: In spec.md, split lines 57-58 into two `*AND*` clauses, one per requirement. Then either add
  a case to plan.md § Implementation Tasks 2.2 that sets `EXAPUMP_DDL_FAIL=1` and asserts the error
  message names the failing statement, or delete the abort clause from the delta as pre-existing
  behavior this plan does not touch.

## Task Breakdown

#### [TRACEABILITY_GAP] BLOCKER
- Location: plan.md § Verification, lines 219-221; spec.md § "The install verification queries the version script and reads its reported value", lines 79-80
- Issue: the plan claims coverage that does not exist for the Exasol Personal path. The spec clause
  reads "the verification SHALL run on every install path, including a `--skip-slc` run and an
  Exasol Personal `--deployment` run". The plan states "`test_skip_slc_gating` and
  `deployment_local_ssh_transport` already run the full `--skip-slc` and Exasol Personal paths end to
  end against the stubs". The `--skip-slc` half is true: `install.test.sh:1643` and
  `install.test.sh:1661` invoke the installer as a file, so `main()` reaches `run_smoke_test`. The
  Personal half is false. `deployment_local_ssh_transport` asserts only connection resolution
  (`rc=0`, `transport=ssh`, `ssh_port=52341`, key path) at `install.test.sh:2065-2068`. Every
  Personal-path test uses the `run_deploy_personal_local` harness, which sources the installer and
  calls `deploy_personal_local` directly. `run_smoke_test` is called from `main()` at
  `install.sh:1529`, after that branch, so no test in the suite ever runs the verification on the
  `--deployment` path.
- Fix: In plan.md § Implementation Tasks item 2, add a task that adds a Personal-path test running
  the installer through `main()` with `--deployment` against the ssh stub and asserting that the run
  log contains `LAKEHOUSE_VERSION() AS LAKEHOUSE_ENGINE_VERSION`. Add that test to the task 2.8
  runner list and to the Scenario Coverage table. If the ssh stub cannot support a full `main()` run,
  instead delete "and an Exasol Personal `--deployment` run" from spec.md:79-80 and rewrite
  plan.md:219-221 to claim only the `--skip-slc` coverage that `test_skip_slc_gating` provides.

#### [TRACEABILITY_GAP] BLOCKER
- Location: plan.md § Implementation Tasks 3.4 and § Scenario Coverage; spec.md § "The install documentation describes the version-based verification", lines 119-120; decision-log.md § [9]
- Issue: the CI half of the documentation clause has no test, and the coverage table claims
  otherwise. Decision [9] widened the clause to "neither the documentation nor the CI job
  descriptions of the install script SHALL present a scan-script call, or an error-kind fingerprint
  check, as an install verification step", explicitly to keep task 3.4 traceable. Task 2.6, the only
  test the Scenario Coverage table maps to that scenario, reads `$REPO_ROOT/docs/install.md` alone.
  Nothing asserts anything about `.github/workflows/ci.yml`, whose `install-script-e2e` comment
  currently claims the job proves a "fingerprint smoke test" (`.github/workflows/ci.yml:424`) and
  names only `LAKEHOUSE_ADAPTER` and `LAKEHOUSE_SCAN` as the sandboxes it spawns (line 444).
- Fix: In plan.md § Implementation Tasks 2.6, extend `test_docs_describe_version_verification` to
  also read `$REPO_ROOT/.github/workflows/ci.yml` and assert that the `install-script-e2e` job
  comment contains no `fingerprint smoke test` text and names `LAKEHOUSE_VERSION` among the scripts
  the job creates. Alternatively, narrow spec.md:119-120 to the documentation only and move task 3.4
  out of the traced task list into plan.md § Dead Code Removal as an incidental stale-comment fix.

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Implementation Tasks 1.6; `deploy/scripts/install.sh:3`
- Issue: task 1.6 leaves a stale claim one line above the one it fixes. It updates only
  `install.sh:4` from "fingerprint smoke test" to "version smoke test". Line 3 of the same comment
  block reads "uploads and registers the engine .so plus its three scripts". After task 1.1 the
  installer creates four scripts, so the header carries a false claim about project behavior, the
  exact defect class decision [9] cites as its reason to fix the CI comment.
- Fix: In plan.md § Implementation Tasks 1.6, extend the task to also change "its three scripts" to
  "its four scripts" at `deploy/scripts/install.sh:3`.

Otherwise no objection on this axis. One Parallelization group is correct rather than a missed
split: every task serves the single `packaging/version-udf` delta, and tasks 2.2, 2.6, and 2.7 all
edit `deploy/scripts/tests/install.test.sh`, so the group shares both a delta and a source module.
No `[CLUSTER_INCOHERENCE]` finding stands, and splitting the script from its test suite would slice
by layer. Task granularity is verifiable per unit: the three `[expert]` tags sit on the three tasks
with real subtlety (the extractor's skip set, the verdict order, the stub's alias discriminator).
The stub-alias trap is threaded consistently: task 1.2 emits the alias, task 1.3 skips it, task 2.1
matches on it, and task 2.4 asserts the executed SQL carries it, which is correct because the smoke
branch at `install.test.sh:158` precedes the `*"CREATE "*` branch at line 164 and the version DDL
contains `LAKEHOUSE_VERSION()` without the alias.

## Design Depth

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: plan.md § Implementation Tasks 1.2, 1.3, and 2.1; decision-log.md § [3]
- Issue: the `LAKEHOUSE_ENGINE_VERSION` literal becomes one decision reflected in three
  independently-written places with nothing enforcing agreement: `version_smoke_sql` emits it as the
  projection alias (task 1.2), `extract_version_value` hard-codes it as a skip pattern (task 1.3),
  and the test stub both matches on it and echoes it as the header (task 2.1). Changing the alias in
  one place makes the extractor return the column header as the reported version, so every install
  fails with a version-mismatch naming the header string. Decision [3] reasons about why the alias
  must be fixed and never notices it is duplicated.
- Fix: In plan.md § Implementation Tasks, add a task under item 1 declaring one readonly constant in
  `deploy/scripts/install.sh` (for example `VERSION_SMOKE_COLUMN="LAKEHOUSE_ENGINE_VERSION"`), and
  change tasks 1.2 and 1.3 to build the projection alias and the skip pattern from that constant
  rather than from a repeated literal.

#### [TACTICAL_SHORTCUT] ADVISORY
- Location: decision-log.md § [2]; spec.md § Background, lines 35-36
- Issue: the second extractor is a tactical shortcut whose follow-up is recorded but not scheduled,
  and the risk assessment that justifies it is unsupported. Decision [2] states "No follow-up issue
  is filed: the consolidation trigger is a recorded spec constraint, not scheduled work", so nothing
  will ever fire the trigger. The rejection of the zero-cost consolidation, replacing
  `extract_query_value`'s `[0-9]*` skip with the same footer-suffix skip the new extractor uses,
  rests on calling that path "a higher-risk change". Three existing tests already cover it:
  `test_script_languages_append_preserves_existing`, `test_script_languages_replace_rust_idempotent`,
  and `test_empty_script_languages_read_hard_fails`, the last of which hard-fails on exactly the
  mis-parse the decision fears.
- Fix: In decision-log.md § [2], either change the decision to consolidate now, adding a task that
  replaces both extractors with one taking the header literal and using the footer-suffix skip and
  naming those three tests as the regression net, or state that a tracked GitHub issue for the
  consolidation is filed and cite it inline in spec.md:35-36.

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: plan.md § Implementation Tasks 2.1
- Issue: the task creates a second copy of the default release version and states the coupling as a
  rule instead of removing it: "The default value `0.26.3` MUST track the curl stub's default
  `GH_ENGINE_TAG` of `v0.26.3`". The curl stub owns that default at `install.test.sh:224`. A future
  full-run test that overrides `GH_ENGINE_TAG` then fails opaquely as a version-mismatch rather than
  as the thing it tests.
- Fix: In plan.md § Implementation Tasks 2.1, change the stub's default reported version to derive
  from the tag default with the leading `v` stripped, rather than restating `0.26.3` as a second
  literal, and delete the sentence stating the tracking rule.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Impact
- Issue: Impact names only what the change gains and never states what it stops covering. The
  removed smoke test called `LAKEHOUSE_SCAN('x', 'y')` and reached that entry point's argument
  deserialization, so `install-script-e2e` proved on a real Exasol that the scan entry point loads
  and dispatches by name. After this change nothing at install time exercises `LAKEHOUSE_SCAN` or
  `LAKEHOUSE_ADAPTER`, so a DDL signature or entry-point naming defect specific to the scan script
  passes the install. The user directed the removal, so the decision stands; the silence about its
  consequence does not.
- Fix: In plan.md § Impact, add two sentences stating that install-time verification no longer
  exercises the scan or adapter entry points, and naming the `e2e` and `e2e-lakekeeper` CI jobs as
  what still covers scan dispatch on a real Exasol.

Otherwise no objection on this axis. The new functions are appropriately shaped:
`classify_version_smoke` is a pure four-argument classifier the caller can unit-test through
`source`, and it owns the verdict order that decision [4] correctly identifies as load-bearing, with
the fingerprint check ahead of the return-code check. `ddl_version` and `version_smoke_sql` match the
existing `ddl_*` sibling shape, so they add no new boundary a reader must learn. Dependency direction
is unchanged: the classifier reads no globals and performs no I/O.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md § Summary, line 5; plan.md § Dependencies, lines 101-103
- Issue: both passages open with a subject-less fragment and join two ideas with `and` past the
  25-word descriptive cap. The Summary reads "Replaces the install script's error-kind fingerprint
  smoke test with a positive version check that calls `LAKEHOUSE_VERSION()` and compares the
  reported value against the resolved release version", 27 words with no named actor. Dependencies
  reads "Prior art: commit `dac42c5` on the local branch `feat/add-lakehouse-version-udf`
  implemented this exact change, and commit `5b3cbed` reverted the `install.sh`, `install.test.sh`,
  and `docs/install.md` parts to split them into this follow-up", a 35-word sentence behind a
  two-word fragment.
- Fix: In plan.md:5, name the actor and split into two sentences, for example "The install script
  calls `LAKEHOUSE_VERSION()` and compares the reported value against the resolved release version.
  That positive check replaces the error-kind fingerprint smoke test." In plan.md:101-103, replace
  the `Prior art:` fragment with a full sentence and split the `and`-joined clause into two
  sentences, one per commit.

Otherwise no objection on this axis. The three artifacts contain no em dashes outside table cells,
no semicolons, no contractions, and no superlatives or intensifiers. RFC keywords are used
normatively throughout the spec delta and the Impact section. Headings summarize their sections, and
the Decision section leads with its conclusion rather than narrating how the plan reached it.
