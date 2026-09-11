# Plan Review Findings: change-install-version-smoke-test (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 10 (Blockers: 0, Advisory: 10)
- Intent Fidelity blockers: 0

## Round-1 Blocker Recheck

- Resolved: [UNSTATED_ASSUMPTION] The credential clause was unenforced and its test could not fail.
  Task 1.6 adds `redact_credentials` and names four real credential sources, each verified present:
  `ARG_PASSWORD` (`install.sh:66`), `url_encode` (`:113`), `url_decode` (`:127`),
  `extract_dsn_password` (`:149`), `RESOLVED_PAT` (`:94`), `read_profile_key` (`:231`),
  `exapump_config_path` (`:222`). Task 1.5 routes the `other-error` output through the redactor and
  states that `err` never receives `$out`. The test can now fail. Task 2.1's `other-error` mode
  echoes the stub's own `$*`, and in host connectivity mode `run_sql` puts
  `-d exasol://myuser:SECRETPW456@...` on that argv (`install.sh:1169`, built at `:1491`).
  `url_encode` maps `SECRETPW456` to itself, so the literal reaches stderr and task 2.7's
  `assert_not_contains` fails without the redaction. Decision [8] now states the echo-output class
  is pre-existing and cites `install.sh:995` and `:1006`, both confirmed to pass captured exapump
  output into an `err` message. A Patterns row records the redaction site.
- Resolved: [COMPLETENESS_GAP] The empty-reported-version clause had no test. Task 2.1 adds a fifth
  stub mode, `empty-version`, printing the banner, the header and `1 row in set` with no value line
  and exit 0. Every one of those lines matches a skip in task 1.3's set (`[*`,
  `LAKEHOUSE_ENGINE_VERSION*`, `* row in set`), so the extractor yields an empty string with return
  code 0. `classify_version_smoke` then returns `version-mismatch`, which task 1.5 renders through
  `${reported:-<empty>}`. Task 2.3's fifth case asserts a non-zero exit and the literal `<empty>`.
- Resolved: [TRACEABILITY_GAP] No test ran the verification on the Exasol Personal `--deployment`
  path. Task 2.8 adds `test_version_smoke_runs_on_the_deployment_path`, and the harness supports the
  full `main()` run. `resolve_deployment_transport` fills `ARG_HOST`, `ARG_USER` and `ARG_PASSWORD`
  from the fixture descriptor and secrets file (`install.sh:382-395`), so `validate_connectivity`
  (`:542`) returns `host` with no `--profile` and no `--dsn`. `DEPLOYMENT_ROOT` derives from `$HOME`
  at `:43` with no environment override, as the task states. The ssh stub returns 0 for `test -e`,
  so `vm_wait_for_reconciled_path` succeeds on the first try and sleeps zero times despite
  `VM_RECONCILE_TRIES=30`. The curl stub serves every `*/releases/download/*` request from
  `GH_ASSET_TARBALL`, and the exapump stub answers the `EXA_PARAMETERS` read with a non-empty
  default, so `read_script_languages`' empty guard stays silent. `run_smoke_test` at `:1529` is
  reached, and the resolved version `0.26.3` matches the stub's default reported value. A Scenario
  Coverage row and a corrected coverage paragraph replace the false claim.
- Resolved: [TRACEABILITY_GAP] The documentation scenario's CI half had no test. Task 2.6 now reads
  `$REPO_ROOT/.github/workflows/ci.yml` and asserts two things: the file carries no
  `fingerprint smoke test` text, and the file names `LAKEHOUSE_VERSION`. Both assertions fail today
  and pass after task 3.4. `fingerprint smoke test` appears only at `ci.yml:424`. The bare word
  `fingerprint` also appears at `:33`, `:74` and `:95` for the SDK and build-cache fingerprints,
  which is why pinning the exact phrase is correct. `LAKEHOUSE_VERSION` appears nowhere in `ci.yml`
  today, and `:444` names only `LAKEHOUSE_ADAPTER` and `LAKEHOUSE_SCAN`.

Two round-1 advisories were also fixed. The Background manifest at `spec.md:10` now reads "FOUR
Background bullets" and names all four added bullets plus the two recorded ones, which matches the
library spec's two bullets (`specs/packaging/version-udf/spec.md:8-10`). Task 1.7 now corrects both
`install.sh:3` ("its three scripts") and `install.sh:4` ("fingerprint smoke test"), both confirmed
present in the current file.

## Premortem

Two failure stories for the revised plan, each routed below.

1. An install fails in profile connectivity mode, and every operator sees a shredded error message.
   `ARG_PASSWORD` is empty in that mode, and an empty pattern in `redact_credentials` matches
   between every character. The plan names the trap and schedules no test that isolates it. Routed
   to Task Breakdown `[COMPLETENESS_GAP]`.
2. A future contributor overrides `GH_ENGINE_TAG` in a full-run test. The deployment test and the
   pass-verdict test both fail as version mismatches rather than as the thing they assert, because
   the stub restates `0.26.3` as a second literal. Routed to Design Depth `[INFORMATION_LEAKAGE]`.

## Intent Fidelity

No objection, axis checked. The revision added one script function, one stub mode and two tests, and
each traces to a spec clause the user's decisions already produced. No revision task reinterprets
the ask. Task 1.6 serves `spec.md:103-105`, task 2.1's `empty-version` mode serves `spec.md:95-97`,
task 2.8 serves `spec.md:83-84`, and task 2.6's CI half serves `spec.md:123-124`. Both user
decisions still hold: no version floor appears anywhere, and no `LAKEHOUSE_SCAN` call survives in
the verification path. The revision dropped nothing from the four issue scope items.

## Feasibility

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Implementation Tasks 2.8
- Issue: task 2.8 states the `HOME` requirement without a mechanism, and no existing helper provides
  one. The task reads "Point `HOME` at a sandbox fake home". `run_file` is
  `LAST_OUT="$( ( export PATH="$RUN_PATH"; exec "$BASH_BIN" "$INSTALLER" "$@" ) 2>&1 )"
  (`install.test.sh:437-440`), which exports `PATH` alone and inherits `HOME` from the test process.
  `reset_env` (`install.test.sh:431-445`) restores `EXAPUMP_CONFIG` and unsets every stub variable,
  and it touches `HOME` nowhere. An implementer who exports `HOME` before the run leaks that value
  into every later test in file order.
- Fix: In plan.md § Implementation Tasks 2.8, state the mechanism: export `HOME` to the fake home
  immediately before the `run_file` call and restore the captured original immediately after. Add
  one sentence instructing the same task to restore `HOME` in `reset_env`, so a later test never
  inherits the fake home.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Impact
- Issue: carried from round 1, still open. Impact names what the change gains and never states what
  it stops covering. The removed smoke test called `LAKEHOUSE_SCAN('x', 'y')` and reached that entry
  point's argument deserialization, so `install-script-e2e` proved on a real Exasol that the scan
  entry point loads and dispatches by name. After this change nothing at install time exercises
  `LAKEHOUSE_SCAN` or `LAKEHOUSE_ADAPTER`. A DDL signature or entry-point naming defect specific to
  the scan script now passes the install.
- Fix: In plan.md § Impact, add two sentences stating that install-time verification no longer
  exercises the scan or adapter entry points, and naming the `e2e` and `e2e-lakekeeper` CI jobs as
  what still covers scan dispatch on a real Exasol.

Otherwise no objection on this axis. Every line reference the revision added checks out against the
current files: `install.sh:43` (`DEPLOYMENT_ROOT="$HOME/..."`), `:1168` (`exapump sql -d "$ARG_DSN"`),
`:1169` (`exapump sql -d "$HOST_DSN"`), `:1491` (`HOST_DSN` assembly), `:1529` (`run_smoke_test` in
`main()`), `:995` and `:1006` (pre-existing captured-output echoes), and `ci.yml:33`, `:74`, `:95`,
`:424`. Task 2.7's run shape matches the existing credential-hygiene block, which already invokes
`run_file --account-id ACC1 --database-id DB1 --host myhost:8563 --user myuser --password
SECRETPW456` and asserts a zero exit, so injecting `EXAPUMP_SMOKE_MODE=other-error` moves the
failure to the verification step rather than to an earlier one. `resolve_saas_pat` sets
`RESOLVED_PAT="$ARG_PASSWORD"` at `install.sh:262` for that run, so task 1.6's `RESOLVED_PAT`
candidate is non-empty there.

## Requirement Quality

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Implementation Tasks 1.5; spec.md § "The install verification passes only on
  an exact version match", lines 103-105
- Issue: one output-derived value still reaches an aborting message unredacted. The clause states
  "no aborting message SHALL carry a credential". Task 1.5 redacts one message: "The one message
  that carries captured output goes through `redact_credentials` first." The `version-mismatch`
  message also carries output-derived text, because `$reported` comes out of
  `extract_version_value` over the captured output. Task 1.3's skip set drops the banner, the
  header, empty lines, `*Error*` lines and the footer, and returns the first surviving line
  verbatim. Any surviving line that carries a connection string therefore lands in the
  `version-mismatch` message with no redaction. The clause holds only because that verdict needs
  return code 0.
- Fix: In plan.md § Implementation Tasks 1.5, change the `version-mismatch` verdict to pass
  `$reported` through `redact_credentials` before interpolating it, and change the closing sentence
  to state that every message carrying output-derived text goes through the redactor, not only the
  one carrying `$out`.

#### [COMPLETENESS_GAP] ADVISORY
- Location: spec.md § "The install script creates the version script with the other deployment
  scripts", lines 61-62
- Issue: carried from round 1, still open. One Gherkin clause carries two separate requirements, and
  the second has no test. The clause reads "every statement SHALL stay idempotent, so a re-run over
  a prior install replaces the scripts in place, and a failure of any statement SHALL abort the
  install naming that statement". Task 2.2 asserts `CREATE OR REPLACE` for the idempotency half.
  Nothing asserts the abort half. The stub carries an `EXAPUMP_DDL_FAIL` branch at
  `install.test.sh:165`, and the only other mention of that variable in the file is `reset_env`'s
  `unset` at line 433, so no test sets it.
- Fix: In spec.md, split lines 61-62 into two `*AND*` clauses, one per requirement. Then either add
  a case to plan.md § Implementation Tasks 2.2 that sets `EXAPUMP_DDL_FAIL=1` and asserts the error
  message names the failing statement, or delete the abort clause from the delta as pre-existing
  behavior this plan does not touch.

## Task Breakdown

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Implementation Tasks 1.6, 2.3 and 2.4
- Issue: the new `[expert]` function has no unit test, and the correctness trap the task itself
  names is covered only by accident. Task 1.6 states "Skip every empty candidate: an empty pattern
  matches everywhere and would corrupt the text." Task 2.4 unit-tests `version_smoke_sql` and
  `extract_version_value` through `source "$INSTALLER"` and names `redact_credentials` nowhere. The
  only run that exercises the redactor is task 2.7's, which pins host connectivity mode where
  `ARG_PASSWORD` is non-empty, so the empty-candidate branch never runs there. Task 2.3's
  `other-error` case would catch the corruption when it runs in profile mode, because
  `HAPPY_ARGS` leaves `ARG_PASSWORD` empty, but task 2.3 states no connectivity mode for that case.
- Fix: In plan.md § Implementation Tasks 2.4, add a `redact_credentials` unit case run through
  `source "$INSTALLER"`: set `ARG_PASSWORD` empty and every other candidate empty, pass a known
  text, and assert the output equals the input unchanged. Add a second case setting `ARG_PASSWORD`
  to a value present in the text and asserting the `<redacted>` placeholder replaces it. In task
  2.3, state that the `other-error` case runs in profile connectivity mode.

Otherwise no objection on this axis. Every spec clause the revision touched now has an implementing
task and a Scenario Coverage row. Task 2.9's "the two renamed functions plus the three new ones"
counts correctly after the renumbering: `test_four_scripts_ddl_saas_path_types` and
`test_version_smoke_pass_and_fail` are the renames, and
`test_version_smoke_query_and_extraction`, `test_docs_describe_version_verification` and
`test_version_smoke_runs_on_the_deployment_path` are the three additions. The single Parallelization
group still holds: every task serves the one `packaging/version-udf` delta, and tasks 2.1 through
2.9 all edit `deploy/scripts/tests/install.test.sh`. The delta conflicts with no recorded spec:
`specs/packaging/single-so-two-entry-points/spec.md:13` states three Rust entry points plus a
separate LUA distributor, which is the same four scripts the delta's `spec.md:57-58` names.

## Design Depth

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: plan.md § Implementation Tasks 1.2, 1.3, 2.1, 2.4 and 2.8; decision-log.md § [3]
- Issue: carried from round 1, and the revision widened it. The `LAKEHOUSE_ENGINE_VERSION` literal
  is one decision reflected in independently-written places with nothing enforcing agreement:
  `version_smoke_sql` emits it as the projection alias (task 1.2), `extract_version_value`
  hard-codes it as a skip pattern (task 1.3), the stub matches on it and echoes it as the header
  (task 2.1), and two tests now assert it in the run log (tasks 2.4 and 2.8). Changing the alias in
  one place makes the extractor return the column header as the reported version, so every install
  fails with a version mismatch naming the header string. Decision [3] reasons about why the alias
  must be fixed and never notices the duplication.
- Fix: In plan.md § Implementation Tasks, add a task under item 1 declaring one readonly constant in
  `deploy/scripts/install.sh` (for example `VERSION_SMOKE_COLUMN="LAKEHOUSE_ENGINE_VERSION"`), and
  change tasks 1.2 and 1.3 to build the projection alias and the skip pattern from that constant
  rather than from a repeated literal.

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: plan.md § Implementation Tasks 2.1 and 2.8
- Issue: carried from round 1, and the revision made the coupling more load-bearing. Task 2.1 states
  the rule instead of removing it: "The default value `0.26.3` MUST track the curl stub's default
  `GH_ENGINE_TAG` of `v0.26.3`". The curl stub owns that default at `install.test.sh:224`. Task 2.8
  now depends on the same coupling, because it asserts a zero exit from a full run whose pass
  verdict needs the resolved tag and the stub's reported version to agree. A future full-run test
  that overrides `GH_ENGINE_TAG` then fails opaquely as a version mismatch in two tests rather than
  one.
- Fix: In plan.md § Implementation Tasks 2.1, change the stub's default reported version to derive
  from the tag default with the leading `v` stripped, rather than restating `0.26.3` as a second
  literal, and delete the sentence stating the tracking rule.

#### [TACTICAL_SHORTCUT] ADVISORY
- Location: decision-log.md § [2]; spec.md § Background, lines 38-40
- Issue: carried from round 1, still open. The second extractor is a tactical shortcut whose
  follow-up is recorded but not scheduled. Decision [2] states "No follow-up issue is filed: the
  consolidation trigger is a recorded spec constraint, not scheduled work", so nothing fires the
  trigger. The rejection of the consolidation rests on calling the `extract_query_value` path "a
  higher-risk change". Three existing tests already cover that path:
  `test_script_languages_append_preserves_existing`,
  `test_script_languages_replace_rust_idempotent` and `test_empty_script_languages_read_hard_fails`.
  The last hard-fails on exactly the mis-parse the decision fears, through the empty-value guard at
  `install.sh:1198-1202`.
- Fix: In decision-log.md § [2], either change the decision to consolidate now, adding a task that
  replaces both extractors with one taking the header literal and using the footer-suffix skip and
  naming those three tests as the regression net, or state that a tracked GitHub issue for the
  consolidation is filed and cite it inline in spec.md:38-40.

Otherwise no objection on this axis. `redact_credentials` reads five ambient globals and reads the
exapump config file, which matches the file's stated convention (`install.sh`, the comment above
`resolve_target_mode`: "Reads the ARG_* globals directly (consistent with the rest of the file)"), so
it introduces no new boundary a reader must learn. Its one-sentence responsibility is clear: replace
every configured credential in one text with a fixed placeholder. `classify_version_smoke` stays a
pure four-argument classifier that reads no globals and performs no I/O, and task 1.5 keeps every
message in the caller.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md § Implementation Tasks 1.6 and 2.8
- Issue: the revision's two longest task bodies exceed the 20-word procedural cap and join two ideas
  per sentence. Task 1.6 reads "Redact the `ARG_DSN` password segment in both its raw and its
  `url_decode`d form, because `run_sql` passes `ARG_DSN` on argv (`install.sh:1168`) and
  `extract_dsn_password` returns the still-encoded segment", 26 words with two `and` joins. Task 2.8
  reads "Point `HOME` at a sandbox fake home and write `write_local_deployment_fixture`'s output
  into `$HOME/.exasol/personal/deployments/<name>`, because `DEPLOYMENT_ROOT` derives from `$HOME`
  at `deploy/scripts/install.sh:43` and takes no environment override", 34 words with two `and`
  joins.
- Fix: In plan.md § Implementation Tasks 1.6, split each `because` clause into its own sentence, one
  instruction and one reason per sentence. In task 2.8, split the quoted sentence into three: the
  `HOME` instruction, the fixture-write instruction, and the `DEPLOYMENT_ROOT` reason.

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md § Summary, line 5; plan.md § Dependencies, lines 102-104
- Issue: carried from round 1, still open. Both passages open with a subject-less fragment and join
  two ideas with `and` past the 25-word descriptive cap. The Summary reads "Replaces the install
  script's error-kind fingerprint smoke test with a positive version check that calls
  `LAKEHOUSE_VERSION()` and compares the reported value against the resolved release version", 27
  words with no named actor. Dependencies reads "Prior art: commit `dac42c5` on the local branch
  `feat/add-lakehouse-version-udf` implemented this exact change, and commit `5b3cbed` reverted the
  `install.sh`, `install.test.sh`, and `docs/install.md` parts to split them into this follow-up", a
  35-word sentence behind a two-word fragment.
- Fix: In plan.md:5, name the actor and split into two sentences, for example "The install script
  calls `LAKEHOUSE_VERSION()` and compares the reported value against the resolved release version.
  That positive check replaces the error-kind fingerprint smoke test." In plan.md:102-104, replace
  the `Prior art:` fragment with a full sentence and split the `and`-joined clause into two
  sentences, one per commit.

Otherwise no objection on this axis. The three artifacts carry no em dashes outside table cells, no
semicolons, no contractions, and no superlatives or intensifiers. The four new `[plan-review]`
entries in decision-log.md state the current decision as fact and lead with the finding rather than
narrating the revision. RFC keywords stay normative throughout the spec delta and the Impact
section.
