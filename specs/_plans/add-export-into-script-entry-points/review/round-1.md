# Plan Review Findings: add-export-into-script-entry-points (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 21 (Blockers: 9, Advisory: 12)
- Intent Fidelity blockers: 1
- Human-escalation blockers: 1

## Premortem

Six months from now this plan failed. Three stories:

1. **Stale base.** The planner researched on `feat/fix-regexp-like-pushdown` (merge base #492). The plan branch sits on origin/main `313b09c`, and the two trees differ in the SDK pin (0.30.0 against 0.33.0), the toolchain (1.94 against 1.98.1), the E2E crate layout, and `specs/architecture.md`. The implementer follows paths that do not exist, or the record step applies an architecture delta that deletes four components and restores `PARALLELISM_FACTOR (default 8)`. Routed to Feasibility, Requirement Quality, Task Breakdown, and Design Depth.
2. **Two writers under SINGLE_COMMIT.** A later issue makes SINGLE_COMMIT write one Iceberg commit. Exasol documents `RANDOM()` as returning a value in [0,1], so one row of a large export gets key 1 under `FLOOR(RANDOM() * 1)`, a second writer runs, and the table gets two commits. Routed to Feasibility.
3. **Grants and bypass.** An operator re-runs `install.sh` to upgrade. `CREATE OR REPLACE` drops the export scripts' ACCESS grants, so every export fails until someone finds the printed block, which #565 had asked the installer to run. Later, an export-role holder calls `LAKEHOUSE_EXPORT_WRITER` directly with a forged writer spec, which skips every check, and `docs/security.md` never warned about it. Routed to Intent Fidelity and Feasibility.

## Intent Fidelity

The SINGLE_COMMIT refinement is not drift on its own. Interview answer 3 ("constant GROUP BY key (no IPROC)") requires one key value and no `IPROC()`. `FLOOR(RANDOM() * 1)` meets both only if `RANDOM()` stays below 1, which Exasol does not document (see Feasibility).

#### [SCOPE_REDUCTION] BLOCKER
- Location: decision-log.md § Design Decisions [3]; plan.md § Implementation Tasks 2.2; export/export-into-script/spec.md § Scenario "The installer creates the export scripts and prints their grants"
- Issue: #565's acceptance criterion reads "The installer creates both scripts and grants them access to the catalog CONNECTION." The plan has the installer print a role block for the operator to run, so the installer grants nothing. The interview did not cover this point, and plan.md names neither a deviation nor an open question. Decision [3]'s reason, "the operator creates the CONNECTION after the install", holds only for a first install. On a re-install the CONNECTION exists, and `CREATE OR REPLACE` drops every ACCESS grant on the export scripts (plan.md § Impact), so exports fail until someone re-runs the printed lines. HUMAN: this changes what the installer does for the operator, and only the requester can accept a changed acceptance criterion.
- Fix: Ask the requester to choose, then record the choice as a decision-log entry that quotes the #565 criterion. Option (a): keep the printed role block, and state in plan.md § Context that the printed template satisfies the criterion instead of executed grants. Option (b): add an installer input, for example `--export-connection <name>`, under which `install.sh` creates `LAKEHOUSE_EXPORT_ROLE_<schema>` and runs its grants after `create_engine_scripts` when that CONNECTION exists. Option (b) also needs an `install.test.sh` case and a THEN step in the installer scenario.
- Escalation: HUMAN

#### [SCOPE_REDUCTION] ADVISORY
- Location: export/export-into-script/spec.md § Background bullet 7 and § Scenario "A column of an unsupported type fails the export naming the column and its type"; decision-log.md [2] Consequences
- Issue: #565 says "A column of an unsupported type ... fails the export with an error that names the column and its type", with no exception. The per-row guard does not run on an empty source. An empty GEOMETRY, HASHTYPE, or INTERVAL column exports 0 rows without error. An empty TIMESTAMP WITH LOCAL TIME ZONE column fails with Exasol's own message, which names no column. The spec narrows the scenario to "a source that returns at least one row". The measured Exasol behavior forces this narrowing, but the requester has not agreed to it.
- Fix: List the empty-source narrowing in plan.md § Context as a deviation from #565, and name it in the PR description for the requester to acknowledge.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: plan.md § Context bullets 3 and 8, § Dependencies, task 1.1; architecture.md § Constraints
- Issue: The plan's SDK evidence comes from a spike against `exasol-udf-sdk` 0.30.0 built in `rust:1.94-trixie`, and § Dependencies states "The version stays 0.30.0". The plan branch pins `exasol-udf-sdk` and `exasol-udf-macros` 0.33.0 (`Cargo.toml:46-47`, `Cargo.lock`), `rust-toolchain.toml` 1.98.1, and `UDF_BUILDER_IMAGE ?= rust:1.98.1-trixie`. `notes/planning.md` shows the research ran on `feat/fix-regexp-like-pushdown`, which pins 0.30.0. crates.io lists an `export` feature for 0.33.0. Four SDK facts are still unverified on 0.33.0: the hook signature and `ExportSpec` fields, the `ctx.next()` first-row behavior that task 1.5's count depends on, `ctx.current_user()`, and the `TestContext` builders. The local container's SLC is 0.30.0, so `make test-e2e` cannot load a 0.33.0 `.so` until the SLC is updated.
- Fix: Rebuild the spike against the pinned 0.33.0 SDK in `UDF_BUILDER_IMAGE`, and load it on an SLC of the same version. Re-confirm the export hook, the `ExportSpec` field names, the `while ctx.next()?` count with the first row already loaded, `ctx.current_user()`, and the `TestContext` builders task 1.5 names. In plan.md, replace every "0.30.0" and "1.94" with "the pinned SDK version" plus the re-measured facts. Change task 1.1 so it adds `export` to the existing 0.33.0 pin.
- Escalation: MECHANICAL

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: plan.md task 1.4 (`GROUP BY FLOOR(RANDOM() * <G>)`); decision-log.md [2]; export spec § Scenario "SINGLE_COMMIT runs one writer for the whole cluster"; architecture.md § Data Flow and § Constraints
- Issue: Exasol's documentation for `RANDOM` (Usage notes) states: "Without any parameters this interval is [0,1]". That interval is closed. A row for which `RANDOM()` returns 1 gets key G. With `SINGLE_COMMIT = 'true'`, G is 1, so `FLOOR(RANDOM() * 1)` can produce keys 0 and 1 and run two writers. That breaks "exactly one writer SHALL run in the whole cluster", the contract later issues rely on for a single commit. At G = 300 the expression can produce 301 groups, above the cap. The literal key from the interview had no such risk, and decision [2] replaced it only to fix guard ordering. The per-row probability is unknown, because Exasol does not document the generator's resolution.
- Fix: Change the group expression to one that cannot reach G, for example `MOD(FLOOR(RANDOM() * G), G)` or `LEAST(FLOOR(RANDOM() * G), G - 1)`. Re-measure on Exasol 2025.1.16 and 8.29.13 that the guard error still comes before Exasol's type error for all four types, at G = 1 and G > 1. Record the result in decision [2], and update task 1.4, the architecture Data Flow line, and the `sql_tests.rs` shape cases.
- Escalation: MECHANICAL

#### [HIDDEN_DEPENDENCY] BLOCKER
- Location: plan.md task 1.12; § Verification bullet 2
- Issue: #565 requires that "a multi-node cluster confirms that only one writer runs". On one node, SINGLE_COMMIT and the default both plan G = 1, so `export_single_commit_runs_one_writer` cannot tell them apart. Only `cloud_export_writer_counts` proves the criterion. Task 1.12 says "the cluster needs the export scripts from a release that contains this change". No release can contain the change before this PR merges, and `CLOUD_LHVS` is provisioned out of band (`cloud_e2e_test.rs` only creates the schema and the virtual schema). No task deploys the branch build to the cluster. The fallback in § Verification ("names that as an open limitation") ships with the acceptance criterion unverified.
- Fix: Add a task that uploads the branch-built `.so` to the cluster's BucketFS through the path `make bench` uses. The task then creates `CLOUD_LHVS.LAKEHOUSE_EXPORT` and `CLOUD_LHVS.LAKEHOUSE_EXPORT_WRITER` with the by-hand DDL from task 2.4, and runs `cloud_export_writer_counts` before the PR leaves draft. Replace the "release that contains this change" text in task 1.12 and in `deploy/README.md` with that procedure. If no cluster can be provisioned, list the unverified criterion in plan.md as an open question for the requester, instead of accepting it in advance.
- Escalation: MECHANICAL

#### [NFR_IGNORED] BLOCKER
- Location: plan.md task 2.5; decision-log.md [3]; export spec § Background bullet 5
- Issue: The export role grants every exporting user `EXECUTE` on `LAKEHOUSE_EXPORT_WRITER` and `ACCESS ON CONNECTION ... FOR SCRIPT` on it. Such a user can call the writer directly, as in `SELECT <schema>.LAKEHOUSE_EXPORT_WRITER('<forged writer spec>', c) FROM t GROUP BY ...`. That call skips every check `LAKEHOUSE_EXPORT` makes: options, type guards, writer count, and SINGLE_COMMIT. Today the writer only counts rows. In later issues it writes with the CONNECTION's credentials, so this is the export counterpart of `docs/security.md` § Who can bypass the adapter. The repo's test-strategy rule states that `docs/security.md` must state known bypasses, and task 2.5 does not state this one. Task 2.5 also keeps `docs/security.md:22`, which tells operators never to give a reader a CONNECTION grant, while the export role grants `CONNECTION` to users who are usually readers too. Finally, task 2.5's claim that the CONNECTION privilege does not let a user "read its password" is unmeasured. Exasol's native IMPORT and EXPORT connectors send a CONNECTION's user and password to its address.
- Fix: Extend task 2.5 with three edits. (1) Add a `docs/security.md` subsection: a holder of the export role can run `LAKEHOUSE_EXPORT_WRITER` directly with any writer spec and any rows, so the writer treats its writer spec as untrusted input. (2) Reconcile `docs/security.md:22` with the export role's `CONNECTION` grant. (3) Drop "but not read its password", or replace it with a measured statement of what Exasol's native IMPORT and EXPORT do with the CONNECTION's credentials. Also add a Consequences line to decision [3] that states the writer bypass.
- Escalation: MECHANICAL

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md tasks 1.10 and 1.11
- Issue: Each "before any data is read" proof depends on `SELECT 1/(a - a) AS x FROM <table>` failing only when Exasol reads its rows. The tests assert only that the option error appears. If Exasol ever folds or reorders that expression, the tests can pass or fail for the wrong reason.
- Fix: In task 1.10, add one positive control: an export of the same source with valid options fails with the division error.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: export/export-into-script/spec.md § Scenario "An empty source exports zero rows"; § Background bullet 8
- Issue: The scenario states without qualification that exporting "a query that returns no rows" "SHALL succeed with a row count of 0". The plan's own measurement contradicts this for one input: an empty source with a TIMESTAMP WITH LOCAL TIME ZONE column fails (decision-log.md [2] Consequences; `notes/planning.md` "TSLTZ -> Exasol 'Column type not supported' even when empty"). Background bullet 8 says only that the type check does not run, which a reader takes to mean success. As written, the spec states false behavior.
- Fix: Restrict the scenario's GIVEN to a source whose columns all have supported types. Add one Background sentence: an empty source with a TIMESTAMP WITH LOCAL TIME ZONE column fails with Exasol's own error, which names no column, and an empty source with a GEOMETRY, HASHTYPE, or INTERVAL column exports 0 rows.
- Escalation: MECHANICAL

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: packaging/single-so-two-entry-points/spec.md § Background (DELTA:CHANGED); packaging/version-udf/spec.md unmarked description and Background
- Issue: The CHANGED Background replaces the recorded bullet "built only inside the `UDF_BUILDER_IMAGE` Rust builder image the Makefile names" with "built only inside the `rust:1.94-trixie` builder image". The Makefile pins `rust:1.98.1-trixie`, so the merged spec would state a false fact, and the bullet adds a version to a spec, which spec-conciseness Rule 4 forbids. The planner copied the delta from the #492-era file. The version-udf delta's unmarked Background has the same cause. It holds changelog bullets that the recorded spec has already removed: "This delta ADDS FOUR scenarios...", "This delta SUPERSEDES no recorded clause...", and "Two install-script functions read...". Unmarked text does not merge, but reviewers read it as the spec.
- Fix: Rebuild the single-so `## Background` block from the current `specs/packaging/single-so-two-entry-points/spec.md`. Keep the version-free builder-image bullet, and edit only the entry-point lines. In the version-udf delta, replace the unmarked description and Background with the current recorded text, or delete them so the file holds only the two CHANGED scenarios.
- Escalation: MECHANICAL

#### [IMPLEMENTATION_LEAKAGE] ADVISORY
- Location: export spec § Background bullet 1; § Scenario "The writer count never exceeds 300"
- Issue: No scenario step depends on "Exasol folds option names to upper case and accepts only quoted option values". The 300-cap scenario says "WHEN the export plans its writers THEN it SHALL plan 300 writers". That describes an internal step a user cannot observe (spec-conciseness Rule 4).
- Fix: Drop the sentence from Background bullet 1 and keep it in `docs/export.md`. Rephrase the scenario as an outcome: "an export whose node count × `WRITERS_PER_NODE` exceeds 300 SHALL run at most 300 writers".

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: export spec § Scenario "WRITERS_PER_NODE sets the number of writers on each node"
- Issue: The title promises a count per node, but the THEN step asserts only the cluster total N × W. No test reads the count per node.
- Fix: Retitle the scenario "WRITERS_PER_NODE sets the writer count", or add a THEN step on the count per node with a matching assertion in `cloud_export_writer_counts`.

#### [COMPLETENESS_GAP] ADVISORY
- Location: export spec § Background bullet 6; plan.md task 1.9
- Issue: The Background implies that every Exasol type except the four refused types reaches the writer. No scenario or test exports a BOOLEAN, DATE, TIMESTAMP(9), DOUBLE, CHAR, or DECIMAL(36,s) column, yet the writer's SDK must accept the metadata of each one. The probes recorded only VARCHAR and DECIMAL(9,0).
- Fix: Give the 1,000-row source in task 1.9 one column of each supported Exasol type.

#### [COMPLETENESS_GAP] ADVISORY
- Location: export spec § Scenario "No CONNECTION value appears in the generated SQL, an error, or the script output"; plan.md § Verification bullet 1
- Issue: No test covers the THEN step "no script output SHALL contain the CONNECTION's password". The plan relies on code review for it, while `specs/testing.md` § Coverage rule expects test proof.
- Fix: Move the script-output clause to an architecture Constraint ("the export module emits no script output"). Otherwise, name code review as its only proof in § Scenario Coverage.

## Task Breakdown

#### [TRACEABILITY_GAP] BLOCKER
- Location: plan.md tasks 1.6 to 1.12; § Parallelization; § Dead Code Removal; § Scenario Coverage
- Issue: The plan cites test files that do not exist on the plan branch: `crates/lakehouse-engine/tests/two_entry_points_test.rs`, `tests/common/e2e_harness.rs`, `tests/e2e_export_test.rs`, `tests/cloud_e2e_test.rs`, and `tests/e2e_version_udf_test.rs`. On origin/main the E2E suites live in `crates/lakehouse-e2e`. The harness is `crates/lakehouse-e2e/tests/common/e2e_harness.rs`. The local suite is one `e2e_local` binary, whose modules `e2e_local/main.rs` declares and whose tests set up through `crate::fixtures::stack()`. The entry-point test is `e2e_local/two_entry_points.rs`, and the cloud suite is `crates/lakehouse-e2e/tests/cloud_e2e_test.rs`. Task 1.8's "OnceLock setup" and "list the binary on the `test-e2e` Makefile line" contradict `specs/testing.md` § E2E suites: "A new test module of the Local Docker suite is declared in `crates/lakehouse-e2e/tests/e2e_local/main.rs`."
- Fix: Rewrite tasks 1.6 to 1.12, § Parallelization, § Dead Code Removal, and § Scenario Coverage against the `crates/lakehouse-e2e` layout. Add `e2e_local/export.rs`, declare it in `e2e_local/main.rs`, set it up through `fixtures::stack()` plus a lazy fixture, and read error text through `error_text`. Edit `so_exports_three_entry_point_symbols` in `e2e_local/two_entry_points.rs`. Point the version and cloud rows at `e2e_local/version_udf.rs` and `crates/lakehouse-e2e/tests/cloud_e2e_test.rs`. Drop the Makefile edit.
- Escalation: MECHANICAL

#### [CLUSTER_INCOHERENCE] ADVISORY
- Location: plan.md § Parallelization
- Issue: Groups A and B both state "Depends on: none". Both groups write the export DDL (tasks 1.7 and 2.1). Task 2.4's `docs/export.md` also quotes the error prefix and the messages that tasks 1.3 to 1.5 define, so Group B depends on Group A's message text.
- Fix: Mark task 2.4 as depending on Group A, or move `docs/export.md` into Group A. State the DDL text, including line breaks, once in task 2.1, and have task 1.7 cite it.

#### [SCOPE_CREEP] ADVISORY
- Location: plan.md tasks 1.4, 1.5, and 1.10
- Issue: `sql_tests.rs` asserts "no sentinel CONNECTION value in the output", but `sql.rs` never receives a CONNECTION value. `export_tests.rs` proves the same property at the place where the value exists. Task 1.10 also runs "two or three invalid examples per scenario" in E2E, while `options_tests.rs` holds the full table. test-strategy-review Rule 3 says to test each behavior once, at the lowest layer that can prove it.
- Fix: Drop the sentinel case from `sql_tests.rs`. In task 1.10, use one invalid example per scenario. One example is enough to prove "before any data is read".

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md task 2.4
- Issue: `docs/install.md:468` reads "One `.so` supplies all three RUST entry points". Task 2.4 updates only the script counts.
- Fix: Add `docs/install.md:468` to task 2.4, with "five RUST entry points".

## Design Depth

No `[ADR_OVERPROMOTION]`: all three decision-log entries are `Promotes to ADR: no`. The `export` module is deep. It keeps the options grammar, the writer spec format, and the guard SQL behind two functions, and `sharding` owns the 300 cap alone. No objection on depth or leakage.

#### [ARCHITECTURE_DRIFT] [ADR_CONFLICT] BLOCKER
- Location: architecture.md (whole file)
- Issue: The BASE comment is `cfa3c9c`, which is the `specs/architecture.md` blob of the #492-era branch. On the plan branch, `git hash-object specs/architecture.md` returns `fd936f3`. Each CHANGED block copies a stale section, so recording would delete current content. The deleted content includes the vs-properties, table-filter, and table-identity components, the REFRESH TABLES data flow, the identifier-note interfaces, and the constraints on the `lakehouse-catalog` boundary, the catalog HTTP client, proxy support, and `VS_SRCS`. Recording would also restore stale facts: `TABLE_MAP`, "Arrow IPC bytes", Rust 1.94, SDK 0.30.0, and Arrow 58. Two of the restored lines contradict the accepted ADR `one-scan-shard-per-node-with-threads-on-every-core`. The first is "`PARALLELISM_FACTOR` (default 8)", where the ADR sets the default to 1. The second is "Exasol runs one scan invocation per shard row on its node VM pool", where the ADR states that the scalar scan runs one VM per node and that the VM pool does not apply to it. `/speq:spec-merge` stops the record on this stale base.
- Fix: Regenerate architecture.md from the current `specs/architecture.md`. Set BASE to `fd936f3b39e69379607d16e357d7e3024bd62462` and copy each of the four sections verbatim. Then apply only this plan's edits: five entry points on udf-entry-points, the sharding line, the new export component, the installer's export role template, the three export Data Flow lines, the two script interfaces, the writer-count constraint, and the export sentence in the credential constraint.
- Escalation: MECHANICAL

#### [TACTICAL_SHORTCUT] ADVISORY
- Location: decision-log.md [2] Consequences
- Issue: The type guard raises its error through a failing cast, so users see Exasol's cast-error prefix. It also adds one `TYPEOF` call and four `LIKE` tests per column per row to every export. Its ordering depends on optimizer behavior that was measured on two Exasol versions. The decision defers the cost under "Performance work is out of scope for #565" but names no follow-up.
- Fix: Add a Consequences bullet naming a follow-up issue, `(#TBD)` listed as an open question, to measure the guard's cost and revisit the mechanism in the export performance work.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: export spec § Background bullet 6
- Issue: The bullet quotes Iceberg's `uuid` and `fixed(L)` but does not say that they are the Iceberg counterparts of Exasol HASHTYPE. A reader cannot tell why the bullet cites them.
- Fix: After the `uuid` and `fixed(L)` quotes, add "(the Iceberg counterparts of HASHTYPE)". After `timestamptz`, add "(the counterpart of TIMESTAMP WITH LOCAL TIME ZONE)".

#### [PROSE_BLOAT] ADVISORY
- Location: packaging/single-so-two-entry-points/spec.md § Feature description and § Background bullet 1
- Issue: Both state that `LAKEHOUSE_DISTRIBUTE_FILES` is a separate LUA SET script that its own DDL creates and that is not part of the `.so`.
- Fix: Keep the statement in the Background and remove it from the description.
