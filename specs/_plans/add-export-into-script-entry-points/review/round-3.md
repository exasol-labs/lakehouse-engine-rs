# Plan Review Findings: add-export-into-script-entry-points (round 3)

## Summary
- Axes checked: 6/6
- Total findings: 6 (Blockers: 4, Advisory: 2)
- Intent Fidelity blockers: 2
- Human-escalation blockers: 2

## Previous Blocker Recheck
- Resolved: round-1 `[SCOPE_REDUCTION]` on installer behavior. The requester chose `--export-connection`; decision [3], tasks 2.2-2.3, and the three installer scenarios now make the installer run the grants when the CONNECTION exists.
- Resolved: round-1 `[UNSTATED_ASSUMPTION]` on SDK compatibility. The context and spike use the pinned SDK and SLC 0.33.0, and task 1.1 only enables its existing `export` feature.
- Resolved: round-1 `[UNSTATED_ASSUMPTION]` on `RANDOM() = 1`. Task 1.4 and decision [2] use `MOD(FLOOR(RANDOM() * G), G)`.
- Resolved: round-1 `[HIDDEN_DEPENDENCY]` on multi-node deployment. Tasks 1.12-1.13 now create the CONNECTION, pass the SQL port and password, grant script access, deploy the branch artifact, and require the cloud proof before the PR leaves draft unless the requester accepts the open deviation.
- Resolved: round-1 `[NFR_IGNORED]` on direct writer invocation. Task 2.5 documents the bypass and decision [3] treats the writer spec as untrusted input.
- Resolved: round-1 spec, test-layout, and architecture blockers. The empty-source scenarios no longer contradict each other, the deltas use the current recorded backgrounds, E2E tests use `crates/lakehouse-e2e`, and the architecture BASE and changed sections match the current architecture.
- Resolved: round-2 `[REQUIREMENT_CONFLICT]` on installer output. The existing-CONNECTION and missing-CONNECTION cases are now separate scenarios and agree with task 2.2.

## Premortem

Six months from now this plan failed in three ways:

1. The required installer E2E never existed. A real `exapump` result did not match the stub, so `--export-connection` printed a warning and installed no grants while CI stayed green.
2. A random writer key left one bucket empty. The exact writer-count E2E failed intermittently, and production used fewer writers than `WRITERS_PER_NODE` promised.
3. An empty query with an unsupported type silently succeeded. A customer interpreted that success as proof that the schema was exportable, then the first non-empty export failed.

## Intent Fidelity

#### [SCOPE_REDUCTION] BLOCKER
- Location: `plan.md` § Context, line 20; `export/export-into-script/spec.md` scenarios "An empty source with an unsupported column type exports zero rows" and "A column of an unsupported type fails the export naming the column and its type"
- Issue: The issue requires "A column of an unsupported type ... fails the export with an error that names the column and its type." The plan instead specifies that the same unsupported schema succeeds when it returns no rows. The issue separately requires an empty source to report 0, but does not authorize unsupported schemas as an exception. Documenting the deviation and asking for later acknowledgement does not constitute requester agreement.
- Fix: Ask the requester to choose between preserving the unconditional unsupported-type rejection and accepting the empty-unsupported-source exception. If rejection remains required, add a live Exasol investigation task for a generated query shape that evaluates each column's declared `TYPEOF` once even when the source is empty, then replace the zero-row exception scenario and test. If the exception is accepted, record that direction change in `decision-log.md` § Review Findings and retain the current scenario and documentation task.
- Escalation: HUMAN - this changes observable export success into failure, or vice versa, and only the requester can accept that scope change.

#### [SCOPE_REDUCTION] BLOCKER
- Location: `plan.md` § Context, line 21; § Verification, lines 159-169; issue acceptance criterion "E2E tests cover every case above"
- Issue: The issue requires E2E coverage of the installer creating both scripts and granting their CONNECTION access. The plan uses a shell stub plus a manual run. `export_role_user_exports_rows` proves that hand-written grants work, not that `install.sh --export-connection` discovers the real CONNECTION, parses real `exapump` output, restores grants after `CREATE OR REPLACE`, and grants the role correctly. The existing `install-script-e2e` CI job already runs `install.sh` against live Exasol, so this is a dropped acceptance case rather than an unavailable test tier.
- Fix: Add a Group B task to extend `.github/workflows/ci.yml`'s `install-script-e2e` job: create a test CONNECTION, invoke `install.sh --export-connection <name>` twice, and query the real privilege views after the second run for the role's `EXPORT`, `CONNECTION`, two `EXECUTE`, and two script-scoped `ACCESS` grants. Map that live check to the installer scenarios in § Scenario Coverage and remove the Context deviation.
- Escalation: HUMAN - retaining only stub/manual evidence drops an explicit acceptance criterion and therefore requires requester approval.

## Feasibility

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: `plan.md` task 2.2, lines 95-98
- Issue: The plan inserts `--export-connection <name>` into both a SQL string literal (`WHERE CONNECTION_NAME = '<NAME>'`) and identifier positions (`GRANT CONNECTION <name>`) without defining or validating the accepted identifier grammar. A valid quoted Exasol name such as `"Glue-Prod"` will not render correctly, while a quote in the argument can alter the lookup SQL. The existing tests cover only a missing value, not identifier boundaries.
- Fix: Define the flag as accepting simple unquoted Exasol identifiers and add one shared validation step before any SQL, with tests for lowercase normalization, quotes, whitespace, punctuation, and overlength input. If quoted CONNECTION names must be supported, instead plan separate SQL-literal escaping and quoted-identifier rendering and test both forms.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `export/export-into-script/spec.md` Background line 10 and scenario "WRITERS_PER_NODE sets the writer count", lines 54-59; `plan.md` tasks 1.4, 1.9, and 1.12
- Issue: The Background correctly says random assignment may leave a group empty, but the scenario promises exactly N x W writers whenever the source has 100 rows per planned writer. More rows make an empty random bucket unlikely, not impossible. The planned local and cloud tests assert exact counts, so the implementation can neither guarantee the SHALL requirement nor make those tests deterministic.
- Fix: Preserve the exact N x W requirement by replacing random assignment with a live-verified deterministic grouping expression that produces every key from 0 through G-1 whenever the source has at least G rows; add a planning spike and tests for G=1, G=300, and a source with exactly G rows. If the requester instead wants a maximum writer count, change the scenario, option documentation, architecture constraint, and tests to say "at most G"; that alternative changes user-visible behavior and must be approved before revision.
- Escalation: MECHANICAL - the current exact SHALL is already the selected contract, so the plan can mechanically preserve it and validate the required Exasol expression.

#### [COMPLETENESS_GAP] ADVISORY
- Location: `plan.md` tasks 1.2-1.3
- Issue: `WRITERS_PER_NODE` has no upper bound, and `group_count` multiplies it by the node count before applying 300. The current `shard_count` uses `saturating_mul`, but the extraction task and its named test do not explicitly preserve or pin that behavior. A value near `usize::MAX` can panic in a checked build or wrap in an optimized build if the extraction uses ordinary multiplication.
- Fix: State in task 1.2 that `group_count` uses `saturating_mul`, and make `group_count_caps_at_300` include `group_count(usize::MAX, usize::MAX) == 300`. Add an options test for the largest accepted numeric value and for a value that does not fit the chosen integer type.

## Task Breakdown

#### [TRACEABILITY_GAP] BLOCKER
- Location: `plan.md` § Verification checklist, lines 188-194
- Issue: The checklist runs `cargo test` without the repository-required explicit `--workspace`, runs `make test-e2e` without starting its stack, and replaces the required `make fmt-check lint` gate with separate `make lint` and `cargo fmt` commands. These conflict with `AGENTS.md` and `specs/testing.md`, so the implementation cannot claim verification discipline from the listed checklist.
- Fix: Change Test to `cargo test --workspace`, E2E to `make e2e-up test-e2e`, and the lint/format entries to the repository gate `make fmt-check lint`. Keep `make cross-udf-build`, the opt-in cloud proof, `make test-install`, and coverage as separate rows.
- Escalation: MECHANICAL - the repository's required commands settle the correction.

## Design Depth

No objection - axis checked. The architecture delta names the export component, both script interfaces, data flow, installer flag, security boundary, and shared 300-group constraint. All decisions remain local rather than ADRs, do not conflict with the accepted execution/privilege ADRs, and keep export validation and wire-format details behind the `export` module.

## Prose Quality

No objection - axis checked. The round-2 ambiguous CONNECTION wording and summary/history issues are corrected. The two `speq plan validate` warnings about four AND steps describe cohesive installer/documentation outcomes and do not make either scenario unclear.
