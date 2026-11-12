# Plan Review Findings: add-export-iceberg-append (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 17 (Blockers: 8, Advisory: 9)
- Intent Fidelity blockers: 3
- Human-escalation blockers: 3

## Premortem

Six months from now this plan failed. These are the likely reasons:

1. A nightly job appends into a Spark-owned table after its owners added an optional column. The export writes NULL into that column and reports success. Issue #567 asked for a failure when "a table column is missing from the export". Dashboards show NULL for weeks. Category: [INTENT_DRIFT], first Intent Fidelity finding.
2. A filtered export returns zero rows on most days, and its source has lost the required `id` column. The job reports a row count of 0 until the first day that has rows, and then it fails at night. Category: [SCOPE_REDUCTION], zero-row finding.
3. A 12-node cluster appends into a Spark table that does not set `commit.retry.num-retries`. Twelve writers commit at once, and each has 5 attempts. Some writers run out of attempts after others have committed, and the rerun duplicates the committed rows. Category: [NFR_IGNORED].
4. Two scheduled exports into a new table overlap. The second export's pre-read check finds the table that the first export just created, so both exports succeed and the table holds both sources. This breaks the recorded "one winner" scenario. Category: [REQUIREMENT_CONFLICT].

## Intent Fidelity

#### [INTENT_DRIFT] BLOCKER: an optional table column that the export lacks is filled with NULL
- Location: `export/export-iceberg-append/spec.md` § Background, Binding bullet (line 10) and Mismatches bullet (line 11); § Scenario "A table that another engine created takes the source rows by column name"; plan.md task 2.2 `append_binds_spark_table_columns_by_name`
- Issue: Issue #567 § Behavior says "The export fails before writing anything if: a table column is missing from the export". The plan narrows this rule to required columns. The Binding bullet says "An optional table column that no source column binds is written as NULL", and the scenario says "the column that the source lacks SHALL read back as NULL". The issue lists "a NULL would go into a required column" as a separate kind, so "a table column is missing" covers optional columns too. Interview Q4 answered "As in issue", and no Design Decisions entry records the narrowing. HUMAN: the narrowing changes what the feature does for a user, and the literal rule has a cost that only the requester can weigh. Under the literal rule, a table with a column the export cannot write, such as the fixture's `meta struct` or `ratio float`, can never take an append.
- Fix: Ask the requester to choose one option. (a) Follow #567. Make every unbound table column a mismatch, and delete the NULL sentence from the Binding bullet. Change the scenario so that the source names every column. Add a mismatch case for a missing optional column to "Every column mismatch is reported in one error". Drop the omitted `note`, `meta`, and `ratio` columns from task 2.2, and state in § Open Questions that a table with a struct or float column then cannot take an append. (b) Keep the NULL fill. Add a decision-log Design Decisions entry that records the requester's agreement, the rejected alternative, and the reason. State in the spec description that only required table columns must be bound.
- Escalation: HUMAN

#### [SCOPE_REDUCTION] BLOCKER: a source with no rows skips every mismatch check
- Location: decision-log.md § Design Decisions [1] Consequences; `export-iceberg-append/spec.md` description (line 3) and § Scenario "An empty source appends nothing"; plan.md task 3.1 `empty_source_appends_nothing`
- Issue: The #567 acceptance criterion reads "Each mismatch kind fails before any data is written, and one error lists all mismatches." Decision [1] says "A source with no rows runs no writer, so it checks no column and appends nothing." An empty source that lacks a required column, carries an extra column, or has an incompatible type therefore succeeds with a row count of 0. The spec description says the export "refuses with one error that lists every mismatch before it writes any data" and states no exception. The empty-source scenario does not say whether the source columns match. Nobody asked the requester: Interview Q3 covers only the required-column NULL check. HUMAN: any fix trades one part of the ask against another. The hook receives only column names (`ExportSpec.source_column_names`, exasol-udf-sdk 0.33.0 `spec.rs` lines 71-79). Checking names in the hook would catch an empty source, but a source with both kinds of mismatch would then get two errors in turn, and decision [1] rejected that.
- Fix: Ask the requester to choose one option. (a) Accept the gap. Add to spec § Background: "A source with no rows runs no writer, so no column is checked: it succeeds with a row count of 0 even when its columns do not match the table", which mirrors export/export-into-script § Background. Change the GIVEN of the empty-source scenario to a source whose columns do not match the table, and record the acceptance in the decision log. (b) Also check name mismatches in the pre-read hook against the loaded table. A source with both kinds of mismatch then gets a second error for its type mismatches. Update decision [1] and the Mismatches bullet, and add a scenario for an empty source that lacks a required column. Before the requester chooses, you can verify on the Docker Exasol (AGENTS.md) whether a writer SELECT can invoke one writer for an empty source. If it can, the plan can meet both parts of the ask.
- Escalation: HUMAN

#### [SCOPE_REDUCTION] BLOCKER: format-version 1 and 3 tables are refused without agreement
- Location: `export-iceberg-append/spec.md` § Background, pre-read bullet (line 9: "refuses a table whose format version is not 2") and Iceberg bullet (line 28: "Format version 1 is out of scope"); plan.md task 1.5 `appendable`, task 2.1 `spark_v1`
- Issue: The #567 § Out of scope section lists only "Partitioned tables" and "Changing the table schema". The plan also refuses every table of format version 1 or 3. No interview answer and no decision-log entry supports that refusal. Version 1 was the default format for new Iceberg tables before Iceberg 1.4.0, so many tables that other engines created are version 1. The pinned iceberg-rust (da5cc19) appends to both versions. In `crates/iceberg/src/transaction/snapshot.rs` it builds v1 manifests (`build_v1`, lines 270-271) and v1 manifest lists (lines 504-505). For v3 it writes the manifest list with `first_row_id` and sets the snapshot's row range (lines 510-516 and 554-558). HUMAN: the refusal drops tables that the ask covers.
- Fix: Ask the requester whether appends must cover format versions 1 and 3. If yes, make these changes. Drop the version refusal from `appendable`. Treat a v1 partition spec whose fields are all `void` transforms as unpartitioned. Write a v3 table's `write-default` into an unbound optional column. Turn `spark_v1` in task 2.1 into a successful v1 append case in task 2.2. If no, add `## Out of Scope` to plan.md that names format versions 1 and 3 and cites the requester's agreement, and correct the v3 reason as the Iceberg-quote finding below requires.
- Escalation: HUMAN

#### [INTENT_DRIFT] ADVISORY: the record-time spec changes go against the Interview Q1 answer, and the decision log does not say so
- Location: decision-log.md § Interview Q1; plan.md § Sequencing With the Stacked PRs, task 5.1
- Issue: Interview Q1 answered "New feature `export/export-iceberg-append` only, ADDED scenarios, no MODIFIED on `export-iceberg-table` or `export-options`". Task 5.1 later writes a REMOVED scenario and a CHANGED Background for export/export-iceberg-table, and a CHANGED description for export/export-into-script. The deferral serves the purpose of that answer, because stack fixes stay unaffected, and it keeps the library consistent. However, no decision-log entry tells the requester that the plan departs from the literal answer. No plan review sees these deltas either, because task 5.1 writes them after review.
- Fix: Add a decision-log Design Decisions entry titled "Record-time deltas on export-iceberg-table and export-into-script". Give the reason: the refused-table scenario would contradict the new feature. Give the rejected alternative: leave the contradiction in the library. Set `Promotes to ADR: no`.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER: task 1.7 allows a fake-server test to close a coverage gap
- Location: plan.md task 1.7
- Issue: The fallback in task 1.7 is "transport-level tests of the existing-table branch of `prepare` on the `spawn_server` and `RecordingCatalog` helpers ... with recorded Lakekeeper answers. Add no fake server." `spawn_server` is a fake server: line 1 of `crates/lakehouse-catalog/src/exported_test_support_tests.rs` describes the module as "a fake HTTP server". specs/testing.md § Testing strategy says "A coverage gap is closed by a unit test of pure logic, never by a multi-module host test or a fake", and "A fake server injects only the faults an E2E test cannot produce". An E2E test can reach the existing-table branch.
- Fix: Delete the transport-level fallback from task 1.7. Close a coverage gap only with unit tests of pure functions. One way is to extract the `prepare` decision for a loaded table (new, existing, or refused) as a pure function over table metadata.
- Escalation: MECHANICAL

#### [NFR_IGNORED] ADVISORY: parallel appends into tables of other engines get 4 commit retries per writer
- Location: plan.md § Context, bullet 6; `export-iceberg-append/spec.md` § Background commit bullet; plan.md task 3.1 `parallel_append_commits_once_per_writer`
- Issue: A table that another engine created usually does not set `commit.retry.num-retries`. Each writer then gets 4 retries (`DEFAULT_RETRIES` in `retry.rs`), with waits from 0.1 s to 0.8 s. The create path sets 20 retries because up to 300 writers commit at once. A parallel append into such a table has the same contention with a fifth of the attempts. The only parallel test runs 4 writers on an export-created table, which has 20 retries. When a writer runs out of attempts, the statement fails after other writers have committed (ADR `export-has-no-final-commit-per-writer-commits`), so a rerun duplicates their rows. Each writer also loads the full metadata with every snapshot (`load_table_with_all_snapshots`) when it opens the table and before each commit, and that cost grows with a long-lived table's history. The plan measures neither cost.
- Fix: Add an append into `spark_target` with `WRITERS_PER_NODE = '16'` to task 2.2, and assert that every row appears exactly once. State in the spec Background and in the docs section of task 4.1 that a parallel append into a table without `commit.retry.num-retries` gets 4 retries per writer, and that `SINGLE_COMMIT` or fewer writers avoid the contention. As an alternative, record a decision on a client-side minimum retry count that leaves the table's properties unchanged.

#### [HIDDEN_DEPENDENCY] ADVISORY: the implementation branch is less isolated from the stack than the Summary claims
- Location: plan.md § Summary, § Dependencies table, task 3.1
- Issue: The Summary says "All new behavior sits in a new feature and new modules, so fixes to the stacked PRs #607 to #610 do not change this plan". This holds for the plan itself: all plan files are new, and they touch no stacked branch. It does not hold for the implementation branch. That branch changes behavior in six stacked source files: `batch.rs` (binding, builders, NULL arrays), `mod.rs` (`write_group`), `iceberg_sink.rs`, `iceberg_rest.rs`, `target.rs`, and `iceberg_write.rs`. Every stack fix to those files forces a rebase of the append branch. The claim "a later stack fix rebases with at most a local conflict" is unverified. The table also omits edits that the tasks need:
  - Task 3.1 reuses `Target`, `summary_values`, `sources()`, and the `STATIC` and `VENDED` targets. All of them are private to `e2e_lakekeeper/export_write.rs` (lines 82, 211, and 36).
  - `binding.rs` needs `batch::arrow_type`, which is private.
  - specs/testing.md § Test code says "A helper that a second test file needs moves into its area's support module". The plan instead makes `assert_refused_before_reading` and `objects_under` `pub(super)` in `e2e_local/export.rs`.
- Fix: Rewrite the Summary to say that the plan artifacts are new files and that the implementation edits the listed stacked files. Add a § Dependencies row for `e2e_lakekeeper/export_write.rs` (helper visibility) and extend the `batch.rs` row with the visibility of `arrow_type`. Then choose one way to handle the shared E2E helpers. One way is to move them into a support module as specs/testing.md requires. The other way is to record in the decision log that they stay in the stacked files until the stack merges, which keeps the stacked diff small, and to add a follow-up task that moves them. Finally, state either that group A rebases after each stack fix, or that group A starts after the review of #608 settles.

## Requirement Quality

#### [COMPLETENESS_GAP] BLOCKER: three Iceberg spec quotes are cut short or rest on an unverified claim
- Location: `export-iceberg-append/spec.md` § Background, last two bullets (lines 27-28)
- Issue: AGENTS.md requires the plan to quote the normative Iceberg section and to record each deviation. I checked the quotes against `format/spec.md` on apache/iceberg main. Three of them do not hold:
  1. § Sorting is cut off mid-sentence. The full sentence is "Writers should use this default sort order to sort the data on write, but are not required to if the default order is prohibitively expensive, as it would be for streaming writes." The cut drops the condition. As a result, "the export does not sort" reads as compliant, although the Background never says why the exemption applies to a table that declares a sort order.
  2. In § Default values, the sentence after the quoted one is "The write default for a field must be written if a field is not supplied to a write." That sentence governs what an unbound optional column receives, yet the Binding bullet writes NULL without citing it. NULL is correct only because default values arrive with format version 3: the spec's Version 3 list includes "Default value support for columns". The Background does not say this.
  3. § Row Lineage: the Background says "The export does not track them, so refusing a format-version 3 table is this feature's scoped exception." The spec assigns lineage to an added row through inheritance: "the `_row_id` field for an added row is set to `null` and assigned when reading", and a manifest's `first_row_id` "is assigned when writing the snapshot root file". The fast append of the pinned iceberg-rust writes both (`transaction/snapshot.rs`, lines 510-516 and 554-558). The stated reason for the v3 refusal is therefore unverified, and the library contradicts it.
- Fix: Make three changes in spec.md § Background. First, quote the full Sorting sentence, and either state that the export is a streaming write or record "does not sort" as a scoped exception. Second, add the write-default sentence and state that format version 2 has no default values, so an unbound optional column is NULL. Third, replace the Row Lineage reason. If v3 stays refused (see the format-version finding), give the missing write-default handling as the reason, or verify the row lineage of a v3 append on the Docker stack.
- Escalation: MECHANICAL

#### [REQUIREMENT_CONFLICT] BLOCKER: the recorded "one winner" scenario no longer holds once appends exist
- Location: plan.md § Sequencing With the Stacked PRs, bullet 2; export/export-iceberg-table § Scenario "Two exports that create the same table leave one winner" (stack top); docs/export.md line 87
- Issue: That scenario says "exactly one statement SHALL succeed" when two exports into a missing table "run at the same time". After this plan, the timing matters. Suppose one export's writer creates the table before the other export runs its pre-read check. The second export then finds the table, appends to it, and also succeeds, so the table holds both sources. The E2E test on the stack still passes only because its slow cross-join source delays both writers until both pre-read checks are done ("so both statements pass the pre-read check", `export_write.rs`). Task 5.1 removes "An export into an existing table is refused", but it leaves this scenario and docs line 87 unchanged.
- Fix: Add a CHANGED scenario for export/export-iceberg-table to § Sequencing. Its GIVEN states that both statements pass the pre-read check while the table does not exist. One added sentence states that an export whose pre-read check finds the table appends to it (export/export-iceberg-append). In task 4.1, change docs/export.md line 87 the same way.
- Escalation: MECHANICAL

#### [COMPLETENESS_GAP] ADVISORY: the spec does not say that a parallel append that fails on a required NULL leaves other writers' rows
- Location: `export-iceberg-append/spec.md` § Background, NULL bullet (line 25) and commit bullet (line 26)
- Issue: Issue #567 says the export "fails before writing anything if ... a NULL would go into a required column". Interview Q3 settled on a check in each writer. Without `SINGLE_COMMIT`, the writers that read no NULL therefore commit, and the failed statement leaves their rows in the table. The spec says only that the failing writer "commits none of its rows". A reader of the issue expects the table to receive no rows.
- Fix: Add this sentence to the NULL bullet: "Without `SINGLE_COMMIT`, the writers that read no such NULL commit their rows, so the failed export leaves them in the table." Cite ADR `export-has-no-final-commit-per-writer-commits`. Say the same in the docs section of task 4.1.

## Task Breakdown

#### [TRACEABILITY_GAP] BLOCKER: the plan cites (#TBD) although issue #567 is open
- Location: `export-iceberg-append/spec.md` description (line 3); plan.md § Summary (line 5) and § Open Questions, item 1 (line 60)
- Issue: Open Question 1 says "The append issue is not open yet ... A human opens it". Issue #567 is open. Its title is "feat(export): [3/8] append exports to existing Iceberg tables", and its body is the requested issue text. AGENTS.md says "Cite an existing issue as (#83), otherwise mark (#TBD)". As written, Open Question 1 would lead a human to open a duplicate issue.
- Fix: Replace "(#TBD)" with "(#567)" in spec.md and in plan.md § Summary. Delete Open Question 1 and renumber the others.
- Escalation: MECHANICAL

#### [TRACEABILITY_GAP] ADVISORY: the required-NULL error in task 1.3 lacks the table name that the spec requires
- Location: plan.md task 1.3
- Issue: Task 1.3 says "A NULL for a non-nullable field fails with an error that names the column". The spec's NULL bullet and the failed `SINGLE_COMMIT` scenario require "an error that names the table and the column". `BatchBuilder` does not know the table name.
- Fix: State in task 1.3 or task 1.6 where the table name enters the error, for example in `write_rows`, which can wrap the batch error. Check for the table name in the NULL case of `failed_single_commit_append_leaves_table_unchanged`.

#### [TRACEABILITY_GAP] ADVISORY: the failed `SINGLE_COMMIT` E2E test cannot detect leftover data files
- Location: plan.md task 2.2 `failed_single_commit_append_leaves_table_unchanged`
- Issue: The scenario "A failed SINGLE_COMMIT append leaves the table unchanged" says "the data files that the writer wrote SHALL NOT remain". Both planned cases fail before the writer writes any file. A writer passes rows to the sink in batches of 8,192 rows (`BATCH_ROWS` in `batch.rs`), so a 1,000-row source never fills a batch. The source-error case fails at row 500, and the NULL case fails at the batch's `take`, before `sink.write`. The data-file assertion therefore passes even when `abort` leaves files behind. The create-mode test proves the same clause with a proxy fault that comes after the data file is written (`export_failed_single_commit_leaves_no_table` in `e2e_local/export.rs`, which uses the `PROXIED` connection and an HTTP 403 under the table's metadata path).
- Fix: Add a third case. An export through the direct connection creates a per-run table, and an append into that table runs through `PROXIED`, with a proxy filter line that answers its metadata path with HTTP 403. Assert from the proxy log that the writer wrote a data file, and assert that no object remains under the table's data location.

#### [TRACEABILITY_GAP] ADVISORY: task 4.1 leaves other docs lines that describe only new tables
- Location: plan.md task 4.1
- Issue: Task 4.1 edits lines 9, 42, and 83 of docs/export.md and adds a section. Other lines on the stack top describe only new tables and stay wrong after the change:
  - line 84: "Each opens the table at its first row: the first creates it"
  - line 101: "the new table has one snapshot and no earlier metadata version"
  - line 103: staged creation as the `SINGLE_COMMIT` mechanism
  - lines 107-108: "After a failed `SINGLE_COMMIT` export no table exists"
  - line 117: the rights of the catalog identity
  - line 119: "vends for the new table"

  AGENTS.md requires the docs update in the same PR.
- Fix: Extend task 4.1 to change those lines. State that each of them applies to a new table, and add the append counterpart for each. One writer appends in one commit without staged creation. The files of a failed `SINGLE_COMMIT` append lie under the existing table's location. The catalog identity needs load and commit rights on the table.

Task granularity and parallelization: no objection. One implementation group shares one spec delta and one module tree, and group R correctly waits for the stack to merge.

## Design Depth

#### [ARCHITECTURE_DRIFT] BLOCKER: the plan changes the architecture, but no architecture delta records it
- Location: plan.md § Sequencing With the Stacked PRs, bullet 2, third sub-bullet; `specs/_plans/add-export-iceberg-append/architecture.md` does not exist
- Issue: plan.md states changes to Components, Data Flow, and Interfaces: the export owns the binding of columns, the pre-read step loads the table and refuses one it cannot append to, the writer spec carries whether the table exists, the writer binds columns with widenings, and a new Interfaces bullet covers the append. No architecture delta holds these changes. The Constraints section changes too, and the plan does not list it: the stack top's `specs/architecture.md` line 110 says "`SINGLE_COMMIT = 'true'` makes the table appear only with its data", which does not fit an existing table. The deferral is not forced. I copied this checkout to a scratch directory and added a delta whose BASE is the stack-top hash `d66175d2e63a6bd647976470a7a65e31dac52b9f`, and `speq plan validate add-export-iceberg-append` passed. The delta is a file local to the plan, and no stack fix edits it.
- Fix: Write `specs/_plans/add-export-iceberg-append/architecture.md` now, with BASE `d66175d2e63a6bd647976470a7a65e31dac52b9f`. Add CHANGED blocks for Components, Data Flow, Interfaces, and Constraints. Copy each block whole from the stack-top `specs/architecture.md`, then edit it as § Sequencing describes. The Constraints block states that `SINGLE_COMMIT` appends to an existing table in exactly one commit. Reduce the third sub-bullet of § Sequencing to "task 5.1 copies each block again from main and refreshes the BASE".
- Escalation: MECHANICAL

#### [INFORMATION_LEAKAGE] ADVISORY: the narrowing in `batch.rs` relies on the precision bounds in `binding.rs`
- Location: plan.md tasks 1.2 and 1.3; decision-log.md [1] Consequences
- Issue: Task 1.2 puts the precision bounds in `binding.rs`: p ≤ 9 for `int` and p ≤ 18 for `long`. Task 1.3 adds `Int32` and `Int64` builders to `batch.rs` and feeds them through `rescaled`, which returns an `i128`. The narrowing to `i32` or `i64` is safe only because `binding.rs` enforced the bound, and nothing enforces that link between the two modules. An unchecked `as` cast would wrap values silently if the bounds ever changed. Decision [1] also says that `binding.rs` owns "the accepted types for both the create and the append path", but task 1.2 takes the base mapping from Exasol to Arrow types from `batch::arrow_type`.
- Fix: In task 1.3, require checked narrowing (`i32::try_from`, `i64::try_from`) that fails with the existing refusal "too large for the table column", and add a unit test for it. Reword decision [1] Consequences: `batch.rs` keeps the mapping from Exasol to Arrow types, and `binding.rs` owns which table types accept a source type.

ADR checks: no objection. All four decision-log entries say `Promotes to ADR: no`, so `[ADR_OVERPROMOTION]` does not apply. I found no `[ADR_CONFLICT]`: the plan keeps one commit per writer, the commit loop that re-checks the writer's tag, and retries owned by each operation (ADRs `export-has-no-final-commit-per-writer-commits`, `export-commit-loop-rechecks-writer-tag`, and `state-changing-catalog-requests-retried-by-their-operation`). The new modules pass the quick diagnostic: `binding.rs` (one function, one owner of the acceptance rules) and `iceberg_append.rs` (one appendability check, which both the hook and the writer reuse).

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY: inaccurate Summary, a bullet about the plan itself, and overloaded bullets
- Location: plan.md § Summary, § Context bullet 7, § Sequencing bullet 2; `export-iceberg-append/spec.md` § Background, Mismatches bullet
- Issue: The second sentence of the Summary is inaccurate (see the [HIDDEN_DEPENDENCY] finding). Context bullet 7 ("The spec delta's Background quotes the Iceberg table spec sections ...") describes the plan, not the problem. Bullet 2 of § Sequencing puts three deltas and their phrase substitutions into nested sub-bullets of more than 60 words each. The Mismatches bullet packs four mismatch kinds, the timing of the check, and the shape of the error into two sentences.
- Fix: Rewrite the second sentence of the Summary as the [HIDDEN_DEPENDENCY] finding describes. Delete Context bullet 7. Split each sub-bullet of § Sequencing into one sentence per change. Turn the Mismatches bullet into a list of the four kinds, followed by one sentence on the timing and one on the error.
