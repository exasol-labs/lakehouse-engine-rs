# Plan Review Findings: add-export-iceberg-rest-write (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 18 (Blockers: 5, Advisory: 13)
- Intent Fidelity blockers: 1
- Human-escalation blockers: 1

Orchestrator questions:
- Decision [1] (deviation from interview answer 2): technically sound, but it overrides an explicit answer and adds user-visible side effects. Escalate: see the [INTENT_DRIFT] BLOCKER.
- Decision [5] (fault injection instead of E2E for the unknown-outcome retry): justified by `specs/testing.md` § Coverage rule and AGENTS.md. No escalation: see the first [SCOPE_REDUCTION] ADVISORY. Its test as written is not implementable within the repo's test rules: see the [HIDDEN_DEPENDENCY] BLOCKER.

## Premortem

Six months from now this plan failed catastrophically. Why?

1. A team points the export at a REST catalog that rejects `stage-create: true`. Every export fails in the pre-read check, parallel mode included, although only `SINGLE_COMMIT` needs a staged table. Failed and zero-row exports have meanwhile left namespaces that nobody asked for. Route: Intent Fidelity [INTENT_DRIFT].
2. The unknown-outcome proof never ran. Each commit attempt writes a manifest and a manifest list through an S3 FileIO before the POST, so the task 1.6 test needed an S3 stand-in that `specs/testing.md` forbids. The implementer weakened the test, and a duplicate-commit regression shipped unproven. Route: Feasibility [HIDDEN_DEPENDENCY].
3. An export without `ALLOW_HTTP` to an `http://` catalog and static `http://` storage succeeds over plaintext, because neither path has a plaintext gate and no task adds one. `export_plain_http_needs_allow_http` fails at task 4.3, and a late gate in the shared `CatalogSession::resolve` changes VS behavior too. Route: Task Breakdown [TRACEABILITY_GAP].

## Intent Fidelity

#### [INTENT_DRIFT] BLOCKER
- Location: decision-log.md § Design Decisions [1]; plan.md § Implementation Tasks 3.5; export/export-iceberg-table/spec.md § Background bullet 3 and Scenario "A failed SINGLE_COMMIT export leaves no table"
- Issue: Interview answer 2 chose "catalog `namespace_exists` call plus a probe write/delete with static storage keys in `generate_export_sql`; vended keys only get the catalog check". Decision [1] substitutes "the export creates the missing namespace levels, stage-creates the target table without committing it, and writes and deletes one probe object at the staged location". The rationale holds: answer 2's static probe has no location to write to before a table exists, a stage-create answer supplies one, and the result serves the issue's "invalid credentials fail the statement before any data is read" better for vended credentials. The substitution still changes what the feature does in two ways the requester never accepted. (a) A missing namespace is created before any row is read, so failed and zero-row exports leave namespaces behind. Issue #566 says only that a zero-row export creates no table. (b) Every export, parallel mode included, now needs a catalog that accepts `stage-create: true`. The plan verified this on Lakekeeper and the local fixture only, and it defines no behavior for a catalog that rejects it. Reviewer recommendation to the requester: accept decision [1], with (b) made explicit.
- Fix: Ask the requester to accept decision [1] or keep interview answer 2. If accepted: add a line to the Interview section pointing to decision [1] as the accepted replacement of answer 2; add to decision [1] Consequences that every export needs catalog support for stage-create; add a Background line or scenario to export/export-iceberg-table/spec.md stating that an export whose catalog rejects stage-create fails before any row is read with an error naming the catalog. If rejected: restore answer 2 in task 3.5 (namespace existence check, static-key probe only where a location exists), move namespace creation into the writer, delete the step "the namespace that the export created SHALL remain", and delete "even when it then writes no rows or fails" from Background bullet 3.
- Escalation: HUMAN (it overrides an explicit interview answer and changes user-visible side effects, so only the requester can accept it)

#### [SCOPE_REDUCTION] ADVISORY
- Location: decision-log.md § Design Decisions [5]; plan.md § Verification, row "A commit retried after an unknown outcome adds its rows once"
- Issue: Issue #566 accepts "E2E tests on the Lakekeeper stack cover every case above". Decision [5] proves the unknown-outcome case with a fault-injection test instead. The deviation is justified and needs no escalation. `specs/testing.md` § Coverage rule admits "a fault-injection test alone" for "a cut-off answer, a transient failure and its retries", and AGENTS.md allows "fake servers only for fault injection". The stack's only fault tool, `scripts/proxy/filter`, answers HTTP 403 before it forwards (`specs/testing.md` line 147), so no E2E test can lose an answer after the catalog applied a commit. The remaining risk is an acceptance box ticked by a non-E2E test without the PR reviewer noticing.
- Fix: Add a plan.md § Impact bullet stating that the unknown-outcome acceptance item of #566 is proven by fault injection (decision [5]), so the PR description names the trade-off.

#### [SCOPE_REDUCTION] ADVISORY
- Location: decision-log.md § Design Decisions [3]; export/export-iceberg-table/spec.md § Background type mapping
- Issue: Issue #566 lists TIMESTAMP as a supported type. The plan refuses `TIMESTAMP(p)` with p > 6. Decision [3] gives a sound reason: a format-version 2 `timestamp` holds microseconds, and the issue also requires every value to read back unchanged. The spec records the refusal as a scoped exception, as AGENTS.md allows. It still narrows a supported type without the requester's agreement.
- Fix: Keep decision [3]. Add a plan.md § Impact bullet: "`TIMESTAMP(7)` to `TIMESTAMP(9)` columns are refused with a hint to cast to `TIMESTAMP(6)` (decision [3])."

## Feasibility

#### [HIDDEN_DEPENDENCY] BLOCKER
- Location: plan.md § Implementation Tasks 1.2, 1.3, 1.6
- Issue: Each commit attempt of `append_files` and `commit_staged` runs `Transaction::fast_append` against the capture catalog. That writes a manifest and a manifest list through the table's FileIO before the POST, and reads the parent manifest list when the table has a current snapshot. Task 1.2 builds the FileIO from `StorageBackend::file_io()`, which supports only S3 and ADLS (`crates/lakehouse-catalog/src/storage.rs:119`), and the recorded Lakekeeper answers carry `s3://` locations. `append_files(ident, files, tag)` takes an identifier, not a table, so the test cannot supply a FileIO. Task 1.6 as written therefore needs either a second stand-in that answers S3 requests, which `specs/testing.md` § Testing strategy ("at most one fake per external system") and the test-strategy-review skill ("Never write a new service stand-in") forbid, or a test-only seam, which `specs/testing.md` also forbids ("No production function exists only so a test can inject a value"). After decision [5] this test is the scenario's only proof.
- Fix: In task 1.3, make the commit loop take the writer's `Table`, which the writer already holds, and reuse that table's FileIO when it rebuilds on a reloaded table. In task 1.6, build the starting table without a snapshot and with a FileIO over `iceberg::io::MemoryStorageFactory` (present at rev `da5cc19`; it accepts any scheme). Serve the tagged case as 504, then 409, then a reload whose tagged snapshot ends the loop without a manifest read. Serve the untagged case as 500 on the first commit, then an unchanged snapshot-free reload, then 200. State both choices in the task lines.
- Escalation: MECHANICAL (settled by reading `storage.rs`, the iceberg-rust checkout, and `specs/testing.md`)

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Implementation Tasks 2.1 and 4.2 (`export_refuses_unsupported_column_types`); § Context bullet 15
- Issue: Task 2.1 hard-codes `type_name` spellings and defers their check to task 4.2, while Group C builds 2.1 long before 4.2 runs. Context bullet 15 implies that GEOMETRY, HASHTYPE, and INTERVAL arrive without an `ExaType` variant. The wire-to-`ExaType` mapping lives in the SLC, not in the SDK (`exasol-udf-sdk` `spec.rs`: "the proto-to-ExaType mapping keeps its single owner in `exa-zmq-protocol`"), and Exasol UDF runtimes commonly pass INTERVAL, GEOMETRY, and HASHTYPE as strings. A classifier that reads `typ` first would then export them as `string`. The spike measured no live spelling: `spike_export_write.rs:629` uses a synthetic `ExaType::Unsupported("GEOMETRY")`. AGENTS.md requires live verification of claims about Exasol behavior.
- Fix: In task 2.1, state that the classifier keys on `type_name` with an allowlist (DECIMAL, DOUBLE, BOOLEAN, VARCHAR, CHAR, DATE, and TIMESTAMP without `WITH LOCAL TIME ZONE`) and refuses every other name whatever its `typ`. Make the first step of 2.1 a live probe of the eight spellings and their `typ` against the Docker Exasol, and record the measured values in § Context.

#### [NFR_IGNORED] ADVISORY
- Location: plan.md § Implementation Tasks 1.3 and 4.1; decision-log.md § Design Decisions [2]
- Issue: At the 300-writer cap, 300 writers commit to one table through optimistic concurrency. Each 409 reloads the metadata with all snapshots, up to 300, so catalog transfer grows roughly with the square of the writer count. The retry budget of 20 is measured only up to the spike's 8 concurrent creates and the E2E's 16 writers. Task 4.1 keeps `export_writer_layout_sets_writer_count`, whose two 300-writer cases now each commit 300 snapshots to the local fixture, with no note on duration or flakiness. Each writer also resolves two catalog sessions, so 600 OAuth grants arrive close together.
- Fix: In task 4.1, state that the two 300-writer cases commit 300 snapshots each. In task 4.3, record their duration and the highest commit attempt count any writer needed. If any writer needed more than half of the retry budget, add a task that scales the back-off with the writer count or raises `commit.retry.num-retries`.

#### [NFR_IGNORED] ADVISORY
- Location: decision-log.md § Design Decisions [4]; plan.md § Implementation Tasks 3.3 and 5.3
- Issue: Two security consequences go undocumented. (a) ADR `allow-http-threaded-as-vended-selector-parameter` makes `ALLOW_HTTP` "the operator's consent gate". On a virtual schema only its owner sets it. As an export option, every exporting user can consent to sending vended session tokens over plaintext. (b) A direct `LAKEHOUSE_EXPORT` call controls the whole writer spec. A forged `export_id` equal to the `lakehouse.export-id` property of an export-created table passes the 409 check of task 1.2, so the call appends rows to that existing table. A forged `allow_http` skips consent. Task 5.3 documents only that a direct call "can create a table anywhere that identity may write".
- Fix: Extend task 5.3: state that any exporting user can set `ALLOW_HTTP`, and that a direct call can append rows to any table an earlier export created and can enable plaintext transport. Add one sentence to decision [4] naming who holds the consent for an export.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] ADVISORY
- Location: recorded specs/export/export-options/spec.md, Scenario "TRUNCATE, REPLACE, and CREATED BY are refused"; delta export/export-iceberg-table/spec.md, Scenario "An export into an existing table is refused"
- Issue: The recorded scenario, which this plan leaves unchanged, requires the error to "state that the export only appends rows" (`crates/lakehouse-engine/src/export.rs:143`, `docs/export.md:40`). The new scenario refuses an existing table because "the export only creates new tables". A user who removes `REPLACE` as the first message suggests is then refused by the second.
- Fix: Add a DELTA:CHANGED block for that scenario to export/export-options/spec.md whose THEN requires the error to state that the export never truncates or replaces a table, which stays true after #567. Add the message change to task 3.2, the expectation change to task 4.1, and the `docs/export.md` line to task 5.1.

#### [COMPLETENESS_GAP] ADVISORY
- Location: export/export-iceberg-table/spec.md, Scenario "A failed SINGLE_COMMIT export leaves no table"; plan.md § Implementation Tasks 4.2 (`export_failed_single_commit_leaves_no_table`)
- Issue: The GIVEN "a source that raises an error after some of its rows are read" and the test's "divides by zero at row 500" assume that the writer runs before the error. The writer is a SET script grouped by a row-number key, a constant under `SINGLE_COMMIT`, so Exasol most likely evaluates the whole source before the first writer call. The existing `FAILING_SOURCE` tests rely on exactly that. The test then passes even if the writer created the table before it committed. The positive scenario's empty `metadata-log` covers the mechanism, so the defect is a misleading proof, not a missing one.
- Fix: Check on the Docker Exasol whether the row-500 error fires before the writer's first row. If it does, change the GIVEN to "a SINGLE_COMMIT export whose statement fails after its writer has written data files". In task 4.2, trigger the failure with `KILL STATEMENT` from a second session once a data file with the export's name prefix appears in storage, and assert that precondition in the test.

#### [COMPLETENESS_GAP] ADVISORY
- Location: export/export-iceberg-table/spec.md § Scenarios; plan.md § Implementation Tasks 1.2 and 1.4 (error messages)
- Issue: Two statements that export into the same new table name both pass the pre-read check. Tasks 1.2 and 1.4 define the loser's errors ("already exists" and "was created by another statement"), but no scenario states that the loser fails and that none of its rows reach the winner's table. No test proves the export-ID check that guards this case.
- Fix: Add a scenario to export/export-iceberg-table/spec.md: two parallel exports into the same new table name; one succeeds, the other fails with an error naming the table, and the table holds only the winner's rows. Map it to a Lakekeeper E2E test in task 4.2 and in § Scenario Coverage.

#### [COMPLETENESS_GAP] ADVISORY
- Location: export/export-iceberg-table/spec.md § Background; plan.md § Implementation Tasks 1.1, 3.5, 3.6
- Issue: Three inputs have no defined outcome. (a) Duplicate source column names, as in `EXPORT (SELECT 1 AS A, 2 AS A FROM DUAL)`: an Iceberg schema needs unique names, so `create_table` fails after rows are read with an error that names no column, unless Exasol refuses first. (b) A CONNECTION with ADLS storage: issue #566 puts Azure out of scope, but `StorageBackend::file_io()` builds an ADLS FileIO, so the export attempts untested ADLS writes. (c) A SigV4 CONNECTION, which is Glue's Iceberg REST route: task 1.1 signs write calls with SigV4, while the issue puts Glue out of scope.
- Fix: In task 3.5, refuse (b) and (c) before any row is read with an error naming the unsupported storage or catalog route. In task 3.6, refuse (a) at the type check with an error naming the repeated column. Add the refusals to task 5.1 and one unit test each.

#### [COMPLETENESS_GAP] ADVISORY
- Location: export/export-iceberg-table/spec.md § Background bullet 5
- Issue: AGENTS.md requires quoting the governing Iceberg spec section for type handling. The delta quotes § Primitive Types only. The export is the engine's first Iceberg writer, and the Parquet appendix of the Iceberg spec governs the files that tasks 3.6 and 3.7 write: field IDs, the physical types for `decimal`, and `timestamp` as microseconds with `isAdjustedToUTC = false`. The planning notes read that appendix but the delta does not quote it.
- Fix: Add one Background bullet quoting the Iceberg spec's Parquet type mapping rows for `decimal`, `date`, `timestamp`, and `string` and its field-ID rule, and state that the written files follow them.

## Task Breakdown

#### [TRACEABILITY_GAP] BLOCKER
- Location: export/export-options/spec.md § Background bullet `ALLOW_HTTP` and Scenario "Plain HTTP needs ALLOW_HTTP"; plan.md § Implementation Tasks 1.2, 3.2, 3.4
- Issue: The scenario requires an export to fail before reading when "a CONNECTION whose catalog address or storage endpoint uses `http://`" runs without `ALLOW_HTTP = 'true'`. No task implements that refusal for the catalog address or for a static storage endpoint, and today neither is gated. `CatalogSession::resolve(catalog_uri, warehouse, creds)` takes no `allow_http` (`crates/lakehouse-catalog/src/session.rs:98`). `catalog_storage_props()` passes no plaintext rule to the FileIO that the export writes through. The only plaintext gates are the vended resolution (`storage.rs:247`) and the STS endpoint. Task 3.2 only parses the option. Task 3.4's "CatalogSession::resolve, with allow_http passed in" contradicts its own "The adapter keeps its behavior", because a gate in the shared `resolve` would newly refuse virtual schemas with an `http://` catalog address. The Background claim that the option "has the meaning of the virtual schema property of the same name" is therefore false for the catalog address. On the local stack, `export_plain_http_needs_allow_http` would see the export succeed.
- Fix: Add a Group B task after 3.4: in the export path only, refuse a catalog address or a static S3 endpoint with the `http` scheme before any catalog request when the export's `ALLOW_HTTP` is false, with an error that names the `ALLOW_HTTP` export option, not the virtual-schema property. Reword the vended-path and STS messages the same way when the export calls them. Delete "with allow_http passed in" from task 3.4. In export/export-options/spec.md § Background, replace "It has the meaning of the virtual schema property of the same name" with the export's own rule.
- Escalation: MECHANICAL (settled by reading `session.rs`, `storage.rs`, and `creds.rs`)

#### [TASK_GRANULARITY] ADVISORY
- Location: plan.md § Implementation Tasks 1.2, 3.6
- Issue: Task 3.6 integrates the whole writer: type check, runtime, session, create or stage, writer ID, rolling Parquet writer, fresh session, and commit. It carries no [expert] tag, while the smaller commit-loop tasks do. Task 1.2 bundles five catalog calls, the 409 property check, and two FileIO sources in one untagged task.
- Fix: Tag task 3.6 [expert], or split it into first-row setup (type check, create or stage) and file writing with commit. Split task 1.2 into the namespace and existence calls, and the create and stage calls with their FileIO resolution.

Checked without objection: every scenario of the four deltas maps to a test in § Scenario Coverage; Groups A, C, and B share spec deltas or source modules, and B's dependency on A and C is declared.

## Design Depth

#### [ARCHITECTURE_DRIFT] BLOCKER
- Location: plan.md § Implementation Tasks 3.4, 3.5 and § Context bullet 18; architecture.md § Components, export line
- Issue: Task 3.4 extracts the CONNECTION resolution of `adapter::resolve_connection_config`, built from `read_connection` and `storage_block` in `adapter/connection.rs`, for the export to call. Task 3.5 runs the pre-read check on "the adapter's current-thread runtime (`build_runtime`)", a private function of `adapter/mod.rs`. The export therefore depends on vs-adapter, but the delta states "export ... depends on: type-mapping, lakehouse-catalog", and Context bullet 18 repeats it. Today `export.rs` imports nothing from `adapter`.
- Fix: Move the shared CONNECTION resolution and `build_runtime` into a module outside `adapter/`, name that destination in task 3.4, and add it to architecture.md § Components with vs-adapter and export depending on it. Alternatively, add vs-adapter to the export line's depends-on list and to Context bullet 18.
- Escalation: MECHANICAL

#### [TACTICAL_SHORTCUT] BLOCKER
- Location: decision-log.md § Design Decisions [6]; plan.md (no open-question list)
- Issue: Decision [6] defers vended-credential refresh to "a follow-up (#TBD)", but no artifact lists it as an open question. AGENTS.md § Workflow requires it: "otherwise mark `(#TBD)` and list it as an open question". The shortcut has real reach. Under `SINGLE_COMMIT` one writer exports the whole source with the stage-create credentials, so a vended export whose writer runs past the one-hour lifetime fails after all its data is written. plan.md § Impact does not mention the limit.
- Fix: Add `## Open Questions` to plan.md with the entry "Vended-credential refresh through `client.refresh-credentials-endpoint` (#TBD): needs a human-opened issue." Add a plan.md § Impact bullet: "With vended credentials a writer must finish within the vended lifetime (Lakekeeper: one hour). A SINGLE_COMMIT export runs one writer for the whole source, so it reaches the limit first."
- Escalation: MECHANICAL

#### [TACTICAL_SHORTCUT] ADVISORY
- Location: plan.md § Implementation Tasks 1.3, 1.4; decision-log.md § Design Decisions [2]
- Issue: The commit loop works around iceberg-rust's crate-private `TableCommit` builder with a private `iceberg::Catalog` impl that stubs 13 of 15 methods, and task 1.4 depends on the update order of `TableMetadataBuilder::from_table_creation` measured in spike lk_6. A rev bump of iceberg-rust can change either without a compile error, and only the Lakekeeper E2E would catch it. No follow-up is scheduled.
- Fix: Add to decision [2] Consequences: "A rev bump of iceberg-rust runs the Lakekeeper export E2E before merge." Add an open question (#TBD) for an upstream change that exposes a commit's requirements and updates, so the capture catalog can go.

Checked without objection: decision [2] is the only `Promotes to ADR: yes` entry; its Rationale names rule-2 criteria 2 and 4 and the ADR search result, its Decision holds no signatures, paths, or flags, and its `Architecture: § Components, § Constraints` line matches the delta's lakehouse-catalog component and new commit-loop constraint. ADR conflict check: `allow-http-threaded-as-vended-selector-parameter`, `catalog-http-times-out-on-silence`, `connection-wins-when-set-vended-addressing`, `two-vended-static-selectors-on-disjoint-inputs`, `vended-extraction-forked-policy-shared`, `lakekeeper-storage-credential-separate-from-engine-reader`, and `lakekeeper-bucket-wide-write-accepted-risk` are not contradicted. The delta's BASE hash matches `specs/architecture.md`, and every CHANGED block keeps the base lines it does not edit.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md § Summary, § Context bullet 14, § Dependencies; export/export-iceberg-table/spec.md, Scenario "Static and vended storage credentials both write the table"
- Issue: Context bullet 14 packs eight measurements into one bullet, against the plan template's "one fact per bullet". The Summary clause "an unsupported column type fails each writer once" does not read on one pass. § Dependencies cites `/home/snips/Documents/code/lakehouse-engine-rs-spike566/...`, an unmerged local path that no other reader can open and that puts a username into a public repository. The scenario uses "read through a virtual schema", whose definition lives only in export/export-into-script § Background.
- Fix: Split Context bullet 14 into one bullet per measurement. Rewrite the Summary clause as "a column of an unsupported type fails the export before any data is written". Replace the absolute spike paths with the spike worktree name and the test names. Add a Background bullet to export/export-iceberg-table/spec.md: "Read through a virtual schema: export/export-into-script § Background."
