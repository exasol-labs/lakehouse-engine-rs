# Plan Review Findings: add-export-iceberg-rest-write (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 12 (Blockers: 1, Advisory: 11)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

Orchestrator questions: none. The one BLOCKER is MECHANICAL, because conforming to the ADR keeps every part of the request.

## Round-1 Blocker Recheck

- Resolved: [INTENT_DRIFT] Decision [1] replaces interview answer 2. The requester accepted decision [1] (decision-log.md § Review Findings [1]). Interview answer 2 now ends with "Superseded by decision [1] after review." Decision [1] Consequences state the stage-create requirement and the namespace side effect. export/export-iceberg-table § Background bullet 4 and the scenario "A catalog that refuses staged table creation fails the export before any data is read" define the outcome for a refusing catalog, and plan.md § Impact and task 5.1 repeat it.
- Resolved: [HIDDEN_DEPENDENCY] The unknown-outcome test needed an S3 stand-in. Task 1.3 now takes the writer's `Table` and keeps its FileIO on every rebuild. Task 1.6 builds the starting table from a snapshot-free answer with a FileIO over `MemoryStorageFactory`, and serves 504, then 409, then a tagged reload; and 500, then an unchanged reload, then 200. Verified at rev `da5cc19`: `io/file_io.rs:99` provides the factory, and `io/storage/memory.rs:87` strips only a `memory:` prefix, so `s3://` paths need no S3 server.
- Resolved: [TRACEABILITY_GAP] No task enforced the export's `ALLOW_HTTP`. Task 3.5 refuses an `http` catalog address, `oauth2_server_uri`, `aws_sts_endpoint`, or static `endpoint` on the export path before any request. Task 1.2 refuses a vended `http` endpoint before `resolve_vended_storage`. Both errors name the export option. Task 3.4 no longer passes `allow_http` into `CatalogSession::resolve`, whose signature stays unchanged (`crates/lakehouse-catalog/src/session.rs:98`), so virtual schemas keep their behavior. export/export-options § Background now states the export's own rule.
- Resolved: [ARCHITECTURE_DRIFT] The export depended on vs-adapter. Task 3.4 reads the CONNECTION in `export/target.rs`, and task 3.6 builds its own runtime, so no task calls `adapter::resolve_connection_config` or `build_runtime`. The delta's export line ("depends on: type-mapping, lakehouse-catalog") matches the tasks. The chosen route moves the adapter's CONNECTION parser into `lakehouse-catalog`, which contradicts an accepted ADR: see the new [ADR_CONFLICT] BLOCKER under Design Depth.
- Resolved: [TACTICAL_SHORTCUT] The vended-credential refresh follow-up was unlisted. plan.md § Open Questions lists it as `(#TBD)`, and § Impact states the one-hour limit and that a `SINGLE_COMMIT` export reaches it first.

## Premortem

Six months from now this plan failed catastrophically. Why?

1. A team exports through an Iceberg REST catalog that ignores `stage-create: true` and creates the table. The pre-read check leaves a one-column placeholder table that carries the export's ID. Each writer's create gets HTTP 409, finds a matching ID, and commits files whose schema differs from the table's. Route: Feasibility [UNSTATED_ASSUMPTION].
2. A later plan reads the new constraint that `lakehouse-catalog` owns "their mapping from a CONNECTION password's JSON fields" and moves more CONNECTION parsing into the catalog crate. ADR `catalog-crate-owns-credential-types-no-shared-types-crate` still says the parsers stay in the engine. The two records disagree, and no reviewer can tell which one binds. Route: Design Depth [ADR_CONFLICT].
3. A `SINGLE_COMMIT` commit gets HTTP 504 after the catalog applied it. The unknown-outcome branch of `commit_staged` was never tested, and it reports "was created by another statement" although the table holds the rows. The user runs the export again and gets "already exists". Route: Requirement Quality [COMPLETENESS_GAP].

## Intent Fidelity

#### [SCOPE_REDUCTION] ADVISORY
- Location: plan.md § Impact; decision-log.md § Design Decisions [3] and [5]
- Issue: Carried from round 1, not addressed. Two narrowings of issue #566 are justified but missing from § Impact, which the PR description draws on. Decision [5] proves the unknown-outcome acceptance item with a fault-injection test, not with the Lakekeeper E2E that #566 names. Decision [3] refuses `TIMESTAMP(7)` to `TIMESTAMP(9)`, although #566 lists TIMESTAMP as supported.
- Fix: Add two plan.md § Impact bullets: "A fault-injection test proves the unknown-outcome commit case of #566, because no E2E stack can lose an answer after the catalog applied a commit (decision [5])." and "`TIMESTAMP(7)` to `TIMESTAMP(9)` columns are refused with a hint to cast to `TIMESTAMP(6)` (decision [3])."

## Feasibility

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Implementation Tasks 1.2 (`stage_table`) and 3.6; export/export-iceberg-table § Background bullet 4
- Issue: The plan assumes that a catalog either honors `stage-create: true` or refuses it. Two other outcomes have no defined behavior. (a) A catalog that ignores the flag creates the table during the pre-read check. `stage_table(ident, schema, export_id)` takes the export ID, so the placeholder carries the export's `lakehouse.export-id`. Each writer's `create_table` then gets HTTP 409, finds a matching ID, and returns the one-column placeholder. Task 3.8 builds the batches from that placeholder schema, or the writer commits files whose schema differs from the table's. (b) Task 1.2 reports every refused stage answer as "the export needs a catalog that supports staged table creation". An HTTP 403 for an identity that may not create tables gets the same message, and the test of task 4.1 simulates the refusal with exactly such a 403 from the proxy filter.
- Fix: In task 3.6, stage the placeholder without `lakehouse.export-id`, then call `table_exists` on the name, and fail with an error that names the catalog when the table now exists. In task 1.2, make the stage refusal quote the HTTP status and the catalog's redacted error body, and state that staged table creation failed instead of asserting that the catalog lacks support. Apply the same wording to the THEN of the scenario "A catalog that refuses staged table creation fails the export before any data is read" and to task 5.1.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Implementation Tasks 3.4, last two sentences
- Issue: Task 3.4 states that a CONNECTION which the skipped adapter checks would refuse "fails the pre-read check at the catalog or the storage". That does not always hold. `ConnectionCreds::supplied_catalog_auth` (`crates/lakehouse-catalog/src/creds.rs:90`) maps a token together with a complete client pair, and a `client_id` without `client_secret`, to `Unauthenticated`. On a catalog without auth, such as the local fixture, the export then succeeds with a CONNECTION that `validate_exclusive_catalog_auth_creds` and `validate_oauth2_creds` (`crates/lakehouse-engine/src/adapter/connection.rs:330` and `:344`) refuse for a virtual schema. An `aws_external_id` without `aws_assume_role_arn` is ignored (`sts.rs:31`), and the export writes with the base keys.
- Fix: Run the adapter's Iceberg REST validators (`validate_creds` with `CatalogKind::IcebergRest`) on the export path, from the shared engine module that the [ADR_CONFLICT] fix creates. If the plan keeps them skipped, delete the claim from task 3.4 and list in task 5.1 which CONNECTIONs the export accepts that a virtual schema refuses.

#### [NFR_IGNORED] ADVISORY
- Location: decision-log.md § Design Decisions [4]; plan.md § Implementation Tasks 3.3 and 5.3
- Issue: Carried from round 1, not addressed. (a) ADR `allow-http-threaded-as-vended-selector-parameter` makes `ALLOW_HTTP` "the operator's consent gate". As an export option, every user who may export gives that consent. (b) A direct `LAKEHOUSE_EXPORT` call controls the whole writer spec. A forged `export_id` that equals the `lakehouse.export-id` property of an export-created table passes the HTTP 409 check of task 1.2 and appends rows, so "It creates only new tables" does not hold for a direct call. A forged `allow_http` skips the consent.
- Fix: Add one sentence to decision [4]: "For an export, the user who runs the statement gives the consent." Extend task 5.3: any exporting user can set `ALLOW_HTTP`, and a direct call can append rows to a table that an earlier export created and can enable plain HTTP.

## Requirement Quality

#### [COMPLETENESS_GAP] ADVISORY
- Location: export/export-iceberg-table, Scenario "A commit retried after an unknown outcome adds its rows once"; plan.md § Implementation Tasks 1.4, 1.6, and 4.2 (`export_concurrent_writers_commit_once_with_the_export_id`); decision-log.md § Design Decisions [5]
- Issue: The scenario covers both commit modes, but both of its tests exercise `append_files` only. The unknown-outcome branch of `commit_staged` in task 1.4 (load the table, succeed only when a snapshot carries the tag) has no test. Decision [5] says "The Lakekeeper E2E proves the tags that the check reads". The check reads `lakehouse.export-id` and `lakehouse.export-writer`, but the E2E test asserts only `lakehouse.export-id`.
- Fix: In task 1.6, add `staged_unknown_outcome_with_created_table_commits_once`: the `assert-create` commit gets HTTP 504, the client's retry gets 409, and the load returns the table with a tagged snapshot; `commit_staged` returns success. Record that load answer with the other fixtures and list the test in § Scenario Coverage. In task 4.2, make `export_concurrent_writers_commit_once_with_the_export_id` also assert 16 distinct `lakehouse.export-writer` values per table.

#### [COMPLETENESS_GAP] ADVISORY
- Location: export/export-iceberg-table § Background bullet 5 and Scenario "A CONNECTION the export cannot write through is refused before any data is read"; plan.md § Implementation Tasks 1.2 and 3.4
- Issue: Background states "The export writes only onto S3-compatible storage", but task 3.4 refuses only static Azure fields. With `use_vended_credentials`, a catalog whose warehouse lies on ADLS returns an `abfss://` location and a SAS token. `resolve_vended_storage` builds an ADLS backend for it (`crates/lakehouse-catalog/src/vended.rs:38`), and task 1.2 builds the table's FileIO from that backend. The export then writes to ADLS, a route that issue #566 leaves out and no test covers.
- Fix: In task 1.2, refuse a staged or created table whose location scheme is not `s3` or `s3a`, before `probe_storage`, with an error that names the scheme. Add "or through a catalog whose table location is not on S3-compatible storage" to the scenario's WHEN, and add one unit test.

#### [COMPLETENESS_GAP] ADVISORY
- Location: export/export-iceberg-table, Scenario "A failed SINGLE_COMMIT export leaves no table"; plan.md § Implementation Tasks 4.2 (`export_failed_single_commit_leaves_no_table`)
- Issue: Carried from round 1, not addressed. Under `SINGLE_COMMIT` the writer is a SET script grouped by a constant key, so Exasol most likely evaluates the whole source, and raises the division by zero at row 500, before the writer's first call. The test then passes even when the writer would commit a table before it fails.
- Fix: First check on the Docker Exasol whether the row-500 error fires before the writer's first row. If it does, change the GIVEN to "a SINGLE_COMMIT export whose statement fails after its writer has written data files". In task 4.2, end the statement with `KILL STATEMENT` from a second session once a data file with the export's name prefix appears in storage, and assert that precondition in the test.

#### [REQUIREMENT_CONFLICT] ADVISORY
- Location: specs/mission.md § Out of Scope and its closing sentence; export/export-iceberg-table § Background bullet 3
- Issue: The mission, which `CLAUDE.md` loads for every agent, names no export capability, lists "materialization" as out of scope, and states "Every query is executed independently, starts from source metadata, and leaves no state behind." After this plan an export creates namespaces and tables, and a failed export leaves data files. An agent that reads the mission can reject later export work as out of scope.
- Fix: Add a docs task that adds an export capability to specs/mission.md § Core Capabilities (writes source rows into a new Iceberg table through the Iceberg REST catalog, #566) and limits the "leaves no state behind" sentence to queries. If mission edits stay outside planning, list the update under plan.md § Open Questions for `/speq:mission`.

## Task Breakdown

#### [TASK_GRANULARITY] ADVISORY
- Location: plan.md § Implementation Tasks 3.7
- Issue: Carried from round 1, where it was task 3.6. Task 3.7 integrates the type check, the runtime, the session, create or stage, the writer ID, the rolling Parquet writer, a fresh session, and the commit. It carries no `[expert]` tag, while the smaller commit-loop tasks 1.3 and 1.4 carry one.
- Fix: Tag task 3.7 `[expert]`, or split it into first-row setup (type check, session, create or stage) and file writing with the commit.

Checked without objection: every scenario of the four deltas maps to a test in § Scenario Coverage, and every task traces to a delta or to a refactor that a delta needs. Groups A and C touch disjoint crates, and Group B declares its dependency on both.

## Design Depth

#### [ADR_CONFLICT] BLOCKER
- Location: plan.md § Implementation Tasks 3.4 ("Move the adapter's private `parse_creds` into `lakehouse-catalog/src/creds.rs` as `ConnectionCreds::from_json`") and § Context bullet 21; architecture.md § Constraints, the credential-types bullet
- Issue: ADR `catalog-crate-owns-credential-types-no-shared-types-crate` decides "The Exasol-facing parsers stay in the engine crate". The base constraint reads "Reading and parsing the Exasol CONNECTION object stays in the engine." Task 3.4 moves the CONNECTION password parser into `lakehouse-catalog`, and the delta rewrites the constraint so that `lakehouse-catalog` declares "their mapping from a CONNECTION password's JSON fields". No decision-log entry records the move, and no entry carries `Supersedes:`. `StorageCreds::from_json` already sits in the catalog crate, so the drift started earlier, but the recorded rule still states the opposite.
- Fix: Conform to the ADR. In task 3.4, move `parse_creds` (and `validate_creds`, per the second [UNSTATED_ASSUMPTION] advisory) from `adapter/connection.rs` into a new engine module outside `adapter/`, for example `crates/lakehouse-engine/src/connection.rs`, which vs-adapter and export both call. In architecture.md § Constraints, restore the base sentence "Reading and parsing the Exasol CONNECTION object stays in the engine." Add the module to § Components, add it to the depends-on lists of vs-adapter and export, and update plan.md § Context bullet 21 and the Group B Knowledge column. Alternatively, keep the move and add a decision-log entry with `Supersedes: catalog-crate-owns-credential-types-no-shared-types-crate`, `Promotes to ADR: yes`, the rule-2 criterion, and the rejected engine-module alternative.
- Escalation: MECHANICAL (conforming changes no user-visible behavior, and the ADR text and `adapter/connection.rs` settle it)

#### [TACTICAL_SHORTCUT] ADVISORY
- Location: decision-log.md § Design Decisions [2]; plan.md § Implementation Tasks 1.3 and 1.4
- Issue: Carried from round 1, not addressed. The commit loop relies on a private `iceberg::Catalog` impl that stubs 13 of 15 methods, and on the update order of `TableMetadataBuilder::from_table_creation` that spike lk_6 measured. A rev bump of iceberg-rust can change either without a compile error, and only the Lakekeeper E2E catches the change.
- Fix: Add to decision [2] Consequences: "A rev bump of iceberg-rust runs the Lakekeeper export E2E before merge." Add a plan.md § Open Questions entry (#TBD) for an upstream iceberg-rust change that exposes a commit's requirements and updates.

Checked without objection: decision [2] is the only `Promotes to ADR: yes` entry. Its Rationale names rule-2 criteria 2 and 4 and the ADR search result, its Decision holds no signature, path, or flag, and its `Architecture: § Components, § Constraints` line matches the delta's lakehouse-catalog component and commit-loop constraint. Decision [2] conforms to ADR `iceberg-rest-requests-run-on-the-catalog-session`, and decision [4] conforms to ADR `allow-http-threaded-as-vended-selector-parameter` (explicit consent, default false). The delta's BASE hash matches `specs/architecture.md`, and each CHANGED block keeps the base lines it does not edit.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: plan.md § Summary and § Context bullet 14; export/export-iceberg-table, Scenarios "Every supported type reads back unchanged, NULLs included" and "Static and vended storage credentials both write the table"
- Issue: Carried from round 1, partly addressed. The Summary clause "an unsupported column type fails each writer once, naming the column" does not read on one pass. Context bullet 14 still packs eight measurements into one bullet. The two scenarios use "read through a virtual schema", which only export/export-into-script § Background defines.
- Fix: Rewrite the Summary clause as "a column of an unsupported type fails the export before any data is written, naming the column". Split Context bullet 14 into one bullet per measurement. Add to export/export-iceberg-table § Background: "Read through a virtual schema: export/export-into-script § Background."
