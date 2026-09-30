# Plan Review Findings: add-glue-catalog-kind (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 18 (Blockers: 10, Advisory: 8)
- Intent Fidelity blockers: 2
- Human-escalation blockers: 2

## Premortem

1. **A `binary` refusal breaks lakes nobody asked about.** A direct-storage lake holds Parquet UUID columns, legacy Impala strings written as unannotated `BYTE_ARRAY`, or parquet-avro enums. After the upgrade, every query on those columns fails and cites #351. The user answered D3 from the issue's table, which names only `binary`. Routed to Intent Fidelity (second finding).
2. **The merged library contradicts itself.** The recorder merges the deltas. `vs-adapter/delta-type-mapping` and `datafusion-scan/nested-json-rendering` still say an Iceberg binary column renders as text. `vs-adapter/create-virtual-schema` still forbids any adapterNotes entry except `TABLE_MAP`. The next plan reads the contradiction and reintroduces the asymmetry. Routed to Requirement Quality.
3. **The implementer follows the census literally and the build breaks.** Task 6.5 declares a second `SkippedTable` beside the imported one. Task 2.7 widens a source-text probe that ADR 091 forbids. Routed to Feasibility and Requirement Quality.

## Intent Fidelity

#### [SCOPE_CREEP] BLOCKER
- Location: decision-log.md § [7] Zero-length objects are never data files, plan.md § Impact ("Direct storage and Unity Parquet: a zero-length object is no longer read"), and `vs-adapter/parquet-directory-seam/spec.md` § "The file pattern selects the listing depth and the file-name rule"
- Issue: The user decided D5/D8 as "`**/*.parquet` for direct storage and Unity (unchanged)". Decision [7] changes both: a zero-length `*.parquet` object now drops silently, where today it fails the footer read. The interview never raised this rule. Its rationale is also partly wrong. `object_store` 0.13.2 `Path::parse` strips a trailing `/` (`src/path/mod.rs:186`), so a directory marker at the listed location has no segment below the prefix, and `data_file_segments` already drops it (`segments.last()?`). The real `*`-pattern cases are Hadoop `<dir>_$folder$` sibling markers and truly empty files. Escalation is HUMAN because the rule changes what direct-storage and Unity users see, against an explicit "unchanged".
- Fix: Ask the user to choose between two options. (a) The zero-length rule applies only under a pattern without a `.parquet` name rule (`*`), so direct storage and Unity keep today's behavior. (b) The rule applies to every pattern, as planned. Record the answer in decision-log.md § Interview. Correct the rationale in decision [7] and in the plan.md § Consequences row. The marker at the listed location is already excluded. Name the `_$folder$` and empty-file cases instead. Until the user answers, delete the direct-storage/Unity zero-length line from plan.md § Impact and the zero-length clause for `**/*.parquet` from the seam scenario.
- Escalation: HUMAN. The user said direct storage and Unity stay unchanged, and only the user can widen that.

#### [SCOPE_CREEP] BLOCKER
- Location: plan.md § Impact (line 120) and § Spec compliance ("Iceberg `binary` and `fixed(L)`"), decision-log.md § [2], `vs-adapter/binary-column-refusal/spec.md` § Background and scenario 1, and tasks 4.1 and 4.2
- Issue: D3 says "`binary` REFUSED everywhere". The plan refuses more than `binary`, and the interview never showed the extra reach:
  - Iceberg `fixed(L)` is refused. `fixed(L)` is a distinct Iceberg primitive (§ Primitive Types).
  - Direct storage refuses Arrow `FixedSizeBinary`. `parquet` 58.4.0 maps a UUID-annotated `FIXED_LEN_BYTE_ARRAY(16)` to `FixedSizeBinary(16)` (`src/arrow/schema/primitive.rs:348`, fall-through arm). So a direct-storage UUID column is refused, while scenario 1 exempts Iceberg `uuid`. The same bytes get two answers.
  - Direct storage refuses Arrow `Binary`. `from_byte_array` maps an unannotated `BYTE_ARRAY` (`(None, ConvertedType::NONE)`) and the `ENUM`, `BSON`, `GEOMETRY`, and `GEOGRAPHY` annotations to `Binary` (`primitive.rs:280-300`). Legacy Impala and Hive strings and parquet-avro enums therefore become refused. Today they return text.
  The user asked for digging and tradeoff context before each decision. Only the unannotated-string case appears, as one Impact sentence, and it was never asked.
- Fix: Put the three cases above to the user as a D3 follow-up, quoting the `parquet` mapping lines. Record the answer in decision-log.md § Interview. Then take one of two routes. (a) Narrow the refusal: refuse Iceberg `binary` and direct-storage columns whose Parquet annotation is absent or `BSON`, keep `uuid`-annotated FLBA and `ENUM` as today, and decide `fixed(L)` per the answer. (b) Keep the full reach: add one scenario each for a direct-storage UUID column, an `ENUM` column, and an unannotated string column to `binary-column-refusal`. Under either route, if `fixed(L)` or `FixedSizeBinary` stays refused, change the task 4.1 and 4.2 refusal text. `binary_cause()` states "has type 'binary'", which misnames a `fixed(16)` column.
- Escalation: HUMAN. The change breaks queries for users on formats and types the requester never approved.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: plan.md § Implementation Tasks, task 6.5 ("Add `SkippedTable { table, reason }` to `adapter/mod.rs`")
- Issue: `adapter/mod.rs:33-35` already imports `lakehouse_catalog::SkippedTable { ident, reason: SkipReason }`, and `skip_warning` (`:246`) and the `VirtualTables` alias (`:441`) use it. A second `SkippedTable` in the same module fails with E0255. Renaming it instead creates a second struct for a concept the neutral type already models.
- Fix: In task 6.5, drop the new struct. Give `build_adapter_notes` the parameter `skipped: &[lakehouse_catalog::SkippedTable]`, and build each `{"table","reason"}` object inside it from `catalog_identifier_string(&entry.ident)` and `skip_reason(entry)`. Keep the census of `&[]` call sites unchanged.
- Escalation: MECHANICAL. The import at `adapter/mod.rs:35` settles it.

#### [NFR_IGNORED] ADVISORY
- Location: `vs-adapter/create-virtual-schema-skipped-tables/spec.md` § Background ("The list has no size cap, like `TABLE_MAP`")
- Issue: This feature targets Hive-heavy Glue databases, where most tables are ORC, text, or views. Each skip adds roughly 150 characters to adapterNotes. If Exasol bounds `ADAPTER_NOTES` (the catalog column suggests `VARCHAR(2000000)`, not verified here), a database with about 10,000 skipped tables fails CREATE. That outcome contradicts D1 ("CREATE never fails on all-skipped").
- Fix: Verify the adapterNotes size limit against the Docker Exasol, per CLAUDE.md § Verification discipline. If a limit exists, cap `SKIPPED_TABLES` at a stated entry count and add a total-count field, then add a scenario for the cap.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Scenario Coverage, `requests_are_signed_with_the_connection_key_not_the_environment`
- Issue: The scenario's GIVEN sets `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION`. Under edition 2024, `std::env::set_var` is `unsafe`, and it races every parallel test in the same binary.
- Fix: Assert the `Credential=<access_key>/<date>/<region>/glue/aws4_request` scope in the `Authorization` header that `mock_glue_tests.rs` records. Isolate any environment mutation in a single-threaded integration-test binary, or drop it and rely on task 1.4's `cargo tree` check that `aws-config` is absent.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `vs-adapter/binary-column-refusal/spec.md` § Background, bullet 3 ("It SUPERSEDES the Iceberg asymmetry...")
- Issue: Two recorded Backgrounds state the opposite of this delta, and no delta edits them:
  - `datafusion-scan/nested-json-rendering/spec.md:16-18`: "a top-level Iceberg binary column keeps its `CAST(col AS VARCHAR)` display-text path".
  - `vs-adapter/delta-type-mapping/spec.md:42-44`: "an ICEBERG table's nested `binary` IS rendered as hexadecimal, because the Iceberg format reader refuses no type at all".
  A "SUPERSEDES" sentence in a new feature does not change recorded text, so the merged library contradicts itself.
- Fix: Add `specs/_plans/add-glue-catalog-kind/vs-adapter/delta-type-mapping/spec.md` and `specs/_plans/add-glue-catalog-kind/datafusion-scan/nested-json-rendering/spec.md`, each with a `<!-- DELTA:CHANGED -->` `## Background` block. Each block replaces the quoted sentence with one line: binary is refused on every format per `vs-adapter/binary-column-refusal`. Delete bullet 3 from the `binary-column-refusal` Background. Add both deltas to plan.md § Features.
- Escalation: MECHANICAL. Reading the three specs settles it.

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `vs-adapter/create-virtual-schema-skipped-tables/spec.md` (whole feature)
- Issue: Two recorded rules forbid the new entry:
  - `vs-adapter/create-virtual-schema/spec.md:87`: "the adapter MUST NOT persist any catalog metadata between requests other than the table-name map recorded in `adapterNotes`".
  - `vs-adapter/create-virtual-schema-adapter-notes/spec.md:10-15`: "`adapterNotes` is reserved for values derived at create time that a pushdown cannot recompute, such as `TABLE_MAP`", and "Only those derived entries round-trip".
  `SKIPPED_TABLES` persists catalog identifiers and reasons that no pushdown reads. D1 stands, so the recorded rules must change.
- Fix: Delete the `create-virtual-schema-skipped-tables` feature. Move its three scenarios into a new `specs/_plans/add-glue-catalog-kind/vs-adapter/create-virtual-schema-adapter-notes/spec.md` as `<!-- DELTA:NEW -->` scenarios. Add a `<!-- DELTA:CHANGED -->` Background there that names `SKIPPED_TABLES` as the one write-only diagnostic entry. Add a `<!-- DELTA:CHANGED -->` block to the `create-virtual-schema` scenario "Create virtual schema enumerates every table in the configured namespace" that permits `SKIPPED_TABLES` in the `:87` clause. Update plan.md § Features and § Scenario Coverage.
- Escalation: MECHANICAL. The recorded clauses settle it.

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: plan.md task 2.7 ("add the `glue/*.rs` files to `CATALOG_SOURCES` (lines 19-35)")
- Issue: `CATALOG_SOURCES` is an `include_str!` list that `demoted_and_deleted_functions_are_not_declared_public` (`catalog_public_surface.rs:78`) and `storage_backend_secret_values_and_file_io_are_reachable` (`:553`) scan as source text. Adding the Glue files widens source-text matching to new production code. ADR 091 (`structural-invariant-not-enforced-by-source-text-matching`) binds every future plan: "no test in this project may enforce a structural invariant by matching production source text". The plan's own delta `catalog-crate-public-surface-extensions-glue` also says the edit "MUST NOT add a source-text assertion".
- Fix: Delete "and add the `glue/*.rs` files to `CATALOG_SOURCES` (lines 19-35)" from task 2.7.
- Escalation: MECHANICAL. ADR 091 and the probe file settle it.

#### [IMPLEMENTATION_LEAKAGE] BLOCKER
- Location: the `## Background` sections listed below
- Issue: Each line below states a fact that no GIVEN, WHEN, or THEN step of its spec depends on:
  - `glue-catalog-client` bullet 1: "through `aws-sdk-glue`, built without `aws-config`".
  - `create-virtual-schema-skipped-tables`, all three bullets (the `udf_log!` history, non-owner readability, "no size cap").
  - `binary-column-refusal` bullets 3 and 4 (the "SUPERSEDES" sentence and "previously returned display text").
  - `glue-table-planning` bullet 5: the Lake Formation, cross-account, and "untested" clauses. Its static-credential clause is used by scenarios and stays.
  - `partition-predicate-declared-types` bullet 3: "The Delta and Iceberg readers keep their own file pruning."
  - `glue-e2e-harness` bullet 2: the IAM action list.
  - `catalog-crate-public-surface-extensions-glue` bullet 2: the list of crates the catalog crate must not name.
- Fix: Delete each listed line. Move the Lake Formation scope to `docs/security.md` (task 8.1 already covers it). Move the IAM action list to `docs/catalogs.md` § "required IAM actions" (task 8.1). For the `create-virtual-schema-skipped-tables` bullets, apply the relocation from the previous finding.
- Escalation: MECHANICAL. The Background rule is checkable from the spec text alone.

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: `vs-adapter/create-virtual-schema-skipped-tables/spec.md` § "Every skipped table is recorded with its reason under every catalog kind", clause 2
- Issue: The clause requires each `reason` to name "the catalog value that decided it". `SkipReason::NotLoadableIcebergTable` and `SkipReason::NoDataFile` (`lakehouse-catalog/src/client.rs:77-84`) carry no value. Clause 3 also freezes the Iceberg warning text byte for byte. No test can show that the Iceberg REST or direct-storage reason names a deciding value.
- Fix: State the reason text per variant: Iceberg REST, the legacy parenthetical. Direct storage, "holds no data file". Unity and Glue, the `detail`. Require "names the catalog value" only for the variants that carry a `detail`.

#### [REQUIREMENT_CONFLICT] ADVISORY
- Location: `vs-adapter/partition-predicate-declared-types/spec.md` § Background, bullet 1
- Issue: The bullet adopts the node set of `vs-adapter/direct-storage-hive-partitioning`. That spec's clause (`:81`) limits nodes to "non-empty string literals ... so a function or a non-string literal never prunes a file". Under a declared integer type, a numeric literal now prunes. The two specs read together give two answers.
- Fix: Add one clause to "A partition value compares under its column's declared type": the literal set widens per declared type to the pairs task 3.4 lists, and a utf8 column keeps the string-literal rule.

## Task Breakdown

#### [TASK_GRANULARITY] ADVISORY
- Location: plan.md § Parallelization
- Issue: All seven groups run in sequence. Tasks 4.1-4.4 (binary refusal) share only `parquet_format_reader.rs:47` with group C and nothing with group B. Tasks 6.5 (skipped tables, every kind) and 7.4-7.6 (Makefile, CI job, sweep workflow) need no Glue code.
- Fix: Move 4.1-4.4 into their own group that depends only on A. Move 7.4-7.6 into a group that depends only on A. Otherwise, state in § Parallelization why each stays sequential.

#### [TASK_GRANULARITY] ADVISORY
- Location: plan.md task 2.4
- Issue: One untagged task covers pagination, the namespace rule, `CatalogId`, Parquet column mapping, Iceberg metadata reads at bounded concurrency, extraction of a shared function from `IcebergRestCatalogClient::load_on_session`, `load_table`, and `load_table_for_planning`. The extraction must keep REST output byte-identical, yet task 2.2 carries `[expert]` and 2.4 does not.
- Fix: Split task 2.4 into 2.4a (the `GetTables` listing and Parquet tables) and 2.4b (Iceberg metadata reads and the extracted column function). Tag 2.4b `[expert]`.

## Design Depth

#### [ADR_OVERPROMOTION] BLOCKER
- Location: decision-log.md § [2] Binary is refused on every table format at every depth until #351
- Issue: The user said to promote "only what are truly architectural decisions that must stay". This decision is temporary by its own title ("until #351"). Its full rule already lives in `vs-adapter/binary-column-refusal`.
- Fix: Set `Promotes to ADR: no` in decision [2].
- Escalation: MECHANICAL. The user's stated bar decides it.

#### [ADR_OVERPROMOTION] BLOCKER
- Location: decision-log.md § [4] Skipped tables are recorded in ADAPTER_NOTES for every catalog kind
- Issue: This is a user-visible feature contract, not an architecture decision. Its content is interview answer D1 plus the skipped-tables scenarios. An ADR adds nothing a future reader lacks, and it fails the user's "truly architectural" bar.
- Fix: Set `Promotes to ADR: no` in decision [4].
- Escalation: MECHANICAL. The user's stated bar decides it.

[no objection on decisions [1] and [3]: one shared Parquet reader plus one Iceberg planner, and an SDK client with no ambient credential chain, are lasting architecture and security boundaries. The Quick diagnostic table answers every design-philosophy question. `CatalogPartition` keeps `aws-sdk-glue` types inside `lakehouse-catalog`. `format_reader` dispatches on the format tag, per ADR 096.]

## Prose Quality

#### [PROSE_BLOAT] BLOCKER
- Location: the spec deltas listed below
- Issue: The user said the specs "are not a changelog". Each phrase below compares against the pre-plan state. The comparison loses its meaning once the delta merges into the permanent library:
  - `glue-table-planning` scenario 1: "SHALL be byte-identical to the output before this feature". Task 5.3 already carries the regression guard. Background bullet 5: "of this first cut" and "untested".
  - `partition-predicate-declared-types`, feature description: "so its pruning is unchanged". Scenario 1: "exactly as before this feature". Scenario 3: "prunes no file, unchanged".
  - `binary-column-refusal` scenario 2: "declared `VARCHAR(2000000)`, unchanged". Scenario 3: "render it as text, unchanged".
  - `create-virtual-schema-skipped-tables` scenario 2: "written as before this feature". Scenario 1: "SHALL stay byte-identical".
  - `catalog-kind-selection`, new scenario "CATALOG_KIND naming Glue resolves the native Glue kind": "SHALL keep its behavior unchanged, because the Glue kind is additive". Glue validation scenario: "apply the SigV4 required-fields rule ... unchanged".
  - `catalog-crate-public-surface-extensions-glue` scenario 1 WHEN: "the engine gains the `GLUE` catalog kind".
- Fix: Restate each phrase as current behavior and remove the comparison. For example, "a utf8 column SHALL compare in codepoint order against a string literal", or "the SigV4 required-fields rule of `vs-adapter/connection-credentials-sigv4` SHALL apply". Delete "byte-identical to the output before this feature" from `glue-table-planning` scenario 1. Where a clause exists only to freeze a legacy text, cite the owning scenario instead.
- Escalation: MECHANICAL. The user already gave the rule, and each rewrite needs no judgment.

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md § Design (Patterns table, Consequences table, Non-Goals) and § Open Questions 1-5, and decision-log.md § [5], [8], [9], [11], [12], [13], [14]
- Issue: The user asked for concise decisions. Each item below repeats text that already exists elsewhere:
  - The plan.md Consequences table repeats decision-log [1]-[7].
  - The Patterns table repeats the Consequences table and decision [1].
  - Non-Goals repeats Open Questions 1-5.
  - Decisions [8], [13], and [14] restate tasks 3.1, 2.2, and 5.1.
  - Decisions [5], [9], [11], and [12] restate spec scenarios or Non-Goals.
- Fix: Delete the Patterns and Consequences tables from plan.md, and keep the architecture diagram and the Quick diagnostic. Merge Non-Goals into Open Questions. Delete decisions [8], [13], and [14], because their content lives in the tasks. Reduce decisions [5], [9], [11], and [12] to one line each, or delete them.

#### [PROSE_BLOAT] ADVISORY
- Location: `partition-predicate-declared-types` § "Each source supplies its partition columns' declared types to the one predicate", the `unity-parquet-table-planning` DELTA:NEW scenario, and `glue-table-planning` § "A partition predicate prunes partitions before their locations are listed"
- Issue: Three scenarios assert that Unity and Glue prune under declared types. Two of them map to the same test, `a_partition_predicate_prunes_under_the_declared_type`.
- Fix: Keep the Unity scenario, which replaces the REMOVED one, and the Glue scenario. Cut "Each source supplies..." down to its direct-storage clause and its ONE-predicate clause.
