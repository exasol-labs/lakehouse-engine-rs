# Plan Review Findings: fix-vs-property-validation

## Summary
- Axes checked: 6/6
- Total findings: 12 (Blockers: 2, Findings: 10)
- Intent Fidelity blockers: 0

## Failure Modes

- The first task cannot pass. Task 1.1 expects the issue's `CREATE ... NAMESPCE ...` statement to succeed, but the adapter rejects a missing `NAMESPACE` under the Iceberg REST kind that the local stack uses. AGENTS.md requires the reproduction before the fix. Routed to `[UNSTATED_ASSUMPTION]` (BLOCKER).
- The new spec forbids every property value check on pushdown, but two recorded features require value checks on pushdown, and the code runs them. An implementer either deletes required checks or ships a spec that the code contradicts. Routed to `[REQUIREMENT_CONFLICT]` (BLOCKER).
- Exasol's own handling of `''` and `null` can make some mapped E2E tests impossible to write, and the gap appears only during implementation. Routed to `[UNSTATED_ASSUMPTION]` (FINDINGS).

## Intent Fidelity

Checked: the issue (#534) asks that CREATE and ALTER ... SET fail on unknown names and invalid tuning values, with each name, a suggestion, and each invalid value in the error. The plan covers all of this. Adding REFRESH, case-sensitive matching, one combined error, and the null-unset rule come from the interview. The split in decision [2] matches the interview, because the interview defines "invalid value" as the 8 numeric properties, `DATAFUSION_THREADING_MODE`, and `ALLOW_HTTP`.

#### [SCOPE_CREEP] FINDINGS
- Location: `scan-runtime/scan-execution-connection-concurrency/spec.md` § Background; `vs-adapter/create-virtual-schema-adapter-notes/spec.md` § Background
- Issue: In each feature, only one Background bullet needed to change: "An absent, empty, zero, or invalid value triggers an AUTO derivation" and "The adapter reads properties by name and ignores unknown names". The CHANGED blocks replace 12 bullets with 4 and 10 bullets with 5. Scenarios restate most of the deleted facts, but no scenario restates these three: "The limiter gates only the delete path's preparation reads. The Parquet opener's data-file reads at execution time are bounded by the HTTP client that the same N configures."; "The adapter opens no connect-back session for topology discovery, at create time or at pushdown time" (no scenario in this feature covers the pushdown half); "The scan UDF uses the same source for DataFusion `target_partitions`". `plan.md` does not mention the trim, so a PR reader cannot tell that the deletions are intended.
- Fix: Restore these three facts in the two Background blocks, folding bullets to stay within 5. Alternatively, add a `plan.md` § Context bullet that says both Background blocks are trimmed to the 5-bullet limit and names the three dropped facts.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: `plan.md` § Implementation Tasks 1.1; `plan.md` § Context, bullet 1
- Issue: Task 1.1 says: "Run the issue's `CREATE VIRTUAL SCHEMA TYPO ...` statement ... Confirm it succeeds, maps every table of the catalog, and records `PARALLELISM_FACTOR` `1` and `DF_BATCH_SIZE` `8192`". The local suite leaves `CATALOG_KIND` absent, which selects Iceberg REST. For that kind, `handle_create_virtual_schema` (`crates/lakehouse-engine/src/adapter/mod.rs:204-211`) returns `property 'NAMESPACE' is required` when `NAMESPACE` is absent. The unit test `create_virtual_schema_rejects_old_namespace_alias_without_replacement` pins this error. The issue's statement therefore fails, the confirmation cannot pass, and task 1.1 says nothing about a mismatch. Context bullet 1 repeats the issue's claim that "`NAMESPCE = 'sales'` enumerates the whole catalog", which holds only under `CATALOG_KIND = 'DIRECT_STORAGE'`. The planning notes record "NAMESPACE required check after config", so the plan contradicts code the planner read. AGENTS.md requires reproducing the bug before the fix, so this task blocks everything after it.
- Fix: Rewrite `plan.md` task 1.1 as three statements. (1) Run the issue's statement and expect `property 'NAMESPACE' is required`: the error names the missing property, not the typo. (2) Spell `NAMESPACE` correctly and keep `PARALLELISM_FACTR = '64'`, `DATAFUSION_BATCH_SIZE = 'abc'`, and `MADE_UP_PROPERTY = 'x'`. Confirm the statement succeeds and records `PARALLELISM_FACTOR` `1` and `DF_BATCH_SIZE` `8192`. (3) Run a `CATALOG_KIND = 'DIRECT_STORAGE'` CREATE with `NAMESPCE`, using the direct-storage fixtures of `crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs`, and confirm it lists from the CONNECTION root. Add the stop-and-report rule of task 1.2 to task 1.1. Rewrite Context bullet 1: `NAMESPCE` lists the whole CONNECTION root under `DIRECT_STORAGE`, and fails as a missing `NAMESPACE` under the other three kinds.

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: `plan.md` § Verification › Scenario Coverage, row "An empty or null property value counts as absent"; `vs-adapter/property-validation/spec.md` § Background, bullet 1
- Issue: The plan maps this scenario to the E2E test `empty_property_values_count_as_absent`. In `prepare_create_virtual_schema` (`crates/lakehouse-e2e/tests/common/e2e_harness.rs`), the shared harness records "Exasol rejects `NAMESPACE = ''`". Exasol also treats `''` as NULL. A CREATE may therefore never deliver an empty value, and `SET <name> = ''` may arrive as a null unset. Task 1.2 records these request shapes, but no task says how task 2.1 proves the scenario when no SQL statement reaches it. The same harness comment contradicts Background bullet 1: "Exasol passes every property name and value ... without checking them".
- Fix: Add to `plan.md` task 2.1: "If task 1.2 shows that no SQL statement delivers an empty or null value to a createVirtualSchema or refresh request, prove those cases with a unit test over a synthesized request (`specs/testing.md` § Coverage rule, last bullet). Keep the E2E test for the reachable `SET <name> = null` case." In Background bullet 1, limit the claim to names: "Exasol passes every property name to the adapter without checking it."

#### [EFFORT_MISESTIMATION] FINDINGS
- Location: `plan.md` § Implementation Tasks 4.1 and 4.2; § Dead Code Removal, row "Test"
- Issue: Task 4.2 changes every `resolve_*` function to take typed values, and § Dead Code Removal deletes `resolve_df_fixed_count`. `adapter_tests.rs` calls these functions with JSON properties at 45 call sites in about 20 tests, not only in the 9 tests the plan lists. Tests outside the list include `default_parallelism_factor_is_one`, `explicit_parallelism_factor_overrides_default`, `df_target_partitions_uses_supplied_value`, `df_target_partitions_explicit_wins`, `df_threads_per_udf_explicit_wins`, `threading_mode_parses_case_insensitively`, `resolve_s3_max_connections_fixed_value_wins`, and `fixed_mode_uses_supplied_values`. Each one must be rewritten or deleted. The valid-value parsing these tests assert moves to `properties_tests.rs`, and `specs/testing.md` § Testing strategy tests each behavior at one layer only.
- Fix: In `plan.md` task 4.1, list every `adapter_tests.rs` test that calls `resolve_parallelism_factor`, `resolve_threading_mode`, `resolve_df_threading`, `resolve_df_fixed_count`, `resolve_s3_max_connections`, `resolve_df_batch_size`, `resolve_memory_pool_fraction`, `resolve_instance_overhead_mb`, or `resolve_join_broadcast_max_bytes`. Mark each test as deleted, because `properties_tests.rs` covers its parsing, or as rewritten over typed inputs. Extend the § Dead Code Removal "Test" row to match.

#### [NFR_IGNORED] FINDINGS
- Location: `plan.md` § Impact, "Breaking" bullet
- Issue: The Impact names `DATAFUSION_BATCH_SIZE = '0'` as newly failing. It omits `S3_MAX_CONNECTIONS = '0'`. Both `docs/tuning.md` ("Absent, invalid, or `0`") and the recorded scenario "AUTO derivation sizes the per-instance budget from the resolved thread count" ("absent, empty, zero, or invalid") document `0` as a way to select AUTO. After the change, a CREATE with that value fails, and an existing schema that holds it fails on REFRESH.
- Fix: Add `S3_MAX_CONNECTIONS = '0'` to the Breaking bullet in `plan.md` § Impact, with the migration: leave the property absent to select AUTO.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `specs/_plans/fix-vs-property-validation/vs-adapter/property-validation/spec.md` § Scenario "Pushdown and drop never validate properties", last AND step
- Issue: The step reads: "the adapter MUST NOT check property names or values on a `pushdown`, `getCapabilities`, or `dropVirtualSchema` request". Two recorded specs require value checks on pushdown. In `vs-adapter/lakekeeper-permission-check`, the scenario "Invalid permission properties are rejected before the catalog is contacted" applies to "a createVirtualSchema, refresh, setProperties, or pushdown request". In `vs-adapter/catalog-kind-selection`, the scenario "An unrecognized CATALOG_KIND value is rejected with a clear error" resolves the kind "on createVirtualSchema and pushdown requests alike". The code also parses `MERGE_SCHEMA`, `HIVE_PARTITIONING`, and the direct-storage `NAMESPACE` on pushdown: `pushdown/scan_resolution.rs:126` calls `resolve_direct_storage_properties`. No task removes these checks, and removing them would break both recorded features. As written, the step is false for the adapter the plan ships, and no verifier can pass it.
- Fix: Replace the step with: "the adapter MUST NOT run this feature's name check or tuning-value check on a `pushdown`, `getCapabilities`, or `dropVirtualSchema` request, and the value checks that `vs-adapter/catalog-kind-selection`, `vs-adapter/lakekeeper-permission-check`, and `direct-storage/direct-storage-properties` run on pushdown stay unchanged". Leave `architecture.md` § Interfaces "check neither" as it is, because that line covers only unknown names and invalid tuning values.

#### [AMBIGUOUS_REQUIREMENT] FINDINGS
- Location: `scan-runtime/scan-execution-threading/spec.md` § Scenario "Threading mode defaults to AUTO when the property is absent", first AND step
- Issue: The step says the adapter "SHALL record `DATAFUSION_THREADING_MODE: AUTO` in the `createVirtualSchema` response `adapterNotes`". The adapter writes the mode under the note key `DF_THREADING_MODE` (`NOTE_DF_THREADING_MODE`, `adapter/mod.rs:64`). `plan.md` § Manual Testing row 3 also expects `DF_THREADING_MODE` `AUTO`. The CHANGED scenario carries the wrong key forward, so the E2E tests mapped to it (`create_without_tuning_properties_records_the_defaults`, `create_rejects_each_invalid_tuning_value`) cannot assert the step as written.
- Fix: In that CHANGED scenario, replace `DATAFUSION_THREADING_MODE: AUTO` with "a `DF_THREADING_MODE` entry of `AUTO`".

#### [COMPLETENESS_GAP] FINDINGS
- Location: `plan.md` § Implementation Tasks 5.1
- Issue: Task 5.1 replaces the `docs/tuning.md` sentence "To change one value, you must recreate the VS" with `ALTER VIRTUAL SCHEMA ... SET`. `docs/architecture.md` line 46 keeps the same claim: "A change to one of them requires a new VS." After the change, the two pages contradict each other about how to change a tuning value.
- Fix: Extend task 5.1 to rewrite `docs/architecture.md` line 46 the same way. Add `docs/architecture.md` to Group B's Knowledge in `plan.md` § Parallelization.

## Task Breakdown

Checked: every delta has an implementing task (property-validation: 3.x, 4.x, and 2.1; the three tuning features: 4.2 and 2.1; adapter-notes: 2.1; direct-storage: 2.3). Group A holds all code and tests, and Group B holds only docs, so no group is sliced by layer. The stub-swap test in task 2.1 is feasible, because `refresh.rs` already swaps an adapter in its own schema with `create_schema_and_scripts_in`.

#### [TRACEABILITY_GAP] FINDINGS
- Location: `plan.md` § Verification › Scenario Coverage, rows "An unknown property name fails the statement" and "A schema persisted with an invalid property refuses refresh until the property is unset"
- Issue: The scenario names "`refresh` (with or without `requestedTables`)". The plan's only E2E refresh, in `stray_persisted_properties_block_refresh_but_not_queries`, is a full REFRESH. No E2E test runs `ALTER VIRTUAL SCHEMA ... REFRESH TABLES` against a stray persisted property. The unit test of task 4.1 does not say that its refresh request carries `requestedTables`. `specs/testing.md` § Coverage rule requires an E2E proof of each scenario.
- Fix: In `plan.md` task 2.1, make `stray_persisted_properties_block_refresh_but_not_queries` run `ALTER VIRTUAL SCHEMA ... REFRESH TABLES <table>` before the unset, and assert the same error. In task 4.1, add a refresh request that carries `requestedTables`.

## Design Depth

Checked: decision-log entries [1] and [2] are both `Promotes to ADR: no`, so there is no `[ADR_OVERPROMOTION]`. The architecture delta's BASE matches `git hash-object specs/architecture.md` (`41cb305d`). The Components and Interfaces blocks copy the base text with only the intended edits. The component, the closed property set, and the dependency all appear in the delta, so there is no `[ARCHITECTURE_DRIFT]`. ADRs checked: `allow-http-threaded-as-vended-selector-parameter` (default false is kept), `one-scan-shard-per-node-with-threads-on-every-core` (`PARALLELISM_FACTOR` default 1 is kept), `adapternotes-admits-only-create-time-values-a-pushdown-cannot-recompute` (note entries are unchanged), and the `CATALOG_KIND` construction-site ADR (`064`, unchanged). No ADR conflicts with the plan. The `vs-properties` module is deep: callers get typed values or one error, and the module hides the names, the rules, the edit distance, and the message format.

#### [INFORMATION_COUPLING] FINDINGS
- Location: `plan.md` § Implementation Tasks 3.2 ("Each rule also exposes a lenient parse for the pushdown's `adapterNotes` reads") and 4.3; `architecture.md` § Components, `vs-properties` line
- Issue: `build_adapter_notes` writes the tuning notes, and it belongs to `vs-adapter`, which "owns: schema-level adapterNotes". The plan moves the parsing of those notes into `vs-properties`, so two modules share the note value format. The property rules also differ from today's note reads. The `DF_BATCH_SIZE` note read clamps `"0"` to 1 (`mod.rs:329-332`), but the property rule rejects 0, and task 4.3's "with today's defaults" does not say which behavior applies. A later change to a property rule, such as accepting a unit suffix, would also change how stored notes parse.
- Fix: Keep note parsing in `adapter/mod.rs` next to `build_adapter_notes`, which owns the note format. Remove the lenient parse from task 3.2 and from the `vs-properties` component line in `architecture.md`. If the shared parse stays, state in task 4.3 that each note read keeps its current accept rule and default, and name the `DF_BATCH_SIZE` `"0"` case.

## Prose Quality

#### [PROSE_UNCLEAR] FINDINGS
- Location: `architecture.md` § Interfaces, bullets "VS properties" and "VS property set"
- Issue: The first bullet says "and for `DIRECT_STORAGE` also `MERGE_SCHEMA` ... and `HIVE_PARTITIONING`". The next bullet says the set is "the same under every `CATALOG_KIND`". Read once, the first bullet says these two names exist only under `DIRECT_STORAGE`.
- Fix: Change "and for `DIRECT_STORAGE` also `MERGE_SCHEMA` (...) and `HIVE_PARTITIONING` (...)" to "`MERGE_SCHEMA` (...) and `HIVE_PARTITIONING` (...), which only `DIRECT_STORAGE` reads".

#### [PROSE_BLOAT] FINDINGS
- Location: `vs-adapter/property-validation/spec.md` § Scenario "An invalid tuning value fails the statement and names the accepted values", AND steps 2 and 3; `plan.md` § Implementation Tasks 1.2
- Issue: Several steps put two or three rules in one sentence, against writing-guardrails rule 2. Example: "the adapter SHALL reject an `INSTANCE_OVERHEAD_MB` value that is not a whole number of at least 0, expecting `a whole number of at least 0`, and a `MEMORY_POOL_FRACTION` value that is not a number greater than 0 and at most 1, expecting ..., so `60` and `0` fail and `1` passes". Task 1.2 puts six observations and two stop conditions in one paragraph.
- Fix: Split the scenario into two: "An invalid numeric tuning value fails the statement" (the six counts, `INSTANCE_OVERHEAD_MB`, and `MEMORY_POOL_FRACTION`, one rule per step) and "An invalid enumerated tuning value fails the statement" (`DATAFUSION_THREADING_MODE` and `ALLOW_HTTP`, one rule per step). Map both new scenarios to `create_rejects_each_invalid_tuning_value` in § Scenario Coverage. In task 1.2, list each observation as a sub-bullet and put the two stop conditions in one closing sentence.
