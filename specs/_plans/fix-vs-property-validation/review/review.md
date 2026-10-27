# Plan Review Findings: fix-vs-property-validation

## Summary
- Axes checked: 6/6
- Total findings: 8 (Blockers: 2, Findings: 6)
- Intent Fidelity blockers: 1

## Failure Modes

- The revision made the adapter-notes scenario require that a refresh or setProperties response hold exactly the listed entries. A recorded scenario, an accepted ADR, the code, and a unit test all keep inherited entries on refresh. The implementer must either add the removal code that the ADR rejects, or ship a spec that the code contradicts. Routed to `[REQUIREMENT_CONFLICT]` (BLOCKER).
- Task 5.2 adds the `AGENTS.md` rule that a removed note key leaves no trace. After the plan lands, one spec step, two unit tests, and one doc still name the removed `CLUSTER_NODES` key, so the commit that adds the rule also breaks it. Routed to `[SCOPE_REDUCTION]` (BLOCKER).
- The task 1.2 stub fails every request, so it never creates a schema. The `ALTER ... SET` and `REFRESH` probes then have no schema to run against, and the E2E test for empty and null values depends on what those probes find. Routed to `[UNSTATED_ASSUMPTION]` (FINDINGS).

## Intent Fidelity

Checked: issue #534 (unknown names and invalid tuning values fail CREATE and SET, with each name, a suggestion, and each invalid value in the error; required-property typos report the typo; every property is accepted under every kind; `= null` still unsets), the interview, and every item of `notes/pr-feedback.md`. Each item is applied: point 1 (task 5.1, the concurrency delta, Review Finding 3), point 2 (the CHANGED AUTO scenarios), point 3 (`DF_THREADING_MODE` in the threading scenario), point 4 (plan Summary, spec intro, `notes/pr-body-sentence.md`), point 5 (task 3.2, the spec ranges, Review Finding 1), point 6 (task 4.3, the architecture delta), point 7 (tasks 1.2, 2.1, and 4.1), point 8 (task 5.1, § Interfaces), point 9 (the echo rule, the task 1.2 stop condition), the listed retired names (tasks 2.2, 2.4, 4.1, and 5.2, the pushdown-planning delta), the docs items (tasks 5.1 and 5.2), the proposal (task 5.2), and no migration notes (task 5.1). Two leftovers remain: the finding below, and the `[AMBIGUOUS_REQUIREMENT]` finding under Requirement Quality.

#### [SCOPE_REDUCTION] BLOCKER
- Location: `plan.md` § Implementation Tasks 4.1, 5.1, and 5.2; `pushdown/pushdown-planning/spec.md` delta; `plan.md` § Dead Code Removal
- Issue: The PR review asks: "Retired properties leave no trace ... We don't keep tests, spec steps, or docs that name it". It also proposes the rule "A removed property, option, or note key leaves no trace. No spec step, test, or doc names it." Task 5.2 adds that rule to `AGENTS.md`. Task 2.4 removes the `CLUSTER_NODES` absence check from `scan.rs`, so the plan itself treats `CLUSTER_NODES` as a removed note key. After the plan lands, these places still name that key:
  - a spec step: `specs/pushdown/pushdown-planning/spec.md:76`, "whose persisted `adapterNotes` carry NO `CLUSTER_NODES` entry";
  - two unit tests: `build_adapter_notes_merges_existing` (`crates/lakehouse-engine/src/adapter/adapter_tests.rs:358`, `:368`) and `adapter_notes_omit_cluster_nodes` (`adapter_tests.rs:375-381`);
  - a doc: `docs/architecture.md:37`, `G = CLUSTER_NODES × PARALLELISM_FACTOR`, which reads as the old key, just as the reviewer said `NR_OF_CORES` does.

  The repository therefore breaks the new rule on the commit that adds it. Two bench log greps, `bench/sweep.sh:27` and `bench/parallelism_sweep.sh:29`, also match `CLUSTER_NODES`. The rule does not cover bench scripts.
- Fix: Make four changes.
  1. In the `pushdown/pushdown-planning/spec.md` delta, add a `DELTA:CHANGED` block for the scenario "Pushdown reads the cluster node count from the UDF handshake". Its GIVEN step reads "whose persisted `adapterNotes` carry no node-count entry". Add a § Scenario Coverage row for that scenario.
  2. In task 4.1, make `build_adapter_notes_merges_existing` use only `OTHER_KEY` as the inherited entry. Remove the `CLUSTER_NODES` assertion from `adapter_notes_omit_cluster_nodes`, or delete that test, because task 2.4's exact-key-set assertion proves the same behavior. Add both tests to § Dead Code Removal.
  3. In task 5.1, replace `G = CLUSTER_NODES × PARALLELISM_FACTOR` at `docs/architecture.md:37` with `G = node_count × PARALLELISM_FACTOR`.
  4. Add `specs/pushdown/pushdown-planning` and `docs/architecture.md:37` to the Knowledge of the matching group in § Parallelization.

## Feasibility

Checked: task 1.1's expectations match the code (`NAMESPACE` is required at `adapter/mod.rs:206-211`, and the `resolve_*` parsers are lenient at `mod.rs:597-686`). Each `TuningNotes` field (`usize`, `u64`) is at least as wide as its planned type, so the widening in task 4.2 loses nothing. The `JOIN_BROADCAST_MAX_BYTES` note read parses `u64` (`mod.rs:348-351`), so the largest accepted value survives the round trip. The check runs before `PermissionSettings::parse` (`mod.rs:199`), so it runs before the CONNECTION read (`mod.rs:163-165`). `USER_MAPPING` without `PERMISSION_CHECK` parses to `Off` (`permission.rs:33-35`). The bench sweeps pass neither `AUTO` nor `0` (`bench/sweep.sh:11-15`, and batch sizes of at least 8192).

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: `plan.md` § Implementation Tasks 1.2
- Issue: The task 1.2 stub "fails with the received `properties` and `schemaMetadataInfo.properties` as its error text". Because it also fails createVirtualSchema, no schema exists. The probes "`ALTER ... SET <name> = ''`, `ALTER ... SET <name> = null`, and an `ALTER ... REFRESH` after each" then have no schema to run against. The probe "whether a `setProperties` response without `schemaMetadata` keeps the tables" needs a stub that succeeds. Task 2.1's `empty_and_null_property_values_count_as_absent` pins these observations. Separately, the task records "whether a failed `ALTER ... SET` leaves `SYS.EXA_ALL_VIRTUAL_SCHEMA_PROPERTIES` unchanged" but gives no stop condition for that observation. The scenario "Set properties validates the merged properties after null unsets" requires that "the virtual schema SHALL keep its previous properties".
- Fix: In task 1.2, define the stub's answer for each request type. It succeeds on createVirtualSchema, setProperties, and refresh, returns one table, and writes the received `properties` and `schemaMetadataInfo.properties` into `adapterNotes`. The probes then read each delivered value from `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`. Use a second stub variant that fails `setProperties` for the failed-SET probe, and a third that omits `schemaMetadata` for the response-shape probe. Add the stop condition "a failed `ALTER ... SET` changes `SYS.EXA_ALL_VIRTUAL_SCHEMA_PROPERTIES`".

#### [EFFORT_MISESTIMATION] FINDINGS
- Location: `plan.md` § Implementation Tasks 4.1; § Dead Code Removal, row "Test"
- Issue: Task 4.2 changes every `resolve_*` function to take typed values, and § Dead Code Removal deletes `resolve_df_fixed_count`. `adapter_tests.rs` calls these functions at 45 call sites in 20 tests. Task 4.1 and § Dead Code Removal list 9 of those tests. The other 11 call the same functions with JSON properties and must also be rewritten or deleted: `auto_mode_uses_every_core_with_twice_as_many_partitions`, `auto_mode_yields_one_thread_on_one_core`, `default_parallelism_factor_is_one`, `df_target_partitions_explicit_wins`, `df_target_partitions_uses_supplied_value`, `df_threads_per_udf_explicit_wins`, `df_threads_per_udf_uses_supplied_value`, `explicit_parallelism_factor_overrides_default`, `fixed_mode_uses_supplied_values`, `resolve_s3_max_connections_fixed_value_wins`, and `threading_mode_parses_case_insensitively`.
- Fix: In task 4.1, list these 11 tests. Mark each one either deleted, because `properties_tests.rs` now covers its parsing, or rewritten over typed inputs. Extend the § Dead Code Removal "Test" row to match.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `vs-adapter/create-virtual-schema-adapter-notes/spec.md` § Scenario "createVirtualSchema adapterNotes omit the cluster node count"; `plan.md` § Context, bullet "Accepted ADRs checked"
- Issue: The scenario's WHEN step covers "a `createVirtualSchema`, `refresh`, or `setProperties` request". Its THEN step says "the adapter SHALL write exactly these schema-level entries into the returned `adapterNotes`: `PARALLELISM_FACTOR`, ... `SKIPPED_TABLES`, plus `SKIPPED_TABLES_OMITTED`". Four sources require a refresh to keep entries the adapter does not write:
  - The recorded scenario "Refresh rebuilds every identifier note and preserves other adapter notes" (`vs-adapter/refresh-and-set-properties`) requires that "the adapter SHALL preserve every other schema-level `adapterNotes` entry, including the entry it does not write".
  - ADR `adapternotes-admits-only-create-time-values-a-pushdown-cannot-recompute` decides that "An inherited node-count entry stays unread and inert", and it rejects "Actively remove an inherited key on every response".
  - `build_adapter_notes` starts from the persisted notes (`adapter/mod.rs:556`).
  - `build_adapter_notes_merges_existing` asserts that a refresh keeps an inherited entry.

  For a refresh of a schema whose notes hold any other entry, the delta and these sources cannot all hold. The wording comes from the PR request to "assert the exact set of note keys", which targets the CREATE test at `scan.rs:1000`. `plan.md` § Context says the ADR "is unaffected, because the notes' entries do not change". Under the new wording, that statement is false.
- Fix: In that scenario, narrow the WHEN step to "Exasol sends a `createVirtualSchema` request naming an Iceberg table". Add an AND step: "a `refresh` or `setProperties` response SHALL carry the same entries and keep every other entry of the persisted `adapterNotes`, per `vs-adapter/refresh-and-set-properties`, so an inherited node-count entry stays unread". In `plan.md` § Context, change the ADR bullet to: "the ADR is unaffected: a refresh keeps an inherited entry, as the ADR decides". Keep task 2.4's exact-key-set assertion on the CREATE test.

#### [AMBIGUOUS_REQUIREMENT] FINDINGS
- Location: `scan-runtime/scan-execution-threading/spec.md` § Background, bullet 1; `vs-adapter/create-virtual-schema-adapter-notes-resources/spec.md` § Scenario "Adapter records the DataFusion target partition count in the virtual-schema adapterNotes", the AND step on AUTO mode
- Issue: The same defects that PR points 2 and 3 named remain in text next to the scenarios the revision fixed. (a) The threading Background, which this delta leaves unchanged, says "The adapter resolves them from the `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF` adapterNotes". The note keys are `DF_TARGET_PARTITIONS` and `DF_THREADS_PER_UDF` (`adapter/mod.rs:62-63`). (b) The resources scenario says "in `AUTO` mode the adapter SHALL set the target partition count to `2 × nr_of_cores` ..., ignoring any supplied `DATAFUSION_TARGET_PARTITIONS` value". The next AND step then fails an invalid value. The threading delta already words this case precisely: "ignore a ... value that `vs-adapter/property-validation` accepts".
- Fix: Mark the threading `## Background` as `DELTA:CHANGED`, and in bullet 1 replace the two property names with the note keys `DF_TARGET_PARTITIONS` and `DF_THREADS_PER_UDF`. In the resources scenario, replace "ignoring any supplied `DATAFUSION_TARGET_PARTITIONS` value" with "ignoring a supplied `DATAFUSION_TARGET_PARTITIONS` value that `vs-adapter/property-validation` accepts".

## Task Breakdown

Checked: every delta has an implementing task. Group A holds all code and tests, and Group B holds the docs and the retired-name edits, so no group is sliced by layer. The stub swap is feasible, because `LEGACY_ADAPTER_BODY` (`refresh.rs:469`) already answers `setProperties` without `schemaMetadata`.

#### [TRACEABILITY_GAP] FINDINGS
- Location: `plan.md` § Implementation Tasks 2.1, the swap-test bullet; § Scenario Coverage, rows "Pushdown and drop never validate properties" and "An unknown property name fails the statement"
- Issue: The swap test "queries a seeded fixture table, refreshes, unsets, refreshes again, and drops". It drops the schema only after the stray properties are unset, so it never proves the step "`DROP VIRTUAL SCHEMA ... CASCADE` SHALL succeed" for a schema that holds stray properties. The scenario "An unknown property name fails the statement" also covers "`refresh` (with or without `requestedTables`)". Neither the E2E tests nor the task 4.1 unit tests send a refresh that carries `requestedTables`.
- Fix: In task 2.1, have the swap test run `ALTER VIRTUAL SCHEMA ... REFRESH TABLES <table>` before the unset step and assert the same error. Have it also drop a second virtual schema that still holds a stray property. That schema is created through the same stub swap, or by swapping the stub in again before the final drop. In task 4.1, add a refresh request that carries `requestedTables`.

#### [CLUSTER_INCOHERENCE] FINDINGS
- Location: `plan.md` § Parallelization
- Issue: Group A includes the verification tasks 6.1-6.3 and depends on nothing. Group B also depends on nothing. Group B edits a test file (`crates/lakehouse-engine/tests/it/scan_telemetry.rs`), `AGENTS.md`, and `specs/mission.md`. If the two groups run in parallel, Group A's `make fmt-check lint`, `cargo test`, and § Manual Testing pass can finish before Group B lands, so Group B's edits are never verified.
- Fix: Move tasks 6.1-6.3 into a Group C that depends on Groups A and B, and remove 6.1-6.3 from Group A's task list.

## Design Depth

No objection. Axis checked:
- Decision-log entries [1], [2], and the five Review Findings are all `Promotes to ADR: no`, so there is no `[ADR_OVERPROMOTION]`.
- The architecture delta's BASE matches `git hash-object specs/architecture.md` (`41cb305d`). A diff against the base shows only the intended edits: the `vs-adapter` line, the new `vs-properties` line, and the § Interfaces "VS properties" line. `plan.md` states no component, boundary, or interface that the delta lacks, so there is no `[ARCHITECTURE_DRIFT]`.
- ADRs checked: `allow-http-threaded-as-vended-selector-parameter` (the `ALLOW_HTTP` default stays false), `one-scan-shard-per-node-with-threads-on-every-core` (the `PARALLELISM_FACTOR` default stays 1, and a range of 1 to 255 does not conflict), and the `CATALOG_KIND` construction-site ADR (unchanged). The conflict with `adapternotes-admits-only-create-time-values-a-pushdown-cannot-recompute` is reported under the `[REQUIREMENT_CONFLICT]` BLOCKER in Requirement Quality, and the same fix resolves it.
- Quick Diagnostic for `vs-properties`: it is deep. One sentence states its job. Callers get typed values or one error. It hides the names, the type ranges, the edit distance, and the message format. It owns one decision, and the notes format stays with `vs-adapter`, as PR point 6 asks.

## Prose Quality

#### [PROSE_BLOAT] FINDINGS
- Location: `vs-adapter/create-virtual-schema-adapter-notes-resources/spec.md`, scenarios for the parallelism factor ("the adapter MUST NOT default it to `1`"), the threading mode ("the adapter MUST NOT select `AUTO` for it"), the memory-pool fraction ("MUST NOT default it to `0.6`"), and the instance overhead ("MUST NOT default it to `200`"); `scan-runtime/scan-execution-threading/spec.md` "FIXED mode ..." ("MUST NOT fall back to the AUTO value for it") and "Threading mode defaults to AUTO ..." ("instead of selecting `AUTO`"); `scan-runtime/scan-execution-connection-concurrency/spec.md` "AUTO derivation sizes ..." ("MUST NOT apply the AUTO derivation for it"); `vs-adapter/property-validation/spec.md` § Scenario "A tuning value is checked on its own and never replaced by its default", last AND step
- Issue: Each of these clauses follows a step that already says the value "SHALL fail the request". A request that fails records no default, so the clause adds no testable behavior. Repeated eight times, it brings back the framing that PR point 4 asked to remove: "reads as if selecting a default were a problem. It isn't."
- Fix: Delete each listed "MUST NOT default ..." or "instead of selecting ..." clause, and keep the "SHALL fail the request" clause. Rename the property-validation scenario to "A tuning value is checked on its own" and delete its last AND step. Update the matching row title in `plan.md` § Scenario Coverage.
