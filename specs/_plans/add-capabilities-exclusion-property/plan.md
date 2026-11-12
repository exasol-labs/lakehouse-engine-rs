# Plan: add-capabilities-exclusion-property

## Summary

Add the `EXCLUDED_CAPABILITIES` virtual schema property, which removes named capabilities from the `getCapabilities` response so Exasol evaluates them itself (#622). In the same change, `ALTER VIRTUAL SCHEMA ... REFRESH` stops checking properties, so only `CREATE` and `SET`, the statements that write properties, check them.

## Context

- The adapter always advertises its full `CAPABILITIES` list (`crates/lakehouse-engine/src/adapter/capabilities.rs`). Stopping one misbehaving or slow pushdown currently needs a new release.
- The Exasol Java virtual schemas offer an `EXCLUDED_CAPABILITIES` property. They parse it only when answering `getCapabilities` and fail every query on a name they do not know. This plan rejects such a name at `CREATE` and `SET` and ignores it at query time instead.
- `getCapabilities` today ignores the request. Exasol sends the persisted properties with it (`schemaMetadataInfo.properties`), which `get_properties` in `adapter/mod.rs` already reads for the other request types.
- Once a capability is advertised, Exasol delegates it fully (ADR `no-exasol-side-fallback-for-an-advertised-capability`). An exclusion makes Exasol send request shapes the adapter never receives today, so each shape must plan correctly or fail, never return different rows.
- A read-only audit of `adapter/pushdown/` found two request shapes that only an exclusion makes reachable and that need a live check. A join request without `selectList` renders its columns in leg order (`joins/sql_builders.rs` near line 166) and is never checked against `selectListDataTypes`. A grouped request without `selectList` routes to the wrapper and ends in an Exasol SQL error.
- An excluded construct runs under Exasol's own virtual-schema behavior. The verified `ORDER BY` with `LIMIT` behavior is recorded in decision-log entry [1]. It also corrects the Background of `pushdown-capabilities/pushdown-planning-order-by-capability`.
- `refresh` runs `check_properties` and `PermissionSettings::parse` today (`handle_create_virtual_schema` in `adapter/mod.rs`), and `resolve_connection_config` applies the permission kind and URI checks. A schema persisted before the checks existed therefore refuses `REFRESH` until its stray properties are unset (#534).
- `docs/capabilities.md` is checked against `CAPABILITIES` by `capabilities_doc_lists_exactly_the_advertised_set`, which reads every table row above `## Handled by Exasol`.
- The plan changes no scan, file selection, or type mapping. The pushdown planning that task 2.2b may change, the column order of a join without `selectList` and the grouped shape without `selectList`, follows the Exasol virtual schema protocol. No section of the Iceberg table spec or the Delta Lake protocol governs either.
- The architecture delta (`architecture.md` in this directory) adds a `capabilities` component and changes the `vs-properties`, `vs-adapter`, and interface bullets.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| excluded-capabilities | NEW | `vs-properties/excluded-capabilities/spec.md` |
| property-name-validation | CHANGED | `vs-properties/property-name-validation/spec.md` |
| property-value-validation | CHANGED | `vs-properties/property-value-validation/spec.md` |
| table-filter | CHANGED | `vs-properties/table-filter/spec.md` |
| create-virtual-schema | CHANGED | `vs-adapter/create-virtual-schema/spec.md` |
| refresh-and-set-properties | CHANGED | `vs-adapter/refresh-and-set-properties/spec.md` |
| lakekeeper-permission-check | CHANGED | `vs-adapter/lakekeeper-permission-check/spec.md` |
| create-virtual-schema-adapter-notes-resources | CHANGED | `vs-properties/create-virtual-schema-adapter-notes-resources/spec.md` |
| pushdown-planning-order-by-capability | CHANGED | `pushdown-capabilities/pushdown-planning-order-by-capability/spec.md` |

## Impact

- New optional property `EXCLUDED_CAPABILITIES`. A virtual schema without it advertises the same list as before. The property withholds exactly the names it lists.
- An excluded construct runs under Exasol's own virtual-schema behavior. Excluding `LIMIT` or `LIMIT_WITH_OFFSET` while an `ORDER_BY_*` capability stays advertised makes `ORDER BY ... LIMIT n OFFSET m` return the first n rows. `docs/semantic-differences.md` documents this and the workaround.
- Behavior change: `ALTER VIRTUAL SCHEMA ... REFRESH` no longer fails on a persisted unknown name, an invalid tuning, table pattern, or `EXCLUDED_CAPABILITIES` value, or an invalid `PERMISSION_CHECK` or `USER_MAPPING`. A persisted invalid tuning value takes its default in the refreshed `adapterNotes`. `CREATE` and `SET` keep rejecting all of them, and a query keeps failing on invalid permission properties.
- Exception: `REFRESH` still fails when `CATALOG_KIND`, `CATALOG_CONNECTION`, `NAMESPACE`, `MERGE_SCHEMA`, or `HIVE_PARTITIONING` cannot be used, because it reads them to list the source and declare its tables (decision-log [3]).
- Downgrade: an older adapter rejects `EXCLUDED_CAPABILITIES` as an unknown property on `CREATE`, `SET`, and `REFRESH`, and advertises its full list. Unset the property before a downgrade.
- No breaking change for existing statements.

## Implementation Tasks

### 1. Property catalog, capability exclusion, and refresh

- [ ] 1.1 In `adapter/capabilities.rs`, add one function that divides a raw `EXCLUDED_CAPABILITIES` value into its advertised entries and its distinct unadvertised entries in value order (split on `,`, trim each entry, skip empty entries, exact case-sensitive match). Change `get_capabilities_response` to take the raw value (`Option<&str>`) and return `CAPABILITIES` in order, less the advertised entries that function returns and nothing else. The module is the one owner of the entry syntax and keeps no dependency on `properties.rs`.
- [ ] 1.2 In `adapter/properties.rs`, add `EXCLUDED_CAPABILITIES` to `PROPERTY_NAMES` (22 names) and an accessor that returns its non-empty value. Add its value rule to `TuningChecker`. The rule does no parsing of its own: it reports the unadvertised entries that the function of 1.1 returns, one `invalid value '<entry>' for 'EXCLUDED_CAPABILITIES': expected an advertised capability name` each, echoed by `echo`, with `(did you mean '<capability>'?)` from `nearest_name` over `CAPABILITIES`. The entries share the property's slot in the alphabetical invalid-value group, joined by `; `.
- [ ] 1.3 In `dispatch` (`adapter/mod.rs`), answer `getCapabilities` with `get_capabilities_response` over the accessor of 1.2 applied to `get_properties(request)`. A `getCapabilities` request never fails because of the property value.
- [ ] 1.4 Split `check_properties` into the failing check for `createVirtualSchema` and `setProperties` and a non-failing read for `refresh`, which runs no unknown-name scan and reads each rejected value as absent. For `refresh`, `handle_create_virtual_schema` also skips `PermissionSettings::parse` and passes the check-off setting to `resolve_connection_config`, so neither the permission kind nor the `/catalog` URI requirement runs. Update the doc comments of `check_properties`, `resolve_boolean`, the `properties.rs` module, `PermissionSettings::parse`, and `PermissionCheck` (`permission.rs`) so none describes a check on `refresh`.
- [ ] 1.5 Unit tests in `capabilities_tests.rs`: withholding in advertised order with whitespace and empty entries; exact, case-sensitive matching (`fn_pred_like` withholds nothing); an unknown entry is ignored; each listed name withholds only itself, so excluding `LIMIT_WITH_OFFSET` leaves `LIMIT` advertised; an absent value, or one of only empty entries such as `' , '`, gives the full list. Update `reports_audited_capability_set` and the other callers to the new signature.
- [ ] 1.6 Unit tests in `properties_tests.rs`: the `EXCLUDED_CAPABILITIES` entries (suggestion, case-folded suggestion, no suggestion beyond two edits, a duplicate reported once, value order, echo cap, placement in the combined error); every advertised name accepted, alone and all together; whitespace around entries and empty entries accepted; the refresh read never fails and reads each rejected value as absent. In `adapter_tests.rs`, drop the `"refresh"` rows from `an_unknown_property_name_fails_every_schema_request_before_the_connection_is_read` and `an_invalid_tuning_value_fails_every_schema_request_before_the_connection_is_read`, and add `refresh_runs_no_property_check`: a `refresh` with an unknown name, an invalid tuning value, and `PERMISSION_CHECK = 'NOT_A_MODE'` fails on the missing CONNECTION, not with a property or permission error.
- [ ] 1.7 Move `PERSISTING_ADAPTER_BODY`, `STUB_ADAPTER_SCHEMA`, and `persist_stray_properties` from `e2e_local/property_validation.rs` into `e2e_local/fixtures.rs`, with the virtual schema name and the `SET` clause as parameters, because 1.8 and group B use them.
- [ ] 1.8 In `e2e_local/property_validation.rs`:
  - Replace `stray_persisted_properties_block_refresh_but_not_queries` with `stray_persisted_properties_refresh_and_query`, which keeps the line `/// Scenario: Pushdown and drop never validate properties`. The stray `SET` adds `EXCLUDED_CAPABILITIES = 'NO_SUCH_CAPABILITY'`. The count query runs once before any `REFRESH` and returns `SEED_TOTAL_ROWS`. `REFRESH` and `REFRESH TABLES <table>` succeed, the notes hold `MEMORY_POOL_FRACTION` `0.6` and `METADATA_READ_CONCURRENCY` `16`, the count query returns `SEED_TOTAL_ROWS`, and `DROP ... CASCADE` succeeds. Delete the unset-then-refresh steps.
  - Add `stray_permission_properties_refresh_and_fail_queries`, one case table on its own virtual schema: `PERMISSION_CHECK = 'NOT_A_MODE'`; `PERMISSION_CHECK = 'LAKEKEEPER'` with a `USER_MAPPING` that does not compile; `PERMISSION_CHECK = 'LAKEKEEPER'` with a compiling `USER_MAPPING` over the local catalog URI, which does not end in `/catalog`. For each case, the stub persists the properties, `REFRESH` succeeds, and a query fails with that check's error.
  - Add `EXCLUDED_CAPABILITIES` with a valid value to `create_accepts_every_known_property_name`.
  - Add `create_and_set_reject_an_unadvertised_capability`: `CREATE` and `SET` with `'FN_PRED_LIK, fn_pred_like, MADE_UP, FN_PRED_LIK'` fail with the exact combined message, and the failed `SET` keeps the schema's properties.
  - Rewrite the module doc comment to name `CREATE` and `ALTER ... SET` only.

### 2. Exclusion behavior through Exasol

- [ ] 2.1 Add `crates/lakehouse-e2e/tests/e2e_local/capability_exclusion.rs` and declare it in `e2e_local/main.rs`. Move `JOIN_ORACLE` and `copy_join_tables_natively` from `e2e_local/join.rs` into `e2e_local/fixtures.rs`, with the source virtual schema as a parameter, because the join cases need them. Each test creates its own virtual schema over `E2E_NAMESPACE` with `VsProps::with_property("EXCLUDED_CAPABILITIES", ...)` and compares rows with a native copy (`native_copy_mismatch`, `PathCheck`). Each test also reads the `capabilities` array that `EXPLAIN VIRTUAL` echoes, as `e2e_local/capability.rs` does. It parses the array instead of matching a substring, because the echoed request also carries the property value. It asserts that each excluded name is absent and that `FN_PRED_EQUAL`, which no value lists, is present. No case excludes `LIMIT`, `LIMIT_WITH_OFFSET`, `ORDER_BY_COLUMN`, or `ORDER_BY_EXPRESSION` (decision-log [1]). The tests:
  - `excluded_like_is_evaluated_by_exasol`: `WHERE name LIKE 'event-1%'` on the events table returns the native rows with `PathCheck::Lacks("LIKE")`, while the same query on `LAKEHOUSE_VS` shows `LIKE` in its pushdown SQL.
  - `set_exclusion_applies_from_the_next_query_without_refresh`: `SET EXCLUDED_CAPABILITIES = 'FN_PRED_LIKE'`, `EXPLAIN VIRTUAL` without `LIKE`, `SET ... = null`, `EXPLAIN VIRTUAL` with `LIKE`, and no `REFRESH`. The schema-level notes hold no `EXCLUDED_CAPABILITIES` entry. This test also verifies against live Exasol that `getCapabilities` carries the properties and runs per query. If it does not, the test fails saying so.
  - `whitespace_and_empty_entries_are_accepted`: `CREATE` with `' FN_PRED_LIKE ,, FN_AGG_COUNT_DISTINCT,'` succeeds, and the echoed capabilities lack exactly `FN_PRED_LIKE` and `FN_AGG_COUNT_DISTINCT`. A second schema with `' , '` echoes the full list.
  - `exclusion_combinations_return_native_rows`: one case table of exclusion set and queries, as the scenario "Request shapes that an exclusion makes reachable return the same rows" lists them. Where an exclusion forces a shape, the case also asserts it on the pushdown SQL, for example no `JOIN` when `JOIN` is excluded. Join cases use the fact and dimension tables and the moved native-copy helper.
  - `get_capabilities_ignores_a_persisted_unadvertised_entry`: the stub from 1.7 persists `EXCLUDED_CAPABILITIES = 'NO_SUCH_CAPABILITY, FN_PRED_LIKE'`. The echoed capabilities lack `FN_PRED_LIKE` and hold `FN_PRED_EQUAL`, and the `LIKE` query returns the native rows with `PathCheck::Lacks("LIKE")`.
- [ ] 2.2a Run 2.1 against the live stack. For each case, capture the pushdown request JSON that `EXPLAIN VIRTUAL` echoes. Record in `notes/` each request shape that errors or returns rows other than the native copy, with its request JSON.
- [ ] 2.2b Fix each shape recorded in 2.2a in the pushdown planner, and pin each fixed rule with a unit test in the sibling `_tests.rs` of the module it changes. A join request without `selectList` returns its columns in the order Exasol declares in `selectListDataTypes`. A grouped request without `selectList` returns one row per group with the columns `selectListDataTypes` declares, in that order. If 2.2a shows that Exasol sends no grouped request without `selectList`, record that in `notes/` and change no code for it. [expert]

### 3. Documentation

- [ ] 3.1 `docs/tuning.md`: rewrite the paragraph under `## Parameters`. `CREATE VIRTUAL SCHEMA` and `ALTER VIRTUAL SCHEMA ... SET` check names and values. `REFRESH` reads the properties without checking them, and a value those statements would reject takes its default. Add an `EXCLUDED_CAPABILITIES` row covering the default, the accepted values (whitespace and empty entries ignored), the effect, `SET` without `REFRESH`, and the `CREATE`/`SET` rejection with suggestion. State the concrete effect: excluding `LIMIT` or `LIMIT_WITH_OFFSET` while an `ORDER_BY_*` capability stays advertised makes `ORDER BY ... LIMIT n OFFSET m` return the first n rows, with a link to the section of 3.5. Add two sentences: unset `EXCLUDED_CAPABILITIES` before a downgrade to an adapter without the property; when an upgrade stops advertising a listed name, remove that name in the same `SET` that changes another property. Use `'FN_PRED_LIKE'` as the example value.
- [ ] 3.2 `docs/capabilities.md`: add a section after `## Handled by Exasol` that explains per-schema exclusion with the example value `'FN_PRED_LIKE'`. Keep `EXCLUDED_CAPABILITIES` out of the table rows above that heading, because `capabilities_doc_lists_exactly_the_advertised_set` reads them.
- [ ] 3.3 `docs/capability-matrix.md`: state in the introduction that a ✓ is the default and `EXCLUDED_CAPABILITIES` withdraws it per schema.
- [ ] 3.4 `docs/security.md`, section `USER_MAPPING`: a template that does not compile rejects create and `SET`, and refuses each query. Remove refresh from that sentence.
- [ ] 3.5 `docs/semantic-differences.md`: add a section on `ORDER BY` with `LIMIT` under `EXCLUDED_CAPABILITIES`.
  - Link Exasol's [virtual schema limitations](https://docs.exasol.com/db/latest/database_concepts/virtual_schema/user_guide.htm#Virtualschemalimitations). Its ORDER BY clause limitations say that the outermost order of imported result rows is not guaranteed, with the workaround `SELECT * FROM (<virtual-schema-query> ORDER BY FALSE) ORDER BY <criteria>`.
  - State that excluding `LIMIT` or `LIMIT_WITH_OFFSET` while an `ORDER_BY_*` capability stays advertised makes `ORDER BY ... LIMIT n OFFSET m` return the first n rows: `ORDER BY id LIMIT 5 OFFSET 3` over the ids 1 to 20 returns 1 to 5, observed on Exasol 2026.2.0-nano.3.
  - Give the workaround: also exclude `ORDER_BY_COLUMN` and `ORDER_BY_EXPRESSION`.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Property catalog, capability exclusion, and refresh | 1.1-1.8 | none | spec deltas `vs-properties/excluded-capabilities` (scenarios 1, 5), `vs-properties/property-name-validation`, `vs-properties/property-value-validation`, `vs-properties/table-filter`, `vs-adapter/create-virtual-schema`, `vs-adapter/refresh-and-set-properties`, `vs-adapter/lakekeeper-permission-check`; `adapter/capabilities.rs`, `adapter/capabilities_tests.rs`, `adapter/properties.rs`, `adapter/properties_tests.rs`, `adapter/permission.rs`, `adapter/mod.rs` (`dispatch`, `handle_create_virtual_schema`, `resolve_connection_config`), `adapter/adapter_tests.rs`, `e2e_local/property_validation.rs`, `e2e_local/fixtures.rs` |
| B: Exclusion behavior through Exasol | 2.1, 2.2a, 2.2b | A (needs the property and the moved stub helper) | spec delta `vs-properties/excluded-capabilities` (scenarios 1-4, 6); decision-log [1]; `adapter/pushdown/mod.rs`, `adapter/pushdown/request_shape.rs`, `adapter/pushdown/grouped_agg.rs`, `adapter/pushdown/joins/sql_builders.rs`, their `_tests.rs` files, `e2e_local/capability_exclusion.rs`, `e2e_local/main.rs`, `e2e_local/capability.rs` (capabilities echoed by `EXPLAIN VIRTUAL`), `e2e_local/join.rs` (`JOIN_ORACLE`, `copy_join_tables_natively`), `e2e_local/fixtures.rs`, `common/e2e_harness.rs` (`native_copy_mismatch`, `PathCheck`) |
| C: Documentation | 3.1-3.5 | B (documents the behavior that 2.1 verifies and the shapes that 2.2b settles) | spec deltas `vs-properties/excluded-capabilities`, `vs-properties/property-value-validation`, `vs-adapter/lakekeeper-permission-check`, `pushdown-capabilities/pushdown-planning-order-by-capability`; decision-log [1]; `docs/tuning.md`, `docs/capabilities.md`, `docs/capability-matrix.md`, `docs/security.md`, `docs/semantic-differences.md`, `adapter/capabilities_tests.rs` (`capabilities_doc_lists_exactly_the_advertised_set`) |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Call | `check_properties` and `PermissionSettings::parse` on `refresh` in `handle_create_virtual_schema`, `crates/lakehouse-engine/src/adapter/mod.rs` | `refresh` runs no property check |
| Test rows | `"refresh"` in the request-type loops of two tests, `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | Asserted the removed refresh check |
| Test | `stray_persisted_properties_block_refresh_but_not_queries` and its unset-then-refresh steps, `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` | Proved the removed scenario; replaced by `stray_persisted_properties_refresh_and_query` |
| Doc text | `REFRESH` in the validation paragraph of `docs/tuning.md`, "refresh" in the template sentence of `docs/security.md`, the module doc comment of `property_validation.rs`, and the doc comments of `PermissionSettings::parse` and `PermissionCheck` | Described the removed refresh checks |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| An excluded capability is withheld from getCapabilities | Unit, E2E | `adapter/capabilities_tests.rs`, `e2e_local/capability_exclusion.rs` | `excluded_capabilities_are_withheld_in_advertised_order`, `an_exclusion_withholds_only_the_listed_names`, `whitespace_and_empty_entries_are_accepted` |
| An excluded capability is evaluated by Exasol | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability_exclusion.rs` | `excluded_like_is_evaluated_by_exasol` |
| A changed exclusion applies from the next query | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability_exclusion.rs` | `set_exclusion_applies_from_the_next_query_without_refresh` |
| Request shapes that an exclusion makes reachable return the same rows | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability_exclusion.rs` | `exclusion_combinations_return_native_rows` |
| Create and set reject an entry the adapter does not advertise | E2E, Unit | `e2e_local/property_validation.rs`, `e2e_local/capability_exclusion.rs`, `adapter/properties_tests.rs` | `create_and_set_reject_an_unadvertised_capability`, `whitespace_and_empty_entries_are_accepted`, `excluded_capability_entries_name_each_unadvertised_entry`, `excluded_capability_entries_accept_whitespace_and_empty_entries` |
| getCapabilities ignores an entry the adapter does not advertise | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability_exclusion.rs` | `get_capabilities_ignores_a_persisted_unadvertised_entry` |
| An unknown property name fails the statement | E2E, Unit | `e2e_local/property_validation.rs`, `adapter/adapter_tests.rs` | `create_rejects_unknown_property_names_before_reading_the_connection`, `set_properties_rejects_problems_and_keeps_the_schema`, `an_unknown_property_name_fails_every_schema_request_before_the_connection_is_read` |
| Every property the adapter reads is known under every catalog kind | E2E | `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` | `create_accepts_every_known_property_name` |
| Pushdown and drop never validate properties | E2E | `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` | `stray_persisted_properties_refresh_and_query` |
| Adapter records the metadata read concurrency in the virtual-schema adapterNotes | E2E | `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` | `create_accepts_every_known_property_name`, `create_rejects_each_invalid_tuning_value`, `stray_persisted_properties_refresh_and_query` |
| An invalid tuning value fails the statement and names the accepted values | E2E | `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` | `create_rejects_each_invalid_tuning_value`, `set_properties_rejects_problems_and_keeps_the_schema` |
| A whole-number tuning value is checked against the range of its type | E2E | `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` | `create_rejects_each_invalid_tuning_value` |
| A tuning value is checked on its own and never replaced by its default | E2E | `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` | `create_rejects_each_invalid_tuning_value` |
| Refresh reads the persisted properties without checking them | E2E, Unit | `e2e_local/property_validation.rs`, `adapter/adapter_tests.rs` | `stray_persisted_properties_refresh_and_query`, `refresh_runs_no_property_check` |
| An empty pattern entry fails the statement | E2E | `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` | `create_rejects_an_empty_table_pattern_entry` |
| Adapter reports its pushdown capabilities | Unit | `crates/lakehouse-engine/src/adapter/capabilities_tests.rs` | `reports_audited_capability_set` |
| Invalid permission properties are rejected before the catalog is contacted | E2E, Unit | `e2e_local/property_validation.rs`, `adapter/permission_tests.rs` | `stray_permission_properties_refresh_and_fail_queries`, `invalid_permission_properties_are_rejected`, `a_template_that_does_not_compile_is_rejected` |
| The check is accepted only for an Iceberg REST catalog URI that ends in /catalog | E2E, Unit | `e2e_local/property_validation.rs`, `adapter/permission_tests.rs` | `stray_permission_properties_refresh_and_fail_queries`, `the_check_needs_the_iceberg_rest_kind`, `the_check_needs_a_catalog_uri_ending_in_catalog` |
| ORDER_BY_COLUMN is advertised so ordered top-N queries can be pushed down | Unit | `crates/lakehouse-engine/src/adapter/capabilities_tests.rs` | `advertises_order_by_column_and_expression` |

`vs-adapter/refresh-and-set-properties` changes only its Background. `pushdown-capabilities/pushdown-planning-order-by-capability` corrects one Background fact and the reason clause of one scenario; the behavior and its tests stay unchanged. The corrected fact was verified on Exasol 2026.2.0-nano.3 with a probe adapter (decision-log [1]).

### Manual Testing

Run after `make e2e-up test-e2e`, which deploys the adapter into schema `LHVS` and the CONNECTION `LAKEHOUSE_CATALOG_CREDS`. Every command is `exapump sql -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0" "<SQL>"`.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| excluded-capabilities | `CREATE VIRTUAL SCHEMA CE_MANUAL USING LHVS.LAKEHOUSE_ADAPTER WITH CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS' ALLOW_HTTP = 'true' NAMESPACE = 'e2e_lakehouse' EXCLUDED_CAPABILITIES = 'FN_PRED_LIK'` | Fails with `invalid virtual schema properties: invalid value 'FN_PRED_LIK' for 'EXCLUDED_CAPABILITIES': expected an advertised capability name (did you mean 'FN_PRED_LIKE'?)` |
| excluded-capabilities | The same statement, including `ALLOW_HTTP = 'true'`, with `EXCLUDED_CAPABILITIES = 'FN_PRED_LIKE'`, then `EXPLAIN VIRTUAL SELECT id FROM CE_MANUAL.EVENTS WHERE name LIKE 'event-1%'` | Succeeds; `PUSHDOWN_SQL` holds no `LIKE` |
| excluded-capabilities | `ALTER VIRTUAL SCHEMA CE_MANUAL SET EXCLUDED_CAPABILITIES = null`, then the same `EXPLAIN VIRTUAL` | `PUSHDOWN_SQL` holds the `LIKE` predicate, with no `REFRESH` run |
| pushdown-planning-order-by-capability | `ALTER VIRTUAL SCHEMA CE_MANUAL SET EXCLUDED_CAPABILITIES = 'LIMIT_WITH_OFFSET'`, then `EXPLAIN VIRTUAL` and the query `SELECT id FROM CE_MANUAL.EVENTS ORDER BY id LIMIT 5 OFFSET 3` | `PUSHDOWN_SQL` orders by `id` and carries no limit; the query returns the ids ranked 1 to 5, the Exasol behavior the corrected Background states |
| property-name-validation, property-value-validation | `ALTER VIRTUAL SCHEMA CE_MANUAL SET MADE_UP = 'x'` | Fails with `invalid virtual schema properties: unknown property 'MADE_UP'` |
| lakekeeper-permission-check | `ALTER VIRTUAL SCHEMA CE_MANUAL SET PERMISSION_CHECK = 'NOT_A_MODE'` | Fails with the unrecognized-value error naming `LAKEKEEPER` |
| property-value-validation, refresh-and-set-properties | `ALTER VIRTUAL SCHEMA CE_MANUAL REFRESH` | Succeeds |
| cleanup | `DROP VIRTUAL SCHEMA CE_MANUAL CASCADE` | Succeeds |

A refresh over persisted invalid properties needs the stub adapter of task 1.7 to persist them, so `stray_persisted_properties_refresh_and_query` and `stray_permission_properties_refresh_and_fail_queries` are its proof.

### Checklist

How to run the suites: `specs/mission.md` § Commands and `specs/testing.md` § E2E suites.

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Unit tests | `cargo test --workspace --lib` | 0 failures |
| E2E (local Docker) | `make e2e-up test-e2e` | 0 failures |
| Lint | `make lint` | 0 errors or warnings |
| Format | `make fmt-check` | No changes |
