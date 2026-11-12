# Plan: add-capabilities-exclusion-property

## Summary

Add the `EXCLUDED_CAPABILITIES` virtual schema property, which removes named capabilities from the `getCapabilities` response so Exasol evaluates them itself (#622).

## Context

- The adapter always advertises its full `CAPABILITIES` list (`crates/lakehouse-engine/src/adapter/capabilities.rs`). Stopping one misbehaving or slow pushdown currently needs a new release.
- The Exasol Java virtual schemas offer an `EXCLUDED_CAPABILITIES` property. They parse it only when answering `getCapabilities` and fail every query on a name they do not know. This plan rejects such a name at `CREATE`, `REFRESH`, and `SET`, together with the other property checks, and ignores it at query time instead.
- `getCapabilities` today ignores the request. Exasol sends the persisted properties with it (`schemaMetadataInfo.properties`), which `get_properties` in `adapter/mod.rs` already reads for the other request types.
- Once a capability is advertised, Exasol delegates it fully (ADR `no-exasol-side-fallback-for-an-advertised-capability`). An exclusion makes Exasol send request shapes the adapter never receives today, so each shape must plan correctly or fail, never return different rows.
- A read-only audit of `adapter/pushdown/` found two request shapes that only an exclusion makes reachable and that need a live check. A join request without `selectList` renders its columns in leg order (`joins/sql_builders.rs` near line 166) and is never checked against `selectListDataTypes`. A grouped request without `selectList` routes to the wrapper and ends in an Exasol SQL error.
- An excluded construct runs under Exasol's own virtual-schema behavior. The verified `ORDER BY` with `LIMIT` behavior is recorded in decision-log entry [1]. It also corrects the Background of `pushdown-capabilities/pushdown-planning-order-by-capability`.
- `docs/capabilities.md` is checked against `CAPABILITIES` by `capabilities_doc_lists_exactly_the_advertised_set`, which reads every table row above `## Handled by Exasol`.
- The plan changes no scan, file selection, or type mapping. The pushdown planning that task 2.2b may change, the column order of a join without `selectList` and the grouped shape without `selectList`, follows the Exasol virtual schema protocol. No section of the Iceberg table spec or the Delta Lake protocol governs either.
- The architecture delta (`architecture.md` in this directory) adds a `capabilities` component and changes the `vs-properties`, `vs-adapter`, and interface bullets.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| excluded-capabilities | NEW | `vs-properties/excluded-capabilities/spec.md` |
| property-name-validation | CHANGED | `vs-properties/property-name-validation/spec.md` |
| property-value-validation | CHANGED | `vs-properties/property-value-validation/spec.md` |
| create-virtual-schema | CHANGED | `vs-adapter/create-virtual-schema/spec.md` |
| pushdown-planning-order-by-capability | CHANGED | `pushdown-capabilities/pushdown-planning-order-by-capability/spec.md` |

## Impact

- New optional property `EXCLUDED_CAPABILITIES`. A virtual schema without it advertises the same list as before. The property withholds exactly the names it lists.
- An excluded construct runs under Exasol's own virtual-schema behavior. Excluding `LIMIT` or `LIMIT_WITH_OFFSET` while an `ORDER_BY_*` capability stays advertised makes `ORDER BY ... LIMIT n OFFSET m` return the first n rows. `docs/semantic-differences.md` documents this and the workaround.
- `CREATE`, `REFRESH`, and `SET` reject an `EXCLUDED_CAPABILITIES` entry the adapter does not advertise, like every other invalid property value. A query never fails because of the value.
- Downgrade: an older adapter rejects `EXCLUDED_CAPABILITIES` as an unknown property on `CREATE`, `SET`, and `REFRESH`, and advertises its full list. Unset the property before a downgrade.
- Upgrade: when a later adapter stops advertising a listed name, `REFRESH` and `SET` fail until a `SET` removes that entry. Queries keep working, because `getCapabilities` ignores the entry (decision-log [2]).
- No breaking change for existing statements.

## Implementation Tasks

### 1. Property catalog and capability exclusion

- [ ] 1.1 In `adapter/capabilities.rs`, add one function that divides a raw `EXCLUDED_CAPABILITIES` value into its advertised entries and its distinct unadvertised entries in value order (split on `,`, trim each entry, skip empty entries, exact case-sensitive match). Change `get_capabilities_response` to take the raw value (`Option<&str>`) and return `CAPABILITIES` in order, less the advertised entries that function returns and nothing else. The module is the one owner of the entry syntax and keeps no dependency on `properties.rs`.
- [ ] 1.2 In `adapter/properties.rs`, add `EXCLUDED_CAPABILITIES` to `PROPERTY_NAMES` (22 names) and an accessor that returns its non-empty value. Add its value rule to `TuningChecker`, so `createVirtualSchema`, `refresh`, and `setProperties` all run it. The rule does no parsing of its own: it reports the unadvertised entries that the function of 1.1 returns, one `invalid value '<entry>' for 'EXCLUDED_CAPABILITIES': expected an advertised capability name` each, echoed by `echo`, with `(did you mean '<capability>'?)` from `nearest_name` over `CAPABILITIES`. The entries share the property's slot in the alphabetical invalid-value group, joined by `; `.
- [ ] 1.3 In `dispatch` (`adapter/mod.rs`), answer `getCapabilities` with `get_capabilities_response` over the accessor of 1.2 applied to `get_properties(request)`. A `getCapabilities` request never fails because of the property value.
- [ ] 1.4 Unit tests in `capabilities_tests.rs`: withholding in advertised order with whitespace and empty entries; exact, case-sensitive matching (`fn_pred_like` withholds nothing); an unknown entry is ignored; each listed name withholds only itself, so excluding `LIMIT_WITH_OFFSET` leaves `LIMIT` advertised; an absent value, or one of only empty entries such as `' , '`, gives the full list. Update `reports_audited_capability_set` and the other callers to the new signature.
- [ ] 1.5 Unit tests in `properties_tests.rs`: the `EXCLUDED_CAPABILITIES` entries (suggestion, case-folded suggestion, no suggestion beyond two edits, a duplicate reported once, value order, echo cap, placement in the combined error); every advertised name accepted, alone and all together; whitespace around entries and empty entries accepted (`excluded_capability_entries_accept_whitespace_and_empty_entries`). In `adapter_tests.rs`, add `an_unadvertised_excluded_capability_fails_every_schema_request_before_the_connection_is_read`, which loops over `createVirtualSchema`, `refresh`, and `setProperties` like the two existing request-type tests.
- [ ] 1.6 Move `PERSISTING_ADAPTER_BODY` and `STUB_ADAPTER_SCHEMA` from `e2e_local/property_validation.rs` into `e2e_local/fixtures.rs`, with a helper that persists a given `SET` clause on a given virtual schema, because 2.1 uses it. `persist_stray_properties` keeps its signature and calls that helper, so `stray_persisted_properties_block_refresh_but_not_queries` stays unchanged.
- [ ] 1.7 In `e2e_local/property_validation.rs`:
  - Add `EXCLUDED_CAPABILITIES` with a valid value to `create_accepts_every_known_property_name`.
  - Add `create_and_set_reject_an_unadvertised_capability`: `CREATE` and `SET` with `EXCLUDED_CAPABILITIES = 'FN_PRED_LIK, fn_pred_like, MADE_UP, FN_PRED_LIK'`, `MADE_UP_PROPERTY = 'x'`, `DATAFUSION_BATCH_SIZE = 'abc'`, and `PARALLELISM_FACTOR = '0'` fail with the exact combined message, in which the `EXCLUDED_CAPABILITIES` entries sit between the two invalid tuning values. The failed `SET` keeps the schema's properties.

### 2. Exclusion behavior through Exasol

- [ ] 2.1 Add `crates/lakehouse-e2e/tests/e2e_local/capability_exclusion.rs` and declare it in `e2e_local/main.rs`. Move `JOIN_ORACLE` and `copy_join_tables_natively` from `e2e_local/join.rs` into `e2e_local/fixtures.rs`, with the source virtual schema as a parameter, because the join cases need them. Each test creates its own virtual schema over `E2E_NAMESPACE` with `VsProps::with_property("EXCLUDED_CAPABILITIES", ...)` and compares rows with a native copy (`native_copy_mismatch`, `PathCheck`). Each test also reads the `capabilities` array that `EXPLAIN VIRTUAL` echoes, as `e2e_local/capability.rs` does. It parses the array instead of matching a substring, because the echoed request also carries the property value. It asserts that each excluded name is absent and that `FN_PRED_EQUAL`, which no value lists, is present. No case excludes `LIMIT`, `LIMIT_WITH_OFFSET`, `ORDER_BY_COLUMN`, or `ORDER_BY_EXPRESSION` (decision-log [1]). The tests:
  - `excluded_like_is_evaluated_by_exasol`: `WHERE name LIKE 'event-1%'` on the events table returns the native rows with `PathCheck::Lacks("LIKE")`, while the same query on `LAKEHOUSE_VS` shows `LIKE` in its pushdown SQL.
  - `set_exclusion_applies_from_the_next_query_without_refresh`: `SET EXCLUDED_CAPABILITIES = 'FN_PRED_LIKE'`, `EXPLAIN VIRTUAL` without `LIKE`, `SET ... = null`, `EXPLAIN VIRTUAL` with `LIKE`, and no `REFRESH`. The schema-level notes hold no `EXCLUDED_CAPABILITIES` entry. This test also verifies against live Exasol that `getCapabilities` carries the properties and runs per query. If it does not, the test fails saying so.
  - `whitespace_and_empty_entries_are_accepted`: `CREATE` with `' FN_PRED_LIKE ,, FN_AGG_COUNT_DISTINCT,'` succeeds, and the echoed capabilities lack exactly `FN_PRED_LIKE` and `FN_AGG_COUNT_DISTINCT`. A second schema with `' , '` echoes the full list.
  - `exclusion_combinations_return_native_rows`: one case table of exclusion set and queries, as the scenario "Request shapes that an exclusion makes reachable return the same rows" lists them. Where an exclusion forces a shape, the case also asserts it on the pushdown SQL, for example no `JOIN` when `JOIN` is excluded. Join cases use the fact and dimension tables and the moved native-copy helper.
  - `get_capabilities_ignores_a_persisted_unadvertised_entry`: the stub helper from 1.6 persists `EXCLUDED_CAPABILITIES = 'NO_SUCH_CAPABILITY, FN_PRED_LIKE'`. The echoed capabilities lack `FN_PRED_LIKE` and hold `FN_PRED_EQUAL`, and the `LIKE` query returns the native rows with `PathCheck::Lacks("LIKE")`. `REFRESH` and a `SET` of another property fail with `invalid virtual schema properties: invalid value 'NO_SUCH_CAPABILITY' for 'EXCLUDED_CAPABILITIES': expected an advertised capability name`, and the `LIKE` query still returns the native rows. `SET EXCLUDED_CAPABILITIES = 'FN_PRED_LIKE'` succeeds, and a later `REFRESH` succeeds.
- [ ] 2.2a Run 2.1 against the live stack. For each case, capture the pushdown request JSON that `EXPLAIN VIRTUAL` echoes. Record in `notes/` each request shape that errors or returns rows other than the native copy, with its request JSON.
- [ ] 2.2b Fix each shape recorded in 2.2a in the pushdown planner, and pin each fixed rule with a unit test in the sibling `_tests.rs` of the module it changes. A join request without `selectList` returns its columns in the order Exasol declares in `selectListDataTypes`. A grouped request without `selectList` returns one row per group with the columns `selectListDataTypes` declares, in that order. If 2.2a shows that Exasol sends no grouped request without `selectList`, record that in `notes/` and change no code for it. [expert]

### 3. Documentation

- [ ] 3.1 `docs/tuning.md`: add an `EXCLUDED_CAPABILITIES` row covering the default, the accepted values (whitespace and empty entries ignored), the effect, `SET` without `REFRESH`, and the rejection with suggestion by `CREATE`, `REFRESH`, and `SET`. State the concrete effect: excluding `LIMIT` or `LIMIT_WITH_OFFSET` while an `ORDER_BY_*` capability stays advertised makes `ORDER BY ... LIMIT n OFFSET m` return the first n rows, with a link to the section of 3.4. Add two sentences: unset `EXCLUDED_CAPABILITIES` before a downgrade to an adapter without the property; when an upgrade stops advertising a listed name, `REFRESH` and `SET` fail until a `SET` of `EXCLUDED_CAPABILITIES` removes that name, while queries keep working. Use `'FN_PRED_LIKE'` as the example value.
- [ ] 3.2 `docs/capabilities.md`: add a section after `## Handled by Exasol` that explains per-schema exclusion with the example value `'FN_PRED_LIKE'`. Keep `EXCLUDED_CAPABILITIES` out of the table rows above that heading, because `capabilities_doc_lists_exactly_the_advertised_set` reads them.
- [ ] 3.3 `docs/capability-matrix.md`: state in the introduction that a ✓ is the default and `EXCLUDED_CAPABILITIES` withdraws it per schema.
- [ ] 3.4 `docs/semantic-differences.md`: add a section on `ORDER BY` with `LIMIT` under `EXCLUDED_CAPABILITIES`.
  - Link Exasol's [virtual schema limitations](https://docs.exasol.com/db/latest/database_concepts/virtual_schema/user_guide.htm#Virtualschemalimitations). Its ORDER BY clause limitations say that the outermost order of imported result rows is not guaranteed, with the workaround `SELECT * FROM (<virtual-schema-query> ORDER BY FALSE) ORDER BY <criteria>`.
  - State that excluding `LIMIT` or `LIMIT_WITH_OFFSET` while an `ORDER_BY_*` capability stays advertised makes `ORDER BY ... LIMIT n OFFSET m` return the first n rows: `ORDER BY id LIMIT 5 OFFSET 3` over the ids 1 to 20 returns 1 to 5, observed on Exasol 2026.2.0-nano.3.
  - Give the workaround: also exclude `ORDER_BY_COLUMN` and `ORDER_BY_EXPRESSION`.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Property catalog and capability exclusion | 1.1-1.7 | none | spec deltas `vs-properties/excluded-capabilities` (scenarios 1, 5), `vs-properties/property-name-validation`, `vs-properties/property-value-validation`, `vs-adapter/create-virtual-schema`; decision-log [2]; `adapter/capabilities.rs`, `adapter/capabilities_tests.rs`, `adapter/properties.rs`, `adapter/properties_tests.rs`, `adapter/mod.rs` (`dispatch`, `get_properties`), `adapter/adapter_tests.rs`, `e2e_local/property_validation.rs`, `e2e_local/fixtures.rs` |
| B: Exclusion behavior through Exasol | 2.1, 2.2a, 2.2b | A (needs the property and the moved stub helper) | spec delta `vs-properties/excluded-capabilities` (scenarios 1-4, 6); decision-log [1]; `adapter/pushdown/mod.rs`, `adapter/pushdown/request_shape.rs`, `adapter/pushdown/grouped_agg.rs`, `adapter/pushdown/joins/sql_builders.rs`, their `_tests.rs` files, `e2e_local/capability_exclusion.rs`, `e2e_local/main.rs`, `e2e_local/capability.rs` (capabilities echoed by `EXPLAIN VIRTUAL`), `e2e_local/join.rs` (`JOIN_ORACLE`, `copy_join_tables_natively`), `e2e_local/fixtures.rs`, `common/e2e_harness.rs` (`native_copy_mismatch`, `PathCheck`) |
| C: Documentation | 3.1-3.4 | B (documents the behavior that 2.1 verifies and the shapes that 2.2b settles) | spec deltas `vs-properties/excluded-capabilities`, `pushdown-capabilities/pushdown-planning-order-by-capability`; decision-log [1], [2]; `docs/tuning.md`, `docs/capabilities.md`, `docs/capability-matrix.md`, `docs/semantic-differences.md`, `adapter/capabilities_tests.rs` (`capabilities_doc_lists_exactly_the_advertised_set`) |

## Dead Code Removal

None.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| An excluded capability is withheld from getCapabilities | Unit, E2E | `adapter/capabilities_tests.rs`, `e2e_local/capability_exclusion.rs` | `excluded_capabilities_are_withheld_in_advertised_order`, `an_exclusion_withholds_only_the_listed_names`, `whitespace_and_empty_entries_are_accepted` |
| An excluded capability is evaluated by Exasol | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability_exclusion.rs` | `excluded_like_is_evaluated_by_exasol` |
| A changed exclusion applies from the next query | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability_exclusion.rs` | `set_exclusion_applies_from_the_next_query_without_refresh` |
| Request shapes that an exclusion makes reachable return the same rows | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability_exclusion.rs` | `exclusion_combinations_return_native_rows` |
| Create, refresh, and set reject an entry the adapter does not advertise | E2E, Unit | `e2e_local/property_validation.rs`, `e2e_local/capability_exclusion.rs`, `adapter/properties_tests.rs`, `adapter/adapter_tests.rs` | `create_and_set_reject_an_unadvertised_capability`, `get_capabilities_ignores_a_persisted_unadvertised_entry`, `whitespace_and_empty_entries_are_accepted`, `excluded_capability_entries_name_each_unadvertised_entry`, `excluded_capability_entries_accept_whitespace_and_empty_entries`, `an_unadvertised_excluded_capability_fails_every_schema_request_before_the_connection_is_read` |
| getCapabilities ignores an entry the adapter does not advertise | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability_exclusion.rs` | `get_capabilities_ignores_a_persisted_unadvertised_entry` |
| Every property the adapter reads is known under every catalog kind | E2E | `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` | `create_accepts_every_known_property_name` |
| One error reports every unknown name and every invalid value | E2E, Unit | `e2e_local/property_validation.rs`, `adapter/properties_tests.rs` | `create_and_set_reject_an_unadvertised_capability`, `excluded_capability_entries_name_each_unadvertised_entry` |
| Adapter reports its pushdown capabilities | Unit | `crates/lakehouse-engine/src/adapter/capabilities_tests.rs` | `reports_audited_capability_set` |
| ORDER_BY_COLUMN is advertised so ordered top-N queries can be pushed down | Unit | `crates/lakehouse-engine/src/adapter/capabilities_tests.rs` | `advertises_order_by_column_and_expression` |

`vs-properties/property-value-validation` also changes one Background bullet, which names the owner of the `EXCLUDED_CAPABILITIES` value check. `pushdown-capabilities/pushdown-planning-order-by-capability` corrects one Background fact and the reason clause of one scenario; the behavior and its tests stay unchanged. The corrected fact was verified on Exasol 2026.2.0-nano.3 with a probe adapter (decision-log [1]).

### Manual Testing

Run after `make e2e-up test-e2e`, which deploys the adapter into schema `LHVS` and the CONNECTION `LAKEHOUSE_CATALOG_CREDS`. Every command is `exapump sql -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0" "<SQL>"`.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| excluded-capabilities | `CREATE VIRTUAL SCHEMA CE_MANUAL USING LHVS.LAKEHOUSE_ADAPTER WITH CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS' ALLOW_HTTP = 'true' NAMESPACE = 'e2e_lakehouse' EXCLUDED_CAPABILITIES = 'FN_PRED_LIK'` | Fails with `invalid virtual schema properties: invalid value 'FN_PRED_LIK' for 'EXCLUDED_CAPABILITIES': expected an advertised capability name (did you mean 'FN_PRED_LIKE'?)` |
| excluded-capabilities, property-name-validation | The same statement, including `ALLOW_HTTP = 'true'`, with `EXCLUDED_CAPABILITIES = 'FN_PRED_LIKE'`, then `EXPLAIN VIRTUAL SELECT id FROM CE_MANUAL.EVENTS WHERE name LIKE 'event-1%'` | Succeeds; `PUSHDOWN_SQL` holds no `LIKE` |
| excluded-capabilities | `ALTER VIRTUAL SCHEMA CE_MANUAL SET EXCLUDED_CAPABILITIES = null`, then the same `EXPLAIN VIRTUAL` | `PUSHDOWN_SQL` holds the `LIKE` predicate, with no `REFRESH` run |
| pushdown-planning-order-by-capability | `ALTER VIRTUAL SCHEMA CE_MANUAL SET EXCLUDED_CAPABILITIES = 'LIMIT_WITH_OFFSET'`, then `EXPLAIN VIRTUAL` and the query `SELECT id FROM CE_MANUAL.EVENTS ORDER BY id LIMIT 5 OFFSET 3` | `PUSHDOWN_SQL` orders by `id` and carries no limit; the query returns the ids ranked 1 to 5, the Exasol behavior the corrected Background states |
| excluded-capabilities, property-value-validation | `ALTER VIRTUAL SCHEMA CE_MANUAL SET EXCLUDED_CAPABILITIES = 'MADE_UP'` | Fails with `invalid virtual schema properties: invalid value 'MADE_UP' for 'EXCLUDED_CAPABILITIES': expected an advertised capability name`; the schema keeps `EXCLUDED_CAPABILITIES = 'LIMIT_WITH_OFFSET'` |
| excluded-capabilities | `ALTER VIRTUAL SCHEMA CE_MANUAL REFRESH` | Succeeds, because `LIMIT_WITH_OFFSET` is an advertised name |
| cleanup | `DROP VIRTUAL SCHEMA CE_MANUAL CASCADE` | Succeeds |

A `REFRESH` over a saved unadvertised entry needs the stub adapter of task 1.6 to save it, so `get_capabilities_ignores_a_persisted_unadvertised_entry` is its proof.

### Checklist

How to run the suites: `specs/mission.md` § Commands and `specs/testing.md` § E2E suites.

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Unit tests | `cargo test --workspace --lib` | 0 failures |
| E2E (local Docker) | `make e2e-up test-e2e` | 0 failures |
| Lint | `make lint` | 0 errors or warnings |
| Format | `make fmt-check` | No changes |
