# Verification Report: add-capabilities-exclusion-property

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | `EXCLUDED_CAPABILITIES` withholds the listed capabilities from `getCapabilities`, and `CREATE`, `REFRESH`, and `SET` reject an unadvertised entry. Every scenario test, the unit suite, and the local E2E suite pass. |
| Code review | 11 findings — 11 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | not measured |
| Integration | not measured |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace --lib`) | 2006 | 2006 | 0 |
| E2E, local Docker (`make e2e-up test-e2e`) | 368 | 368 | 0 |

The local stack runs Exasol 2025.1.16. The project hook limits this run to the local E2E suite. No remote suite applies, because the plan changes no catalog-specific or storage-specific behavior.

### Manual Tests

| Test | Result |
|------|--------|
| `CREATE ... EXCLUDED_CAPABILITIES = 'FN_PRED_LIK'` fails with `invalid value 'FN_PRED_LIK' for 'EXCLUDED_CAPABILITIES': expected an advertised capability name (did you mean 'FN_PRED_LIKE'?)` | ✓ |
| `CREATE ... EXCLUDED_CAPABILITIES = 'FN_PRED_LIKE'` succeeds, and `EXPLAIN VIRTUAL ... WHERE name LIKE 'event-1%'` pushes no filter | ✓ |
| `SET EXCLUDED_CAPABILITIES = null` without `REFRESH`: the next `EXPLAIN VIRTUAL` pushes `"NAME" LIKE 'event-1%'` | ✓ |
| `SET EXCLUDED_CAPABILITIES = 'LIMIT_WITH_OFFSET'`: `EXPLAIN VIRTUAL ... ORDER BY id LIMIT 5 OFFSET 3` orders by `ID` with no limit, and the query returns ids 1 to 5 | ✓ |
| `SET EXCLUDED_CAPABILITIES = 'MADE_UP'` fails with `invalid value 'MADE_UP' for 'EXCLUDED_CAPABILITIES': expected an advertised capability name`, and the schema keeps `LIMIT_WITH_OFFSET` | ✓ |
| `ALTER VIRTUAL SCHEMA CE_MANUAL REFRESH` succeeds | ✓ |
| `DROP VIRTUAL SCHEMA CE_MANUAL CASCADE` succeeds | ✓ |

## Tool Evidence

### Linter

```
make lint: exit 0, no warnings
```

### Formatter

```
make fmt-check: exit 0, no changes
```

### Build

```
make cross-udf-build: exit 0, 4 UDFs validated
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-properties | excluded-capabilities | An excluded capability is withheld from getCapabilities | `adapter/capabilities_tests.rs`, `e2e_local/capability_exclusion.rs` | `excluded_capabilities_are_withheld_in_advertised_order`, `an_exclusion_withholds_only_the_listed_names`, `whitespace_and_empty_entries_are_accepted` | Pass |
| vs-properties | excluded-capabilities | An excluded capability is evaluated by Exasol | `e2e_local/capability_exclusion.rs` | `excluded_like_is_evaluated_by_exasol` | Pass |
| vs-properties | excluded-capabilities | A changed exclusion applies from the next query | `e2e_local/capability_exclusion.rs` | `set_exclusion_applies_from_the_next_query_without_refresh` | Pass |
| vs-properties | excluded-capabilities | Request shapes that an exclusion makes reachable return the same rows | `e2e_local/capability_exclusion.rs` | `exclusion_combinations_return_native_rows` | Pass |
| vs-properties | excluded-capabilities | Create, refresh, and set reject an entry the adapter does not advertise | `e2e_local/property_validation.rs`, `e2e_local/capability_exclusion.rs`, `adapter/properties_tests.rs`, `adapter/adapter_tests.rs` | `create_and_set_reject_an_unadvertised_capability`, `get_capabilities_ignores_a_persisted_unadvertised_entry`, `whitespace_and_empty_entries_are_accepted`, `excluded_capability_entries_name_each_unadvertised_entry`, `excluded_capability_entries_accept_whitespace_and_empty_entries`, `an_unadvertised_excluded_capability_fails_every_schema_request_before_the_connection_is_read` | Pass |
| vs-properties | excluded-capabilities | getCapabilities ignores an entry the adapter does not advertise | `e2e_local/capability_exclusion.rs` | `get_capabilities_ignores_a_persisted_unadvertised_entry` | Pass |
| vs-properties | property-name-validation | Every property the adapter reads is known under every catalog kind | `e2e_local/property_validation.rs` | `create_accepts_every_known_property_name` | Pass |
| vs-properties | property-value-validation | One error reports every unknown name and every invalid value | `e2e_local/property_validation.rs`, `adapter/properties_tests.rs` | `create_and_set_reject_an_unadvertised_capability`, `excluded_capability_entries_name_each_unadvertised_entry` | Pass |
| vs-adapter | create-virtual-schema | Adapter reports its pushdown capabilities | `adapter/capabilities_tests.rs` | `reports_audited_capability_set` | Pass |
| pushdown-capabilities | pushdown-planning-order-by-capability | ORDER_BY_COLUMN is advertised so ordered top-N queries can be pushed down | `adapter/capabilities_tests.rs` | `advertises_order_by_column_and_expression` | Pass |

## Notes

- The planner needed no change for task 2.2b. On live Exasol, every exclusion case returned the native rows. Exasol never sent a grouped request without `selectList`. A join request without `selectList` declares its columns in leg order, which the planner already renders. `notes/B-shapes.md` holds the captured request JSON.
- One case remains open and was not checked on live Exasol. With `SELECTLIST_PROJECTION` excluded, a request without `selectList` counts every column as referenced. A query over a table with a refused column (a type the adapter cannot map) would then fail with an error, even when the query does not name that column. The result is an error, not wrong rows. No table in the E2E namespace has a refused column.
- The `ORDER BY ... LIMIT n OFFSET m` effect under a `LIMIT_WITH_OFFSET` exclusion reproduced on Exasol 2025.1.16, the same result as on Exasol 2026.2.0-nano.3.
