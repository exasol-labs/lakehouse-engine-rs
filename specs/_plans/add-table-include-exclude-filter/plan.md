# Plan: add-table-include-exclude-filter

## Summary

Add the virtual schema properties `TABLE_INCLUDE` and `TABLE_EXCLUDE`, which choose the exposed tables by glob patterns over the Exasol virtual table name, before any per-table catalog request or storage read (#544). `ALTER VIRTUAL SCHEMA ... SET` re-lists the tables, and `REFRESH ... TABLES` cannot add a hidden table back.

## Context

- Issue #544: a virtual schema exposes every table of its namespace. Users need to hide a few tables or expose only a few, and listing every table by name does not scale.
- Every listing runs through `list_source` in `crates/lakehouse-engine/src/adapter/mod.rs`. A full listing (`createVirtualSchema`, `setProperties`, `refresh` without `requestedTables`) calls `CatalogClient::list_tables`. A partial refresh calls `list_table_idents`, resolves the requested names in `ScopedRefresh`, and calls `load_listed_tables`.
- Iceberg REST (`crates/lakehouse-catalog/src/client.rs`) and direct storage (`crates/lakehouse-engine/src/adapter/direct_storage.rs`) implement `list_tables` as `list_table_idents` followed by a load of each table. Unity Catalog (`crates/lakehouse-catalog/src/unity/client.rs`) and Glue (`crates/lakehouse-catalog/src/glue/client.rs`) read every table from one listing sweep and then admit each record.
- `flatten_table_name` (`crates/lakehouse-engine/src/adapter/tables.rs`) builds the virtual table name that the patterns match.
- `PROPERTY_NAMES` and `TuningChecker` in `crates/lakehouse-engine/src/adapter/properties.rs` hold the known names and collect every rejected value into one error.
- The plan changes no scan, pushdown, schema, or type handling, so no section of the Iceberg table spec or the Delta Lake protocol governs it.
- The interview answers are in `decision-log.md` § Interview. The component changes are in `architecture.md` in this directory.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| table-filter | NEW | `vs-properties/table-filter/spec.md` |
| property-name-validation | CHANGED | `vs-properties/property-name-validation/spec.md` |

## Impact

- Two new optional properties. A virtual schema that sets neither one exposes every table, as before.
- `CREATE VIRTUAL SCHEMA`, `ALTER VIRTUAL SCHEMA ... SET`, and `ALTER VIRTUAL SCHEMA ... REFRESH` reject a `TABLE_INCLUDE` or `TABLE_EXCLUDE` value with an empty entry, in the same error that reports unknown names and invalid tuning values.
- `CatalogClient::list_tables` gains a predicate parameter. The trait is internal to the workspace, so no user or operator sees the change.
- No breaking change.

## Implementation Tasks

1. Listing predicate in `CatalogClient`
   - [ ] 1.1 In `crates/lakehouse-catalog/src/client.rs`, change `CatalogClient::list_tables` to `list_tables<'a>(&'a self, namespace: &[String], exposes: &'a (dyn Fn(&CatalogTableIdent) -> bool + Sync))`, with the boxed future bound to `'a`, and make it a provided method: its body calls `list_table_idents`, keeps the identifiers `exposes` accepts, and calls `load_listed_tables`. The trait doc states that an override applies `exposes` to each listed identifier before it admits, skips, or loads a table. Delete the `IcebergRestCatalogClient` and `DirectStorageCatalogClient` overrides, which the provided body replaces. In the Unity Catalog and Glue overrides, drop each listed record whose identifier `exposes` rejects before `admit`. Pass `&|_| true` at every existing test call site of `list_tables` and adapt `FixedCatalogClient` in `crates/lakehouse-catalog/src/client_tests.rs`: `crates/lakehouse-catalog/src/{client_tests,unity/client_tests,unity/auth_tests,glue/client_tests}.rs`, `crates/lakehouse-catalog/tests/it/catalog_public_surface.rs`, and `crates/lakehouse-engine/src/adapter/{direct_storage_tests,parquet_fixture_tests}.rs`. Add no test; the E2E tests of task 4 prove the predicate.
2. Table filter module
   - [ ] 2.1 Create `crates/lakehouse-engine/src/adapter/table_filter.rs`, declare `mod table_filter;` in `adapter/mod.rs`, and declare `#[cfg(test)] #[path = "table_filter_tests.rs"] mod tests;` inside `table_filter.rs`. The module holds `TableFilter` (an optional include list and an exclude list), a parse function, and `TableFilter::exposes(&self, virtual_table_name: &str) -> bool` (include absent or any include match, and no exclude match). The parse function splits a value on `,`, trims the whitespace around each entry, rejects an empty entry with the expectation `a comma-separated list of non-empty table name patterns`, and uppercases each pattern with `str::to_uppercase`. The matcher compares `char`s over the whole name: `*` matches any run, the empty one included, `?` matches one `char`, and every other `char` matches only itself. It is an iterative loop that keeps one backtrack point, the last `*`, so it runs in O(pattern length × name length) time without recursion.
   - [ ] 2.2 Unit tests in `crates/lakehouse-engine/src/adapter/table_filter_tests.rs`. Tag each of these with `/// Scenario: A pattern matches the whole virtual table name without regard to case`: `star_and_question_mark_follow_the_glob_rules` (`*` matches the empty run and a long run, `?` matches one `char` and neither zero nor two, `A*B*C` matches `AXBYBZC`), `other_characters_match_only_themselves` (`[A]\B*` parses, matches `[A]\BX`, and does not match `AB`), `a_pattern_matches_the_whole_name` (`ORDERS` does not match `ORDERS_2024` or `STAGING__ORDERS`), and `entries_are_trimmed_and_uppercased` (` orders_?0* ` matches `ORDERS_2024`). Add two untagged tests: `blank_entries_are_rejected` (`'A,'` and `' , '`) and `many_stars_do_not_backtrack_exponentially` (`*A*A*A*A*A*A*A*A*A*A*B` against a name of 128 `A`s returns `false`, so an exponential matcher hangs the test). Add no test of the precedence or of `'A,,B'`, which the E2E tests of task 4 prove.
3. Property names, values, and listing wiring
   - [ ] 3.1 In `crates/lakehouse-engine/src/adapter/properties.rs`, add `TABLE_INCLUDE` and `TABLE_EXCLUDE` to `PROPERTY_NAMES` (19 names). Apply the table-filter parse function to both through `TuningChecker::value`, so a rejection joins the one error and echoes the value per the existing echo rule. Change `check_properties` to return `CheckedProperties { tuning: TuningProperties, table_filter: TableFilter }`, and adapt its callers: `handle_create_virtual_schema` in `adapter/mod.rs`, `checked` in `adapter/adapter_tests.rs`, and the existing tests in `properties_tests.rs`. Add no test.
   - [ ] 3.2 In `list_source` (`crates/lakehouse-engine/src/adapter/mod.rs`), take the `TableFilter` from `handle_create_virtual_schema` and build the predicate `|ident| filter.exposes(&flatten_table_name(configured_ns, ident))`. The full listing passes it to `list_tables`. The partial refresh drops the identifiers it rejects from the `list_table_idents` output before `ScopedRefresh::new`, so a requested hidden name resolves as an unlisted name and a persisted `SKIPPED_TABLES` entry of a hidden table drops out of the merge. Add no test.
4. E2E tests
   - [ ] 4.1 Create `crates/lakehouse-e2e/tests/e2e_local/table_filter.rs` and declare it in `crates/lakehouse-e2e/tests/e2e_local/main.rs`. With the seeding helpers of `refresh.rs`, seed the Iceberg namespace `e2e_table_filter` with `orders`, `orders_2024`, `orders_tmp`, and `audit_log`, and its child namespace `staging` with `orders`. Write `include_and_exclude_choose_the_exposed_tables`: `CREATE VIRTUAL SCHEMA` with `TABLE_INCLUDE = 'ORDERS*'` and `TABLE_EXCLUDE = 'ORDERS_TMP'` declares `ORDERS` and `ORDERS_2024`; with `TABLE_INCLUDE = 'AUDIT_LOG'` declares `AUDIT_LOG`; with `TABLE_EXCLUDE = 'STAGING__*, ORDERS_*'` declares `AUDIT_LOG` and `ORDERS`. Write `set_properties_relists_the_exposed_tables`: a schema created without a filter declares five tables; `SET TABLE_EXCLUDE = 'ORDERS_TMP'` drops `ORDERS_TMP` and keeps `ORDERS`; a full `REFRESH` keeps `ORDERS_TMP` dropped; `SET TABLE_EXCLUDE = null` declares it again.
   - [ ] 4.2 In `crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs`, write `hidden_directories_are_never_read_or_recorded_as_skipped`: a base path with `good` (one Parquet file), `conflict` (two files that store one column as `INT64` and `STRING`, built as in `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema`), and `empty` (only `_SUCCESS`). `CREATE` without a filter fails naming the conflicting column, which shows that a read of `conflict` fails the statement. `CREATE` with `TABLE_EXCLUDE = 'CONFLICT, EMPTY'` succeeds, declares `GOOD` only, and leaves `SKIPPED_TABLES` an empty array. `REFRESH TABLES GOOD CONFLICT` then succeeds and still declares `GOOD` only.
   - [ ] 4.3 In `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs`, add `("TABLE_INCLUDE", "*")` and `("TABLE_EXCLUDE", "NO_SUCH_TABLE")` to `create_accepts_every_known_property_name`. Write `create_rejects_an_empty_table_pattern_entry`: `CREATE` with `TABLE_INCLUDE = 'ORDERS,,AUDIT_LOG'` fails with `invalid value 'ORDERS,,AUDIT_LOG' for 'TABLE_INCLUDE': expected a comma-separated list of non-empty table name patterns`.
5. Documentation
   - [ ] 5.1 In `docs/catalogs.md`, add a section "Choosing which tables to expose": both properties, the pattern syntax, matching on the virtual table name with the `STAGING__*` example, the precedence with the issue example, and the behavior at `SET`, at `REFRESH ... TABLES`, and in `SKIPPED_TABLES`. In the property table of `docs/tuning.md` § Parameters, which lists every property, add one row per new property that links to the section, and name the filter in the `NAMESPACE` row.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Listing predicate | 1.1 | none | `decision-log.md` [1]; `architecture.md` § Components (lakehouse-catalog); `crates/lakehouse-catalog/src/client.rs`, `crates/lakehouse-catalog/src/unity/client.rs`, `crates/lakehouse-catalog/src/glue/client.rs`, `crates/lakehouse-engine/src/adapter/direct_storage.rs`, and the test files of task 1.1 |
| B: Table filter end to end | 2.1-5.1 | A (`list_source` calls the new `list_tables` signature) | spec deltas `vs-properties/table-filter`, `vs-properties/property-name-validation`; `decision-log.md` [2]; `crates/lakehouse-engine/src/adapter/table_filter.rs`, `properties.rs`, `mod.rs` (`list_source`, `handle_create_virtual_schema`), and their `_tests.rs` files; `crates/lakehouse-e2e/tests/e2e_local/{table_filter,direct_storage,property_validation}.rs`; `docs/catalogs.md`, `docs/tuning.md` |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Trait override | `IcebergRestCatalogClient::list_tables` in `crates/lakehouse-catalog/src/client.rs` | The provided `list_tables` body does the same identifier listing and load (task 1.1) |
| Trait override | `DirectStorageCatalogClient::list_tables` in `crates/lakehouse-engine/src/adapter/direct_storage.rs` | Same reason |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Include and exclude patterns choose the exposed tables, and exclude wins | E2E | `crates/lakehouse-e2e/tests/e2e_local/table_filter.rs` | `include_and_exclude_choose_the_exposed_tables` |
| A pattern matches the whole virtual table name without regard to case | Unit | `crates/lakehouse-engine/src/adapter/table_filter_tests.rs` | `star_and_question_mark_follow_the_glob_rules`, `other_characters_match_only_themselves`, `a_pattern_matches_the_whole_name`, `entries_are_trimmed_and_uppercased` |
| A hidden table is never loaded and never recorded as skipped | E2E | `crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs` | `hidden_directories_are_never_read_or_recorded_as_skipped` |
| Setting either property re-lists the exposed tables | E2E | `crates/lakehouse-e2e/tests/e2e_local/table_filter.rs` | `set_properties_relists_the_exposed_tables` |
| A partial refresh treats a hidden table as one the source does not serve | E2E | `crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs` | `hidden_directories_are_never_read_or_recorded_as_skipped` |
| An empty pattern entry fails the statement | E2E | `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` | `create_rejects_an_empty_table_pattern_entry` |
| Every property the adapter reads is known under every catalog kind | E2E | `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` | `create_accepts_every_known_property_name` |

"A pattern matches the whole virtual table name without regard to case" is pure computation, so its unit tests are its one proof and carry its `/// Scenario:` line. Every other scenario has an E2E test, and no unit test repeats it (`specs/testing.md` § Coverage rule, § Testing strategy). One direct-storage test proves two scenarios, because both need the same table whose load fails the statement.

### Manual Testing

Run against the local Docker Exasol after `make cross-udf-build` and the deployment that `make test-e2e` performs, with the adapter installed in `LHVS` and a CONNECTION `LAKEHOUSE_CATALOG_CREDS` over the namespace `e2e_table_filter` that task 4.1 seeds.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| table-filter | `exapump sql "CREATE VIRTUAL SCHEMA TF_MANUAL USING LHVS.LAKEHOUSE_ADAPTER WITH CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS' NAMESPACE = 'e2e_table_filter' ALLOW_HTTP = 'true' TABLE_INCLUDE = 'ORDERS*' TABLE_EXCLUDE = 'ORDERS_TMP'; SELECT TABLE_NAME FROM SYS.EXA_ALL_VIRTUAL_TABLES WHERE TABLE_SCHEMA = 'TF_MANUAL' ORDER BY 1"` | Two rows: `ORDERS`, `ORDERS_2024` |
| table-filter | `exapump sql "ALTER VIRTUAL SCHEMA TF_MANUAL SET TABLE_INCLUDE = null; ALTER VIRTUAL SCHEMA TF_MANUAL REFRESH TABLES ORDERS_TMP; SELECT TABLE_NAME FROM SYS.EXA_ALL_VIRTUAL_TABLES WHERE TABLE_SCHEMA = 'TF_MANUAL' ORDER BY 1"` | Both statements succeed. Rows: `AUDIT_LOG`, `ORDERS`, `ORDERS_2024`, `STAGING__ORDERS`; no `ORDERS_TMP` |
| property-name-validation | `exapump sql "ALTER VIRTUAL SCHEMA TF_MANUAL SET TABLE_INCLUD = 'X'"` | Fails with `unknown property 'TABLE_INCLUD' (did you mean 'TABLE_INCLUDE'?)` |
| table-filter | `exapump sql "ALTER VIRTUAL SCHEMA TF_MANUAL SET TABLE_EXCLUDE = 'ORDERS,,AUDIT_LOG'"` | Fails with `invalid virtual schema properties: invalid value 'ORDERS,,AUDIT_LOG' for 'TABLE_EXCLUDE': expected a comma-separated list of non-empty table name patterns`; the table list is unchanged |
| table-filter | `exapump sql "DROP VIRTUAL SCHEMA TF_MANUAL CASCADE"` | Succeeds |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Unit tests | `cargo test --workspace` | 0 failures |
| E2E (local Docker) | `make test-e2e` | 0 failures |
| E2E regression (Unity Catalog, Glue) | `make test-e2e-unity`, `make test-e2e-glue` | 0 failures in the existing listing tests of the two changed `list_tables` overrides; both need credentials (`specs/testing.md` § E2E suites), otherwise CI runs them |
| Lint and format | `make fmt-check lint` | 0 errors and warnings, no format changes |
