# Verification Report: add-table-include-exclude-filter

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | `TABLE_INCLUDE` and `TABLE_EXCLUDE` choose the exposed tables before any per-table catalog request or storage read. All suites that ran are green. |
| Code review | 8 findings (7 standard, 1 expert) — 8 fixed |

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
| Unit and integration (`cargo test --workspace`) | 2033 | 2031 | 2 |
| E2E local Docker (`make test-e2e`) | 326 | 326 | 0 |
| E2E Unity Catalog (`make test-e2e-unity`) | 43 | 43 | 0 |
| E2E Glue (`make test-e2e-glue`, real AWS) | 20 | 20 | 0 |

The plan changes the Unity Catalog and Glue `list_tables` overrides, so the hook ran both remote suites. Lakekeeper and Azure suites were not run, because the plan changes nothing that only they exercise. CI runs them on the PR.

The first `make test-e2e` run failed 17 of 326 tests. The Docker stack had been torn down, so the Spark fixture tables did not exist (15 tests) and two further tests failed against the rebuilt stack. After restarting the stack and running `spark-iceberg-fixtures`, the same suite on the same code passed 326 of 326.

### Manual Tests

| Test | Result |
|------|--------|
| `CREATE VIRTUAL SCHEMA` with `TABLE_INCLUDE = 'ORDERS*'` and `TABLE_EXCLUDE = 'ORDERS_TMP'` declares `ORDERS`, `ORDERS_2024` | ✓ |
| `SET TABLE_INCLUDE = null`, then `REFRESH TABLES ORDERS_TMP`: both succeed, rows `AUDIT_LOG`, `ORDERS`, `ORDERS_2024`, `STAGING__ORDERS`, no `ORDERS_TMP` | ✓ |
| `SET TABLE_INCLUD = 'X'` fails with `unknown property 'TABLE_INCLUD' (did you mean 'TABLE_INCLUDE'?)` | ✓ |
| `SET TABLE_EXCLUDE = 'ORDERS,,AUDIT_LOG'` fails with `invalid value 'ORDERS,,AUDIT_LOG' for 'TABLE_EXCLUDE': expected a comma-separated list of non-empty table name patterns`; table list unchanged | ✓ |
| `DROP VIRTUAL SCHEMA TF_MANUAL CASCADE` succeeds | ✓ |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets --all-features -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s)
exit 0
```

### Formatter

```
cargo fmt --all -- --check
exit 0
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-properties | table-filter | Include and exclude patterns choose the exposed tables, and exclude wins | `crates/lakehouse-e2e/tests/e2e_local/table_filter.rs` | `include_and_exclude_choose_the_exposed_tables` | Pass |
| vs-properties | table-filter | A pattern matches the whole virtual table name without regard to case | `crates/lakehouse-engine/src/adapter/table_filter_tests.rs` | `star_and_question_mark_follow_the_glob_rules`, `other_characters_match_only_themselves`, `a_pattern_matches_the_whole_name`, `entries_are_trimmed_and_uppercased` | Pass |
| vs-properties | table-filter | A hidden table is never loaded and never recorded as skipped | `crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs` | `hidden_directories_are_never_read_or_recorded_as_skipped` | Pass |
| vs-properties | table-filter | Setting either property re-lists the exposed tables | `crates/lakehouse-e2e/tests/e2e_local/table_filter.rs` | `set_properties_relists_the_exposed_tables` | Pass |
| vs-properties | table-filter | A partial refresh treats a hidden table as one the source does not serve | `crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs` | `hidden_directories_are_never_read_or_recorded_as_skipped` | Pass |
| vs-properties | table-filter | An empty pattern entry fails the statement | `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` | `create_rejects_an_empty_table_pattern_entry` | Pass |
| vs-properties | property-name-validation | Every property the adapter reads is known under every catalog kind | `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` | `create_accepts_every_known_property_name` | Pass |

## Notes

- The review fix pass added two E2E checks beyond the plan: `unity_table_include_narrows_the_list_sweep` (Unity Catalog) and `check_a_hidden_broken_table_does_not_fail_the_listing` (Glue). Each fails when the matching `list_tables` override stops applying `exposes`.
- The Unity E2E test leaves its `UNITY_TABLE_FILTER_CREDS` connection behind, as the test it follows does.
- Coverage percentages were not measured in this run.
- The version files are unchanged, per the project hook.
