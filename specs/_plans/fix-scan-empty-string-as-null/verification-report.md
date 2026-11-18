# Verification Report: fix-scan-empty-string-as-null

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | The scan reads an empty string as NULL in every computed expression, `REPLACE` reads a NULL argument as native Exasol does, and every check is green. |
| Code review | 3 findings — 3 fixed |

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
| Unit (`cargo test --workspace`) | 2220 | 2218 | 2 |
| Integration (`make test-e2e`, local Docker suite) | 429 | 429 | 0 |

The first full E2E run passed 425 tests and failed the 4 `export_append` tests, because this clone's catalog lacked the Spark-written `e2e_export_append` namespace. After `make e2e-up` ran the Spark fixture job again, those 4 tests passed. No remote suite was run: the change targets no catalog-specific or cloud-storage feature.

### Manual Tests

| Test | Result |
|------|--------|
| `SELECT COUNT(*) FROM EMPTY_STRING_VS.EMPTY_STRINGS WHERE RTRIM(s, s) IS NULL` | ✓ 9, the table's row count |
| `SELECT REPLACE(s, 'U') FROM EMPTY_STRING_VS.EMPTY_STRINGS WHERE s = 'TRUCK'` | ✓ `TRCK` |
| `SELECT COUNT(*) FROM EMPTY_STRING_VS.EMPTY_STRINGS WHERE s IS NULL` | ✓ 3: two rows that store `''` and one that stores NULL |
| `SELECT B_LABEL FROM MY_LAKEHOUSE_JOIN.DIM_BLANK_LABEL ORDER BY B_LABEL NULLS LAST LIMIT 3` | ✓ `alpha`, `bravo`, `charlie` |
| `EXPLAIN VIRTUAL SELECT COUNT(*) FROM EMPTY_STRING_VS.EMPTY_STRINGS WHERE LENGTH(RTRIM(s, s)) = 0` | ✓ the scan filter is `(character_length(nullif(rtrim(nullif("S", ''), nullif("S", '')), '')) = 0)` |

## Tool Evidence

### Linter

```
make lint: exit 0
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 13.65s
```

### Formatter

```
make fmt-check: exit 0, no changes
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| pushdown-types | pushdown-planning-scan-empty-string | A text function that returns an empty string returns NULL | `crates/lakehouse-e2e/tests/e2e_local/empty_string.rs` | `text_function_empty_result_is_null` | Pass |
| pushdown-types | pushdown-planning-scan-empty-string | A stored empty string reads as NULL in every computed expression | `crates/lakehouse-e2e/tests/e2e_local/empty_string.rs` | `stored_empty_string_reads_as_null` | Pass |
| pushdown-types | pushdown-planning-scan-empty-string | A filter over a bare string column keeps the bare column | `crates/lakehouse-e2e/tests/e2e_local/empty_string.rs` | `bare_string_filter_keeps_the_bare_column` | Pass |
| pushdown-types | pushdown-planning-scan-empty-string | A guarded string filter still skips row groups | `crates/lakehouse-engine/tests/it/scan_parquet_pruning.rs` | `guarded_string_filter_skips_row_groups` | Pass |
| pushdown-types | pushdown-planning-scan-empty-string | An equality of two string columns never matches empty strings | `crates/lakehouse-e2e/tests/e2e_local/empty_string.rs` | `string_key_equality_never_matches_empty_strings` | Pass |
| pushdown-types | pushdown-planning-scan-empty-string | REPLACE reads a NULL argument as the empty string | `crates/lakehouse-e2e/tests/e2e_local/empty_string.rs` | `replace_reads_null_argument_as_empty` | Pass |
| pushdown-types | pushdown-planning-scan-empty-string | A text function with a NULL argument returns what native Exasol returns | `crates/lakehouse-e2e/tests/e2e_local/empty_string.rs` | `text_function_null_argument_matches_native` | Pass |
| file-planning | pruning-predicate-translation | A null test keeps the files that hold empty values | `crates/lakehouse-e2e/tests/e2e_local/empty_string.rs` | `null_test_keeps_file_of_empty_strings` | Pass |
| pushdown-capabilities | pushdown-planning-topn | A per-shard top-N ranks a stored empty string as NULL | `crates/lakehouse-e2e/tests/e2e_local/empty_string.rs` | `per_shard_top_n_ranks_empty_string_as_null` | Pass |
| sql-comprehension | vs-expression-translator-predicates | IS NULL and IS NOT NULL predicates translate correctly | `crates/lakehouse-e2e/tests/e2e_local/empty_string.rs` | `null_test_over_string_column_keeps_the_bare_column` | Pass |
| sql-functions | vs-expression-translator-scalar-fns | String scalar functions translate to DataFusion string calls | `crates/vs-expression/src/lib_tests.rs` | `renders_string_scalar_functions` | Pass |

## Notes

- Live capture: native Exasol returns NULL for a NULL in any argument of `TRIM`, `LTRIM`, `RTRIM`, `TRANSLATE`, `LPAD`, `RPAD`, `INSTR`, `LOCATE`, `SUBSTR`, `LEFT`, `RIGHT`, `REPEAT`, `REVERSE`, `UPPER`, `LOWER`, `INITCAP`, and `LENGTH`, as DataFusion does. Only `REPLACE` differs, so no function declines and `docs/capabilities.md` lists no form that runs in Exasol.
- An aggregate over a bare string column (`COUNT(s)`, `MIN(s)`, `MAX(s)`) reached the scan as a column name, outside the translator. It now renders as `nullif("S", '')`. The plan did not list this site, and the scenario "A stored empty string reads as NULL in every computed expression" requires it.
- The top-N test uses the new empty-string fixture, not the blank-label fixture the plan names, because that table has one data file and the scenario needs files that span several shards.
- `TO_DATE` and `TO_TIMESTAMP` lost their own `nullif(<source>, '')`, because the general rule now supplies it. Their typed scan SQL is unchanged.
- The deployed `.so` of the E2E run predates the three review fixes, which changed only tests and comments.
- Out of scope: `JSON_VALUE` can return an empty string that the scan does not read as NULL. A select-list comparison of a binary column with a hex literal is rewritten to a byte comparison as a filter is, which returns FALSE where Exasol returns NULL for an empty value.
