# Plan: fix-scan-empty-string-as-null

## Summary

The scan reads an empty string as NULL wherever it computes with it, from a stored column value or from a text function result, and text functions read a NULL argument as native Exasol does. String filters over a bare column keep their row-group pruning.

## Context

- In Exasol an empty string is NULL. DataFusion keeps `''` as a value, so a pushed-down expression that consumes one returns wrong rows or values without an error (#203).
- An empty string has two sources: a string column of a data file, and a text function such as `RTRIM(c, c)`.
- A value the scan emits already arrives in Exasol as NULL, so only expressions computed inside the scan are wrong: filters, function arguments, aggregates, join keys, and per-shard sort keys.
- DataFusion's statistics pruning reads only a bare column, a cast, or a negation on the column side of a comparison. A column wrapped in `nullif` is never pruned.
- Plan-time file pruning reads the request's filter JSON, not the scan SQL. It prunes a file with a null count of 0 for `col IS NULL`, although the file can hold `''` or an empty binary value, which both read as NULL.
- Exasol sends `REPLACE(s, 'U')` and `REPLACE(s, 'U', '')` with a NULL third argument. DataFusion's `replace` returns NULL for it.
- The broadcast join top-N already ranks an emitted `''` as NULL (#309). The single-table top-N does not.
- Table format specs, which keep `''` and null distinct. Reading `''` as NULL is the named trade-off of Exasol's VARCHAR domain, not a gap:
  - Iceberg spec, Primitive Types: `string`, "Arbitrary-length character sequences", "Encoded with UTF-8".
  - Iceberg spec, Manifests, `data_file`: `110 null_value_counts`, "Map from column id to number of null values in the column". `125 lower_bounds`, "Each value must be less than or equal to all non-null, non-NaN values in the column for the file". A file that holds `''` therefore has the lower bound `''`, which the changed null test reads.
  - Delta protocol, Per-file Statistics: `nullCount`, "The number of `null` values for this column". `minValues` with `tightBounds=false`, "A value that is less than or equal to all valid values present in this file for this column".
  - Delta protocol, Partition Value Serialization: "An empty string for any type translates to a `null` partition value", which the engine already follows.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| pushdown-planning-scan-empty-string | NEW | `pushdown-types/pushdown-planning-scan-empty-string/spec.md` |
| pruning-predicate-translation | CHANGED | `file-planning/pruning-predicate-translation/spec.md` |
| pushdown-planning-topn | CHANGED | `pushdown-capabilities/pushdown-planning-topn/spec.md` |
| vs-expression-translator-scalar-fns | CHANGED | `sql-functions/vs-expression-translator-scalar-fns/spec.md` |
| vs-expression-translator-predicates | CHANGED | `sql-comprehension/vs-expression-translator-predicates/spec.md` |

## Impact

- Queries over data that stores `''`, or over text functions that return `''`, now return native Exasol's rows and values. Results change for such queries: `IS NULL`, `LENGTH`, `NVL`, `COUNT(col)`, `MIN`, `<>`, `<`, `<=`, `NOT (...)`, join keys, and top-N ranking.
- `REPLACE(s, 'U')` and `REPLACE(s, 'U', '')` now remove the search text. They returned NULL.
- A text function whose NULL-argument result the scan cannot reproduce with a NULL guard runs in Exasol whenever that argument is not a non-NULL literal, so a filter over such a call prunes no files.
- Comparisons, `IN`, `BETWEEN`, `LIKE`, `IS NULL`, and `IS NOT NULL` over a bare string column prune as before, in the scan and at plan time.
- A comparison, `IN`, `BETWEEN`, or `LIKE` over a string column under a `NOT`, and a comparison of two string columns other than `=`, no longer skip row groups in the scan.
- No property, CONNECTION field, capability, or install step changes. No breaking change.

## Requirements

| Requirement | Details |
|-------------|---------|
| Live capture first | Before any rendering change, capture on the Docker Exasol what native Exasol returns for each text function with a NULL in each argument position, and what Exasol sends for `''` and omitted arguments. The capture decides fix or decline per function. |
| One rule, one place | The typed pass owns the rule. It marks a string column and a text function result with one synthetic node that the DataFusion dialect renders as `nullif(x, '')`. The Exasol dialect is unchanged. |
| Null test form | A null test over a bare character column renders `IS NULL OR = ''`, or `IS NOT NULL AND <> ''`, at any position. |
| Bare-column filter form | The typed pass keeps the bare column for the comparison forms the spec lists, only where the node is reached from a filter root or a join condition root through `AND` and `OR`. `binary_filter.rs` does the same for binary columns and is the model. |
| Character columns | The rule reads the per-column type map and covers every column it declares as a character type. |
| Plan-time null test | In the one parsed pruning filter, a null test on a string column becomes `IS NULL OR = ''`, and the negated one `IS NOT NULL AND <> ''`. A null test on a variable-length binary column, hex or `BINARY_AS_STRING`, takes the same form over the empty value or constrains nothing. |
| Per-shard sort | The single-table per-shard sort ranks a string key as the join top-N does. Share that rendering between both paths. |
| Docs | `docs/capabilities.md` names each text function form that runs in Exasol after the capture. |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Code | `crates/vs-expression/src/lib.rs`, `CONCAT` arm | Its hand-written `nullif(concat(...), '')` is the general text-result rule. Keep one copy. |
| Function | `crates/lakehouse-engine/src/scan/join_scan.rs`, `render_join_sort_target` | Its string and NaN ranking moves to the rendering both top-N paths share. |
| Test | Golden SQL and unit expectations that pin an unwrapped text function or column | Updated to the new scan SQL. An intended change of generated SQL (`specs/testing.md` § Regression guards). |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A text function that returns an empty string returns NULL | E2E | `crates/lakehouse-e2e/tests/e2e_local/empty_string.rs` | `text_function_empty_result_is_null` |
| A stored empty string reads as NULL in every computed expression | E2E | `crates/lakehouse-e2e/tests/e2e_local/empty_string.rs` | `stored_empty_string_reads_as_null` |
| A filter over a bare string column keeps the bare column | E2E | `crates/lakehouse-e2e/tests/e2e_local/empty_string.rs` | `bare_string_filter_keeps_the_bare_column` |
| A guarded string filter still skips row groups | Host scan test | `crates/lakehouse-engine/tests/it/scan_parquet_pruning.rs` | `guarded_string_filter_skips_row_groups` |
| An equality of two string columns never matches empty strings | E2E | `crates/lakehouse-e2e/tests/e2e_local/empty_string.rs` | `string_key_equality_never_matches_empty_strings` |
| REPLACE reads a NULL argument as the empty string | E2E | `crates/lakehouse-e2e/tests/e2e_local/empty_string.rs` | `replace_reads_null_argument_as_empty` |
| A text function with a NULL argument returns what native Exasol returns | E2E | `crates/lakehouse-e2e/tests/e2e_local/empty_string.rs` | `text_function_null_argument_matches_native` |
| A null test keeps the files that hold empty values | E2E | `crates/lakehouse-e2e/tests/e2e_local/empty_string.rs` | `null_test_keeps_file_of_empty_strings` |
| A per-shard top-N ranks a stored empty string as NULL | E2E | `crates/lakehouse-e2e/tests/e2e_local/empty_string.rs` | `per_shard_top_n_ranks_empty_string_as_null` |
| IS NULL and IS NOT NULL predicates translate correctly | E2E | `crates/lakehouse-e2e/tests/e2e_local/empty_string.rs` | `null_test_over_string_column_keeps_the_bare_column` |
| String scalar functions translate to DataFusion string calls | Unit | `crates/vs-expression/src/lib_tests.rs` | the existing string-function case table, with the wrapped expectations |

- The E2E tests share one new Iceberg fixture, seeded through `tests/common/seed.rs`: a string column that stores `''`, NULL, and text across two data files, one of which holds `''` and no NULL, a variable-length binary column with an empty value and no NULL in that same file, plus a native copy of the same rows. A second virtual schema reads the table under `BINARY_AS_STRING`. The top-N test reuses the blank-label fixture of #309 with a shard count above 1.
- Each E2E test asserts the pushed scan SQL from `EXPLAIN VIRTUAL` beside the rows, so a silent fallback fails.
- The row-group scenario has no Exasol oracle, so a host scan test proves it over a multi-row-group file.

### Manual Testing

With this clone's stack variables exported and `EXAPUMP_DSN='exasol://sys:exasol@127.0.0.1:38563?validateservercertificate=0'`:

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| pushdown-planning-scan-empty-string | `exapump sql "SELECT COUNT(*) FROM <vs>.<fixture> WHERE RTRIM(s, s) IS NULL"` | The table's row count, not 0 |
| pushdown-planning-scan-empty-string | `exapump sql "SELECT REPLACE(s, 'U') FROM <vs>.<fixture> WHERE s = 'TRUCK'"` | `TRCK` |
| pruning-predicate-translation | `exapump sql "SELECT COUNT(*) FROM <vs>.<fixture> WHERE s IS NULL"` | The rows that store `''` plus the rows that store NULL |
| pushdown-planning-topn | `exapump sql "SELECT label FROM <vs>.<blank label fixture> ORDER BY label NULLS LAST LIMIT 3"` | The three smallest non-empty labels |
| vs-expression-translator-scalar-fns | `exapump sql "EXPLAIN VIRTUAL SELECT COUNT(*) FROM <vs>.<fixture> WHERE LENGTH(RTRIM(s, s)) = 0"` | The scan filter holds `nullif(rtrim(` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test --workspace` | 0 failures |
| E2E | `make e2e-up test-e2e` | 0 failures |
| Lint | `make lint` | 0 errors/warnings |
| Format | `make fmt-check` | No changes |

## Out of scope

- Row-group pruning for a comparison, `IN`, `BETWEEN`, or `LIKE` over a string column under a `NOT`.
- #227 (string functions on non-string arguments) and #432 (`SUBSTR` with a start at or below 0).
