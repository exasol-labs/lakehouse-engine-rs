# Plan: fix-regexp-like-pushdown

## Summary

The adapter stops advertising `FN_PRED_REGEXP_LIKE`, so Exasol evaluates `REGEXP_LIKE` with its own PCRE semantics, and the translator and adapter code that rendered the predicate is removed (#471). `docs/capabilities.md` is corrected to list exactly the advertised capabilities, and a docs-consistency test fails when the page and the `CAPABILITIES` list differ. The implementing commit uses `Closes #471`.

## Context

- Pushed `REGEXP_LIKE` returns wrong rows. Measured on the Docker Exasol (2025.1.16) against `MY_LAKEHOUSE.EVENTS`, whose `NAME` values are `event-01` to `event-20`: `NAME REGEXP_LIKE 'event-1'` returns 10 rows through the virtual schema and 0 natively, `'(e|ev)(e|ent-01)?'` returns 20 and 0, and `'(?=event-0[1-3]).*'` fails the scan with `look-around ... is not supported` where native Exasol returns 3 rows.
- DataFusion's `regexp_like` matches anywhere in a value. Exasol accepts a value only when the first PCRE match from the value's start ends at its end, so anchoring the pattern does not help: `'ARGENTINA' REGEXP_LIKE '(A|AR)(G|GENTINA)?'` is false in Exasol and true under `^(?:<pat>)$`. Exasol's `\d` does not match `UNICODECHR(1635)`, and Exasol accepts backreferences (`'abab' REGEXP_LIKE '(ab)\1'` is true). All three facts were measured on the Docker Exasol.
- ADR `affirm-regexp-scalar-function-decline-re-verified-pinned-versions` (`specs/_decision/018-add-fn-regexp-pushdown.md`) already leaves the four regular-expression scalar functions to Exasol for this reason (#106), but keeps `FN_PRED_REGEXP_LIKE`. Decision-log [1] supersedes it.
- Measured with `EXPLAIN VIRTUAL` and the unadvertised `REGEXP_INSTR`: Exasol still pushes the other `AND` conjuncts and the projection, an `OR` containing the function pushes no filter, and an aggregate over a query with it runs in Exasol. No benchmark query uses `REGEXP_LIKE`.
- `docs/capabilities.md` omits ten advertised capabilities (`FN_CAST`, `FN_NEG`, `FN_WEEK`, `FN_DAYS_BETWEEN`, `FN_HOURS_BETWEEN`, `FN_MINUTES_BETWEEN`, `FN_SECONDS_BETWEEN`, `JOIN`, `JOIN_TYPE_INNER`, `JOIN_CONDITION_EQUI`) and lists joins under "Handled by Exasol". A `LEFT JOIN` between two virtual-schema tables is pushed as two separate table scans (measured with `EXPLAIN VIRTUAL`).
- The recorded scenario `vs-adapter/create-virtual-schema` "Adapter reports its pushdown capabilities" forbids `ORDER_BY_COLUMN`, `ORDER_BY_EXPRESSION`, `JOIN*`, `AGGREGATE_GROUP_BY_TUPLE`, and every `*_DISTINCT` aggregate, all of which are advertised today. Its delta replaces the hand-kept list with the documented set.
- Iceberg and Delta compliance: checked, not engaged. The Iceberg table spec's Scan Planning section requires that "Scan predicates are converted to partition predicates using an _inclusive projection_: if a scan predicate matches a row, then the partition predicate must match that row’s partition." Neither the Iceberg pruning predicate nor the Delta statistics predicate ever translated `predicate_like_regexp`, so file pruning is unchanged, and a predicate that leaves the pushed filter can only keep more files, never fewer. No type mapping or schema handling changes.
- Architecture: no change. The plan changes the capability list and translator arms inside existing components; `specs/architecture.md` does not mention regular expressions.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| VS Expression Translator — Predicates | CHANGED | `sql-comprehension/vs-expression-translator-predicates/spec.md` |
| VS Expression Translator | CHANGED | `sql-comprehension/vs-expression-translator/spec.md` |
| VS Expression Translator — Scalar Functions | CHANGED | `sql-functions/vs-expression-translator-scalar-fns/spec.md` |
| Pushdown Planning — LIKE Type Coercion | CHANGED | `pushdown-types/pushdown-planning-like-type-coercion/spec.md` |
| Pushdown Planning | CHANGED | `pushdown/pushdown-planning/spec.md` |
| Pushdown Planning — Select-List Expression Pushdown | CHANGED | `pushdown/pushdown-planning-selectlist-expressions/spec.md` |
| Pushdown Planning — Join Filter Type Coercion | CHANGED | `pushdown-joins/pushdown-planning-join-filter-type-coercion/spec.md` |
| Pushdown Planning — String Function and Type-Conversion Capability Extensions | CHANGED | `pushdown-capabilities/pushdown-planning-string-conversion-capability/spec.md` |
| Create Virtual Schema | CHANGED | `vs-adapter/create-virtual-schema/spec.md` |

## Impact

- `REGEXP_LIKE` returns the rows native Exasol returns, and a look-around or backreference pattern no longer fails the query.
- A `REGEXP_LIKE` predicate is no longer evaluated in the scan. Its sibling `AND` conjuncts and the projection still push down, but an `OR` that contains it pushes no filter, and an aggregate over a query with it runs in Exasol over the returned rows. Such queries can run slower. No benchmark query is affected.
- Not breaking: no SQL syntax, virtual schema property, or result shape changes.

## Implementation Tasks

### A: Translator declines REGEXP_LIKE

- [ ] 1.1 In `crates/vs-expression/src/lib_tests.rs`, add `"REGEXP_LIKE"` to the name list of `regexp_scalar_functions_decline_in_both_dialects` and give the test the line `/// Scenario: Regexp scalar functions are deliberately not translated`. Add `regexp_like_predicate_declines_in_both_dialects` (no scenario tag): a `predicate_like_regexp` node with a `column` expression and a `literal_string` pattern returns the error `unsupported expression node type: predicate_like_regexp` from `render_expression` and `render_expression_exasol`, and `None` from `render_expression_safe`, `render_df_filter_safe`, `render_expression_exasol_safe`, and `render_df_filter_exasol_safe`. Both tests fail before 1.2.
- [ ] 1.2 In `crates/vs-expression/src/lib.rs`, remove `("REGEXP_LIKE", ExasolForm::Shaped)` from `TRANSLATED_SCALAR_FNS`, the `function_scalar` `"REGEXP_LIKE"` arm, the `"predicate_like_regexp"` arm of `render_expression_inner`, and `"predicate_like_regexp"` from `is_boolean_producing`. Remove "infix predicate" from the `ExasolForm::Shaped` doc comment, because no declared name has that form any more.
- [ ] 1.3 In `lib_tests.rs`, delete `renders_regexp_like`, `renders_regexp_like_as_infix_predicate_in_exasol_dialect`, `regexp_like_predicate_missing_operand_errors_in_both_dialects`, `regexp_like_scalar_arity_errors_in_both_dialects`, and `regexp_scalar_exclusion_leaves_regexp_like_untouched`. Remove the `shaped("REGEXP_LIKE", ...)` row of the declaration sweep (the sweep fails on a row whose name is undeclared), the `predicate_like_regexp` row of the node-type sweep, and the `"regexp_like("` entry of the forbidden-token list. Reword the comment in `regexp_scalar_functions_decline_in_both_dialects` to state the match-semantics reason in at most 2 lines. Verify group A with `cargo test -p vs-expression`; the `lakehouse-engine` test rows that still render `predicate_like_regexp` belong to group B.

### B: Adapter LIKE guard and select-list node set

- [ ] 2.1 In `crates/lakehouse-engine/src/adapter/pushdown/support.rs`, make `like_subject_type_guard` match `Some("predicate_like")` only, and drop `"predicate_like_regexp"` from the pushable node arm of `project_columns`. An unexpected `predicate_like_regexp` select item then falls to the existing full-base-row arm.
- [ ] 2.2 In `support_tests.rs`, delete `like_guard_regexp_date_subject_wraps_cast`, the `predicate_like_regexp` row of `selectlist_predicate_node_projects_as_expr`, and the `predicate_like_regexp over c_decimal_a` row of `selectlist_like_over_non_string_subject_falls_back_to_full_row`. Leave `iceberg_predicate_tests.rs::and_all_untranslatable_returns_none` unchanged: it feeds `predicate_like_regexp` as an untranslatable input and handles nothing.
- [ ] 2.3 Add the missing `/// Scenario: <title>` lines to the existing tests that the Scenario Coverage table maps to the changed scenarios of group B's deltas (`like_guard_*`, `declined_like_on_*`, `selectlist_like_*`, `selectlist_predicate_node_projects_as_expr`, `e2e_broadcast_like_on_decimal_column_falls_back_and_filters`), quoting each title verbatim.

### C: Capability set, reference page, and E2E proof

- [ ] 3.1 In `crates/lakehouse-engine/src/adapter/capabilities_tests.rs`, add `capabilities_doc_lists_exactly_the_advertised_set`, a docs-consistency test with no scenario tag: the docs follow `CAPABILITIES`, never the reverse. It reads `docs/capabilities.md` with `include_str!("../../../../docs/capabilities.md")` and splits the page at the `## Handled by Exasol` heading. From the table rows (lines starting with `|`) above that heading it collects every backticked token that is an uppercase identifier (`[A-Z][A-Z0-9_]*`), so example SQL and prose mentions such as `FN_PRED_GREATER` are not read. It asserts that this set equals the `CAPABILITIES` set and names the missing and the extra entries in the failure message. It also asserts that no backticked token in the first column of the "Handled by Exasol" table is in `CAPABILITIES`. Only the first column is read there, because the other cells mention advertised names such as `LIMIT` in prose. Use plain string splitting, not a new dependency. The test fails before 3.4.
- [ ] 3.2 In `reports_audited_capability_set`, move `FN_PRED_REGEXP_LIKE` from the advertised list to the not-advertised list beside the four `FN_REGEXP_*` names, with a one-line comment on the match-semantics divergence. Add the absent names the create-virtual-schema scenario lists and the test does not yet check: `FN_CURRENT_USER`, `FN_SYS_GUID`, `FN_CURRENT_SCHEMA`, `LITERAL_INTERVAL`, and any name starting with `FN_ST_`. Add the line `/// Scenario: Adapter reports its pushdown capabilities`.
- [ ] 3.3 In `crates/lakehouse-engine/src/adapter/capabilities.rs`, remove `"FN_PRED_REGEXP_LIKE"` and put a comment of at most 2 lines in its place: the predicate is not advertised because DataFusion's regex matching cannot reproduce Exasol's whole-string PCRE match.
- [ ] 3.4 Edit `docs/capabilities.md`, keeping its sections and tables:
  - Comparison row: remove `FN_PRED_REGEXP_LIKE`.
  - Scalar functions: add `FN_NEG` to the Arithmetic row, a Conversion row with `FN_CAST` (example `SELECT CAST(id AS VARCHAR(20))`), and `FN_WEEK`, `FN_DAYS_BETWEEN`, `FN_HOURS_BETWEEN`, `FN_MINUTES_BETWEEN`, and `FN_SECONDS_BETWEEN` to the Date / time row.
  - Add a `## Joins` section before "Handled by Exasol" with a table row for `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI`, and one sentence: a two-table inner equi-join whose smaller side is below `JOIN_BROADCAST_MAX_BYTES` ([tuning](tuning.md)) is broadcast into every shard, and any other inner equi-join runs as an unaccelerated join the adapter generates.
  - "Handled by Exasol": replace the JOIN row with a row for outer, non-equi, and Cartesian joins (`JOIN_TYPE_LEFT_OUTER`, `JOIN_TYPE_RIGHT_OUTER`, `JOIN_TYPE_FULL_OUTER`, `JOIN_CONDITION_ALL`), which Exasol runs by pushing each table separately and joining the results. Add a row for `REGEXP_LIKE`, `REGEXP_REPLACE`, `REGEXP_SUBSTR`, `REGEXP_INSTR`, and `REGEXP_COUNT`: Exasol evaluates them over the returned rows because DataFusion's regex matching differs from Exasol's PCRE matching, the other `AND` conjuncts and the projection still push down, and an `OR` that contains one pushes no filter. Add this third reason to the section's opening list of reasons.
  - Filtering intro: replace "Exasol re-checks only the predicates that the adapter cannot translate" with a sentence saying that Exasol applies only the predicates whose capability the adapter does not advertise, such as `REGEXP_LIKE`.
- [ ] 3.5 In `crates/lakehouse-engine/tests/e2e_capability_test.rs`, replace `e2e_regexp_like_in_filter` with one test, `e2e_regexp_like_returns_native_rows`, tagged `/// Scenario: Regexp scalar functions are deliberately not translated`. For each predicate below, run `SELECT id FROM <vs_table()> WHERE <predicate>` and assert that the sorted ids equal the ids derived from the seeded names. Also run the same predicate natively over `SELECT 'event-' || LPAD(TO_CHAR(LEVEL), 2, '0') AS name FROM DUAL CONNECT BY LEVEL <= 20` and assert the native count, so that a fixture change that stops the test from discriminating fails with a message saying so. Predicates, with expected ids, native count, and the defect's result:
  - `name REGEXP_LIKE 'event-1'`: none, 0, defect 10 rows.
  - `NOT (name REGEXP_LIKE 'event-1')`: 1 to 20, 20, defect 10 rows.
  - `name REGEXP_LIKE '(e|ev)(e|ent-01)?'`: none, 0, defect 20 rows.
  - `name REGEXP_LIKE '(?=event-0[1-3]).*'`: 1 to 3, 3, defect a scan error.

  The same test then runs `SELECT id, name FROM <vs_table()> WHERE id < 5 AND name REGEXP_LIKE 'event-0[1-3]'`. Through `explain_virtual_pushdown_request`, it asserts that the request's `filter` is a `predicate_less` over column `ID` and that the serialized filter contains no `regexp` in any case. Through `explain_virtual_sql`, it asserts that the echoed capability list does not contain `"FN_PRED_REGEXP_LIKE"`. It runs the query and asserts that the sorted ids are `[1, 2, 3]`. Id 4 satisfies `id < 5` but not the pattern, so a dropped regex predicate returns four rows.
- [ ] 3.6 Keep `e2e_count_distinct_regexp_like_matches_native_oracle` unchanged. Exasol now evaluates its `REGEXP_LIKE` select item, and the test still compares against the native oracle.

### Verification

- [ ] 4.1 Run `cargo fmt --all`, `cargo clippy --all-targets` (zero warnings), and `cargo test --workspace`.
- [ ] 4.2 Run `make test-e2e` (which runs `make cross-udf-build`) against the Docker stack, and report the real result.
- [ ] 4.3 Run `cargo llvm-cov --workspace --lcov --output-path <scratchpad>/lcov.info` and the changed-line coverage check. Changed production lines must reach 90% coverage.
- [ ] 4.4 Run `grep -rn -i "regexp_like\|predicate_like_regexp" crates --include='*.rs' | grep -v '_tests.rs' | grep -v '/tests/'`. Expect only the comment from 3.3.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Translator declines REGEXP_LIKE | 1.1-1.3 | — | spec deltas `sql-comprehension/vs-expression-translator-predicates`, `sql-comprehension/vs-expression-translator`, `sql-functions/vs-expression-translator-scalar-fns`; `crates/vs-expression/src/lib.rs`, `crates/vs-expression/src/lib_tests.rs` |
| B: Adapter LIKE guard and select-list node set | 2.1-2.3 | A (two `support_tests.rs` rows render `predicate_like_regexp` through `vs-expression` and fail once A lands; B deletes them) | spec deltas `pushdown-types/pushdown-planning-like-type-coercion`, `pushdown/pushdown-planning`, `pushdown/pushdown-planning-selectlist-expressions`, `pushdown-joins/pushdown-planning-join-filter-type-coercion`; `crates/lakehouse-engine/src/adapter/pushdown/support.rs`, `support_tests.rs`, `crates/lakehouse-engine/tests/e2e_join_test.rs` (scenario lines only) |
| C: Capability set, reference page, and E2E proof | 3.1-3.6, 4.1-4.4 | A, B (the E2E run and the final grep need both) | spec deltas `pushdown-capabilities/pushdown-planning-string-conversion-capability`, `vs-adapter/create-virtual-schema`; decision-log [1], [2]; `crates/lakehouse-engine/src/adapter/capabilities.rs`, `capabilities_tests.rs`, `docs/capabilities.md`, `crates/lakehouse-engine/tests/e2e_capability_test.rs`; `specs/testing.md` § Correctness oracles |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Declaration row | `crates/vs-expression/src/lib.rs` `TRANSLATED_SCALAR_FNS` `("REGEXP_LIKE", ExasolForm::Shaped)` | REGEXP_LIKE is no longer translated |
| Match arm | `crates/vs-expression/src/lib.rs` `render_expression_inner` `"predicate_like_regexp"` | Same |
| Match arm | `crates/vs-expression/src/lib.rs` `function_scalar` `"REGEXP_LIKE"` | Same |
| Match entry | `crates/vs-expression/src/lib.rs` `is_boolean_producing` `"predicate_like_regexp"` | No arm produces the node |
| Capability | `crates/lakehouse-engine/src/adapter/capabilities.rs` `"FN_PRED_REGEXP_LIKE"` | Exasol evaluates REGEXP_LIKE |
| Match entry | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` `like_subject_type_guard`, `project_columns` `"predicate_like_regexp"` | Exasol never sends the node |
| Test | `crates/vs-expression/src/lib_tests.rs` `renders_regexp_like`, `renders_regexp_like_as_infix_predicate_in_exasol_dialect`, `regexp_like_predicate_missing_operand_errors_in_both_dialects`, `regexp_like_scalar_arity_errors_in_both_dialects`, `regexp_scalar_exclusion_leaves_regexp_like_untouched`, two sweep rows, one forbidden token | Test the removed rendering |
| Test | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` `like_guard_regexp_date_subject_wraps_cast`, two table rows | Test the removed guard and select-list entry |
| Test | `crates/lakehouse-engine/tests/e2e_capability_test.rs` `e2e_regexp_like_in_filter` | Replaced by `e2e_regexp_like_returns_native_rows`; its pattern matches the same rows under both engines |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Regexp scalar functions are deliberately not translated | Unit + E2E | `crates/vs-expression/src/lib_tests.rs`, `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `regexp_scalar_functions_decline_in_both_dialects`, `e2e_regexp_like_returns_native_rows` |
| LIKE on a VARCHAR or CHAR column pushes down unchanged | Unit (existing) | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | `like_guard_varchar_subject_unchanged`, `like_guard_char_subject_unchanged` |
| LIKE on a DATE column pushes down wrapped in CAST-to-VARCHAR | Unit (existing) | `support_tests.rs` | `like_guard_date_subject_wraps_cast` |
| LIKE on a DECIMAL column declines the whole filter | Unit (existing) | `support_tests.rs` | `like_guard_decimal_subject_declines` |
| LIKE on an integer column declines the whole filter | Unit (existing) | `support_tests.rs` | `like_guard_integer_subject_declines`, `declined_like_on_integer_column_routes_to_wrapper_where` |
| LIKE on a non-column subject is left untouched | Unit (existing) | `support_tests.rs` | `like_guard_non_column_subject_untouched` |
| LIKE on a bare column whose type cannot be resolved declines the whole filter | Unit (existing) | `support_tests.rs` | `like_guard_unresolvable_column_declines`, `declined_like_on_unresolvable_column_routes_to_wrapper_where` |
| A select-list LIKE over a DATE column projects the CAST-to-VARCHAR form | Unit (existing) | `support_tests.rs` | `selectlist_like_over_date_projects_cast_expr` |
| A select-list LIKE over a non-string column widens the projection to the full base row | Unit (existing) | `support_tests.rs` | `selectlist_like_over_non_string_subject_falls_back_to_full_row` |
| Filter predicate is pushed into the scan spec | Unit (existing) | `support_tests.rs` | `filter_in_common_arg` |
| Scalar select-list expression is pushed into the scan-driving query | Unit (existing) | `support_tests.rs` | `selectlist_predicate_node_projects_as_expr` |
| A broadcast-join filter over a non-string LIKE subject declines the broadcast plan | E2E (existing) | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_broadcast_like_on_decimal_column_falls_back_and_filters` |
| Adapter reports its pushdown capabilities | Unit | `crates/lakehouse-engine/src/adapter/capabilities_tests.rs` | `reports_audited_capability_set` |

`regexp_like_predicate_declines_in_both_dialects` and `capabilities_doc_lists_exactly_the_advertised_set` map to no scenario. The `sql-comprehension/vs-expression-translator` delta changes only its description.

### Manual Testing

Connection for every command: `-d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"`, after `make test-e2e` has deployed the `.so` and created `MY_LAKEHOUSE`.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| VS Expression Translator — Predicates, VS Expression Translator, VS Expression Translator — Scalar Functions | `cargo test -p vs-expression regexp` | The two decline tests pass |
| Pushdown Planning — LIKE Type Coercion, Pushdown Planning, Select-List Expression Pushdown, Join Filter Type Coercion | `cargo test -p lakehouse-engine --lib -- like_guard selectlist_ filter_in_common_arg` | All selected tests pass |
| String Function and Type-Conversion Capability Extensions | `exapump sql "SELECT COUNT(*) FROM MY_LAKEHOUSE.EVENTS WHERE NAME REGEXP_LIKE 'event-1'"` | `0` (was `10`) |
| String Function and Type-Conversion Capability Extensions | `exapump sql "SELECT COUNT(*) FROM MY_LAKEHOUSE.EVENTS WHERE NAME REGEXP_LIKE '(?=event-0[1-3]).*'"` | `3`, no UDF error |
| String Function and Type-Conversion Capability Extensions | `exapump sql "EXPLAIN VIRTUAL SELECT ID, NAME FROM MY_LAKEHOUSE.EVENTS WHERE ID < 5 AND NAME REGEXP_LIKE 'event-0[1-3]'"` | Pushdown request filter `EVENTS.ID<5` only; no `FN_PRED_REGEXP_LIKE` in the echoed capabilities |
| Create Virtual Schema | `cargo test -p lakehouse-engine capabilities_doc_lists_exactly_the_advertised_set` | Passes; it fails if a name is added to `CAPABILITIES` without a `docs/capabilities.md` entry |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test --workspace` | 0 failures |
| E2E | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt --all --check` | No changes |
| Coverage | `cargo llvm-cov --workspace --lcov --output-path <scratchpad>/lcov.info` and the changed-line check | Changed production lines at or above 90% |
