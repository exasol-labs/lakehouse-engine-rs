# Plan Review Findings: fix-scan-empty-string-as-null

## Summary
- Axes checked: 4/4
- Total findings: 13 (Blockers: 1, Findings: 12)
- Intent Fidelity blockers: 0

## Intent Fidelity

No drift, creep, or reduction against the interview. Checked: both empty-string sources (Interview Q1) map to scenarios 1 and 2; the bare-column filter forms match Interview Q2; REPLACE and the live capture of the other text functions match Interview Q3. The single-table top-N and the plan-time null test are not in the interview, but both are wrong-result paths of the same `''`-is-NULL gap that #203 names ("the same gap applies to source data"), so they are not scope creep.

#### [INTENT_GAP] FINDINGS
- Location: intent.md § Directions D4; plan.md § Requirements, "String columns only"
- Issue: D4 says "The rule covers columns whose source type is a string." A variable-length binary column under `BINARY_AS_STRING = TRUE` has a binary source type, but the scan converts it to UTF-8 text, Exasol declares it `VARCHAR(2000000)`, and `vs-adapter/binary-column-rendering` says "the column's filters SHALL use plain string semantics". An empty binary value then reads as `''` in every computed expression, the exact defect this plan fixes. Neither the interview nor a direction says whether such a column is in scope. Separately, the plan keeps a conditional "Widen its entry if it cannot tell a string source column from a nested or JSON-fallback column", which would change the per-column type map whose contents `specs/architecture.md` § Components (pushdown-planner) lists, with no architecture delta. Neither distinction is needed: the hex rendering already reads an empty binary value as NULL (`binary_hex.rs` `render_hex`), and a JSON document is never `''`, so `nullif` over those columns changes no result and loses no pruning (none of them has Parquet statistics on the rendered text).
- Fix: Rewrite D4 to "The rule covers every column the per-column type map declares as a character type, which includes a `BINARY_AS_STRING` column. A hex-rendered binary column and a JSON column are covered too, where `nullif` changes nothing." Delete the "Widen its entry if it cannot tell..." sentence from plan.md § Requirements, "String columns only", and add a `BINARY_AS_STRING` column holding an empty value to the `stored_empty_string_reads_as_null` fixture or case table.

## Feasibility

No objection on hidden dependencies or NFRs. Checked: the live capture needs only the local Docker stack, which the plan names; the blank-label fixture of #309 exists (`seed.rs`, `E2E_BLANK_LABEL_TABLE`); the plan-time null-test rewrite is correct under the Iceberg spec text the plan quotes (a file holding `''` has the lower bound `''`), and the parsed filter's exactness flag gates only negation, so `NOT (col IS NULL)` negates `IS NULL OR = ''` soundly.

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: plan.md § Out of scope, first bullet
- Issue: "An empty binary value inside a function, such as `LENGTH(bin_col)`, still reads as `''`." This is false. The scan renders an empty binary value as NULL in its select list (`binary_hex.rs` `render_hex`, comment "An empty value renders NULL"), and the recorded scenario `vs-adapter/binary-column-rendering` "A binary column reads as lowercase hex on every source" states "an empty value and NULL SHALL both read as NULL". `LENGTH(bin_col)` already returns NULL for an empty value. The only binary column that still reads as `''` is a `BINARY_AS_STRING` one (see the Intent Fidelity finding).
- Fix: Delete the first Out of scope bullet from plan.md.

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: plan.md § Requirements, "One rule, one place"; intent.md § Directions
- Issue: The synthetic `nullif` node must be typed exactly as the column or call it wraps. Split aggregates take exact types or decline (ADR `split-aggregate-columns-take-exact-types-or-decline`: "`MIN` and `MAX` keep the argument's type"), select-list items declare types from the one type inference, and CASE and COALESCE unify branch types. If the typed pass gives the new node no type, or a different one, `MIN(s)`, `COUNT(s)`, and every grouped aggregate over a string column silently route to the qualified wrapper, and declared select-list types may change. The plan never states that the node is type-transparent.
- Fix: Add a direction to intent.md: "The empty-string node takes the exact type of its argument, so declared types, split aggregates, and branch unification are unchanged." Add one AND step to the scenario "A stored empty string reads as NULL in every computed expression": "`COUNT(s)` and `MIN(s)` SHALL split into scan partials", and assert the `group_keys` or `PARTIAL_` evidence in `stored_empty_string_reads_as_null`.

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: intent.md § Directions D5; pushdown-planning-scan-empty-string § "A text function with a NULL argument returns what native Exasol returns"
- Issue: D5 says a function "declines for that argument", and the scenario covers a NULL "as a literal or as a column value". The adapter sees a NULL literal at plan time, but a column value that is NULL, or `''` read as NULL, appears only at run time. For a function and argument position whose NULL result the scan cannot reproduce, the plan does not say whether the call declines for every non-literal argument in that position, or renders a guard such as `CASE WHEN arg IS NULL THEN ... END`. The first choice silently withdraws pushdown, and file pruning for filters over the call, for common forms with column arguments.
- Fix: Extend D5 with the rule for a non-literal argument: either "declines whenever that argument is not a non-NULL literal" or "renders a NULL guard for that argument". Name the choice in plan.md § Impact.

## Requirement Quality

#### [AMBIGUOUS_REQUIREMENT] BLOCKER
- Location: pushdown-planning-scan-empty-string § "A text function that returns an empty string returns NULL", second step; plan.md § Manual Testing, first row
- Issue: "`SELECT COUNT(*) FROM t WHERE RTRIM(s, s) IS NULL` SHALL count every row with a non-NULL `s`". In Exasol `RTRIM(NULL, NULL)` is NULL, so the filter is TRUE for every row, including rows that store NULL or `''`. The planned shared fixture stores `''`, NULL, and text, so native Exasol returns the total row count, and a test written to this step fails against the native oracle. The Manual Testing row repeats the error ("The count of rows with a non-NULL `s`").
- Fix: Change the step to "`SELECT COUNT(*) FROM t WHERE RTRIM(s, s) IS NULL` SHALL count every row of `t`, and the same filter with `LTRIM` SHALL too". Change the Manual Testing expected output to "The table's row count, not 0".

#### [COMPLETENESS_GAP] FINDINGS
- Location: pushdown-planning-scan-empty-string § "An equality of two string columns never matches empty strings"
- Issue: The scenario requires "compare the bare columns and add that one of them is `<> ''`" for any equality of two string columns, with no position limit. Under `NOT` this form returns wrong rows: for `a = ''` and `b = 'x'`, `NOT (a = b AND a <> '')` is TRUE, while native `NOT (NULL = 'x')` is NULL. plan.md § Requirements limits the bare form to a node reached from the filter root or the join condition root through `AND` and `OR`, but the recorded spec would not.
- Fix: In the scenario's WHEN, add "reached from the filter's root or the join condition's root through `AND` and `OR` only", and change the last step to "any other comparison of two string columns, and an equality under a `NOT`, SHALL read each column as `nullif(<column>, '')`".

#### [COMPLETENESS_GAP] FINDINGS
- Location: pushdown-planning-scan-empty-string § "A text function with a NULL argument returns what native Exasol returns", WHEN step
- Issue: Interview Q3 answered "All text functions". The WHEN list leaves out five translated functions that take or return text: `ASCII`, `CHR`, `OCTET_LENGTH`, `UNICODE`, and `UNICODECHR` (`scalar_functions.rs`, and the list in the changed `vs-expression-translator-scalar-fns` scenario). `CONCAT` is covered by its own spec, but these five have no NULL-argument requirement.
- Fix: Add `ASCII`, `CHR`, `OCTET_LENGTH`, `UNICODE`, and `UNICODECHR` to the WHEN list, and add them to the case table of `text_function_null_argument_matches_native`.

#### [AMBIGUOUS_REQUIREMENT] FINDINGS
- Location: pruning-predicate-translation § "A null test on a string column keeps the files that hold empty strings", last step
- Issue: "a null test on a column of any other type SHALL translate as before" names no result. Once the delta is recorded, "as before" refers to nothing.
- Fix: Replace the step with "a null test on a column of any other type SHALL translate as the column is NULL, or for `IS NOT NULL` as the column is not NULL".

#### [COMPLETENESS_GAP] FINDINGS
- Location: plan.md § Dead Code Removal, `render_join_sort_target` row; pushdown-planning-topn § "A per-shard top-N ranks a stored empty string as NULL"
- Issue: The plan moves "Its string and NaN ranking" into one rendering that both top-N paths share. The single-table per-shard sort then also ranks a float `NaN` as NULL, which changes its results, but no scenario step or test covers that. ADR `join-shard-ranks-by-emitted-value` expects this ("Any future fix of the `NaN` or flat-scan ranking divergence follows this rule"), so the change is sound, but it is unrecorded.
- Fix: Add to the topn scenario: "*AND* each shard SHALL rank a `NaN` in a `Float32` or `Float64` key as NULL". Add a NaN key case to `per_shard_top_n_ranks_empty_string_as_null`, or a sibling case, and list the NaN change in plan.md § Impact.

#### [BACKGROUND_DRIFT] FINDINGS
- Location: pushdown-planning-scan-empty-string § Background, last bullet
- Issue: "A binary column read as hex text follows `vs-adapter/binary-column-rendering`." No scenario step depends on it.
- Fix: Delete the bullet. If the Intent Fidelity fix adds a `BINARY_AS_STRING` case, state that case in the scenario step that tests it.

#### [COMPLETENESS_GAP] FINDINGS
- Location: plan.md § Scenario Coverage, row "String scalar functions translate to DataFusion string calls"
- Issue: The row names no test ("the existing string-function case table"). The scenario's current proof is the E2E test `text_conversions_match_native_under_session_settings` in `crates/lakehouse-e2e/tests/e2e_local/text_conversion.rs`, which carries its `/// Scenario:` line. The local planning rules require one named test and layer per scenario.
- Fix: Name one test. Either keep `text_conversions_match_native_under_session_settings` (E2E) and add a case that asserts `nullif(rtrim(` in the pushed SQL, or name the concrete `lib_tests.rs` unit test function that pins the wrapped rendering.

#### [COMPLETENESS_GAP] FINDINGS
- Location: intent.md § Directions D6; plan.md § Verification, first bullet below Scenario Coverage
- Issue: D6 proves the plan-time null test "on the Iceberg copy only" through a new Iceberg fixture. The every-format pruning case table already exists for this purpose (`crates/lakehouse-e2e/tests/common/pruning_cases.rs`): it runs each predicate against a native oracle on Iceberg, Delta, and Hive copies, checks that every file holding a matching row is scanned ("Sound"), and its fixture has a string column `S`. The new fixture duplicates that harness and leaves Delta and Hive unproven, although the Delta kernel and the Hive three-valued evaluation each read the new `= ''` node in their own way.
- Fix: Map the pruning scenario to `pruning_cases.rs`: store `''` and no NULL in `S` in one file of the pruning fixture, add the cases `S IS NULL`, `S IS NOT NULL`, and `NOT (S IS NOT NULL)` with their kept files, and drop D6. Keep the new fixture only for the scenarios that need it.

## Design Depth

No objection on ADR promotion, architecture drift, or ADR conflict. Checked: D1 names rule-2 criterion 4 and states its search result, and no accepted ADR covers empty strings (digest searched for empty, null, nullif, and binary). D1 conforms to `binary-reads-as-lowercase-hex-text` (rewrite only under `AND` and `OR`), `join-shard-ranks-by-emitted-value`, `pruning-paths-share-one-parsed-filter` (the rewrite sits in the one parse), and `delta-kernel-prunes-adapter-only-translates`. plan.md adds no component, boundary, or data flow beyond `specs/architecture.md`, apart from the conditional type-map widening covered by the Intent Fidelity finding.

#### [DESIGN_VIOLATION] FINDINGS
- Location: plan.md § Dead Code Removal
- Issue: Information hiding: the empty-string decision should have one owner. Once every string column and text function result is normalized, `TO_DATE` and `TO_TIMESTAMP` with a format no longer need their own `nullif` (`crates/vs-expression/src/lib.rs`, the `to_date(nullif({source}, ''), ...)` and `TO_TIMESTAMP_FN` arms), because their source can no longer be `''`. The table removes only the `CONCAT` copy, so a second copy of the rule stays.
- Fix: Add a Dead Code Removal row for the two `nullif` wrappers in the `TO_DATE` and `TO_TIMESTAMP` format arms of `crates/vs-expression/src/lib.rs`, with the reason "the source is already normalized by the general rule", and update their golden expectations.
