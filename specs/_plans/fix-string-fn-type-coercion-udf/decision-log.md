# Decision Log: fix-string-fn-type-coercion-udf

## Interview

**Q:** Issue #227's "Recommended approach" is fully specified (exa_to_varchar DataFusion UDF, move string_position_args into vs-expression, one decline check for DOUBLE/BOOLEAN/TIMESTAMP columns, remove the old adapter rewrites). Should the plan adopt it as-is, or do you want the planner to weigh alternatives first?
**A:** Adopt as specified. Plan directly to the issue's design. Do not re-litigate alternatives.

**Q:** exa_to_varchar is a new session UDF, following the existing checked_div.rs registration pattern. Issue #227 says it can "share registration code" with #431/#201 (also open, not yet implemented) but has "no dependency" on them. Should this plan build a shared/generic session-UDF registration mechanism now, or just add exa_to_varchar the same minimal way checked_div was added?
**A:** Minimal, per-UDF. Register exa_to_varchar the same standalone way `CheckedFloatDivUdf` is registered today. Do NOT build a generic/shared session-UDF registry for the benefit of #431/#201. Both are still open and unimplemented, so that would be premature abstraction.

**Q:** The plan will remove string_function_arg_type_guard, rewrite_decimal_stringifications, and decimal_to_varchar_exasol from apply_type_rewrites (adapter/pushdown/support.rs), since the UDF replaces their job everywhere (WHERE, select list, GROUP BY, aggregate args, HAVING, ORDER BY). Confirm this removal is in scope for this plan, not deferred to a follow-up?
**A:** Yes, remove in this plan. The old rewrites are deleted as part of this plan, not deprecated alongside the new path, because keeping both risks the double-rewrite and text-matching bugs the issue describes.

## Design Decisions

### [1] String conversion lives in the DataFusion session function, and the adapter only declines

- **Decision:** Adopt issue #227's design as specified (interview Q1). `vs-expression` renders every string-converted argument as `exa_to_varchar(<arg>)` in the DataFusion dialect only, except a boolean-producing argument, which renders through the #200 CASE form. The scan session function `exa_to_varchar` picks the conversion from the argument's Arrow type. The adapter runs one read-only check that declines a string conversion of a bare DOUBLE, BOOLEAN, or TIMESTAMP column, and it never rewrites an expression tree for string conversion.
- **Alternatives:** Apply `apply_type_rewrites` on the two unguarded paths (GROUP BY keys and grouped select items, and aggregate arguments). Rejected, per the issue: aggregates and group keys are matched by rendered SQL text in `ordinary_plans`, `single_group_plan_types`, `empty_result.rs`, `render_having_over_merge`, `render_scalar_over_merge`, `parse_count_distinct`, `detect_group_by_aggregates`, and `group_key_output_ordinal`, so every site would need the same rewritten tree or fall back silently or panic in an `.expect`. The rewritten `decimal_to_varchar_exasol` node is valid only in DataFusion, and Exasol rejects its `CAST(x AS VARCHAR)` (SQL state `42000`). Column types cover bare columns only.
- **Rationale:** A deterministic renderer gives every text match the same string. A DataFusion-dialect-only wrapper cannot reach Exasol-dialect SQL. The Arrow type is known for computed arguments too, which the adapter cannot see.
- **Consequences:**
  - The string-converted argument table moves from the adapter into `vs-expression` and is exposed as `string_converted_args`. The renderer and the adapter check both read it, so neither holds a copy.
  - `vs-expression` exports `EXA_TO_VARCHAR_FN` and does not implement the function, the same split as `CHECKED_FLOAT_DIV_FN`. A DataFusion-dialect consumer of the sibling-shared crate MUST register the function.
  - `like_subject_type_guard` (#207) keeps its adapter-side rewrite and is out of scope.
- **Promotes to ADR:** yes

### [2] exa_to_varchar is registered standalone, like the checked float division

- **Decision:** Add `ExaToVarcharUdf` and `register_exa_to_varchar_udf` in a new `crates/lakehouse-engine/src/scan/to_varchar.rs`, and call the registration from `build_session_context` (`scan/object_store.rs`) beside `register_checked_float_div_udf` (interview Q2).
- **Alternatives:** A shared session-UDF registry for #431 (`exa_trunc`/`exa_round`) and #201. Rejected: both issues are open and unimplemented, so the registry would be shaped around one consumer.
- **Rationale:** Two registrations are one line each at one call site. A registry earns its place only when a real third consumer shows the shape.
- **Consequences:** The registration site is `build_session_context` in `scan/object_store.rs`. `scan/mod.rs` only declares the module. Whichever of #431 or #201 lands next MAY extract shared registration if duplication appears.
- **Promotes to ADR:** no

### [3] The adapter rewrites are deleted in this plan

- **Decision:** Delete `string_function_arg_type_guard`, `coerce_string_position_arg`, `StringPositionArgs`, `string_position_args`, `rewrite_decimal_stringifications`, `is_bare_decimal_column`, `wrap_decimal_to_varchar`, the `decimal_to_varchar_exasol` renderer arm, and `format_decimal_exasol_style` in this plan (interview Q3).
- **Alternatives:** Keep the rewrites beside the new path and remove them later. Rejected: both would wrap the same argument, which is the double-rewrite and text-match hazard the issue describes.
- **Rationale:** The renderer wrapping covers every surface the rewrites covered.
- **Consequences:** The renderer arm stays until the adapter stops producing the node, so task 4.4 depends on task 4.3.
- **Promotes to ADR:** no

### [4] One decline predicate, consumed by the pipeline and by the request-shape classifier

- **Decision:** `string_conversion_declined(expr, col_types) -> bool` in `pushdown/support.rs` is the single owner of the decision. `apply_type_rewrites` calls it after `like_subject_type_guard`, which reaches the single-table WHERE filter, each select-list item, and both join filter surfaces. `classify_request_shape` calls it on `groupBy`, `selectList`, `having`, and `orderBy` before tier 1, and routes a decline to `GroupByWrapper` for a GROUP BY request and to `RowScan` otherwise.
- **Alternatives:** (a) One whole-request check at the top of `build_dispatch_sql` routing every hit to the declined-filter wrapper. Rejected: it gives up the scan-side filter when only a select-list item declines, and it misses the join paths and the empty-result path. (b) A check inside `grouped_agg.rs` and `scalar_over_agg.rs`. Rejected: it re-creates the per-site fan-out the issue warns against.
- **Rationale:** Each surface keeps its existing fallback. The classifier is shared by the non-empty and the empty-result paths, so both see the same shape. The predicate walks the tree through `rewrite_expr_tree` and discards the result, so it has the LIKE guard's reach and decline propagation without rewriting anything.
- **Consequences:** A `RowScan` route for a single-group aggregate works on the non-empty path because `project_columns` already widens any select list that carries an aggregate. On the empty-result path, the widened `RowScan` arm renders the same wrapper select list over a zero-row derived table, so Exasol returns the one row an aggregate over zero rows yields (Review Findings, first entry). For a non-aggregate row scan, an ORDER BY string function already renders in the Exasol dialect through the declined-ORDER-BY path, so the classifier hit changes nothing there. The check's pass set (`Character`, `Date`, `Decimal`) is a subset of what `exa_to_varchar` converts, so the two can drift in one safe direction only: a pass the function cannot convert fails loudly at planning time, and a decline the function could have converted only runs slower.
- **Promotes to ADR:** no

### [5] exa_to_varchar converts JSON-fallback types to the scan's emitted text, and a Null type to NULL

- **Decision:** Beyond the issue's table, `exa_to_varchar` converts a type the scan emits through its JSON-fallback VARCHAR path (`needs_json_fallback`: a `Decimal128` outside Exasol's DECIMAL domain, a nested type, `Binary`, `Time32`/`Time64`) to the same text the scan emits for it, reusing that conversion. It converts the Arrow `Null` type to NULL. Only `Float16`/`Float32`/`Float64`, `Boolean`, and `Timestamp` raise the planning error.
- **Alternatives:** The issue's "anything else → planning error" row with every `Decimal128` trimmed. Rejected: Exasol sees a `Decimal128(38, s)` column (common in Spark and Databricks tables) as the VARCHAR text `123.4500`, so trimming inside a string function would disagree with the column's own returned value. A `CAST` over a `Binary` or `Time64` column that pushes down today would start failing, and `CONCAT(c, NULL)` would fail on the `Null` argument.
- **Rationale:** A string function MUST see the text the column itself returns. The decimal domain is an Exasol target-type limit, so this is the deliberate trade-off CLAUDE.md requires to be named in the spec.
- **Consequences:** No E2E fixture carries a 37- or 38-digit decimal, so this row is covered by unit tests that compare against the emit path.
- **Promotes to ADR:** no

### [6] Unconvertible literals and long INSTR/LOCATE calls are DataFusion-dialect render errors

- **Decision:** In a string-converted position, a `literal_double`, a `literal_exactnumeric` whose value carries a decimal point or an exponent, and a `literal_timestamp*` node are DataFusion-dialect render errors. `INSTR`/`LOCATE` with more than two arguments are a DataFusion-dialect render error (issue #228, step 1).
- **Alternatives:** (a) Extend the adapter check to literals. Rejected: the adapter would encode DataFusion's literal typing (`parse_float_as_decimal` off in `session_config_for_spec`), a decision it does not own. (b) Render a fractional literal as `CAST('<text>' AS DECIMAL(p,s))`. Rejected: more rendering logic for a shape Exasol may constant-fold before pushdown; the decline is correct.
- **Rationale:** A render error already routes every surface to native Exasol evaluation, and the renderer is the module that knows how it types a literal.
- **Consequences:** Whether Exasol sends a fractional literal unfolded is checked live by task 6.1. `classify_scalar_over_aggregate` probes renderability in the DataFusion dialect, so a scalar-over-aggregate residual that uses `INSTR` with three arguments now declines to the wrapper: slower, correct.
- **Promotes to ADR:** no

### [7] The Iceberg and Delta spec check is engaged and finds no deviation

- **Decision:** CLAUDE.md's compliance rule applies, because the feature changes pushdown and dispatches on primitive types. The check quotes the Iceberg table spec's Primitive Types rows (`boolean` "True or false", `double` "64-bit IEEE 754 floating point", `timestamp` "Timestamp, microsecond precision, without timezone", `date` "Calendar date without timezone or time", `decimal(P,S)` "Fixed-point decimal; precision P, scale S" / "Scale is fixed, precision must be 38 or less", v3 `unknown`) and the Delta protocol's `§ Primitive Types` rows (`boolean`, `double`, `date`, `decimal` "The precision and scale can be up to 38", `void`). Neither spec defines a query text form for a value, so Exasol's conversion is the reference. Delta's `§ Partition Value Serialization` governs partition strings in the log, not query results.
- **Alternatives:** Record the rule as not applicable because string conversion is Exasol and DataFusion dialect semantics. Rejected: the feature does dispatch on the Arrow types those primitives map to, and the 37- and 38-digit decimal range is a real Exasol target-type trade-off that MUST be named.
- **Rationale:** Quoting the normative rows keeps the determination checkable.
- **Consequences:** The only named trade-off is decision [5]'s out-of-domain decimal. It is recorded in `datafusion-scan/scan-execution-exa-to-varchar` and `vs-adapter/pushdown-planning-decimal-string-format`.
- **Promotes to ADR:** no

### [8] Tracked exceptions after this plan

- **Decision:** #223 stays open only for a computed DOUBLE, BOOLEAN, or TIMESTAMP argument, which fails at planning time with an error naming `exa_to_varchar`. A computed integer, DECIMAL, or DATE argument now converts, so the recorded computed-DECIMAL exception closes. #216 (NLS date format) is unchanged. #228 gets step 1 and stays open for the faithful three- and four-argument rendering.
- **Alternatives:** none
- **Rationale:** This is the boundary issue #227's "Remaining limits" section sets.
- **Consequences:** The implementing commit uses `Closes #227` and `Refs #223, #228`, not `Closes` for either.
- **Promotes to ADR:** no

### [9] Accepted behavior changes outside the issue's examples

- **Decision:** Accept three visible changes. A string CAST over a bare DOUBLE, BOOLEAN, or TIMESTAMP column now declines to native Exasol evaluation, where it previously pushed down with DataFusion's text. A computed DOUBLE, BOOLEAN, or TIMESTAMP argument of `CONCAT` or a string CAST now fails at planning time, where it previously returned DataFusion's text. Every string-converted argument gains one `exa_to_varchar` call per batch.
- **Alternatives:** Exempt `CAST` and `CONCAT` from the new rules to keep today's pushdowns. Rejected: today's results for those shapes differ from Exasol's without an error.
- **Rationale:** A clear error or a slower correct result is preferred to a silently wrong one, the project's recorded correctness-first direction.
- **Consequences:** `plan.md` Impact calls these out for operators.
- **Promotes to ADR:** no

### [10] Background edits in features this plan does not own stay limited to drifted bullets and their chains

- **Decision:** Rewrite the Background of the three features the issue names, which this plan owns. In `pushdown-planning-like-type-coercion`, `pushdown-planning-join-filter-type-coercion`, `pushdown-module-dedup-consolidation`, and `pushdown-col-types-consolidation`, collapse each type-rewrite bullet this plan makes inaccurate, together with the `SUPERSEDES` chain it sits in, into current-state bullets. Leave the storage, case-fold, and visibility bullets verbatim.
- **Alternatives:** (a) Rewrite those Backgrounds in full. Rejected: their storage and case-fold bullets back scenarios this plan does not change. (b) Collapse only the drifted bullets and reproduce their chains verbatim. Rejected: a `DELTA:CHANGED` Background replaces the permanent section, so a reproduced chain carries its retrospective prose into the permanent spec.
- **Rationale:** A permanent spec states current behavior, and a limited edit keeps the unrelated evidence intact.
- **Consequences:** `pushdown-planning-join-fallback`'s Background shows a leg fragment as `CAST(<col> AS VARCHAR) LIKE …`. The node it describes is unchanged and the fragment is illustrative, so this plan leaves that feature untouched.
- **Promotes to ADR:** no

### [11] The #227 E2E tests reuse the typed probe fixture

- **Decision:** Add the #227 E2E tests to `crates/lakehouse-engine/tests/e2e_capability_test.rs` over `typed_distinct_probe`: `ID` (Iceberg `long`) stands for `c_custkey`, `C_DECIMAL_A` (`DECIMAL(9,2)`) for `c_acctbal`, and `C_DOUBLE` for the decline path. Expected values come from the file's independent oracles (`TYPED_DECIMAL_A_UNSCALED`, `exasol_trim_decimal_string`, in-session native literal queries).
- **Alternatives:** A TPC-H `customer` fixture with `c_acctbal`. Rejected: the local seed has no `c_acctbal`, and the typed probe already carries every needed type.
- **Rationale:** The Makefile `test-e2e` target already runs this file, and its oracles are independent of production code.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] A declined single-group aggregate returned zero rows on the empty-result path

- **Finding:** Round 1 BLOCKER ([COMPLETENESS_GAP] [REQUIREMENT_CONFLICT]). A non-GROUP-BY aggregate that declines routes to `RequestShape::RowScan`. The empty path answered a widened `RowScan` with `empty_select_list_typed_sql` (`... FROM DUAL WHERE 1=0`), so `COUNT(UPPER(c_double))` over a fully pruned file list returned zero rows where Exasol returns `0`. This contradicted the recorded empty-result scenario that requires exactly one row, and decision [4]'s Consequences claimed the route worked on both paths.
- **Direction change:** `empty_result_sql` routes a widened `RowScan` whose select list carries an aggregate to a new `joins/sql_builders.rs` builder. The builder renders the qualified wrapper's select list and trailing clauses in the Exasol dialect over a zero-row derived table typed from `referenced_column_projection`. Exasol then evaluates the aggregate over zero rows itself. A dedicated `RequestShape` variant was rejected: its empty arm would need per-item empty literals for items that failed to parse, which is the reason they declined. The fix also covers an aggregate the numeric gate demotes, because that request reaches the same arm. New scenario in `vs-adapter/pushdown-planning-empty-result`, task 4.5, task 6.1, and decision [4] Consequences corrected.
- **Promotes to ADR:** no

### [plan-review] Boolean-producing string-converted arguments had two conflicting renderings

- **Finding:** Round 1 BLOCKER ([REQUIREMENT_CONFLICT] [AMBIGUOUS_REQUIREMENT]). The first string-conversion scenario wrapped every string-converted argument in `exa_to_varchar`, and the literal scenario required the CASE form for `literal_bool`, so `UPPER(TRUE)` had two required renderings. The CAST scenario exempted a boolean source only for CAST and `CONCAT`, and task 2.3 said "Keep" for arms where the renderer applies no CASE form today. `UPPER(c_a > 1)` would reach `exa_to_varchar` with a `Boolean` argument and fail at scan planning.
- **Direction change:** The first string-conversion scenario states that a boolean-producing argument renders through the #200 CASE form with no wrapper in every arm (string functions, `INSTR`/`LOCATE`, `CONCAT`, string CAST). The CAST and literal scenarios, the table scenario ("either wrapped or CASE-rendered"), and the scalar-fns and CAST deltas align with that rule. Task 2.3 says "Apply" and adds `UPPER(<predicate_less>)` and `UPPER(TRUE)` cases. The table test is renamed `string_converted_args_returns_exactly_the_converted_arguments`.
- **Promotes to ADR:** no

### [plan-review] Background bullets backed no scenario

- **Finding:** Round 1 BLOCKER ([IMPLEMENTATION_LEAKAGE]). The `exa-to-varchar` NULL bullet named Iceberg v3 `unknown` and Delta `void` sources that no scenario or fixture exercises. The pinned iceberg-rust `PrimitiveType` has no `Unknown` variant. The string-conversion bullet on `EXA_TO_VARCHAR_FN` backed no scenario in that spec.
- **Direction change:** The NULL bullet states only that a NULL literal reaches the function as the Arrow `Null` type. The `EXA_TO_VARCHAR_FN` bullet moved into `scan-execution-exa-to-varchar`'s Background, beside the registration scenario that reads the constant.
- **Promotes to ADR:** no
