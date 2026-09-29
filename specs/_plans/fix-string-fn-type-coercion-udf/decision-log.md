# Decision Log: fix-string-fn-type-coercion-udf

## Interview

**Q:** Issue #227's "Recommended approach" is fully specified (exa_to_varchar DataFusion UDF, move string_position_args into vs-expression, one decline check for DOUBLE/BOOLEAN/TIMESTAMP columns, remove the old adapter rewrites). Should the plan adopt it as-is, or do you want the planner to weigh alternatives first?
**A:** Adopt as specified. Plan directly to the issue's design. Do not re-litigate alternatives. (Superseded in part by decision [1] after review: the decline check and the planning error are dropped.)

**Q:** exa_to_varchar is a new session UDF, following the existing checked_div.rs registration pattern. Issue #227 says it can "share registration code" with #431/#201 (also open, not yet implemented) but has "no dependency" on them. Should this plan build a shared/generic session-UDF registration mechanism now, or just add exa_to_varchar the same minimal way checked_div was added?
**A:** Minimal, per-UDF. Register exa_to_varchar the same standalone way `CheckedFloatDivUdf` is registered today. Do NOT build a generic/shared session-UDF registry for the benefit of #431/#201. Both are still open and unimplemented, so that would be premature abstraction.

**Q:** The plan will remove string_function_arg_type_guard, rewrite_decimal_stringifications, and decimal_to_varchar_exasol from apply_type_rewrites (adapter/pushdown/support.rs), since the UDF replaces their job everywhere (WHERE, select list, GROUP BY, aggregate args, HAVING, ORDER BY). Confirm this removal is in scope for this plan, not deferred to a follow-up?
**A:** Yes, remove in this plan. The old rewrites are deleted as part of this plan, not deprecated alongside the new path, because keeping both risks the double-rewrite and text-matching bugs the issue describes.

## Design Decisions

### [1] exa_to_varchar converts every Arrow type, and the adapter makes no string-conversion decision

- **Decision:** `vs-expression` renders every string-converted argument as `exa_to_varchar(<arg>)` in the DataFusion dialect only. The scan session function picks the conversion from the argument's Arrow type and covers every type the scan can produce, DOUBLE, BOOLEAN, and TIMESTAMP included. The adapter reads no column type for string conversion and rewrites no tree for it.
- **Alternatives:** (a) Issue #227's design as first planned: an adapter decline check for bare DOUBLE, BOOLEAN, and TIMESTAMP columns, and a planning error in `exa_to_varchar` for `Float64`, `Boolean`, and `Timestamp`. Rejected: DataFusion yields `Float64` where Exasol yields DECIMAL (`c_acctbal * 1.5`, because fractional literals parse as `Float64`) or DOUBLE (`ROUND(c_acctbal / 3, 2)`). Both queries convert correctly today, and the planning error would fail them with no fallback, because the adapter cannot see a computed type. The three types also have deterministic Exasol text (captured live, see [6]). (b) Run `apply_type_rewrites` on the GROUP BY and aggregate-argument paths. Rejected: eight sites match plans and group keys by rendered text, each would need the same rewritten tree, and Exasol rejects the rewritten `decimal_to_varchar_exasol` node's `CAST(x AS VARCHAR)` (SQL state `42000`).
- **Rationale:** The scan session is the only point that knows the Arrow type of a computed argument. A syntactic wrapper gives every text match the same string, and a DataFusion-dialect-only wrapper cannot reach Exasol SQL.
- **Consequences:**
  - No `string_conversion_declined` predicate exists, and `classify_request_shape` is unchanged.
  - The string-converted argument table is private to `vs-expression`, because no adapter code reads it.
  - `vs-expression` exports `EXA_TO_VARCHAR_FN` and does not implement the function, the same split as `CHECKED_FLOAT_DIV_FN`. A DataFusion-dialect consumer of the sibling-shared crate MUST register the function.
  - #223 closes: its "possible fix" section proposes exactly this conversion.
- **Promotes to ADR:** yes

### [2] exa_to_varchar is registered standalone, like the checked float division

- **Decision:** Add `ExaToVarcharUdf` and `register_exa_to_varchar_udf` in a new `crates/lakehouse-engine/src/scan/to_varchar.rs`, and call the registration from `build_session_context` (`scan/object_store.rs`) beside `register_checked_float_div_udf` (interview Q2).
- **Alternatives:** A shared session-UDF registry for #431 (`exa_trunc`/`exa_round`) and #201. Rejected: both issues are open and unimplemented, so the registry would be shaped around one consumer.
- **Rationale:** Two registrations are one line each at one call site. A registry earns its place only when a real third consumer shows the shape.
- **Consequences:** Whichever of #431 or #201 lands next MAY extract shared registration if duplication appears.
- **Promotes to ADR:** no

### [3] The adapter rewrites are deleted in this plan

- **Decision:** Delete `string_function_arg_type_guard`, `coerce_string_position_arg`, `StringPositionArgs`, `string_position_args`, `rewrite_decimal_stringifications`, `is_bare_decimal_column`, `wrap_decimal_to_varchar`, the `decimal_to_varchar_exasol` renderer arm, and `format_decimal_exasol_style` (interview Q3).
- **Alternatives:** Keep the rewrites beside the new path and remove them later. Rejected: both would wrap the same argument, which is the double-rewrite and text-match hazard the issue describes.
- **Rationale:** The renderer wrapping covers every surface the rewrites covered.
- **Consequences:** The renderer arm stays until the adapter stops producing the node, so task 4.2 depends on task 4.1. `apply_type_rewrites` keeps its name, signature, and callers with one pass, `like_subject_type_guard`. `column_exa_type` and `classify_exa_type` keep one consumer, and `ExaTypeClass::Decimal` is no longer a distinct branch anywhere. Narrowing the classifier is a separate cleanup.
- **Promotes to ADR:** no

### [4] The DataFusion dialect wraps boolean arguments like any other

- **Decision:** A boolean-producing string-converted argument renders as `exa_to_varchar(<arg>)` in every DataFusion-dialect arm, and a string CAST of a boolean renders `CAST(exa_to_varchar(<source>) AS VARCHAR)`. The #200 CASE rewrite stays in the Exasol dialect, byte-identical.
- **Alternatives:** (a) The #200 CASE form in every arm with no wrapper (the round-1 revision). Rejected: `exa_to_varchar` converts `Boolean`, so the CASE form would be a second owner of boolean text. (b) Keep the CASE form in the DataFusion `CAST` and `CONCAT` arms only. Rejected: two DataFusion rules for one conversion.
- **Rationale:** One DataFusion rule, and one owner of each type's text.
- **Consequences:** The three tests of `tests/boolean_to_string_casing_test.rs` move into `scan/to_varchar_tests.rs` with unchanged assertions, because their `SessionContext` needs the registered function.
- **Promotes to ADR:** no

### [5] exa_to_varchar covers every Arrow type the scan can produce

- **Decision:** Beyond integers, decimals, and dates, the function converts: `Float32`/`Float64` with the DOUBLE rule ([6]), a NaN to NULL, and an infinite value to an error. It converts `Boolean` to `TRUE`/`FALSE`, and a `Timestamp` of any unit to six fraction digits, truncated, with the time zone ignored. A JSON-fallback type (`needs_json_fallback`: out-of-domain `Decimal128`, nested types, `Binary`, `Time32`/`Time64`, `Float16`) converts to the text the scan emits for it. The `Null` type converts to NULL.
- **Alternatives:** The issue's "anything else → planning error" row with every `Decimal128` trimmed. Rejected: Exasol sees a `Decimal128(38, s)` column as the VARCHAR text `123.4500`, so trimming would disagree with the column's own returned value, and a working `CAST` over a `Binary` or `Time64` column would start failing.
- **Rationale:** A string function MUST see the text the column itself returns. NaN yields NULL because the raw scan emits a stored NaN as NULL (#246). An infinite value errors because Exasol's DOUBLE admits none. The timestamp truncation matches the captured `.9999999` → `.999999`, and the ignored time zone matches `scan/convert.rs`, which emits the epoch value as wall-clock time.
- **Consequences:** No E2E fixture carries a 37- or 38-digit decimal, so that row is covered by unit tests that compare against the emit path.
- **Promotes to ADR:** no

### [6] DOUBLE text follows Exasol's formatter and is gated by a live parity corpus

- **Decision:** Render `Float64` as Exasol does: at most 15 significant digits, fixed notation for a decimal exponent from -4 to 14, otherwise a bare lowercase `e` exponent. Accept the implementation only when every captured value matches native Exasol byte for byte, in unit tests and in the E2E parity test.
- **Alternatives:** C's `%.15g` with the exponent reformatted. Rejected: live captures on the Docker container differ from it. `999999999999999.5` prints `1000000000000000` where `%.15g` gives `1e+15`, and `CAST(1e-20 AS DOUBLE)`, `1e23`, and `1e-16` print `9.99999999999999e-21`, `9.99999999999999e22`, and `9.99999999999999e-17` where `%.15g` rounds to a power of ten.
- **Rationale:** Exasol's exact algorithm is not documented, so a captured corpus is the only falsifiable contract.
- **Consequences:** Task 3.5 carries `[expert]`. If a captured value cannot be reproduced, the implementer stops and reports rather than weakening the corpus.
- **Promotes to ADR:** no

### [7] INSTR and LOCATE beyond two arguments are a render error, and literals are not

- **Decision:** `INSTR`/`LOCATE` with more than two arguments are a DataFusion-dialect render error (issue #228, step 1). A string-converted literal of any type renders inside the wrapper.
- **Alternatives:** Also make fractional, exponent, and timestamp literals a render error (the prior design). Rejected: `exa_to_varchar` now converts `Float64` and `Timestamp`, so those literals no longer reach an unconvertible type.
- **Rationale:** A render error already routes every surface to native Exasol evaluation, and `strpos` cannot express a start position or an occurrence. Exasol pushes each `INSTR` call with the arguments as written: `INSTR(c_name, '0', 12)` arrives with three (`C_NAME`, `'0'`, `12`), a four-argument call with four, and a two-argument call with two. The rule covers the three- and four-argument forms, and ordinary two-argument `INSTR` keeps its pushdown.
- **Consequences:** `MAX(INSTR(c_name, '0', 12))` returns `12` instead of `10`. A single-group aggregate over such an argument moves from `SingleGroupAgg` to `RowScan`, and [8] keeps its empty-result row. A scalar-over-aggregate residual that uses `INSTR` with three arguments declines to the wrapper: slower, correct.
- **Promotes to ADR:** no

### [8] An ungrouped aggregate on the RowScan path returns one row when every file is pruned

- **Decision:** For a request with no GROUP BY whose select list is aggregates and that classifies as `RowScan`, `empty_result_sql` returns one row: NULL for each aggregate and `0` for each COUNT family aggregate, each cast to its declared `selectListDataTypes` type. A new delta of `vs-adapter/pushdown-planning-empty-result` states the rule. The `RowScan` arms of `empty_result_sql` otherwise keep `FROM DUAL WHERE 1=0`.
- **Alternatives:** Leave the empty path unchanged and accept zero rows. Rejected: an aggregate without GROUP BY returns one row on native Exasol. The bug already exists for shapes that reach `RowScan` today, for example `SELECT MAX(CAST(o_orderdate AS TIMESTAMP(4))), COUNT(*) FROM orders WHERE o_orderkey < 0` (zero rows on the VS, one row `NULL, 0` natively, captured live). The `INSTR` render error of [7] adds one more shape to it.
- **Rationale:** The empty single-group shape already produces this row (`empty_agg_sql`), so the rule is the single-group empty semantics applied to the shape the non-empty path answers through the qualified wrapper.
- **Consequences:** The fix closes the existing zero-row bug and keeps a fully pruned `MAX(INSTR(c_name, 'c', 2))` at one NULL row. The existing bug has no issue yet, so the implementing commit references the issue that task 1.3 opens for it. A grouped request on the wrapper path keeps zero rows.
- **Promotes to ADR:** no

### [9] Session NLS settings are one named trade-off, tracked by #216

- **Decision:** `exa_to_varchar` reproduces the default session settings only. DECIMAL and DOUBLE text depend on `NLS_NUMERIC_CHARACTERS`, DATE text on `NLS_DATE_FORMAT`, and TIMESTAMP text on `NLS_TIMESTAMP_FORMAT`. All four cite #216, widened to "session NLS formats: date, timestamp, numeric characters".
- **Alternatives:** Decline DOUBLE and TIMESTAMP for their session dependence while pushing DECIMAL and DATE. Rejected: DECIMAL depends on the session too (captured: `',.'` turns `0.5` into `0,5`), so the split had no consistent basis.
- **Rationale:** The pushdown request carries no session setting, so every session-dependent type has the same limit. Integer and BOOLEAN text depend on no setting (captured).
- **Consequences:** The #216 issue title and body need widening on GitHub (follow-up, not done by this plan).
- **Promotes to ADR:** no

### [10] A value Exasol types DECIMAL and DataFusion computes as Float64 is a named exception

- **Decision:** Such a value converts with the DOUBLE rule. The text equals Exasol's DECIMAL text for at most 15 significant digits and a magnitude from `1e-4` below `1e15`. Outside that range it diverges, and a new issue tracks it (`#TBD`, task 1.3). The range covers arithmetic with a fractional literal and Exasol division over operand pairs that Exasol types DECIMAL: over table columns, `i / 2` is `DECIMAL(19,1)` and `d / 2` (`d` is `DECIMAL(12,2)`) is `DECIMAL(13,3)`, while `i / 3` is `DOUBLE`. `ROUND` and `TRUNC` over a DECIMAL stay with #431.
- **Alternatives:** (a) Set `parse_float_as_decimal` to `true`. Rejected: it changes the arithmetic typing of every pushdown, which is outside this plan. (b) Render a string-converted fractional literal as a DECIMAL cast. Rejected: it covers a bare literal only, not arithmetic over one.
- **Rationale:** Live captures bound the range: `CAST(CAST(1.00 AS DECIMAL(12,2)) * 0.00001 AS VARCHAR(40))` returns `0.00001` and `1234567890123.45 * 1.5` returns `1851851835185.175`, where the DOUBLE rule gives `1e-5` and `1851851835185.17`. Division diverges the same way: `CAST(CAST(1.00 AS DECIMAL(12,2)) / 100000 AS VARCHAR(60))` returns `0.00001` natively and `1e-5` under the DOUBLE rule.
- **Consequences:** #223's scope does not cover this range, so #223 still closes.
- **Promotes to ADR:** no

### [11] A string argument simplifies away

- **Decision:** `ExaToVarcharUdf::simplify` returns the argument for a `Utf8`, `LargeUtf8`, or `Utf8View` input. `return_field_from_args` returns the argument's own data type and nullability for such an input.
- **Alternatives:** A pass-through at execution time only. Rejected: the call would stay in the plan and block the optimizer's view of the column.
- **Rationale:** DataFusion requires a simplified expression to keep the original schema, data type and nullability included (`datafusion-expr` 54.1.0, `ScalarUDFImpl::simplify` docs), so the return field mirrors the input.
- **Consequences:** A VARCHAR argument's optimized plan equals today's apart from output column names.
- **Promotes to ADR:** no

### [12] The Iceberg and Delta spec check is engaged and finds no deviation

- **Decision:** CLAUDE.md's compliance rule applies, because the feature changes pushdown and dispatches on primitive types. The quoted Primitive Types rows of both specs live in `datafusion-scan/scan-execution-exa-to-varchar` only, and the other deltas link to it.
- **Alternatives:** Record the rule as not applicable. Rejected: the conversion dispatches on the Arrow types those primitives map to, and the 37- and 38-digit decimal is a real Exasol target-type trade-off that MUST be named.
- **Rationale:** Neither spec defines a query text form for a value, so Exasol's conversion is the reference.
- **Promotes to ADR:** no

### [13] Tracked exceptions after this plan

- **Decision:** #227 and #223 close. #228 gets step 1 and stays open for the faithful three- and four-argument rendering. #216 widens to the numeric-characters and timestamp settings. The `Float64` value range gets a new issue, and the fully pruned ungrouped aggregate bug gets another (task 1.3). #201 and #431 stay open and are named as text exceptions: #201 returns `Timestamp(ns)` for `date_trunc` over a DATE, so `CAST(DATE_TRUNC('month', o_orderdate) AS VARCHAR(40))` yields `1996-01-01 00:00:00.000000` where Exasol yields `1996-01-01`, and #431 makes `ROUND`/`TRUNC` over a DECIMAL run as `Float64`.
- **Alternatives:** Keep #223 open for DOUBLE, BOOLEAN, and TIMESTAMP. Rejected: this plan converts all three.
- **Rationale:** Each remaining limit has exactly one issue.
- **Consequences:** The fix for #201 returns `Date32`, so `exa_to_varchar` needs no change for it. The new `Float64` issue excludes `ROUND`/`TRUNC` (#431). The implementing commit uses `Closes #227`, `Closes #223`, `Closes` for the pruned-aggregate issue, and `Refs #228, #216, #201, #431`.
- **Promotes to ADR:** no

### [14] Background copies in features this plan does not own stay exact apart from drifted bullets

- **Decision:** In `pushdown-col-types-consolidation`, `pushdown-module-dedup-consolidation`, `pushdown-planning-like-type-coercion`, `pushdown-planning-join-filter-type-coercion`, `vs-expression-translator-concat`, and `vs-expression-translator-scalar-ops`, the `DELTA:CHANGED` Background is the recorded text, edited only in the bullets this plan makes inaccurate. Supersedes the earlier decision "Background edits in features this plan does not own stay limited to drifted bullets and their chains".
- **Alternatives:** Collapse the `SUPERSEDES` chains in those Backgrounds (the earlier decision). Rejected: shortening those Backgrounds belongs in a separate cleanup, and an exact copy limits the permanent change to the drifted bullets.
- **Rationale:** A `DELTA:CHANGED` Background replaces the whole section on record, so every other byte must match the recorded text.
- **Consequences:** `pushdown-planning-join-fallback`'s Background shows a leg fragment as `CAST(<col> AS VARCHAR) LIKE …`. The node it describes is unchanged and the fragment is illustrative, so this plan leaves that feature untouched.
- **Promotes to ADR:** no

### [15] One surface scenario replaces the per-feature pushdown scenarios

- **Decision:** `sql-comprehension/vs-expression-translator-string-conversion` carries one scenario, "One node renders one text on every DataFusion surface", for GROUP BY keys, grouped ORDER BY, aggregate arguments, `COUNT(DISTINCT)`, HAVING, scalar-over-aggregate items, and the empty path. `pushdown-planning-decimal-string-format` keeps only its GROUP BY and aggregate scenario and links to `scan-execution-exa-to-varchar` for the trim.
- **Alternatives:** One scenario per aggregate feature (`grouped-agg-multikey`, `expression-aggregate`, both scalar-over-aggregate features). Rejected: each restated the same property.
- **Rationale:** The property belongs to the renderer's determinism, and each aggregate feature's own rules are unchanged.
- **Consequences:** Those four aggregate features have no delta in this plan.
- **Promotes to ADR:** no

### [16] The #227 E2E tests reuse the typed probe and the dim_customer fixtures

- **Decision:** Add the #227 E2E tests to `crates/lakehouse-engine/tests/e2e_capability_test.rs` over `typed_distinct_probe`: `ID` (Iceberg `long`) stands for `c_custkey`, `C_DECIMAL_A` (`DECIMAL(9,2)`) for `c_acctbal`, and `C_DOUBLE`, `C_BOOL`, `C_TS` for the other types. The `INSTR` repro uses `dim_customer.C_NAME` (`customer-01` ... `customer-05`), where `INSTR(C_NAME, 'c', 2)` is `0` natively and `1` as a start-less `strpos`. Expected values come from independent oracles: `TYPED_DECIMAL_A_UNSCALED`, `exasol_trim_decimal_string`, and in-session native queries.
- **Alternatives:** A TPC-H `customer` fixture with `c_acctbal`. Rejected: the local seed has no `c_acctbal`, and the two fixtures already carry every needed type.
- **Rationale:** The Makefile `test-e2e` target already runs this file, and its oracles are independent of production code.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] Boolean-producing string-converted arguments had two conflicting renderings

- **Finding:** Round 1 BLOCKER ([REQUIREMENT_CONFLICT] [AMBIGUOUS_REQUIREMENT]). The first string-conversion scenario wrapped every string-converted argument in `exa_to_varchar`, and the literal scenario required the CASE form for `literal_bool`, so `UPPER(TRUE)` had two required renderings.
- **Direction change:** Superseded by decision [4]: `exa_to_varchar` converts `Boolean`, so the DataFusion dialect wraps every string-converted argument, and the conflict no longer exists.
- **Promotes to ADR:** no

### [plan-review] Background bullets backed no scenario

- **Finding:** Round 1 BLOCKER ([IMPLEMENTATION_LEAKAGE]). The `exa-to-varchar` NULL bullet named Iceberg v3 `unknown` and Delta `void` sources that no scenario or fixture exercises. The string-conversion bullet on `EXA_TO_VARCHAR_FN` backed no scenario in that spec.
- **Direction change:** The NULL bullet states only that a NULL literal reaches the function as the Arrow `Null` type. The `EXA_TO_VARCHAR_FN` bullet moved into `scan-execution-exa-to-varchar`'s Background, beside the registration scenario that reads the constant.
- **Promotes to ADR:** no
