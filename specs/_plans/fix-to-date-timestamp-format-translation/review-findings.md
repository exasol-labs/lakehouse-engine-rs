# Code Review Findings: fix-to-date-timestamp-format-translation

## Summary
- Files reviewed: 6
- Total findings: 8 (standard: 8, expert: 0)

## Standard fixes

### crates/vs-expression/src/datetime_format.rs

#### [BOOLEAN_FLAG_PARAMETER] The fraction's leading dot is a bool field on every element token
- Location: lines 18-21 (`Token`), 47-81 (`next_token`), 89-94 (`count_elements`), 116-128 (`strftime`)
- Issue: `Token::Element(Element, bool)` carries a `dotted` flag that only `Element::Fraction` uses. Every other element sets it to `false`, and `strftime` ignores it in six of its eight arms. The flag picks the branch between `%.{w}f` and `%{w}f`. The fraction also has its own parsing branch in `next_token`, with the token length computed as `usize::from(dotted) + 3`, where `3` is a bare literal for `"FF".len() + 1`. The element-to-strftime mapping is therefore split across `next_token`'s local `ELEMENTS` table, the `FF` branch, and `strftime`.
- Fix: In crates/vs-expression/src/datetime_format.rs, make these changes. (a) Change `Element::Fraction(u8)` to the unit variant `Element::Fraction`. (b) Change `Token::Element(Element, bool)` to `Token::Element(Element, &'static str)`, where the string is the strftime form. (c) Move `ELEMENTS` to module scope as `const ELEMENTS: [(&str, Element, &str); 12]` with these rows in this order: `.FF3`→`%.3f`, `.FF6`→`%.6f`, `.FF9`→`%.9f`, `FF3`→`%3f`, `FF6`→`%6f`, `FF9`→`%9f` (all `Element::Fraction`), then `HH24`→`%H` (`Hour`), `YYYY`→`%Y` (`Year`), `MM`→`%m` (`Month`), `MI`→`%M` (`Minute`), `DD`→`%d` (`Day`), `SS`→`%S` (`Second`). (d) Rewrite `next_token` to return `Option<(Token, &str)>`, where the `&str` is the remaining input. It loops over `ELEMENTS` with `strip_prefix_ignore_case` and returns the first match. Otherwise it falls back to a `SEPARATORS` character. Delete the `strip_prefix('.')` branch, the `FF` digit match, and the `usize::from(dotted) + 3` arithmetic. (e) In `tokenize`, assign the returned remainder to `rest`. (f) Count fractions with `count_of(Element::Fraction)` and delete `count_elements`. (g) Make `strftime` return the token's stored pattern or the separator character. Run `cargo test -p vs-expression datetime_format`. `translates_supported_formats` and `declines_unsupported_formats` must pass unchanged.

#### [MISSING_DOC_COMMENT] `FormatTarget` has no doc comment
- Location: lines 1-5
- Issue: `FormatTarget` is a `pub(crate)` type that `lib.rs` passes into `chrono_format`. It has no doc comment, so a caller cannot tell that the variant decides which time elements a format may hold.
- Fix: In crates/vs-expression/src/datetime_format.rs, add this one-line doc comment above `FormatTarget`: `/// The conversion that parses with the format. Only \`Timestamp\` admits time elements.`

#### [IMPLEMENTATION_IN_NAME] `chrono_format` names the parser's crate instead of the result
- Location: line 27
- Issue: The function name refers to `chrono`, the date library inside DataFusion. The function's doc comment, the spec delta, and the decision log all call its result a strftime pattern. The code therefore uses two names for the same thing, and one of them is a vendor name.
- Fix: Rename `chrono_format` to `strftime_pattern` with Serena `rename_symbol` in crates/vs-expression/src/datetime_format.rs. The rename must update the `use` line and the call in crates/vs-expression/src/lib.rs, and both calls in crates/vs-expression/src/datetime_format_tests.rs.

### crates/vs-expression/src/datetime_format_tests.rs

#### [MISSING_BOUNDARY_TEST] No case for a bare `FF` or a multibyte character in the format
- Location: `declines_unsupported_formats`, lines 63-87
- Issue: Decision-log [3] states that a bare `FF` declines. No case covers it, so the `digits.bytes().next()?` path in `next_token` has no test. No case contains a non-ASCII character either. `tokenize` slices the input by byte length, so a multibyte character is the input that would expose a slicing panic. A scratch copy of the module returns `None` for `YYYY-MM-DD HH24:MI:SS.FF`, `YYYY-MM-DD HH24:MI:SSFF`, and `YYYY年MM月DD日`, so the behavior is correct but nothing pins it.
- Fix: In crates/vs-expression/src/datetime_format_tests.rs `declines_unsupported_formats`, add the cases `("YYYY-MM-DD HH24:MI:SS.FF", FormatTarget::Timestamp)`, `("YYYY-MM-DD HH24:MI:SSFF", FormatTarget::Timestamp)`, and `("YYYY年MM月DD日", FormatTarget::Date)`. Raise the array length from 14 to 17. Run `cargo test -p vs-expression declines_unsupported_formats`.

### crates/vs-expression/src/lib.rs

#### [TOO_MANY_ARGUMENTS] `render_to_datetime` takes 4 arguments, two of which always travel as a fixed pair
- Location: line 247 (`render_to_datetime`), call sites at lines 1101 and 1103
- Issue: `render_to_datetime(function, target, args, dialect)` has 4 parameters. `function` and `target` are always the pairs `("to_date", Date)` and `("to_timestamp", Timestamp)`, so `function` is derivable from `target`. Because `function` is the DataFusion name, every error message now names a `function_scalar to_date` node. The node Exasol sent is `TO_DATE`, and the replaced arms named it correctly.
- Fix: In crates/vs-expression/src/lib.rs, remove the `function: &str` parameter from `render_to_datetime` so that it takes `(target, args, dialect)`. At the top of the function, bind `let (node, function) = match target { FormatTarget::Date => ("TO_DATE", "to_date"), FormatTarget::Timestamp => ("TO_TIMESTAMP", "to_timestamp") };`. Render the DataFusion call with `function`. Use `node` in every `UdfError::User` message of `render_to_datetime` and as the first argument of both `render_conversion_source` calls. Change the call sites in `render_expression_inner` to `render_to_datetime(FormatTarget::Date, args, dialect)` and `render_to_datetime(FormatTarget::Timestamp, args, dialect)`, and collapse the `"TO_TIMESTAMP"` arm to a single line. Run `cargo test -p vs-expression to_date`.

#### [CONTEXTLESS_ERROR] The format decline error does not name the format that failed
- Location: line 272
- Issue: The message `function_scalar {function} format is not a translatable string literal` states neither the format argument that failed nor which constraint it broke. The constraints are a non-`literal_string` node type and a model with no exact strftime equivalent. A decline reason in a log cannot be traced to the query's format model.
- Fix: In crates/vs-expression/src/lib.rs `render_to_datetime`, change the message to `format!("function_scalar {node} format {format} is not a string literal with an exact strftime equivalent")`. `{node}` is the Exasol node name `TO_DATE` or `TO_TIMESTAMP` from the [TOO_MANY_ARGUMENTS] fix, and `{format}` is the format argument's JSON value.

#### [INFORMATION_LEAKAGE] The translated pattern is interpolated into SQL without escaping
- Location: line 277
- Issue: `render_to_datetime` writes `'{pattern}'` into the DataFusion SQL without escaping. The output is well-formed only because `SEPARATORS` in `datetime_format.rs` contains no single quote. Two modules therefore depend on one fact, and nothing enforces it. A later change that admits quoted literal text in format models, such as `"T"`, would produce broken SQL. Every other string literal in this file is rendered through `sql_escape`.
- Fix: In crates/vs-expression/src/lib.rs `render_to_datetime`, render the pattern as `sql_escape(&pattern)` inside the quotes of the two-argument `format!`. Run `cargo test -p vs-expression to_date`. The rendered output must not change.

### crates/lakehouse-engine/tests/e2e_capability_test.rs

#### [UNTESTED_ERROR_PATH] The rejected-input test accepts any query failure
- Location: `e2e_to_date_translated_format_fails_on_rejected_input`, lines 3568-3582
- Issue: The three virtual-schema queries call `query_error` and discard the error text it returns. The test therefore passes for any failure, for example broken SQL rendered on the `COUNT(*)` filter path, which no other new test covers. It does not prove that the scan's parser rejected the string under the translated pattern, which is what the scenario requires. `assert_query_fails` in `common::e2e_harness` already checks error fragments.
- Fix: In crates/lakehouse-engine/tests/e2e_capability_test.rs `e2e_to_date_translated_format_fails_on_rejected_input`, replace the three `query_error` calls on `vs_table()` queries with `assert_query_fails(&mut conn, <sql>, <fragments>)`. Use `&["event-01", "%Y-%m-%d"]` for the select-list query and the `COUNT(*)` filter query. Use `&["2024-01-13", "%Y-%d-%m"]` for the `id = 13` query. Before you commit, run each query against the Docker Exasol with `exapump sql` and set the fragments to text that appears in the actual error. Keep the native-oracle `query_error` call and the `assert_pushed_into_scan` check.

## Expert fixes
[none]
