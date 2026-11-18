# Tasks: fix-string-function-integer-args

- [x] 1. Unit test `string_function_numeric_argument_rounds_to_int64` (RED), then wrap DataFusion's `lpad`, `rpad`, `repeat`, `left`, `right`, `substr`, `chr` in the scan session (`scan/string_argument.rs`, D7) (GREEN). Scenario: A string function's numeric argument rounds to an integer as Exasol does.
- [x] 2. Unit test `string_function_numeric_argument_outside_exasols_range_fails` (RED), then the ±999,999,999 range check with `numeric_overflow` recorded on the session (GREEN). Scenario: A string function's numeric argument outside Exasol's range fails the query.
- [x] 2b. Unit test `negative_count_or_length_returns_null_or_refuses_repeat` (RED: `rpad` panics), then NULL for a negative pad/LEFT/RIGHT count or SUBSTR length and Exasol's REPEAT error (GREEN). Scenario: A negative count or length makes a string function return NULL as Exasol does.
- [x] 3. E2E `string_function_numeric_arguments_match_native` in `text_conversion.rs`: the eight functions with arithmetic, DECIMAL, and DOUBLE arguments in the select list, a filter, a GROUP BY key, and an aggregate argument, plus negative counts and lengths and out-of-range and negative-REPEAT error cases, against the native copy.
- [x] 4. Live check of the reproduction from #604 against the Docker Exasol before and after the fix (manual testing rows).
- [x] 5. Code review, verification checklist, verification report.
