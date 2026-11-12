# Code Review Findings: add-capabilities-exclusion-property

## Summary
- Files reviewed: 15
- Total findings: 11 (standard: 11, expert: 0)
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` (the `make lint` command) reports no issues on the working tree.

## Standard fixes

### crates/lakehouse-engine/src/adapter/properties.rs

#### [MIXED_ABSTRACTION_LEVEL] `TuningChecker::excluded_capabilities` formats each entry inline
- Location: lines 244-265
- Issue: The method reads the raw value, splits it, and records the result in `self.invalid`. It also builds each message: it matches on `nearest_name(entry, CAPABILITIES)`, formats the `(did you mean ...)` suffix, and calls `invalid_entry`. The unknown-name path already keeps this formatting in its own function, `unknown_entry`, so the two message builders sit at different levels of abstraction.
- Fix: In `crates/lakehouse-engine/src/adapter/properties.rs`, add a private free function `fn unadvertised_capability_entry(entry: &str) -> String` directly below `unknown_entry`. Move the `nearest_name(entry, CAPABILITIES)` match and the `invalid_entry(PROP_EXCLUDED_CAPABILITIES, entry, &expected)` call into it. In `TuningChecker::excluded_capabilities`, replace the `.map(|entry| { ... })` closure with `.map(unadvertised_capability_entry)`. Run `cargo test --workspace --lib properties` and confirm that every test passes unchanged.

#### [OUTDATED_COMMENT] Module and `check_properties` docs omit the new value rule
- Location: lines 1-2 (module doc) and lines 103-104 (`check_properties` doc)
- Issue: The module doc says the module holds "the value rule of each tuning property", and the `check_properties` doc says it rejects "every invalid tuning or table pattern value". Since this change, `check_properties` also rejects each unadvertised `EXCLUDED_CAPABILITIES` entry, which is neither a tuning value nor a table pattern.
- Fix: In `crates/lakehouse-engine/src/adapter/properties.rs`, change the module doc's second line to `//! rule of each tuning property and of EXCLUDED_CAPABILITIES.` (keep the first line). Change the `check_properties` doc's first sentence to `/// Rejects every unknown property name, every invalid tuning or table pattern value, and every unadvertised EXCLUDED_CAPABILITIES entry in one error.` and keep the line width at 100 characters.

### crates/lakehouse-engine/src/adapter/properties_tests.rs

#### [MISSING_BOUNDARY_TEST] The "no suggestion beyond two edits" assertion passes regardless of the rule
- Location: lines 502-516, `an_excluded_capability_is_suggested_within_two_edits_only`
- Issue: The test intends `FN_PRED_` to get no suggestion. `FN_PRED_` is two edits from `FN_PRED_OR` (two insertions), so the adapter appends `(did you mean 'FN_PRED_OR'?)`. The same holds for `FN_PRED_L`. The test still passes, because it checks `msg.contains(&excluded_capabilities_entry("FN_PRED_", None))`, and the entry without a suggestion is a prefix of the entry with one. No assertion pins the boundary at three edits, so a change of `SUGGESTION_MAX_EDITS` to 3 would also pass. Evidence: a Python copy of `nearest_name` (uppercase fold, Levenshtein distance, at most 2 edits, alphabetical tie-break) over the 124 names of `CAPABILITIES` returns `FN_PRED_OR` at distance 2 for both `FN_PRED_` and `FN_PRED_L`.
- Fix: In `crates/lakehouse-engine/src/adapter/properties_tests.rs`, rewrite the body of `an_excluded_capability_is_suggested_within_two_edits_only` to `assert_eq!(excluded_capabilities_message("FN_PRED_LIKEXY, FN_PRED_LIKEXYZ"), format!("invalid virtual schema properties: {}; {}", excluded_capabilities_entry("FN_PRED_LIKEXY", Some("FN_PRED_LIKE")), excluded_capabilities_entry("FN_PRED_LIKEXYZ", None)));`. `FN_PRED_LIKEXY` is exactly two edits from `FN_PRED_LIKE`. `FN_PRED_LIKEXYZ` is exactly three edits from `FN_PRED_LIKE`, and every other advertised name is five or more edits away from it. Run `cargo test --workspace --lib an_excluded_capability_is_suggested_within_two_edits_only` and confirm that it passes.

#### [REDUNDANT_COMMENT] Unit tests carry `/// Scenario:` lines for scenarios that have E2E proofs
- Location: lines 486, 502, 518, 532, 555, 567
- Issue: `specs/testing.md` § Coverage rule states: "A unit test carries the line only when it is the scenario's one proof." Each of the six tests carries the line for "Create, refresh, and set reject an entry the adapter does not advertise" or "One error reports every unknown name and every invalid value". `e2e_local/property_validation.rs` (`create_and_set_reject_an_unadvertised_capability`) and `e2e_local/capability_exclusion.rs` (`get_capabilities_ignores_a_persisted_unadvertised_entry`, `whitespace_and_empty_entries_are_accepted`) prove both scenarios. The file's existing unit tests carry no `/// Scenario:` line, consistent with that rule.
- Fix: In `crates/lakehouse-engine/src/adapter/properties_tests.rs`, delete the `/// Scenario:` line above `excluded_capability_entries_name_each_unadvertised_entry`, `an_excluded_capability_is_suggested_within_two_edits_only`, `excluded_capability_entries_echo_under_the_echo_rule`, `excluded_capability_entries_share_the_alphabetical_slot_of_the_property`, `every_advertised_capability_is_accepted_alone_and_all_together`, and `excluded_capability_entries_accept_whitespace_and_empty_entries`. Keep the test names.

### crates/lakehouse-engine/src/adapter/capabilities_tests.rs

#### [SHRINKABLE] Five tests repeat one body over different values
- Location: lines 601-654 (`excluded_capabilities_are_withheld_in_advertised_order`, `the_order_of_the_value_does_not_reorder_the_response`, `matching_is_exact_and_case_sensitive`, `an_unknown_entry_is_ignored`, `an_absent_or_empty_value_gives_the_full_list`)
- Issue: Each test calls `advertised_names(value)` and compares the result with the full list less a fixed set of names. `specs/testing.md` § Test code states: "Variants of one scenario are rows of one case table, not copies of one test body." `the_order_of_the_value_does_not_reorder_the_response` also compares two outputs of the function under test with each other, so it would pass if both outputs were wrong in the same way.
- Fix: In `crates/lakehouse-engine/src/adapter/capabilities_tests.rs`, rewrite `excluded_capabilities_are_withheld_in_advertised_order` as one case table of `(Option<&str>, &[&str])` rows (the value, then the names it withholds): `(Some(" FN_PRED_LIKE ,, FN_AGG_COUNT_DISTINCT,"), &["FN_PRED_LIKE", "FN_AGG_COUNT_DISTINCT"])`, `(Some("FN_AGG_COUNT_DISTINCT,FN_PRED_LIKE"), &["FN_PRED_LIKE", "FN_AGG_COUNT_DISTINCT"])`, `(Some("fn_pred_like"), &[])`, `(Some("FN_PRED_LIK"), &[])`, `(Some("NO_SUCH_CAPABILITY, FN_PRED_LIKE"), &["FN_PRED_LIKE"])`, `(None, &[])`, `(Some(""), &[])`, `(Some(" , "), &[])`, `(Some(",,"), &[])`. For each row, assert `advertised_names(value) == full_list_without(withheld)` with `{value:?}` in the failure message. Delete `the_order_of_the_value_does_not_reorder_the_response`, `matching_is_exact_and_case_sensitive`, `an_unknown_entry_is_ignored`, `an_absent_or_empty_value_gives_the_full_list`, and the then-unused helper `full_list`. Keep `an_exclusion_withholds_only_the_listed_names`, because the plan's Scenario Coverage table names it. Run `cargo test --workspace --lib capabilities` and confirm that every test passes.

#### [REDUNDANT_COMMENT] Unit tests carry `/// Scenario:` lines for scenarios that have E2E proofs
- Location: lines 601, 612, 621, 628, 637, 647
- Issue: `specs/testing.md` § Coverage rule states: "A unit test carries the line only when it is the scenario's one proof." "An excluded capability is withheld from getCapabilities" has the E2E proof `whitespace_and_empty_entries_are_accepted`, and "getCapabilities ignores an entry the adapter does not advertise" has the E2E proof `get_capabilities_ignores_a_persisted_unadvertised_entry`, both in `e2e_local/capability_exclusion.rs`.
- Fix: In `crates/lakehouse-engine/src/adapter/capabilities_tests.rs`, delete every `/// Scenario:` line that this change added: the lines above `excluded_capabilities_are_withheld_in_advertised_order` and `an_exclusion_withholds_only_the_listed_names`, and the lines above any test that the [SHRINKABLE] fix in this file did not delete. Leave the existing lines above `reports_audited_capability_set` and `reports_supported_aggregate_capabilities` unchanged.

### crates/lakehouse-engine/src/adapter/adapter_tests.rs

#### [DUPLICATE_TEST] `get_capabilities_withholds_the_persisted_exclusion_and_ignores_an_unadvertised_entry` repeats a behavior proved at two other layers
- Location: lines 183-204
- Issue: The test sends a synthesized `getCapabilities` request with `EXCLUDED_CAPABILITIES = 'NO_SUCH_CAPABILITY, FN_PRED_LIKE'` and checks that `FN_PRED_LIKE` is withheld. The E2E test `get_capabilities_ignores_a_persisted_unadvertised_entry` sends the same value through a live Exasol, and the unit test `an_unknown_entry_is_ignored` (or its row after the case-table fix in `capabilities_tests.rs`) pins the pure logic. `specs/testing.md` § Testing strategy states: "Each behavior is tested at one layer", and "A unit test over a synthesized request proves a state that no well-formed Exasol request can reach." This request is well formed, and the plan's task 1.5 does not list the test.
- Fix: In `crates/lakehouse-engine/src/adapter/adapter_tests.rs`, delete `get_capabilities_withholds_the_persisted_exclusion_and_ignores_an_unadvertised_entry` together with its `/// Scenario:` line. Remove any import that becomes unused, then run `cargo clippy --workspace --all-targets --all-features -- -D warnings` and confirm that it reports no warning.

#### [SHRINKABLE] Third copy of the request-type loop
- Location: lines 82-104, 130-151, 154-181
- Issue: `an_unknown_property_name_fails_every_schema_request_before_the_connection_is_read`, `an_invalid_tuning_value_fails_every_schema_request_before_the_connection_is_read`, and the new `an_unadvertised_excluded_capability_fails_every_schema_request_before_the_connection_is_read` repeat one body. Each test loops over the three request types, builds `schema_request` with `PROP_CATALOG_CONNECTION: "NO_SUCH_CONNECTION"` plus one property, dispatches the request, and checks the message. This change wrote the third copy. `/speq:code-guardrails` extracts duplication on the third occurrence, and `specs/testing.md` § Test code requires the change that writes another copy of an assertion helper to extract it. The tuning-value copy also lacks the `NO_SUCH_CONNECTION` assertion that its name promises.
- Fix: In `crates/lakehouse-engine/src/adapter/adapter_tests.rs`, add `#[track_caller] fn assert_every_schema_request_fails_before_the_connection_is_read(property: &str, value: &str, expected_entry: &str)`. For each of `createVirtualSchema`, `refresh`, and `setProperties`, it builds `schema_request(request_type, json!({ PROP_CATALOG_CONNECTION: "NO_SUCH_CONNECTION", property: value }))`, dispatches the request with `TestContext::scalar(vec![])`, expects an error, and asserts that the message contains `expected_entry` and does not contain `NO_SUCH_CONNECTION`, naming the request type in each failure message. Replace the body of each of the three tests with one call: `("NAMESPCE", "sales", "unknown property 'NAMESPCE' (did you mean 'NAMESPACE'?)")`, `(PROP_DF_BATCH_SIZE, "0", "invalid value '0' for 'DATAFUSION_BATCH_SIZE': expected a whole number from 1 to 4294967295")`, and `("EXCLUDED_CAPABILITIES", "FN_PRED_LIK, FN_PRED_LIKE", "invalid value 'FN_PRED_LIK' for 'EXCLUDED_CAPABILITIES': expected an advertised capability name (did you mean 'FN_PRED_LIKE'?)")`. Keep the three test names. Run `cargo test --workspace --lib fails_every_schema_request_before_the_connection_is_read` and confirm that the three tests pass.

#### [REDUNDANT_COMMENT] Unit test carries a `/// Scenario:` line for a scenario that has E2E proofs
- Location: line 154
- Issue: `specs/testing.md` § Coverage rule states: "A unit test carries the line only when it is the scenario's one proof." `create_and_set_reject_an_unadvertised_capability` and `get_capabilities_ignores_a_persisted_unadvertised_entry` prove "Create, refresh, and set reject an entry the adapter does not advertise" end to end. The two sibling request-type tests carry no `/// Scenario:` line.
- Fix: In `crates/lakehouse-engine/src/adapter/adapter_tests.rs`, delete the `/// Scenario: Create, refresh, and set reject an entry the adapter does not advertise` line above `an_unadvertised_excluded_capability_fails_every_schema_request_before_the_connection_is_read`.

### crates/lakehouse-e2e/tests/e2e_local/capability_exclusion.rs

#### [INFORMATION_COUPLING] Second parser of the `EXPLAIN VIRTUAL` echo format
- Location: lines 47-69 (`echoed_messages`, `echoed_requests`); first copy in `crates/lakehouse-e2e/tests/e2e_local/capability.rs` line 1886 (`explain_virtual_pushdown_request`)
- Issue: Both modules run `EXPLAIN VIRTUAL`, parse each JSON array in the result, and look up the `pushdownRequest` messages. A change of the echo format would need an edit in both modules. `specs/testing.md` § Test code states: "A helper that a second test file needs moves into its area's support module", and the change that writes the second copy extracts it.
- Fix: Move `echoed_messages` from `crates/lakehouse-e2e/tests/e2e_local/capability_exclusion.rs` into `crates/lakehouse-e2e/tests/e2e_local/fixtures.rs` as `pub fn explain_virtual_messages(conn: &mut ExaConn, query: &str) -> Vec<Vec<Value>>`, with its doc comment and its `PUSHDOWN_JSON_COLUMN` constant. Add `pub fn echoed_pushdown_requests(conn: &mut ExaConn, query: &str) -> Vec<Value>` beside it, which returns the `pushdownRequest` of each row of `explain_virtual_messages`. In `capability_exclusion.rs`, call `explain_virtual_messages` from `echoed_exchange`, and build `echoed_requests` from `echoed_pushdown_requests`. In `crates/lakehouse-e2e/tests/e2e_local/capability.rs`, rewrite the body of `explain_virtual_pushdown_request` to take the first element of `echoed_pushdown_requests(conn, sql)` and keep its panic message for an empty result. Run `cargo clippy --workspace --all-targets --all-features -- -D warnings`, then `make e2e-up test-e2e`, and confirm that both report no failure.

### docs/capabilities.md

#### [OUTDATED_COMMENT] The ORDER BY warning names only `LIMIT`
- Location: line 166 (last paragraph of `## Excluding a capability`)
- Issue: The paragraph says "Excluding `LIMIT` while an `ORDER_BY_*` capability stays advertised changes the result of `ORDER BY ... LIMIT n OFFSET m`." Decision-log entry [1], `docs/tuning.md`, and `docs/semantic-differences.md` state that excluding `LIMIT` or `LIMIT_WITH_OFFSET` has this effect. The plan's manual test uses `EXCLUDED_CAPABILITIES = 'LIMIT_WITH_OFFSET'` as the example, and this page does not warn about that case.
- Fix: In `docs/capabilities.md`, change "Excluding `LIMIT` while an `ORDER_BY_*` capability stays advertised" to "Excluding `LIMIT` or `LIMIT_WITH_OFFSET` while an `ORDER_BY_*` capability stays advertised".

## Expert fixes
[none]
