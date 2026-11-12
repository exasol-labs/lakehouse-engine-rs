# Tasks: add-capabilities-exclusion-property

## Phase 2: Implementation (Group A: Property catalog and capability exclusion)
- [x] 1.1 Capability exclusion function and `get_capabilities_response(Option<&str>)` in `adapter/capabilities.rs`
- [x] 1.2 `EXCLUDED_CAPABILITIES` in `PROPERTY_NAMES`, accessor, and `TuningChecker` rule in `adapter/properties.rs`
- [x] 1.3 `dispatch` answers `getCapabilities` from the persisted property
- [x] 1.4 Unit tests in `capabilities_tests.rs`
- [x] 1.5 Unit tests in `properties_tests.rs` and `adapter_tests.rs`
- [x] 1.6 Move `PERSISTING_ADAPTER_BODY` and `STUB_ADAPTER_SCHEMA` into `e2e_local/fixtures.rs` with a persist helper
- [x] 1.7 E2E in `e2e_local/property_validation.rs`

## Phase 2: Implementation (Group B: Exclusion behavior through Exasol)
- [x] 2.1 `e2e_local/capability_exclusion.rs` tests, move `JOIN_ORACLE` and `copy_join_tables_natively` to fixtures
- [x] 2.2a Run 2.1 against live stack, record failing request shapes in `notes/`
- [x] 2.2b Fix recorded shapes in the pushdown planner with unit tests [expert]

## Phase 2: Implementation (Group C: Documentation)
- [x] 3.1 `docs/tuning.md` row
- [x] 3.2 `docs/capabilities.md` section
- [x] 3.3 `docs/capability-matrix.md` intro
- [x] 3.4 `docs/semantic-differences.md` ORDER BY with LIMIT section

## Phase 4: Review Fixes
- [x] 4.6 `properties.rs`: extract `unadvertised_capability_entry` below `unknown_entry` and use it in `TuningChecker::excluded_capabilities`
- [x] 4.7 `properties.rs`: update the module doc and the `check_properties` doc for the `EXCLUDED_CAPABILITIES` rule
- [x] 4.8 `properties_tests.rs`: rewrite `an_excluded_capability_is_suggested_within_two_edits_only` to pin the two-edit and three-edit boundary
- [x] 4.9 `properties_tests.rs`: delete the six added `/// Scenario:` lines that have E2E proofs
- [x] 4.10 `capabilities_tests.rs`: fold the five repeated-body tests into one case table and delete `full_list`
- [x] 4.11 `capabilities_tests.rs`: delete the added `/// Scenario:` lines that have E2E proofs
- [x] 4.12 `adapter_tests.rs`: delete `get_capabilities_withholds_the_persisted_exclusion_and_ignores_an_unadvertised_entry`
- [x] 4.13 `adapter_tests.rs`: extract `assert_every_schema_request_fails_before_the_connection_is_read` and use it in the three request-type tests
- [x] 4.14 `adapter_tests.rs`: delete the `/// Scenario:` line above the unadvertised-capability request-type test
- [x] 4.15 `fixtures.rs`, `capability_exclusion.rs`, `capability.rs`: share one `EXPLAIN VIRTUAL` echo parser (`explain_virtual_messages`, `echoed_pushdown_requests`)
- [x] 4.16 `docs/capabilities.md`: name `LIMIT_WITH_OFFSET` in the ORDER BY warning

## Phase 3: Verification
- [x] 4.1 `make cross-udf-build`
- [x] 4.2 `cargo test --workspace --lib`
- [x] 4.3 `make e2e-up test-e2e`
- [x] 4.4 `make lint`
- [x] 4.5 `make fmt-check`
