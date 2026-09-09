# Feature: Adapter Module Structure

This is the adapter root's structural feature, the sibling of the pushdown and joins structural features. It exists because the duplication it removes spans several behavioral features at once, so no single behavioral feature can own it without leaking a structural decision across a boundary.

## Background

<!-- DELTA:NEW -->
* Seven `UdfContext` doubles across `adapter_tests.rs`, `connection_tests.rs`, and `unity_schema_tests.rs` repeat the same four required trait methods. `exasol-udf-sdk` 0.24.0 provides SDK-maintained test doubles as replacements.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: No hand-rolled UdfContext doubles in adapter tests

* *GIVEN* the seven `UdfContext` doubles in the adapter test modules
* *WHEN* the adapter unit-test suite compiles and runs
* *THEN* no file under `crates/lakehouse-engine/src/adapter/` SHALL declare an `impl UdfContext` (bare or fully-qualified form)
* *AND* every existing adapter test MUST pass with no change to any assertion or expected value
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: The node-count fallback test asserts the trait default

* *GIVEN* `cluster_nodes_from_context_defaults_to_one_when_node_count_zero`, which drives the `0 → 1` fallback through the `UdfContext` trait default `node_count` of 0
* *WHEN* the hand-rolled double is replaced
* *THEN* the replacement MUST NOT override `node_count`, so the test keeps asserting the trait behavior rather than a double's return value
<!-- /DELTA:NEW -->
