# Feature: Adapter Module Structure

Each repeated read-a-JSON-field shape in the adapter-root modules (`adapter/mod.rs`, `adapter/connection.rs`) has exactly one implementation. Behavior is unchanged: this feature constrains where the code lives, not what the adapter returns.

## Background

* Every scenario of `vs-adapter/create-virtual-schema`, `vs-adapter/create-virtual-schema-adapter-notes`, `vs-adapter/create-virtual-schema-adapter-notes-resources`, `vs-adapter/refresh-and-set-properties`, `connection/connection-credentials`, and `connection/connection-credentials-catalog-auth` stays accurate. Those suites are the characterization gate for "behavior unchanged".
* `adapter::connection` is a child module of `adapter`, so it can name a private item of `adapter`. A shared helper stays private to `adapter`.
* A helper whose whole body calls another helper with the same arguments is a pass-through. The scenarios delete the single-purpose names instead of keeping pass-throughs.
* `Json` in `adapter/mod.rs` aliases `serde_json::Value`.
* The adapter has eleven `resolve_*` property readers. Only one pair has byte-identical bodies, and only that pair is folded. The other readers have per-property defaults and validation, so a generic framework over all eleven would add indirection without removing complexity.
* Seven `UdfContext` doubles across `adapter_tests.rs`, `connection_tests.rs`, and `unity_schema_tests.rs` repeat the same four required trait methods. `exasol-udf-sdk` provides test doubles that replace them.

## Scenarios

### Scenario: One accessor reads a non-empty string field for both adapter-root modules

* *GIVEN* the two byte-identical private accessors — `str_prop` in `adapter/mod.rs` and `str_field` in `adapter/connection.rs` — each returning `Some(&str)` only when a named JSON field is present, string-typed, and not the empty string
* *WHEN* the adapter reads a VS or connection property, or reads a credential field out of a CONNECTION password JSON object
* *THEN* exactly one accessor, named `nonempty_str`, SHALL implement that read, and it SHALL be declared private in `adapter/mod.rs`
* *AND* `adapter/connection.rs` SHALL reach it as a private item of its parent module, so NO item's visibility widens beyond the `adapter` module
* *AND* both `str_prop` and `str_field` SHALL be deleted rather than retained as pass-through wrappers, and every call site in both files SHALL call `nonempty_str` directly
* *AND* the accessor MUST keep the present-string-non-empty contract exactly — an absent field, a JSON null, a non-string value, and an empty string all yield `None` — so every property default and every credential default is reached on exactly the same inputs as before
* *AND* every prose reference to a deleted name SHALL name `nonempty_str` instead, including the `S3_MAX_CONNECTIONS` resolver's doc comment, which cites the shared parse shape by name
* *AND* the `connection/connection-credentials`, `connection/connection-credentials-catalog-auth`, `vs-adapter/create-virtual-schema`, `vs-adapter/create-virtual-schema-adapter-notes`, `vs-adapter/create-virtual-schema-adapter-notes-resources`, and `vs-adapter/refresh-and-set-properties` suites MUST pass with no change to any test assertion or expected value

### Scenario: One resolver reads both DataFusion FIXED-mode count properties

* *GIVEN* `resolve_df_target_partitions` and `resolve_df_threads_per_udf` in `adapter/mod.rs`, whose bodies are identical apart from the property key they read — `DATAFUSION_TARGET_PARTITIONS` versus `DATAFUSION_THREADS_PER_UDF` — and whose doc comments describe the same rule twice
* *WHEN* the adapter resolves the FIXED-mode DataFusion target-partition and threads-per-UDF budgets for a `createVirtualSchema` request
* *THEN* one key-parameterized resolver SHALL implement the shared rule — read the non-empty string property, parse it as an unsigned integer, keep the value when it is at least 1, otherwise fall back to `max(nr_of_cores, 1)` — and the `ThreadingMode::Fixed` arm SHALL call it once per property key
* *AND* both single-purpose names SHALL be deleted rather than retained as pass-through wrappers, and the existing unit tests SHALL call the parameterized resolver with the property key, keeping every test name and every asserted expected value unchanged
* *AND* the `DF_TARGET_PARTITIONS` and `DF_THREADS_PER_UDF` values the adapter records in `adapterNotes` MUST stay identical for every input, so both count-recording scenarios of `vs-adapter/create-virtual-schema-adapter-notes-resources` hold unedited
* *AND* the AUTO-mode path MUST stay untouched: `auto_threads_per_udf` keeps deriving both counts from the core count and the per-node instance share, because AUTO ignores both properties and so shares no body with the FIXED path
* *AND* the `S3_MAX_CONNECTIONS` resolver SHALL NOT be folded in, because its fallback derives an AUTO value from `auto_threads_per_udf` rather than returning `max(nr_of_cores, 1)` — the two bodies differ, so folding them would require a second parameter that re-splits the function at every call
* *AND* no generic property-parsing framework SHALL be introduced: the fold SHALL stay one concrete function over the one identical pair

### Scenario: No hand-rolled UdfContext doubles in adapter tests

* *GIVEN* the seven `UdfContext` doubles in the adapter test modules
* *WHEN* the adapter unit-test suite compiles and runs
* *THEN* no file under `crates/lakehouse-engine/src/adapter/` SHALL declare an `impl UdfContext` (bare or fully-qualified form)
* *AND* every existing adapter test MUST pass with no change to any assertion or expected value

### Scenario: The node-count fallback test asserts the trait default

* *GIVEN* `cluster_nodes_from_context_defaults_to_one_when_node_count_zero`, which drives the `0 → 1` fallback through the `UdfContext` trait default `node_count` of 0
* *WHEN* the hand-rolled double is replaced
* *THEN* the replacement MUST NOT override `node_count`, so the test keeps asserting the trait behavior rather than a double's return value
