# Feature: Adapter Module Structure

Gives each repeated read-a-JSON-field shape in the adapter-root modules (`adapter/mod.rs`, `adapter/connection.rs`) exactly one implementation, so a change to how the adapter reads a property, a credential field, or a resource count has one place to land. Behavior is unchanged: this feature constrains where the code lives, never what the adapter returns.

This is the adapter root's structural feature, the sibling of `vs-adapter/pushdown-module-structure` and `datafusion-scan/scan-module-structure`. It exists because the duplication it removes spans four behavioral features at once — `vs-adapter/create-virtual-schema`, `vs-adapter/create-virtual-schema-adapter-notes-resources`, `vs-adapter/refresh-and-set-properties`, and `vs-adapter/connection-credentials` — so no single behavioral feature can own it without leaking a structural decision across a boundary.

## Background

* Both scenarios are pure refactors. Every scenario of `vs-adapter/create-virtual-schema`, `vs-adapter/create-virtual-schema-adapter-notes`, `vs-adapter/create-virtual-schema-adapter-notes-resources`, `vs-adapter/refresh-and-set-properties`, `vs-adapter/connection-credentials`, and `vs-adapter/connection-credentials-catalog-auth` stays accurate and unedited, and those suites are the characterization gate that makes "behavior unchanged" falsifiable.
* `adapter::connection` is a child module of `adapter`, so it can name a private item of `adapter` directly. A helper shared between the two therefore stays private to `adapter` — hoisting it widens nothing.
* A helper whose whole body is a call to another helper with the same arguments is a pass-through, the shallow-module red flag from `/speq:design-philosophy`. Both scenarios below delete the single-purpose names rather than keep them as pass-throughs, so the deduplication actually removes indirection instead of adding a layer.
* `Json` in `adapter/mod.rs` is an alias for `serde_json::Value`; the two duplicated accessors differ only in which spelling they use.
* The adapter has eleven `resolve_*` property readers. Only ONE pair has byte-identical bodies, and only that pair is folded. A generic property-parsing framework over all eleven was considered and rejected in issue #177 — the readers are individually documented one-liners whose per-property defaults and validation differ, so a generic plus a config table would relocate them into indirection without removing complexity.

## Scenarios

### Scenario: One accessor reads a non-empty string field for both adapter-root modules

* *GIVEN* the two byte-identical private accessors — `str_prop` in `adapter/mod.rs` and `str_field` in `adapter/connection.rs` — each returning `Some(&str)` only when a named JSON field is present, string-typed, and not the empty string
* *WHEN* the adapter reads a VS or connection property, or reads a credential field out of a CONNECTION password JSON object
* *THEN* exactly one accessor, named `nonempty_str`, SHALL implement that read, and it SHALL be declared private in `adapter/mod.rs`
* *AND* `adapter/connection.rs` SHALL reach it as a private item of its parent module, so NO item's visibility widens beyond the `adapter` module
* *AND* both `str_prop` and `str_field` SHALL be deleted rather than retained as pass-through wrappers, and every call site in both files SHALL call `nonempty_str` directly
* *AND* the accessor MUST keep the present-string-non-empty contract exactly — an absent field, a JSON null, a non-string value, and an empty string all yield `None` — so every property default and every credential default is reached on exactly the same inputs as before
* *AND* every prose reference to a deleted name SHALL name `nonempty_str` instead, including the `S3_MAX_CONNECTIONS` resolver's doc comment, which cites the shared parse shape by name
* *AND* the `vs-adapter/connection-credentials`, `vs-adapter/connection-credentials-catalog-auth`, `vs-adapter/create-virtual-schema`, `vs-adapter/create-virtual-schema-adapter-notes`, `vs-adapter/create-virtual-schema-adapter-notes-resources`, and `vs-adapter/refresh-and-set-properties` suites MUST pass with no change to any test assertion or expected value

### Scenario: One resolver reads both DataFusion FIXED-mode count properties

* *GIVEN* `resolve_df_target_partitions` and `resolve_df_threads_per_udf` in `adapter/mod.rs`, whose bodies are identical apart from the property key they read — `DATAFUSION_TARGET_PARTITIONS` versus `DATAFUSION_THREADS_PER_UDF` — and whose doc comments describe the same rule twice
* *WHEN* the adapter resolves the FIXED-mode DataFusion target-partition and threads-per-UDF budgets for a `createVirtualSchema` request
* *THEN* one key-parameterized resolver SHALL implement the shared rule — read the non-empty string property, parse it as an unsigned integer, keep the value when it is at least 1, otherwise fall back to `max(nr_of_cores, 1)` — and the `ThreadingMode::Fixed` arm SHALL call it once per property key
* *AND* both single-purpose names SHALL be deleted rather than retained as pass-through wrappers, and the existing unit tests SHALL call the parameterized resolver with the property key, keeping every test name and every asserted expected value unchanged
* *AND* the `DF_TARGET_PARTITIONS` and `DF_THREADS_PER_UDF` values the adapter records in `adapterNotes` MUST stay identical for every input, so both count-recording scenarios of `vs-adapter/create-virtual-schema-adapter-notes-resources` hold unedited
* *AND* the AUTO-mode path MUST stay untouched: `auto_threads_per_udf` keeps deriving both counts from the core count and the per-node instance share, because AUTO ignores both properties and so shares no body with the FIXED path
* *AND* the `S3_MAX_CONNECTIONS` resolver SHALL NOT be folded in, because its fallback derives an AUTO value from `auto_threads_per_udf` rather than returning `max(nr_of_cores, 1)` — the two bodies differ, so folding them would require a second parameter that re-splits the function at every call
* *AND* no generic property-parsing framework SHALL be introduced: the fold SHALL stay one concrete function over the one identical pair
