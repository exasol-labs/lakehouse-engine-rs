<!-- DELTA:CHANGED -->
# Feature: Adapter Module Structure

Gives each repeated read-a-JSON-field shape in the adapter-root modules (`adapter/mod.rs`, `adapter/connection.rs`) exactly one implementation, so a change to how the adapter reads a property, a credential field, or a resource count has one place to land. Behavior is unchanged: this feature constrains where the code lives, never what the adapter returns.

This is the adapter root's structural feature, the sibling of `pushdown/pushdown-module-structure` and `datafusion-scan/scan-module-structure`. It exists because the duplication it removes spans four behavioral features at once — `vs-adapter/create-virtual-schema`, `vs-adapter/create-virtual-schema-adapter-notes-resources`, `vs-adapter/refresh-and-set-properties`, and `connection/connection-credentials` — so no single behavioral feature can own it without leaking a structural decision across a boundary.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* Both scenarios are pure refactors. Every scenario of `vs-adapter/create-virtual-schema`, `vs-adapter/create-virtual-schema-adapter-notes`, `vs-adapter/create-virtual-schema-adapter-notes-resources`, `vs-adapter/refresh-and-set-properties`, `connection/connection-credentials`, and `connection/connection-credentials-catalog-auth` stays accurate and unedited, and those suites are the characterization gate that makes "behavior unchanged" falsifiable.
* `adapter::connection` is a child module of `adapter`, so it can name a private item of `adapter` directly. A helper shared between the two therefore stays private to `adapter` — hoisting it widens nothing.
* A helper whose whole body is a call to another helper with the same arguments is a pass-through, the shallow-module red flag from `/speq:design-philosophy`. Both scenarios below delete the single-purpose names rather than keep them as pass-throughs, so the deduplication actually removes indirection instead of adding a layer.
* `Json` in `adapter/mod.rs` is an alias for `serde_json::Value`; the two duplicated accessors differ only in which spelling they use.
* The adapter has eleven `resolve_*` property readers. Only ONE pair has byte-identical bodies, and only that pair is folded. A generic property-parsing framework over all eleven was considered and rejected in issue #177 — the readers are individually documented one-liners whose per-property defaults and validation differ, so a generic plus a config table would relocate them into indirection without removing complexity.
* Seven `UdfContext` doubles across `adapter_tests.rs`, `connection_tests.rs`, and `unity_schema_tests.rs` repeat the same four required trait methods. `exasol-udf-sdk` 0.24.0 provides SDK-maintained test doubles as replacements.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: One accessor reads a non-empty string field for both adapter-root modules

* *GIVEN* the two byte-identical private accessors — `str_prop` in `adapter/mod.rs` and `str_field` in `adapter/connection.rs` — each returning `Some(&str)` only when a named JSON field is present, string-typed, and not the empty string
* *WHEN* the adapter reads a VS or connection property, or reads a credential field out of a CONNECTION password JSON object
* *THEN* exactly one accessor, named `nonempty_str`, SHALL implement that read, and it SHALL be declared private in `adapter/mod.rs`
* *AND* `adapter/connection.rs` SHALL reach it as a private item of its parent module, so NO item's visibility widens beyond the `adapter` module
* *AND* both `str_prop` and `str_field` SHALL be deleted rather than retained as pass-through wrappers, and every call site in both files SHALL call `nonempty_str` directly
* *AND* the accessor MUST keep the present-string-non-empty contract exactly — an absent field, a JSON null, a non-string value, and an empty string all yield `None` — so every property default and every credential default is reached on exactly the same inputs as before
* *AND* every prose reference to a deleted name SHALL name `nonempty_str` instead, including the `S3_MAX_CONNECTIONS` resolver's doc comment, which cites the shared parse shape by name
* *AND* the `connection/connection-credentials`, `connection/connection-credentials-catalog-auth`, `vs-adapter/create-virtual-schema`, `vs-adapter/create-virtual-schema-adapter-notes`, `vs-adapter/create-virtual-schema-adapter-notes-resources`, and `vs-adapter/refresh-and-set-properties` suites MUST pass with no change to any test assertion or expected value
<!-- /DELTA:CHANGED -->
