# Feature: Version Query UDF

A zero-argument `LAKEHOUSE_VERSION()` scalar script returns the engine crate version compiled into
the `.so`. Needs no CONNECTION and no Virtual Schema.

## Background

* `LAKEHOUSE_VERSION` is a third Rust entry point in the `.so`, driven by a `RUST SCALAR SCRIPT`
  with a `RETURNS` clause (not `EMITS`).
* The version value has one owner: a crate-level constant fed by `env!("CARGO_PKG_VERSION")`.

## Scenarios

### Scenario: The version entry point reports the compiled engine version

* *GIVEN* the `.so` built from the engine crate and a `RUST SCALAR SCRIPT` with no parameters
  returning `VARCHAR(100)` bound to it
* *WHEN* a client calls the script
* *THEN* it SHALL return the crate version as a plain `X.Y.Z` string
* *AND* the call MUST NOT read any CONNECTION, catalog, or object storage
