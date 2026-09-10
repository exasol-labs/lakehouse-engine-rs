# Feature: Version Query UDF

A zero-argument `LAKEHOUSE_VERSION()` scalar script returns the engine crate version compiled into
the `.so`. Needs no CONNECTION and no Virtual Schema. The installer uses the same call as its smoke
test, matching the returned value against the downloaded release version.

## Background

* `LAKEHOUSE_VERSION` is a third Rust entry point in the `.so`, driven by a `RUST SCALAR SCRIPT`
  with a `RETURNS` clause (not `EMITS`).
* CI derives each release tag from the crate manifest (`tag=v$VER`), so the reported version and
  `RESOLVED_ENGINE_VERSION` (with its leading `v` stripped) are the same bare `X.Y.Z` string.

## Scenarios

### Scenario: The version entry point reports the compiled engine version

* *GIVEN* the `.so` built from the engine crate and a `RUST SCALAR SCRIPT` with no parameters
  returning `VARCHAR(100)` bound to it
* *WHEN* a client calls the script
* *THEN* it SHALL return the crate version as a plain `X.Y.Z` string
* *AND* the call MUST NOT read any CONNECTION, catalog, or object storage

### Scenario: The smoke test verifies the deployed version matches the downloaded release

* *GIVEN* the installer resolved engine release `RESOLVED_ENGINE_VERSION`
* *WHEN* the smoke test calls `LAKEHOUSE_VERSION()`
* *THEN* it SHALL pass only when the returned value equals `RESOLVED_ENGINE_VERSION` exactly
* *AND* a returned value different from `RESOLVED_ENGINE_VERSION` SHALL abort with an error naming
  both the expected and actual version
* *AND* a fingerprint-mismatch error SHALL abort with its own distinct message directing the
  operator to align the SLC version
* *AND* any other error SHALL abort with the underlying database error text
