# Feature: Version Query UDF

A zero-argument `LAKEHOUSE_VERSION()` scalar script returns the engine crate version compiled into
the `.so`. It needs no CONNECTION and no Virtual Schema. The install script creates this script and
calls it as its install verification, comparing the returned value against the release it
downloaded.

## Background

* `LAKEHOUSE_VERSION` is a third Rust entry point in the `.so`, driven by a `RUST SCALAR SCRIPT`
  with a `RETURNS` clause (not `EMITS`).
* CI derives every release tag from the engine crate manifest (`tag=v$VER`), and the install script
  strips the leading `v` from the resolved tag. The reported version and the resolved release
  version are the same bare `X.Y.Z` string, so an exact string comparison is the correct check.
* The install script creates the version script for every resolved release version, with no
  engine-version floor. A release older than v0.45.0 has no version entry point, and its install
  fails at verification through the generic error path. That input is unsupported.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The install script creates the version script with the other deployment scripts

* *GIVEN* the install script has uploaded the engine `.so` and resolved its `%udf_object` path
* *WHEN* the script creates the deployment scripts in the target schema
* *THEN* it SHALL issue six script statements in that schema: the RUST ADAPTER SCRIPT, the RUST
  SCALAR scan script, the RUST SCALAR version script, the RUST SCALAR export script, the RUST SET
  export writer script, and the LUA SET file distributor
* *AND* the version script's DDL SHALL declare `RETURNS VARCHAR(100)`, MUST NOT declare an `EMITS`
  clause, and SHALL reference the same `%udf_object` path as the adapter script and the scan script
* *AND* every statement SHALL stay idempotent, so a re-run over a prior install replaces the
  scripts in place, and a failure of any statement SHALL abort the install naming that statement
* *AND* the script SHALL issue the version DDL for every resolved release version, applying no
  minimum-version check
* *AND* the printed next-step template MUST NOT emit a `GRANT ACCESS ON CONNECTION` line for the
  version script, which reads no CONNECTION
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: The install documentation describes the version-based verification

* *GIVEN* the install documentation
* *WHEN* an operator reads it
* *THEN* the command walkthrough SHALL name six created scripts, including the version script, and
  SHALL describe the verification as a comparison of the reported version against the downloaded
  release
* *AND* the by-hand script appendix SHALL carry the version script's `RETURNS VARCHAR(100)` DDL, so
  an operator who creates the scripts by hand creates all six
* *AND* the verification appendix SHALL give `SELECT <schema>.LAKEHOUSE_VERSION();` as the by-hand
  check, and SHALL state that a returned value differing from the installed release means the wrong
  artifact reached the target
* *AND* the documentation SHALL offer that same query as a standalone operator diagnostic that
  needs no CONNECTION and no Virtual Schema
* *AND* neither the documentation nor the CI job descriptions of the install script SHALL present a
  scan-script call, or an error-kind fingerprint check, as an install verification step
<!-- /DELTA:CHANGED -->
