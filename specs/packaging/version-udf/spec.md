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

### Scenario: The version entry point reports the compiled engine version

* *GIVEN* the `.so` built from the engine crate and a `RUST SCALAR SCRIPT` with no parameters
  returning `VARCHAR(100)` bound to it
* *WHEN* a client calls the script
* *THEN* it SHALL return the crate version as a plain `X.Y.Z` string
* *AND* the call MUST NOT read any CONNECTION, catalog, or object storage

### Scenario: The install script creates the version script with the other deployment scripts

* *GIVEN* the install script has uploaded the engine `.so` and resolved its `%udf_object` path
* *WHEN* the script creates the deployment scripts in the target schema
* *THEN* it SHALL issue four script statements in that schema: the RUST ADAPTER SCRIPT, the RUST
  SCALAR scan script, the RUST SCALAR version script, and the LUA SET file distributor
* *AND* the version script's DDL SHALL declare `RETURNS VARCHAR(100)`, MUST NOT declare an `EMITS`
  clause, and SHALL reference the same `%udf_object` path as the adapter script and the scan script
* *AND* every statement SHALL stay idempotent, so a re-run over a prior install replaces the
  scripts in place, and a failure of any statement SHALL abort the install naming that statement
* *AND* the script SHALL issue the version DDL for every resolved release version, applying no
  minimum-version check
* *AND* the printed next-step template MUST NOT emit a `GRANT ACCESS ON CONNECTION` line for the
  version script, which reads no CONNECTION

### Scenario: The install verification queries the version script and reads its reported value

* *GIVEN* the install script created the deployment scripts
* *WHEN* the script runs its install verification
* *THEN* it SHALL call the version script through one aliased projection,
  `SELECT <schema>.LAKEHOUSE_VERSION() AS LAKEHOUSE_ENGINE_VERSION`, so the returned column header
  is a fixed literal rather than a database-generated name
* *AND* it MUST NOT invoke the scan script, or any script other than the version script, as part of
  the verification
* *AND* it SHALL read the reported version with an extractor that ACCEPTS a value beginning with a
  digit, and that skips the connection banner, the aliased column header, the row-count footer, and
  error lines
* *AND* that header skip SHALL tolerate trailing whitespace after the column name
* *AND* the verification SHALL run on every install path, including a `--skip-slc` run and an
  Exasol Personal `--deployment` run

### Scenario: The install verification passes only on an exact version match

* *GIVEN* the install script resolved the engine release version and read a reported version from
  the version script
* *WHEN* the script classifies the verification result
* *THEN* it SHALL report the verification as passed only when the reported version equals the
  resolved release version exactly
* *AND* a reported version different from the resolved release version SHALL abort the install with
  an error naming both the expected value and the reported value, rendering an empty reported value
  as a visible placeholder rather than as nothing
* *AND* a fingerprint-mismatch error from the language container SHALL abort the install with a
  distinct message directing the operator to align the SLC version, and that classification SHALL
  take precedence over the query's return code
* *AND* any other failure of the verification query SHALL abort the install and surface the
  underlying database error text

### Scenario: The install documentation describes the version-based verification

* *GIVEN* the install documentation
* *WHEN* an operator reads it
* *THEN* the command walkthrough SHALL name four created scripts, including the version script, and
  SHALL describe the verification as a comparison of the reported version against the downloaded
  release
* *AND* the by-hand script appendix SHALL carry the version script's `RETURNS VARCHAR(100)` DDL, so
  an operator who creates the scripts by hand creates all four
* *AND* the verification appendix SHALL give `SELECT <schema>.LAKEHOUSE_VERSION();` as the by-hand
  check, and SHALL state that a returned value differing from the installed release means the wrong
  artifact reached the target
* *AND* the documentation SHALL offer that same query as a standalone operator diagnostic that
  needs no CONNECTION and no Virtual Schema
* *AND* neither the documentation nor the CI job descriptions of the install script SHALL present a
  scan-script call, or an error-kind fingerprint check, as an install verification step
