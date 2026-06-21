# Feature: Single Shared Object, Two Entry Points

Packages the VS adapter and the DataFusion scan UDF as two named entry points in one
Rust crate that builds to a single `.so`, exploiting language-container-rs 0.14.0's
multiple-entry-points-per-`.so` capability. One artifact is uploaded to BucketFS and
both Exasol scripts reference it.

## Background

* The crate is a `cdylib` depending on `exasol-udf-sdk` 0.14.0 (connect-back feature)
  and `exasol-udf-macros` 0.14.0.
* The `.so` is built only inside the `rust:1.92-bookworm` builder image; it is never
  built with host `cargo build --release`.
* Each entry point is declared with the `exasol_udf` macro; Exasol's `CREATE SCRIPT`
  statements reference each by its registered name against the same `%udf_object`.

## Scenarios

### Scenario: One crate exports both the adapter and the scan entry points

* *GIVEN* the UDF crate source declaring a VS adapter entry point and a scan SET-UDF entry point
* *WHEN* the crate is built in the builder image
* *THEN* the build SHALL produce exactly one `.so` artifact
* *AND* that `.so` SHALL export both the adapter entry-point symbol and the scan entry-point symbol

### Scenario: Both scripts resolve from the same uploaded artifact

* *GIVEN* the single `.so` has been uploaded to BucketFS
* *AND* an ADAPTER SCRIPT and a SET SCRIPT have each been created referencing that `%udf_object`
* *WHEN* each script is invoked
* *THEN* the adapter invocation SHALL run the adapter entry point
* *AND* the SET-script invocation SHALL run the scan entry point
* *AND* neither invocation SHALL require a second uploaded artifact

### Scenario: Host release build of the .so is rejected by convention

* *GIVEN* the crate manifest and Makefile
* *WHEN* the documented build path is followed
* *THEN* the `.so` SHALL be produced only by the containerized build target
* *AND* the build documentation MUST state that host `cargo build --release` produces an unloadable artifact
