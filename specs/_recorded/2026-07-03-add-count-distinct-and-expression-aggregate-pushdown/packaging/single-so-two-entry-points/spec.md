# Feature: Single Shared Object, Two Entry Points

Packages the VS adapter and the DataFusion scan UDF as named entry points in one
Rust crate that builds to a single `.so`, exploiting language-container-rs 0.14.0's
multiple-entry-points-per-`.so` capability. One artifact is uploaded to BucketFS and
each Exasol script references it.

## Background

* The crate is a `cdylib` depending on `exasol-udf-sdk` 0.14.0 (connect-back feature)
  and `exasol-udf-macros` 0.14.0.
* The `.so` is built only inside the `rust:1.92-bookworm` builder image; it is never
  built with host `cargo build --release`.
* Each entry point is declared with the `exasol_udf` macro; Exasol's `CREATE SCRIPT`
  statements reference each by its registered name against the same `%udf_object`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: One crate exports both the adapter and the scan entry points

* *GIVEN* the UDF crate source declaring a VS adapter entry point, a scan SET-UDF entry point, and a scalar distinct-merge entry point
* *WHEN* the crate is built in the builder image
* *THEN* the build SHALL produce exactly one `.so` artifact
* *AND* that `.so` SHALL export the adapter entry-point symbol, the scan entry-point symbol, and the scalar distinct-merge entry-point symbol
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Crate exports a scalar distinct-merge entry point in the same .so

* *GIVEN* the UDF crate declaring a scalar distinct-merge entry point (used by the outer wrapper SQL to merge per-shard `COUNT(DISTINCT)` local distinct sets)
* *WHEN* the crate is built in the builder image and a SCALAR SCRIPT referencing that `%udf_object` is created
* *THEN* the scalar-script invocation SHALL run the distinct-merge entry point from the same uploaded `.so`, requiring no second uploaded artifact
* *AND* the pushdown wrapper SQL SHALL reference that scalar script schema-qualified so it resolves outside the adapter script's schema context, using the same scan-UDF schema resolution the SET script uses
<!-- /DELTA:NEW -->
