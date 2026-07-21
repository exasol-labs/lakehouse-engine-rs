# Feature: Single Shared Object, Two Entry Points

Packages the VS adapter and the DataFusion scan UDF as named entry points in one Rust
crate that builds to a single `.so`, exploiting language-container-rs's
multiple-entry-points-per-`.so` capability. The scan entry point is a single `.so`
symbol driven by a SCALAR EMIT script. The cluster fan-out distributor
(`LAKEHOUSE_DISTRIBUTE_FILES`) is a separate LUA SET script created by its own DDL — NOT
a Rust entry point in the `.so`. One artifact is uploaded to BucketFS and each Exasol
script references it.

## Background

* Both Rust entry points (adapter, scan) are exported from the single uploaded `.so`; the `LAKEHOUSE_DISTRIBUTE_FILES` fan-out distributor is a separate LUA SET script created by its own DDL and is not part of the `.so`.
* The crate is a `cdylib` depending on `exasol-udf-sdk` 0.14.0 (connect-back feature)
  and `exasol-udf-macros` 0.14.0.
* The `.so` is built only inside the `rust:1.94-bookworm` builder image; it is never
  built with host `cargo build --release`.
* Each entry point is declared with the `exasol_udf` macro; Exasol's `CREATE SCRIPT`
  statements reference each by its registered name against the same `%udf_object`.

## Scenarios

### Scenario: One crate exports the adapter and the scan entry points

* *GIVEN* the UDF crate source declaring a VS adapter entry point and a scan entry point (driven as a SCALAR EMIT script)
* *WHEN* the crate is built in the builder image
* *THEN* the build SHALL produce exactly one `.so` artifact
* *AND* that `.so` SHALL export the adapter entry-point symbol and the scan entry-point symbol (`__exa_udf_entry_LAKEHOUSE_SCAN`, unchanged by the SET→SCALAR script-type change)
* *AND* the `LAKEHOUSE_DISTRIBUTE_FILES` fan-out distributor SHALL NOT be one of the crate's Rust entry points and SHALL NOT be exported from the `.so`

### Scenario: Both scripts resolve from the same uploaded artifact

* *GIVEN* the single `.so` has been uploaded to BucketFS
* *AND* an ADAPTER SCRIPT and a SCALAR SCRIPT have each been created referencing that `.so`
* *WHEN* each script is invoked
* *THEN* the adapter invocation SHALL run the adapter entry point
* *AND* the SCALAR-script invocation SHALL run the scan entry point
* *AND* neither invocation SHALL require a second uploaded artifact

### Scenario: The file distributor is a separate LUA SET script created by its own DDL

* *GIVEN* the deployment DDL that creates the scan scripts
* *WHEN* the fan-out distributor is provisioned
* *THEN* the deployment SHALL create `LAKEHOUSE_DISTRIBUTE_FILES` as a standalone LUA SET SCRIPT (a pure passthrough re-emitting its `files` input, one row per group), independent of the uploaded `.so`
* *AND* the pushdown wrapper SQL SHALL reference `LAKEHOUSE_DISTRIBUTE_FILES` schema-qualified so it resolves outside the adapter script's schema context, using the same schema resolution the scan script uses — the schema of the running adapter script, read from the UDF handshake via `ctx.script_schema()`, NOT a VS property
* *AND* the distributor DDL MUST NOT reference the uploaded scan `.so` and MUST NOT declare a Rust `%udf_object` entry point

### Scenario: Host release build of the .so is rejected by convention

* *GIVEN* the crate manifest and Makefile
* *WHEN* the documented build path is followed
* *THEN* the `.so` SHALL be produced only by the containerized build target
* *AND* the build documentation MUST state that host `cargo build --release` produces an unloadable artifact
