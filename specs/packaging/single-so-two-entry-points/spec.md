# Feature: Single Shared Object, Four Entry Points

One Rust crate builds a single `.so` that exports the VS adapter, the DataFusion scan UDF, the version query UDF, and the export script as named entry points. One artifact is uploaded to BucketFS and each Exasol script references it.

## Background

* The scan entry point is a SCALAR EMIT script. The version entry point is a SCALAR RETURNS script. The export entry point is a SET RETURNS script. Exasol calls it for `EXPORT ... INTO SCRIPT`, and the SELECT it returns calls the same script as the writer.
* The `LAKEHOUSE_DISTRIBUTE_FILES` fan-out distributor is a separate LUA SET script created by its own DDL. It is not a Rust entry point and is not part of the `.so`.
* Each `CREATE SCRIPT` statement references its entry point by registered name against the same `%udf_object`.
* The `.so` is built only inside the `UDF_BUILDER_IMAGE` Rust builder image the Makefile names, never with host `cargo build --release`.

## Scenarios

### Scenario: All scripts resolve from the same uploaded artifact

* *GIVEN* the single `.so` has been uploaded to BucketFS
* *AND* an ADAPTER SCRIPT, a scan SCALAR SCRIPT, a version SCALAR SCRIPT, and an export SET SCRIPT have each been created referencing that `.so`
* *WHEN* each script is invoked
* *THEN* each invocation SHALL run the entry point registered under the script's name
* *AND* an `EXPORT ... INTO SCRIPT` through the export script SHALL run the export entry point both for the statement and for the writers
* *AND* no invocation SHALL require a second uploaded artifact

### Scenario: The file distributor is a separate LUA SET script created by its own DDL

* *GIVEN* the deployment DDL that creates the scan scripts
* *WHEN* the fan-out distributor is provisioned
* *THEN* the deployment SHALL create `LAKEHOUSE_DISTRIBUTE_FILES` as a standalone LUA SET SCRIPT (a pure passthrough re-emitting its `files` input, one row per group), independent of the uploaded `.so`
* *AND* the pushdown wrapper SQL SHALL reference `LAKEHOUSE_DISTRIBUTE_FILES` schema-qualified so it resolves outside the adapter script's schema context, using the same schema resolution the scan script uses: the schema of the running adapter script, read from the UDF handshake via `ctx.script_schema()`, NOT a VS property
* *AND* the distributor DDL MUST NOT reference the uploaded scan `.so` and MUST NOT declare a Rust `%udf_object` entry point

### Scenario: Host release build of the .so is rejected by convention

* *GIVEN* the crate manifest and Makefile
* *WHEN* the documented build path is followed
* *THEN* the `.so` SHALL be produced only by the containerized build target
* *AND* the build documentation MUST state that host `cargo build --release` produces an unloadable artifact

### Scenario: One crate exports every Rust entry point

* *GIVEN* the UDF crate source declaring the VS adapter, scan, version, and export entry points
* *WHEN* the crate is built in the builder image
* *THEN* the build SHALL produce exactly one `.so` artifact
* *AND* that `.so` SHALL export exactly four entry-point symbols: `__exa_udf_entry_LAKEHOUSE_ADAPTER`, `__exa_udf_entry_LAKEHOUSE_SCAN`, `__exa_udf_entry_LAKEHOUSE_VERSION`, and `__exa_udf_entry_LAKEHOUSE_EXPORT`
* *AND* the `LAKEHOUSE_DISTRIBUTE_FILES` fan-out distributor SHALL NOT be one of the crate's Rust entry points and SHALL NOT be exported from the `.so`
