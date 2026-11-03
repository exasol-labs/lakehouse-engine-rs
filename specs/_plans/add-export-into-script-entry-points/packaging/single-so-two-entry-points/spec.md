<!-- DELTA:CHANGED -->
# Feature: Single Shared Object for All Rust Entry Points

Packages the VS adapter, the DataFusion scan UDF, the version query UDF, the export script, and the export writer as named entry points in one Rust crate that builds to a single `.so`. The scan entry point is a SCALAR EMIT script, and the version entry point is a SCALAR RETURNS script. The export entry point is a SCALAR script that Exasol calls for `EXPORT ... INTO SCRIPT`, and the export writer is a SET RETURNS script. The cluster fan-out distributor (`LAKEHOUSE_DISTRIBUTE_FILES`) is a separate LUA SET script created by its own DDL, not a Rust entry point in the `.so`. One artifact is uploaded to BucketFS and each Exasol script references it.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* All five Rust entry points (adapter, scan, version, export, export writer) are exported from the single uploaded `.so`. The `LAKEHOUSE_DISTRIBUTE_FILES` fan-out distributor is a separate LUA SET script created by its own DDL and is not part of the `.so`.
* The `.so` is built only inside the `rust:1.94-trixie` builder image. It is never built with host `cargo build --release`.
* Each entry point is declared with the `exasol_udf` macro. Exasol's `CREATE SCRIPT` statements reference each by its registered name against the same `%udf_object`.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: One crate exports the adapter, scan, and version entry points

* *GIVEN* the UDF crate source declaring a VS adapter entry point, a scan entry point (driven as a SCALAR EMIT script), and a version entry point (driven as a SCALAR RETURNS script)
* *WHEN* the crate is built in the builder image
* *THEN* the build SHALL produce exactly one `.so` artifact
* *AND* that `.so` SHALL export the adapter entry-point symbol, the scan entry-point symbol (`__exa_udf_entry_LAKEHOUSE_SCAN`), and the version entry-point symbol (`__exa_udf_entry_LAKEHOUSE_VERSION`)
* *AND* the `LAKEHOUSE_DISTRIBUTE_FILES` fan-out distributor SHALL NOT be one of the crate's Rust entry points and SHALL NOT be exported from the `.so`
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: One crate exports every Rust entry point

* *GIVEN* the UDF crate source declaring the VS adapter, scan, version, export, and export writer entry points
* *WHEN* the crate is built in the builder image
* *THEN* the build SHALL produce exactly one `.so` artifact
* *AND* that `.so` SHALL export exactly five entry-point symbols: `__exa_udf_entry_LAKEHOUSE_ADAPTER`, `__exa_udf_entry_LAKEHOUSE_SCAN`, `__exa_udf_entry_LAKEHOUSE_VERSION`, `__exa_udf_entry_LAKEHOUSE_EXPORT`, and `__exa_udf_entry_LAKEHOUSE_EXPORT_WRITER`
* *AND* the `LAKEHOUSE_DISTRIBUTE_FILES` fan-out distributor SHALL NOT be one of the crate's Rust entry points and SHALL NOT be exported from the `.so`
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: All scripts resolve from the same uploaded artifact

* *GIVEN* the single `.so` has been uploaded to BucketFS
* *AND* an ADAPTER SCRIPT, a scan SCALAR SCRIPT, a version SCALAR SCRIPT, an export SCALAR SCRIPT, and an export writer SET SCRIPT have each been created referencing that `.so`
* *WHEN* each script is invoked
* *THEN* each invocation SHALL run the entry point registered under the script's name
* *AND* an `EXPORT ... INTO SCRIPT` through the export script SHALL run the export entry point and then the export writer entry point
* *AND* no invocation SHALL require a second uploaded artifact
<!-- /DELTA:CHANGED -->
