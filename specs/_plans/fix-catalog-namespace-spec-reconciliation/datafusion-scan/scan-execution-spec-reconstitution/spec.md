# Feature: DataFusion Scan Execution — Spec Reconstitution

Extends `datafusion-scan/scan-execution` with the mechanics of the scan UDF's two-argument
input: a shard-invariant common-spec JSON blob (arg 0) and a per-shard file list (arg 1),
which the UDF deserializes and merges into one `ScanSpec` before running the shared scan path.

## Background

* **This delta corrects format-scoped naming and is issue #324. It changes no wire encoding and no behavior.** The common blob's `table_root` is a neutral field both format readers populate, and a per-shard entry's byte size is whatever the table's own metadata reported — an Iceberg manifest's `file_size_in_bytes` for an Iceberg table, a Delta `add` action's `size` for a Delta one. Naming only the Iceberg source made the wire read as Iceberg-only when it is not.
* **Every recorded byte-identity clause is untouched.** The 2-tuple legacy form, the 3-tuple delete-carrying form with its pinned key order, the self-describing object form, and the Iceberg-side byte identity of the common blob all stand exactly as recorded. This delta renames prose, never an encoding.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scan reconstitutes the ScanSpec from the common and per-shard arguments

* *GIVEN* a scan invocation whose first argument is a common-spec JSON blob carrying every shard-invariant field (including the table root) and whose second argument is a JSON array of `[path, size]` 2-tuples
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize the common-spec JSON and the per-shard file-list JSON and MERGE them into one `ScanSpec` value whose `files` are the `(path, size)` entries from the second argument and whose every other field — including the table root — comes from the first argument, equivalent to the pre-split single-argument spec for the same shard
* *AND* the merge SHALL store each file entry's path verbatim (relative or absolute) without resolving it, so path reconstruction is deferred to file registration (see `datafusion-scan/scan-execution`)
* *AND* a parse failure on either argument SHALL surface an error that identifies scan-spec deserialization failure and MUST NOT contain any storage access key, secret key, or session token
* *AND* the reconstituted `ScanSpec` MUST NOT carry any catalog identifier field, because the scan UDF never contacts the catalog
* *AND* the merge SHALL follow these same rules for arguments produced by EITHER format reader, because the table root and each entry's byte size are neutral values both populate
<!-- /DELTA:CHANGED -->
