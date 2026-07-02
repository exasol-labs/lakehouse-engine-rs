# Feature: DataFusion Scan Execution — Spec Reconstitution

Extends `datafusion-scan/scan-execution` with the mechanics of the scan UDF's two-argument
input: a shard-invariant common-spec JSON blob (arg 0) and a per-shard file-URI list (arg 1),
which the UDF deserializes and merges into one `ScanSpec` before running the shared scan path.

## Background

* The scan UDF's first argument is the shard-invariant common spec (projection, filter,
  limit, aggregates, group keys, logical schema, EMITS types, storage credentials, and
  tuning knobs), serialized once per fan-out; the second argument is this shard's file-URI
  list. See `datafusion-scan/scan-execution` for the scan behavior once the spec is merged.
* `ScanSpec` carries no catalog identifier block — the scan UDF never contacts the catalog.
* A parse failure on either argument MUST surface an error identifying scan-spec
  deserialization failure and MUST NOT contain any storage access key, secret key, or
  session token.

## Scenarios

### Scenario: Scan reconstitutes the ScanSpec from the common and per-shard arguments

* *GIVEN* a scan invocation whose first argument is a common-spec JSON blob carrying every shard-invariant field and whose second argument is a JSON array of file URIs
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize the common-spec JSON and the file-list JSON and MERGE them into one `ScanSpec` value equivalent to the pre-split single-argument spec for the same shard
* *AND* a parse failure on either argument SHALL surface an error that identifies scan-spec deserialization failure and MUST NOT contain any storage access key, secret key, or session token
* *AND* the reconstituted `ScanSpec` MUST NOT carry any catalog identifier field, because the scan UDF never contacts the catalog
