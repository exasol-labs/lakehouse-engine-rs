# Feature: DataFusion Scan Execution — Broadcast Join

Extends `datafusion-scan/scan-execution` with node-local broadcast inner equi-join execution. A join scan invocation receives, in addition to its per-shard fact-file subset, the FULL dimension-side file list carried once in the shard-invariant common spec. The UDF registers both sides as tables in ONE DataFusion session, executes the inner equi-join with the pushed projection, filter, and LIMIT, and streams the joined rows back as Arrow IPC batches. It holds no state and discovers no files of its own.

## Background

* **This delta is issue #135. It amends TWO scenarios and changes no join rule.** The two-file-list reconstitution, the two-store registration, the per-side size index, the inner equi-join, the build-side choice, the LIMIT handling, and the early-termination diagnostic are all UNCHANGED.
* **Each side's storage value becomes a REFERENCE or an INLINE backend**, specified by `vs-adapter/scan-spec-credential-reference`, which this feature CITES. Both sides of one join are planned under ONE virtual schema and therefore ONE CONNECTION, so both sides carry the same variant while their inline payloads may still differ under vending.
* **SUPERSEDES the recorded per-side redaction clause "so the redaction set guarding that side's read errors holds that side's credential values rather than the other side's".** The redaction set is now built from the RESOLVED backends and, on a join spec, is the UNION of both sides' resolved secrets — because resolution happens once per invocation before any store exists, and a per-side set would be undefined between the two reads. Registering each side against its OWN backend is UNCHANGED; only where the secret set comes from changes.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scan reconstitutes a join scan spec carrying two file lists

* *GIVEN* a scan invocation whose common-spec argument carries a join block (the dimension side's table root, full file list, logical schema, name mapping, its OWN storage value, the rendered join condition, and the join type) and whose per-shard argument carries the fact side's `(path, size)` file subset
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL reconstitute one join `ScanSpec` whose fact files come from the per-shard argument and whose dimension side and every other field come from the common spec
* *AND* the join block's storage value SHALL be a REQUIRED field with no deserialization default, so a join block that carries none fails to deserialize rather than silently reusing the whole-spec storage value
* *AND* the reconstituted spec SHALL carry TWO storage values — the fact side's as the whole-spec `storage` value and the dimension side's inside the join block, each either a connection REFERENCE or an INLINE backend under `vs-adapter/scan-spec-credential-reference` — and the UDF MUST NOT read either side's files through the other's resolved backend
* *AND* a parse failure on either argument SHALL surface an error identifying scan-spec deserialization failure and MUST NOT contain any storage access key, secret key, or session token from EITHER side's backend
* *AND* the reconstituted spec MUST NOT carry any catalog identifier, because the scan UDF never contacts the catalog
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Scan registers both tables and executes the inner equi-join

* *GIVEN* a reconstituted join scan spec
* *WHEN* the scan UDF runs for that invocation
* *THEN* the UDF SHALL register the fact side's assigned files and the dimension side's full file list as two separate tables in ONE DataFusion session, each with its declared logical schema and each exposing its columns under the Exasol-facing (uppercased) names the pushed condition and projection reference
* *AND* the UDF SHALL register each side's table against that side's OWN RESOLVED storage backend, and the redaction set guarding a read error SHALL be the UNION of both sides' resolved secret values, because the one per-invocation resolution defines the set before either store exists — SUPERSEDING the recorded per-side set
* *AND* the UDF SHALL execute an inner equi-join of the two registered tables on the rendered join condition
* *AND* the UDF MUST NOT resolve or discover any file beyond the two file lists carried in the spec
* *AND* the UDF SHALL register a side whose spec a Delta reader produced by the SAME path it registers an Iceberg-produced one, reading only the neutral logical schema and file list carried in the spec
<!-- /DELTA:CHANGED -->
