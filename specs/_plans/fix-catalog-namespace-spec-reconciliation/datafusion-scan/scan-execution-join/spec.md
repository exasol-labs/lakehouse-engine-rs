# Feature: DataFusion Scan Execution — Broadcast Join

Extends `datafusion-scan/scan-execution` with node-local broadcast inner equi-join execution. A join scan invocation receives, in addition to its per-shard fact-file subset, the FULL dimension-side file list carried once in the shard-invariant common spec. The UDF registers both sides as tables in ONE DataFusion session, executes the inner equi-join with the pushed projection, filter, and LIMIT, and streams the joined rows back as Arrow IPC batches. It holds no state and discovers no files of its own.

## Background

* **This delta corrects a behavioural mis-statement and is issue #324. Join execution is unchanged.** The recorded text said the UDF registers both sides "as Iceberg tables" against "its own logical Iceberg schema". The join scan path holds no Iceberg-versus-Delta dispatch at all: it registers each side from the neutral `logical_schema` the format reader produced, and a broadcast join over Delta tables reached through Unity Catalog is covered end to end by `e2e-harness/unity-catalog-e2e-harness-delta-queries`.
* **The Iceberg-grounded clauses of this feature are NOT neutralized and stay as recorded.** The path-first credential routing rule and its Iceberg-spec support (Appendix E Version 4's absolute-path rule, `data_file.file_path`, `location`), and the tracked multi-bucket refusal `(#304)`, are genuine Iceberg statements with quoted normative backing. Only the two clauses naming the registration schema are renamed.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scan registers both tables and executes the inner equi-join

* *GIVEN* a reconstituted join scan spec
* *WHEN* the scan UDF runs for that invocation
* *THEN* the UDF SHALL register the fact side's assigned files and the dimension side's full file list as two separate tables in ONE DataFusion session, each with its declared logical schema and each exposing its columns under the Exasol-facing (uppercased) names the pushed condition and projection reference
* *AND* the UDF SHALL register each side's table against that side's OWN storage backend, so the redaction set guarding that side's read errors holds that side's credential values rather than the other side's
* *AND* the UDF SHALL execute an inner equi-join of the two registered tables on the rendered join condition
* *AND* the UDF MUST NOT resolve or discover any file beyond the two file lists carried in the spec
* *AND* the UDF SHALL register a side whose spec a Delta reader produced by the SAME path it registers an Iceberg-produced one, reading only the neutral logical schema and file list carried in the spec
<!-- /DELTA:CHANGED -->
