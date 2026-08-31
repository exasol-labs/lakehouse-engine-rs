# Feature: Pushdown Planning — Broadcast Inner Equi-Join

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the broadcast inner equi-join shape. When Exasol pushes a two-table join whose smaller side is below the broadcast threshold, the adapter resolves both sides' file lists once, shards only the larger (fact) side through the nested distributor + scalar scan fan-out, replicates the smaller (dimension) side's full file list into the shard-invariant common spec, and drives a node-local DataFusion join inside the scalar scan UDF (`datafusion-scan/scan-execution-join`). Every join outside this broadcast contract — above threshold, non-two-table, needing Exasol postprocessing, or otherwise ineligible — is served by the unified unaccelerated fallback renderer (`vs-adapter/pushdown-planning-join-fallback`), so a join is never wrong, only sometimes unaccelerated.

## Background

* **This delta is issue #135. It amends ONE scenario and one Background bullet, and changes no join rule.** Capability advertisement, broadcast eligibility, small-side selection, the ordering wrapper, the decline paths, per-table projection and filter rendering, and condition translation are all UNCHANGED.
* **SUPERSEDES this feature's recorded unscoped bullet "Credentials MUST NOT appear in any returned SQL string or error message, and MUST NOT be repeated per shard."** Scoped replacement: a CONNECTION-supplied storage credential is carried as a connection REFERENCE and does not appear; a VENDED storage credential still appears there under issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378); no credential of either kind appears in an error message. The once-per-fan-out rule is unchanged and applies to the reference exactly as it applied to the credential.
* **SUPERSEDES the recorded bullet stating that a broadcast join's emitted common blob "carries TWO credential sets instead of one".** With vending disabled it now carries TWO REFERENCES and no credential set at all; with vending enabled it still carries two credential sets, once per side, under #378. Both sides of one join are planned under ONE virtual schema and therefore ONE CONNECTION, so both carry the same variant while their inline payloads may still differ per table.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Broadcast-eligible inner equi-join is planned as a broadcast fan-out

* *GIVEN* a virtual schema over a namespace whose tables are backed by MinIO
* *AND* a `pushdown` request whose `from` clause is a `join` node over exactly two involved tables joined by an equi-condition
* *AND* the smaller side's table-metadata byte size is at or below the broadcast threshold
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL resolve BOTH sides through the SAME format-reader seam the single-table scan uses (`vs-adapter/pushdown-format-neutral-resolution`) — obtaining each side's data-file list, per-file byte size, logical schema, table root, and effective storage exactly once — recovering each table's original-cased catalog identifier from the schema-metadata mapping by its involved-table name
* *AND* the adapter SHALL designate the larger side as the sharded fact side (its file list partitioned into G byte-balanced work-unit shards and driven through the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor exactly as the single-table path does) and the smaller side as the replicated dimension side
* *AND* the adapter SHALL carry the dimension side's FULL file list, table root, logical schema, and its OWN storage value — a connection REFERENCE with vending disabled, its own effective backend INLINE with vending enabled — in the shard-invariant common spec's join block (spliced once as the `LAKEHOUSE_SCAN` scalar UDF's first argument), and the fact side's per-shard file subset flowed through the distributor as the second argument
* *AND* the whole-spec `storage` value SHALL be the FACT side's own storage value under the same rule, so each side of the emitted spec names either one CONNECTION reference or the backend resolved for that side's own table location, and neither side's storage is dropped
* *AND* the generated scan-driving SQL SHALL drive the `LAKEHOUSE_SCAN` SCALAR EMIT UDF so that each shard invocation joins its fact-file subset against the full replicated dimension side node-locally, with no cross-shard exchange, and with NO `SELECT * FROM (...)` wrapper for an UNORDERED request; an ORDERED request carries the outer wrapper this feature's ordering scenario specifies (issue #307)
* *AND* the adapter MUST NOT read either side's Parquet row data in the planning layer — only file-level metadata and, per side, a credential reference or a vended credential cross into the scan spec
* *AND* the dimension side's storage value SHALL be serialized ONCE inside the shard-invariant common blob and MUST NOT be repeated per shard, exactly as the fact side's already is
* *AND* a join whose two sides are DELTA tables reached through Unity Catalog SHALL take this same broadcast path with no Iceberg-specific step, because the resolution seam and the broadcast decision read only neutral resolved values
<!-- /DELTA:CHANGED -->
