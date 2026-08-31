# Feature: Work-Unit Sharding

Partitions the once-resolved data-file list into G oversubscribed work-units
("shards") and drives them across the cluster. Rather than sharding one-per-node, the
adapter sizes G to oversubscribe the cluster (G = node_count × parallelism_factor,
capped so the group set stays in Exasol's round-robin distribution regime) and emits a
fan-out. Cluster fan-out is separated from the scan itself: a tiny `LAKEHOUSE_DISTRIBUTE_FILES`
LUA SET script re-emits each shard's per-file list once per `shard_key` group so
`GROUP BY shard_key` distributes the assignments round-robin across nodes, and the
`LAKEHOUSE_SCAN` scalar EMIT UDF then scans each distributed file list node-locally
and STREAMS its rows. Because the scan is scalar (no top-level `GROUP BY`), Exasol does
not materialize the scan output. The shard-invariant common spec (including the table
root) is serialized ONCE as the scalar scan's first-argument literal; only each
shard's per-file subset flows through the distributor. Work assignment is computed
entirely in the planning layer; each scan invocation reads only its own shard of files
and no file is scanned twice.

## Background

* **This delta is issue #135. It adds ONE scenario, amends one Background bullet, and changes no sharding rule.** The shard count formula, the 300 cap, the byte-balanced disjoint partitioning, the one-shard-per-file floor, the distributor fan-out shape, the passthrough distributor, and the single-shard short circuit are all UNCHANGED.
* **SUPERSEDES the recorded Background sentence "Credentials MUST NOT appear repeated per shard; they live once in the common spec literal."** The once-per-fan-out half is UNCHANGED and is the point of this feature. The "they live once in the common spec literal" half is now wrong for a CONNECTION-supplied credential: what lives once in that literal is a REFERENCE to the Exasol CONNECTION, and the credential itself lives in the CONNECTION. A VENDED credential does still live once in the literal, under the tracked exception issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378).
* **The shard cap is why the deferral is affordable and why it is stated here.** The scan UDF now performs one engine-local `ctx.connection()` lookup per shard invocation, and G is capped at 300, so the added cost is bounded by this feature's own cap rather than by the file count.
* **`vs-adapter/scan-spec-credential-reference` owns the reference contract, the resolution, and the #378 residual.** This feature CITES it and restates none of it.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The shard-invariant literal carries one credential reference for the whole fan-out

* *GIVEN* a pushdown plan whose file list is partitioned into G work-unit shards, over a virtual schema whose CONNECTION supplies static storage credentials and does not enable `use_vended_credentials`
* *WHEN* the adapter serializes the shard-invariant common spec once as the scalar scan's first-argument literal and flows each shard's per-file subset through the distributor
* *THEN* the common literal SHALL carry exactly ONE storage reference for the whole fan-out, and the per-shard file subsets MUST NOT carry any storage value, so the reference is not repeated per shard
* *AND* the credential itself SHALL NOT appear in that literal, superseding the recorded claim that credentials "live once in the common spec literal" — a VENDED credential still does, under issue #378
* *AND* each shard invocation SHALL resolve that one reference ITSELF through `ctx.connection()`, so the resolution count is bounded by G and therefore by this feature's cap of 300 rather than by the file count
<!-- /DELTA:NEW -->
