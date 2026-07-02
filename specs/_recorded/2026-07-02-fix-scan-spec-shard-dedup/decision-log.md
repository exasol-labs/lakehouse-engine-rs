# Decision Log: fix-scan-spec-shard-dedup

Date: 2026-07-01

Tracks GitHub issue #25 (`Closes #25` in the implementing commit).

## Interview

**Q1:** Mechanism for carrying the shard-invariant part without per-shard repetition?
**A1:** Split the scan UDF into two arguments. Change the SET UDF signature from
`LAKEHOUSE_SCAN(spec VARCHAR(2000000))` to a two-argument form: one argument carries the
shard-INVARIANT common spec (serialized ONCE as a SQL literal in the SELECT list —
credentials, projection, filter, aggregates, group_keys, logical_schema, emit_exa_types, and
all `df_*`/memory tuning knobs), and the second carries only THIS shard's file-URI list (the
per-shard varying part, placed in the `VALUES` rows). The UDF merges the two JSON blobs back
into a `ScanSpec` at entry. Multi-argument UDF signatures are supported by
`exasol-udf-macros` 0.20.0 (`input(a: T, b: T, ...)`, verified in the macro crate's
`tests/annotation_typed.rs`). Applies to BOTH the row-scan/single-group fan-out path and the
grouped-aggregate fan-out path — they share `build_fan_out_inner_with_spec`.

**Q2:** Connect-back as an alternative for credentials?
**A2:** EXCLUDED. Connect-back was deliberately removed from this project by issue #32 /
ADR-048 (topology discovery moved to in-process `UdfContext::node_count()`, and the
`CONNECTION_NAME` VS property was deleted). The scan UDF opens no connection back to Exasol.
Do NOT reintroduce connect-back for credentials.

**Q3:** Scope of the dead-field cleanup?
**A3:** Full audit. Verify EVERY `ScanSpec` field against actual scan-UDF reads and drop any
dead weight, not just `catalog`. `catalog: CatalogProps` is confirmed dead (no production
readers; only tests reference it). Some tuning fields ARE read by the scan
(`df_target_partitions`, `df_batch_size`, `df_threads_per_udf`, `memory_pool_fraction`,
`instance_overhead_mb`) — keep those; they become part of the INVARIANT common blob.

## Design Decisions

### [1] Two-argument UDF: invariant common literal + per-shard files

- **Decision:** Split `LAKEHOUSE_SCAN` into `(common VARCHAR, files VARCHAR)`. The adapter
  serializes the shard-invariant common spec once as a SELECT-list literal shared by all
  shards, and places only each shard's file-URI JSON in the `VALUES` rows. `run_scan` reads
  both arguments and reconstitutes a `ScanSpec`.
- **Alternatives:** (a) connect-back to fetch credentials at scan time — rejected, connect-back
  was removed by #32/ADR-048; (b) stage the common spec as a BucketFS file referenced by all
  shards — rejected, adds state/staging to a stateless disposable UDF; (c) keep the single-arg
  form — rejected, that is the bug.
- **Rationale:** The common blob was the entire duplicated payload; emitting it once collapses
  the ~300× repetition, shrinks the statement, and reduces the credential surface. Multi-arg
  UDFs need no new infrastructure and only `Value::String` still crosses the `.so` boundary.
- **Promotes to ADR:** yes

### [2] Drop the dead `catalog` field from `ScanSpec`

- **Decision:** Remove `ScanSpec.catalog: CatalogProps` entirely. `ScanSpec` carries no
  catalog `uri`/`warehouse`/`table` after this change. `CatalogProps` the type stays — it is
  still used adapter-side for catalog interaction, just not on the UDF-boundary payload.
- **Alternatives:** Keep it for symmetry / possible future use — rejected (YAGNI; it is a
  credential-adjacent field the UDF never reads).
- **Rationale:** Field audit confirmed no production scan-UDF reader; all catalog interaction
  is adapter-side before the UDF runs. Dropping it trims the boundary payload and strengthens
  the "catalog auth never in any scan spec" guarantee to "no catalog block at all".
- **Promotes to ADR:** yes

### [3] Grouped-aggregate LIMIT-exclusion invariant preserved by a shared common blob

- **Decision:** For grouped queries, build the common blob once with `limit = None`; the
  per-shard LIMIT-strip closure is removed. Row-scan queries keep `limit` in the common blob.
- **Alternatives:** Keep the per-shard `spec_to_json` LIMIT-strip closure — rejected as
  redundant once the spec is shared.
- **Rationale:** With one shared common blob, stripping the LIMIT there guarantees the
  invariant for every shard structurally, with no per-shard mutation path to get wrong.
- **Promotes to ADR:** no

### [4] No single-argument backward-compatibility path

- **Decision:** Remove the single-argument `from_json` scan-entry path; do not dual-read.
- **Alternatives:** Keep a single-arg fallback in `from_json` — rejected.
- **Rationale:** The `.so`, SLC, and adapter deploy together and the scan SET SCRIPT DDL is
  recreated per deployment (see e2e tests); the UDF is stateless/disposable, so no in-flight
  spec crosses a version boundary. A breaking two-arg change is safe.
- **Promotes to ADR:** no

### [5] Field audit as an explicit artifact

- **Decision:** Record the full per-field audit (read-by-UDF? → invariant/per-shard/drop) in
  plan.md. Result: `files` is the sole per-shard field; `catalog` is dropped; all remaining
  fields are invariant.
- **Alternatives:** Audit only `catalog` — rejected per interview A3.
- **Rationale:** Prevents future re-litigation of which fields may vary per shard and documents
  why each tuning knob stays in the common blob.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
