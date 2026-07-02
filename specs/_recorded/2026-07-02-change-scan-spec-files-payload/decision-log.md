# Decision Log: change-scan-spec-files-payload

Date: 2026-07-02

Tracks GitHub issues #45 and #29 (`Closes #45` and `Closes #29` in the implementing commit).

## Interview

**Q1:** Per-shard `files` encoding for carrying path + size.
**A1 (chosen default — user away from keyboard):** Compact 2-tuple array, e.g.
`"files": [["data/part-00000.parquet", 84213], ["data/part-00001.parquet", 91002]]`. This is
exactly what serde produces for a `Vec<(String, u64)>`. Minimal bytes, reinforcing #45's
SQL-shrink goal. Alternatives rejected for verbosity: struct-per-file objects
(`[{"path":...,"size":...}]`) and parallel arrays (`{paths:[...],sizes:[...]}`).

**Q2:** Registration approach for supplying caller-known sizes so the scan UDF skips the
per-file `HEAD`, WITHOUT losing the field-id expr adapter
(`with_expr_adapter_factory(FieldIdExprAdapterFactory)`) that requires `ListingTable`.
**A2 (chosen default — research least-invasive path first):** Keep `ListingTable` + the
field-id adapter; supply sizes via a thin spec-backed `ObjectStore` wrapper that answers
`head()` from the spec's known sizes so DataFusion skips the network HEAD. Fall back to a
`PartitionedFile` / `FileScanConfig` rewrite ONLY if research shows `ListingTable` cannot
accept caller-supplied sizes without losing field-id projection. Whichever path is chosen,
the field-id projection behavior (feature `scan-execution-field-id-projection`) MUST be
preserved and verified against that feature's scenarios.

## Design Decisions

### [1] Compact 2-tuple `(path, size)` per-shard file encoding

- **Decision:** Change `ScanSpec.files` from `Vec<String>` to `Vec<(String, u64)>`; the JSON
  wire form is a compact array of `[path, size]` pairs. `files_json` / `files_from_json` and
  `partition_files_by_bytes` are retyped to carry the pair end-to-end.
- **Alternatives:** struct-per-file objects `[{"path":...,"size":...}]` (self-describing but
  ~3× the bytes per entry); parallel arrays `{paths:[...],sizes:[...]}` (compact but easy to
  desynchronize and awkward to shard).
- **Rationale:** The pair is the minimal payload that carries both facts, serde-native for a
  tuple, and directly reinforces #45's byte-shrink goal. Positional pairing cannot drift out
  of sync the way parallel arrays can.
- **Promotes to ADR:** yes

### [2] Carry the Iceberg table root once in the common spec; emit paths relative

- **Decision:** Add `table_root: String` (`#[serde(default)]`, empty ⇒ all-absolute) to
  `CommonScanSpec` and `ScanSpec`. The adapter threads the already-resolved
  `result.metadata.location()` out of `resolve_file_list` into the spec builder. Because it is
  shard-invariant, it is serialized ONCE in the common blob, never per shard.
- **Alternatives:** repeat the full absolute prefix on every file path (status quo — the bug);
  a separate BucketFS-staged prefix table (adds state to a stateless UDF).
- **Rationale:** The repeated ~40–70-char table-location prefix is pure per-file overhead in
  a fan-out capped at 300 shards. The root is already computed at the resolve-once seam (as the
  vended-credential anchor), so carrying it forward is free.
- **Promotes to ADR:** yes

### [3] Strip-if-prefix / absolute-passthrough path reconstruction (#45 correctness rule)

- **Decision:** In the adapter, strip `table_root` from a file path ONLY when
  `path.starts_with(table_root)`; otherwise store the absolute path unchanged. In the UDF's
  `register_files`, reconstruct symmetrically: an entry containing `://` is absolute and parses
  as-is; a relative entry is joined onto `table_root` (trailing `/` normalized) before
  `ListingTableUrl::parse`. A shard MAY mix relative and absolute entries.
- **Alternatives:** assume all data files live under `metadata.location()` and always strip /
  always join (simpler but INCORRECT).
- **Rationale:** Iceberg data-file paths are NOT guaranteed to live under
  `metadata.location()` — `write.data.path`, `write.object-storage.enabled` hash injection,
  and migrated/Databricks layouts place files elsewhere. Strip-if-prefix + absolute-passthrough
  captures the common-case win while staying correct for any layout.
- **Promotes to ADR:** yes

### [4] Supply file sizes via a spec-backed ObjectStore head() wrapper, keeping ListingTable + field-id adapter

- **Decision:** CONFIRMED against DataFusion 54.0.0 + object_store 0.13.2 source. Keep the
  existing `ListingTableConfig::new_with_multi_paths(...).with_expr_adapter_factory(FieldIdExprAdapterFactory)`
  wiring and wrap the `AmazonS3` store in a thin `ObjectStore` that overrides `head(&Path)` to
  return an `ObjectMeta` built from the spec's known size, delegating every other method to the
  inner store. Register the wrapper in the session `RuntimeEnv`'s `ObjectStoreRegistry` under the
  same `ObjectStoreUrl`. Set `last_modified = chrono::Utc.timestamp_nanos(0)` (the same epoch
  `PartitionedFile::new` uses), `size` (a `u64`), `e_tag = None`, `version = None`.
- **Why it works (verified):** for an exact-file (non-collection) URL, DF 54's
  `ListingTableUrl::list_prefixed_files` calls `store.head(&path)` per path and wraps the result
  in a single-item stream; the `head()` branch is NOT cached (only the collection/`list` branch
  uses `list_with_cache`), so the override is consulted on every query and no network HEAD is
  issued. `last_modified` is not used for scan correctness — ParquetExec reads by known size via
  `get`/`get_range`; its only consumer is the optional `FileStatisticsCache`, irrelevant to a
  per-query disposable UDF.
- **Alternatives:** `PartitionedFile::new(path, size)` (builds `ObjectMeta` with no HEAD) +
  `FileScanConfigBuilder` — VIABLE in DF 54 because `FileScanConfig` carries
  `expr_adapter_factory` and the builder exposes `with_expr_adapter(...)` (the SAME
  `PhysicalExprAdapterFactory` trait), so field-id projection is retained. Rejected as the primary
  path only because it replaces `ListingTable` wholesale (a much larger change); kept as the
  fallback. Leaving the per-file HEAD in place is the bug.
- **Rationale:** The wrapper is additive and keeps the entire existing registration + field-id
  path untouched, so `scan-execution-field-id-projection` scenarios keep passing unchanged. It
  removes N per-shard HEAD round-trips on the pre-scan critical path with a single small shim.
- **Promotes to ADR:** yes

### [5] No cross-version wire-compat decoder (stateless, single-`.so`)

- **Decision:** Change the `files` element type (String → 2-tuple) outright. Do NOT build a
  dual-format decoder that also accepts the old bare-string array. Keep `#[serde(default)]` on
  `table_root` and tolerate a size-less legacy shape only as far as the existing serde-default
  conventions make free.
- **Alternatives:** a dual-format `files` deserializer accepting both `["s3://..."]` and
  `[["path", size]]` (defensive but unnecessary).
- **Rationale:** Per mission.md the engine is STATELESS — specs are generated fresh per query
  and never persisted — and per `packaging/single-so-two-entry-points` the adapter that WRITES
  a spec and the UDF that READS it ship in ONE `.so` at the same version. No spec ever crosses
  a version boundary at runtime, so there is no migration to support. The existing
  `#[serde(default)]` tests are about robustness/consistency, not persisted-data migration.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
