# Decision Log: add-deletion-vector-application

Date: 2026-07-07

## Interview

**Q1 — DV bitmap decoder source:** Track an unmerged upstream iceberg-rust DV branch, or hand-roll
our own `deletion-vector-v1` decoder?
**A:** Hand-roll our own decoder. iceberg-rust is pinned by release tag (`v0.10.0-rc.2`) per the
project's crate-version discipline; the upstream DV work (apache/iceberg-rust #2414, #2681) is
open/unmerged and could still change shape; and issue #12 itself names "implement our own Puffin DV
apply path" as the fallback. We decode the `deletion-vector-v1` blob ourselves using the `roaring`
crate, which is already a direct workspace dependency (`roaring = 0.11`, consumed by
`scan/positional_deletes.rs`). No new dependency.

**Q2 — Mixed-mechanism scope:** Is a single scan invocation with some data files under Parquet
positional deletes and others under Puffin DVs in scope?
**A:** Yes, in scope. A table upgraded to format-version 3 without a full rewrite keeps old data
files under v2-style position deletes while newer deletes use DVs; the v3 spec guarantees at most
one DV per data file, not per table. Per-data-file delete resolution must pick its mechanism
independently per file, and this needs an explicit e2e fixture/test covering the mixed case (not
just DV-only and positional-only tables in isolation).

**Q3 — Cardinality validation:** Trust the decoded bitmap, or validate against the Puffin
`BlobMetadata.properties["cardinality"]` and fail loud on mismatch?
**A:** Validate, fail loud on mismatch. After decoding the bitmap, cross-check its actual count
against the declared `cardinality` and fail loud (clean, credential-redacted error) on mismatch,
consistent with the project's fail-loud posture (manifest gate, read-time backstop) and the
mission's "correctness and safety guards are first-class" constraint. A mismatch signals a corrupt
Puffin file or our own parser bug — silent misapplication is exactly the silent-correctness failure
mode this effort line (#11 → #68/#72 → #12) exists to close.

**Q4 — GitHub issue:** New issue, or existing?
**A:** Existing — issue #12 ("Read Iceberg v3 deletion vectors (Puffin)"), which narrows #11.
Reference `Closes #12` in the implementing commit per CLAUDE.md's feature-tracking convention; no
new issue needed.

## Design Decisions

### [1] Hand-roll the deletion-vector-v1 decoder rather than depend on upstream

- **Decision:** Decode the `deletion-vector-v1` Puffin blob payload ourselves (magic, big-endian
  length, portable Roaring vector, big-endian CRC-32) on top of iceberg-rust's `PuffinReader`
  (which handles only the file container, footer parse, and blob decompression).
- **Alternatives:** Track/vendor apache/iceberg-rust #2681's DV-read branch; bump iceberg-rust to a
  version that reads DVs (none exists at a pinned release).
- **Rationale:** Version is pinned by release tag; upstream is unmerged and could change; #12 names
  our own path as the intended fallback; `roaring` and `crc32fast` are already available.
- **Promotes to ADR:** yes

### [2] Reuse the positional-delete union point and RowSelection/ParquetAccessPlan machinery

- **Decision:** A decoded DV becomes a per-data-file `RoaringTreemap` fed into the SAME
  `access_plan_for_data_file` union point and `build_deletes_row_selection`/`build_access_plan`
  path the positional-delete feature already uses, dispatched per delete ref on `content_type`.
- **Alternatives:** A separate DV-specific access-plan builder; applying DVs via a distinct
  DataFusion filter stage rather than the base `ParquetAccessPlan`.
- **Rationale:** A DV-derived selection is indistinguishable downstream from a positional-delete
  one, so DVs compose with projection/filter/LIMIT/pruning for free and mixed shards fall out
  naturally; avoids duplicating correctness-critical selection logic.
- **Promotes to ADR:** yes

### [3] Source DV references from the manifest walk, not FileScanTask.deletes

- **Decision:** Extract DV coordinates (`content_offset`, `content_size_in_bytes`, and
  `referenced_data_file`) from the manifest / `DataFile`-level walk that already gates unsupported
  mechanisms. `referenced_data_file` is used ONLY to associate each DV with its data file's entry;
  it is NOT serialized onto the wire (see decision [8]) — the association is structural on the data
  file's `deletes` list, and the decoder cross-checks it from the Puffin `BlobMetadata` at read time.
- **Alternatives:** Wait for iceberg-rust `plan_files`/`FileScanTask.deletes` to surface DV files.
- **Rationale:** iceberg-rust has no DV scan-planning; `plan_files` drops the Puffin discriminator
  and DV coordinates; the manifest walk is the only place they survive and it already runs for the gate.
- **Promotes to ADR:** yes
- **E2E CORRECTION (discovered during `make test-e2e` against a real Spark v3 fixture):** the
  premise that `FileScanTask.deletes` does NOT surface DV files was only half true. iceberg-rust
  0.10 DOES surface the Puffin deletion-vector delete file in `FileScanTask.deletes` (as a
  position-delete-typed entry) even though it drops the coordinates. The manifest walk is still the
  authoritative source for DV coordinates, but the positional-delete producer
  (`plan_files_from_table`) must EXCLUDE any delete file whose path is a DV Puffin container already
  collected by the manifest walk — otherwise the same Puffin file is emitted BOTH as a mis-typed
  POS_DEL ref (which then opens the Puffin footer as Parquet → "Corrupt footer") AND as the correct
  DV ref. Fix: thread the manifest walk's DV container-path set into `plan_files_from_table` and skip
  those paths (Phase 2b task R.6). Host integration tests did not catch this because they built the
  scan spec by hand rather than driving iceberg-rust's real `plan_files`.

### [4] Validate cardinality, magic, and CRC; fail loud on any mismatch

- **Decision:** The decoder verifies the `D1 D3 39 64` magic, the trailing CRC-32, and that the
  decoded position count equals the declared `cardinality`, returning a clean credential-redacted
  error on any mismatch before emitting a row.
- **Alternatives:** Trust the bitmap; log-and-continue on mismatch.
- **Rationale:** A mismatch means a corrupt Puffin file or a parser bug; silent misapplication is
  the exact silent-correctness failure this effort exists to close (Q3).
- **Promotes to ADR:** yes

### [5] DV support as new sibling features plus narrowing deltas on the positional-delete features

- **Decision:** New features `scan-execution-deletion-vectors`, `deletion-vector-fixtures`, and
  `e2e-harness-deletion-vectors`; narrowing deltas on `pushdown-file-pruning`,
  `scan-execution-spec-reconstitution`, `scan-execution-positional-deletes`, and
  `e2e-harness-positional-deletes` to move DVs out of their "unsupported" sets.
- **Alternatives:** Fold all DV behavior into the existing positional-delete features.
- **Rationale:** Mirrors how #11 was narrowed into standalone positional-delete features; keeps each
  feature single-purpose while the shared union point is reused (not duplicated) and the three
  fail-loud gates (plan-time, wire, read-time) are each narrowed exactly where DV was listed.
- **Promotes to ADR:** no

### [6] Grow DeleteFileRef with optional DV coordinates rather than a sibling struct — SUPERSEDED by [8]

- **SUPERSEDED:** This decision was replaced during plan review by decision [8] (normalized
  interned per-shard wire). The flat "grow `DeleteFileRef` with optional coordinate fields" shape
  was rejected as inconsistent (partition-granularity positional deletes duplicate a delete ref
  across every data file they cover while DVs would use a `referenced_data_file` back-reference) and
  non-compact (each physical delete file's path is repeated per data file — catastrophic when many
  data files share one packed Puffin DV container). Retained here for history; do NOT implement.
- **Decision (superseded):** Add `referenced_data_file`, `content_offset`, `content_size_in_bytes`
  as optional fields on `DeleteFileRef`, discriminated by the existing `content_type`, preserving
  the untagged `FileEntryWire` legacy 2-tuple/3-tuple serde.
- **Alternatives:** A separate `DeletionVectorRef` type; a tagged enum of delete-ref variants.
- **Rationale (superseded):** Keeps one flat wire shape and the existing backward-compatible
  fallback with minimal churn; the `content_type` discriminator already distinguishes mechanisms.
- **Promotes to ADR:** no

### [7] Precondition: depends on PR #72 merged to main

- **Decision:** Record #72 as a hard precondition; do not start implementation until it lands on
  `main`.
- **Alternatives:** Author on top of the unmerged branch.
- **Rationale:** Every seam this plan extends (backstop, gate, `DeleteFileRef`, union point,
  `mor_dv_unsupported` fixture) is introduced by #72; the plan is authored before the merge.
- **Promotes to ADR:** no

### [8] Normalized interned per-shard wire (deleteFiles pool + df-indexed refs) — SUPERSEDES [6]

- **Decision:** The per-shard argument (UDF arg 1) is a JSON OBJECT with two arrays: an interned
  `deleteFiles` pool (each physical delete file/container interned EXACTLY ONCE per shard, carrying
  `path`, `size`, `type` = `POS_DEL`/`EQ_DEL`/`DV`, `format` = `PARQUET`/`AVRO`/`ORC`/`PUFFIN`,
  serialized SCREAMING_SNAKE_CASE) and a `dataFiles` list whose entries carry `path`, `size`, and an
  OPTIONAL `deletes` array (omitted/empty when the data file has no deletes). Each `deletes` entry
  is a reference object: `df` (integer index into `deleteFiles`) plus OPTIONAL `offset`/`length`,
  present only for a blob-addressed DV inside a Puffin container and ABSENT for whole-file
  POS_DEL/EQ_DEL. `referenced_data_file` is DROPPED from the wire — the association is structural
  (it lives on the data file's `deletes` list). The pool is scoped PER-SHARD: a delete file spanning
  N shards appears once in each of those N shards' pools (NOT placed in the shard-invariant common
  blob, arg 0). This is a clean break from the untagged `FileEntryWire` 2-tuple/3-tuple serde, which
  is retired along with the flat `DeleteFileRef` and the `DeleteFileContentType` enum.
- **Alternatives:**
  - Grow the flat `DeleteFileRef` with optional DV coordinate fields, keeping the untagged tuple
    serde (former decision [6]) — REJECTED: inconsistent (partition-granularity positional deletes
    duplicate a delete ref across every data file they cover while DVs use a `referenced_data_file`
    back-reference) and non-compact (each physical delete file's path is repeated per data file —
    catastrophic when e.g. 10,000 data files share one packed Puffin DV container), and it does not
    extend cleanly to other formats/mechanisms.
  - Place the interned pool in the shard-invariant common blob (arg 0) — REJECTED: breaks shard
    self-containment; each shard must carry exactly the delete files its own data files reference.
  - A separate `DeletionVectorRef` type; a tagged delete-ref enum.
- **Rationale:** The normalized shape is clear and unambiguous, consistent across every delete
  mechanism, compact (each physical delete file's path is interned once per shard regardless of how
  many data files reference it), and extensible via the `type`+`format` split. No correctness is
  lost by dropping `referenced_data_file`: the DV decoder cross-checks the blob's
  referenced-data-file against the Puffin `BlobMetadata` at read time. There is no cross-version
  wire-compat requirement (the same `.so` produces and consumes the spec within one deploy), so the
  legacy tuple serde is retired outright.
- **Promotes to ADR:** yes

### [9] Reuse the normalized file-set shape for the merged join feature's dimension side

- **Decision:** `main` merged the broadcast inner-join pushdown feature (#71) after this plan was
  authored. That feature added a shard-invariant `JoinSpec` block to `CommonScanSpec` (UDF arg 0)
  whose broadcast dimension side is `JoinSpec.files: Vec<FileEntry>` and MAY carry its own
  positional-delete `DeleteFileRef`s. Because this plan retires `FileEntry`, `DeleteFileRef`,
  `DeleteFileContentType`, and `FileEntryWire` (decision [8]), `JoinSpec.files` — a second consumer
  of exactly those types — must migrate to the SAME normalized `{deleteFiles, dataFiles}` file-set
  shape. The join dimension side reuses the interned pool + `df`-indexed refs identically to the
  per-shard fact side; its pool is scoped to the (shard-invariant) join block in arg 0, keeping the
  block self-contained. As a free consequence the dimension side gains DV support on the same path.
- **Alternatives:**
  - Keep `FileEntry`/`DeleteFileRef` alive solely for `JoinSpec` alongside the new normalized shape —
    REJECTED: two parallel file-encodings for the same concept, defeating the uniformity that
    motivated decision [8], and leaving the retired-type dead-code removal half-done.
  - Rebase this plan onto the pre-join `main` and defer the join reconciliation — REJECTED: #71 is
    already merged; the wire rewrite would not compile against current `main`.
- **Rationale:** One normalized file-set shape for both the fact (per-shard, arg 1) and dimension
  (shard-invariant, arg 0) sides is consistent, keeps the retirement complete, and requires no new
  concepts — the join dimension side is just another file list with optional deletes. The
  delete-classification code this plan relaxes was untouched by #71, so only the wire layer needs
  reconciling.
- **Promotes to ADR:** no (scoped reconciliation of decision [8] against a concurrently-merged feature)

## Review Findings

<!-- Populated by speq-implement after code review. -->
