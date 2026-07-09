# Decision Log: fix-iceberg-spec-gaps

Date: 2026-07-09

## Interview

No live interview was conducted — this plan was produced in headless (non-interactive)
mode. The task specification stood in for the interview. The open decisions the task
delegated to planner judgment, and how they were resolved, are recorded below as design
decisions (per headless-mode conventions: assume and document conventional/bounded
choices, escalate only irreducible ones).

**Q (delegated):** Scope of gap 1 — full identity-partition value reconstruction, or a
fail-loud interim guard (error instead of NULL-fill when an identity-partition field-id is
missing from a file)?
**A (planner):** Fail-loud interim guard. See decision [1].

**Q (delegated):** Bundle gap 2 (`initial-default` for optional columns) implementation now,
or defer with an accurate tracked note?
**A (planner):** Bundle the fail-loud guard for gap 2 (same mechanism/seam as gap 1); defer
the actual default-value materialization. See decision [2].

## Design Decisions

### [1] Gap 1: fail-loud no-null-fill guard now, full partition-value reconstruction deferred

- **Decision:** Detect at scan time that an identity-partition source field-id is absent from a
  data file and return a clean error, rather than reconstructing the value from the file's
  partition metadata. The set of identity-partition source field-ids is resolved once per query
  from `TableMetadata` partition specs (`Transform::Identity` → `source_id`) and threaded
  shard-invariant into the scan spec. Full reconstruction is deferred to issue #99 / backlog BL-003.
- **Alternatives:** (a) Full rule-#1 reconstruction now — add a per-file partition tuple to
  `FileEntry`, thread the partition spec, and synthesize a typed constant column intercepting the
  default adapter. Rejected for this plan: it is a genuine feature (new per-file wire field +
  typed-literal synthesis), not a gap fix, and expands the scan-spec payload materially. (b) Leave
  as-is. Rejected: it is silent wrong data.
- **Rationale:** The defect is silent wrong data (a NULL that is not really NULL, or a misattributed
  required-missing error). The mission makes bounded correctness first-class; a clean error is
  strictly better than a wrong value and costs a fraction of the surface. The guard uses the exact
  `name_mapping` threading seam, so full reconstruction becomes a clean follow-on that flips the
  guard into a value-materialization step.
- **Promotes to ADR:** yes

### [2] Gap 2: fail-loud guard for any-nullability initial-default, materialization deferred

- **Decision:** Extend the same guard to field-ids carrying a non-null Iceberg `initial-default`
  (any nullability), so an absent such field-id errors cleanly instead of NULL-filling. Broaden the
  spec's out-of-scope text and issue #27 from "required only" to "any added field". Actual
  default-value materialization is deferred to #27 / backlog BL-004.
- **Alternatives:** (a) Implement full `initial-default` fill now. Rejected: producing a typed Arrow
  literal from the Iceberg `Literal` for every supported type is the same hard synthesis machinery
  gap 1 reconstruction needs, and belongs with it. (b) Keep #27 required-only. Rejected: factually
  wrong per the Iceberg spec — `initial-default` applies to optional fields too, which is exactly the
  silent-wrong-NULL case. (c) A separate guard field for gap 2. Rejected: one guard set with a
  `reason` tag keeps the wire compact and yields accurate per-reason errors.
- **Rationale:** Same seam, same defect class (silent wrong NULL), same deferred materialization
  machinery — bundling the two guards is cheaper than one and leaves two clean, accurately-scoped
  follow-ons.
- **Promotes to ADR:** yes

### [3] Guard both optional and required absent fields; collect identity sources across all specs

- **Decision:** The guard fires for an absent guarded field-id regardless of nullability (for a
  required field it replaces the generic required-missing message with the accurate reason), and the
  identity-source set is collected across all `partition_specs_iter()` entries, not only the default
  spec.
- **Alternatives:** Guard only optional (silent-NULL) fields; use only `default_partition_spec()`.
- **Rationale:** For a required guarded field the current error is misattributed — naming the real
  cause is strictly better. A file may have been written under an older partition spec; over-guarding
  fails loud (safe), under-guarding risks a silent wrong NULL.
- **Promotes to ADR:** no

### [4] Backlog entry text (authored here; task 5.1 writes it into specs/backlog.md)

- **Decision:** Record the exact BL-003 / BL-004 text now so the deferral is captured verbatim per
  the repo's Iceberg-spec-compliance rule; the implementer copies it into `specs/backlog.md`.
- **Promotes to ADR:** no

  **BL-003: Identity-partition source column value reconstruction (Iceberg rule #1)**
  Raised by: `fix-iceberg-spec-gaps` (2026-07-09). Status: Open — fail-loud guard shipped by
  this plan; value reconstruction deferred. Tracks issue #99. Iceberg column-projection rule #1:
  a field-id absent from a data file whose value is an Identity-Transform partition source SHALL
  be reconstructed from that file's `data_file.partition` struct. Today `FieldEntry` carries only
  `{path, size, deletes}` and `plan_files_from_table` drops the partition tuple, and the scan
  fails loud (no-null-fill guard) rather than reconstructing. To implement: carry each file's
  identity-partition value(s) on `FileEntry` (per-shard), thread the identity source_id→field-id
  mapping (shard-invariant), and synthesize a typed constant column at the guard point instead of
  erroring. Real-world driver: metadata-only Hive→Iceberg migrations whose data files omit the
  partition source column.

  **BL-004: initial-default value materialization for any added field (Iceberg rule #3)**
  Raised by: `fix-iceberg-spec-gaps` (2026-07-09). Status: Open — fail-loud guard shipped by this
  plan; value materialization deferred. Cross-references issue #27 (scope broadened here from
  required-only to any nullability). Iceberg column-projection rule #3: a field-id absent from a
  data file that carries a non-null `initial-default` SHALL surface that default (not NULL) for
  pre-add rows — for optional AND required fields. Today `LogicalField` carries no
  `initial_default` and the scan fails loud (no-null-fill guard) rather than materializing. To
  implement: thread each guarded field's `initial-default` (as a portable typed value) into the
  scan spec and synthesize the typed Arrow literal at the guard point. Shares the typed-literal
  synthesis machinery with BL-003.

## Review Findings

<!-- Populated by speq-implement after code review. -->
