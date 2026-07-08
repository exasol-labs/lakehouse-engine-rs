# Decision Log: change-name-mapping-fallback

Date: 2026-07-08

## Interview

**Q1 (scope: drop+rename-reuse collision).** The comment in `rename_physical_to_logical`
(`crates/lakehouse-engine/src/scan/mod.rs`) says name collisions from
drop+rename-into-a-reused-name are "out of scope and belong to the name-mapping work tracked
in issue #28". Should this plan also fix that collision case?
**A:** That premise is WRONG. `schema.name-mapping.default` maps *physical column names as
they currently appear in a data file* to field-ids — it cannot retroactively disambiguate a
dropped column whose old physical name was later reused by an unrelated new column, because
the mapping only reflects current-state naming, not history. Name-mapping does NOT solve the
collision case. Action: correct the misleading comment to state plainly that this is a
distinct, still-open concern unrelated to name-mapping (not "out of scope pending #28"). Do
NOT attempt to fix the collision itself; no follow-up issue is required for it.

**Q2 (nested/struct name-mapping entries).** Iceberg's name-mapping format supports nested
`fields` entries for structs/maps/lists. Parse them?
**A:** Flat/top-level entries only for this plan. Parse only the top-level list of
`{names, field-id}` objects; do not recurse into nested `fields`. Rationale: this engine's
Exasol type mapping flattens nested Iceberg types to VARCHAR-via-JSON rather than exposing
real nested columns, so nested name-mapping entries would be unused even if parsed. A
follow-up GitHub issue (#83, "Support nested/struct entries in schema.name-mapping.default
parsing", label `feature`) tracks the deferred scope.

**Q3 (resolution order when name-mapping exists but doesn't cover a field).** Told to honor
the official Iceberg spec (`#column-projection`) rather than guess.
**A:** The spec defines four rules for a field id absent from a data file: (1) partition
Identity-Transform substitution, (2) `schema.name-mapping.default`, (3) `initial-default`,
(4) null. Rules #1 and #3 are NOT implemented anywhere in this codebase, are not requested by
#28, and are OUT OF SCOPE (noted for traceability). This plan implements ONLY rule #2, as a
NEW resolution step slotted BETWEEN the existing embedded-field-id match (highest priority,
unchanged) and the existing physical-name-match fallback (already shipped). Final order for a
physical field with no embedded field-id: (1) name-mapping maps its physical name to a
field-id present in the logical schema → resolve via that field-id (new); else (2) fall
through to the existing physical-name-match fallback (name-mapping augments, does not replace,
the shipped fallback); else (3) existing nullable/required handling applies unchanged. The
field-id-projection spec's assertion that the UDF "MUST NOT parse or honor any table-level
name-mapping property" becomes false and must be delta'd.

## Design Decisions

### [1] Reuse the `iceberg` crate's `NameMapping` deserializer to parse the property

- **Decision:** Parse `schema.name-mapping.default` in the VS with
  `serde_json::from_str::<iceberg::spec::NameMapping>` (confirmed exported at
  `iceberg::spec::{NameMapping, MappedField, DEFAULT_SCHEMA_NAME_MAPPING}` in the pinned
  `v0.10.0-rc.2`/`be6cc96` checkout), rather than hand-rolling a JSON parser.
- **Alternatives:** A bespoke serde struct for the property.
- **Rationale:** The crate ships a spec-accurate, tested deserializer (kebab-case `field-id`,
  optional `field-id`, `DefaultOnNull` for `fields`, nested children). Reuse over reinvention;
  it also tracks the Iceberg spec as the pin advances.
- **Promotes to ADR:** yes

### [2] Store a flat `Vec<NameMappingEntry { name, field_id }>` in the scan spec

- **Decision:** Flatten the parsed top-level entries (one `{name, field_id}` per name in an
  entry's `names`, entries without a `field-id` skipped) into a compact
  `Vec<NameMappingEntry>` carried in `CommonScanSpec`/`ScanSpec`/`JoinSpec`, mirroring
  `logical_schema`. The scan builds a `HashMap<&str, i32>` from it for lookup.
- **Alternatives:** Thread the raw JSON string and re-parse in the UDF; carry the nested
  `iceberg::spec::NameMapping` type over the wire.
- **Rationale:** The repo rule requires parsing once in the VS (not per UDF invocation). A
  flat name→field-id list is the exact lookup shape the resolver needs and is wire-compact;
  nested entries are unused (Decision [4]).
- **Promotes to ADR:** no

### [3] Name-mapping resolution order: strictly between embedded field-id and physical name

- **Decision:** In `rename_physical_to_logical`, the name-mapping step applies ONLY to a
  physical field that carries no embedded `PARQUET:field_id`, is tried AFTER embedded-field-id
  resolution and BEFORE the physical-name fallback, and augments (never replaces) that
  fallback.
- **Alternatives:** Consult the name-mapping for every field (even those with an embedded
  id); replace the physical-name fallback entirely with name-mapping.
- **Rationale:** Iceberg column-projection rule #2 scopes name-mapping to files "without field
  id information"; an embedded id is authoritative. Augmenting preserves all current behavior
  for the no-mapping and uncovered-field cases.
- **Promotes to ADR:** yes

### [4] Flat/top-level entries only; nested struct/map/list entries deferred

- **Decision:** Parse only top-level mapping objects; ignore nested `fields`. Deferred scope
  tracked by follow-up issue #83.
- **Alternatives:** Recurse into `fields` now.
- **Rationale:** The engine flattens nested Iceberg types to VARCHAR-via-JSON, so nested
  name-mapping entries would never be consulted; parsing them now is dead capability.
- **Promotes to ADR:** yes

### [5] Iceberg column-projection rules #1 and #3 remain unimplemented and out of scope

- **Decision:** Do not implement partition Identity-Transform substitution (rule #1) or
  `initial-default` values (rule #3); note their existence as adjacent, currently-unimplemented
  spec rules in the feature's out-of-scope background.
- **Alternatives:** Implement them alongside rule #2.
- **Rationale:** Neither exists anywhere in the codebase today, neither is requested by #28;
  documenting them preserves traceability without scope creep.
- **Promotes to ADR:** no

### [6] Malformed present `schema.name-mapping.default` → clean plan-time error

- **Decision:** When the property is present but not valid name-mapping JSON, fail the query
  at plan time in the VS with a clean, credential-free error naming the property. Absent
  property → empty mapping (no error).
- **Alternatives:** Silently ignore a malformed value and fall through to the physical-name
  fallback.
- **Rationale:** Consistent with the repo's fail-loud-at-plan-time correctness gates
  (`ensure_supported_delete_mechanisms`): a malformed mapping is a real config error and
  should surface once, in the VS, rather than silently degrade column binding. Surfacing it in
  the VS (resolve-once) keeps it off the per-shard hot path.
- **Promotes to ADR:** yes

### [7] The drop+rename-into-a-reused-name collision is unrelated to name-mapping

- **Decision:** Correct the `rename_physical_to_logical` comment: this collision is a distinct,
  still-open concern that name-mapping does NOT resolve (the mapping reflects current-state
  physical naming, not history). No fix and no follow-up issue for the collision itself.
- **Alternatives:** Leave the comment implying #28 resolves it; file a tracking issue.
- **Rationale:** Repo owner confirmed the original premise was wrong and asked for the comment
  correction only; a false forward-reference misleads future planners.
- **Promotes to ADR:** yes

## Review Findings

<!-- Populated by speq-implement after code review. -->
