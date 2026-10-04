# Decisions: add-type-relaxation

## ADR: The recorded `.without_row_transforms()` correctness hole does not exist

**ID:** delta-without-row-transforms-hole-does-not-exist
**Plan:** `add-type-relaxation`
**Status:** Accepted

### Context

`vs-adapter/delta-reader-feature-gating` recorded that `DeltaSnapshot::active_files` builds its
kernel scan with `.without_row_transforms()`, so no per-file cast transform is applied, and a
widened column is read at each older data file's OLD physical Parquet type against the table's NEW
logical type — wrong values, no error. `delta_kernel` 0.26's own documentation scopes that call to
partition-column injection, column-mapping renames, and generated row ids, and the kernel implements
no type-widening cast anywhere — its `TableFeature::TypeWidening` handling is a capability
declaration and a schema-comparison validator, never a cast. There was no cast transform for that
call to discard.

### Decision

Supersede the recorded claim. The widening cast is performed by this engine's own format-neutral
adapter chain: `register_file_list` registers the DataFusion table schema from the scan spec's
logical schema rather than from a Parquet footer, `bind_columns` renames without comparing data
types, and DataFusion's `DefaultPhysicalExprAdapter` inserts a `CastExpr` on any field inequality.

### Options Considered

| Option | Verdict |
|--------|---------|
| Supersede the recorded claim and attribute the cast to the existing adapter chain | ✓ Chosen — verified directly against `delta_kernel` 0.26 source and docs |
| Keep the recorded justification and treat this plan as adding the missing cast | ✗ Rejected — would build a duplicate of a mechanism that already exists and is already recorded as delegating type divergence to `DefaultPhysicalExprAdapter` |
| Stop using `.without_row_transforms()` and take the kernel's transforms instead | ✗ Rejected — the kernel has no widening cast to take, while re-introducing partition-column and column-mapping handling this engine deliberately owns |

### Consequences

Leaving the false claim recorded would have sent the next reader to build a cast layer this plan
proves unnecessary. Superseding it lets the plan stay verification-first: tests over the existing
chain rather than new infrastructure.

## ADR: Iceberg `date` → `timestamp` / `timestamp_ns` is refused at plan time from the schema history

**ID:** refuse-iceberg-date-promotion-from-schema-history
**Plan:** `add-type-relaxation`
**Status:** Accepted

### Context

The Iceberg spec requires a manifest bound's write-time type to be inferred from its byte width.
`iceberg` 0.10.0 implements that inference for `long`-from-4-bytes and `double`-from-4-bytes but
reads `timestamp` and `timestamp_ns` bounds as 8 bytes unconditionally, so a pre-promotion file's
4-byte bound fails `bytes.try_into()`. The failure sits inside Avro deserialization inside manifest
decode, so it fires for an unfiltered `SELECT *` as well as a filtered query — this engine loads
every manifest in `ensure_supported_delete_mechanisms` before pruning runs — and a second bounds
decode in the same crate `unwrap()`s, giving the shape a reachable panic path; a panic in a UDF makes
the engine SIGKILL every sibling VM of the statement part.

### Decision

Refuse the two Iceberg `date` promotions with a `UdfError` naming the table, the column, both
Iceberg types, and a tracked issue, decided from `TableMetadata::schemas_iter` before any manifest is
loaded. Delta's `date` → `timestampNtz` stays supported, because the asymmetry lives in the metadata
format (typed JSON per-file stats for Delta versus untyped Avro byte buffers for Iceberg), not in the
read path.

### Options Considered

| Option | Verdict |
|--------|---------|
| Plan-time refusal from schema history, before any manifest read | ✓ Chosen — cheap, fires for filtered and unfiltered queries alike, avoids the panic path entirely |
| Support the promotions by vendoring the missing bounds-width inference | ✗ Rejected — a dependency fork for two promotion pairs |
| Catch and re-word the manifest decode error | ✗ Rejected — sits downstream of the panic path and depends on an error string |
| Leave the opaque failure in place | ✗ Rejected — surfaces `failed to convert byte slice to array`, naming neither column nor promotion |

### Consequences

A `date`-promoted table gets a named, scoped refusal instead of an opaque decode error or a
reachable panic. The gate is conservative — it refuses on the recorded promotion alone, without
checking whether a pre-promotion file survives, because proving that requires the manifest read that
fails.

## ADR: Iceberg `unknown` → any type is recorded as unreachable, with a build tripwire, and no gate

**ID:** iceberg-unknown-type-unreachable-build-tripwire
**Plan:** `add-type-relaxation`
**Status:** Accepted

### Context

`iceberg` 0.10.0's `PrimitiveType` has 16 variants and none is `Unknown`; the type name has no
`serde` arm, so a v3 schema declaring `"unknown"` fails table-metadata deserialization before any
engine code runs. `iceberg_primitive_to_arrow` and `iceberg_primitive_to_exasol` are already
exhaustive over `PrimitiveType` with no catch-all arm.

### Decision

Write no refusal arm and no mapping arm for `unknown`. Record it as a tracked exception citing its
own issue (linking upstream `apache/iceberg-rust#2581`), and pin the exhaustiveness of both mapping
functions with a test so a dependency upgrade that adds the variant breaks the build rather than
silently falling through to the `utf8`/`VARCHAR` fallback.

### Options Considered

| Option | Verdict |
|--------|---------|
| No gate, no mapping arm; pin exhaustiveness as the tripwire; track as an exception | ✓ Chosen — a gate would be unreachable from its first commit at the pinned dependency version |
| Write a named refusal for `unknown` | ✗ Rejected — unreachable dead code today |
| Upgrade `iceberg` within this plan to gain the variant | ✗ Rejected — out of scope; this plan verifies existing behavior, it does not chase a dependency upgrade |

### Consequences

The exhaustiveness pin is a stronger guarantee than a runtime refusal: a future `iceberg` upgrade
that adds `Unknown` (or `variant`, `geometry`, `geography`) fails the build instead of silently
mis-mapping the new variant.

