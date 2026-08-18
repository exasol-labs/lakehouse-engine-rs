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

## ADR: The vendored `type-widening` fixture is not modified; `seed.sh` is extended instead

**ID:** extend-seed-sh-not-vendored-type-widening-fixture
**Plan:** `add-type-relaxation`
**Status:** Accepted

### Context

The plan's interview assumed the vendored `type-widening` Delta fixture needed its log rewritten to
genuinely widen columns across commits. That premise was wrong: commit 2 already widens all thirteen
columns in `metaData.schemaString` and records each under `delta.typeChanges`; commit 0's data file
is physically narrow and commit 2's is physically wide; both files are referenced by `add` actions
and neither is orphaned. The only real gap is Unity Catalog registration — `scripts/unity/seed.sh`
registers 3 of the fixture's 13 columns, and a column absent from it is not selectable from Exasol.

### Decision

Change no byte of `scripts/unity/fixtures/type-widening/`, and instead register all thirteen of its
columns in `scripts/unity/seed.sh` at their widened types.

### Options Considered

| Option | Verdict |
|--------|---------|
| Extend `seed.sh` to register all 13 columns, leave the fixture untouched | ✓ Chosen — the fixture is already the straddling shape the plan needs |
| Rewrite the fixture's Delta log to widen columns across commits, per the interview's original premise | ✗ Rejected — the premise was factually wrong, and `PROVENANCE.md` records these tables as "read fixtures — never mutated" |

### Consequences

Full thirteen-column E2E coverage of the fixture is unlocked with no fixture byte changed and no
vendoring contract broken.

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

## ADR: The Iceberg `date` E2E fixture is dropped; the plan's pre-authorized fallback applies

**ID:** drop-iceberg-date-promotion-e2e-fixture-fallback
**Plan:** `add-type-relaxation`
**Status:** Accepted

### Context

The plan originally scoped a second Spark-authored Iceberg fixture table for the `date` →
`timestamp` promotion. Implementation of task 6.1 found that Apache Iceberg Java itself never
implements this promotion — `TypeUtil.isPromotionAllowed` is byte-identical across
`apache-iceberg-1.10.1`, `apache-iceberg-1.11.0`, and `main`, and switches on `INTEGER`, `FLOAT`,
`DECIMAL` only. `ALTER TABLE … ALTER COLUMN … TYPE` from `date` fails outright at every runtime
version this stack can use. A raw REST metadata commit (`add-schema`/`set-current-schema`) was
proven to produce the exact schema-history shape the refusal reads, with the expected 4-byte
pre-promotion manifest bound, but Iceberg Java's own Spark reader then fails the same table with
`TimeStampMicroVector cannot be cast to DateDayVector` — no conforming Iceberg writer or reader
produces or reads this shape today.

### Decision

Author only the readable-promotion fixture table (`iceberg_type_promotion`: `int`→`long`,
`float`→`double`, `decimal(10,2)`→`decimal(20,2)`) via Spark. Do not author the
`iceberg_date_promotion` table or its E2E tests. Coverage for the `date` → `timestamp` /
`timestamp_ns` refusal rests entirely on unit tests over a synthetic `TableMetadata`, which already
exercise `refuse_date_promotion` directly and do not depend on a live fixture.

### Options Considered

| Option | Verdict |
|--------|---------|
| Drop the second fixture; rely on unit-test coverage for the refusal | ✓ Chosen — the plan's own interview pre-authorized this exact fallback when live E2E proves infeasible |
| Author the `date` promotion via a raw REST `add-schema` + `set-current-schema` commit, bypassing Spark's `ALTER TABLE` | ✗ Rejected — produces a table no conforming Iceberg reader (including Iceberg Java's own Spark reader) can read, and adds a REST-commit code path this plan's Non-Goals otherwise avoid |
| Keep trying to make Spark author it | ✗ Rejected — Iceberg Java never implements this promotion at any version this stack can run |

### Consequences

The refusal's coverage is unit-only rather than unit-plus-live-E2E, which is an explicit, recorded
scope reduction rather than a silent gap — the exact contingency the plan's interview pre-authorized.
No conforming writer or reader exists for the shape a live fixture would need to exercise.

## ADR: This project's issues cite upstream tracking where it exists, and nothing is filed upstream

**ID:** cite-upstream-tracking-file-nothing-upstream
**Plan:** `add-type-relaxation`
**Status:** Accepted

### Context

This plan files two new tracked-exception issues: the Iceberg bounds-width gap and Iceberg `unknown`
support. A `gh search issues --repo apache/iceberg-rust` run during planning matched
`apache/iceberg-rust#2581` (an open enhancement adding the missing `Unknown` primitive type, a
sub-item of the `#2411` v3-support epic) exactly to the `unknown` tripwire's blocker. The same search,
run with numerous further terms over issues and PRs, surfaced nothing tracking the bounds-width gap.

### Decision

Link `apache/iceberg-rust#2581` from this project's `unknown` issue and from the `unknown` tracked
exception in `vs-adapter/iceberg-type-promotion`. State in the bounds-width issue that the upstream
search found no existing `apache/iceberg-rust` issue or PR tracking that gap, as of the search run
during planning, and leave the `date`-to-`timestamp` refusal citing this repository's issue alone.
File nothing on `apache/iceberg-rust`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Cite existing upstream tracking where found; state "none found" where a search turned up nothing; file nothing upstream | ✓ Chosen — points the `unknown` issue at the work that actually unblocks it, and respects this project's no-upstream-filing rule |
| Cite no upstream issue and duplicate tracking that already exists | ✗ Rejected — `#2581` already tracks exactly the `unknown` blocker |
| File a new bug report upstream for the bounds-width gap | ✗ Rejected — this project does not file issues on third-party repositories |

### Consequences

The `unknown` issue stays correctly linked to its actual upstream blocker instead of duplicating
tracking. The bounds-width issue is honest that an absence of found tracking is not proof no such
tracking exists.
