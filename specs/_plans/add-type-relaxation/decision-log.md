# Decision Log: add-type-relaxation

## Interview

**Q:** Initial research concluded a new per-file schema-reconciliation cast layer would be needed so
the scan widens an old file's narrow physical type before DataFusion filters, joins, or aggregates
run on it. Is that right?

**A:** No — the per-file cast already happens. DataFusion's `DefaultPhysicalExprAdapter` handles it
and the engine already delegates to it. `FieldIdExprAdapterFactory`
(`field_id_projection.rs:108-151`) composes around `DefaultPhysicalExprAdapterFactory`: it feeds the
default adapter a physical schema renamed to logical names, so column binding by
field-id / physical-name / identity works, and the default adapter does the rest — including
automatic type casting when the physical and logical types diverge.
`DefaultPhysicalExprAdapter::rewrite_column` resolves the logical column to its physical counterpart
by name, checks whether the fields match, and when they do not, validates type compatibility and
wraps the column in a `CastExpr` from the physical to the logical type. That cast is inserted into
the physical expression tree, so filters, projections, and aggregates all see the cast column, and it
happens at Parquet-read time before DataFusion evaluates anything. The logical schema — from the
Delta log or Iceberg metadata — carries the widened type; the physical Parquet file carries the old
narrow one; `bind_columns` renames the physical field to the logical name; the default adapter sees
the mismatch and inserts the cast. What the plan needs to VERIFY, not build: (1) that the logical
schema the engine constructs carries the current widened type — `snapshot.schema()` for Delta and
`table.metadata().current_schema()` for Iceberg, both of which return the latest evolved schema;
(2) that `arrow::compute::cast` supports every pair in the union table; (3) that the Exasol `EMITS`
declaration also uses the widened type, so `coerce_batch_to_exa_types` is a no-op or a compatible
cast. No new per-file cast layer is needed. The work is verification and testing, not new
infrastructure. Re-verify this against the actual current source rather than taking it as gospel.

**Q:** Delta fixture scope — extend the vendored `type-widening` fixture and `scripts/unity/seed.sh`
to register all missing pairs for full live E2E coverage, or keep the existing partial fixture and
cover the gaps by unit test only?

**A:** Extend the fixture and `seed.sh`. Register the columns that research reported unregistered
(`byte_long`, `short_long`, `int_long`, `float_double`, `decimal_decimal_same_scale`,
`decimal_decimal_greater_scale`, `date_timestamp_ntz`) so all pairs get live E2E coverage against the
Unity stack, in addition to confirming the three already-registered columns still pass.

**Q:** Iceberg fixture scope — build a new Spark one-shot fixture for true Iceberg promotion E2E
parity with Delta, or rely on unit tests only and defer a live fixture?

**A:** Add a new Spark one-shot fixture, following the existing `scripts/spark-fixtures/` pattern
(`create_int96_timestamp_fixture.sql` as the model): add a column at the old type, write data, evolve
the schema to the new type, write more data, read both — giving Iceberg promotion true E2E parity
with Delta in this plan.

**Q:** Are the Iceberg v3-only pairs (`date` → `timestamp`, `date` → `timestamp_ns`, `unknown` → any
type) in scope now, or contingent on confirming the engine reads Iceberg v3 tables at all?

**A:** In scope regardless, but with a fallback. They MUST be verified or implemented in this plan
rather than deferred to a follow-up as an exception; but if live E2E against a real v3 table proves
infeasible, fall back to unit-test coverage for those specific pairs rather than blocking the whole
plan on baseline v3-table support. If baseline v3 support turns out to be a real prerequisite gap,
flag it clearly rather than silently building it as an unacknowledged side effect.

## Design Decisions

### [1] The recorded `.without_row_transforms()` correctness hole does not exist

- **Decision:** Supersede `vs-adapter/delta-reader-feature-gating`'s Background claim that
  `.without_row_transforms()` leaves a widened column read at its old physical type. `delta_kernel`
  0.26 documents that call as scoping out partition-column injection, column-mapping renames, and
  generated row ids, and implements NO type-widening cast anywhere — its `TableFeature::TypeWidening`
  handling is a capability declaration and a schema-comparison validator. There was no cast transform
  to discard. The widening cast is performed by this engine's own format-neutral adapter chain.
- **Alternatives:** Keep the recorded justification and treat this plan as adding the missing cast;
  or stop using `.without_row_transforms()` and take the kernel's transforms instead.
- **Rationale:** The first would build a duplicate of a mechanism that already exists and is already
  recorded, in `datafusion-scan/scan-execution-field-id-projection`, as delegating type divergence to
  `DefaultPhysicalExprAdapter`. The second would buy nothing — the kernel has no widening cast to
  take — while re-introducing partition-column and column-mapping handling this engine deliberately
  owns. Leaving the false claim recorded is the real hazard: it would send the next reader to build
  the layer this plan proves unnecessary.
- **Promotes to ADR:** yes

### [2] The plan is verification-first; no cast infrastructure is added

- **Decision:** Add no relaxation-aware layer to the scan, the emit path, or the column-binding
  adapter. The deliverables are tests over the existing chain, two plan-time gates, an allow-list
  change, and fixture coverage.
- **Alternatives:** Add a per-pair promotion table in `coerce_batch_to_exa_types`; add a
  schema-reconciliation pass in `delta_replay.rs`.
- **Rationale:** `register_file_list` already registers the DataFusion table schema from the scan
  spec's `LogicalField` list and never from a Parquet footer; `bind_columns` renames without ever
  comparing data types; `DefaultPhysicalExprAdapter` inserts a `CastExpr` on any field inequality.
  Each of those was confirmed against current source, not taken from the interview. A second cast
  site would be a decision with two owners.
- **Promotes to ADR:** no

### [3] The vendored `type-widening` fixture is not modified; `seed.sh` is extended instead

- **Decision:** Change no byte of `scripts/unity/fixtures/type-widening/`, and instead register all
  thirteen of its columns in `scripts/unity/seed.sh` at their widened types.
- **Alternatives:** Rewrite the fixture's Delta log to genuinely widen columns across commits, as the
  interview answer to Q2 directed.
- **Rationale:** The premise behind that answer was factually wrong, and this supersedes it. Commit 2
  already widens ALL THIRTEEN columns in `metaData.schemaString` and records each under
  `delta.typeChanges`; commit 0's data file is physically narrow and commit 2's is physically wide;
  both files are referenced by `add` actions and neither is orphaned. The fixture is already the
  straddling shape the plan needs. The only real gap was Unity Catalog registration — `seed.sh`
  registers 3 of 13, and a column absent from it is not selectable from Exasol. `PROVENANCE.md` also
  records these tables as "read fixtures — never mutated", so rewriting the log would break the
  vendoring contract to obtain something already present.
- **Promotes to ADR:** yes

### [4] Four Delta widening pairs are covered at the scan tier, not by a new Delta fixture

- **Decision:** Cover `byte` → `short`, `byte` → `int`, `short` → `int`, and `short` → `long` with
  purpose-written Parquet files in `type_relaxation_tests.rs` rather than by authoring a new Delta
  table.
- **Alternatives:** Add a Spark + delta-spark one-shot job to the Unity stack and author a fixture
  carrying the four missing columns.
- **Rationale:** Three of the four are invisible in this engine's logical schema — `byte`, `short`,
  and `integer` all tag `int32` — so a fixture would assert nothing a narrower test does not. The
  Unity stack has no delta-spark job at all, so the fourth pair would cost a new one-shot service,
  its CI wiring, and a new vendoring provenance entry. A Parquet file written at `Int16` under an
  `Int64` logical schema exercises the identical cast at the identical seam.
- **Promotes to ADR:** no

### [5] Iceberg `date` → `timestamp` / `timestamp_ns` is refused at plan time from the schema history

- **Decision:** Refuse the two Iceberg `date` promotions with a `UdfError` naming table, column, both
  Iceberg types, and a tracked issue, decided from `TableMetadata::schemas_iter` before any manifest
  is loaded. Delta's `date` → `timestampNtz` stays supported.
- **Alternatives:** Support them by vendoring the missing bounds-width inference; catch and re-word
  the manifest decode error; leave the opaque failure in place.
- **Rationale:** The Iceberg spec requires a manifest bound's write-time type to be inferred from its
  byte width; `iceberg` 0.10.0 implements that for `long`-from-4-bytes and `double`-from-4-bytes but
  reads `timestamp` and `timestamp_ns` bounds as 8 bytes unconditionally, so a pre-promotion file's
  4-byte bound fails `bytes.try_into()`. The failure is inside Avro deserialization, so it fires for
  an unfiltered `SELECT *` too — this engine loads every manifest in
  `ensure_supported_delete_mechanisms` before pruning runs — and the same crate carries a second
  bounds decode that `unwrap()`s, giving the shape a reachable panic path; a panic in a UDF makes the
  engine SIGKILL every sibling VM of the statement part. Catching the error would sit downstream of
  the panic and depend on an error string. Vendoring a decode fix is a dependency fork for two pairs.
  The asymmetry with Delta is real, not an oversight: Delta records per-file statistics as typed JSON
  with no width-versus-type ambiguity, so the same logical pair is safe there.
- **Promotes to ADR:** yes

### [6] The Iceberg gate is conservative and says so

- **Decision:** Refuse on the recorded promotion alone, without checking whether a pre-promotion data
  file survives; state that conservatism in the gate's own doc comment and in the spec.
- **Alternatives:** Refuse only when an old file is still live.
- **Rationale:** Establishing that no pre-promotion file remains requires reading the manifests,
  which is the operation that fails. The precise answer is unobtainable, so the cheap answer is taken
  and named rather than left silently imprecise. It over-refuses a table whose files were all
  rewritten after the promotion.
- **Promotes to ADR:** no

### [7] Iceberg `unknown` → any type is recorded as unreachable, with a build tripwire, and no gate

- **Decision:** Write no refusal arm and no mapping arm for `unknown`. Record it as a tracked
  exception citing its own issue, and pin the exhaustiveness of `iceberg_primitive_to_arrow` and
  `iceberg_primitive_to_exasol` with a test so a dependency upgrade that adds the variant breaks the
  build.
- **Alternatives:** Write a named refusal for `unknown`; upgrade `iceberg` within this plan.
- **Rationale:** `iceberg` 0.10.0's `PrimitiveType` has 16 variants and none is `Unknown`; the type
  name has no `serde` arm, so a v3 schema declaring `"unknown"` fails table-metadata deserialization
  before any engine code runs. A gate would be unreachable from its first commit. Both mapping
  functions are already exhaustive with no catch-all, so the build already fails if the variant
  appears — pinning that costs one assertion and is a stronger guarantee than a runtime refusal. This
  is the "flag the prerequisite gap rather than silently absorb it" branch of the Q4 answer; baseline
  v3 support itself is NOT a gap — `iceberg` 0.10.0 parses `FormatVersion::V3` with no rejection
  path, and this repo already commits two v3 Iceberg tables.
- **Promotes to ADR:** yes

### [8] `date` → `timestamp_ns` needs no fixture, and the microsecond emit collapse is not this plan's

- **Decision:** Cover the pair by the refusal in decision [5] and add no `timestamp_ns` fixture. Do
  not change `exasol_type_to_arrow`'s collapse of every declared `TIMESTAMP` precision to Arrow
  microseconds.
- **Alternatives:** Author a `timestamp_ns` Iceberg fixture; give nanosecond timestamps their own
  Arrow unit at the emit boundary.
- **Rationale:** The pair is refused, so no read fixture is meaningful. On the precision question,
  `datafusion-scan/type-mapping` already records that every `TIMESTAMP(p)` maps to
  `Timestamp(Microsecond, None)` because "Arrow's Microsecond unit is this project's fixed internal
  representation for every Exasol TIMESTAMP precision", and
  `datafusion-scan/scan-execution-field-id-projection` already records nanosecond timestamps as
  covered by unit round-trip rather than by fixture. Relaxation neither introduces nor worsens that
  collapse: a `date`-sourced value has no sub-microsecond digits to lose, so the only loss would be
  on genuinely nanosecond-precision rows, which is the pre-existing recorded behavior. Changing it
  would be a separate plan against a spec that already owns the decision. A related arrow-cast
  hazard — `Date32` → `Timestamp(Nanosecond)` multiplies without an overflow check and ignores
  `CastOptions`, so an out-of-range date wraps silently, contradicting the Iceberg spec's "values
  outside the promoted type's range must result in a runtime failure" — is moot here for the same
  reason: the only pair that would reach it is refused.
- **Promotes to ADR:** no

### [9] Allow-listing `typeWidening` pulls in the `delta.typeChanges` validation obligation

- **Decision:** Implement the per-column validation of every recorded `delta.typeChanges` entry
  against the Delta protocol's supported list, refusing an unsupported change through the existing
  refused-column mechanism.
- **Alternatives:** Allow-list the feature and skip the validation; refuse the whole table on an
  unsupported change.
- **Rationale:** `PROTOCOL.md` § Reader Requirements for Type Widening makes it a reader MUST:
  "Readers must validate that they support all type changes in the `delta.typeChanges` field … and
  fail when finding any unsupported type change." Allow-listing the feature without it would claim
  support the engine has not checked — the exact posture #322 refused the feature to avoid. Table
  scope was rejected for the same reason the type refusals are per column: one bad column should not
  make an otherwise readable table unreachable.
- **Promotes to ADR:** yes

### [10] The decimal widening rule is encoded as `k1 >= k2 >= 0`, not as `P' >= P && S' >= S`

- **Decision:** Validate a Delta decimal widening as `Decimal(p,s)` → `Decimal(p+k1, s+k2)` with
  `k1 >= k2 >= 0`, so `decimal(10,1)` → `decimal(11,3)` is refused and `decimal(10,1)` →
  `decimal(12,3)` is accepted. Record Iceberg's own decimal promotion separately as precision-only
  with scale unchanged.
- **Alternatives:** Use issue #349's summary — "P'≥P and S'≥S (both precision and scale can grow)".
- **Rationale:** The issue's paraphrase is refuted by `PROTOCOL.md`, whose constraint `k1 >= k2` also
  forbids the INTEGRAL digit count shrinking. The Iceberg spec is stricter still: its Requirements
  cell reads "Widen precision only" with the scale symbol literally unchanged and `P' > P` strict.
  Encoding either as a paraphrase would accept tables no conforming writer produces.
- **Promotes to ADR:** no

### [11] `delta.typeChanges` is a validation input only, and a `fieldPath` entry is checked by its pair

- **Decision:** Never consult `delta.typeChanges` to decide a cast, and validate an entry carrying a
  `fieldPath` by its `fromType`/`toType` pair alone, without parsing the path grammar. Ignore
  unrecognized entry keys, notably the superseded `tableVersion`.
- **Alternatives:** Drive the cast from the recorded history; parse `fieldPath` and validate per
  nested element; reject an entry carrying an unknown key.
- **Rationale:** The protocol lets a writer remove the annotation once every data file matches the
  schema and REQUIRES its removal when the feature is dropped, so a cast that depended on it would
  break on a legal table; the cast reads each file's own footer against the current logical type. A
  `fieldPath` names a map key/value or array element, and this engine refuses `map` outright and
  text-renders `array<E>`, so no scalar value is at risk. `tableVersion` was required by the accepted-
  and-superseded RFC and Delta 3.2-era clients still write it — all thirteen entries of the vendored
  fixture carry it — so rejecting unknown keys would refuse the plan's own fixture.
- **Promotes to ADR:** no

### [12] Two new tracked issues, not #349

- **Decision:** File separate issues for the Iceberg bounds-width gap and for Iceberg `unknown`
  support, and cite those in the refusal text and the specs.
- **Alternatives:** Cite #349.
- **Rationale:** #349 is this plan and closes with it. A closed issue cited in a shipped error reads
  as an unfixed gap with no owner — the same argument that moved the Delta type refusals off #322
  onto #350.
- **Promotes to ADR:** no

### [14] The Iceberg `date` E2E fixture is dropped; the fallback in Q4 applies

- **Decision:** Author only the readable-promotion fixture table (`iceberg_type_promotion`:
  `int`→`long`, `float`→`double`, `decimal(10,2)`→`decimal(20,2)`) via Spark. Do not author the
  `iceberg_date_promotion` table or its two E2E tests. Coverage for the `date` → `timestamp` /
  `timestamp_ns` refusal rests entirely on task 4.3's unit tests over a synthetic `TableMetadata`,
  which already exercise the refusal (`refuse_date_promotion`) directly and do not depend on a live
  fixture. `packaging/iceberg-type-promotion-fixture/spec.md` and `plan.md`'s Verification section are
  updated to drop the second table and its Integration-test rows.
- **Alternatives:** Author the `date` promotion via a raw REST `add-schema` + `set-current-schema`
  commit (bypassing Spark's `ALTER TABLE`), either scripted against the REST catalog directly or from
  `tests/common/` via `iceberg`'s `TableUpdate::AddSchema` / `SetCurrentSchema`; or keep trying to make
  Spark author it.
- **Rationale:** Live verification during implementation of task 6.1 found that Apache Iceberg Java
  itself never implements this promotion — `TypeUtil.isPromotionAllowed`
  (`api/src/main/java/org/apache/iceberg/types/TypeUtil.java`) is byte-identical across
  `apache-iceberg-1.10.1`, `apache-iceberg-1.11.0`, and `main`, and switches on `INTEGER`, `FLOAT`,
  `DECIMAL` only — `date` is not a case, so `ALTER TABLE … ALTER COLUMN … TYPE` fails with
  `Cannot change column type: date_timestamp: date -> timestamp` at every runtime version this stack
  can use (1.11.0 is additionally unusable here: it needs Java 17 and `apache/spark:3.5.7` ships Java
  11). A raw metadata-level commit (`add-schema`/`set-current-schema`) WAS proven to work and produces
  the exact schema-history shape `refuse_date_promotion` reads, with the expected 4-byte pre-promotion
  manifest bound — but it was also proven that Iceberg Java's own Spark reader then fails the same
  table with `TimeStampMicroVector cannot be cast to DateDayVector`. No conforming Iceberg writer or
  reader produces or reads this shape today; a fixture authored by hand-committing metadata would
  exercise nothing beyond what the unit tests already exercise, while adding a REST-commit code path
  this plan's Non-Goals otherwise avoids. This is precisely the contingency the interview's Q4 answer
  pre-authorized: "if live E2E against a real v3 table proves infeasible, fall back to unit-test
  coverage for those specific pairs rather than blocking the whole plan," with the added instruction
  to "flag it clearly" if a v3 prerequisite gap turns out to be real. The gap here is not baseline v3
  support (this repo already commits v3 tables) but the specific `date` promotion, which upstream
  Iceberg Java has never implemented — recorded here and in `packaging/iceberg-type-promotion-fixture`
  rather than left as a silent scope reduction.
- **Promotes to ADR:** yes

### [15] Two of the vendored fixture's thirteen recorded changes are outside the protocol's supported list

- **Decision:** Accept that `byte_decimal` (`byte`→`decimal(4,1)`) and `short_decimal`
  (`short`→`decimal(6,1)`) are refused per column by the `delta.typeChanges` validation (task 3.2),
  rather than widening or special-casing the predicate to admit them. Update
  `e2e-harness/unity-catalog-e2e-harness-delta-queries/spec.md` and plan task 5.3 to assert ELEVEN
  columns queryable and these two refused, not all thirteen queryable.
- **Alternatives:** Derive each decimal target's base precision from the SOURCE type's own range
  (matching the vendored fixture's apparent intent) instead of the protocol's fixed `Decimal(10+k1,k2)`
  / `Decimal(20+k1,k2)` bases; or treat this as a fixture defect and change the seed/E2E expectations
  to skip these two columns silently.
- **Rationale:** `PROTOCOL.md`'s finalized § Type Widening fixes the base at precision 10 for
  `Byte`/`Short`/`Int` sources and 20 for `Long`, not at each source type's own range — verified
  against the spec text and against `delta-spark`'s own `isWiderThan` implementation, which agree.
  `decimal(4,1)` and `decimal(6,1)` both derive a negative `k1` against base 10, so they fail
  `k1 >= k2 >= 0` under the protocol as it stands today. The vendored fixture is from
  `delta-kernel-rs` v0.26.0 test data under `typeWidening-preview` — the RFC-preview era before the
  bases were finalized — so this is the fixture predating the spec it now must be validated against,
  not a bug in the validation. Softening the predicate to admit these two would accept a shape no
  CURRENT conforming writer produces, reintroducing exactly the unsoundness the finalized protocol
  removed. Per-column refusal already exists for this reason (decision [9]): one non-conforming column
  does not make the other eleven, or the rest of the table, unreachable.
- **Promotes to ADR:** no

### [13] Our issues cite upstream tracking where it exists, and nothing is filed upstream

- **Decision:** Link `apache/iceberg-rust#2581` (open enhancement "v3 spec types — Variant,
  Geometry, Geography, Unknown", a sub-item of the `#2411` v3-support epic) from the `unknown` issue
  and from the `unknown` tracked exception in `vs-adapter/iceberg-type-promotion`. State in the
  bounds-width issue that an upstream search found no `apache/iceberg-rust` issue or PR tracking that
  gap, and leave the date-to-timestamp scenario citing this repository's issue alone. File nothing on
  `apache/iceberg-rust`.
- **Alternatives:** Cite no upstream issue and duplicate tracking that already exists; file a new bug
  report upstream for the bounds-width gap.
- **Rationale:** A `gh search issues --repo apache/iceberg-rust` run during planning matched #2581
  exactly to the tripwire's blocker, so citing it points our issue at the work that unblocks it
  instead of re-tracking it. The same search — "manifest bound bytes", "type promotion", "timestamp
  bound", "Datum::try_from_bytes", "lower_bounds upper_bounds", and nine further terms, over issues
  and PRs — surfaced nothing for the bounds-width gap; that is an absence of found tracking, not
  proof of absence, so the issue says "none found as of this search" rather than asserting none
  exists. The user instructed that this project does not file issues on third-party repos, which
  settles the bounds-width gap: our own issue carries it and upstream stays untouched.
- **Promotes to ADR:** yes

## Review Findings
