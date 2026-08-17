# Decision Log: add-delta-reader-gating-and-type-mapping

## Interview

**Q:** Which Delta reader features should the engine allow, and which should it refuse?
**A:** Default-deny. The allow-list is exactly `ColumnMapping`, `DeletionVectors`,
`TimestampWithoutTimezone` (`timestampNtz`), `V2Checkpoint`, and `VacuumProtocolCheck`. Everything else
is refused, including `TypeWidening`/`TypeWideningPreview` (tracked separately as follow-up issue
#349), `VariantType`/`VariantTypePreview`, `VariantShredding`/`VariantShreddingPreview`,
`CatalogManaged`, `CatalogOwnedPreview`, `AdaptiveMetadataPreview`, and any `TableFeature::Unknown(_)`.
`min_reader_version` 1–3 is allowed, matching `delta_kernel`'s own `MAX_VALID_READER_VERSION = 3`;
anything outside that range is refused. `DomainMetadata` and `InCommitTimestamp` are deliberately
absent from the allow-list: `delta_kernel` 0.26 classifies both as writer-only features, so they cannot
appear in `protocol.readerFeatures` at all and a read-only engine gates neither.

**Q:** Where should the gate run?
**A:** In `delta_replay.rs`, at plan time, right after the snapshot is opened — inside or immediately
after `DeltaSnapshot::open` — before `build_delta_table_schema` and before `active_files()` or
`scan_builder()` are called, so an unsupported table is refused before any log replay or object-store
listing beyond opening the snapshot itself.

**Q:** How should a Delta `variant` column be treated in the type mapping?
**A:** Explicit refusal, not a JSON-`VARCHAR` fallback. Variant's on-disk shape is an opaque
`(metadata: BINARY, value: BINARY)` pair in a Delta/Spark-specific binary encoding; generic JSON
serialization of that would emit meaningless base64 blobs. Usually moot because `variantType` and
`variantType-preview` are also refused reader features, but the type refusal is a second, independent
safety net.

**Q:** Should Delta `struct` and `map` columns fall back to JSON `VARCHAR` per the project's Iceberg
convention?
**A:** No — explicit refusal. The convention was verified to be partly broken: `arrow-cast` reports no
cast from `Struct` or `Map` to `Utf8`, and DataFusion validates physical-against-logical castability at
file open, before any per-value JSON logic runs. Neither type can reach the JSON path on either table
format. Every existing test asserting that fallback uses a zero-field struct, which sidesteps the cast.
This plan stays Delta-only and refuses both; issue #350 was filed to design real JSON rendering for
struct and map on BOTH formats and to remove Delta's refusal once it lands.

**Q:** And Delta `binary`?
**A:** Explicit refusal too. Casting `binary` to `utf8` silently turns every non-UTF-8 byte sequence
into NULL — exactly the "wrong data instead of an error" failure mode this issue exists to prevent.
Cited under #350 with struct and map.

**Q:** How should the E2E acceptance criteria use the existing fixtures?
**A:** No new fixture. The `type-widening` (`typeWidening-preview`) and `unshredded-variant`
(`variantType-preview`) fixtures already cover "unsupported reader feature → clear error" directly.
`stats-all-types` is the varied-types fixture, but its `map_col` and `nested_struct` columns must now be
asserted as REFUSED rather than queried, so the varied-types scenario queries and asserts over that
fixture's other, mappable columns rather than `SELECT *`.

## Design Decisions

### [1] Refuse struct, map, binary, and variant instead of completing the JSON-`VARCHAR` convention

- **Decision:** Delta `struct`, `map`, `binary`, and `variant` columns are refused by name at plan time,
  each with a reason naming its own cause. `binary`, `struct`, and `map` cite issue #350; `variant`
  cites its binary-encoding shape. Issue #322's scope text asked for the JSON-`VARCHAR` convention to be
  completed for them; it is not.
- **Alternatives:** (a) complete the convention as the issue's scope text asks — rejected, the
  convention is unreachable for struct and map and lossy for binary; (b) implement real JSON rendering
  here — rejected, it is a design problem spanning both table formats and belongs in its own plan
  (#350); (c) keep the existing generic "issue #322" error text — rejected, #322 is this plan and a
  closed issue in a shipped error reads as an unfixed gap with no owner.
- **Rationale:** Verified against `arrow-cast` 58.3's `can_cast_types`: `(Struct(_), _) => false` makes
  `Struct → Utf8` unavailable, and `Map` reaches `(_, Utf8) => from_type.is_primitive()` as `false`.
  `raw_scan` registers the logical schema as the DataFusion table schema and DataFusion validates
  castability at file open, so neither type reaches the per-value JSON conversion at all. `Binary → Utf8`
  IS available and replaces every non-UTF-8 byte sequence with NULL — silent corruption.
- **Promotes to ADR:** yes

### [2] Scope the type refusal to the COLUMN, not the table

- **Decision:** A refused column is omitted from the logical schema and recorded on `ResolvedScan`. One
  adapter gate refuses a pushdown request that reads or emits a refused column; every other request
  against the same table plans normally. Supersedes the reading of the brief that this plan merely
  "confirms and preserves" the shipped table-scoped refusal from PR #340.
- **Alternatives:** (a) keep the shipped table-scoped refusal — rejected, see rationale; (b) omit
  refused columns from the `createVirtualSchema` declaration so Exasol never offers them — rejected, it
  would put the same classification decision in two places over two type vocabularies (Unity Catalog
  type names and `delta_kernel::DataType`), which is exactly the back-door duplication the design
  guidance warns about, and it silently shrinks `SELECT *` for the operator; (c) author a new
  all-mappable Spark fixture and keep table scope — rejected, it adds a network-dependent seed step the
  #325 harness deliberately avoided, for strictly less useful engine behavior.
- **Rationale:** Empirically forced. `stats-all-types` — the fixture vendored specifically for issue
  #322's type coverage — carries `binary_col`, `map_col`, and `nested_struct` alongside 13 mappable
  columns. Table scope leaves it wholly unqueryable, which makes issue #322's own E2E acceptance
  criterion ("a fixture table spanning varied Delta types returns the expected Exasol types and
  values") unreachable with the fixtures on hand, and it makes any real Delta table with one struct
  column unreachable over a column nobody selected. Column scope also matches Iceberg, which refuses
  nothing.
- **Promotes to ADR:** yes

### [3] The refusal gate reads a total recursive JSON walk, never a per-clause enumeration

- **Decision:** The gate's referenced-column set is one recursive walk over the whole pushdown request
  JSON collecting every `column` node's name, unioned with the final projection the adapter renders.
- **Alternatives:** Enumerate the clauses that can carry a column — select list, WHERE, GROUP BY, ORDER
  BY, aggregate arguments, join conditions. Rejected.
- **Rationale:** A per-clause list silently omits every pushdown capability added after it, and a miss
  is not a decline — it routes a refused column into the scan. For `binary` specifically that means a
  filter comparing the column as text with every non-UTF-8 value silently NULL, which is the exact
  failure this plan exists to prevent. The projection half is needed on top because the adapter widens
  to the full base row for a `SELECT *`, an aggregate select list, and an untranslatable select-list
  item — cases whose column set the request JSON does not name.
- **Promotes to ADR:** yes

### [4] A refused column is absent from the logical schema, as defense in depth

- **Decision:** `build_delta_table_schema` emits no `LogicalField` for a refused column, rather than
  tagging it `utf8` and relying on the gate.
- **Alternatives:** Keep the column in the logical schema tagged `utf8` and let the gate be the only
  guard.
- **Rationale:** The two designs differ only when the gate misses a path. Absent from the schema, a miss
  produces a DataFusion unresolved-column error. Present and tagged `utf8`, a miss produces a
  silently-NULLed `binary` column — wrong rows. The correctness of a fail-loud feature must not rest on
  a single gate being exhaustive.
- **Promotes to ADR:** no

### [5] Gate inside `DeltaSnapshot::open`, not at the format reader's entry point

- **Decision:** The protocol gate runs inside the snapshot constructor, so construction either returns a
  gated snapshot or returns the refusal.
- **Alternatives:** Call the gate from `read_delta_log` immediately after `DeltaSnapshot::open` returns
  — the placement the interview described.
- **Rationale:** Same ordering, stronger guarantee. A gate outside the constructor leaves the
  constructor reachable from a second caller — including `delta_replay_tests.rs`, which already opens
  snapshots directly over local fixtures — that would then exercise an ungated snapshot and record the
  ungated behavior as correct. Making the gated snapshot the only obtainable snapshot removes the
  bypass rather than documenting it.
- **Promotes to ADR:** no

### [6] The gate takes `(min_reader_version, reader_features)`, not `&Protocol`

- **Decision:** `ensure_readable(min_reader_version: i32, reader_features: Option<&[TableFeature]>)`.
  `DeltaSnapshot::open` extracts both through `table_configuration().protocol()`.
- **Alternatives:** Take `&delta_kernel::actions::Protocol` directly.
- **Rationale:** `Protocol`'s constructors (`try_new`, `try_new_modern`, `try_new_legacy`) are
  `pub(crate)` WITHOUT `#[internal_api]`, so no test in this crate can build one. Taking the two values
  makes the gate a pure function unit-testable per feature, keeps the `internal-api` reach at exactly
  one extraction site, and states the gate's inputs in the engine's own vocabulary rather than the
  provider's type.
- **Promotes to ADR:** no

### [7] Map `byte` and `short` to the existing `int32` tag rather than adding `int8`/`int16`

- **Decision:** Both reuse `int32`.
- **Alternatives:** Add `"int8"` and `"int16"` to the compact tag vocabulary shared by
  `arrow_type_to_tag` / `arrow_type_from_tag`.
- **Rationale:** Exasol gives Int8, Int16, and Int32 the same `DECIMAL(precision, 0)` shape with no
  Exasol-visible distinction, and Unity Catalog already declares `BYTE` as `DECIMAL(3,0)` and `SHORT` as
  `DECIMAL(5,0)`, which bound the range at the SQL surface regardless. The Parquet reader produces
  physical `Int8`/`Int16`, which the scan's existing physical-expression adapter widens to logical
  `Int32` losslessly. A new tag would touch the cross-format classifier every reader reads, for no
  observable difference.
- **Promotes to ADR:** no

### [8] `array<E>` is classified recursively on its element type

- **Decision:** `array<E>` carries the `utf8` tag when `E` is itself mappable or text-rendered at any
  nesting depth, and is refused when `E` is refused.
- **Alternatives:** A blanket `utf8` tag for every `array`, as the brief's research section proposed.
- **Rationale:** `can_cast_types` recurses through `(List(inner), Utf8) => can_cast_types(inner, Utf8)`.
  `array<integer>` and `array<array<integer>>` are castable; `array<struct<…>>` and `array<variant>` are
  not, and `array<binary>` inherits binary's lossiness. A blanket tag would send
  `unshredded-variant`'s `array_of_variants` into an opaque file-open cast error instead of a named
  refusal.
- **Promotes to ADR:** no

### [9] Pin the three type sets to arrow's own castability answer with assertions

- **Decision:** A unit test asserts `can_cast_types(physical, Utf8)` for a representative of each set —
  `true` for `Binary`, `List(Int32)`, both interval units, and an out-of-domain `Decimal128`; `false`
  for a POPULATED `Struct`, a `Map`, and a `List(Struct)`.
- **Alternatives:** State the castability facts in the spec prose alone.
- **Rationale:** The set membership IS a claim about arrow behavior, and the existing
  `convert_tests`/`mapping_tests` assertions passed against a convention that does not hold precisely
  because they used a zero-field struct that sidesteps the field-wise cast check. Compiling the claim
  into an assertion is what makes an `arrow-cast` upgrade that changes an answer a test failure instead
  of a silent re-partition.
- **Promotes to ADR:** yes

### [10] Split gating and type mapping into two new sibling features

- **Decision:** New `vs-adapter/delta-reader-feature-gating` and `vs-adapter/delta-type-mapping`;
  `vs-adapter/delta-table-planning` receives only a removal and one changed scenario.
- **Alternatives:** Add both halves as scenarios on `vs-adapter/delta-table-planning`.
- **Rationale:** That feature already carries nine scenarios spanning log replay, partition values,
  deletion vectors, column mapping, credentials, format dispatch, and Iceberg parity. A protocol gate
  and a full type-surface mapping are distinct reasons to change, each carrying its own normative
  `PROTOCOL.md` citations, and adding five more scenarios would push one spec past this library's
  per-spec organization threshold.
- **Promotes to ADR:** no

### [11] The refused-column list rides on `ResolvedScan`, not on `ScanSpec`

- **Decision:** `ResolvedScan` gains a `refused_columns` field carrying a named `RefusedColumn` struct;
  the type is added to the frozen `pushdown` façade and both surface probes move to 26 and 16 items.
- **Alternatives:** (a) a new `ScanSpec`/`LogicalField` field — rejected, the scan never reads it, so a
  wire field would need the `ScanSpec` format-neutrality rule widened for nothing and would move golden
  encodings; (b) `Vec<(String, String)>` to avoid touching the frozen façade — rejected, an external
  test crate reads `ResolvedScan`'s fields and a positional pair gives its two strings no
  distinguishing name at the read site.
- **Rationale:** The list is a property of one table's resolution, so it belongs on the resolution
  result. The Iceberg reader returns an empty list, which makes the field format-neutral by
  construction.
- **Promotes to ADR:** no

### [12] `void` is mapped rather than refused, and its all-NULL read is asserted

- **Decision:** `void` carries the `utf8` tag and is declared `VARCHAR(2000000)`; its values are always
  NULL.
- **Alternatives:** Refuse it alongside the other unmapped types.
- **Rationale:** The protocol is normative and unambiguous here: writers "MUST omit `void` columns from
  data files" and readers "MUST reconstruct them as all-`null` columns". A `VARCHAR` column of NULLs is
  a faithful surfacing, and the scan's existing missing-physical-column rule already supplies the
  nulls. The plan asserts that end to end — including under `name` column mapping, where the column's
  physical name appears in no data file — rather than assuming it.
- **Promotes to ADR:** no

### [13] Gate before the zero-active-files early return

- **Decision:** The adapter's refused-column gate runs before `handle_pushdown`'s
  `if files.is_empty()` early return to `empty_result_sql`.
- **Alternatives:** Place it after, alongside the rest of the post-resolution dispatch.
- **Rationale:** Placed after, a query naming a refused column against a Delta table with zero active
  files would be answered with an empty result instead of refused — a wrong answer that looks like a
  correct one, and the one shape where the refusal would be silently skipped.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by /speq:plan after plan-reviewer resolves a blocker, and by /speq:implement after code review. -->
