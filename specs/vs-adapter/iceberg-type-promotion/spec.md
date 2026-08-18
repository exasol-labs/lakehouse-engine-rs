# Feature: Iceberg Type Promotion

Decides, at plan time, which Apache Iceberg type promotions this engine reads — `int` → `long`,
`float` → `double`, and decimal precision widening are read through the format-neutral relaxation
cast; a `date` → `timestamp` or `date` → `timestamp_ns` promotion is refused by name — so a promoted
Iceberg table either returns correct rows or says why it cannot, and never fails with an opaque
byte-slice error from inside a manifest decode.

## Background

The Apache Iceberg table specification (<https://iceberg.apache.org/spec/>, `apache/iceberg` `main`,
`format/spec.md` at commit `d8170b0dfae4ecc0716de8b823c5d0987fc21fa8`) defines the whole promotion
surface in ONE five-row table (§ Schema Evolution). Quoted verbatim:

> Valid primitive type promotions are:
>
> | Primitive type   | v1, v2 valid type promotions | v3+ valid type promotions    | Requirements |
> |------------------|------------------------------|------------------------------|--------------|
> | `unknown`        |                              | _any type_                   | |
> | `int`            | `long`                       | `long`                       | |
> | `date`           |                              | `timestamp`, `timestamp_ns`  | Promotion to `timestamptz` or `timestamptz_ns` is **not** allowed; values outside the promoted type's range must result in a runtime failure |
> | `float`          | `double`                     | `double`                     | |
> | `decimal(P, S)`  | `decimal(P', S)` if `P' > P` | `decimal(P', S)` if `P' > P` | Widen precision only |

* **The decimal rule is precision-only.** The Requirements cell reads *"Widen precision only"* and
  the scale symbol `S` is literally unchanged on both sides, with the constraint strictly `P' > P`.
  Iceberg permits NO scale change. Delta separately permits scale growth
  (`vs-adapter/delta-type-mapping`), and `datafusion-scan/type-relaxation` records that row as
  Delta-only for exactly this reason.
* **The bounds-width inference rule, § Schema Evolution, is the one that decides this feature's
  shape.** Quoted verbatim: *"Iceberg's Avro manifest format does not store the type of lower and
  upper bounds, and type promotion does not rewrite existing bounds. For example, when a `float` is
  promoted to `double`, existing data file bounds are encoded as 4 little-endian bytes rather than 8
  little-endian bytes for `double`. To correctly decode the value, the original type at the time the
  file was written must be inferred according to the following table:"* — whose rows include
  `long`/4 bytes → `int`, `double`/4 bytes → `float`, `timestamp`/4 bytes → `date`, and
  `timestamp_ns`/4 bytes → `date`.
* **`iceberg` 0.10.0 implements that table for TWO of its four non-decimal rows.**
  `Datum::try_from_bytes` branches on a 4-byte buffer for `PrimitiveType::Long` and
  `PrimitiveType::Double` only; `Timestamp`, `TimestampNs`, `Timestamptz`, and `TimestamptzNs` each
  read 8 bytes unconditionally. A 4-byte bound under a `timestamp` column therefore fails
  `bytes.try_into()` and surfaces as `DataInvalid: failed to convert byte slice to array`. The
  decimal row needs no branch: the decode is a length-agnostic big-endian two's-complement read and
  the spec keeps the scale unchanged.
* **That failure is NOT scoped to pruning, which is why it cannot be worked around.** The bounds
  decode runs inside manifest Avro deserialization, so it fires for every manifest entry whether or
  not a filter predicate was supplied — and in this engine it fires FIRST from
  `ensure_supported_delete_mechanisms`, which loads every manifest to read only content type and file
  format. An unfiltered `SELECT *` against a `date`-promoted table therefore fails just as a filtered
  one does, with a message naming neither bounds nor promotion. `iceberg` 0.10.0 also carries a
  second bounds decode that `unwrap()`s rather than propagating (manifest-list partition field
  summaries), so the same shape has a reachable panic path — and a panic inside the adapter aborts
  the UDF's VM, which makes the engine SIGKILL every sibling VM of the statement part.
* **The refusal is therefore a plan-time gate on the SCHEMA HISTORY, not on a decode error.** A gate
  that caught and re-worded the decode error would sit downstream of the panic path and would depend
  on an error string. `TableMetadata::schemas_iter` already carries every schema the table has held,
  so the promotion is visible before any manifest is read and before any object-store byte is spent.
* **The gate is deliberately CONSERVATIVE and this is stated rather than hidden.** It refuses on the
  recorded promotion alone, without checking whether any pre-promotion data file survives. A table
  whose files were all rewritten after the promotion carries only 8-byte bounds and would read fine,
  and is refused anyway. Establishing that no old file remains requires reading the manifests, which
  is the operation that fails — so the cheap conservative answer is the only one available, and it
  errs toward a named refusal rather than toward an opaque failure.
* **Iceberg `date` → `timestamp` is refused while Delta `date` → `timestampNtz` is supported, and the
  asymmetry is real rather than an oversight.** The two are the same logical pair and the same Arrow
  cast. They differ because Delta records per-file statistics as typed JSON in the `add` action,
  which carries its own lexical form and has no width-versus-type ambiguity, while Iceberg records
  them as untyped Avro byte buffers whose width must be inferred. The gap lives entirely in the
  metadata format, not in the read path, and `datafusion-scan/type-relaxation` owns the shared cast
  both formats reach.
* **`unknown` → any type is UNREACHABLE rather than refused.** `iceberg` 0.10.0's `PrimitiveType` has
  16 variants and none of them is `Unknown`; the type name has no `serde` arm, so a v3 schema
  declaring `"unknown"` fails deserialization and takes the whole table-metadata parse with it,
  before any code in this engine runs. No gate can improve on that, and writing one would be dead
  code. What this feature DOES record is the tripwire: `iceberg_primitive_to_exasol` and
  `iceberg_primitive_to_arrow` (`crates/lakehouse-engine/src/types/mapping.rs`) are both EXHAUSTIVE
  over `PrimitiveType` with no catch-all arm, so an `iceberg` upgrade that adds `Unknown` breaks the
  BUILD rather than silently mapping it to the `utf8` fallback.
* **The `unknown` read rule is quoted so the future implementation is not re-derived.** The spec's
  § Primitive Types requires `unknown` to be *"Must be optional with `null` defaults; not stored in
  data files"*, its § Parquet states *"When reading an `unknown` column, any corresponding column
  must be ignored and replaced with `null` values"*, and its § Default values adds *"All columns of
  `unknown`, `variant`, `geometry`, and `geography` types must default to null."* Once the dependency
  represents the type, an `unknown` column is a column absent from every data file, which
  `datafusion-scan/scan-execution-field-id-projection`'s existing per-file NULL fill already answers
  — so `unknown` → any type needs no cast at all.
* **Format version is not gated and this feature adds no gate.** `iceberg` 0.10.0 parses
  `FormatVersion::V3` with no rejection path, and
  `datafusion-scan/scan-execution-field-id-projection` already records that this engine's read path
  is format-version-agnostic. The v3 shortfall is the TYPE VOCABULARY (`unknown`, `variant`,
  `geometry`, `geography`), never the metadata envelope.
* **The logical schema already carries the promoted type and nothing changes there.**
  `build_logical_schema` reads `table.metadata().current_schema()`, so a promoted column's
  `LogicalField` names the current type without this feature touching it. `datafusion-scan/type-relaxation`
  owns what the scan then does with it.
* Every error this feature surfaces is a `UdfError`, never a panic, and carries no vended or static
  credential value.

## Scenarios

### Scenario: A promotion this engine reads resolves through the shared relaxation cast

* *GIVEN* an Iceberg table whose schema history records a promotion the spec permits at the table's
  format version and this engine supports — `int` → `long`, `float` → `double`, or `decimal(P,S)` →
  `decimal(P',S)` with `P' > P` — and whose data files straddle that promotion
* *WHEN* the adapter resolves the file list for a pushdown request against that table
* *THEN* the adapter SHALL build the logical schema from `table.metadata().current_schema()`, so the
  promoted column's `LogicalField` carries the PROMOTED type rather than any data file's type
* *AND* the adapter SHALL plan the request normally, adding no promotion-specific field to
  `ScanSpec`, `FileEntry`, or `LogicalField`, because the promoted type is already what the logical
  schema names and the cast is the scan's business
  (`datafusion-scan/type-relaxation`)
* *AND* file pruning SHALL keep working across the promotion for these three pairs, because `iceberg`
  0.10.0 implements the spec's bounds-width inference for `long` from 4 bytes and `double` from 4
  bytes, and decodes a decimal bound length-agnostically at the unchanged scale
* *AND* the resolver MUST NOT re-derive the promotion from the schema history for these pairs, so no
  promotion allow-list exists on the read path — the spec's own writer-side rules already bound which
  pairs a conforming table can present

### Scenario: A date-to-timestamp promotion is refused at plan time by name

* *GIVEN* an Iceberg table whose schema history records a field id declared `date` in an earlier
  schema and `timestamp` or `timestamp_ns` in the current schema — the spec's v3+ `date` promotion
* *WHEN* the adapter resolves a pushdown request against that table
* *THEN* the adapter SHALL refuse the request with a `UdfError` naming the table, the column, both
  the earlier and the current Iceberg type, and issue #355 — the repository issue that tracks this
  gap — so the refusal reads as scoped work rather than a permanent limitation
* *AND* the refusal SHALL be decided from `TableMetadata::schemas_iter` alone, BEFORE any manifest is
  loaded and before `ensure_supported_delete_mechanisms` runs, because the failure it replaces occurs
  inside manifest deserialization and would otherwise surface as `failed to convert byte slice to
  array`, naming neither the column nor the promotion
* *AND* the refusal SHALL fire for an unfiltered request as well as a filtered one, because the
  bounds decode this gate stands in front of runs during manifest deserialization rather than during
  pruning, so scoping the gate to requests carrying a predicate would leave `SELECT *` broken
* *AND* the gate SHALL refuse the table on the recorded promotion ALONE, without establishing that a
  pre-promotion data file survives, and this conservatism SHALL be stated in the gate's own doc
  comment — proving no old file remains requires the manifest read that fails
* *AND* a table whose `date` column was never promoted SHALL be unaffected, and a table promoted
  `int` → `long`, `float` → `double`, or decimal-precision SHALL keep planning normally, so the gate
  refuses exactly the two `date` rows and nothing adjacent
* *AND* the refusal SHALL be returned as a `UdfError` value, never raised as a panic, and MUST NOT
  contain any vended or static credential value

### Scenario: The unknown primitive type is unrepresentable, and the mapping is the tripwire

* *GIVEN* `iceberg` 0.10.0, whose `PrimitiveType` enumerates 16 variants and declares no `Unknown`,
  so an Iceberg v3 schema declaring a column of type `unknown` fails to deserialize before any engine
  code runs
* *WHEN* the type-mapping test suite runs
* *THEN* the suite SHALL assert that `iceberg_primitive_to_exasol` and `iceberg_primitive_to_arrow`
  each match every `PrimitiveType` variant EXHAUSTIVELY with no catch-all arm, so a dependency
  upgrade that adds `Unknown` — or `variant`, `geometry`, or `geography` — fails the BUILD rather
  than silently mapping the new variant onto the `VARCHAR(2000000)` / `utf8` fallback
* *AND* this engine MUST NOT add a gate, a refusal message, or a mapping arm for `unknown`, because
  the type cannot reach this engine at the pinned dependency version and such code would be
  unreachable from its first commit
* *AND* the unreachability SHALL be recorded as a tracked exception citing issue #356 — the
  repository issue that owns Iceberg `unknown` support — AND the upstream `apache/iceberg-rust` issue
  it links — `apache/iceberg-rust#2581`, which adds the missing `Unknown` primitive type — rather
  than left as a silent gap, because the spec permits `unknown` → any type at v3+ and this engine
  reads no such table
* *AND* the eventual implementation SHALL be the EXISTING per-file NULL fill rather than a cast,
  because the spec requires an `unknown` column to be omitted from data files and read as all-`null`,
  which is the absent-field case `datafusion-scan/scan-execution-field-id-projection` already
  resolves
