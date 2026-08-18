# Feature: Delta Schema Type Mapping

Maps every type a Delta table schema can declare either onto the Arrow tag the scan binds it by or
onto a named per-column refusal, so a Delta column is queryable when this engine can render its value
faithfully and refused when it cannot — never described by a tag that returns the wrong value.

## Background

* **This delta is issue #349.** It adds ONE scenario: the per-column validation of a table's recorded
  `delta.typeChanges` history, which the Delta protocol makes a READER obligation the moment
  `typeWidening` is allow-listed (`vs-adapter/delta-reader-feature-gating`). No existing scenario
  changes — the native, text-rendered, and refused type sets, the shared decimal predicate, the
  per-column refusal scoping, the whole-table refusal, and the castability assertions are all
  untouched.
* **The Delta Lake protocol specification (`delta-io/delta`, `PROTOCOL.md`, `master`) states two
  reader obligations, and this feature owns the second.** § Reader Requirements for Type Widening:
  *"Readers must allow reading data files written before the table underwent any supported type
  change, and must convert such values to the current, wider type."* — met by
  `datafusion-scan/type-relaxation`. *"Readers must validate that they support all type changes in
  the `delta.typeChanges` field in the table schema for the table version they are reading and fail
  when finding any unsupported type change."* — met here.
* **The supported type changes, quoted verbatim from § Type Widening**, are the set this validation
  accepts:

  > - Integer widening:
  >   - `Byte` -> `Short` -> `Int` -> `Long`
  > - Floating-point widening:
  >   - `Float` -> `Double`
  >   - `Byte`, `Short` or `Int` -> `Double`
  > - Date widening:
  >   - `Date` -> `Timestamp without timezone`
  > - Decimal widening - `p` and `s` denote the decimal precision and scale respectively.
  >   - `Decimal(p, s)` -> `Decimal(p + k1, s + k2)` where `k1 >= k2 >= 0`.
  >   - `Byte`, `Short` or `Int` -> `Decimal(10 + k1, k2)` where `k1 >= k2 >= 0`.
  >   - `Long` -> `Decimal(20 + k1, k2)` where `k1 >= k2 >= 0`.

* **The decimal constraint is `k1 >= k2 >= 0`, which is STRICTLY STRONGER than "precision and scale
  may both grow".** `k2 >= 0` forbids the scale shrinking and `k1 >= k2` forbids the INTEGRAL digit
  count shrinking, so `decimal(10,1)` → `decimal(11,3)` is not a legal widening even though both
  precision and scale grow. Encoding the rule as the pair of inequalities rather than as
  `P' >= P && S' >= S` is what makes the validation match the protocol instead of a paraphrase of it.
* **`long` → `double` is deliberately ABSENT from the protocol's list** — the floating-point bullet
  names `Byte`, `Short` or `Int` and omits `Long`, which is lossy above 2^53. A validation that
  admitted it would accept a table no conforming Delta writer produces.
* **The metadata shape, quoted from § Type Change Metadata**, is a JSON list whose objects carry
  `fromType` (required), `toType` (required), and `fieldPath` (optional — *"When updating the type of
  a map key/value or array element only"*, with values `"key"`, `"value"`, `"element"`, dotted for
  nesting). `tableVersion` was REQUIRED in the accepted-and-superseded RFC and is absent from the
  current specification, yet Delta 3.2-era clients still write it — the vendored `type-widening`
  fixture carries `tableVersion: 2` on all thirteen of its entries. The parser therefore MUST ignore
  keys it does not know rather than reject the entry.
* **`delta.typeChanges` is a VALIDATION input, never a cast input.** The protocol's conversion rule
  names only *"the current, wider type"*, writers *"may remove the `delta.typeChanges` metadata …
  if all data files use the same field types as the table schema"*, and removing the feature
  REQUIRES removing it. A reader that consulted it to decide a cast would therefore break on a table
  that legally carries none. The scan reads the physical Parquet type from each file's own footer and
  casts to the current logical type, which is what `datafusion-scan/type-relaxation` records.
* **A `fieldPath` entry names a map key/value or an array element, and this engine already answers
  those columns without a scalar cast.** A `map` column is refused outright and an `array<E>` column
  is text-rendered, so a widening recorded inside either changes the rendered text at worst and can
  never surface a wrong scalar value. Validating such an entry by its pair alone — ignoring the path
  — is therefore sufficient and avoids parsing a nested path grammar for a column whose value never
  reaches Exasol as that type.
* **The refusal reuses the EXISTING per-column mechanism rather than adding a table-scoped gate.** An
  unsupported recorded change concerns exactly one column, and this feature already carries a
  refused-column list on `ResolvedScan` that refuses only the requests reading or emitting that
  column. A table-scoped refusal would make an otherwise readable table unreachable over a column
  nobody selected — the same argument that scoped the type refusals per column.
* **Apache Iceberg spec check — this delta changes no Iceberg behavior.** It reads a Delta-specific
  schema annotation and adds no code on the Iceberg resolution path. Iceberg's own promotions,
  including the two `date` rows this engine refuses, are `vs-adapter/iceberg-type-promotion`'s scope.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Every recorded Delta type change is validated, and an unsupported one refuses its column

* *GIVEN* a Delta table whose schema carries `delta.typeChanges` entries on one or more fields — the
  shape a `typeWidening` or `typeWidening-preview` table records — including at least one entry whose
  `fromType`/`toType` pair the Delta protocol's supported list does NOT contain
* *WHEN* the Delta format reader builds that table's logical schema
* *THEN* the reader SHALL validate EVERY recorded entry's `fromType`/`toType` pair against the
  protocol's supported list, because the protocol obliges a reader to *"validate that they support
  all type changes in the `delta.typeChanges` field … and fail when finding any unsupported type
  change"*
* *AND* the reader SHALL accept exactly the protocol's list — the `Byte` → `Short` → `Int` → `Long`
  integer chain, `Float` → `Double`, `Byte`/`Short`/`Int` → `Double`, `Date` → `Timestamp without
  timezone`, `Decimal(p,s)` → `Decimal(p+k1,s+k2)`, `Byte`/`Short`/`Int` → `Decimal(10+k1,k2)`, and
  `Long` → `Decimal(20+k1,k2)` — and SHALL refuse every other pair, including `Long` → `Double`,
  which the protocol omits because it is lossy above 2^53
* *AND* each decimal target SHALL be checked as `k1 >= k2 >= 0` rather than as `P' >= P` and
  `S' >= S`, so `decimal(10,1)` → `decimal(11,3)` is REFUSED — its integral digit count shrinks from
  9 to 8 — while `decimal(10,1)` → `decimal(12,3)` is accepted
* *AND* the reader SHALL record the offending column's name and a refusal reason naming both types,
  through the SAME refused-column list this feature already carries, so an unsupported change refuses
  only the requests that read or emit that column and leaves the rest of the table queryable
* *AND* the reader SHALL ignore an entry key it does not recognize — notably `tableVersion`, which
  the superseded RFC required and Delta 3.2-era clients still write, including on all thirteen
  entries of the vendored `type-widening` fixture — and MUST NOT refuse an otherwise valid entry for
  carrying one
* *AND* an entry carrying a `fieldPath` SHALL be validated by its `fromType`/`toType` pair alone,
  without parsing the path, because a `fieldPath` names a map key/value or array element and this
  engine already refuses `map` outright and text-renders `array<E>`, so no scalar value is at risk
* *AND* the reader MUST NOT consult `delta.typeChanges` to decide any CAST, because the protocol lets
  a writer remove the annotation once every data file matches the schema and REQUIRES its removal
  when the feature is dropped — the cast reads each file's own physical Parquet type against the
  current logical type (`datafusion-scan/type-relaxation`)
* *AND* a table carrying NO `delta.typeChanges` annotation SHALL pass this validation unchanged,
  whether or not it declares the `typeWidening` reader feature, so every already-queryable Delta
  fixture keeps its recorded behavior byte-identical
<!-- /DELTA:NEW -->
