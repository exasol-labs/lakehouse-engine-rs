# Feature: Iceberg Type Promotion

Decides at plan time which Iceberg type promotions this engine reads. The engine reads `int` to `long`, `float` to `double`, and decimal precision widening through the format-neutral relaxation cast, and refuses a `date` to `timestamp` or `timestamp_ns` promotion by name. A promoted table either returns correct rows or says why it cannot.

## Background

* Iceberg table spec, § Schema Evolution, defines the promotion surface:

> Valid primitive type promotions are:
>
> | Primitive type   | v1, v2 valid type promotions | v3+ valid type promotions    | Requirements |
> |------------------|------------------------------|------------------------------|--------------|
> | `unknown`        |                              | _any type_                   | |
> | `int`            | `long`                       | `long`                       | |
> | `date`           |                              | `timestamp`, `timestamp_ns`  | Promotion to `timestamptz` or `timestamptz_ns` is **not** allowed; values outside the promoted type's range must result in a runtime failure |
> | `float`          | `double`                     | `double`                     | |
> | `decimal(P, S)`  | `decimal(P', S)` if `P' > P` | `decimal(P', S)` if `P' > P` | Widen precision only |

* The decimal rule is precision-only: the scale `S` is unchanged and `P' > P`.
* Bounds-width inference, § Schema Evolution: "Iceberg's Avro manifest format does not store the type of lower and upper bounds, and type promotion does not rewrite existing bounds. For example, when a `float` is promoted to `double`, existing data file bounds are encoded as 4 little-endian bytes rather than 8 little-endian bytes for `double`." The original type must be inferred from the width, and the spec's table includes `timestamp` with 4 bytes as `date` and `timestamp_ns` with 4 bytes as `date`.
* The `iceberg` dependency implements that inference for `long` and `double` only. `Timestamp`, `TimestampNs`, `Timestamptz`, and `TimestamptzNs` read 8 bytes unconditionally, so a 4-byte bound under a `timestamp` column fails with `DataInvalid: failed to convert byte slice to array`. A decimal bound decodes length-agnostically at the unchanged scale.
* That failure is not limited to pruning. The bounds decode runs during manifest Avro deserialization, so an unfiltered `SELECT *` fails too, first inside `plan_files`, which reads every manifest when no filter prunes one. A second bounds decode in the `iceberg` dependency uses `unwrap()` and can panic, which aborts the UDF's VM and makes the engine SIGKILL every sibling VM of the statement part.
* The refusal is therefore a plan-time gate on the schema history, decided from `TableMetadata::schemas_iter` before any manifest is read.
* The gate is conservative: it refuses on the recorded promotion alone, without checking whether a pre-promotion data file survives. Establishing that requires the manifest read that fails.
* `unknown` to any type is unreachable. The `iceberg` dependency's `PrimitiveType` has no `Unknown` variant, so a v3 schema declaring `"unknown"` fails deserialization before engine code runs. `iceberg_primitive_to_exasol` and `iceberg_primitive_to_arrow` match `PrimitiveType` exhaustively with no catch-all arm, so an `iceberg` upgrade that adds `Unknown` breaks the build instead of mapping it to the `utf8` fallback.
* The spec's read rule for `unknown`: § Primitive Types requires it to be "Must be optional with `null` defaults; not stored in data files", and § Parquet states "When reading an `unknown` column, any corresponding column must be ignored and replaced with `null` values". Such a column is absent from every data file, which the per-file NULL fill of `scan-read-path/scan-execution-field-id-projection-absent-fields` already answers.
* The Iceberg `FormatReader` builds the logical schema from the table's current schema, so a promoted column's `LogicalField` names the current type. `scan-types/type-relaxation` owns the scan's cast.
* Every error is a `UdfError`, never a panic, and carries no credential value.

## Scenarios

### Scenario: A promotion this engine reads resolves through the shared relaxation cast

* *GIVEN* an Iceberg table whose schema history records a promotion the spec permits at the table's
  format version and this engine supports, `int` → `long`, `float` → `double`, or `decimal(P,S)` →
  `decimal(P',S)` with `P' > P`, and whose data files straddle that promotion
* *WHEN* the adapter resolves the scan for a pushdown request against that table
* *THEN* the adapter SHALL build the logical schema from the table's current schema, so the
  promoted column's `LogicalField` carries the PROMOTED type rather than any data file's type
* *AND* the adapter SHALL plan the request normally, adding no promotion-specific field to
  `ScanSpec`, `FileEntry`, or `LogicalField`, because the promoted type is already what the logical
  schema names and the cast is the scan's business
  (`scan-types/type-relaxation`)
* *AND* file pruning SHALL keep working across the promotion for these three pairs, because the `iceberg`
  dependency implements the spec's bounds-width inference for `long` from 4 bytes and `double` from 4
  bytes, and decodes a decimal bound length-agnostically at the unchanged scale
* *AND* the resolver MUST NOT re-derive the promotion from the schema history for these pairs, so no
  promotion allow-list exists on the read path, the spec's own writer-side rules already bound which
  pairs a conforming table can present

### Scenario: A date-to-timestamp promotion is refused at plan time by name

* *GIVEN* an Iceberg table whose schema history records a field id declared `date` in an earlier
  schema and `timestamp` or `timestamp_ns` in the current schema, the spec's v3+ `date` promotion
* *WHEN* the adapter resolves a pushdown request against that table
* *THEN* the adapter SHALL refuse the request with a `UdfError` naming the table, the column, both
  the earlier and the current Iceberg type, and issue #355, the repository issue that tracks this
  gap, so the refusal reads as scoped work rather than a permanent limitation
* *AND* the refusal SHALL be decided from `TableMetadata::schemas_iter` alone, BEFORE any manifest is
  loaded and before `plan_files` runs, because the failure it replaces occurs
  inside manifest deserialization and would otherwise surface as `failed to convert byte slice to
  array`, naming neither the column nor the promotion
* *AND* the refusal SHALL fire for an unfiltered request as well as a filtered one, because the
  bounds decode this gate stands in front of runs during manifest deserialization rather than during
  pruning, so scoping the gate to requests carrying a predicate would leave `SELECT *` broken
* *AND* the gate SHALL refuse the table on the recorded promotion ALONE, without establishing that a
  pre-promotion data file survives, and this conservatism SHALL be stated in the gate's own doc
  comment, proving no old file remains requires the manifest read that fails
* *AND* a table whose `date` column was never promoted SHALL be unaffected, and a table promoted
  `int` → `long`, `float` → `double`, or decimal-precision SHALL keep planning normally, so the gate
  refuses exactly the two `date` rows and nothing adjacent
* *AND* the refusal SHALL be returned as a `UdfError` value, never raised as a panic, and MUST NOT
  contain any vended or static credential value

### Scenario: The unknown primitive type is unrepresentable, and the mapping is the tripwire

* *GIVEN* the `iceberg` dependency, whose `PrimitiveType` declares no `Unknown` variant,
  so an Iceberg v3 schema declaring a column of type `unknown` fails to deserialize before any engine
  code runs
* *WHEN* the type-mapping test suite runs
* *THEN* the suite SHALL assert that `iceberg_primitive_to_exasol` and `iceberg_primitive_to_arrow`
  each match every `PrimitiveType` variant EXHAUSTIVELY with no catch-all arm, so a dependency
  upgrade that adds `Unknown`, or `variant`, `geometry`, or `geography`, fails the BUILD rather
  than silently mapping the new variant onto the `VARCHAR(2000000)` / `utf8` fallback
* *AND* this engine MUST NOT add a gate, a refusal message, or a mapping arm for `unknown`, because
  the type cannot reach this engine with the current dependency and such code would be
  unreachable from its first commit
* *AND* the unreachability SHALL be recorded as a tracked exception in the
  repository issue that owns Iceberg `unknown` support and in the upstream `apache/iceberg-rust` issue
  that adds the missing `Unknown` primitive type, rather
  than left as a silent gap, because the spec permits `unknown` → any type at v3+ and this engine
  reads no such table
* *AND* the eventual implementation SHALL be the EXISTING per-file NULL fill rather than a cast,
  because the spec requires an `unknown` column to be omitted from data files and read as all-`null`,
  which is the absent-field case `scan-read-path/scan-execution-field-id-projection-absent-fields` already
  resolves
