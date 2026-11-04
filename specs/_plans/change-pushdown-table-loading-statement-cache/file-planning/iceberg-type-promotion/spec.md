# Feature: Iceberg Type Promotion

Decides at plan time which Iceberg type promotions this engine reads. The engine reads `int` to `long`, `float` to `double`, and decimal precision widening through the format-neutral relaxation cast, and refuses a `date` to `timestamp` or `timestamp_ns` promotion by name. A promoted table either returns correct rows or says why it cannot.

<!-- DELTA:CHANGED -->
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
* The Iceberg library this engine uses implements that inference for `long` and `double` only. It reads every `timestamp`, `timestamp_ns`, `timestamptz`, and `timestamptz_ns` bound as 8 bytes, so a 4-byte bound under a `timestamp` column fails with `DataInvalid: failed to convert byte slice to array`. It decodes a decimal bound of any length at the unchanged scale.
* That failure is not limited to pruning. The library decodes bounds while it reads a manifest, so an unfiltered `SELECT *`, which reads every manifest, fails too. A second bounds decode in the library can panic, which aborts the UDF's VM and makes the engine SIGKILL every sibling VM of the statement part.
* The refusal is therefore a plan-time gate on the table's schema history, decided before any manifest is read.
* The gate is conservative: it refuses on the recorded promotion alone, without checking whether a pre-promotion data file survives. Establishing that requires the manifest read that fails.
* `unknown` to any type is unreachable, because the Iceberg library fails to read a v3 schema that declares `"unknown"`, so such a table fails to load.
* The spec's read rule for `unknown`: § Primitive Types requires it to be "Must be optional with `null` defaults; not stored in data files", and § Parquet states "When reading an `unknown` column, any corresponding column must be ignored and replaced with `null` values". Such a column is absent from every data file, which the per-file NULL fill of `scan-read-path/scan-execution-field-id-projection-absent-fields` already answers.
* An Iceberg table's logical schema comes from its current schema, so a promoted column has its current type. `scan-types/type-relaxation` owns the scan's cast.
* Every failure returns an error, never a panic, and no error text carries a credential value.
<!-- /DELTA:CHANGED -->

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
