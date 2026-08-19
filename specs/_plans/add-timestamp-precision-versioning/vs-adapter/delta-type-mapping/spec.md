# Feature: Delta Schema Type Mapping

Maps every type a Delta table schema can declare either onto the Arrow tag the scan binds it by or
onto a named per-column refusal, so a Delta column is queryable when this engine can render its value
faithfully and refused when it cannot — never described by a tag that returns the wrong value.

## Background

* **This delta is issue #359.** It AMENDS the "Declared Exasol type" column of TWO rows in ONE
  scenario's mapping table and adds no scenario. Every Arrow tag in that table, every nullability
  clause, the `byte`/`short` `int32` clause, the shared decimal-guard clause, and the
  ten-tags-byte-identical clause stay unchanged, as do the text-rendered set, the per-name refusal set,
  and every other scenario of this feature.
* **The Arrow side does NOT move; only the Exasol declaration does.** A Delta `timestamp` keeps the
  `timestamptz_us` tag and a `timestamp without time zone` keeps `timestamp_us`, so the scan binds,
  filters, and coerces exactly as recorded. What changes is the string Exasol is told the column is —
  and only on an engine that can express a fractional-second precision.
* **The declared type is not this feature's own decision to make.** The version rule and both
  declaration strings are owned by `datafusion-scan/type-mapping`; the single `ctx.database_version()`
  read is owned by `vs-adapter/create-virtual-schema`; and the production function that renders a Delta
  column's declaration is `unity_type_name_to_exasol`, whose clause `vs-adapter/
  unity-catalog-create-virtual-schema` owns. This table's "Declared Exasol type" column mirrors those
  answers so a reader of the Delta mapping sees the same declaration the adapter emits — it MUST NOT
  become a second statement of the rule.
* **Delta's own timestamp resolution is microsecond, so the amended declaration is faithful rather
  than generous.** The Delta protocol defines `timestamp` and `timestamp without time zone` as
  microsecond-precision types, matching the Iceberg spec's microsecond `timestamp`/`timestamptz` this
  plan quotes, so the same `TIMESTAMP(6)` target is correct for both formats and neither needs a
  format-specific precision.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Every Delta type Exasol represents natively maps to its own Arrow tag

* *GIVEN* a Delta table schema declaring one column of each type in the native set — exactly
  `boolean`, `byte`, `short`, `integer`, `long`, `float`, `double`, `string`, `date`, `timestamp`,
  `timestamp without time zone`, and `decimal(p,s)` whose `p` and `s` satisfy Exasol's catalog-decimal
  domain
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* each column SHALL carry exactly the Arrow tag this table gives it, and SHALL carry its
  nullability from the Delta schema:

  | Delta type | Arrow tag | Declared Exasol type |
  |---|---|---|
  | `boolean` | `bool` | BOOLEAN |
  | `byte` | `int32` | DECIMAL(3,0) |
  | `short` | `int32` | DECIMAL(5,0) |
  | `integer` | `int32` | DECIMAL(10,0) |
  | `long` | `int64` | DECIMAL(20,0) |
  | `float` | `float32` | DOUBLE PRECISION |
  | `double` | `float64` | DOUBLE PRECISION |
  | `string` | `utf8` | VARCHAR(2000000) |
  | `date` | `date32` | DATE |
  | `timestamp` | `timestamptz_us` | TIMESTAMP(6) on Exasol 2025.x and later, TIMESTAMP on 8.x |
  | `timestamp without time zone` | `timestamp_us` | TIMESTAMP(6) on Exasol 2025.x and later, TIMESTAMP on 8.x |
  | `decimal(p,s)`, `1 ≤ p ≤ 36` and `s ≤ p` | `decimal128(p,s)` | DECIMAL(p,s) |

* *AND* `byte` and `short` SHALL both map to the EXISTING `int32` tag, and this feature MUST NOT add an
  `int8` or an `int16` tag to the shared tag vocabulary, because Exasol gives Int8, Int16, and Int32
  the same `DECIMAL(precision, 0)` shape and the Parquet reader's physical `Int8`/`Int16` widens to
  logical `Int32` losslessly through the scan's existing physical-expression adapter
* *AND* the decimal domain check SHALL read the SINGLE shared
  `exasol_representable_catalog_decimal` predicate in `crates/lakehouse-engine/src/types/mapping.rs`
  and MUST NOT carry its own copy, so the Delta, Iceberg, and Unity Catalog answers stay in lockstep
  by construction, as `datafusion-scan/type-mapping` requires
* *AND* the two TIMESTAMP declarations SHALL read the version rule and both declaration strings from
  the SINGLE owner `datafusion-scan/type-mapping` specifies, so a Delta timestamp and an Iceberg
  timestamp are declared at the same precision by construction; this table mirrors that answer and
  MUST NOT restate the rule or either literal
* *AND* the ten tags this table shares with the superseded scenario SHALL stay byte-identical, so no
  already-queryable Delta column changes the Arrow tag it is bound by; the ONLY declared type that
  moves under this delta is the fractional-second precision of the two timestamp rows, and it moves on
  the 2025.x arm only
<!-- /DELTA:CHANGED -->
</content>
