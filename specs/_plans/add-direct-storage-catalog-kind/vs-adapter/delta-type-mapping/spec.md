# Feature: Delta Schema Type Mapping

Maps every type a Delta table schema can declare onto the Arrow tag the scan binds it by or onto a named per-column refusal. This delta changes no recorded description. The paragraph here is plan-local context only.

## Background

* This delta amends ONE scenario and is issue #407. It changes NO Delta answer. Every Arrow tag,
  every declared Exasol type, and every emitted value stays byte-identical.
* **The recorded premise that the shared tag vocabulary holds no `int8` or `int16` entry is
  SUPERSEDED.** `datafusion-scan/type-mapping` widens that vocabulary to every Arrow type the
  compatible-type classifier admits. The widening serves the direct-storage producer this plan
  adds. Both entries exist once this plan merges, so the recorded reason stops describing the code.
* **The recorded rationale beside that premise is also wrong against the code today.**
  `compatible_exasol_type` (`crates/lakehouse-engine/src/types/mapping.rs`) returns
  `DECIMAL(3,0)` for `Int8`, `DECIMAL(5,0)` for `Int16`, and `DECIMAL(10,0)` for `Int32`. The three
  carry three different Exasol declarations rather than one shared `DECIMAL(precision, 0)` shape.
  The recorded mapping table already states those same three values. The table was right and the
  sentence beside it was not.
* **Delta's own answer does not move.** `byte` and `short` keep the `int32` tag. The reason becomes
  the Delta reader's own deliberate choice. The Parquet reader produces physical `Int8` and `Int16`.
  The scan's existing physical-expression adapter widens each to logical `Int32` losslessly.
* The declared Exasol type for a Delta column comes from `unity_type_name_to_exasol`. That function
  reads the Delta type name rather than the Arrow tag. `DECIMAL(3,0)` for `byte` and `DECIMAL(5,0)`
  for `short` are therefore unaffected by any tag decision. The amended clause can drop the
  shared-shape claim without touching a declaration.
* Apache Iceberg and Delta specification check: NOT implicated. This delta changes no Delta type, no
  Arrow tag, no declared Exasol type, and no emitted value. Every recorded conformance argument of
  this feature therefore holds unchanged.

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

* *AND* `byte` and `short` SHALL both keep the EXISTING `int32` tag, and the Delta reader MUST NOT
  tag either column `int8` or `int16`, because the Parquet reader produces physical `Int8` and
  `Int16` and the scan's existing physical-expression adapter widens each to logical `Int32`
  losslessly
* *AND* that mapping SHALL be the Delta reader's OWN deliberate choice rather than a consequence of
  what the shared tag vocabulary spells, SUPERSEDING the recorded reason that the vocabulary holds no
  `int8` or `int16` entry, because `datafusion-scan/type-mapping` adds both entries for the
  direct-storage producer
* *AND* the recorded claim that Exasol gives Int8, Int16, and Int32 one shared
  `DECIMAL(precision, 0)` shape SHALL be DELETED rather than retained, because
  `compatible_exasol_type` returns `DECIMAL(3,0)`, `DECIMAL(5,0)`, and `DECIMAL(10,0)` for the three,
  exactly as the table above already declares
* *AND* NO Delta declared Exasol type and NO Delta emitted value SHALL change under this amendment,
  so every row of the table above stays byte-identical and every recorded assertion of this feature's
  suite passes with no change to any expected value
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
  moves under the recorded #359 amendment is the fractional-second precision of the two timestamp
  rows, and it moves on the 2025.x arm only
<!-- /DELTA:CHANGED -->
