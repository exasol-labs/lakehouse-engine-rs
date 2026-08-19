# Feature: Unity Catalog Create Virtual Schema

Enumerates a Unity Catalog namespace during createVirtualSchema and returns one virtual table per Delta base table, mapping each `catalog.schema.table` identifier to an Exasol table name and each Unity Catalog column to an Exasol column type.

## Background

* **This delta is issue #359.** It AMENDS ONE clause of ONE scenario and adds no scenario. The amended
  clause is the declared Exasol type for the Spark type name `TIMESTAMP`, which becomes version-gated.
  Every other declared type in that scenario, the exhaustive-match requirement, the parameterized-
  descriptor requirement, the case-fold clause, the Delta-base filter, the exclusion warnings, and the
  incompatible-type VARCHAR fallback stay byte-identical.
* **The Delta declaration path IS this Unity path, which is why the amendment lands here.** A Delta
  table reaches `createVirtualSchema` only through the Unity Catalog kind, so
  `unity_type_name_to_exasol` is the one production function that declares a Delta timestamp column's
  Exasol type. Widening issue #359 from its Iceberg-only wording to cover Delta means amending this
  clause, not the Arrow-input resolver its scope text names.
* **This delta closes the timestamp-precision half of this feature's own recorded #322 deferral.** The
  feature description defers *"deeper Delta schema fidelity — reader-feature gating, timestamp
  precision, type widening, and variant types"* to #322. The DECLARATION half of "timestamp precision"
  is settled here; reader-feature gating, type widening, and variant types are unaffected and stay
  where they are recorded.
* **The version rule and both declaration strings have ONE owner outside this feature.**
  `datafusion-scan/type-mapping` owns them, and `vs-adapter/create-virtual-schema` owns the single
  `ctx.database_version()` read. This feature only records which string a Unity `TIMESTAMP` and
  `TIMESTAMP_NTZ` column receives, and MUST NOT restate the rule or either literal.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Unity Catalog Spark column types map to Exasol types sufficient for listing

* *GIVEN* a Unity Catalog table whose columns declare the Spark type names `BOOLEAN`, `INT`, `LONG`, `DOUBLE`, `STRING`, `DATE`, `TIMESTAMP`, and `DECIMAL(p,s)` whose `p` and `s` fall inside Exasol's `DECIMAL` domain — `1 ≤ p ≤ 36` and `s ≤ p`
* *WHEN* the adapter builds the virtual table's column list
* *THEN* the adapter SHALL declare `BOOLEAN` as `BOOLEAN`, `INT` as `DECIMAL(10,0)`, `LONG` as `DECIMAL(20,0)`, `DOUBLE` as `DOUBLE PRECISION`, `STRING` as `VARCHAR(2000000)`, `DATE` as `DATE`, and `DECIMAL(p,s)` as `DECIMAL(p,s)`, reusing the project's Arrow-to-Exasol convention
* *AND* the adapter SHALL declare `TIMESTAMP` and `TIMESTAMP_NTZ` as `TIMESTAMP(6)` on an Exasol version of 2025.x or later and as the bare string `TIMESTAMP` on 8.x, reading BOTH the version rule and both declaration strings from the single owner `datafusion-scan/type-mapping` specifies — so a Delta timestamp column and an Iceberg timestamp column are declared at the same precision by construction, and this feature carries no copy of either literal
* *AND* the source-tagged type descriptor the mapping reads SHALL carry the FULL parameterized Spark type — the type name plus precision and scale from the wire `type_precision`/`type_scale` — so the `DECIMAL` case resolves to `DECIMAL(p,s)` from the descriptor's own `p` and `s` rather than from a bare `DECIMAL` type name that carries neither; and the descriptor MUST NOT be described as reading `type_text`, because `ColumnInfo` declares no such field and deserializes no such value, so no `type_text` recovery path exists for a column whose `type_precision` or `type_scale` is absent
* *AND* the adapter SHALL reach that mapping through the SAME exhaustive match over the neutral column's source-tagged type descriptor that maps an Iceberg column, so the two catalog kinds have ONE Exasol type-mapping home and a third source type is a build failure there
* *AND* the adapter SHALL declare each column name uppercased through the shared case-fold site, so an unquoted column reference in user SQL resolves against the declared name
<!-- /DELTA:CHANGED -->
</content>
