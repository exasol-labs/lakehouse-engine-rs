# Feature: Unity Catalog Create Virtual Schema

Enumerates a Unity Catalog namespace during createVirtualSchema and returns one virtual table per Delta base table, mapping each `catalog.schema.table` identifier to an Exasol table name and each Unity Catalog column to an Exasol column type.

## Background

* **This delta amends TWO scenarios and supersedes ONE Background clause, and is issue #329.** It carries the Unity half of the shared catalog-decimal guard whose contract `datafusion-scan/type-mapping` owns, and deletes a recorded recovery path the code never implemented. No enumeration, Delta-base filtering, skip-warn, `TABLE_MAP`, collision, case-fold, or credential-redaction behavior changes.
* **This delta SUPERSEDES the recorded Background clause naming `type_text` as a source of a Unity column's precision and scale.** The recorded sentence — "which the neutral column carries as a source-tagged type descriptor holding the FULL parameterized type — the type name plus precision and scale from the wire `type_precision`/`type_scale`, or `type_text` — so a `DECIMAL` column carries its `p` and `s` rather than a bare `DECIMAL`" — is replaced by the same sentence with the phrase "or `type_text`" DELETED. The descriptor's precision and scale come from `type_precision`/`type_scale` alone. `ColumnInfo` (`crates/lakehouse-catalog/src/unity/client.rs`) declares no `type_text` field and never deserializes one, so naming it advertised a recovery path that does not exist and would mislead a reader debugging a null-precision column into looking for a fallback the code cannot take.
* **An absent `type_precision` is exactly the `p = 0` case the widened guard now absorbs.** `neutral_column` resolves both `Option<u32>` fields through `.unwrap_or(0)`, so a `DECIMAL` column whose `type_precision` is null on the wire reaches the type mapping as `p = 0` and, before this delta, produced the invalid Exasol type `DECIMAL(0,0)` rather than the VARCHAR fallback. Deleting the phantom `type_text` recovery path and widening the guard are therefore two halves of one fix, not two unrelated edits.
* **The guard's predicate, its single-owner requirement, and the Exasol target-type trade-off are owned by `datafusion-scan/type-mapping` and are consumed here, NOT restated.** This feature records only that the Unity arm reads its answer from that one owner, so the two catalog kinds cannot drift.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Unity Catalog Spark column types map to Exasol types sufficient for listing

* *GIVEN* a Unity Catalog table whose columns declare the Spark type names `BOOLEAN`, `INT`, `LONG`, `DOUBLE`, `STRING`, `DATE`, `TIMESTAMP`, and `DECIMAL(p,s)` whose `p` and `s` fall inside Exasol's `DECIMAL` domain — `1 ≤ p ≤ 36` and `s ≤ p`
* *WHEN* the adapter builds the virtual table's column list
* *THEN* the adapter SHALL declare `BOOLEAN` as `BOOLEAN`, `INT` as `DECIMAL(10,0)`, `LONG` as `DECIMAL(20,0)`, `DOUBLE` as `DOUBLE PRECISION`, `STRING` as `VARCHAR(2000000)`, `DATE` as `DATE`, `TIMESTAMP` as `TIMESTAMP`, and `DECIMAL(p,s)` as `DECIMAL(p,s)`, reusing the project's Arrow-to-Exasol convention
* *AND* the source-tagged type descriptor the mapping reads SHALL carry the FULL parameterized Spark type — the type name plus precision and scale from the wire `type_precision`/`type_scale` — so the `DECIMAL` case resolves to `DECIMAL(p,s)` from the descriptor's own `p` and `s` rather than from a bare `DECIMAL` type name that carries neither; and the descriptor MUST NOT be described as reading `type_text`, because `ColumnInfo` declares no such field and deserializes no such value, so no `type_text` recovery path exists for a column whose `type_precision` or `type_scale` is absent
* *AND* the adapter SHALL reach that mapping through the SAME exhaustive match over the neutral column's source-tagged type descriptor that maps an Iceberg column, so the two catalog kinds have ONE Exasol type-mapping home and a third source type is a build failure there
* *AND* the adapter SHALL declare each column name uppercased through the shared case-fold site, so an unquoted column reference in user SQL resolves against the declared name
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: An incompatible Unity Catalog column type is declared as VARCHAR rather than failing

* *GIVEN* a Unity Catalog table whose columns include an array, map, struct, binary, interval, or variant type, or a `DECIMAL` whose precision and scale fall outside Exasol's `DECIMAL` domain — a precision above 36, a precision of 0 (which an absent wire `type_precision` produces through `neutral_column`'s `.unwrap_or(0)`), or a scale exceeding its own precision
* *WHEN* the adapter builds the virtual table's column list
* *THEN* the adapter SHALL declare each such column as `VARCHAR(2000000)` rather than aborting the enumeration
* *AND* the adapter SHALL resolve the `DECIMAL` cases through the SINGLE shared guard `datafusion-scan/type-mapping` owns, and MUST NOT carry a Unity-local copy of the precision/scale predicate, so a Unity `DECIMAL(0,0)` and an Iceberg `decimal(0,0)` are declared identically by construction rather than by coincidence
* *AND* the adapter MUST NOT declare an Exasol type Exasol rejects — in particular MUST NOT declare `DECIMAL(0,0)` or a `DECIMAL(p,s)` with `s > p` — because such a declaration fails `createVirtualSchema` outright, which is the failure this VARCHAR fallback exists to prevent
* *AND* the adapter SHALL treat this listing-sufficient mapping as a deliberate boundary, deferring reader-feature gating and full Delta type fidelity to #322, so #318 never produces an untyped or silently-dropped column
<!-- /DELTA:CHANGED -->
