# Feature: Type Relaxation

Reads a data file whose physical column type is NARROWER than the table's current logical type — the
shape Delta type widening and Iceberg type promotion both leave behind — by casting each file's
column up to the current type at scan time, so a schema-evolved table returns its real values under
its current types instead of wrong values or an unresolved-column error.

## Background

* This delta amends one scenario and adds three, each split for scenario size. It supersedes the recorded narrowing-direction clauses, so the recorded Background statement that the overflow asymmetry is unreachable from this feature holds again.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A narrow physical column binds to the current wider logical type and is cast per file

* *GIVEN* a scan spec whose logical schema declares a column at the table's CURRENT type — the type after a Delta type widening, an Iceberg type promotion, or a direct-storage footer fold
* *AND* two assigned files, one written BEFORE the change whose physical Parquet column carries the narrow source type, and one written AFTER whose physical column carries the current type
* *WHEN* the scan UDF reads both files in one shard
* *THEN* the UDF SHALL register the DataFusion table schema from the scan spec's `LogicalField` list and MUST NOT infer it from any data file, so the column's declared type is the CURRENT one for both files; the column-binding adapter SHALL choose the narrow physical field by its binding key alone — field-id, declared physical name, or identity — and MUST NOT compare Arrow data types to decide which field binds, because a type-equality binding would fail on exactly the file this scenario exists for
* *AND* the delegated `DefaultPhysicalExprAdapter` SHALL insert the physical-to-logical cast into the physical expression tree, so every filter, projection, aggregate, and join key evaluated by DataFusion sees the column at its CURRENT type rather than its physical one
* *AND* the emitted rows from the OLD file SHALL carry that file's real values widened to the current type, and the emitted rows from the NEW file SHALL carry theirs unchanged; the cast SHALL be decided PER FILE from that file's own footer schema, so a shard straddling the change needs no per-shard grouping by physical layout
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: The cast resolution holds across every format and stops at the refusal boundary

* *GIVEN* the resolution above applies to an Iceberg scan, a Delta scan, a direct-storage scan, and a Unity Catalog Parquet scan, because all four populate the same `LogicalField` list and install the same adapter
* *WHEN* each format's scan installs that adapter
* *THEN* the resolution SHALL hold identically for all four, and the scan side MUST NOT branch on table format nor on whether a writer or the engine decided the current type
* *AND* a logical type NARROWER than a file's physical column SHALL NOT reach this cast, SUPERSEDING the recorded clauses that routed it through a narrowing cast, and SHALL instead be refused as the scenario "A physical type outside the admitted set is refused before any cast" states
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A physical type outside the admitted set is refused before any cast

* *GIVEN* a scan spec carrying a logical schema, and assigned files whose physical columns bind to logical columns by field-id, by declared physical name, or by identity
* *WHEN* the scan UDF opens each file
* *THEN* the UDF SHALL hand a bound column to the cast only when its physical type is ADMITTED for the declared logical type, decided per file from that file's own footer schema: admitted when the supported-set owner (`widen`) resolves the pair to the logical type (an equal type or any supported-set row); admitted for two timestamp types when the physical unit equals the logical unit or is coarser, unless the physical type carries a time zone other than `UTC` or `+00:00` and the logical type carries none, because only that pair changes a stored instant; and admitted for a string-tagged logical field against every string encoding and every primitive type the JSON-fallback classifier routes to text (`datafusion-scan/type-mapping`) — binary, fixed-size binary, time, an out-of-domain decimal — because that text rendering IS the declared type, a deliberate Exasol trade-off since Exasol has no binary or time type; and admitted for a physical `Null` column under every logical type, because an all-NULL column's cast changes no value
* *AND* a dictionary-encoded physical column that no rule above admits SHALL be judged by its value type
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A refused pair fails only a query that reads it, for every format, with no credential in the error

* *GIVEN* a bound column pair that no admission rule above admits, reached through any binding key — field-id, declared physical name, or identity — on any scan source
* *WHEN* a query references that column in its projection or filter
* *THEN* the UDF SHALL fail the query with a clean error naming the table's storage location, the column, the declared type, and the physical type — for every refused pair, including `double` under `int`, `int64` under `int32`, `timestamp` under `date`, `string` under a numeric type, and a numeric type under `string` — and the error MUST NOT contain any credential value; the UDF MUST NOT cast a refused pair, so no truncated, rounded, overflowing, or unparseable value reaches a filter, an aggregate, or an emitted row
* *AND* a query that references no refused column of a file SHALL still read that file, and a nested-descriptor column's JSON rendering, an unclaimed column's NULL-fill, `initial-default`, and required-absent error, and a spec without a logical schema SHALL keep their recorded behavior, because none of them reaches the cast
* *AND* the rule SHALL hold identically for every format and every binding key
<!-- /DELTA:NEW -->
