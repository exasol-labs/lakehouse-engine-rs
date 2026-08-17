# Feature: Unity Catalog E2E Harness — Delta Query Result Coverage

End-to-end coverage of the actual rows a query returns over the seeded Delta fixtures — delete-free,
deletion-vector, column-mapped, partitioned, join/aggregate, and unplannable-type tables — run through
the same `unity-e2e` stack and virtual schema as `e2e-harness/unity-catalog-e2e-harness`. Split out of
that feature once its scenario count crossed this library's per-spec organization threshold.

## Background

* **This delta is issue #322.** It replaces the single "cannot plan" scenario with three, because the
  engine's answer for the two fixtures that scenario covered has split three ways: `type_widening` and
  `unshredded_variant` are now refused on their READER FEATURE, `stats_all_types` is now QUERYABLE on
  its 13 mappable columns, and `stats_all_types` is refused only on the three columns whose type this
  engine cannot render.
* **`type_widening` is already seeded and was never asserted.** `scripts/unity/seed.sh` registers it as
  `unity.delta_e2e.type_widening`, and `scripts/unity/README.md` lists it as a #322 fail-loud fixture,
  but the shipped scenario names only `unshredded_variant` and `stats_all_types`. This delta puts the
  seeded fixture under assertion.
* **No new fixture, Makefile target, or test tier is added.** These scenarios extend the existing
  `make test-e2e-unity` suite, in the same `e2e_unity_test.rs` binary as the sibling feature's
  scenarios. The three fixtures involved — `stats_all_types` (16 columns, 4 rows, `timestampNtz` +
  `columnMapping`), `unshredded_variant` (`variantType-preview`), and `type_widening`
  (`typeWidening-preview`) — are all already vendored and seeded.
* **`stats_all_types` is registered in Unity Catalog with all 16 columns**, its `array`, `map`,
  `struct`, and `variant` columns declared `STRING` and its `binary_col` declared `BINARY`, so Exasol
  declares every column and a query naming a refused one reaches the adapter rather than failing in
  Exasol's own parser. That is what makes the per-column refusal observable end to end.

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: A Delta table this engine cannot plan fails the query loud

* *GIVEN* the recorded rule that BOTH `unity.delta_e2e.unshredded_variant` and
  `unity.delta_e2e.stats_all_types` fail every query with a plan-time error naming a column and its
  Delta type, and citing issue #322
* *WHEN* the suite issues a query against each
* *THEN* this scenario SHALL be REMOVED, because issue #322 splits its two fixtures apart:
  `stats_all_types` is now queryable on 13 of its 16 columns, so a scenario asserting that EVERY query
  against it fails would now assert the opposite of the intended behavior
* *AND* its two surviving guarantees — that the failure arrives as a SQL error rather than a crashed
  UDF VM, and that the error text carries no credential value — SHALL be restated in each replacement
  scenario rather than dropped
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: A Delta table using an unsupported reader feature fails the query loud

* *GIVEN* the seeded fixtures whose Delta `protocol` action declares a reader feature this engine does
  not implement — `unity.delta_e2e.type_widening` (`typeWidening-preview`) and
  `unity.delta_e2e.unshredded_variant` (`variantType-preview`)
* *WHEN* the suite issues a query against each
* *THEN* each query SHALL fail with the reader's plan-time gating error naming the refused feature by
  its Delta protocol name — `typeWidening-preview` and `variantType-preview` respectively — and MUST
  NOT return a row
* *AND* the `type_widening` error SHALL cite issue #349, so the refusal is traceable to the follow-up
  that will lift it
* *AND* the error MUST NOT be a type-mapping error naming a column, because both tables' reader
  features are refused BEFORE their schemas are mapped, and a column-typed error here would prove the
  gate ran too late
* *AND* the failure MUST arrive as a SQL error rather than as a crashed UDF VM — checked by a
  follow-up query surviving on the same connection — so an unsupported table is a diagnosable refusal
  rather than an abnormal exit
* *AND* the error text MUST NOT contain any credential value

### Scenario: A Delta table spanning varied types returns the expected Exasol types and values

* *GIVEN* the seeded broad-type fixture `unity.delta_e2e.stats_all_types`, whose 4 rows span 13
  mappable Delta types under `name` column mapping — `byte`, `short`, `integer`, `long`, `float`,
  `double`, `date`, `timestamp`, `timestamp_ntz`, `string`, `decimal(10,2)`, `boolean`, and
  `array<integer>` — and whose reader features are `timestampNtz` and `columnMapping`, both supported
* *WHEN* the suite issues `SELECT` naming those 13 columns explicitly, plus `SELECT COUNT(*)`
* *THEN* `SELECT COUNT(*)` SHALL return 4 and the projection SHALL return those 4 rows with the values
  the fixture's data file holds, under the virtual table's declared column names
* *AND* each column SHALL arrive as its declared Exasol type: `byte_col` as `DECIMAL(3,0)`,
  `short_col` as `DECIMAL(5,0)`, `int_col` as `DECIMAL(10,0)`, `long_col` as `DECIMAL(20,0)`,
  `float_col` and `double_col` as `DOUBLE PRECISION`, `date_col` as `DATE`, `timestamp_col` and
  `timestamp_ntz_col` as `TIMESTAMP`, `string_col` as `VARCHAR`, `decimal_col` as `DECIMAL(10,2)`,
  `boolean_col` as `BOOLEAN`, and `array_col` as `VARCHAR`
* *AND* `byte_col` and `short_col` SHALL return their real logged values rather than NULL, so the
  `int32` logical tag over the Parquet reader's physical `Int8`/`Int16` is proven end to end and not
  only in a unit test
* *AND* `array_col` SHALL return a non-NULL `VARCHAR` carrying a bracketed rendering of the array's
  integer elements, which is what the engine's incompatible-type-to-`VARCHAR` path produces for it;
  the suite MUST NOT assert strict JSON conformance of that text, because exact JSON rendering for
  nested types is issue #350
* *AND* the suite SHALL capture the generated pushdown SQL for at least one of these queries and
  assert it drives the scan UDF, so a silent fallback to an unaccelerated wrapper fails the suite
  rather than passing on correct rows
* *AND* the suite MUST fail (not skip) when the Unity Catalog server, MinIO, or Exasol is unreachable

### Scenario: A Delta column this engine cannot render refuses only the queries that name it

* *GIVEN* the same `unity.delta_e2e.stats_all_types` fixture, whose `binary_col`, `map_col`, and
  `nested_struct` columns Unity Catalog declares — as `BINARY`, `STRING`, and `STRING` — and whose
  Delta types this engine refuses
* *WHEN* the suite issues `SELECT binary_col`, `SELECT map_col`, `SELECT nested_struct`, and
  `SELECT *` against that virtual table
* *THEN* each of the four queries SHALL fail with a plan-time error naming the refused column and its
  Delta type, and MUST NOT return a row — including `SELECT *`, whose full-row projection covers all
  three
* *AND* the `binary_col`, `map_col`, and `nested_struct` errors SHALL cite issue #350, so the refusal
  is traceable to the follow-up that will lift it
* *AND* a query whose WHERE clause names a refused column while its select list names only mappable
  ones SHALL ALSO fail, because a `binary` column pushed into the scan's filter would otherwise be
  compared as text with every non-UTF-8 value silently NULL
* *AND* the 13-column projection of the scenario above SHALL keep succeeding on the SAME table in the
  SAME suite run, which is what proves the refusal is scoped to the column rather than to the table
* *AND* the failure MUST arrive as a SQL error rather than as a crashed UDF VM — checked by a
  follow-up query surviving on the same connection — and the error text MUST NOT contain any
  credential value
<!-- /DELTA:NEW -->
