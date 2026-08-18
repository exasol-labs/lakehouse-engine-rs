# Feature: Unity Catalog E2E Harness — Delta Query Result Coverage

End-to-end coverage of the actual rows a query returns over the seeded Delta fixtures — delete-free,
deletion-vector, column-mapped, partitioned, join/aggregate, type-widened, and unplannable-type
tables — run through the same `unity-e2e` stack and virtual schema as
`e2e-harness/unity-catalog-e2e-harness`.

## Background

* **This delta is issue #350.** The vendored `stats-all-types` fixture already carries every column
  this delta needs: `array_col` (`array<integer>`), `map_col` (`map<string, integer>`), and
  `nested_struct` (`struct<inner_int, inner_string, inner_double>`), populated across 4 rows. No new
  Delta fixture is provisioned.
* **`stats-all-types` is the nested column-mapping case, which is why it is the load-bearing
  fixture here.** Its metadata declares `delta.columnMapping.mode = name`, and its three inner
  `StructField`s carry `delta.columnMapping.physicalName` values
  `col-7f2f94cf-7082-430c-bba7-852bc6c5215e`, `col-26fcfd6b-04c7-4772-8bdf-04ac9425f06e`, and
  `col-92dcf16d-d249-48a9-afb8-93deeaf7ce23`. A rendering that read the PHYSICAL nested names would
  emit those identifiers as JSON keys, so this fixture is the only end-to-end proof that
  `datafusion-scan/nested-json-rendering`'s logical-name resolution actually fires on the Delta path.
* **Two recorded scenarios of this feature change their expected column sets, and the change is a
  narrowing of the refused set, not a new capability claim.** `map_col` and `nested_struct` move from
  refused to queryable; `binary_col` stays refused (issue #351).

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A refused column refuses only the queries naming it

* *GIVEN* the `stats_all_types` Delta table, whose 16 declared columns are now 15 mappable and exactly ONE refused — `binary_col`, refused because casting binary to text replaces every non-UTF-8 byte sequence with NULL (issue #351)
* *WHEN* the harness queries that table through the virtual schema
* *THEN* a projection naming only mappable columns SHALL return its rows, and a query that reads or emits `binary_col` — including `SELECT *`, which widens to the full base row — SHALL fail with an error naming `binary_col` and its refusal reason
* *AND* `map_col` and `nested_struct` MUST NOT appear in any refusal, because both are now rendered as JSON `VARCHAR(2000000)` per `datafusion-scan/nested-json-rendering`
* *AND* a WHERE clause referencing `binary_col` SHALL still refuse the query even when the select list names only mappable columns
* *AND* the refusal message for `binary_col` SHALL cite issue #351 and MUST NOT cite issue #350, because #350 is this plan and a closed issue cited in a shipped refusal reads as an unfixed gap with no owner
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A Delta table's varied types return their expected Exasol types and values

* *GIVEN* the `stats_all_types` Delta table's 15 mappable columns, in fixture column order, with `array_col`, `map_col`, and `nested_struct` now among them
* *WHEN* the harness queries every mappable column and compares the returned Exasol types and values
* *THEN* the 12 natively-representable columns SHALL keep their recorded Exasol types and values byte-identical, unchanged by this delta
* *AND* `array_col`, `map_col`, and `nested_struct` SHALL each be declared `VARCHAR(2000000)` and SHALL each return a value that parses as JSON
* *AND* `array_col` SHALL return a JSON array of bare numbers, so its recorded bracketed display rendering (`[1, 2]`, an Arrow value-formatter artifact) is replaced by a strict-JSON array
* *AND* `nested_struct` SHALL return an object keyed by the LOGICAL inner names `inner_int`, `inner_string`, and `inner_double`, and MUST NOT return any `col-` prefixed physical name — the assertion that makes the nested column-mapping resolution falsifiable
* *AND* `map_col` SHALL return an object keyed by its own string keys
* *AND* a row whose nested value is NULL SHALL return SQL NULL rather than the text `null`
<!-- /DELTA:CHANGED -->
