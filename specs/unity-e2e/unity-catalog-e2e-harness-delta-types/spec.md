# Feature: Unity Catalog E2E Harness — Delta Column Type Coverage

End-to-end coverage of the Exasol TYPES and VALUES a Delta column declares over the seeded
`stats_all_types` and `type_widening` fixtures — type widening across a narrow/wide file boundary,
per-column refusal of an unmappable type, the full varied-types matrix, and the version-gated
timestamp declared-type precision — run through the same `unity-e2e` stack and virtual schema as
`unity-e2e/unity-catalog-e2e-harness-delta-queries`. Split out of that feature once its scenario
count crossed this library's per-spec organization threshold; that sibling feature keeps every
scenario that asserts the ROWS a query returns rather than the declared TYPE of a column.

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
* The seeded fixtures these scenarios query are `stats_all_types` (16 columns, 4 rows, `timestampNtz`
  + `columnMapping`) and `type_widening` (`typeWidening-preview`), both already vendored and seeded;
  no new fixture, Makefile target, or test tier is added. These scenarios extend the existing
  `make test-e2e-unity` suite, in the same `e2e_unity_test.rs` binary as the sibling feature's
  scenarios.
* **This delta is issue #322.** It replaces the single "cannot plan" scenario with three, because the
  engine's answer for the two fixtures that scenario covered has split three ways: `type_widening` and
  `unshredded_variant` are now refused on their READER FEATURE (`unity-e2e/unity-catalog-e2e-harness-delta-queries`
  owns that refusal scenario), `stats_all_types` is now QUERYABLE on its 13 mappable columns, and
  `stats_all_types` is refused only on the three columns whose type this engine cannot render.
* **`type_widening` is already seeded and was never asserted.** `scripts/unity/seed.sh` registers it as
  `unity.delta_e2e.type_widening`, and `scripts/unity/README.md` lists it as a #322 fail-loud fixture,
  but the shipped scenario named only `unshredded_variant` and `stats_all_types`. This feature's added
  scenario puts the seeded fixture under type assertion.
* **`stats_all_types` is registered in Unity Catalog with all 16 columns**, its `array`, `map`,
  `struct`, and `variant` columns declared `STRING` and its `binary_col` declared `BINARY`, so Exasol
  declares every column and a query naming a refused one reaches the adapter rather than failing in
  Exasol's own parser. That is what makes the per-column refusal observable end to end.
* **This delta is issue #349.** `unity.delta_e2e.type_widening` moves from the refused set to the
  queryable set, so the reader-feature refusal scenario (owned by the sibling feature) loses one of
  its two fixtures and a new scenario here asserts the types and values the widened table actually
  returns. No new fixture, Makefile target, or test tier is added.
* **The vendored fixture already carries everything this needs, and MUST NOT be modified.**
  `scripts/unity/fixtures/PROVENANCE.md` records the vendored delta-kernel-rs tables as *"read
  fixtures — never mutated"*. The `type-widening` fixture's commit 0 declares thirteen columns at
  their NARROW types and commits one data file whose Parquet columns are physically narrow; its
  commit 2 widens all thirteen declared types in `metaData.schemaString`, records each change under
  `delta.typeChanges`, and commits a second data file at the WIDE types. Two `add` actions, two data
  files, no `remove` — both files are live at the current snapshot, so a single query reads one
  narrow file and one wide file. That is exactly the straddling shape
  `datafusion-scan/type-relaxation` exists to prove, and no fixture byte needs to change to get it.
* **What DOES have to change is `scripts/unity/seed.sh`, which registers only 3 of the 13 columns.**
  For an EXTERNAL Delta table the engine reads schema and protocol from the Delta log, but the Unity
  Catalog column list is what `createVirtualSchema` enumerates — a column absent from it is not
  selectable from Exasol at all. Registering all thirteen at their WIDENED types is what makes the
  fixture's coverage reachable; it adds no fixture bytes and mutates nothing vendored.
* **The widened `float` column's value is the f32's exact double expansion, not the decimal literal.**
  The narrow file stores `3.4` as a 32-bit float, and widening to `double` preserves those bits
  exactly rather than re-parsing the decimal text — so the returned value is `3.4`'s single-precision
  representation expanded to double, and an assertion written against the literal `3.4` would fail on
  a correct read. This is a property of IEEE 754 widening, not of this engine, and it is recorded
  here so the expectation is set once rather than rediscovered as a bug.
* **Four Delta widening pairs are NOT reachable through this fixture and are covered below the E2E
  tier.** `byte` → `short`, `byte` → `int`, and `short` → `int` all tag `int32` on both sides in this
  engine's vocabulary and are therefore invisible in any logical schema, and `short` → `long` has no
  fixture column. Covering them would need a new Delta table authored by a Spark + delta-spark
  one-shot job this stack does not have. `datafusion-scan/type-relaxation` covers all four against
  purpose-written Parquet files at the scan tier, which exercises the same cast at the same seam for
  a fraction of the cost.
* **Two of the fixture's thirteen recorded changes are outside the Delta protocol's supported list,
  and `vs-adapter/delta-type-mapping`'s validation refuses exactly those two columns.** `byte_decimal`
  (`byte` → `decimal(4,1)`) and `short_decimal` (`short` → `decimal(6,1)`) both derive `k1` as
  NEGATIVE against the protocol's `Byte`/`Short`/`Int` → `Decimal(10+k1,k2)` base of precision 10 —
  `10+k1=4` and `10+k1=6` respectively — so `k1 >= k2 >= 0` fails for both. This is not a defect in
  the fixture or the validation: the fixture is vendored from `delta-kernel-rs` v0.26.0 test data
  under the `typeWidening-preview` (not the finalized `typeWidening`) name, and its preview-era
  decimal target was derived per-source-type rather than from the finalized `Decimal(10+k1,k2)` /
  `Decimal(20+k1,k2)` bases the current protocol specifies. The other eleven columns, including
  `int_decimal` (`decimal(11,1)`, `k1=1`) and `long_decimal` (`decimal(21,1)`, `k1=1`), are within the
  supported list and stay queryable. `byte_decimal` and `short_decimal` are refused per column — the
  same existing mechanism `vs-adapter/delta-type-mapping` already uses for `binary`, `map`, `struct`,
  and `variant` — so a query selecting either of them fails naming the column and both types, while
  the rest of the table, including its other eleven widened columns, stays queryable.
* **This delta is issue #359.** It adds ONE scenario and amends no recorded clause. The added scenario
  makes the Delta suite's timestamp declared-type expectations version-aware and EXACT, so the Delta
  half of the precision change is asserted rather than tolerated. No fixture, row set, value, refusal,
  pruning, or JSON-rendering expectation changes.
* **The recorded Delta declared-type expectations survive the change WITHOUT being amended, because
  `assert_col_type` matches by the space-stripped PREFIX.** `TIMESTAMP(6)` starts with `TIMESTAMP`, so
  `date_timestamp_ntz` in the type-widening scenario and `TIMESTAMP_COL`/`TIMESTAMP_NTZ_COL` in the
  varied-types scenario keep passing on both version arms unchanged. That tolerance is exactly why a
  second, EXACT assertion is needed: without it the Delta path could regress to bare `TIMESTAMP` on a
  2025.x engine with the suite still green.
* **This suite is deliberately NOT matrixed over Exasol versions.** `e2e-unity` stays single-version
  (`e2e-harness/e2e-harness` records why), so the added assertion is version-aware for correctness
  when a developer points the stack at an 8.x image, not because CI exercises both arms here.
* **The expected declared type comes from the ONE shared helper** `e2e-harness/e2e-harness` introduces,
  read from the live session. This suite MUST NOT carry a second version-to-precision table.

## Scenarios

### Scenario: A type-widened Delta table returns its current wider types across the widening boundary

* *GIVEN* the seeded fixture `unity.delta_e2e.type_widening`, whose `readerFeatures` list is
  `timestampNtz` and `typeWidening-preview` — both now allow-listed — and whose two live data files
  are one written BEFORE the widening at the narrow physical types and one written after at the wide
  ones
* *AND* all thirteen of its columns registered in Unity Catalog at their WIDENED types
* *WHEN* the suite issues `SELECT` naming the ELEVEN protocol-supported columns explicitly, plus
  `SELECT COUNT(*)`, and separately issues a `SELECT` naming each of the two refused columns
* *THEN* `SELECT COUNT(*)` SHALL return 2 and the eleven-column projection SHALL return both rows, so
  the pre-widening file is read rather than skipped or failed
* *AND* each of the eleven columns SHALL arrive as its WIDENED declared Exasol type: `byte_long` and
  `int_long` as `DECIMAL(20,0)`; `float_double`, `byte_double`, `short_double`, and `int_double` as
  `DOUBLE PRECISION`; `decimal_decimal_same_scale` as `DECIMAL(20,2)`;
  `decimal_decimal_greater_scale` as `DECIMAL(20,5)`; `int_decimal` as `DECIMAL(11,1)`; `long_decimal`
  as `DECIMAL(21,1)`; and `date_timestamp_ntz` as `TIMESTAMP`
* *AND* `byte_decimal` and `short_decimal` SHALL each fail their `SELECT` with the per-column refusal
  naming the column and both its Delta types (`byte`/`decimal(4,1)` and `short`/`decimal(6,1)`),
  because their recorded `delta.typeChanges` entries derive a NEGATIVE `k1` against the protocol's
  `Decimal(10+k1,k2)` base and are therefore outside the supported list — and MUST NOT return a row
  or a NULL value
* *AND* the PRE-WIDENING row SHALL return its real logged values widened to those types rather than
  NULL — `byte_long` as 1, `int_long` as 2, `int_decimal` as 3.0, `long_decimal` as 4.0, and
  `date_timestamp_ntz` as the midnight instant of its logged date — because a NULL here is exactly
  the silent-wrong-value outcome the reader-feature refusal previously existed to prevent
* *AND* the POST-WIDENING row SHALL return values only the wide types can hold — `byte_long` and
  `int_long` as 9223372036854775807, `long_decimal` as 123456789012345678.9 — so a scan that read
  both files at the narrow width would fail rather than pass on coincidentally-equal values
* *AND* `float_double`'s pre-widening value SHALL be asserted as the stored 32-bit float's exact
  double expansion, and the suite MUST NOT assert the decimal literal `3.4`, because widening
  preserves the single-precision bits rather than re-parsing the text
* *AND* the suite SHALL capture the generated pushdown SQL for at least one of the eleven-column
  queries and assert it drives the scan UDF, so a silent fallback to an unaccelerated wrapper fails
  the suite rather than passing on correct rows
* *AND* the error text of any failure in this scenario MUST NOT contain any credential value

### Scenario: A refused column refuses only the queries naming it

* *GIVEN* the `stats_all_types` Delta table, whose 16 declared columns are now 15 mappable and exactly ONE refused — `binary_col`, refused because casting binary to text replaces every non-UTF-8 byte sequence with NULL (issue #351)
* *WHEN* the harness queries that table through the virtual schema
* *THEN* a projection naming only mappable columns SHALL return its rows, and a query that reads or emits `binary_col` — including `SELECT *`, which widens to the full base row — SHALL fail with an error naming `binary_col` and its refusal reason
* *AND* `map_col` and `nested_struct` MUST NOT appear in any refusal, because both are now rendered as JSON `VARCHAR(2000000)` per `datafusion-scan/nested-json-rendering`
* *AND* a WHERE clause referencing `binary_col` SHALL still refuse the query even when the select list names only mappable columns
* *AND* the refusal message for `binary_col` SHALL cite issue #351 and MUST NOT cite issue #350, because #350 is this plan and a closed issue cited in a shipped refusal reads as an unfixed gap with no owner

### Scenario: A Delta table's varied types return their expected Exasol types and values

* *GIVEN* the `stats_all_types` Delta table's 15 mappable columns, in fixture column order, with `array_col`, `map_col`, and `nested_struct` now among them
* *WHEN* the harness queries every mappable column and compares the returned Exasol types and values
* *THEN* the 12 natively-representable columns SHALL keep their recorded Exasol types and values byte-identical, unchanged by this delta
* *AND* `array_col`, `map_col`, and `nested_struct` SHALL each be declared `VARCHAR(2000000)` and SHALL each return a value that parses as JSON
* *AND* `array_col` SHALL return a JSON array of bare numbers, so its recorded bracketed display rendering (`[1, 2]`, an Arrow value-formatter artifact) is replaced by a strict-JSON array
* *AND* `nested_struct` SHALL return an object keyed by the LOGICAL inner names `inner_int`, `inner_string`, and `inner_double`, and MUST NOT return any `col-` prefixed physical name — the assertion that makes the nested column-mapping resolution falsifiable
* *AND* `map_col` SHALL return an object keyed by its own string keys
* *AND* a row whose nested value is NULL SHALL return SQL NULL rather than the text `null`

### Scenario: A Delta timestamp column's declared Exasol type is asserted exactly at the engine's precision

* *GIVEN* the `stats_all_types` Delta table's `timestamp_col` (Delta `timestamp`) and `timestamp_ntz_col` (Delta `timestamp without time zone`) columns, and the `type_widening` table's `date_timestamp_ntz` column
* *AND* the running engine's own version, read from the live session through the ONE shared helper `e2e-harness/e2e-harness` introduces
* *WHEN* the harness reads each column's `COLUMN_TYPE` from `SYS.EXA_ALL_COLUMNS`
* *THEN* each of the three columns' declared type SHALL equal EXACTLY `TIMESTAMP(6)` when the helper's expected precision is 6 and EXACTLY `TIMESTAMP` when it is 3, matched in full rather than by the space-stripped prefix the recorded assertions use
* *AND* the recorded prefix-tolerant expectations for those same columns SHALL be left in place unchanged, because they still hold on both arms — this scenario ADDS the exact check rather than tightening theirs
* *AND* the assertion SHALL cover BOTH Delta timestamp type names, so a change that gated only one of the two Unity type names would fail here rather than pass by covering the other
* *AND* no row value, JSON rendering, refusal, or pruning expectation of this feature SHALL change
* *AND* the scenario SHALL FAIL, not skip, when no live `unity-e2e` stack is available, per this repo's E2E contract
