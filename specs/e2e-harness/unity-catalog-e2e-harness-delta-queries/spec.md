# Feature: Unity Catalog E2E Harness — Delta Query Result Coverage

End-to-end coverage of the actual rows a query returns over the seeded Delta fixtures — delete-free,
deletion-vector, column-mapped, partitioned, join/aggregate, type-widened, and unplannable-type
tables — run through the same `unity-e2e` stack and virtual schema as
`e2e-harness/unity-catalog-e2e-harness`. Split out of that feature once its scenario count crossed
this library's per-spec organization threshold.

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
* **Split from `e2e-harness/unity-catalog-e2e-harness`, issue #320.** That feature keeps harness
  bring-up, createVirtualSchema enumeration, the virtual schema's storage-credential wiring, the
  stack-unavailable failure contract, and the credential-leak guarantee. This feature owns every
  scenario that asserts the ROWS a query returns over a seeded Delta table.
* The seeded fixtures these scenarios query are `multi_part_stats` (5 files, 5 rows, delete-free,
  unpartitioned), `table_with_dv` (1 file, 10 physical rows, a UUID-relative deletion vector of
  cardinality 2), `cm_id_mode` and `cm_name_mode` (`col-<uuid>` physical names under `id` and `name`
  column mapping), `basic_partitioned` (6 files, 6 rows, partitioned by `letter`, one file under the
  Hive default-partition directory), and `unshredded_variant` / `stats_all_types` (types this engine
  does not map).
* No new fixture, Makefile target, or test tier is added. These scenarios extend the existing
  `make test-e2e-unity` suite, in the same `e2e_unity_test.rs` binary as the sibling feature's
  scenarios.
* The virtual schema these scenarios query against is the one
  `e2e-harness/unity-catalog-e2e-harness` § "The suite's virtual schema carries the storage
  credentials a UDF-side scan needs" creates; that scenario's guarantee — a CONNECTION carrying the
  MinIO endpoint and static storage credentials, provisioned through the shared harness scan-UDF
  definition — is a precondition every scenario below relies on.
* **This delta is issue #322.** It replaces the single "cannot plan" scenario with three, because the
  engine's answer for the two fixtures that scenario covered has split three ways: `type_widening` and
  `unshredded_variant` are now refused on their READER FEATURE, `stats_all_types` is now QUERYABLE on
  its 13 mappable columns, and `stats_all_types` is refused only on the three columns whose type this
  engine cannot render.
* **`type_widening` is already seeded and was never asserted.** `scripts/unity/seed.sh` registers it as
  `unity.delta_e2e.type_widening`, and `scripts/unity/README.md` lists it as a #322 fail-loud fixture,
  but the shipped scenario names only `unshredded_variant` and `stats_all_types`. This delta puts the
  seeded fixture under assertion.
* **No new fixture, Makefile target, or test tier is added, for this delta either.** These scenarios
  extend the existing `make test-e2e-unity` suite, in the same `e2e_unity_test.rs` binary as the
  sibling feature's scenarios. The three fixtures involved — `stats_all_types` (16 columns, 4 rows,
  `timestampNtz` + `columnMapping`), `unshredded_variant` (`variantType-preview`), and `type_widening`
  (`typeWidening-preview`) — are all already vendored and seeded.
* **`stats_all_types` is registered in Unity Catalog with all 16 columns**, its `array`, `map`,
  `struct`, and `variant` columns declared `STRING` and its `binary_col` declared `BINARY`, so Exasol
  declares every column and a query naming a refused one reaches the adapter rather than failing in
  Exasol's own parser. That is what makes the per-column refusal observable end to end.
* **This delta is issue #321.** It adds the row-level half of Delta plan-time file pruning: proof that
  a query whose files were pruned returns the SAME rows it returned before pruning existed. The
  plan-side half — which files survive, asserted on the resolved file list and on the generated
  pushdown SQL — belongs to `vs-adapter/delta-file-pruning`, because it asserts a planning outcome
  rather than a returned row. This feature keeps its recorded charter: every scenario that asserts the
  ROWS a query returns over a seeded Delta table.
* **No new fixture, Makefile target, or test tier is added, for this delta either.** The two fixtures
  involved are already seeded and already under assertion: `basic_partitioned` (6 files, 6 rows,
  partitioned by `letter`, one file under the Hive default-partition directory) and `multi_part_stats`
  (5 files, 5 rows, delete-free, unpartitioned, disjoint per-file statistics). These scenarios extend
  the existing `make test-e2e-unity` suite in the same `e2e_unity_test.rs` binary.
* **The shipped partitioned scenario is the anchor this delta builds on, and it stays unedited.** Its
  clause requiring `SELECT * FROM ... WHERE letter = 'a'` to return exactly the rows whose logged
  partition value is `a` was written when the filter narrowed rows only. Pruning does not change the
  rows it demands, so it holds verbatim and becomes the regression that catches an unsound prune.
* **This delta is issue #349.** `unity.delta_e2e.type_widening` moves from the refused set to the
  queryable set, so the reader-feature refusal scenario loses one of its two fixtures and a new
  scenario asserts the rows the widened table actually returns. No new fixture, Makefile target, or
  test tier is added: these scenarios extend the existing `make test-e2e-unity` suite in the same
  `e2e_unity_test.rs` binary.
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
* **`unshredded_variant` keeps its refusal unchanged.** `variantType-preview` stays off the
  allow-list, so the reader-feature refusal scenario keeps a live fixture and the gate keeps an
  end-to-end assertion.
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

## Scenarios

### Scenario: A delete-free Delta table returns its rows end to end

* *GIVEN* the seeded delete-free, unpartitioned fixture registered as `unity.delta_e2e.multi_part_stats`, whose five active data files hold five rows in total
* *WHEN* the suite issues `SELECT *` and `SELECT COUNT(*)` against that virtual table
* *THEN* `SELECT COUNT(*)` SHALL return 5 and `SELECT *` SHALL return those 5 rows with their column values, which is this engine's FIRST full round trip over a Delta table
* *AND* the rows SHALL arrive under the virtual table's declared column names and Exasol types
* *AND* the suite MUST fail (not skip) when the Unity Catalog server, MinIO, or Exasol is unreachable

### Scenario: A Delta table with deletion vectors returns only its live rows

* *GIVEN* the seeded deletion-vector fixture registered as `unity.delta_e2e.table_with_dv`, whose single active data file physically holds 10 rows and carries a deletion vector of cardinality 2 removing the rows whose `value` is 0 and 9
* *WHEN* the suite issues `SELECT COUNT(*)` and `SELECT value` against that virtual table
* *THEN* `SELECT COUNT(*)` SHALL return 8, not 10, so the aggregate observes post-delete rows
* *AND* the returned `value` set MUST NOT contain 0 or 9, and SHALL contain every other value the file holds
* *AND* a query whose predicate selects a deleted row — `WHERE value = 0` — SHALL return no row, so the deletion vector is applied beneath the pushed-down filter rather than after it

### Scenario: A column-mapped Delta table returns values under its logical column names

* *GIVEN* the seeded column-mapping fixtures registered as `unity.delta_e2e.cm_id_mode` and `unity.delta_e2e.cm_name_mode`, whose Parquet columns are named `col-<uuid>` while their Delta schemas declare `id`, `name`, and `value`
* *WHEN* the suite issues `SELECT id, name, value` against EACH virtual table
* *THEN* both queries SHALL return the real physical values under the logical column names, so the id-mode table binds by Parquet field id and the name-mode table binds by declared physical name
* *AND* neither query SHALL return NULL for a column the data file carries, which is what a logical-name-only binding would produce against a `col-<uuid>` physical name
* *AND* both tables SHALL return the SAME rows for the same projection, because the two column-mapping modes differ only in the binding key

### Scenario: A partitioned Delta table returns its partition column values

* *GIVEN* the seeded partitioned fixture registered as `unity.delta_e2e.basic_partitioned`, partitioned by `letter` across six data files holding six rows, one of which lives under the Hive default-partition directory because its `letter` is NULL
* *WHEN* the suite issues `SELECT letter, number, a_float` against that virtual table
* *THEN* each row SHALL carry the `letter` value logged for the file it came from, and the row from the default-partition file SHALL carry NULL
* *AND* no row SHALL carry the Hive default-partition directory name as its `letter` value
* *AND* `SELECT * FROM ... WHERE letter = 'a'` SHALL return exactly the rows whose logged partition value is `a`, and `SELECT letter, COUNT(*) ... GROUP BY letter` SHALL group on the materialized values, so a partition column is usable as a predicate target and as a group key

### Scenario: Join and aggregate pushdown reach a Delta table by the same route as a scan

* *GIVEN* the seeded fixtures `unity.delta_e2e.basic_partitioned` and `unity.delta_e2e.multi_part_stats` in one virtual schema
* *WHEN* the suite issues a grouped aggregate over one table, an ORDER BY with LIMIT over one table, and an inner equi-join between the two whose broadcast side is the PARTITIONED table
* *THEN* every query SHALL return the same rows a single-node engine returns for the same data, so no request shape is left unreachable or wrong by the Delta routing
* *AND* the join result SHALL carry the broadcast side's partition column values, so partitioning the broadcast side changes nothing about the join result
* *AND* the suite SHALL capture the generated pushdown SQL for at least one of these queries and assert it drives the scan UDF, so a silent fallback to an unaccelerated wrapper fails the suite rather than passing on correct rows

### Scenario: A Delta table using an unsupported reader feature fails the query loud

* *GIVEN* the seeded fixture whose Delta `protocol` action declares a reader feature this engine does
  not implement — `unity.delta_e2e.unshredded_variant` (`variantType-preview`)
* *WHEN* the suite issues a query against it
* *THEN* the query SHALL fail with the reader's plan-time gating error naming the refused feature by
  its Delta protocol name — `variantType-preview` — and MUST NOT return a row
* *AND* the error MUST NOT be a type-mapping error naming a column, because the table's reader
  feature is refused BEFORE its schema is mapped, and a column-typed error here would prove the gate
  ran too late
* *AND* the error MUST NOT cite issue #349, because type widening is no longer refused and a closed
  issue cited in a shipped refusal reads as an unfixed gap with no owner
* *AND* the failure MUST arrive as a SQL error rather than as a crashed UDF VM — checked by a
  follow-up query surviving on the same connection — so an unsupported table is a diagnosable refusal
  rather than an abnormal exit
* *AND* the error text MUST NOT contain any credential value

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

### Scenario: A query whose files were pruned returns the same rows as before pruning

* *GIVEN* the seeded fixtures `unity.delta_e2e.basic_partitioned` and `unity.delta_e2e.multi_part_stats`
* *WHEN* the suite issues a partition-column predicate against the first, a statistics-excluded range
  predicate and an equality against the second, and a predicate matching no file at all
* *THEN* every query SHALL return exactly the rows the same query returns against the same data with no
  pruning predicate applied, so pruning is invisible in every result
* *AND* a predicate matching NO file SHALL return zero rows as a normal empty result, and MUST NOT
  fail, hang, or return a row
* *AND* a query mixing a prunable predicate with an unprunable one — for example an equality alongside
  a `LIKE` — SHALL return the rows BOTH predicates select, proving the unprunable half is still
  evaluated above the scan rather than dropped with the pruning it could not drive
* *AND* the suite SHALL capture the generated pushdown SQL for at least one pruning query and assert it
  drives the scan UDF, so a silent fallback to an unaccelerated wrapper fails the suite rather than
  passing on correct rows
* *AND* the suite MUST fail (not skip) when the Unity Catalog server, MinIO, or Exasol is unreachable
