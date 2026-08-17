# Feature: Unity Catalog E2E Harness — Delta Query Result Coverage

End-to-end coverage of the actual rows a query returns over the seeded Delta fixtures — delete-free,
deletion-vector, column-mapped, partitioned, join/aggregate, and unplannable-type tables — run through
the same `unity-e2e` stack and virtual schema as `e2e-harness/unity-catalog-e2e-harness`. Split out of
that feature once its scenario count crossed this library's per-spec organization threshold.

## Background

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
