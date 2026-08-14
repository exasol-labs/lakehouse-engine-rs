# Feature: Unity Catalog E2E Harness

End-to-end coverage of the Virtual Schema against a native Unity Catalog OSS server backed by MinIO
and seeded with the vendored Delta fixtures, run through the shared harness so the script DDL is
byte-identical to every other E2E binary. The suite fails, never skips, when the stack is unavailable.

## Background

* **This delta is issue #320 and lifts the suite's scan-execution ceiling.** The suite stops at
  catalog metadata and plan-time scan resolution today; it now issues real queries through Exasol and
  asserts the rows they return.
* The seeded fixtures this delta queries are `multi_part_stats` (5 files, 5 rows, delete-free,
  unpartitioned), `table_with_dv` (1 file, 10 physical rows, a UUID-relative deletion vector of
  cardinality 2), `cm_id_mode` and `cm_name_mode` (`col-<uuid>` physical names under `id` and `name`
  column mapping), and `basic_partitioned` (6 files, 6 rows, partitioned by `letter`, one file under
  the Hive default-partition directory).
* No new fixture, Makefile target, or test tier is added. The scenarios extend the existing
  `make test-e2e-unity` suite.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Create virtual schema over a Unity Catalog namespace lists the fixture tables and columns

* *GIVEN* a running Unity Catalog stack seeded with the fixtures and an Exasol CONNECTION whose address is `http://unitycatalog:8080`, whose password supplies no auth field, and a createVirtualSchema request whose `CATALOG_KIND` is `UNITY_CATALOG` and whose `ICEBERG_NAMESPACE` property is `unity.delta_e2e`
* *WHEN* the suite issues createVirtualSchema against that CONNECTION
* *THEN* the created virtual schema SHALL expose one virtual table per seeded fixture table, each named by the shared flatten-and-uppercase rule
* *AND* each virtual table SHALL declare its columns with Exasol types mapped from the Unity Catalog column types, so a seeded fixture's columns appear with the expected Exasol types
* *AND* the suite SHALL assert the presence of a representative fixture table and its column set, so a regression in enumeration or column mapping fails the suite
* *AND* this scenario SHALL keep issuing no scan-driving query of its own, SUPERSEDING the recorded clause that bounded the WHOLE SUITE to no scan execution: the scenarios below now run real queries, so what this scenario asserts is enumeration alone rather than the suite's ceiling
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: The suite's virtual schema carries the storage credentials a UDF-side scan needs

* *GIVEN* the seeded fixtures on MinIO and an OSS Unity Catalog server that vends no object-storage endpoint
* *WHEN* the suite creates the virtual schema the query scenarios below run against
* *THEN* that virtual schema's CONNECTION SHALL supply the MinIO endpoint and static storage credentials, so the scan UDF running inside Exasol reaches MinIO with a credential resolved from the CONNECTION rather than from a test-process injection
* *AND* the suite SHALL provision the scan UDF script through the SAME shared harness definition every other E2E binary uses, so the scan script DDL is byte-identical across suites
* *AND* adding those credentials MUST NOT change the enumeration scenario's result, because listing reads catalog metadata and no object storage
* *AND* the vended-versus-static planning scenario SHALL keep running unchanged, so credential vending stays covered where the OSS server can serve it

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

### Scenario: A Delta table this engine cannot plan fails the query loud

* *GIVEN* the seeded fixtures whose Delta schemas declare types this engine does not map — `unity.delta_e2e.unshredded_variant` and `unity.delta_e2e.stats_all_types`
* *WHEN* the suite issues a query against each
* *THEN* each query SHALL fail with the reader's plan-time error naming the column and its Delta type, and MUST NOT return a row
* *AND* the failure MUST arrive as a SQL error rather than as a crashed UDF VM, so an unsupported table is a diagnosable refusal rather than an abnormal exit
* *AND* the error text MUST NOT contain any credential value
<!-- /DELTA:NEW -->
