# Feature: Unity Catalog E2E Harness: Parquet Table Coverage

End-to-end coverage of a Unity Catalog table whose `data_source_format` is `PARQUET`. It runs in the existing `make test-e2e-unity` suite and CI `e2e-unity` job, against the same stack, test binary, and virtual schema as the Delta coverage.

## Background

* The suite writes the fixture's raw Parquet files to MinIO and registers the table through the Unity Catalog REST API (`POST /tables`, `table_type` `EXTERNAL`, `data_source_format` `PARQUET`, each column carrying `type_json` and `partition_index`). It does both in its one-time setup, before it creates the virtual schema.
* The fixture `unity.delta_e2e.sales_parquet` holds four rows in three files, under `year=2024/region=eu/`, `year=2024/region=us/`, and `year=2025/region=eu/`. Its columns are `id` (`LONG`) and `amount` (`DOUBLE`), stored in the files, and the partition columns `year` (`INT`, `partition_index` 0) and `region` (`STRING`, `partition_index` 1), stored in no file.

## Scenarios

### Scenario: A Unity Parquet table appears in the createVirtualSchema listing

* *GIVEN* the running Unity Catalog stack with `sales_parquet` registered
* *WHEN* the suite creates the virtual schema over `unity.delta_e2e`
* *THEN* the virtual schema SHALL expose `SALES_PARQUET`, a table the listing skipped before this feature
* *AND* `SALES_PARQUET` SHALL declare `ID` as `DECIMAL(20,0)`, `AMOUNT` as `DOUBLE`, `YEAR` as `DECIMAL(10,0)`, and `REGION` as `VARCHAR(2000000)`, in that column order

### Scenario: A Unity Parquet table returns its rows and partition values end to end

* *GIVEN* the virtual schema exposing `SALES_PARQUET`
* *WHEN* the suite selects every column ordered by `ID`, and separately runs the same query with `WHERE REGION = 'eu'` and with `WHERE YEAR = 2024`
* *THEN* the unfiltered query SHALL return the four fixture rows, each carrying the `YEAR` and `REGION` values of its own file's directory
* *AND* each filtered query SHALL return exactly the fixture rows that satisfy its predicate
* *AND* the suite MUST fail, not skip, when the Unity Catalog server, MinIO, or Exasol is unreachable

### Scenario: A Unity Parquet table's scan resolves identically under vended and static credentials

* *GIVEN* the registered `sales_parquet` table and the OSS Unity Catalog server's vended MinIO session
* *WHEN* the suite resolves that table's scan through the format-reader seam, once with `use_vended_credentials` enabled and once with the CONNECTION's static MinIO credentials
* *THEN* BOTH runs SHALL return the same file list, the same per-file partition values, and the same table root
* *AND* the vended run SHALL list the files with the credentials the Unity Catalog temporary-table-credentials response returns for the table's own vending key
* *AND* the suite SHALL assert three files and each file's `year` and `region` values, so a regression in credential vending, listing, or partition filling fails the suite
* *AND* no vended or static credential value SHALL appear in any assertion message or test output
