# Feature: INT96 Far-Future-Timestamp E2E Fixture (Apache Spark)

Adds an Iceberg fixture to the end-to-end test stack whose timestamp column is physically encoded
as Parquet INT96 and carries a value outside the Arrow nanosecond range, produced by Apache Spark
(Iceberg Spark runtime), so the scan path's INT96 decode-coercion is validated full-stack against
the exact overflow issue #143 reports. Real-world writers such as Fivetran emit INT96 timestamps
with far-future sentinel values into Iceberg-registered tables; this fixture reproduces that shape.

## Background

* The fixture reuses the existing `spark-iceberg-fixtures` one-shot Compose job and the shared
  Iceberg REST catalog over MinIO every other E2E table uses — no new dependency is introduced.
* Iceberg's own Spark writer emits Parquet INT64 timestamps regardless of
  `spark.sql.parquet.outputTimestampType` (verified), so a plain `INSERT INTO <iceberg_table>` does
  NOT produce INT96. INT96 reaches an Iceberg table only when Spark's NATIVE Parquet writer (with
  `spark.sql.parquet.outputTimestampType=INT96`) writes the file and it is then registered — as-is,
  without rewrite — via the Iceberg `add_files` procedure, the real-world Hive/Fivetran-style path.
* apache/iceberg#8949 confirms INT96 files CAN be registered into an Iceberg table via `add_files`;
  its reported `long overflow` is a defect in Iceberg's own Java INT96 reader, NOT evidence about
  `add_files`, and irrelevant to this engine's arrow-rs read path. Because `add_files` registers the
  data file as-is, a fixture-shape test MUST assert the committed data file is physically INT96, so a
  silent INT64 result (or an unexpected rewrite) fails loudly rather than making the scan test pass
  vacuously.
* The fixture column is Iceberg `timestamp` WITHOUT time zone, never `timestamptz`: a `timestamptz`
  column would add an unrelated time-zone-mapping variable (#118) to a fixture whose only job is
  proving the INT96 decode fix. `timestamp` maps to a plain Exasol `TIMESTAMP`, isolating this
  fixture to the INT96 decode path under test.
* The `outputTimestampType=INT96` setting is scoped to this fixture's script so it does not change
  the positional-delete fixtures' writes.
* The far-future value is `9999-12-31 23:59:59`: outside the Arrow nanosecond range (max 2262) so it
  reproduces the overflow, inside the Arrow microsecond range so the coerced decode succeeds, and
  inside Exasol's `TIMESTAMP` maximum (year 9999) so the value emits without an Exasol range error.
* Ground truth (table name `int96_ts_far_future`, its Iceberg `timestamp`-without-tz column,
  inserted rows, and timestamp values) lives in the Rust test harness and MUST stay in lockstep with
  the Spark SQL script that produces the fixture.

## Scenarios

### Scenario: Spark produces an INT96-encoded far-future-timestamp fixture

* *GIVEN* the E2E stack is running with the shared REST catalog over MinIO and an Apache Spark service with the Iceberg Spark runtime
* *AND* Spark is configured to write Parquet timestamps as INT96 (`spark.sql.parquet.outputTimestampType=INT96`), scoped to this fixture's script
* *WHEN* the fixture step writes a Spark-native Parquet table holding at least one row whose Iceberg `timestamp` (WITHOUT time zone) column is outside the Arrow nanosecond range (`9999-12-31 23:59:59`) and imports it into an Iceberg table via the Iceberg `add_files` procedure
* *THEN* the fixture SHALL commit an Iceberg snapshot whose data file encodes the `timestamp` column as Parquet INT96, and a fixture-shape test SHALL assert that physical INT96 encoding directly from the committed data file so a silent INT64 result fails loudly
* *AND* the fixture SHALL record the exact inserted rows and their timestamp values so a test can assert the scan result
* *AND* the fixture step SHALL fail, not skip, if the Spark service, the REST catalog, or MinIO is unavailable
