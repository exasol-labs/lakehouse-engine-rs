-- arrow-rs decodes INT96 as Timestamp(Nanosecond) by default, which overflows
-- past 2262-04-11 (#143). This table carries a genuinely INT96-encoded file.
--
-- Iceberg's Spark writer always emits INT64, so the file is written natively and
-- registered as-is via add_files. INT96 output applies only to Spark's zoned
-- TIMESTAMP, so the source column is TIMESTAMP while the Iceberg column is
-- TIMESTAMP_NTZ (avoiding the unrelated timestamptz mapping, #118). The path
-- uses s3:// because add_files records the writer's scheme and the scan UDF
-- registers its object store under s3://.
--
-- Ground truth, in lockstep with crates/lakehouse-engine/tests/common/int96_fixtures.rs:
--   table:  rest_catalog.e2e_lakehouse.int96_ts_far_future
--   column: ts  -- Iceberg `timestamp` WITHOUT time zone (Spark TIMESTAMP_NTZ)
--   rows:   1
--   value:  9999-12-31 23:59:59  (physically INT96-encoded)

-- Each fixture runs in its own spark-sql process, so these SETs do not leak.
SET spark.sql.parquet.outputTimestampType=INT96;
-- Without this, Spark 3.5 falls back to INT64.
SET spark.sql.parquet.writeLegacyFormat=true;
SET spark.sql.session.timeZone=UTC;

CREATE NAMESPACE IF NOT EXISTS rest_catalog.e2e_lakehouse;

DROP TABLE IF EXISTS rest_catalog.e2e_lakehouse.int96_ts_far_future;

CREATE TABLE rest_catalog.e2e_lakehouse.int96_ts_far_future (
  ts TIMESTAMP_NTZ
)
USING iceberg
TBLPROPERTIES (
  'format-version' = '2'
);

-- REPARTITION(1) so the fixture-shape test finds exactly one data file.
INSERT OVERWRITE DIRECTORY 's3://warehouse/e2e_lakehouse/int96_ts_far_future_source'
USING parquet
SELECT /*+ REPARTITION(1) */ ts
FROM VALUES (CAST('9999-12-31 23:59:59' AS TIMESTAMP)) AS t(ts);

CALL rest_catalog.system.add_files(
  table => 'e2e_lakehouse.int96_ts_far_future',
  source_table => '`parquet`.`s3://warehouse/e2e_lakehouse/int96_ts_far_future_source`'
);
