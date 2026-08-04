-- INT96 far-future-timestamp fixture (issue #143).
--
-- WHY THIS FIXTURE EXISTS: arrow-rs decodes a Parquet INT96 timestamp column as
-- Timestamp(Nanosecond) by default, whose i64 range ends at 2262-04-11, so a
-- far-future value like 9999-12-31 23:59:59 overflows at decode time on a plain
-- SELECT *. The scan fix (coerce_int96="us" / coerce_int96_tz="UTC") decodes such
-- columns at microsecond resolution instead; this fixture is the genuinely
-- INT96-encoded Iceberg table that exercises that path end-to-end.
--
-- WHY NATIVE-WRITE + add_files (NOT INSERT INTO): Iceberg's own Spark writer
-- emits INT64 timestamps regardless of spark.sql.parquet.outputTimestampType, so
-- INSERT INTO an Iceberg table can never produce INT96. Only a *native*
-- (non-Iceberg) Spark Parquet write honors outputTimestampType=INT96, and the
-- Iceberg add_files procedure registers that file into the table AS-IS (no
-- rewrite) -- the one path that lands a genuinely INT96-encoded data file in an
-- Iceberg table. apache/iceberg#8949 confirms add_files DOES register INT96
-- files; the `long overflow` it reports is a bug in Iceberg *Java*'s reader, not
-- evidence against add_files and irrelevant to this engine's arrow-rs path. Task
-- 2.3's fixture-shape test asserts the committed data file is physically INT96,
-- failing loud if this ever silently degrades to INT64.
--
-- WHY THE SOURCE COLUMN IS `TIMESTAMP` BUT THE ICEBERG COLUMN IS `TIMESTAMP_NTZ`:
-- outputTimestampType=INT96 is honored ONLY for Spark's zoned TIMESTAMP (LTZ);
-- a TIMESTAMP_NTZ column is written as INT64 in Spark 3.5 regardless. So the
-- native SOURCE column must be TIMESTAMP (LTZ) to be encoded as INT96. An INT96
-- value carries no timezone annotation, so add_files maps it by name into the
-- Iceberg `timestamp` WITHOUT time zone column (Spark TIMESTAMP_NTZ) declared
-- below. That column is deliberately WITHOUT zone: a `timestamptz` column would
-- add an unrelated time-zone-mapping variable (#118) to a fixture whose only
-- job is proving the #143 overflow fix, so it is avoided here. session.timeZone
-- is pinned to UTC so the wall-clock literal round-trips deterministically
-- through INT96 (stored as a UTC instant) back to exactly 9999-12-31 23:59:59.
--
-- WHY s3:// (NOT s3a://): the scan UDF registers its object store under the
-- `s3://` scheme (register_bucket_store, crates/lakehouse-engine/src/scan/mod.rs),
-- and add_files records a data file's path with whatever scheme wrote it. A file
-- recorded under `s3a://` would resolve to an unregistered store and fail the
-- scan. run_fixtures.sh aliases the `s3` scheme to Hadoop's S3AFileSystem for
-- this fixture (Hadoop 3.3.4 binds only `s3a` by default) so the native write --
-- and thus the registered data-file path -- uses `s3://`, matching every other
-- table's scheme.
--
-- Ground truth (kept in lockstep with
-- crates/lakehouse-engine/tests/common/int96_fixtures.rs -- do not change one
-- without the other):
--   table:  rest_catalog.e2e_lakehouse.int96_ts_far_future
--   column: ts  -- Iceberg `timestamp` WITHOUT time zone (Spark TIMESTAMP_NTZ)
--   rows:   1
--   value:  9999-12-31 23:59:59  (physically INT96-encoded)
--
-- DROP CONDITION: this fixture stands on its own -- it does NOT share the
-- positional-delete fixtures' apache/iceberg-rust#340 drop condition. Retire it
-- only if the scan stops reading through a custom DataFusion ParquetSource or the
-- INT96 coercion is removed.

-- INT96 physical encoding for zoned TIMESTAMP columns, scoped to THIS spark-sql
-- invocation only: run_fixtures.sh runs each fixture as its own `spark-sql -f`
-- process, so this SET cannot leak into the other fixture scripts' sessions.
SET spark.sql.parquet.outputTimestampType=INT96;
-- Spark 3.5 treats INT96 as a legacy encoding; writeLegacyFormat=true makes the
-- writer actually emit INT96 for the TIMESTAMP column rather than falling back to
-- the modern INT64 form. Harmless here -- the only column is a timestamp.
SET spark.sql.parquet.writeLegacyFormat=true;
-- Deterministic round-trip: interpret the wall-clock literal below in UTC so the
-- INT96-stored instant reads back as exactly 9999-12-31 23:59:59.
SET spark.sql.session.timeZone=UTC;

CREATE NAMESPACE IF NOT EXISTS rest_catalog.e2e_lakehouse;

DROP TABLE IF EXISTS rest_catalog.e2e_lakehouse.int96_ts_far_future;

-- Iceberg target table: a single `timestamp` WITHOUT time zone column. Spark
-- TIMESTAMP_NTZ maps to Iceberg `timestamp` (Spark TIMESTAMP would map to
-- `timestamptz` -- see the header for why that is deliberately avoided).
CREATE TABLE rest_catalog.e2e_lakehouse.int96_ts_far_future (
  ts TIMESTAMP_NTZ
)
USING iceberg
TBLPROPERTIES (
  'format-version' = '2'
);

-- Native (non-Iceberg) Spark Parquet write of the single far-future row. The
-- source column is Spark TIMESTAMP (LTZ) so outputTimestampType=INT96 is honored.
-- REPARTITION(1) forces a single output file (mirrors the positional-delete
-- fixtures' INSERTs) so the fixture-shape test resolves exactly one data file to
-- inspect. The `_source` directory is a sibling of the Iceberg table location;
-- add_files references the file there in place (no copy), and the scan reads it
-- from the same MinIO bucket.
INSERT OVERWRITE DIRECTORY 's3://warehouse/e2e_lakehouse/int96_ts_far_future_source'
USING parquet
SELECT /*+ REPARTITION(1) */ ts
FROM VALUES (CAST('9999-12-31 23:59:59' AS TIMESTAMP)) AS t(ts);

-- Register the native INT96 file into the Iceberg table AS-IS (no rewrite), so
-- the committed data file stays physically INT96. The parquet column name `ts`
-- matches the Iceberg column, so add_files (and the scan's field-id-with-name
-- fallback binding) map it correctly despite the file carrying no Iceberg
-- field-ids.
CALL rest_catalog.system.add_files(
  table => 'e2e_lakehouse.int96_ts_far_future',
  source_table => '`parquet`.`s3://warehouse/e2e_lakehouse/int96_ts_far_future_source`'
);
