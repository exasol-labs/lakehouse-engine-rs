-- Still-unsupported-mechanism fixture: an Iceberg table whose data file is
-- ORC (not Parquet).
--
-- `add-deletion-vector-application` task 2.E.4 narrows
-- `e2e_unsupported_delete_fails_loud` away from the deletion-vector table
-- (now a SUPPORTED positive-path fixture -- see
-- create_deletion_vector_fixture.sql) onto a mechanism that remains
-- rejected. Equality deletes cannot be produced by this stack (only Flink's
-- row-level upsert connectors write them, and Flink is not part of this
-- stack -- see run_fixtures.sh's header), so this fixture instead exercises
-- the ORC-data-file arm of `UnsupportedDeleteMechanism`
-- (`classify_manifest_file` in adapter/pushdown.rs rejects ANY non-Parquet
-- data file, independent of whether it carries deletes -- no DELETE is
-- needed here).
--
-- Ground truth (kept in lockstep with
-- crates/lakehouse-engine/tests/common/pos_delete_fixtures.rs -- do not
-- change one without the other):
--   table: rest_catalog.e2e_lakehouse.mor_orc_unsupported
--   rows:  3 (id 1..=3), written as an ORC data file
--
-- This table is NEVER expected to return rows through the VS -- the engine
-- must fail loud naming "ORC data files" before building any scan plan.

CREATE NAMESPACE IF NOT EXISTS rest_catalog.e2e_lakehouse;

DROP TABLE IF EXISTS rest_catalog.e2e_lakehouse.mor_orc_unsupported;

CREATE TABLE rest_catalog.e2e_lakehouse.mor_orc_unsupported (
  id  BIGINT,
  val STRING
)
USING iceberg
TBLPROPERTIES (
  'write.format.default' = 'orc'
);

INSERT INTO rest_catalog.e2e_lakehouse.mor_orc_unsupported
SELECT /*+ REPARTITION(1) */ id, val FROM VALUES
  (1, 'row-01'), (2, 'row-02'), (3, 'row-03')
  AS t(id, val);
