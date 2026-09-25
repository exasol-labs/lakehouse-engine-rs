-- format-version=3 makes a merge-on-read DELETE commit a Puffin deletion vector,
-- which the engine must reject at plan time (e2e_unsupported_delete_fails_loud).
-- Once iceberg-rust reads v3 DVs (apache/iceberg-rust#2681, #2580, #2411), this
-- fixture becomes readable and that test must be re-pointed or retired.
--
-- Ground truth, in lockstep with crates/lakehouse-engine/tests/common/pos_delete_fixtures.rs:
--   table:   rest_catalog.e2e_lakehouse.mor_dv_unsupported
--   rows:    10 (id 1..=10), ONE data file
--   deleted: id IN (3, 7)

CREATE NAMESPACE IF NOT EXISTS rest_catalog.e2e_lakehouse;

DROP TABLE IF EXISTS rest_catalog.e2e_lakehouse.mor_dv_unsupported;

CREATE TABLE rest_catalog.e2e_lakehouse.mor_dv_unsupported (
  id  BIGINT,
  val STRING
)
USING iceberg
TBLPROPERTIES (
  'format-version'    = '3',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode'  = 'merge-on-read'
);

-- REPARTITION(1): under `local[*]` a bare INSERT ... VALUES writes one file per core.
INSERT INTO rest_catalog.e2e_lakehouse.mor_dv_unsupported
SELECT /*+ REPARTITION(1) */ id, val FROM VALUES
  (1, 'row-01'), (2, 'row-02'), (3, 'row-03'), (4, 'row-04'), (5, 'row-05'),
  (6, 'row-06'), (7, 'row-07'), (8, 'row-08'), (9, 'row-09'), (10, 'row-10')
  AS t(id, val);

-- A strict subset, so Iceberg writes a deletion vector instead of rewriting the file.
DELETE FROM rest_catalog.e2e_lakehouse.mor_dv_unsupported
WHERE id IN (3, 7);
