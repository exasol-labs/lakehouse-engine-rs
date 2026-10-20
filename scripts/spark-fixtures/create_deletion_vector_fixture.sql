-- format-version=3 makes a merge-on-read DELETE commit a Puffin deletion vector.
-- Three data files, so a filter can prune every file an unsupported mechanism
-- applies to (e2e_unsupported_delete_fails_loud).
--
-- Ground truth, in lockstep with crates/lakehouse-engine/tests/common/pos_delete_fixtures.rs:
--   table:   rest_catalog.e2e_lakehouse.mor_dv_unsupported
--   files:   id 1..=10 Parquet, deletion vector deletes id IN (3, 7)
--            id 11..=20 Parquet, no delete
--            id 21..=30 ORC

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
SELECT /*+ REPARTITION(1) */ id, printf('row-%02d', id) FROM range(1, 11) AS t(id);

-- A strict subset, so Iceberg writes a deletion vector instead of rewriting the file.
DELETE FROM rest_catalog.e2e_lakehouse.mor_dv_unsupported
WHERE id IN (3, 7);

INSERT INTO rest_catalog.e2e_lakehouse.mor_dv_unsupported
SELECT /*+ REPARTITION(1) */ id, printf('row-%02d', id) FROM range(11, 21) AS t(id);

ALTER TABLE rest_catalog.e2e_lakehouse.mor_dv_unsupported
SET TBLPROPERTIES ('write.format.default' = 'orc');

INSERT INTO rest_catalog.e2e_lakehouse.mor_dv_unsupported
SELECT /*+ REPARTITION(1) */ id, printf('row-%02d', id) FROM range(21, 31) AS t(id);
