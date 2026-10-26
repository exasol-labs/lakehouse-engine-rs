-- Ground truth, in lockstep with crates/lakehouse-e2e/tests/common/pos_delete_fixtures.rs:
--   rest_catalog.e2e_lakehouse.mor_dv, data files in commit order:
--     A id 1..=10       v2 positional delete {2}, then a v3 deletion vector {2, 5}
--     D id 41..=50      v2 positional delete {42} only
--     B id 11..=20      deletion vector {15}, replaced by {15, 17}
--     C id 1001..=200000 deletion vector {id % 3 = 0} plus 150000..=160000
--   A's and C's vectors share one Puffin file; B's replacement sits in another.

CREATE NAMESPACE IF NOT EXISTS rest_catalog.e2e_lakehouse;

DROP TABLE IF EXISTS rest_catalog.e2e_lakehouse.mor_dv;

CREATE TABLE rest_catalog.e2e_lakehouse.mor_dv (
  id  BIGINT,
  val STRING
)
USING iceberg
TBLPROPERTIES (
  'format-version'                     = '2',
  'write.delete.mode'                  = 'merge-on-read',
  'write.update.mode'                  = 'merge-on-read',
  'write.merge.mode'                   = 'merge-on-read',
  'write.delete.granularity'           = 'file',
  'write.parquet.row-group-size-bytes' = '131072'
);

-- REPARTITION(1): under `local[*]` a bare INSERT ... VALUES writes one file per core.
INSERT INTO rest_catalog.e2e_lakehouse.mor_dv
SELECT /*+ REPARTITION(1) */ id, printf('row-%02d', id) FROM range(1, 11) AS t(id);

INSERT INTO rest_catalog.e2e_lakehouse.mor_dv
SELECT /*+ REPARTITION(1) */ id, printf('row-%02d', id) FROM range(41, 51) AS t(id);

DELETE FROM rest_catalog.e2e_lakehouse.mor_dv WHERE id IN (2, 42);

ALTER TABLE rest_catalog.e2e_lakehouse.mor_dv SET TBLPROPERTIES ('format-version' = '3');

INSERT INTO rest_catalog.e2e_lakehouse.mor_dv
SELECT /*+ REPARTITION(1) */ id, printf('row-%02d', id) FROM range(11, 21) AS t(id);

INSERT INTO rest_catalog.e2e_lakehouse.mor_dv
SELECT /*+ REPARTITION(1) */ id, printf('row-%02d', id) FROM range(1001, 200001) AS t(id);

DELETE FROM rest_catalog.e2e_lakehouse.mor_dv
WHERE id IN (5, 15)
   OR (id > 1000 AND (id % 3 = 0 OR id BETWEEN 150000 AND 160000));

DELETE FROM rest_catalog.e2e_lakehouse.mor_dv WHERE id = 17;
