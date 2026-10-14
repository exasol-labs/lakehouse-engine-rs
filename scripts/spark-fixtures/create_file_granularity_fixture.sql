-- Ground truth, in lockstep with crates/lakehouse-engine/tests/common/pos_delete_fixtures.rs:
--   table:   rest_catalog.e2e_lakehouse.mor_pos_file
--   rows:    20 (id 1..=20), TWO data files (ids 1..=10, 11..=20)
--   deleted: id IN (3, 8, 13, 17)   -- two ids from each data file
--   remain:  16 rows

CREATE NAMESPACE IF NOT EXISTS rest_catalog.e2e_lakehouse;

DROP TABLE IF EXISTS rest_catalog.e2e_lakehouse.mor_pos_file;

CREATE TABLE rest_catalog.e2e_lakehouse.mor_pos_file (
  id  BIGINT,
  val STRING
)
USING iceberg
TBLPROPERTIES (
  'format-version'           = '2',
  'write.delete.mode'        = 'merge-on-read',
  'write.update.mode'        = 'merge-on-read',
  'write.merge.mode'         = 'merge-on-read',
  'write.delete.granularity' = 'file',
  -- The default 'hash' mode shuffles into one partition (shuffle.partitions=1),
  -- so each delete file would reference both data files; 'none' keeps one per
  -- data file. DELETE FROM has no REPARTITION hint.
  'write.delete.distribution-mode' = 'none'
);

-- Two INSERTs -> two data files. REPARTITION(1): under `local[*]` a bare
-- INSERT ... VALUES writes one file per core.
INSERT INTO rest_catalog.e2e_lakehouse.mor_pos_file
SELECT /*+ REPARTITION(1) */ id, val FROM VALUES
  (1, 'row-01'), (2, 'row-02'), (3, 'row-03'), (4, 'row-04'), (5, 'row-05'),
  (6, 'row-06'), (7, 'row-07'), (8, 'row-08'), (9, 'row-09'), (10, 'row-10')
  AS t(id, val);

INSERT INTO rest_catalog.e2e_lakehouse.mor_pos_file
SELECT /*+ REPARTITION(1) */ id, val FROM VALUES
  (11, 'row-11'), (12, 'row-12'), (13, 'row-13'), (14, 'row-14'), (15, 'row-15'),
  (16, 'row-16'), (17, 'row-17'), (18, 'row-18'), (19, 'row-19'), (20, 'row-20')
  AS t(id, val);

-- A strict subset of each file, so Iceberg writes delete files instead of rewriting.
DELETE FROM rest_catalog.e2e_lakehouse.mor_pos_file
WHERE id IN (3, 8, 13, 17);
