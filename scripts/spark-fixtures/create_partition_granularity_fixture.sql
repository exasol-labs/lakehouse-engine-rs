-- Ground truth, in lockstep with crates/lakehouse-engine/tests/common/pos_delete_fixtures.rs:
--   table:      rest_catalog.e2e_lakehouse.mor_pos_partition
--   partitions: region IN ('east', 'west'), TWO data files each (4 total)
--     east: file 1 = id 1..=5,  file 2 = id 6..=10
--     west: file 3 = id 11..=15, file 4 = id 16..=20
--   rows:       20
--   deleted:    id IN (2, 4, 7, 9, 13, 14, 17, 19)  -- 2 ids per data file
--   remain:     12 rows
-- Partition granularity yields one delete file per partition, each referencing
-- both of that partition's data files.

CREATE NAMESPACE IF NOT EXISTS rest_catalog.e2e_lakehouse;

DROP TABLE IF EXISTS rest_catalog.e2e_lakehouse.mor_pos_partition;

CREATE TABLE rest_catalog.e2e_lakehouse.mor_pos_partition (
  id     BIGINT,
  region STRING,
  val    STRING
)
USING iceberg
PARTITIONED BY (region)
TBLPROPERTIES (
  'format-version'           = '2',
  'write.delete.mode'        = 'merge-on-read',
  'write.update.mode'        = 'merge-on-read',
  'write.merge.mode'         = 'merge-on-read',
  'write.delete.granularity' = 'partition'
);

-- REPARTITION(1): under `local[*]` a bare INSERT ... VALUES writes one file per core.
INSERT INTO rest_catalog.e2e_lakehouse.mor_pos_partition
SELECT /*+ REPARTITION(1) */ id, region, val FROM VALUES
  (1, 'east', 'row-01'), (2, 'east', 'row-02'), (3, 'east', 'row-03'), (4, 'east', 'row-04'), (5, 'east', 'row-05')
  AS t(id, region, val);
INSERT INTO rest_catalog.e2e_lakehouse.mor_pos_partition
SELECT /*+ REPARTITION(1) */ id, region, val FROM VALUES
  (6, 'east', 'row-06'), (7, 'east', 'row-07'), (8, 'east', 'row-08'), (9, 'east', 'row-09'), (10, 'east', 'row-10')
  AS t(id, region, val);

INSERT INTO rest_catalog.e2e_lakehouse.mor_pos_partition
SELECT /*+ REPARTITION(1) */ id, region, val FROM VALUES
  (11, 'west', 'row-11'), (12, 'west', 'row-12'), (13, 'west', 'row-13'), (14, 'west', 'row-14'), (15, 'west', 'row-15')
  AS t(id, region, val);
INSERT INTO rest_catalog.e2e_lakehouse.mor_pos_partition
SELECT /*+ REPARTITION(1) */ id, region, val FROM VALUES
  (16, 'west', 'row-16'), (17, 'west', 'row-17'), (18, 'west', 'row-18'), (19, 'west', 'row-19'), (20, 'west', 'row-20')
  AS t(id, region, val);

-- A strict subset of every file, so Iceberg writes delete files instead of rewriting.
DELETE FROM rest_catalog.e2e_lakehouse.mor_pos_partition
WHERE id IN (2, 4, 7, 9, 13, 14, 17, 19);
