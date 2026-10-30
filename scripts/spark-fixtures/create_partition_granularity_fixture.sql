-- Ground truth, in lockstep with crates/lakehouse-e2e/tests/common/pos_delete_fixtures.rs:
--   table:      rest_catalog.e2e_lakehouse.mor_pos_partition
--   partitions: region IN ('east=', 'west#?'), TWO data files each (4 total); Iceberg
--               escapes them, so every data and delete file key holds '%'
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

-- REPARTITION(1): under `local[*]` a bare INSERT ... SELECT writes one file per core.
INSERT INTO rest_catalog.e2e_lakehouse.mor_pos_partition
SELECT /*+ REPARTITION(1) */ id, 'east=', format_string('row-%02d', id) FROM range(1, 6);
INSERT INTO rest_catalog.e2e_lakehouse.mor_pos_partition
SELECT /*+ REPARTITION(1) */ id, 'east=', format_string('row-%02d', id) FROM range(6, 11);

INSERT INTO rest_catalog.e2e_lakehouse.mor_pos_partition
SELECT /*+ REPARTITION(1) */ id, 'west#?', format_string('row-%02d', id) FROM range(11, 16);
INSERT INTO rest_catalog.e2e_lakehouse.mor_pos_partition
SELECT /*+ REPARTITION(1) */ id, 'west#?', format_string('row-%02d', id) FROM range(16, 21);

-- A strict subset of every file, so Iceberg writes delete files instead of rewriting.
DELETE FROM rest_catalog.e2e_lakehouse.mor_pos_partition
WHERE id IN (2, 4, 7, 9, 13, 14, 17, 19);
