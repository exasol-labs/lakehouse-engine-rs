-- Positional-delete fixture: write.delete.granularity=partition.
--
-- UPSTREAM TRACKING (apache/iceberg-rust#340): see
-- create_file_granularity_fixture.sql's header for the full rationale and
-- drop condition — identical here, this is its partition-granularity sibling.
--
-- Ground truth (kept in lockstep with
-- crates/lakehouse-engine/tests/common/pos_delete_fixtures.rs — do not change
-- one without the other):
--   table:      rest_catalog.e2e_lakehouse.mor_pos_partition
--   partitions: region IN ('east', 'west'), TWO data files each (4 total)
--     east: file 1 = id 1..=5,  file 2 = id 6..=10
--     west: file 3 = id 11..=15, file 4 = id 16..=20
--   rows:       20
--   deleted:    id IN (2, 4, 7, 9, 13, 14, 17, 19)  -- 2 ids per data file
--   remain:     12 rows
--
-- write.delete.granularity=partition makes Iceberg write ONE positional-
-- delete file PER PARTITION (not per data file): the "east" delete file
-- references BOTH east data files, and the "west" delete file references
-- BOTH west data files. So the two committed delete files collectively span
-- multiple partitions, while each individually spans multiple data files
-- within its own partition — the scenario `write.delete.granularity=file`
-- above cannot exercise.

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

-- "east" partition: two separate INSERTs -> two data files in the SAME
-- partition value. The REPARTITION(1) hint is required: under `local[*]`, a
-- bare `INSERT ... VALUES` fans out across every core (observed: 4 files for
-- 5 rows on a 4-core runner) instead of writing one file per statement.
INSERT INTO rest_catalog.e2e_lakehouse.mor_pos_partition
SELECT /*+ REPARTITION(1) */ id, region, val FROM VALUES
  (1, 'east', 'row-01'), (2, 'east', 'row-02'), (3, 'east', 'row-03'), (4, 'east', 'row-04'), (5, 'east', 'row-05')
  AS t(id, region, val);
INSERT INTO rest_catalog.e2e_lakehouse.mor_pos_partition
SELECT /*+ REPARTITION(1) */ id, region, val FROM VALUES
  (6, 'east', 'row-06'), (7, 'east', 'row-07'), (8, 'east', 'row-08'), (9, 'east', 'row-09'), (10, 'east', 'row-10')
  AS t(id, region, val);

-- "west" partition: two separate INSERTs -> two data files in the SAME
-- partition value.
INSERT INTO rest_catalog.e2e_lakehouse.mor_pos_partition
SELECT /*+ REPARTITION(1) */ id, region, val FROM VALUES
  (11, 'west', 'row-11'), (12, 'west', 'row-12'), (13, 'west', 'row-13'), (14, 'west', 'row-14'), (15, 'west', 'row-15')
  AS t(id, region, val);
INSERT INTO rest_catalog.e2e_lakehouse.mor_pos_partition
SELECT /*+ REPARTITION(1) */ id, region, val FROM VALUES
  (16, 'west', 'row-16'), (17, 'west', 'row-17'), (18, 'west', 'row-18'), (19, 'west', 'row-19'), (20, 'west', 'row-20')
  AS t(id, region, val);

-- ONE DELETE whose affected rows span BOTH partitions and BOTH data files
-- within each partition -> two partition-scoped positional-delete files,
-- each referencing two data files. Every data file loses a strict subset of
-- its rows (never the whole file), so Iceberg commits delete files rather
-- than rewriting/dropping a data file.
DELETE FROM rest_catalog.e2e_lakehouse.mor_pos_partition
WHERE id IN (2, 4, 7, 9, 13, 14, 17, 19);
