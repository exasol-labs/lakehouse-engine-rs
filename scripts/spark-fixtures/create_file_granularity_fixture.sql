-- Positional-delete fixture: write.delete.granularity=file.
--
-- UPSTREAM TRACKING (apache/iceberg-rust#340): iceberg-rust 0.10 has no
-- position-delete writer, so this fixture is authored by Apache Spark (an
-- official Apache Iceberg ecosystem engine) instead of the Rust seeder in
-- tests/common/seed.rs. Plain `DELETE FROM` against a
-- `write.delete.mode=merge-on-read` table writes Parquet POSITION deletes
-- (Flink's row-level upsert connectors are the ones that write EQUALITY
-- deletes instead), which is exactly the mechanism this feature applies.
-- DROP CONDITION: once #340 lands and iceberg-rust exposes a position-delete
-- writer, replace this fixture with native Rust fixture authoring in
-- tests/common/seed.rs (matching its other seed tables) and delete this file,
-- its partition-granularity sibling, run_fixtures.sh, and the
-- spark-iceberg-fixtures docker-compose service.
--
-- Ground truth (kept in lockstep with
-- crates/lakehouse-engine/tests/common/pos_delete_fixtures.rs — do not change
-- one without the other):
--   table:   rest_catalog.e2e_lakehouse.mor_pos_file
--   rows:    20 (id 1..=20), written as TWO data files (ids 1..=10, 11..=20)
--   deleted: id IN (3, 8, 13, 17)   -- two ids from each of the two data files
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
  -- Default delete distribution mode is 'hash': Spark shuffles matching rows
  -- before writing position deletes, and with `spark.sql.shuffle.partitions`
  -- pinned to 1 (see run_fixtures.sh) that shuffle collapses BOTH data
  -- files' matching rows into indistinguishable Spark write tasks, so the
  -- committed delete files end up referencing BOTH data files instead of
  -- one each. 'none' disables that shuffle: the DELETE's write tasks then
  -- follow the natural read-side task assignment (one task per small data
  -- file, since each file was written as its own single-file INSERT below),
  -- so `write.delete.granularity=file` gets exactly the per-data-file split
  -- it's meant to produce. This is the DELETE-compatible equivalent of the
  -- INSERTs' `/*+ REPARTITION(1) */` hint below (`DELETE FROM` has no hint
  -- clause in Spark's SQL grammar), applied for the identical reason.
  'write.delete.distribution-mode' = 'none'
);

-- Two separate INSERTs -> two Iceberg data files, so "file" granularity (one
-- Parquet positional-delete file per AFFECTED data file) is genuinely
-- exercised rather than trivially satisfied by a single-file table. The
-- REPARTITION(1) hint is required: under `local[*]`, a bare `INSERT ...
-- VALUES` fans out across every core (observed: 4 files for 10 rows on an
-- 4-core runner) instead of writing one file per statement.
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

-- Deletes a strict subset of each data file's rows (never a whole file), so
-- Iceberg commits Parquet positional-delete files rather than rewriting or
-- dropping a data file outright.
DELETE FROM rest_catalog.e2e_lakehouse.mor_pos_file
WHERE id IN (3, 8, 13, 17);
