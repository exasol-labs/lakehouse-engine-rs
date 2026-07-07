-- Mixed-mechanism fixture: one data file under legacy Parquet positional
-- deletes, another under a v3 Puffin deletion vector -- the realistic
-- v2->v3 migration shape (`add-deletion-vector-application` task 2.E.1 /
-- `packaging/deletion-vector-fixtures`).
--
-- Built by upgrading a single table's format-version mid-fixture:
--   1. Create the table at format-version=2 (merge-on-read).
--   2. Insert data file A (ids 1..=10) and DELETE a subset of it -> Iceberg
--      commits a Parquet positional-delete file referencing file A (v2
--      semantics).
--   3. ALTER TABLE ... SET TBLPROPERTIES to format-version=3. This is a
--      metadata-only, non-destructive upgrade: file A's existing Parquet
--      positional-delete file is untouched.
--   4. Insert data file B (ids 11..=20) and DELETE a subset of it -> the
--      table is now v3, so this DELETE commits a Puffin deletion vector
--      referencing file B instead.
-- The resulting snapshot therefore has ONE data file resolved via a Parquet
-- positional-delete file and ANOTHER resolved via a v3 deletion vector, both
-- visible in the same scan -- exactly what a scan UDF sharding these two
-- files together (or apart) must resolve per file.
--
-- Ground truth (kept in lockstep with
-- crates/lakehouse-engine/tests/common/pos_delete_fixtures.rs -- do not
-- change one without the other):
--   table:       rest_catalog.e2e_lakehouse.mor_mixed
--   file A:      ids 1..=10,  positional delete removes id IN (3, 7)
--   file B:      ids 11..=20, deletion vector removes id IN (13, 17)
--   rows:        20 seeded, 4 deleted (3, 7, 13, 17), 16 remain

CREATE NAMESPACE IF NOT EXISTS rest_catalog.e2e_lakehouse;

DROP TABLE IF EXISTS rest_catalog.e2e_lakehouse.mor_mixed;

CREATE TABLE rest_catalog.e2e_lakehouse.mor_mixed (
  id  BIGINT,
  val STRING
)
USING iceberg
TBLPROPERTIES (
  'format-version'                 = '2',
  'write.delete.mode'              = 'merge-on-read',
  'write.update.mode'              = 'merge-on-read',
  'write.merge.mode'               = 'merge-on-read',
  'write.delete.granularity'       = 'file',
  -- Same rationale as create_file_granularity_fixture.sql: disable the
  -- shuffle so the DELETE's write tasks follow the single-file read-side
  -- task assignment instead of collapsing into one indistinguishable task.
  'write.delete.distribution-mode' = 'none'
);

-- File A: one INSERT -> one Iceberg data file (v2, ids 1..=10).
INSERT INTO rest_catalog.e2e_lakehouse.mor_mixed
SELECT /*+ REPARTITION(1) */ id, val FROM VALUES
  (1, 'row-01'), (2, 'row-02'), (3, 'row-03'), (4, 'row-04'), (5, 'row-05'),
  (6, 'row-06'), (7, 'row-07'), (8, 'row-08'), (9, 'row-09'), (10, 'row-10')
  AS t(id, val);

-- Deletes a strict subset of file A's rows -> Iceberg commits a Parquet
-- positional-delete file referencing file A (still format-version=2).
DELETE FROM rest_catalog.e2e_lakehouse.mor_mixed
WHERE id IN (3, 7);

-- Upgrade to format-version=3: metadata-only, does not rewrite file A's
-- existing Parquet positional-delete file.
ALTER TABLE rest_catalog.e2e_lakehouse.mor_mixed SET TBLPROPERTIES (
  'format-version' = '3'
);

-- File B: one INSERT -> one Iceberg data file (v3, ids 11..=20).
INSERT INTO rest_catalog.e2e_lakehouse.mor_mixed
SELECT /*+ REPARTITION(1) */ id, val FROM VALUES
  (11, 'row-11'), (12, 'row-12'), (13, 'row-13'), (14, 'row-14'), (15, 'row-15'),
  (16, 'row-16'), (17, 'row-17'), (18, 'row-18'), (19, 'row-19'), (20, 'row-20')
  AS t(id, val);

-- Deletes a strict subset of file B's rows -> the table is now v3, so this
-- DELETE commits a Puffin deletion vector referencing file B instead of a
-- Parquet positional-delete file.
DELETE FROM rest_catalog.e2e_lakehouse.mor_mixed
WHERE id IN (13, 17);
