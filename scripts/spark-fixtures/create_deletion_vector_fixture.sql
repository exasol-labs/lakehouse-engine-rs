-- Positive deletion-vector fixture: Iceberg format-version=3 Puffin
-- deletion vector (`deletion-vector-v1`).
--
-- Originally authored (PR #72) as a fail-loud-only fixture, before this
-- engine could apply v3 deletion vectors on read. `add-deletion-vector-
-- application` task 2.E.1 repurposes it into a POSITIVE fixture: the engine
-- now decodes the Puffin `deletion-vector-v1` blob and applies it (see
-- `datafusion-scan/scan-execution-deletion-vectors`), so this table is
-- expected to return its post-delete rows through the VS, exactly like the
-- two positional-delete fixtures (`create_file_granularity_fixture.sql` /
-- `create_partition_granularity_fixture.sql`).
--
-- `format-version` = '3' makes Iceberg 1.10.1 write a Puffin deletion vector
-- instead of a Parquet positional-delete file for a
-- `write.delete.mode=merge-on-read` DELETE (v3 DVs became GA in Apache
-- Iceberg 1.10.0, Spark is the reference writer) -- see
-- `e2e_dv_returns_post_delete_rows` in tests/e2e_deletion_vectors_test.rs.
--
-- Ground truth (kept in lockstep with
-- crates/lakehouse-engine/tests/common/pos_delete_fixtures.rs -- do not
-- change one without the other):
--   table:   rest_catalog.e2e_lakehouse.mor_dv
--   rows:    10 (id 1..=10), written as ONE data file
--   deleted: id IN (3, 7)  -- a strict subset, so Iceberg commits a Puffin
--            deletion vector referencing the data file rather than rewriting
--            or dropping it
--   remain:  8 rows -- ids 1,2,4,5,6,8,9,10

CREATE NAMESPACE IF NOT EXISTS rest_catalog.e2e_lakehouse;

DROP TABLE IF EXISTS rest_catalog.e2e_lakehouse.mor_dv;

CREATE TABLE rest_catalog.e2e_lakehouse.mor_dv (
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

-- Single INSERT -> one Iceberg data file. The REPARTITION(1) hint is required:
-- under `local[*]`, a bare `INSERT ... VALUES` fans out across every core
-- instead of writing one file per statement (see the sibling fixtures'
-- comments for the observed effect).
INSERT INTO rest_catalog.e2e_lakehouse.mor_dv
SELECT /*+ REPARTITION(1) */ id, val FROM VALUES
  (1, 'row-01'), (2, 'row-02'), (3, 'row-03'), (4, 'row-04'), (5, 'row-05'),
  (6, 'row-06'), (7, 'row-07'), (8, 'row-08'), (9, 'row-09'), (10, 'row-10')
  AS t(id, val);

-- Deletes a strict subset of the single data file's rows (never the whole
-- file), so a format-version=3 merge-on-read table commits a Puffin deletion
-- vector referencing that data file rather than rewriting or dropping it.
DELETE FROM rest_catalog.e2e_lakehouse.mor_dv
WHERE id IN (3, 7);
