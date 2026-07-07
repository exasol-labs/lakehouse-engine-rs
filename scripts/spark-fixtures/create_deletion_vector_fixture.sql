-- Unsupported-delete fixture: Iceberg format-version=3 Puffin deletion vector.
--
-- Task 1.3 (adapter/pushdown.rs) fails loud at plan time on any delete
-- mechanism this engine cannot apply -- equality deletes, ORC/Avro, AND v3
-- Puffin deletion vectors. Group C's other two fixtures
-- (create_file_granularity_fixture.sql / create_partition_granularity_fixture.sql)
-- only exercise the SUPPORTED mechanism (Parquet positional deletes, format-
-- version=2). This fixture exercises the REJECTED path: `format-version` =
-- '3' makes Iceberg 1.10.1 write deletion vectors instead of Parquet
-- positional-delete files for a `write.delete.mode=merge-on-read` DELETE (V3
-- DVs became GA in Apache Iceberg 1.10.0, Spark is the reference writer) --
-- see e2e_unsupported_delete_fails_loud in
-- tests/e2e_positional_deletes_test.rs.
--
-- Equality deletes still cannot be produced by this stack (only Flink's
-- row-level upsert connectors write them; Flink is not part of this stack --
-- see run_fixtures.sh's header), so this fixture covers the DeletionVector
-- arm of `UnsupportedDeleteMechanism`, not the EqualityDelete arm. The two
-- arms share the same plan-time gate (`classify_manifest_file` in
-- adapter/pushdown.rs), so exercising one of them for real is sufficient to
-- prove the gate fires end-to-end; unit tests already cover the
-- EqualityDelete arm's classification logic directly.
--
-- Ground truth (kept in lockstep with
-- crates/lakehouse-engine/tests/common/pos_delete_fixtures.rs -- do not
-- change one without the other):
--   table:   rest_catalog.e2e_lakehouse.mor_dv_unsupported
--   rows:    10 (id 1..=10), written as ONE data file
--   deleted: id IN (3, 7)  -- a strict subset, so Iceberg commits a Puffin
--            deletion vector referencing the data file rather than rewriting
--            or dropping it
--
-- This table is NEVER expected to return rows through the VS -- the engine
-- must fail loud naming "Iceberg v3 Puffin deletion vectors" (or
-- "deletion vector" / "puffin") before building any scan plan. No
-- post-delete row ground truth is tracked (unlike the two positional-delete
-- fixtures) because a successful read is not the point of this fixture.
--
-- UPSTREAM TRACKING (apache/iceberg-rust#2681, #2580, #2411): once
-- iceberg-rust gains v3 deletion-vector READ support, this fixture becomes
-- READABLE (not just rejected) -- at that point `e2e_unsupported_delete_fails_loud`
-- should be re-pointed at a genuinely unsupported mechanism (or retired) and
-- a new positive-path DV test should be added instead.

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

-- Single INSERT -> one Iceberg data file. The REPARTITION(1) hint is required:
-- under `local[*]`, a bare `INSERT ... VALUES` fans out across every core
-- instead of writing one file per statement (see the sibling fixtures'
-- comments for the observed effect).
INSERT INTO rest_catalog.e2e_lakehouse.mor_dv_unsupported
SELECT /*+ REPARTITION(1) */ id, val FROM VALUES
  (1, 'row-01'), (2, 'row-02'), (3, 'row-03'), (4, 'row-04'), (5, 'row-05'),
  (6, 'row-06'), (7, 'row-07'), (8, 'row-08'), (9, 'row-09'), (10, 'row-10')
  AS t(id, val);

-- Deletes a strict subset of the single data file's rows (never the whole
-- file), so a format-version=3 merge-on-read table commits a Puffin deletion
-- vector referencing that data file rather than rewriting or dropping it.
DELETE FROM rest_catalog.e2e_lakehouse.mor_dv_unsupported
WHERE id IN (3, 7);
