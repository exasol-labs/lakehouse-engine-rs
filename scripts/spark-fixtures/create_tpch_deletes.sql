-- TPC-H merge-on-read position-delete authoring: deletes ~5% of every TPC-H
-- table into a SEPARATE delete-bearing namespace (the baseline stays pristine).
--
-- UPSTREAM TRACKING (apache/iceberg-rust#340): iceberg-rust 0.10 has no
-- position-delete writer, and pyiceberg (deploy/scripts/gen_load.py) is
-- copy-on-write only — its table.delete() rewrites data files, it does not
-- author merge-on-read position-delete files. So Apache Spark — an official
-- Apache Iceberg ecosystem engine — is used instead: a plain `DELETE FROM`
-- against a `write.delete.mode=merge-on-read` (format-version=2) table commits
-- Parquet POSITION deletes (Flink's row-level upsert connectors are the ones
-- that commit EQUALITY deletes instead), which is exactly the delete mechanism
-- this benchmark exercises on read. This is the SAME precedent the E2E fixtures
-- already rely on (scripts/spark-fixtures/create_file_granularity_fixture.sql).
--
-- DROP CONDITION: once #340 lands and iceberg-rust exposes a position-delete
-- writer, replace this Spark authoring step with native Rust delete authoring
-- (matching the E2E seeder's other tables) and delete this file plus its
-- docker (bench/make_deletes_docker.sh) and remote
-- (deploy/scripts/make_deletes_remote.py) callers.
--
-- PARAMETERIZED — this file hardcodes neither the catalog nor the namespaces.
-- Substitute three Spark SQL variables at the caller (spark-sql --define / -d):
--   ${catalog}    the Iceberg catalog: `rest_catalog` in docker mode
--                 (see scripts/spark-fixtures/run_fixtures.sh), `glue` in
--                 remote mode (see deploy/scripts/spark_queries.py).
--   ${source_ns}  the pristine baseline TPC-H namespace (e.g. `tpch`).
--   ${target_ns}  the delete-bearing namespace to author (e.g. `tpch_deletes`).
-- Every table reference below is `${catalog}.${source_ns}.<t>` (read) or
-- `${catalog}.${target_ns}.<t>` (write). Reused by BOTH the docker-mode caller
-- (bench/make_deletes_docker.sh) and the remote EMR caller
-- (deploy/scripts/make_deletes_remote.py); neither mode's specifics leak here.
--
-- DETERMINISTIC-5% CONTRACT: `<surrogate_key> % 20 = 0` deletes ≈5% of rows per
-- table, with NO random seed — the same deleted set on every run, on every
-- machine, so benchmark numbers stay comparable across runs. The surrogate keys
-- are the TPC-H dense integer keys, uniformly distributed mod 20, so the ratio
-- is an accurate ≈5% on the cost-dominant tables (LINEITEM / ORDERS / PARTSUPP).
-- LINEITEM is keyed on L_ORDERKEY (NOT L_LINENUMBER) so its position deletes
-- spread across ALL of LINEITEM's data files, not just one. REGION (5 rows) and
-- NATION (25 rows) get 0-1 imprecise deletions from `% 20`; that is deliberately
-- accepted — those dimensions contribute negligibly to scan/merge cost, so
-- exactness there is pointless (see plan.md's Consequences table).
--
-- IDEMPOTENT BY CALLER CONTRACT: this script does NOT drop the target first
-- (no `DROP TABLE IF EXISTS`, unlike the E2E fixtures) — the CTAS is meant to
-- FAIL LOUDLY if the target namespace/tables already exist, because re-applying
-- the DELETE over an already-deleted target would double-delete and corrupt the
-- 5% contract. The CALLER (make_deletes_docker.sh / make_deletes_remote.py) owns
-- the skip-if-already-populated check; this script assumes a clean target.
--
-- Column/table names are lowercase to match the DuckDB/PyIceberg-authored
-- schema (deploy/scripts/gen_load.py's TPCH_TABLES + DuckDB's lowercase TPC-H
-- columns), NOT the uppercase Exasol-facing names used in bench/run.sh's SQL.

CREATE NAMESPACE IF NOT EXISTS ${catalog}.${target_ns};

-- ---- region (surrogate: r_regionkey) ----
CREATE TABLE ${catalog}.${target_ns}.region
USING iceberg
TBLPROPERTIES (
  'format-version'    = '2',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode'  = 'merge-on-read'
)
AS SELECT * FROM ${catalog}.${source_ns}.region;

DELETE FROM ${catalog}.${target_ns}.region WHERE r_regionkey % 20 = 0;

-- ---- nation (surrogate: n_nationkey) ----
CREATE TABLE ${catalog}.${target_ns}.nation
USING iceberg
TBLPROPERTIES (
  'format-version'    = '2',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode'  = 'merge-on-read'
)
AS SELECT * FROM ${catalog}.${source_ns}.nation;

DELETE FROM ${catalog}.${target_ns}.nation WHERE n_nationkey % 20 = 0;

-- ---- supplier (surrogate: s_suppkey) ----
CREATE TABLE ${catalog}.${target_ns}.supplier
USING iceberg
TBLPROPERTIES (
  'format-version'    = '2',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode'  = 'merge-on-read'
)
AS SELECT * FROM ${catalog}.${source_ns}.supplier;

DELETE FROM ${catalog}.${target_ns}.supplier WHERE s_suppkey % 20 = 0;

-- ---- customer (surrogate: c_custkey) ----
CREATE TABLE ${catalog}.${target_ns}.customer
USING iceberg
TBLPROPERTIES (
  'format-version'    = '2',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode'  = 'merge-on-read'
)
AS SELECT * FROM ${catalog}.${source_ns}.customer;

DELETE FROM ${catalog}.${target_ns}.customer WHERE c_custkey % 20 = 0;

-- ---- part (surrogate: p_partkey) ----
CREATE TABLE ${catalog}.${target_ns}.part
USING iceberg
TBLPROPERTIES (
  'format-version'    = '2',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode'  = 'merge-on-read'
)
AS SELECT * FROM ${catalog}.${source_ns}.part;

DELETE FROM ${catalog}.${target_ns}.part WHERE p_partkey % 20 = 0;

-- ---- partsupp (surrogate: ps_partkey) ----
-- partsupp has no single-column PK; keying on ps_partkey deletes all supplier
-- rows for ~5% of parts, which is a uniform ≈5% of partsupp rows (4 suppliers
-- per part). This is the plan's chosen surrogate.
CREATE TABLE ${catalog}.${target_ns}.partsupp
USING iceberg
TBLPROPERTIES (
  'format-version'    = '2',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode'  = 'merge-on-read'
)
AS SELECT * FROM ${catalog}.${source_ns}.partsupp;

DELETE FROM ${catalog}.${target_ns}.partsupp WHERE ps_partkey % 20 = 0;

-- ---- orders (surrogate: o_orderkey) ----
CREATE TABLE ${catalog}.${target_ns}.orders
USING iceberg
TBLPROPERTIES (
  'format-version'    = '2',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode'  = 'merge-on-read'
)
AS SELECT * FROM ${catalog}.${source_ns}.orders;

DELETE FROM ${catalog}.${target_ns}.orders WHERE o_orderkey % 20 = 0;

-- ---- lineitem (surrogate: l_orderkey) ----
-- Keyed on l_orderkey (NOT l_linenumber): l_orderkey spreads the position
-- deletes across all of LINEITEM's data files, exercising the read path at
-- scale rather than concentrating deletes in one file.
CREATE TABLE ${catalog}.${target_ns}.lineitem
USING iceberg
TBLPROPERTIES (
  'format-version'    = '2',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode'  = 'merge-on-read'
)
AS SELECT * FROM ${catalog}.${source_ns}.lineitem;

DELETE FROM ${catalog}.${target_ns}.lineitem WHERE l_orderkey % 20 = 0;
