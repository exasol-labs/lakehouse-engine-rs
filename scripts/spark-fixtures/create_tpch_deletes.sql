-- Authors merge-on-read position deletes (~5% per table) into a separate
-- namespace; Spark is used because iceberg-rust has no position-delete writer
-- and pyiceberg is copy-on-write only (apache/iceberg-rust#340).
--
-- Callers pass -d catalog=, source_ns=, target_ns=.
--
-- `<key> % 20 = 0` is deterministic so benchmark runs stay comparable.
-- No DROP TABLE: the CTAS must fail if the target exists, since re-applying the
-- DELETE would double-delete; callers own the skip-if-populated check.
--
-- Names are lowercase to match the DuckDB/PyIceberg-authored schema.

CREATE NAMESPACE IF NOT EXISTS ${catalog}.${target_ns};

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

-- l_orderkey (not l_linenumber) spreads deletes across all data files.
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
