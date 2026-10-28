-- Iceberg tables whose data files a metadata-only Hive import (`add_files`) registered
-- WITHOUT the identity-partition source column. The manifest `partition` struct carries the
-- value instead, and the Iceberg spec's "Column Projection" rule (1) returns it.
--
-- Spark's native Parquet writer lays the source out Hive-style (`region=eu/part-*.parquet`,
-- the file holds no `region`) and `add_files` registers it unchanged. The paths use s3://
-- because add_files records the writer's scheme and the scan UDF registers its object
-- store under s3://. Each fixture runs in its own spark-sql process, so no SET leaks.
--
-- Ground truth, in lockstep with crates/lakehouse-e2e/tests/common/identity_partition_fixtures.rs.
--
--   ident_part_absent    (id INT, amount INT, region STRING), identity(region), then the
--                        field is dropped (spec id 1, unpartitioned)
--     id 1,2  imported  region=eu
--     id 3,4  imported  region=us
--     id 7    imported  region=NULL (Hive default partition)
--     id 5    native    apac
--     id 6    native    eu
--     id 8    imported under spec id 1, no region anywhere, reads NULL
--     id 9    imported under region=eu from a file that holds region without field ids
--             (add_files sets schema.name-mapping.default, which maps it): stored us, reads eu
--     id 10   the same file: stored NULL, reads eu
--   ident_part_types     (id INT, b, i, l, f, d, dec_small, dec_large, dt, s), identity on
--                        every column but id
--     id 1  imported, all partition values set
--     id 2  native, the same values as id 1
--     id 3  imported, every partition value NULL

CREATE NAMESPACE IF NOT EXISTS rest_catalog.e2e_lakehouse;

-- ident_part_absent

DROP TABLE IF EXISTS rest_catalog.e2e_lakehouse.ident_part_absent;
DROP TABLE IF EXISTS spark_catalog.default.ident_part_absent_source;

CREATE TABLE rest_catalog.e2e_lakehouse.ident_part_absent (id INT, amount INT, region STRING)
USING iceberg
PARTITIONED BY (region)
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE spark_catalog.default.ident_part_absent_source (id INT, amount INT, region STRING)
USING parquet
PARTITIONED BY (region)
LOCATION 's3://warehouse/e2e_lakehouse/ident_part_absent_source';

-- OVERWRITE empties the external table's directory, so a rerun starts from these rows only.
INSERT OVERWRITE spark_catalog.default.ident_part_absent_source
VALUES (1, 10, 'eu'), (2, 20, 'eu'), (3, 30, 'us'), (4, 40, 'us'), (7, 70, CAST(NULL AS STRING));

CALL rest_catalog.system.add_files(
  table => 'e2e_lakehouse.ident_part_absent',
  source_table => '`parquet`.`s3://warehouse/e2e_lakehouse/ident_part_absent_source`'
);

-- This file holds region with other values than its partition value, so a reader that lets name
-- mapping bind the column ahead of rule (1) returns them instead of eu.
INSERT OVERWRITE DIRECTORY 's3://warehouse/e2e_lakehouse/ident_part_absent_held/region=eu'
USING parquet
SELECT /*+ REPARTITION(1) */ id, amount, region
FROM VALUES (9, 90, 'us'), (10, 100, CAST(NULL AS STRING)) AS t(id, amount, region);

CALL rest_catalog.system.add_files(
  table => 'e2e_lakehouse.ident_part_absent',
  source_table => '`parquet`.`s3://warehouse/e2e_lakehouse/ident_part_absent_held`'
);

INSERT INTO rest_catalog.e2e_lakehouse.ident_part_absent VALUES (5, 50, 'apac'), (6, 60, 'eu');

ALTER TABLE rest_catalog.e2e_lakehouse.ident_part_absent DROP PARTITION FIELD region;

INSERT OVERWRITE DIRECTORY 's3://warehouse/e2e_lakehouse/ident_part_absent_unpartitioned_source'
USING parquet
SELECT /*+ REPARTITION(1) */ id, amount
FROM VALUES (8, 80) AS t(id, amount);

CALL rest_catalog.system.add_files(
  table => 'e2e_lakehouse.ident_part_absent',
  source_table => '`parquet`.`s3://warehouse/e2e_lakehouse/ident_part_absent_unpartitioned_source`'
);

-- ident_part_types
-- A float that is not exact in binary (0.1) tells a float widened from its stored value apart
-- from one parsed from its text. dec_large lies outside Exasol's catalog-decimal domain, and
-- carries more integral digits than a double holds.

DROP TABLE IF EXISTS rest_catalog.e2e_lakehouse.ident_part_types;
DROP TABLE IF EXISTS spark_catalog.default.ident_part_types_source;

CREATE TABLE rest_catalog.e2e_lakehouse.ident_part_types (
  id INT,
  b BOOLEAN,
  i INT,
  l BIGINT,
  f FLOAT,
  d DOUBLE,
  dec_small DECIMAL(10,2),
  dec_large DECIMAL(38,6),
  dt DATE,
  s STRING
)
USING iceberg
PARTITIONED BY (b, i, l, f, d, dec_small, dec_large, dt, s)
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE spark_catalog.default.ident_part_types_source (
  id INT,
  b BOOLEAN,
  i INT,
  l BIGINT,
  f FLOAT,
  d DOUBLE,
  dec_small DECIMAL(10,2),
  dec_large DECIMAL(38,6),
  dt DATE,
  s STRING
)
USING parquet
PARTITIONED BY (b, i, l, f, d, dec_small, dec_large, dt, s)
LOCATION 's3://warehouse/e2e_lakehouse/ident_part_types_source';

INSERT OVERWRITE spark_catalog.default.ident_part_types_source
SELECT id, b, i, l, f, d, dec_small, dec_large, dt, s
FROM (
  SELECT 1 AS id,
         true AS b,
         -2147483648 AS i,
         CAST(9007199254740993 AS BIGINT) AS l,
         CAST(0.1 AS FLOAT) AS f,
         CAST(1234.5678 AS DOUBLE) AS d,
         CAST(12345678.90 AS DECIMAL(10,2)) AS dec_small,
         CAST(1234567890123456789012345678.123456 AS DECIMAL(38,6)) AS dec_large,
         CAST('2024-02-29' AS DATE) AS dt,
         'north' AS s
  UNION ALL
  SELECT 3,
         CAST(NULL AS BOOLEAN),
         CAST(NULL AS INT),
         CAST(NULL AS BIGINT),
         CAST(NULL AS FLOAT),
         CAST(NULL AS DOUBLE),
         CAST(NULL AS DECIMAL(10,2)),
         CAST(NULL AS DECIMAL(38,6)),
         CAST(NULL AS DATE),
         CAST(NULL AS STRING)
);

CALL rest_catalog.system.add_files(
  table => 'e2e_lakehouse.ident_part_types',
  source_table => '`parquet`.`s3://warehouse/e2e_lakehouse/ident_part_types_source`'
);

INSERT INTO rest_catalog.e2e_lakehouse.ident_part_types
SELECT 2,
       true,
       -2147483648,
       CAST(9007199254740993 AS BIGINT),
       CAST(0.1 AS FLOAT),
       CAST(1234.5678 AS DOUBLE),
       CAST(12345678.90 AS DECIMAL(10,2)),
       CAST(1234567890123456789012345678.123456 AS DECIMAL(38,6)),
       CAST('2024-02-29' AS DATE),
       'north';
