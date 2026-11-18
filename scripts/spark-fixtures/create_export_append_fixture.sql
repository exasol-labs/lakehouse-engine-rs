-- Iceberg tables that another engine created, for the export's append into an existing table.
-- The append test resets each table's `main` branch to the snapshot written here before it
-- appends, because no test can rerun Spark.
--
-- Ground truth, in lockstep with crates/lakehouse-e2e/tests/common/export_append_fixtures.rs.
--
--   appendable_v2  format version 2, identity(day) dropped after the rows: spec ID 1, no field
--   appendable_v1  the same columns and rows at format version 1, identity(day) as day_part added
--     and dropped after the rows: the dropped field becomes void, spec ID 2
--     (id BIGINT NOT NULL, qty INT, amount DECIMAL(20,2), name STRING NOT NULL, day DATE,
--      ts TIMESTAMP_NTZ, created TIMESTAMP (Iceberg timestamptz), flag BOOLEAN, score DOUBLE)
--     id 1  every column set
--     id 2  every column set, negative numbers
--     id 3  every optional column NULL
--   odd_types      (id BIGINT NOT NULL, ratio FLOAT, note STRING), id 1
--   partitioned    format version 2, identity(day), (id BIGINT, day DATE), id 1
--   v3             format version 3, (id BIGINT), id 1
--   v1_void_column format version 1, identity(day) dropped after the rows: a void field named day,
--     (id BIGINT, day DATE), id 1
--
-- Every timestamp is exact to the millisecond, so it reads the same at every precision. The
-- `created` literals carry their UTC offset, so the session time zone does not shift them.

CREATE NAMESPACE IF NOT EXISTS rest_catalog.e2e_export_append;

-- appendable_v2

DROP TABLE IF EXISTS rest_catalog.e2e_export_append.appendable_v2;

CREATE TABLE rest_catalog.e2e_export_append.appendable_v2 (
  id      BIGINT NOT NULL,
  qty     INT,
  amount  DECIMAL(20,2),
  name    STRING NOT NULL,
  day     DATE,
  ts      TIMESTAMP_NTZ,
  created TIMESTAMP,
  flag    BOOLEAN,
  score   DOUBLE
)
USING iceberg
PARTITIONED BY (day)
TBLPROPERTIES ('format-version' = '2');

INSERT INTO rest_catalog.e2e_export_append.appendable_v2 VALUES
  (1, 7, 12.50, 'alpha', DATE '2024-01-15', TIMESTAMP_NTZ '2024-01-15 10:30:00.123',
   TIMESTAMP '2024-01-15 10:30:00.123+00:00', true, 1.5),
  (2, -3, -99999.99, 'beta', DATE '2024-02-20', TIMESTAMP_NTZ '2024-02-20 23:59:59.999',
   TIMESTAMP '2024-02-20 00:00:00.001+00:00', false, -0.25),
  (3, CAST(NULL AS INT), CAST(NULL AS DECIMAL(20,2)), 'gamma', CAST(NULL AS DATE),
   CAST(NULL AS TIMESTAMP_NTZ), CAST(NULL AS TIMESTAMP), CAST(NULL AS BOOLEAN),
   CAST(NULL AS DOUBLE));

ALTER TABLE rest_catalog.e2e_export_append.appendable_v2 DROP PARTITION FIELD day;

-- appendable_v1

DROP TABLE IF EXISTS rest_catalog.e2e_export_append.appendable_v1;

CREATE TABLE rest_catalog.e2e_export_append.appendable_v1 (
  id      BIGINT NOT NULL,
  qty     INT,
  amount  DECIMAL(20,2),
  name    STRING NOT NULL,
  day     DATE,
  ts      TIMESTAMP_NTZ,
  created TIMESTAMP,
  flag    BOOLEAN,
  score   DOUBLE
)
USING iceberg
TBLPROPERTIES ('format-version' = '1');

-- A partition field named unlike its column: the export refuses a void field named like a column.
ALTER TABLE rest_catalog.e2e_export_append.appendable_v1 ADD PARTITION FIELD day AS day_part;

INSERT INTO rest_catalog.e2e_export_append.appendable_v1
SELECT id, qty, amount, name, day, ts, created, flag, score
FROM rest_catalog.e2e_export_append.appendable_v2;

-- Format version 1 keeps a dropped partition field with the void transform.
ALTER TABLE rest_catalog.e2e_export_append.appendable_v1 DROP PARTITION FIELD day_part;

-- odd_types: no Exasol type fills a float column.

DROP TABLE IF EXISTS rest_catalog.e2e_export_append.odd_types;

CREATE TABLE rest_catalog.e2e_export_append.odd_types (
  id    BIGINT NOT NULL,
  ratio FLOAT,
  note  STRING
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

INSERT INTO rest_catalog.e2e_export_append.odd_types VALUES (1, CAST(0.5 AS FLOAT), 'one');

-- partitioned

DROP TABLE IF EXISTS rest_catalog.e2e_export_append.partitioned;

CREATE TABLE rest_catalog.e2e_export_append.partitioned (id BIGINT, day DATE)
USING iceberg
PARTITIONED BY (day)
TBLPROPERTIES ('format-version' = '2');

INSERT INTO rest_catalog.e2e_export_append.partitioned VALUES (1, DATE '2024-01-15');

-- v3

DROP TABLE IF EXISTS rest_catalog.e2e_export_append.v3;

CREATE TABLE rest_catalog.e2e_export_append.v3 (id BIGINT)
USING iceberg
TBLPROPERTIES ('format-version' = '3');

INSERT INTO rest_catalog.e2e_export_append.v3 VALUES (1);

-- v1_void_column: Spark keeps the dropped field as void under the column's name.

DROP TABLE IF EXISTS rest_catalog.e2e_export_append.v1_void_column;

CREATE TABLE rest_catalog.e2e_export_append.v1_void_column (id BIGINT, day DATE)
USING iceberg
PARTITIONED BY (day)
TBLPROPERTIES ('format-version' = '1');

INSERT INTO rest_catalog.e2e_export_append.v1_void_column VALUES (1, DATE '2024-01-15');

ALTER TABLE rest_catalog.e2e_export_append.v1_void_column DROP PARTITION FIELD day;
