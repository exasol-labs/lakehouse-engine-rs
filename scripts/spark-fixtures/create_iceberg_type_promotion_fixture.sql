-- Iceberg type promotion (#349) is metadata-only, so pre-promotion data files
-- keep their source Parquet types and the scan must cast them up per file. The
-- pre-promotion INSERT is load-bearing: e2e_type_relaxation_test asserts that
-- file's physical types are still the source types, so an Iceberg-side rewrite
-- fails loudly instead of passing vacuously.
--
-- The decimal widens precision only: the Iceberg spec's promotion table reads
-- "Widen precision only". No date -> timestamp table: Iceberg Java's
-- TypeUtil.isPromotionAllowed rejects it, so Spark cannot author it.
--
-- Pre-promotion values make a wrong-width read return a wrong number;
-- post-promotion values do not fit the narrow source types at all. float
-- pre-promotion values are exact in binary32. Literals are CAST so the inserted
-- type is the declared column type, not Spark's inference.
--
-- Ground truth, in lockstep with crates/lakehouse-engine/tests/common/type_promotion_fixtures.rs:
--   table:   rest_catalog.e2e_lakehouse.iceberg_type_promotion
--            format-version 2
--   columns: id              BIGINT         -- Iceberg `long`, never promoted
--            int_long        INT           -> BIGINT
--            float_double    FLOAT         -> DOUBLE
--            decimal_decimal DECIMAL(10,2) -> DECIMAL(20,2)
--   schemas: 4 in the metadata's schema history, one per statement below --
--            schema 0 is the source types, schema 3 is current; the field
--            ids (1..4) are stable across all four
--   rows:    4, as exactly TWO data files -- ids 1,2 written BEFORE the
--            promotions, ids 3,4 written AFTER
--     id | int_long    | float_double       | decimal_decimal
--      1 |  2147483647 |                3.5 |             12345678.90
--      2 | -2147483648 |              -1.25 |            -12345678.90
--      3 |  2147483648 |  1.234567890123457 |   123456789012345678.90
--      4 | -2147483649 | -9.876543210987654 |  -123456789012345678.90
--   files:   the ids 1,2 file is physically int / float / decimal(10,2); the
--            ids 3,4 file is physically bigint / double / decimal(20,2)

CREATE NAMESPACE IF NOT EXISTS rest_catalog.e2e_lakehouse;

DROP TABLE IF EXISTS rest_catalog.e2e_lakehouse.iceberg_type_promotion;

CREATE TABLE rest_catalog.e2e_lakehouse.iceberg_type_promotion (
  id              BIGINT,
  int_long        INT,
  float_double    FLOAT,
  decimal_decimal DECIMAL(10,2)
)
USING iceberg
TBLPROPERTIES (
  'format-version' = '2'
);

-- REPARTITION(1): under `local[*]` a bare INSERT ... VALUES writes one file per core.
INSERT INTO rest_catalog.e2e_lakehouse.iceberg_type_promotion
SELECT /*+ REPARTITION(1) */ id, int_long, float_double, decimal_decimal
FROM VALUES
  (CAST(1 AS BIGINT), CAST(2147483647 AS INT), CAST(3.5 AS FLOAT),
   CAST(12345678.90 AS DECIMAL(10,2))),
  (CAST(2 AS BIGINT), CAST(-2147483648 AS INT), CAST(-1.25 AS FLOAT),
   CAST(-12345678.90 AS DECIMAL(10,2)))
  AS t(id, int_long, float_double, decimal_decimal);

ALTER TABLE rest_catalog.e2e_lakehouse.iceberg_type_promotion
  ALTER COLUMN int_long TYPE BIGINT;

ALTER TABLE rest_catalog.e2e_lakehouse.iceberg_type_promotion
  ALTER COLUMN float_double TYPE DOUBLE;

ALTER TABLE rest_catalog.e2e_lakehouse.iceberg_type_promotion
  ALTER COLUMN decimal_decimal TYPE DECIMAL(20,2);

INSERT INTO rest_catalog.e2e_lakehouse.iceberg_type_promotion
SELECT /*+ REPARTITION(1) */ id, int_long, float_double, decimal_decimal
FROM VALUES
  (CAST(3 AS BIGINT), CAST(2147483648 AS BIGINT),
   CAST(1.234567890123457 AS DOUBLE),
   CAST(123456789012345678.90 AS DECIMAL(20,2))),
  (CAST(4 AS BIGINT), CAST(-2147483649 AS BIGINT),
   CAST(-9.876543210987654 AS DOUBLE),
   CAST(-123456789012345678.90 AS DECIMAL(20,2)))
  AS t(id, int_long, float_double, decimal_decimal);
