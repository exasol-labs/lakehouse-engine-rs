-- Iceberg type-promotion fixture (issue #349).
--
-- WHY THIS FIXTURE EXISTS: Iceberg promotes a column's type in METADATA only.
-- Data files written before the promotion keep their SOURCE physical Parquet
-- encoding, while the table's current schema names the TARGET type, so a scan
-- of a promoted table reads two physical layouts of the same column in one
-- query and must cast the older file up per file. This table is that shape
-- end-to-end: it is written to BEFORE the three promotions and again AFTER
-- them, so both layouts are live in the same table.
--
-- WHY THE PRE-PROMOTION INSERT IS THE LOAD-BEARING STEP: a table promoted
-- before any write -- or one whose files were all rewritten afterwards --
-- carries only target-type data files. It would pass the read test without
-- ever exercising the cast. The fixture-shape test in
-- tests/e2e_type_relaxation_test.rs asserts the committed pre-promotion data
-- file's physical Parquet types are still the SOURCE types, so an
-- Iceberg-side rewrite fails the suite loudly rather than making the read
-- test pass vacuously.
--
-- WHY format-version 2: the Iceberg spec's promotion table permits
-- int -> long, float -> double, and decimal(P,S) -> decimal(P',S) at v1 and
-- v2, so this fixture uses the same format version every other Iceberg
-- fixture here uses and stays about promotion rather than about v3.
--
-- WHY THE DECIMAL WIDENS PRECISION ONLY: the spec's Requirements cell for the
-- decimal row reads "Widen precision only", with the scale symbol unchanged
-- on both sides and P' > P strict. Scale 2 therefore holds across the
-- promotion; Delta's scale-growing rule is a Delta-only widening.
--
-- WHY THERE IS NO date -> timestamp TABLE HERE: Apache Iceberg's Java API
-- implements NO `date` promotion at any released version --
-- TypeUtil.isPromotionAllowed covers int -> long, float -> double, and
-- decimal precision only, on 1.10.1, 1.11.0, and main alike -- so
-- `ALTER TABLE ... ALTER COLUMN ... TYPE TIMESTAMP_NTZ` fails with
-- "Cannot change column type: date -> timestamp" and no Spark-authored
-- fixture can carry that promotion. Producing it needs a raw metadata
-- schema replacement (add-schema + set-current-schema), which is not Spark
-- SQL and so is not this script's to author.
--
-- WHY THESE VALUES: every pre-promotion value is chosen so a scan that read
-- the old file at the WRONG width returns a wrong number rather than a
-- coincidentally-equal one, and every post-promotion value is chosen so a
-- scan that read the new file at the OLD narrow type could not represent it
-- at all:
--   * int_long straddles the 32-bit boundary in both directions. The
--     pre-promotion rows are exactly i32::MAX and i32::MIN -- a wrong-width
--     or unsigned read loses the sign on the latter -- and the
--     post-promotion rows are exactly one step outside that range on each
--     side, the tightest values a 32-bit column cannot hold.
--   * float_double's pre-promotion values (3.5, -1.25) are exactly
--     representable in binary32, so widening them to double is exact and the
--     expected value is the literal itself rather than an f32 expansion. The
--     post-promotion values need more than binary32's 24 mantissa bits, so a
--     narrow read would round them visibly.
--   * decimal_decimal's post-promotion values use all 18 integral digits of
--     decimal(20,2) and do not fit decimal(10,2) at all.
-- Every literal is explicitly CAST so the inserted type is the declared
-- column type rather than whatever Spark infers for the bare literal.
--
-- Ground truth (kept in lockstep with
-- crates/lakehouse-engine/tests/common/type_promotion_fixtures.rs -- do not
-- change one without the other):
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
--
-- DROP CONDITION: this fixture stands as long as the scan reads a narrow
-- physical column through the current wider logical type. Retire it only if
-- that relaxation cast is removed.

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

-- Written at the SOURCE types, BEFORE the promotions below: this is the data
-- file whose physical Parquet encoding the fixture-shape test asserts. The
-- REPARTITION(1) hint is required: under `local[*]`, a bare `INSERT ...
-- VALUES` fans out across every core instead of writing one file per
-- statement (see create_file_granularity_fixture.sql for the observed
-- effect).
INSERT INTO rest_catalog.e2e_lakehouse.iceberg_type_promotion
SELECT /*+ REPARTITION(1) */ id, int_long, float_double, decimal_decimal
FROM VALUES
  (CAST(1 AS BIGINT), CAST(2147483647 AS INT), CAST(3.5 AS FLOAT),
   CAST(12345678.90 AS DECIMAL(10,2))),
  (CAST(2 AS BIGINT), CAST(-2147483648 AS INT), CAST(-1.25 AS FLOAT),
   CAST(-12345678.90 AS DECIMAL(10,2)))
  AS t(id, int_long, float_double, decimal_decimal);

-- The three promotions. Each rewrites only the table's current schema: the
-- data file committed above keeps its int / float / decimal(10,2) encoding,
-- and every schema stays in the metadata's schema history.
ALTER TABLE rest_catalog.e2e_lakehouse.iceberg_type_promotion
  ALTER COLUMN int_long TYPE BIGINT;

ALTER TABLE rest_catalog.e2e_lakehouse.iceberg_type_promotion
  ALTER COLUMN float_double TYPE DOUBLE;

ALTER TABLE rest_catalog.e2e_lakehouse.iceberg_type_promotion
  ALTER COLUMN decimal_decimal TYPE DECIMAL(20,2);

-- Written at the TARGET types, AFTER the promotions: a second data file, so
-- one query reads both physical layouts. The values are outside what the
-- source types could hold -- see the header for why each was chosen.
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
