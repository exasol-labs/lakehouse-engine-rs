-- Ground truth, in lockstep with crates/lakehouse-engine/tests/common/pos_delete_fixtures.rs:
--   rest_catalog.e2e_lakehouse.unsupported_format:
--     id 1..=10 Parquet, id 21..=30 ORC

CREATE NAMESPACE IF NOT EXISTS rest_catalog.e2e_lakehouse;

-- The former name of this table, which a stack provisioned earlier still holds.
DROP TABLE IF EXISTS rest_catalog.e2e_lakehouse.mor_dv_unsupported;
DROP TABLE IF EXISTS rest_catalog.e2e_lakehouse.unsupported_format;

CREATE TABLE rest_catalog.e2e_lakehouse.unsupported_format (
  id  BIGINT,
  val STRING
)
USING iceberg;

INSERT INTO rest_catalog.e2e_lakehouse.unsupported_format
SELECT /*+ REPARTITION(1) */ id, printf('row-%02d', id) FROM range(1, 11) AS t(id);

ALTER TABLE rest_catalog.e2e_lakehouse.unsupported_format
SET TBLPROPERTIES ('write.format.default' = 'orc');

INSERT INTO rest_catalog.e2e_lakehouse.unsupported_format
SELECT /*+ REPARTITION(1) */ id, printf('row-%02d', id) FROM range(21, 31) AS t(id);
