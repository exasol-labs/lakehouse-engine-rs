-- Iceberg tables that read a file without embedded field ids only through
-- `schema.name-mapping.default`. `add_files` imports the file, because only a metadata-only
-- import registers Parquet without field ids. The paths use s3:// because add_files records the
-- writer's scheme and the scan UDF registers its object store under s3://.
--
-- Ground truth, in lockstep with crates/lakehouse-e2e/tests/common/name_mapping_fixtures.rs.
--
--   nested_name_map      (id, label, addr struct, items list<struct>, attrs map<string, struct>),
--                        unpartitioned, whose nested members are renamed after the imports
--     id 1  imported without field ids under the first names, which only the name mapping binds
--     id 2  native before the renames, so its embedded ids bind it
--     id 3  native after the renames
--   projection_by_id     (id, note, info struct<code, tag, memo>, extra), unpartitioned, whose
--                        hand-set name mapping omits extra and info.memo
--     id 1  native, then note and info.tag are dropped and re-added, so both read NULL
--     id 2  imported without field ids, so its unmapped extra and info.memo read NULL
--     id 3  native after the drops

CREATE NAMESPACE IF NOT EXISTS rest_catalog.e2e_lakehouse;

-- nested_name_map
-- addr.zip_v1 is renamed before add_files creates the mapping, so the mapping never learns that
-- name. addr.city is renamed to town and a new addr.city is added, so the mapping maps `city` to
-- the new member while id 2's file embeds town's id under that name.

DROP TABLE IF EXISTS rest_catalog.e2e_lakehouse.nested_name_map;

CREATE TABLE rest_catalog.e2e_lakehouse.nested_name_map (
  id INT,
  label_v1 STRING,
  addr STRUCT<street_v1: STRING, city: STRING, zip_v1: STRING, geo: STRUCT<lat_v1: DOUBLE>>,
  items ARRAY<STRUCT<qty_v1: INT>>,
  attrs MAP<STRING, STRUCT<amt_v1: INT>>
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

ALTER TABLE rest_catalog.e2e_lakehouse.nested_name_map RENAME COLUMN addr.zip_v1 TO zip;

INSERT INTO rest_catalog.e2e_lakehouse.nested_name_map
SELECT 2,
       'two',
       named_struct('street_v1', 'Elm St', 'city', 'Oslo', 'zip', '0150',
                    'geo', named_struct('lat_v1', 59.9D)),
       array(named_struct('qty_v1', 2)),
       map('b', named_struct('amt_v1', 20));

INSERT OVERWRITE DIRECTORY 's3://warehouse/e2e_lakehouse/nested_name_map_source'
USING parquet
SELECT /*+ REPARTITION(1) */
       1 AS id,
       'one' AS label_v1,
       named_struct('street_v1', 'Main St', 'zip_v1', '10115',
                    'geo', named_struct('lat_v1', 52.5D)) AS addr,
       array(named_struct('qty_v1', 1), named_struct('qty_v1', 3)) AS items,
       map('a', named_struct('amt_v1', 10)) AS attrs;

CALL rest_catalog.system.add_files(
  table => 'e2e_lakehouse.nested_name_map',
  source_table => '`parquet`.`s3://warehouse/e2e_lakehouse/nested_name_map_source`'
);

ALTER TABLE rest_catalog.e2e_lakehouse.nested_name_map RENAME COLUMN label_v1 TO label;
ALTER TABLE rest_catalog.e2e_lakehouse.nested_name_map RENAME COLUMN addr.street_v1 TO street_v2;
ALTER TABLE rest_catalog.e2e_lakehouse.nested_name_map RENAME COLUMN addr.street_v2 TO street;
ALTER TABLE rest_catalog.e2e_lakehouse.nested_name_map RENAME COLUMN addr.geo.lat_v1 TO lat;
ALTER TABLE rest_catalog.e2e_lakehouse.nested_name_map RENAME COLUMN items.element.qty_v1 TO qty;
ALTER TABLE rest_catalog.e2e_lakehouse.nested_name_map RENAME COLUMN attrs.value.amt_v1 TO amt;
ALTER TABLE rest_catalog.e2e_lakehouse.nested_name_map RENAME COLUMN addr.city TO town;
ALTER TABLE rest_catalog.e2e_lakehouse.nested_name_map ADD COLUMN addr.city STRING;

INSERT INTO rest_catalog.e2e_lakehouse.nested_name_map
SELECT 3,
       'three',
       named_struct('street', 'Oak St', 'town', 'Rome', 'zip', '00184',
                    'geo', named_struct('lat', 41.9D), 'city', 'Roma'),
       array(named_struct('qty', 4)),
       map('c', named_struct('amt', 30));

-- projection_by_id
-- The mapping is set before add_files, which keeps an existing one, so it never maps `extra` or
-- `info.memo`. `note` and `info.tag` are dropped and re-added after id 1 was written, so id 1's
-- file holds both names under ids the current schema no longer has.

DROP TABLE IF EXISTS rest_catalog.e2e_lakehouse.projection_by_id;

CREATE TABLE rest_catalog.e2e_lakehouse.projection_by_id (
  id INT,
  note STRING,
  info STRUCT<code: STRING, tag: STRING, memo: STRING>,
  extra STRING
)
USING iceberg
TBLPROPERTIES (
  'format-version' = '2',
  'schema.name-mapping.default' = '[{"field-id": 1, "names": ["id"]}, {"field-id": 2, "names": ["note"]}, {"field-id": 3, "names": ["info"], "fields": [{"field-id": 5, "names": ["code"]}, {"field-id": 6, "names": ["tag"]}]}]'
);

INSERT INTO rest_catalog.e2e_lakehouse.projection_by_id
SELECT 1, 'old note', named_struct('code', 'c1', 'tag', 'old tag', 'memo', 'm1'), 'e1';

INSERT OVERWRITE DIRECTORY 's3://warehouse/e2e_lakehouse/projection_by_id_source'
USING parquet
SELECT /*+ REPARTITION(1) */
       2 AS id,
       'imported note' AS note,
       named_struct('code', 'c2', 'tag', 't2', 'memo', 'm2') AS info,
       'e2' AS extra;

CALL rest_catalog.system.add_files(
  table => 'e2e_lakehouse.projection_by_id',
  source_table => '`parquet`.`s3://warehouse/e2e_lakehouse/projection_by_id_source`'
);

ALTER TABLE rest_catalog.e2e_lakehouse.projection_by_id DROP COLUMN note;
ALTER TABLE rest_catalog.e2e_lakehouse.projection_by_id ADD COLUMN note STRING;
ALTER TABLE rest_catalog.e2e_lakehouse.projection_by_id DROP COLUMN info.tag;
ALTER TABLE rest_catalog.e2e_lakehouse.projection_by_id ADD COLUMN info.tag STRING;

INSERT INTO rest_catalog.e2e_lakehouse.projection_by_id
SELECT 3, named_struct('code', 'c3', 'memo', 'm3', 'tag', 't3'), 'e3', 'n3';
