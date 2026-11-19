-- Partitioned Iceberg tables that Spark created and filled, for the export into partitioned
-- tables. Spark is the oracle that computes every transform: a test compares the partition tuple
-- of each data file the export writes with the tuple Spark gave the rows of the same `rid`. The
-- test resets each table's `main` branch to the snapshot written here before it appends.
--
-- Ground truth, in lockstep with crates/lakehouse-e2e/tests/common/export_partition_fixtures.rs.
--
--   by_value  format version 2, one transform per column:
--     identity(s_id), identity(l_id), identity(d_id),
--     bucket(2, i_b), bucket(2, l_b), bucket(2, dec_b), bucket(2, s_b), bucket(2, d_b),
--     bucket(2, ts_b), bucket(2, tz_b),
--     truncate(10, i_t), truncate(10, l_t), truncate(100, dec_t), truncate(2, s_t)
--     (rid BIGINT, s_id STRING, l_id BIGINT, d_id DATE, i_b INT, l_b BIGINT, dec_b DECIMAL(20,2),
--      s_b STRING, d_b DATE, ts_b TIMESTAMP_NTZ, tz_b TIMESTAMP, i_t INT, l_t BIGINT,
--      dec_t DECIMAL(20,2), s_t STRING)
--   by_time   format version 2: year(d), month(ts_m), day(tz_d), hour(ts_h), hour(tz_h)
--     (rid BIGINT, d DATE, ts_m TIMESTAMP_NTZ, tz_d TIMESTAMP, ts_h TIMESTAMP_NTZ, tz_h TIMESTAMP)
--   evolved   format version 2, (rid BIGINT, id BIGINT, region STRING): rids 1 to 4 under
--     identity(region), which is then replaced by bucket(8, id), then rids 5 to 8
--
-- Rows come in pairs (rid 2k-1, 2k) that share every partition value, so every partition of every
-- field holds two rows. Within a pair, each bucket column holds one value below and one above the
-- pruning test's literal, both in one bucket, so a file of another bucket has column bounds that
-- hold the literal and only its partition value excludes it. Truncate pairs sit on negative
-- values and width boundaries, and multibyte strings, which truncate counts by code point. One
-- pair is NULL in every partition column, and one pair per column is NULL in that column only.
-- Every timestamp is a whole second, so it reads the same at every precision. The TIMESTAMP
-- literals carry their UTC offset, so the session time zone does not shift them.

CREATE NAMESPACE IF NOT EXISTS rest_catalog.e2e_export_partition;

-- by_value

DROP TABLE IF EXISTS rest_catalog.e2e_export_partition.by_value;

CREATE TABLE rest_catalog.e2e_export_partition.by_value (
  rid   BIGINT,
  s_id  STRING,
  l_id  BIGINT,
  d_id  DATE,
  i_b   INT,
  l_b   BIGINT,
  dec_b DECIMAL(20,2),
  s_b   STRING,
  d_b   DATE,
  ts_b  TIMESTAMP_NTZ,
  tz_b  TIMESTAMP,
  i_t   INT,
  l_t   BIGINT,
  dec_t DECIMAL(20,2),
  s_t   STRING
)
USING iceberg
PARTITIONED BY (s_id, l_id, d_id, bucket(2, i_b), bucket(2, l_b), bucket(2, dec_b),
  bucket(2, s_b), bucket(2, d_b), bucket(2, ts_b), bucket(2, tz_b), truncate(10, i_t),
  truncate(10, l_t), truncate(100, dec_t), truncate(2, s_t))
TBLPROPERTIES ('format-version' = '2');

CREATE OR REPLACE TEMPORARY VIEW by_value_pairs AS
SELECT * FROM VALUES
  -- pair 1: every bucket value in bucket 0, the literals' buckets are 1
  (1, 'north', -100L, DATE '1969-07-20', 1, 1L, 10.00BD, 'g', DATE '2022-01-08',
   TIMESTAMP_NTZ '1970-01-01 00:00:00', TIMESTAMP '1970-01-01 00:00:00+00:00',
   -1, -1L, -0.01BD, 'äöx'),
  (2, 'north', -100L, DATE '1969-07-20', 15, 15L, 20.00BD, 'l', DATE '2024-02-07',
   TIMESTAMP_NTZ '2023-11-14 22:18:20', TIMESTAMP '2023-11-14 22:18:20+00:00',
   -9, -9L, -0.99BD, 'äöy'),
  -- pair 2: bucket 0 again, values before 1970
  (3, 'south', 0L, DATE '1970-01-01', -1, -1L, 1.25BD, 'a', DATE '1969-12-31',
   TIMESTAMP_NTZ '1969-12-31 23:00:00', TIMESTAMP '1969-12-31 23:00:00+00:00',
   0, 0L, 0.00BD, 'ab'),
  (4, 'south', 0L, DATE '1970-01-01', 19, 19L, 33.33BD, 'n', DATE '2024-10-04',
   TIMESTAMP_NTZ '2023-11-14 22:18:20', TIMESTAMP '2023-11-14 22:18:20+00:00',
   9, 9L, 0.99BD, 'abz'),
  -- pair 3: the literals' buckets, holding each literal
  (5, 'east', 100L, DATE '2024-02-29', 9, 9L, 12.50BD, 'h', DATE '2023-12-29',
   TIMESTAMP_NTZ '2023-11-14 22:15:00', TIMESTAMP '2023-11-14 22:15:00+00:00',
   10, 10L, 12.34BD, '日本語'),
  (6, 'east', 100L, DATE '2024-02-29', 13, 13L, 25.00BD, 'k', DATE '2024-01-18',
   TIMESTAMP_NTZ '2023-11-14 22:20:00', TIMESTAMP '2023-11-14 22:20:00+00:00',
   19, 19L, 12.99BD, '日本'),
  -- pair 4: the literals' buckets, straddling each literal
  (7, 'west', 1099511627776L, DATE '2024-03-01', 3, 3L, 5.50BD, 'b', DATE '1967-10-24',
   TIMESTAMP_NTZ '1969-12-31 23:59:59', TIMESTAMP '1969-12-31 23:59:59+00:00',
   -11, -11L, -12.01BD, 'zz'),
  (8, 'west', 1099511627776L, DATE '2024-03-01', 17, 17L, 30.00BD, 's', DATE '2023-05-23',
   TIMESTAMP_NTZ '2023-11-14 22:21:40', TIMESTAMP '2023-11-14 22:21:40+00:00',
   -20, -20L, -12.99BD, 'zzz')
AS t(rid, s_id, l_id, d_id, i_b, l_b, dec_b, s_b, d_b, ts_b, tz_b, i_t, l_t, dec_t, s_t);

-- Pairs 1 to 4, the pair NULL in every partition column (rids 9, 10), and one pair per column,
-- a copy of pair 1 NULL in that column only (rids 11 to 38, in column order).
INSERT INTO rest_catalog.e2e_export_partition.by_value
SELECT * FROM by_value_pairs
UNION ALL
SELECT rid + 8, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM by_value_pairs WHERE rid <= 2
UNION ALL
SELECT rid + 8 + 2 * c.n,
  IF(c.n = 1, NULL, s_id), IF(c.n = 2, NULL, l_id), IF(c.n = 3, NULL, d_id),
  IF(c.n = 4, NULL, i_b), IF(c.n = 5, NULL, l_b), IF(c.n = 6, NULL, dec_b),
  IF(c.n = 7, NULL, s_b), IF(c.n = 8, NULL, d_b), IF(c.n = 9, NULL, ts_b),
  IF(c.n = 10, NULL, tz_b), IF(c.n = 11, NULL, i_t), IF(c.n = 12, NULL, l_t),
  IF(c.n = 13, NULL, dec_t), IF(c.n = 14, NULL, s_t)
FROM by_value_pairs CROSS JOIN (SELECT explode(sequence(1, 14)) AS n) c
WHERE rid <= 2;

-- by_time

DROP TABLE IF EXISTS rest_catalog.e2e_export_partition.by_time;

CREATE TABLE rest_catalog.e2e_export_partition.by_time (
  rid  BIGINT,
  d    DATE,
  ts_m TIMESTAMP_NTZ,
  tz_d TIMESTAMP,
  ts_h TIMESTAMP_NTZ,
  tz_h TIMESTAMP
)
USING iceberg
PARTITIONED BY (years(d), months(ts_m), days(tz_d), hours(ts_h), hours(tz_h))
TBLPROPERTIES ('format-version' = '2');

CREATE OR REPLACE TEMPORARY VIEW by_time_pairs AS
SELECT * FROM VALUES
  -- pair 1: before 1970, so each ordinal is negative
  (1, DATE '1969-01-01', TIMESTAMP_NTZ '1969-12-01 00:00:00',
   TIMESTAMP '1969-12-31 00:00:00+00:00', TIMESTAMP_NTZ '1969-12-31 23:00:00',
   TIMESTAMP '1969-12-31 23:00:00+00:00'),
  (2, DATE '1969-12-31', TIMESTAMP_NTZ '1969-12-31 23:59:59',
   TIMESTAMP '1969-12-31 23:59:59+00:00', TIMESTAMP_NTZ '1969-12-31 23:59:59',
   TIMESTAMP '1969-12-31 23:59:59+00:00'),
  -- pair 2: the epoch, on each unit's lower and upper bound
  (3, DATE '1970-01-01', TIMESTAMP_NTZ '1970-01-01 00:00:00',
   TIMESTAMP '1970-01-01 00:00:00+00:00', TIMESTAMP_NTZ '1970-01-01 00:00:00',
   TIMESTAMP '1970-01-01 00:00:00+00:00'),
  (4, DATE '1970-12-31', TIMESTAMP_NTZ '1970-01-31 23:59:59',
   TIMESTAMP '1970-01-01 23:59:59+00:00', TIMESTAMP_NTZ '1970-01-01 00:59:59',
   TIMESTAMP '1970-01-01 00:59:59+00:00'),
  -- pair 3: a leap day
  (5, DATE '2024-02-29', TIMESTAMP_NTZ '2024-02-01 00:00:00',
   TIMESTAMP '2024-02-29 00:00:00+00:00', TIMESTAMP_NTZ '2024-02-29 13:00:00',
   TIMESTAMP '2024-02-29 13:00:00+00:00'),
  (6, DATE '2024-12-31', TIMESTAMP_NTZ '2024-02-29 12:00:00',
   TIMESTAMP '2024-02-29 23:59:59+00:00', TIMESTAMP_NTZ '2024-02-29 13:30:00',
   TIMESTAMP '2024-02-29 13:30:00+00:00')
AS t(rid, d, ts_m, tz_d, ts_h, tz_h);

-- Pairs 1 to 3, the pair NULL in every partition column (rids 7, 8), and one pair per column,
-- a copy of pair 1 NULL in that column only (rids 9 to 18, in column order).
INSERT INTO rest_catalog.e2e_export_partition.by_time
SELECT * FROM by_time_pairs
UNION ALL
SELECT rid + 6, NULL, NULL, NULL, NULL, NULL FROM by_time_pairs WHERE rid <= 2
UNION ALL
SELECT rid + 6 + 2 * c.n,
  IF(c.n = 1, NULL, d), IF(c.n = 2, NULL, ts_m), IF(c.n = 3, NULL, tz_d),
  IF(c.n = 4, NULL, ts_h), IF(c.n = 5, NULL, tz_h)
FROM by_time_pairs CROSS JOIN (SELECT explode(sequence(1, 5)) AS n) c
WHERE rid <= 2;

-- evolved

DROP TABLE IF EXISTS rest_catalog.e2e_export_partition.evolved;

CREATE TABLE rest_catalog.e2e_export_partition.evolved (rid BIGINT, id BIGINT, region STRING)
USING iceberg
PARTITIONED BY (region)
TBLPROPERTIES ('format-version' = '2');

INSERT INTO rest_catalog.e2e_export_partition.evolved VALUES
  (1, 1, 'eu'), (2, 2, 'eu'), (3, 3, 'us'), (4, 4, 'us');

ALTER TABLE rest_catalog.e2e_export_partition.evolved
  REPLACE PARTITION FIELD region WITH bucket(8, id);

INSERT INTO rest_catalog.e2e_export_partition.evolved VALUES
  (5, 1, 'eu'), (6, 2, 'eu'), (7, 3, 'us'), (8, 4, 'us');
