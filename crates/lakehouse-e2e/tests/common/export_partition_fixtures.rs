//! Ground truth for the Spark export-partition fixture. Must stay in lockstep with
//! `scripts/spark-fixtures/create_export_partition_fixture.sql`.

pub const NAMESPACE: &str = "e2e_export_partition";

pub const BY_VALUE: &str = "by_value";
pub const BY_TIME: &str = "by_time";
pub const EVOLVED: &str = "evolved";

/// The name and transform of each field of `by_value`'s partition spec, in order.
pub const BY_VALUE_SPEC: [(&str, &str); 14] = [
    ("s_id", "identity"),
    ("l_id", "identity"),
    ("d_id", "identity"),
    ("i_b_bucket", "bucket[2]"),
    ("l_b_bucket", "bucket[2]"),
    ("dec_b_bucket", "bucket[2]"),
    ("s_b_bucket", "bucket[2]"),
    ("d_b_bucket", "bucket[2]"),
    ("ts_b_bucket", "bucket[2]"),
    ("tz_b_bucket", "bucket[2]"),
    ("i_t_trunc", "truncate[10]"),
    ("l_t_trunc", "truncate[10]"),
    ("dec_t_trunc", "truncate[100]"),
    ("s_t_trunc", "truncate[2]"),
];

/// The name and transform of each field of `by_time`'s partition spec, in order.
pub const BY_TIME_SPEC: [(&str, &str); 5] = [
    ("d_year", "year"),
    ("ts_m_month", "month"),
    ("tz_d_day", "day"),
    ("ts_h_hour", "hour"),
    ("tz_h_hour", "hour"),
];

/// `evolved`'s current spec, which replaced `identity(region)`.
pub const EVOLVED_SPEC: [(&str, &str); 1] = [("id_bucket_8", "bucket[8]")];
/// The `rid`s Spark wrote under `evolved`'s current spec.
pub const EVOLVED_CURRENT_RIDS: std::ops::RangeInclusive<i64> = 5..=8;

pub const BY_VALUE_ROWS: i64 = 38;
pub const BY_TIME_ROWS: i64 = 18;
/// The `rid`s of the rows NULL in every partition column, and of the rows NULL in one partition
/// column each, of `by_value` and `by_time`.
pub const BY_VALUE_NULL_RIDS: std::ops::RangeInclusive<i64> = 9..=38;
pub const BY_TIME_NULL_RIDS: std::ops::RangeInclusive<i64> = 7..=18;
/// The `rid`s of the rows NULL in every partition column.
pub const BY_VALUE_ALL_NULL_RIDS: [i64; 2] = [9, 10];
pub const BY_TIME_ALL_NULL_RIDS: [i64; 2] = [7, 8];

/// One equality filter per `by_value` field: the column, the Exasol literal, and a `rid` of a row
/// that holds it. Each bucket column's other bucket holds a pair whose values straddle the literal.
pub const BY_VALUE_FILTERS: [(&str, &str, i64); 14] = [
    ("S_ID", "'east'", 5),
    ("L_ID", "100", 5),
    ("D_ID", "DATE '2024-02-29'", 5),
    ("I_B", "9", 5),
    ("L_B", "9", 5),
    ("DEC_B", "12.50", 5),
    ("S_B", "'h'", 5),
    ("D_B", "DATE '2023-12-29'", 5),
    ("TS_B", "TIMESTAMP '2023-11-14 22:15:00'", 5),
    ("TZ_B", "TIMESTAMP '2023-11-14 22:15:00'", 5),
    ("I_T", "19", 6),
    ("L_T", "19", 6),
    ("DEC_T", "12.99", 6),
    ("S_T", "'日本'", 6),
];

/// The bucket columns and the rid pairs of the bucket other than the literal's, whose values
/// straddle the literal, for the fixture guard.
pub const STRADDLING_PAIRS: [(i64, i64); 2] = [(1, 2), (3, 4)];
