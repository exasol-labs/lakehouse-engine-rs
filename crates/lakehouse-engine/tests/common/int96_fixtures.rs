//! Ground truth for the Spark far-future INT96 timestamp fixture (#143). Authored at
//! stack bring-up by the `spark-iceberg-fixtures` job, not by this harness: only a
//! native Spark write with `outputTimestampType=INT96` plus Iceberg `add_files` lands
//! a genuine INT96 column. Must stay in lockstep with
//! `scripts/spark-fixtures/create_int96_timestamp_fixture.sql`.

pub const NAMESPACE: &str = "e2e_lakehouse";

pub const INT96_TS_FAR_FUTURE_TABLE: &str = "int96_ts_far_future";

pub const INT96_TS_FAR_FUTURE_COLUMN: &str = "ts";

/// Overflows arrow-rs's default INT96→Nanosecond decode (#143).
pub const INT96_TS_FAR_FUTURE_EXPECTED_VALUE: &str = "9999-12-31 23:59:59";
