//! Independent oracle for the timestamp-precision version gate. Duplicates the
//! version-to-precision rule instead of calling `EngineTimestampSupport`, and reads the
//! version from the live session rather than `EXASOL_IMAGE`, so a wrong rule or stale
//! env var cannot silently pick the wrong assertion arm.

use super::exasol_ws::ExaConn;

/// The WebSocket protocol always renders six fractional digits, so `distinct_count`
/// over the seeded `.000001/.000002/.123456/.123457` values is what separates the arms.
pub struct ExpectedTimestampPrecision {
    /// `SYS.EXA_ALL_COLUMNS` never reports a bare `TIMESTAMP`. Usable as a CAST target only
    /// for `p in {3, 6}`, so callers must pick the arm via [`engine_honors_declared_precision`].
    pub declared_column_type: &'static str,
    pub distinct_count: i64,
    pub retained_fractional_digits: u32,
}

impl ExpectedTimestampPrecision {
    pub const MICROSECOND: Self = Self {
        declared_column_type: "TIMESTAMP(6)",
        distinct_count: 4,
        retained_fractional_digits: 6,
    };
    pub const MILLISECOND: Self = Self {
        declared_column_type: "TIMESTAMP(3)",
        distinct_count: 2,
        retained_fractional_digits: 3,
    };
    /// `distinct_count` still describes the microsecond-seeded fixture.
    pub const NANOSECOND: Self = Self {
        declared_column_type: "TIMESTAMP(9)",
        distinct_count: 4,
        retained_fractional_digits: 9,
    };
}

/// `databaseProductVersion`: a `databaseVersion` row does not exist on either supported engine.
pub fn live_engine_version(conn: &mut ExaConn) -> String {
    conn.query_columns(
        "SELECT PARAM_VALUE FROM SYS.EXA_METADATA WHERE PARAM_NAME = 'databaseProductVersion'",
    )[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("databaseProductVersion missing from SYS.EXA_METADATA"))
        .to_string()
}

fn leading_version_component(version: &str) -> Option<u32> {
    version.split('.').next().and_then(|s| s.parse().ok())
}

pub fn expected_timestamp_precision_for(version: &str) -> ExpectedTimestampPrecision {
    match leading_version_component(version) {
        Some(year) if year < 2025 => ExpectedTimestampPrecision::MILLISECOND,
        _ => ExpectedTimestampPrecision::MICROSECOND,
    }
}

pub fn expected_timestamp_precision(conn: &mut ExaConn) -> ExpectedTimestampPrecision {
    expected_timestamp_precision_for(&live_engine_version(conn))
}

/// True on `>= 2025` or an unparseable version.
pub fn engine_honors_declared_precision(conn: &mut ExaConn) -> bool {
    let version = live_engine_version(conn);
    leading_version_component(&version).is_none_or(|year| year >= 2025)
}
