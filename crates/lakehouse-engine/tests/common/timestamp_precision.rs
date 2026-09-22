//! Independent E2E oracle for the timestamp-precision version gate
//! (`add-timestamp-precision-versioning` task 5).
//!
//! Deliberately duplicates the version-to-precision mapping rather than calling
//! `EngineTimestampSupport::from_database_version`: a test that computes its expectation by calling the
//! rule under test cannot fail when that rule is wrong (decision-log.md `[9]`). Reads the version from
//! the live session rather than the `EXASOL_IMAGE` env var for the same reason — an absent or stale
//! variable would silently select the wrong assertion arm.

use super::exasol_ws::ExaConn;

/// What tasks 7 and 9 should observe on the live engine for one timestamp-precision arm.
///
/// `distinct_count` is the sharper discriminator: the WebSocket protocol always renders six
/// fractional digits regardless of declared precision, so digit-count alone cannot distinguish the
/// arms, but `COUNT(DISTINCT)` over the seeded `.000001/.000002/.123456/.123457` values can.
pub struct ExpectedTimestampPrecision {
    /// The exact `SYS.EXA_ALL_COLUMNS.COLUMN_TYPE` string this arm declares — `TIMESTAMP(3)` for
    /// the millisecond arm, never bare `TIMESTAMP` — `SYS.EXA_ALL_COLUMNS` never reports a bare
    /// `TIMESTAMP` on either supported engine (decision-log.md `[C1]`).
    ///
    /// Usable as a CAST target only within `p in {3, 6}` (`[C3]`), so a caller casting to it
    /// must select the arm through [`engine_honors_declared_precision`].
    pub declared_column_type: &'static str,
    /// The number of distinct values `COUNT(DISTINCT)` reports over the seeded
    /// `.000001/.000002/.123456/.123457` fixture at this arm's precision.
    pub distinct_count: i64,
    /// The fractional-second digits this arm actually retains — the integer form of
    /// `declared_column_type`'s `TIMESTAMP(p)`, kept as its own field so a consumer needing the
    /// digit count does not have to decode it back out of the display string.
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
    /// The arm an Iceberg `timestamp_ns` column reaches on an engine that emits nine digits.
    /// `distinct_count` still describes the microsecond-seeded fixture.
    pub const NANOSECOND: Self = Self {
        declared_column_type: "TIMESTAMP(9)",
        distinct_count: 4,
        retained_fractional_digits: 9,
    };
}

/// Read the live engine's version string from `SYS.EXA_METADATA`.
///
/// `databaseProductVersion`, not `databaseVersion` — the latter row does not exist on either
/// supported engine (confirmed live, decision-log.md `[C5]`). Returns the bare version string, e.g.
/// `"2025.2.1"` or `"8.29.13"`, the same string `ctx.database_version()` reports.
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

/// This oracle's own `>= 2025` gate: the same shape as the production rule, separately written.
pub fn expected_timestamp_precision_for(version: &str) -> ExpectedTimestampPrecision {
    match leading_version_component(version) {
        Some(year) if year < 2025 => ExpectedTimestampPrecision::MILLISECOND,
        _ => ExpectedTimestampPrecision::MICROSECOND,
    }
}

/// Read the live engine's version and return the precision arm expected on it.
pub fn expected_timestamp_precision(conn: &mut ExaConn) -> ExpectedTimestampPrecision {
    expected_timestamp_precision_for(&live_engine_version(conn))
}

/// True on `>= 2025` or an unparseable version. Both recorded engine differences turn on this
/// boundary: `[C3]`'s CAST-target domain and `[C2]`'s pushdown echo. Each caller names which.
pub fn engine_honors_declared_precision(conn: &mut ExaConn) -> bool {
    let version = live_engine_version(conn);
    leading_version_component(&version).is_none_or(|year| year >= 2025)
}
