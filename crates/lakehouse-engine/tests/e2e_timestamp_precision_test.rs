//! End-to-end coverage for the version-gated timestamp precision declaration
//! (`add-timestamp-precision-versioning`): an Iceberg `timestamp` /
//! `timestamptz` column is declared — and round-trips — at the precision the
//! running engine actually supports.
//!
//! Seeds `e2e_tsprecision.ts_precision_probe` (`common::seed::
//! seed_timestamp_precision_probe`) into its OWN Iceberg namespace and creates
//! its own Virtual Schema over it, so the probe never enters another suite's
//! table enumeration. The four seeded values (`.000001`, `.000002`, `.123456`,
//! `.123457`) are two pairs that stay distinct at microsecond precision and
//! collapse pairwise at millisecond precision.
//!
//! Expectations come from `common::timestamp_precision`, an oracle that reads
//! the live engine version and maps it with its own table rather than by
//! calling the production rule under test.
//!
//! The rendered fractional DIGIT COUNT is deliberately never asserted: the
//! WebSocket protocol renders six fractional digits for every `TIMESTAMP`
//! regardless of declared precision (a `TIMESTAMP(3)` column renders
//! `...123000`), so only the rendered VALUE and `COUNT(DISTINCT)` discriminate
//! the two arms.
//!
//! Per project rules this test FAILS (never skips) when its stack is
//! unreachable: the `wait_for_*` helpers panic rather than return `Err`.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::e2e_harness::*;
use common::exasol_ws::ExaConn;
use common::seed::{
    E2E_TSPRECISION_NAMESPACE, E2E_TSPRECISION_TABLE, TSPRECISION_COL_TS, TSPRECISION_COL_TSTZ,
    TSPRECISION_MICROS, seed_timestamp_precision_probe,
};
use common::stack::{
    iceberg_catalog_url, wait_for_exasol, wait_for_iceberg_catalog, wait_for_minio,
};
use common::timestamp_precision::expected_timestamp_precision;
use std::sync::OnceLock;

const VS_NAME: &str = "TS_PRECISION_VS";

static SETUP_DONE: OnceLock<()> = OnceLock::new();

/// Seed the probe and provision its Virtual Schema, once per binary.
fn setup() {
    SETUP_DONE.get_or_init(|| {
        wait_for_exasol();
        wait_for_minio();
        wait_for_iceberg_catalog();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(async {
            seed_timestamp_precision_probe(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed timestamp-precision probe table")
        });

        install_slc();
        upload_so();
        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
        create_virtual_schema(&mut conn, &VsProps::new(VS_NAME, E2E_TSPRECISION_NAMESPACE));
    });
}

/// The Exasol-served name of the seeded probe table.
fn served_table() -> String {
    E2E_TSPRECISION_TABLE.to_uppercase()
}

/// The declared `COLUMN_TYPE` for `column`, whitespace-stripped.
fn declared_type(conn: &mut ExaConn, table: &str, column: &str) -> String {
    let ty = conn.query_columns(&format!(
        "SELECT COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS \
         WHERE COLUMN_SCHEMA='{VS_NAME}' AND COLUMN_TABLE='{table}' AND COLUMN_NAME='{column}'"
    ))[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("{column} has no declared type"))
        .to_string();
    ty.chars().filter(|c| !c.is_whitespace()).collect()
}

/// The microsecond value that survives storage at `precision` fractional digits.
fn retained_at(micros: i64, precision: u32) -> i64 {
    let step = 10i64.pow(6 - precision);
    micros.div_euclid(step) * step
}

/// Render `micros` the way the WebSocket protocol renders a `TIMESTAMP` — six
/// fractional digits, whatever the declared precision.
fn rendered(micros: i64) -> String {
    chrono::DateTime::from_timestamp_micros(micros)
        .unwrap_or_else(|| panic!("seeded value {micros} must be a valid instant"))
        .format("%Y-%m-%d %H:%M:%S%.6f")
        .to_string()
}

/// Assert `sql` reaches the scan UDF rather than an unaccelerated fallback.
fn assert_pushed_to_scan_udf(conn: &mut ExaConn, sql: &str, shape: &str) {
    let pushed = explain_virtual_sql(conn, sql);
    assert!(
        pushed.contains(SCAN_SCRIPT_NAME),
        "{shape} must drive the scan UDF: {pushed}"
    );
}

/// Scenario: Microsecond-distinct Iceberg timestamps round-trip at the declared
/// precision, and `createVirtualSchema` declares that precision from the live
/// engine version.
///
/// On an engine that supports microseconds both columns are declared
/// `TIMESTAMP(6)` and all four seeded values stay distinct; on a
/// millisecond-only engine both are declared `TIMESTAMP(3)` and the four
/// collapse to two. Rendered values are asserted for the naive `ts` column only
/// — a `timestamptz` rendering additionally depends on the session time zone,
/// which is not what this scenario pins.
#[test]
fn iceberg_microsecond_timestamps_round_trip_at_the_declared_precision() {
    setup();
    let mut conn = exa_conn();
    let table = served_table();
    let qualified = format!("{VS_NAME}.{table}");

    let expected = expected_timestamp_precision(&mut conn);
    let ts_column = TSPRECISION_COL_TS.to_uppercase();
    let tstz_column = TSPRECISION_COL_TSTZ.to_uppercase();

    for column in [&ts_column, &tstz_column] {
        let declared = declared_type(&mut conn, &table, column);
        assert_eq!(
            declared, expected.declared_column_type,
            "{column} must be declared {} on this engine, got {declared}",
            expected.declared_column_type
        );
    }

    let precision = expected.retained_fractional_digits;
    let expected_rendered: Vec<String> = TSPRECISION_MICROS
        .iter()
        .map(|&micros| rendered(retained_at(micros, precision)))
        .collect();

    let projection_sql = format!("SELECT ID, {ts_column} FROM {qualified} ORDER BY ID");
    let projected = conn.query_columns(&projection_sql);
    let actual_rendered: Vec<String> = projected[1]
        .iter()
        .map(|value| {
            value
                .as_str()
                .unwrap_or_else(|| panic!("{ts_column} must render as a string, got {value:?}"))
                .to_string()
        })
        .collect();
    assert_eq!(
        actual_rendered, expected_rendered,
        "{ts_column} must round-trip every seeded value at {} — a value truncated further \
         means the declared precision never reached the scan output",
        expected.declared_column_type
    );
    assert_pushed_to_scan_udf(&mut conn, &projection_sql, "the timestamp projection");

    for column in [&ts_column, &tstz_column] {
        let distinct_sql = format!("SELECT COUNT(DISTINCT {column}) FROM {qualified}");
        let distinct_count = conn.query_scalar_i64(&distinct_sql);
        assert_eq!(
            distinct_count, expected.distinct_count,
            "COUNT(DISTINCT {column}) must be {} at {} — the seeded \
             .000001/.000002/.123456/.123457 values collapse pairwise below microsecond precision",
            expected.distinct_count, expected.declared_column_type
        );
        assert_pushed_to_scan_udf(
            &mut conn,
            &distinct_sql,
            &format!("COUNT(DISTINCT {column})"),
        );
    }
}
