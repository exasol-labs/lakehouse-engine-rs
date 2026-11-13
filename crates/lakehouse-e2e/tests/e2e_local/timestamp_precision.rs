//! Seeded values (`.000001`, `.000002`, `.123456`, `.123457`) stay distinct at microsecond and
//! collapse pairwise at millisecond precision. The rendered fractional digit count is never
//! asserted: a `TIMESTAMP` renders through `NLS_TIMESTAMP_FORMAT`, not its declared precision.

use crate::common::e2e_harness::*;
use crate::common::exasol_ws::ExaConn;
use crate::common::seed::{
    E2E_TSPRECISION_NAMESPACE, E2E_TSPRECISION_TABLE, TSPRECISION_COL_TS, TSPRECISION_COL_TS_NS,
    TSPRECISION_COL_TSTZ, TSPRECISION_MICROS, TSPRECISION_NANOS, seed_timestamp_precision_probe,
};
use crate::common::stack::iceberg_catalog_url;
use crate::common::timestamp_precision::{
    ExpectedTimestampPrecision, engine_honors_declared_precision, expected_timestamp_precision,
    live_engine_version,
};
use crate::fixtures::{assert_pushed_to_scan_udf, emits_clause, stack};
use std::sync::OnceLock;

const VS_NAME: &str = "TS_PRECISION_VS";

const NANOSECOND_TIMESTAMP_FORMAT: &str = "YYYY-MM-DD HH24:MI:SS.FF9";

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup() {
    SETUP_DONE.get_or_init(|| {
        stack();
        runtime()
            .block_on(seed_timestamp_precision_probe(
                &iceberg_catalog_url(),
                "s3://warehouse/",
            ))
            .expect("seed timestamp-precision probe table");
        create_virtual_schema(
            &mut exa_conn(),
            &VsProps::new(VS_NAME, E2E_TSPRECISION_NAMESPACE),
        );
    });
}

fn served_table() -> String {
    E2E_TSPRECISION_TABLE.to_uppercase()
}

/// Clamped at six: the seeded values carry no finer digit.
fn retained_at(micros: i64, precision: u32) -> i64 {
    let step = 10i64.pow(6 - precision.min(6));
    micros.div_euclid(step) * step
}

/// The WebSocket protocol renders six fractional digits whatever the declared precision.
fn rendered(micros: i64) -> String {
    chrono::DateTime::from_timestamp_micros(micros)
        .unwrap_or_else(|| panic!("seeded value {micros} must be a valid instant"))
        .format("%Y-%m-%d %H:%M:%S%.6f")
        .to_string()
}

fn rendered_column(column: &[serde_json::Value], name: &str) -> Vec<String> {
    column
        .iter()
        .map(|value| {
            value
                .as_str()
                .unwrap_or_else(|| panic!("{name} must render as a string, got {value:?}"))
                .to_string()
        })
        .collect()
}

fn without_trailing_fraction_zeros(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| match value.split_once('.') {
            Some((instant, fraction)) => match fraction.trim_end_matches('0') {
                "" => instant.to_string(),
                kept => format!("{instant}.{kept}"),
            },
            None => value,
        })
        .collect()
}

fn accepts_every_cast_precision(conn: &mut ExaConn) -> bool {
    if engine_honors_declared_precision(conn) {
        return true;
    }
    eprintln!(
        "engine {} rejects a TIMESTAMP(p) CAST target outside {{3, 6}} as `0A000 Feature not \
         supported` (decision-log.md [C3]) — this width is not measurable on this leg",
        live_engine_version(conn)
    );
    false
}

fn assert_emits_declares(conn: &mut ExaConn, query_sql: &str, expected: &str) -> String {
    let pushdown_sql = isolated_pushdown_statement(conn, query_sql);
    let emits = emits_clause(&pushdown_sql);
    assert!(
        emits.contains(expected),
        "the scan must declare {expected} in its EMITS clause, got {emits}. Neither cause is an \
         SLC rejection of the Arrow unit: either this build stripped `fractionalSecondsPrecision` \
         from the pushdown echo (decision-log.md [C2]), or it rejected the `TIMESTAMP(p)` CAST \
         target as `0A000 Feature not supported` ([C3]).\ngenerated SQL: {pushdown_sql}"
    );
    pushdown_sql
}

/// Scenario: A timestamp filter compares at full precision
/// Scenario: A TIMESTAMP(p) column declaration serializes fractionalSecondsPrecision
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
        let declared = declared_type(&mut conn, VS_NAME, &table, column);
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
    let actual_rendered = rendered_column(&projected[1], &ts_column);
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

    if engine_honors_declared_precision(&mut conn) {
        assert_filters_match_native_copy(
            &mut conn,
            &qualified,
            "TS, TSTZ, TS_NS",
            &[
                "TS < TIMESTAMP '2024-01-01 00:00:00.000001001'",
                "TS = TIMESTAMP '2024-01-01 00:00:00.000001001'",
                "TS <= TIMESTAMP '2024-01-01 00:00:00.123456999'",
                "TS > TIMESTAMP '2024-01-01 00:00:00.123456001'",
                "TSTZ < TIMESTAMP '2024-01-01 00:00:00.000001001'",
                "TS_NS = TIMESTAMP '2024-01-01 00:00:00.000000001'",
                "TS_NS < TIMESTAMP '2024-01-01 00:00:00.000000002'",
            ],
        );
    }
}

fn qualified_probe() -> String {
    format!("{VS_NAME}.{}", served_table())
}

fn cast_projection_sql(column: &str, target: &str) -> String {
    format!(
        "SELECT ID, CAST({column} AS {target}) FROM {} ORDER BY ID",
        qualified_probe()
    )
}

fn rendered_nanos(nanos: i64) -> String {
    chrono::DateTime::from_timestamp_nanos(nanos)
        .format("%Y-%m-%d %H:%M:%S%.9f")
        .to_string()
}

/// Scenario: Projected CAST expression preserves the declared TIMESTAMP fractional-seconds precision in its EMITS type
#[test]
fn cast_to_timestamp9_emits_nanoseconds_and_keeps_every_seeded_digit() {
    setup();
    let mut conn = exa_conn();
    if !accepts_every_cast_precision(&mut conn) {
        return;
    }
    // The seeded instants differ in the ninth digit, which the default six-digit render drops.
    conn.execute(&format!(
        "ALTER SESSION SET NLS_TIMESTAMP_FORMAT='{NANOSECOND_TIMESTAMP_FORMAT}'"
    ));

    let target = ExpectedTimestampPrecision::NANOSECOND.declared_column_type;
    let ts_ns_column = TSPRECISION_COL_TS_NS.to_uppercase();
    let projection_sql = cast_projection_sql(&ts_ns_column, target);

    assert_emits_declares(&mut conn, &projection_sql, target);

    let projected = conn.query_columns(&projection_sql);
    let actual = without_trailing_fraction_zeros(rendered_column(&projected[1], target));
    let want = without_trailing_fraction_zeros(
        TSPRECISION_NANOS
            .iter()
            .copied()
            .map(rendered_nanos)
            .collect(),
    );
    assert_eq!(
        actual, want,
        "every seeded value must survive a {target} emit unchanged; a differing value means the \
         nanosecond Arrow unit lost digits at the emit boundary, not that the SLC rejected it"
    );
    assert_pushed_to_scan_udf(
        &mut conn,
        &projection_sql,
        "the TIMESTAMP(9) cast projection",
    );

    // NANOSECOND.distinct_count describes the TSPRECISION_MICROS fixture, not this one.
    let distinct_sql = format!(
        "SELECT COUNT(DISTINCT CAST({ts_ns_column} AS {target})) FROM {}",
        qualified_probe()
    );
    assert_eq!(
        conn.query_scalar_i64(&distinct_sql),
        2,
        "COUNT(DISTINCT) must be 2 at {target}; a smaller count under a query that raised no \
         error is this build accepting the declaration and silently clamping it"
    );
}

/// Scenario: CAST to TIMESTAMP renders the declared fractional-seconds precision per SQL dialect
#[test]
fn cast_to_timestamp3_emits_milliseconds_and_collapses_the_seeded_pairs() {
    setup();
    let mut conn = exa_conn();
    let expected = ExpectedTimestampPrecision::MILLISECOND;
    let target = expected.declared_column_type;
    let projection_sql = cast_projection_sql(&TSPRECISION_COL_TS.to_uppercase(), target);

    // Exasol treats bare `TIMESTAMP` and `TIMESTAMP(3)` as one type, and the adapter spells
    // precision 3 the first way on every version.
    let pushdown_sql = assert_emits_declares(&mut conn, &projection_sql, "TIMESTAMP");
    let emits = emits_clause(&pushdown_sql);
    assert!(
        !emits.contains("TIMESTAMP("),
        "precision 3 must declare a bare TIMESTAMP, not a parameterized one: {emits}"
    );

    let projected = conn.query_columns(&projection_sql);
    let actual = without_trailing_fraction_zeros(rendered_column(&projected[1], target));
    let want = without_trailing_fraction_zeros(
        TSPRECISION_MICROS
            .iter()
            .map(|&micros| rendered(retained_at(micros, expected.retained_fractional_digits)))
            .collect(),
    );
    assert_eq!(
        actual, want,
        "a {target} emit must retain exactly three fractional digits of every seeded value"
    );
    assert_pushed_to_scan_udf(
        &mut conn,
        &projection_sql,
        "the TIMESTAMP(3) cast projection",
    );

    let distinct_sql = format!(
        "SELECT COUNT(DISTINCT CAST({} AS {target})) FROM {}",
        TSPRECISION_COL_TS.to_uppercase(),
        qualified_probe()
    );
    assert_eq!(
        conn.query_scalar_i64(&distinct_sql),
        expected.distinct_count,
        "COUNT(DISTINCT) must be {} at {target} — the two seeded pairs share a millisecond prefix",
        expected.distinct_count
    );
}

/// Scenario: A CAST to TIMESTAMP without a precision compares at three fractional digits
#[test]
fn cast_to_bare_timestamp_filters_at_milliseconds_like_native() {
    setup();
    let mut conn = exa_conn();
    let table = qualified_probe();

    // The seeded pairs share a millisecond prefix: `.000001` and `.000002` truncate to `.000`,
    // `.123456` and `.123457` to `.123`.
    let filters = [
        "CAST(TS AS TIMESTAMP) = TIMESTAMP '2024-01-01 00:00:00.000'",
        "CAST(TS AS TIMESTAMP) = TIMESTAMP '2024-01-01 00:00:00.123'",
        "CAST(TS AS TIMESTAMP) > TIMESTAMP '2024-01-01 00:00:00.000'",
    ];
    assert_filters_match_native_copy(&mut conn, &table, "TS", &filters);
    /// Each filter matches one seeded pair of rows.
    const SEEDED_PAIR_ROWS: i64 = 2;
    for filter in &filters {
        let count_sql = format!("SELECT COUNT(*) FROM {table} WHERE {filter}");
        assert_eq!(
            conn.query_scalar_i64(&count_sql),
            SEEDED_PAIR_ROWS,
            "{count_sql} must count one seeded pair"
        );
        let pushed = isolated_pushdown_statement(&mut conn, &count_sql);
        assert!(
            pushed.contains(SCAN_SCRIPT_NAME) && pushed.contains("AS TIMESTAMP(3))"),
            "{filter} must stay in the scan's WHERE clause at three fractional digits: {pushed}"
        );
    }
}

/// Scenario: A CAST to TIMESTAMP WITH LOCAL TIME ZONE keeps its declared precision
/// Scenario: Projected CAST expression preserves the declared TIMESTAMP fractional-seconds precision in its EMITS type
#[test]
fn cast_to_timestamp_with_local_time_zone_matches_native() {
    setup();
    let mut conn = exa_conn();
    let target = if engine_honors_declared_precision(&mut conn) {
        "TIMESTAMP(6) WITH LOCAL TIME ZONE"
    } else {
        "TIMESTAMP WITH LOCAL TIME ZONE"
    };
    let query = format!("SELECT ID, CAST(TS AS {target}) FROM {{T}} ORDER BY ID");
    let table = qualified_probe();

    assert_queries_match_native_copy(&mut conn, &table, "TS", &[query.as_str()]);

    let projection_sql = query.replace("{T}", &table);
    assert_pushed_to_scan_udf(&mut conn, &projection_sql, "the TSLTZ cast projection");
    let pushed = isolated_pushdown_statement(&mut conn, &projection_sql);
    let emits = emits_clause(&pushed);
    assert!(
        !emits.contains("LOCAL TIME ZONE"),
        "the scan cannot emit a TIMESTAMP WITH LOCAL TIME ZONE column: {emits}"
    );
    assert!(
        pushed.contains(&format!("AS {target})")),
        "the wrapper must cast to {target}: {pushed}"
    );
}

#[test]
fn declined_cast_to_timestamp2_is_computed_natively_by_exasol_in_the_wrapper() {
    setup();
    let mut conn = exa_conn();
    if !accepts_every_cast_precision(&mut conn) {
        return;
    }
    let declined_target = "TIMESTAMP(2)";
    let projection_sql = cast_projection_sql(&TSPRECISION_COL_TS.to_uppercase(), declined_target);

    let pushdown_sql = isolated_pushdown_statement(&mut conn, &projection_sql);
    assert!(
        pushdown_sql.contains(r#"AS "LHS_T0""#),
        "a declined CAST target must route to the qualified single-table wrapper: {pushdown_sql}"
    );
    assert!(
        pushdown_sql.contains(&format!("AS {declined_target})")),
        "the wrapper's outer select list must carry the declined CAST: {pushdown_sql}"
    );
    let emits = emits_clause(&pushdown_sql);
    assert!(
        !emits.contains(declined_target),
        "the scan must emit the raw {} column, never the declined {declined_target} target: \
         {emits}",
        TSPRECISION_COL_TS.to_uppercase()
    );

    let native_sql = format!(
        "SELECT {}",
        TSPRECISION_MICROS
            .iter()
            .map(|&micros| format!(
                "CAST(TIMESTAMP '{}' AS {declined_target})",
                rendered(micros)
            ))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let native = without_trailing_fraction_zeros(
        conn.query_columns(&native_sql)
            .iter()
            .enumerate()
            .map(|(i, column)| rendered_column(column, &format!("native cast {i}"))[0].clone())
            .collect(),
    );

    let projected = conn.query_columns(&projection_sql);
    let actual = without_trailing_fraction_zeros(rendered_column(&projected[1], declined_target));
    assert_eq!(
        actual, native,
        "the declined CAST must return exactly what Exasol computes natively for the same \
         expression over the same literals — the wrapper is the component evaluating it"
    );
}

#[test]
fn iceberg_nanosecond_source_column_is_declared_and_retained_per_engine_arm() {
    setup();
    let mut conn = exa_conn();
    let table = served_table();
    let ts_ns_column = TSPRECISION_COL_TS_NS.to_uppercase();

    let (expected, expected_distinct) = if engine_honors_declared_precision(&mut conn) {
        (ExpectedTimestampPrecision::NANOSECOND, 2)
    } else {
        (ExpectedTimestampPrecision::MILLISECOND, 1)
    };

    let declared = declared_type(&mut conn, VS_NAME, &table, &ts_ns_column);
    assert_eq!(
        declared, expected.declared_column_type,
        "an Iceberg timestamp_ns column must be declared {} on this engine, got {declared}",
        expected.declared_column_type
    );

    let distinct_sql = format!(
        "SELECT COUNT(DISTINCT {ts_ns_column}) FROM {}",
        qualified_probe()
    );
    assert_eq!(
        conn.query_scalar_i64(&distinct_sql),
        expected_distinct,
        "the two seeded instants differ only below the microsecond, so COUNT(DISTINCT \
         {ts_ns_column}) must be {expected_distinct} at {}",
        expected.declared_column_type
    );
    assert_pushed_to_scan_udf(&mut conn, &distinct_sql, "the nanosecond distinct count");
}

/// Scenario: The four functions over a TIMESTAMP keep its fractional seconds in the scan
/// Scenario: A timestamp filter compares at full precision
#[test]
fn date_addition_over_a_nanosecond_timestamp_runs_in_the_scan() {
    setup();
    let mut conn = exa_conn();
    if !engine_honors_declared_precision(&mut conn) {
        return;
    }
    let qualified = qualified_probe();
    create_native_copy(&mut conn, &qualified, "TS, TSTZ, TS_NS");

    // The seeded instants differ only in their last nanosecond digit, so only a result that keeps
    // it equals the literal for a strict subset of the rows.
    let first = chrono::DateTime::from_timestamp_nanos(TSPRECISION_NANOS[0]).naive_utc();
    let queries: Vec<String> = [
        ("ADD_DAYS", chrono::Duration::days(1)),
        ("ADD_WEEKS", chrono::Duration::weeks(1)),
        ("ADD_HOURS", chrono::Duration::hours(1)),
        ("ADD_MINUTES", chrono::Duration::minutes(1)),
    ]
    .into_iter()
    .flat_map(|(function, one)| {
        let result = (first + one).format("%Y-%m-%d %H:%M:%S%.9f");
        [
            format!("SELECT ID, {function}(TS_NS, ID) FROM {{T}}"),
            format!("SELECT ID FROM {{T}} WHERE {function}(TS_NS, 1) = TIMESTAMP '{result}'"),
        ]
    })
    .collect();
    let failures: Vec<String> = queries
        .iter()
        .filter_map(|query| {
            native_copy_mismatch(
                &mut conn,
                &qualified,
                NATIVE_COPY,
                query,
                PathCheck::Has("vs_add_"),
                Expect::Rows,
            )
        })
        .collect();

    assert!(
        failures.is_empty(),
        "a date addition over a TIMESTAMP(9) column must run in the scan and match native:\n{}",
        failures.join("\n")
    );
}

/// Scenario: A result outside the nanosecond range fails the query
#[test]
fn a_nanosecond_date_addition_outside_the_arrow_range_fails_in_the_scan() {
    setup();
    let mut conn = exa_conn();
    if !engine_honors_declared_precision(&mut conn) {
        return;
    }
    let qualified = qualified_probe();
    create_native_copy(&mut conn, &qualified, "TS, TSTZ, TS_NS");

    // docs/semantic-differences.md lists both, and native Exasol returns rows for each.
    let cases = [
        (
            "SELECT ID, ADD_DAYS(TS_NS, 100000) FROM {T}",
            "lies after 2262-04-11 23:47:16.854775807",
        ),
        (
            "SELECT ID FROM {T} WHERE ADD_DAYS(TS_NS, 1) < TIMESTAMP '9999-12-31 00:00:00'",
            "to Timestamp(ns)",
        ),
    ];
    for (query, message) in cases {
        let native = conn.query_columns(&query.replace("{T}", NATIVE_COPY));
        assert!(!native[0].is_empty(), "`{query}` must return rows natively");
        let error = error_text(&conn.try_execute(&query.replace("{T}", &qualified)), query);
        assert!(
            error.contains(message),
            "`{query}` must fail in the scan with `{message}`, got: {error}"
        );
    }
}

/// Scenario: A timestamp filter compares at full precision
#[test]
fn a_derived_timestamp_operand_compares_with_a_finer_literal_like_native() {
    setup();
    let mut conn = exa_conn();
    let table = qualified_probe();

    // Every literal spells more digits than its operand's precision, so each case is floored
    // except the DATE, which DataFusion compares in nanoseconds.
    let filters = [
        "COALESCE(TS, TIMESTAMP '2000-01-01 00:00:00') < TIMESTAMP '2024-01-01 00:00:00.1234565'",
        "GREATEST(TS, TIMESTAMP '2000-01-01 00:00:00.1') = TIMESTAMP '2024-01-01 00:00:00.1234561'",
        "LEAST(TS, TIMESTAMP '2099-01-01 00:00:00') BETWEEN TIMESTAMP '2024-01-01 00:00:00.0000015' AND TIMESTAMP '2024-01-01 00:00:00.1234565'",
        "CASE WHEN ID > 0 THEN TS END >= TIMESTAMP '2024-01-01 00:00:00.0000015'",
        "NULLIF(TS, TIMESTAMP '2024-01-01 00:00:00.000001') > TIMESTAMP '2024-01-01 00:00:00.0000015'",
        "DATE_TRUNC('second', TS) <> TIMESTAMP '2024-01-01 00:00:00.0000005'",
        "CAST(TS AS DATE) < TIMESTAMP '2024-01-01 00:00:00.5'",
    ];
    assert_filters_match_native_copy(&mut conn, &table, "TS", &filters);
    for filter in filters {
        let pushed = isolated_pushdown_statement(
            &mut conn,
            &format!("SELECT ID FROM {table} WHERE {filter}"),
        );
        assert!(
            pushed.contains(SCAN_SCRIPT_NAME) && pushed.contains("\"filter\":\""),
            "{filter} must stay in the scan's WHERE clause: {pushed}"
        );
    }
}

/// Scenario: DATE_TRUNC translates to the DataFusion date_trunc call
/// Scenario: A timestamp filter compares at full precision
#[test]
fn date_trunc_and_a_date_comparison_outside_the_nanosecond_range_fail_in_the_scan() {
    setup();
    let mut conn = exa_conn();
    let qualified = qualified_probe();
    create_native_copy(&mut conn, &qualified, "TS");

    // ADD_DAYS moves each value to about 9964, which a column, unlike a literal, does not fold.
    let row_cases = [
        "SELECT ID, DATE_TRUNC('day', ADD_DAYS(TS, 2900000)) FROM {T}",
        "SELECT ID, DATE_TRUNC('second', ADD_DAYS(TS, 2900000)) FROM {T}",
        "SELECT ID, ADD_DAYS(DATE_TRUNC('month', CAST(TS AS DATE)), 1) FROM {T}",
    ];
    let failures: Vec<String> = row_cases
        .iter()
        .filter_map(|query| {
            native_copy_mismatch(
                &mut conn,
                &qualified,
                NATIVE_COPY,
                query,
                PathCheck::Has("date_trunc"),
                Expect::Rows,
            )
        })
        .collect();
    assert!(
        failures.is_empty(),
        "DATE_TRUNC must run in the scan and match native:\n{}",
        failures.join("\n")
    );

    // docs/semantic-differences.md lists each: native Exasol returns rows for all of them.
    let error_cases = [
        (
            "SELECT ID, DATE_TRUNC('month', CASE WHEN ID > 0 THEN DATE '9999-12-31' END) FROM {T}",
            "to Timestamp(ns)",
        ),
        (
            "SELECT ID, DATE_TRUNC('month', ADD_DAYS(TS, 2900000)) FROM {T}",
            "out of range",
        ),
        (
            "SELECT ID FROM {T} WHERE ADD_DAYS(CAST(TS AS DATE), 2900000) > TIMESTAMP '2024-01-01 00:00:00'",
            "to Timestamp(ns)",
        ),
    ];
    for (query, message) in error_cases {
        let native = conn.query_columns(&query.replace("{T}", NATIVE_COPY));
        assert!(!native[0].is_empty(), "`{query}` must return rows natively");
        let error = error_text(&conn.try_execute(&query.replace("{T}", &qualified)), query);
        assert!(
            error.contains(message),
            "`{query}` must fail in the scan with `{message}`, got: {error}"
        );
    }
}
