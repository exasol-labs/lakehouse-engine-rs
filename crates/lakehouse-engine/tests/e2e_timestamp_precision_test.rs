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
    E2E_TSPRECISION_NAMESPACE, E2E_TSPRECISION_TABLE, TSPRECISION_COL_TS, TSPRECISION_COL_TS_NS,
    TSPRECISION_COL_TSTZ, TSPRECISION_MICROS, TSPRECISION_NANOS, seed_timestamp_precision_probe,
};
use common::stack::{
    iceberg_catalog_url, wait_for_exasol, wait_for_iceberg_catalog, wait_for_minio,
};
use common::timestamp_precision::{
    ExpectedTimestampPrecision, engine_honors_declared_precision, expected_timestamp_precision,
    live_engine_version,
};
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
///
/// Clamped at six digits: the seeded values carry no sub-microsecond digit, so every finer
/// declaration retains all of them.
fn retained_at(micros: i64, precision: u32) -> i64 {
    let step = 10i64.pow(6 - precision.min(6));
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

/// Every value of `column` as the string the WebSocket protocol rendered it as.
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

/// `values` with each rendering's trailing fractional zeros removed, so a comparison
/// asserts the VALUE and lets the protocol render whatever digit count it chose.
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

/// [`engine_honors_declared_precision`] for the `[C3]` CAST-target domain, logging why
/// a width was not exercised so a declining leg is not silently vacuous.
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

/// The `EMITS (...)` declaration list of `sql`'s scan-UDF call, parentheses balanced so a
/// `DECIMAL(20,0)` column does not truncate the clause.
fn emits_clause(sql: &str) -> String {
    const MARKER: &str = "EMITS (";
    let at = sql
        .find(MARKER)
        .unwrap_or_else(|| panic!("generated SQL declares no EMITS clause:\n{sql}"));
    let body = &sql[at + MARKER.len() - 1..];
    let mut depth = 0usize;
    for (i, c) in body.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return body[..=i].to_string();
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced EMITS clause in:\n{sql}")
}

/// PRECONDITION for every value assertion below: the scan's `EMITS` clause declares
/// `expected`, so the width under test actually reached the emit boundary. Returns the
/// generated SQL so a caller can assert further on the wrapper shape.
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
}

// ---------------------------------------------------------------------------
// Projected CAST emit widths (issue #405)
// ---------------------------------------------------------------------------
//
// The three widths a projected `CAST(ts AS TIMESTAMP(p))` reaches once
// `ExaType::Timestamp { precision }` carries the declaration to the emit
// boundary. The `Nanosecond` and `Millisecond` units are ones no emit path in
// this repo had ever fed the SLC, so those two legs are a measurement rather
// than a regression test.

/// The VS-served `schema.table` the probe is queried through.
fn qualified_probe() -> String {
    format!("{VS_NAME}.{}", served_table())
}

/// `SELECT ID, CAST(<column> AS <target>) FROM <probe> ORDER BY ID`, the shape every width below
/// projects. `ID` keeps the ordering deterministic without relying on scan order.
fn cast_projection_sql(column: &str, target: &str) -> String {
    format!(
        "SELECT ID, CAST({column} AS {target}) FROM {} ORDER BY ID",
        qualified_probe()
    )
}

/// The instant `nanos` names, rendered at nine fractional digits.
fn rendered_nanos(nanos: i64) -> String {
    chrono::DateTime::from_timestamp_nanos(nanos)
        .format("%Y-%m-%d %H:%M:%S%.9f")
        .to_string()
}

/// Scenario (type-mapping-timestamp-precision): a projected `CAST(ts_ns AS TIMESTAMP(9))` declares
/// nine digits in the scan's `EMITS` clause and emits an Arrow `Timestamp(Nanosecond, None)`
/// column the SLC accepts, carrying every seeded nanosecond digit through to Exasol.
///
/// Casts the NANOSECOND-seeded column, not `ts`: `TSPRECISION_MICROS` carries no sub-microsecond
/// digit, so casting it to `TIMESTAMP(9)` widens a value rather than carrying one and the
/// assertions below would hold just as well against a silently narrowed unit.
///
/// Guarded on the `>= 2025` arm: 8.29.13 rejects `TIMESTAMP(9)` as a CAST target outright
/// (`0A000`, decision-log.md `[C3]`).
#[test]
fn cast_to_timestamp9_emits_nanoseconds_and_keeps_every_seeded_digit() {
    setup();
    let mut conn = exa_conn();
    if !accepts_every_cast_precision(&mut conn) {
        return;
    }
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

    // The seeded instants differ ONLY below the microsecond, so this count is 2 at TIMESTAMP(9)
    // and 1 at every coarser width. TSPRECISION_MICROS cannot make that distinction, which is
    // why `ExpectedTimestampPrecision::NANOSECOND.distinct_count` is not the oracle here.
    let distinct_sql = format!(
        "SELECT COUNT(DISTINCT CAST({ts_ns_column} AS {target})) FROM {}",
        qualified_probe()
    );
    assert_eq!(
        conn.query_scalar_i64(&distinct_sql),
        2,
        "COUNT(DISTINCT) must be 2 at {target}; a smaller count under a query that raised no \
         error is this engine build ACCEPTING the declaration and silently clamping it, an \
         engine limit of the measured build rather than an SLC rejection"
    );
}

/// Scenario (type-mapping-timestamp-precision): a projected `CAST(ts AS TIMESTAMP(3))` emits an
/// Arrow `Timestamp(Millisecond, None)` column and the seeded `.000001/.000002` and
/// `.123456/.123457` pairs collapse pairwise, leaving two distinct values.
///
/// Unguarded: `p = 3` is inside `[C3]`'s CAST-target domain on both engines. Only the EMITS
/// literal is arm-selected, because 8.29.13 strips `fractionalSecondsPrecision` from the
/// pushdown echo (`[C2]`); both declarations resolve to the same `Millisecond` unit.
#[test]
fn cast_to_timestamp3_emits_milliseconds_and_collapses_the_seeded_pairs() {
    setup();
    let mut conn = exa_conn();
    let expected = ExpectedTimestampPrecision::MILLISECOND;
    let target = expected.declared_column_type;
    let projection_sql = cast_projection_sql(&TSPRECISION_COL_TS.to_uppercase(), target);

    let honors_declared_precision = engine_honors_declared_precision(&mut conn);
    let echoed_declaration = if honors_declared_precision {
        target
    } else {
        "TIMESTAMP"
    };
    let pushdown_sql = assert_emits_declares(&mut conn, &projection_sql, echoed_declaration);
    if !honors_declared_precision {
        let emits = emits_clause(&pushdown_sql);
        assert!(
            !emits.contains("TIMESTAMP("),
            "the clamped arm must declare a bare TIMESTAMP, not a parameterized one: {emits}"
        );
    }

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

/// Scenario (sql-comprehension/vs-expression-translator-cast): `TIMESTAMP(2)` is a precision
/// DataFusion's SQL frontend cannot parse, so the DataFusion-dialect renderer declines it and the
/// adapter routes the request to its qualified single-table wrapper, which computes the CAST in
/// Exasol's own dialect over the scanned rows.
///
/// The scan's `EMITS` therefore carries the RAW `ts` declaration, and the returned values equal
/// what Exasol computes natively over the same literals in the same session.
///
/// Guarded on the `>= 2025` arm: 8.29.13 rejects `TIMESTAMP(2)` as a CAST target outright
/// (`0A000`, decision-log.md `[C3]`).
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

// ---------------------------------------------------------------------------
// A genuine nanosecond SOURCE column (issue #405)
// ---------------------------------------------------------------------------

/// Scenario (type-mapping-timestamp-precision): an Iceberg `timestamp_ns` column — a v3 type,
/// seeded through the `format-version` table PROPERTY because `TableCreation::format_version` is
/// a no-op against a REST catalog — is declared at the width the engine can emit, and its
/// genuinely sub-microsecond digit survives to Exasol on the arm that can hold it.
///
/// The only end-to-end proof that a real ninth digit reaches Exasol: `TSPRECISION_MICROS`'s
/// finest gap is a whole microsecond, so `CAST(ts AS TIMESTAMP(9))` widens a value rather than
/// carrying one, while `TSPRECISION_NANOS` carries two instants that differ ONLY below the
/// microsecond.
///
/// Both arms assert. On `>= 2025` the column is declared `TIMESTAMP(9)` and both instants
/// survive. On `< 2025` the clamp declares it bare `TIMESTAMP`, reported as `TIMESTAMP(3)`
/// (decision-log.md `[C1]`), and the two collapse to ONE.
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

    let declared = declared_type(&mut conn, &table, &ts_ns_column);
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
