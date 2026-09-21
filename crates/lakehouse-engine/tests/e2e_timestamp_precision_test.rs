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
    TSPRECISION_COL_TSTZ, TSPRECISION_MICROS, seed_timestamp_precision_probe,
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

/// `values` with each rendering's trailing fractional zeros removed.
///
/// The module note above records the WebSocket protocol rendering six fractional digits for a
/// `TIMESTAMP(3)` column; how many it renders for a `TIMESTAMP(9)` one is not recorded anywhere
/// and is not what these tests pin. Normalising both sides of a comparison keeps the assertion on
/// the VALUE and lets the digit count be whatever the engine chose.
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

/// [`engine_honors_declared_precision`] for the `[C3]` CAST-target domain, reporting on stderr
/// when it declines so a leg's log records WHY a width was not exercised there rather than
/// leaving the test silently vacuous.
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

/// PRECONDITION for every value assertion below: the scan's `EMITS` clause declares `expected`,
/// so the width under test actually reached the emit boundary.
///
/// Asserted BEFORE any value is compared, because a failure here has two causes and neither is
/// the SLC rejecting an Arrow unit — the outcome this plan exists to measure. Reported as a
/// returned string rather than an `assert_eq!` on the whole statement so the wrapper shape,
/// which carries its own declarations, stays readable.
fn assert_emits_declares(conn: &mut ExaConn, query_sql: &str, expected: &str) -> String {
    let pushdown_sql = isolated_pushdown_statement(conn, query_sql);
    let emits = emits_clause(&pushdown_sql);
    assert!(
        emits.contains(expected),
        "the scan must declare {expected} in its EMITS clause, got {emits}. Two causes produce \
         this, NEITHER of them an SLC rejection of the Arrow unit: (1) this engine build stripped \
         `fractionalSecondsPrecision` from the pushdown echo, the behaviour decision-log.md [C2] \
         captured on 8.29.13 and this run measures on its own build; (2) the engine rejected the \
         `TIMESTAMP(p)` CAST target as `0A000 Feature not supported`, the rejection [C3] records \
         on 8.29.13 for every p outside {{3, 6}}.\ngenerated SQL: {pushdown_sql}"
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
// boundary. (a) and (b) feed the SLC's strict Arrow-IPC block a `Nanosecond`
// and a `Millisecond` unit — units no emit path in this repo had ever fed it,
// which is what makes them a measurement rather than a regression test. (c)
// pins the declined width, computed by Exasol in the adapter's own wrapper.

/// The VS-served `schema.table` the probe is queried through.
fn qualified_probe() -> String {
    format!("{VS_NAME}.{}", served_table())
}

/// `SELECT ID, CAST(ts AS <target>) FROM <probe> ORDER BY ID` — the shape every width below
/// projects. `ID` keeps the ordering deterministic without relying on scan order.
fn cast_projection_sql(target: &str) -> String {
    format!(
        "SELECT ID, CAST({} AS {target}) FROM {} ORDER BY ID",
        TSPRECISION_COL_TS.to_uppercase(),
        qualified_probe()
    )
}

/// Scenario (type-mapping-timestamp-precision): a projected `CAST(ts AS TIMESTAMP(9))` declares
/// nine digits in the scan's `EMITS` clause and emits an Arrow `Timestamp(Nanosecond, None)`
/// column, which the SLC accepts and the engine honors — every seeded value survives and all four
/// stay distinct.
///
/// Guarded on the `>= 2025` arm: 8.29.13 rejects `TIMESTAMP(9)` as a CAST target outright
/// (`0A000`, decision-log.md `[C3]`), before any pushdown happens.
#[test]
fn cast_to_timestamp9_emits_nanoseconds_and_keeps_every_seeded_value() {
    setup();
    let mut conn = exa_conn();
    if !accepts_every_cast_precision(&mut conn) {
        return;
    }
    let expected = ExpectedTimestampPrecision::NANOSECOND;
    let target = expected.declared_column_type;
    let projection_sql = cast_projection_sql(target);

    assert_emits_declares(&mut conn, &projection_sql, target);

    let projected = conn.query_columns(&projection_sql);
    let actual = without_trailing_fraction_zeros(rendered_column(&projected[1], target));
    let want =
        without_trailing_fraction_zeros(TSPRECISION_MICROS.iter().copied().map(rendered).collect());
    assert_eq!(
        actual, want,
        "every seeded value must survive a {target} emit unchanged — a differing value means the \
         nanosecond Arrow unit lost digits at the emit boundary, not that the SLC rejected it"
    );
    assert_pushed_to_scan_udf(
        &mut conn,
        &projection_sql,
        "the TIMESTAMP(9) cast projection",
    );

    let distinct_sql = format!(
        "SELECT COUNT(DISTINCT CAST({} AS {target})) FROM {}",
        TSPRECISION_COL_TS.to_uppercase(),
        qualified_probe()
    );
    assert_eq!(
        conn.query_scalar_i64(&distinct_sql),
        expected.distinct_count,
        "COUNT(DISTINCT) must be {} at {target}; a smaller count under a query that raised no \
         error is this engine build ACCEPTING the declaration and silently clamping it — an \
         engine limit of the measured build, not an SLC rejection",
        expected.distinct_count
    );
}

/// Scenario (type-mapping-timestamp-precision): a projected `CAST(ts AS TIMESTAMP(3))` emits an
/// Arrow `Timestamp(Millisecond, None)` column and the seeded `.000001/.000002` and
/// `.123456/.123457` pairs collapse pairwise, leaving two distinct values.
///
/// Unguarded: `p = 3` is inside `[C3]`'s CAST-target domain on both engines. The EMITS literal
/// itself IS arm-selected — `[C2]` captured 8.29.13 stripping `fractionalSecondsPrecision` from
/// the pushdown echo, which leaves the adapter declaring a bare `TIMESTAMP`. Both declarations
/// resolve to the same `Millisecond` unit, so the value assertion holds on both legs.
#[test]
fn cast_to_timestamp3_emits_milliseconds_and_collapses_the_seeded_pairs() {
    setup();
    let mut conn = exa_conn();
    let expected = ExpectedTimestampPrecision::MILLISECOND;
    let target = expected.declared_column_type;
    let projection_sql = cast_projection_sql(target);

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
/// The scan's `EMITS` therefore carries the RAW `ts` declaration and must never carry
/// `TIMESTAMP(2)`, and the returned values must equal what Exasol computes natively for the same
/// expression over the same literals in the same session.
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
    let projection_sql = cast_projection_sql(declined_target);

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
/// This is the only end-to-end proof that a real ninth digit reaches Exasol: the
/// `TSPRECISION_MICROS` fixture's finest gap is a whole microsecond, so `CAST(ts AS
/// TIMESTAMP(9))` widens a value rather than carrying one. `TSPRECISION_NANOS` carries two
/// instants that differ ONLY below the microsecond, so `COUNT(DISTINCT)` separates the arms.
///
/// Both arms assert; neither is skipped. On `>= 2025` the column is declared `TIMESTAMP(9)` and
/// both instants survive. On `< 2025` the engine clamp declares it bare `TIMESTAMP`, which
/// `SYS.EXA_ALL_COLUMNS` reports as `TIMESTAMP(3)` (decision-log.md `[C1]`), and the two collapse
/// to ONE — the six-digit loss the 8.x limitation names, measured rather than assumed.
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
