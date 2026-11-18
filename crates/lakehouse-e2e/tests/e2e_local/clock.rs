//! Clock functions and `TIMESTAMP WITH LOCAL TIME ZONE` literals folded by the adapter. A clock
//! value is compared inside Exasol with the same statement's native value, within 60 seconds.

use std::collections::BTreeSet;
use std::sync::OnceLock;

use crate::common::e2e_harness::{
    VsProps, create_schema_and_scripts_with_env, create_virtual_schema, exa_conn,
    explain_virtual_sql, parse_numeric, pushdown_statements, runtime,
};
use crate::common::exasol_ws::{ExaConn, error_text, parse_int};
use crate::common::seed::{
    CLOCK_ROWS, E2E_CLOCK_NAMESPACE, E2E_CLOCK_TABLE, E2E_CLOCK_TABLE_2, seed_clock_probe,
};
use crate::common::stack::{ExasolFlavor, exasol_flavor, iceberg_catalog_url};
use crate::common::timestamp_precision::engine_honors_declared_precision;
use crate::fixtures::{explain_virtual_messages, explain_virtual_pushdown_sql, stack, vs_table};

const VS: &str = "CLOCK_VS";
const ORACLE: &str = "CLOCK_ORACLE.CLOCK_PROBE";
/// The fixture's rows with `TZ` a `TIMESTAMP WITH LOCAL TIME ZONE` holding the same instants.
const INSTANT_ORACLE: &str = "CLOCK_ORACLE.CLOCK_PROBE_INSTANTS";
const CLOCK_FUNCTIONS: [&str; 5] = [
    "CURRENT_DATE",
    "CURRENT_TIMESTAMP",
    "LOCALTIMESTAMP",
    "SYSDATE",
    "SYSTIMESTAMP",
];
const DATE_FUNCTIONS: [&str; 2] = ["CURRENT_DATE", "SYSDATE"];
const TOLERANCE_SECONDS: f64 = 60.0;
const TOKYO: &str = "ASIA/TOKYO";
const BERLIN: &str = "EUROPE/BERLIN";
/// UTC+14 all year, so a value compared in it is a day off a UTC or European wall clock.
const KIRITIMATI: &str = "PACIFIC/KIRITIMATI";
/// The adapter's error for a clock value it cannot fold in the `TZ` zone.
const TZ_REFUSAL: &str = "cannot fold a clock function or TIMESTAMP WITH LOCAL TIME ZONE literal in the adapter's TZ zone";
/// The TSTZ literal of the fixture's boundary, 10:00 in the session zone.
const TSTZ_BOUNDARY: &str =
    "CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)";

fn setup() -> ExaConn {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        stack();
        runtime()
            .block_on(seed_clock_probe(&iceberg_catalog_url(), "s3://warehouse/"))
            .unwrap_or_else(|e| panic!("seed clock_probe: {e:?}"));
        let mut conn = exa_conn();
        conn.execute("CREATE SCHEMA IF NOT EXISTS CLOCK_ORACLE");
        conn.execute(&format!(
            "CREATE OR REPLACE TABLE {ORACLE} \
             (ID DECIMAL(18,0), D DATE, TS TIMESTAMP, TZ TIMESTAMP, REGION VARCHAR(10))"
        ));
        let rows: Vec<String> = CLOCK_ROWS
            .iter()
            .map(|(id, d, ts, tz, region)| {
                format!("({id}, DATE '{d}', TIMESTAMP '{ts}', TIMESTAMP '{tz}', '{region}')")
            })
            .collect();
        conn.execute(&format!("INSERT INTO {ORACLE} VALUES {}", rows.join(", ")));
        conn.execute(&format!(
            "CREATE OR REPLACE TABLE {INSTANT_ORACLE} \
             (ID DECIMAL(18,0), D DATE, TS TIMESTAMP, TZ TIMESTAMP WITH LOCAL TIME ZONE, \
             REGION VARCHAR(10))"
        ));
        // A plain timestamp converts to TSTZ as session-local, so UTC keeps each instant.
        set_session_zone(&mut conn, "UTC");
        conn.execute(&format!(
            "INSERT INTO {INSTANT_ORACLE} SELECT * FROM {ORACLE}"
        ));
        create_virtual_schema(&mut conn, &VsProps::new(VS, E2E_CLOCK_NAMESPACE));
    });
    exa_conn()
}

fn table(vs: &str) -> String {
    vs_table(vs, E2E_CLOCK_TABLE)
}

fn create_clock_vs(conn: &mut ExaConn, props: VsProps) {
    create_virtual_schema(conn, &props);
}

fn set_session_zone(conn: &mut ExaConn, zone: &str) {
    conn.execute(&format!("ALTER SESSION SET TIME_ZONE = '{zone}'"));
}

fn db_timezone(conn: &mut ExaConn) -> String {
    conn.query_columns("SELECT DBTIMEZONE")[0][0]
        .as_str()
        .expect("DBTIMEZONE is text")
        .to_string()
}

/// Pins the session zone to `DBTIMEZONE`. On docker-db it is no UTC zone, so a value folded in
/// UTC differs from the native one. Nano runs with `sysTZ=UTC` (`docker-compose.nano.yml`).
fn session_in_db_timezone(conn: &mut ExaConn) {
    let zone = db_timezone(conn);
    let utc_by_workaround = exasol_flavor() == ExasolFlavor::Nano;
    assert_eq!(
        zone == "UTC",
        utc_by_workaround,
        "fixture precondition: DBTIMEZONE is UTC exactly on Nano, is {zone}"
    );
    set_session_zone(conn, &zone);
}

fn ids(conn: &mut ExaConn, sql: &str) -> Vec<i64> {
    let columns = conn.query_columns(sql);
    let mut ids: Vec<i64> = columns
        .first()
        .map(|column| column.iter().map(parse_int).collect())
        .unwrap_or_default();
    ids.sort_unstable();
    ids
}

/// The ids `predicate` keeps through `vs`, asserted equal to the native oracle's, whose `TZ`
/// holds the UTC dates and times of the instants.
#[track_caller]
fn assert_native_rows(conn: &mut ExaConn, vs: &str, predicate: &str) -> Vec<i64> {
    assert_oracle_rows(conn, vs, ORACLE, predicate)
}

/// [`assert_native_rows`] against the oracle whose `TZ` holds the instants.
#[track_caller]
fn assert_instant_rows(conn: &mut ExaConn, vs: &str, predicate: &str) -> Vec<i64> {
    assert_oracle_rows(conn, vs, INSTANT_ORACLE, predicate)
}

#[track_caller]
fn assert_oracle_rows(conn: &mut ExaConn, vs: &str, oracle: &str, predicate: &str) -> Vec<i64> {
    let through_vs = ids(
        conn,
        &format!("SELECT ID FROM {} WHERE {predicate}", table(vs)),
    );
    let native = ids(conn, &format!("SELECT ID FROM {oracle} WHERE {predicate}"));
    assert_eq!(
        through_vs, native,
        "{vs} `{predicate}` must return the rows of {oracle}"
    );
    through_vs
}

/// Seconds between `native` evaluated by Exasol and `function` projected through `vs` in one
/// statement; a date function's difference counts in whole days.
fn seconds_off(conn: &mut ExaConn, vs: &str, function: &str, native: &str) -> f64 {
    let projected = format!("(SELECT {function} FROM {} WHERE ID = 1)", table(vs));
    let sql = if DATE_FUNCTIONS.contains(&function) {
        format!("SELECT ABS(DAYS_BETWEEN({native}, {projected})) * 86400")
    } else {
        format!("SELECT ABS(SECONDS_BETWEEN({native}, {projected}))")
    };
    parse_numeric(&conn.query_columns(&sql)[0][0])
}

#[track_caller]
fn assert_matches_native(conn: &mut ExaConn, vs: &str, function: &str, native: &str) {
    let off = seconds_off(conn, vs, function, native);
    assert!(
        off <= TOLERANCE_SECONDS,
        "{function} through {vs} must be within {TOLERANCE_SECONDS}s of native {native}, \
         is {off}s off"
    );
}

/// Whether native Exasol types `LOCALTIMESTAMP` `TIMESTAMP WITH LOCAL TIME ZONE` (Exasol 8)
/// rather than `TIMESTAMP` (Exasol 2025), read from a native table's declared column.
fn localtimestamp_is_tstz(conn: &mut ExaConn) -> bool {
    conn.execute(
        "CREATE OR REPLACE TABLE CLOCK_ORACLE.LOCALTIMESTAMP_TYPE AS SELECT LOCALTIMESTAMP L",
    );
    let declared = conn.query_columns(
        "SELECT COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS WHERE COLUMN_SCHEMA = 'CLOCK_ORACLE' \
         AND COLUMN_TABLE = 'LOCALTIMESTAMP_TYPE'",
    )[0][0]
        .as_str()
        .expect("COLUMN_TYPE is text")
        .to_string();
    declared.contains("WITH LOCAL TIME ZONE")
}

/// Every `YYYY-MM-DD HH:MI:SS.FFF` literal of `sql` outside the fixture's 2024 and 2099 rows.
fn folded_timestamps(sql: &str) -> BTreeSet<String> {
    let bytes = sql.as_bytes();
    let shape = |window: &[u8]| {
        window.iter().enumerate().all(|(i, b)| match i {
            4 | 7 => *b == b'-',
            10 => *b == b' ',
            13 | 16 => *b == b':',
            19 => *b == b'.',
            _ => b.is_ascii_digit(),
        })
    };
    bytes
        .windows(23)
        .filter(|window| shape(window))
        .map(|window| String::from_utf8_lossy(window).to_string())
        .filter(|literal| !literal.starts_with("2024-") && !literal.starts_with("2099-"))
        .collect()
}

#[track_caller]
fn assert_names_no_clock_function(pushed: &str, context: &str) {
    let upper = pushed.to_uppercase();
    for function in CLOCK_FUNCTIONS {
        assert!(
            !upper.contains(function),
            "{context}: the pushed SQL must name no {function}: {pushed}"
        );
    }
}

fn advertised(conn: &mut ExaConn, vs: &str) -> Vec<String> {
    let query = format!("SELECT ID FROM {}", table(vs));
    explain_virtual_messages(conn, &query)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("EXPLAIN VIRTUAL echoed no pushdown for {query}"))
        .iter()
        .filter(|m| m["type"] == "getCapabilities")
        .find_map(|m| m["capabilities"].as_array().cloned())
        .unwrap_or_else(|| panic!("{query} echoed no getCapabilities answer"))
        .iter()
        .filter_map(|c| c.as_str().map(str::to_string))
        .collect()
}

/// Scenario: The five clock functions are advertised
#[test]
fn clock_functions_are_advertised_and_withheld_by_exclusion() {
    const EXCLUDING: &str = "CLOCK_VS_EXCLUDED";
    let mut conn = setup();
    let capabilities = advertised(&mut conn, VS);
    for function in CLOCK_FUNCTIONS {
        let name = format!("FN_{function}");
        assert!(capabilities.contains(&name), "{name} must be advertised");
    }
    for name in ["FN_DBTIMEZONE", "FN_SESSIONTIMEZONE"] {
        assert!(
            !capabilities.iter().any(|c| c == name),
            "{name} must not be advertised"
        );
    }

    let excluded = CLOCK_FUNCTIONS.map(|f| format!("FN_{f}")).join(",");
    create_clock_vs(
        &mut conn,
        VsProps::new(EXCLUDING, E2E_CLOCK_NAMESPACE)
            .with_property("EXCLUDED_CAPABILITIES", &excluded),
    );
    let withheld = advertised(&mut conn, EXCLUDING);
    for function in CLOCK_FUNCTIONS {
        let name = format!("FN_{function}");
        assert!(!withheld.contains(&name), "{name} must be withheld");
    }
    session_in_db_timezone(&mut conn);
    for predicate in [
        "TS < SYSTIMESTAMP",
        "D <= CURRENT_DATE",
        "TS < CURRENT_TIMESTAMP",
    ] {
        assert_eq!(
            assert_native_rows(&mut conn, EXCLUDING, predicate),
            [1, 2, 3]
        );
    }
}

/// Scenario: A clock function reaches the pushed SQL as a literal
#[test]
fn each_clock_function_reaches_the_pushed_sql_as_a_literal() {
    let mut conn = setup();
    session_in_db_timezone(&mut conn);
    let cases = [
        ("D <= CURRENT_DATE", false),
        ("D <= SYSDATE", false),
        ("TS < LOCALTIMESTAMP", true),
        ("TS < SYSTIMESTAMP", true),
        ("TS < CURRENT_TIMESTAMP", true),
    ];
    for (predicate, timestamp) in cases {
        let query = format!("SELECT ID FROM {} WHERE {predicate}", table(VS));
        let pushed = explain_virtual_pushdown_sql(&mut conn, &query);
        assert_names_no_clock_function(&pushed, predicate);
        if timestamp {
            assert_eq!(
                folded_timestamps(&pushed).len(),
                1,
                "{predicate}: the scan filter must hold one millisecond timestamp literal: {pushed}"
            );
        } else {
            let today = conn.query_columns("SELECT TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')")[0][0]
                .as_str()
                .expect("a date text")
                .to_string();
            // The scan spec is an SQL string literal, which doubles each quote.
            assert!(
                pushed.contains(&format!("DATE ''{today}''")),
                "{predicate}: the scan filter must hold today's DATE literal: {pushed}"
            );
        }
        assert_eq!(assert_native_rows(&mut conn, VS, predicate), [1, 2, 3]);
    }
}

/// Scenario: A TIMESTAMP WITH LOCAL TIME ZONE literal in a comparison runs in the scan
#[test]
fn a_tstz_literal_comparison_runs_in_the_scan() {
    let mut conn = setup();
    // In a session whose zone is the folding zone, the literal folds to its own wall clock.
    session_in_db_timezone(&mut conn);
    let predicate = format!("TS > {TSTZ_BOUNDARY}");
    let pushed = explain_virtual_pushdown_sql(
        &mut conn,
        &format!("SELECT ID FROM {} WHERE {predicate}", table(VS)),
    );
    assert!(
        pushed.contains("2024-03-01 10:00:00.000") && !pushed.contains("SESSIONTIMEZONE"),
        "{predicate}: the scan filter must compare with the folding zone's wall clock: {pushed}"
    );
    assert_eq!(assert_native_rows(&mut conn, VS, &predicate), [3, 4]);
    let recent = "TS > ADD_HOURS(CURRENT_TIMESTAMP, -1)";
    let pushed = explain_virtual_pushdown_sql(
        &mut conn,
        &format!("SELECT ID FROM {} WHERE {recent}", table(VS)),
    );
    assert!(!pushed.contains("SESSIONTIMEZONE"), "{recent}: {pushed}");
    assert_eq!(folded_timestamps(&pushed).len(), 1, "{recent}: {pushed}");
    assert_eq!(assert_native_rows(&mut conn, VS, recent), [4]);
}

/// Scenario: The clock functions fold in the zone that Exasol uses when the session zone matches
#[test]
fn clock_functions_match_native_when_the_session_zone_matches() {
    let mut conn = setup();
    session_in_db_timezone(&mut conn);
    for function in CLOCK_FUNCTIONS {
        assert_matches_native(&mut conn, VS, function, function);
        for column in ["D", "TS"] {
            assert_native_rows(&mut conn, VS, &format!("{column} <= {function}"));
        }
    }
    let column_type = |conn: &mut ExaConn, sql: &str| {
        conn.execute(sql)["responseData"]["results"][0]["resultSet"]["columns"][0]["dataType"]
            ["type"]
            .clone()
    };
    let through_vs = column_type(
        &mut conn,
        &format!("SELECT CURRENT_TIMESTAMP FROM {}", table(VS)),
    );
    assert_eq!(through_vs, "TIMESTAMP WITH LOCAL TIME ZONE");
    assert_eq!(
        through_vs,
        column_type(&mut conn, "SELECT CURRENT_TIMESTAMP")
    );
}

/// Scenario: A TIMESTAMP WITH LOCAL TIME ZONE value compared with an instant column compares the instants
#[test]
fn a_tstz_value_compared_with_an_instant_column_compares_the_instants() {
    const INSTANT_VS: &str = "CLOCK_VS_INSTANTS";
    // 23:30 on March 1st in Kiritimati is 09:30 UTC, row 2's instant.
    const ROW_2_IN_KIRITIMATI: &str =
        "CAST(TIMESTAMP '2024-03-01 23:30:00' AS TIMESTAMP WITH LOCAL TIME ZONE)";
    let mut conn = setup();
    let local_is_tstz = localtimestamp_is_tstz(&mut conn);
    create_clock_vs(
        &mut conn,
        VsProps::new(INSTANT_VS, E2E_CLOCK_NAMESPACE).with_property("SESSION_TIME_ZONE", BERLIN),
    );
    let mut predicates = vec![
        format!("TZ > {TSTZ_BOUNDARY}"),
        "TZ > ADD_HOURS(CURRENT_TIMESTAMP, -1)".to_string(),
        format!("TZ BETWEEN {TSTZ_BOUNDARY} AND CURRENT_TIMESTAMP"),
        format!("TZ IN ({TSTZ_BOUNDARY})"),
        format!("TZ IN ({ROW_2_IN_KIRITIMATI})"),
        format!("TZ <= {ROW_2_IN_KIRITIMATI}"),
    ];
    if local_is_tstz {
        predicates.push("TZ < LOCALTIMESTAMP".to_string());
    }
    let projected = |relation: &str| {
        format!(
            "SELECT CASE WHEN TZ < CURRENT_TIMESTAMP THEN 1 ELSE 0 END FROM {relation} \
             ORDER BY ID"
        )
    };

    set_session_zone(&mut conn, KIRITIMATI);
    let first = format!(
        "SELECT ID FROM {} WHERE {}",
        table(INSTANT_VS),
        predicates[0]
    );
    let pushed = explain_virtual_pushdown_sql(&mut conn, &first);
    assert!(
        pushed.contains("2024-02-29 20:00:00.000") && !pushed.contains("SESSIONTIMEZONE"),
        "{first}: the scan filter must compare with the instant's UTC date and time: {pushed}"
    );
    assert_eq!(
        assert_instant_rows(
            &mut conn,
            INSTANT_VS,
            &format!("TZ <= {ROW_2_IN_KIRITIMATI}")
        ),
        [1, 2],
        "fixture precondition: a Berlin wall clock would keep row 3 too"
    );
    // The fixture's rows lie far from now, so only the pushed literal shows the clock's fold.
    let mut clocks = vec!["CURRENT_TIMESTAMP"];
    if local_is_tstz {
        clocks.push("LOCALTIMESTAMP");
    }
    for function in clocks {
        let query = format!("SELECT ID FROM {} WHERE TZ < {function}", table(INSTANT_VS));
        let pushed = explain_virtual_pushdown_sql(&mut conn, &query);
        let folded: Vec<String> = folded_timestamps(&pushed).into_iter().collect();
        let [folded] = folded.as_slice() else {
            panic!("{query} must fold to one timestamp literal: {pushed}");
        };
        let off = parse_numeric(
            &conn.query_columns(&format!(
                "SELECT ABS(SECONDS_BETWEEN(TIMESTAMP '{folded}', \
                 CONVERT_TZ(SYSTIMESTAMP, DBTIMEZONE, 'UTC')))"
            ))[0][0],
        );
        assert!(
            off <= TOLERANCE_SECONDS,
            "{query} must fold to the UTC date and time of now, {folded} is {off}s off"
        );
    }

    for (folding_zone, session_zone) in [(BERLIN, KIRITIMATI), (TOKYO, KIRITIMATI), (TOKYO, "UTC")]
    {
        conn.execute(&format!(
            "ALTER VIRTUAL SCHEMA {INSTANT_VS} SET SESSION_TIME_ZONE = '{folding_zone}'"
        ));
        set_session_zone(&mut conn, session_zone);
        for predicate in &predicates {
            assert_instant_rows(&mut conn, INSTANT_VS, predicate);
        }
        let through_vs = conn.query_columns(&projected(&table(INSTANT_VS)));
        let native = conn.query_columns(&projected(INSTANT_ORACLE));
        assert_eq!(
            through_vs, native,
            "{folding_zone} folding zone, {session_zone} session: the projected comparison"
        );
    }
}

/// Scenario: A plain value compared with an instant column compares with its UTC date and time
#[test]
fn a_plain_value_compared_with_an_instant_column_compares_its_utc_date_and_time() {
    const PLAIN_VS: &str = "CLOCK_VS_PLAIN_VALUES";
    let mut conn = setup();
    let local_is_tstz = localtimestamp_is_tstz(&mut conn);
    create_clock_vs(
        &mut conn,
        VsProps::new(PLAIN_VS, E2E_CLOCK_NAMESPACE).with_property("SESSION_TIME_ZONE", BERLIN),
    );
    set_session_zone(&mut conn, BERLIN);
    let boundary = "TZ > TIMESTAMP '2024-03-01 10:00:00'";
    assert_eq!(
        assert_native_rows(&mut conn, PLAIN_VS, boundary),
        [3, 4],
        "{boundary}: a session-local reading of TZ would keep row 2 too"
    );
    let mut predicates = vec![
        "TZ >= DATE '2024-03-01'",
        "TZ <= SYSTIMESTAMP",
        "TZ >= CURRENT_DATE",
    ];
    if !local_is_tstz {
        predicates.push("TZ <= LOCALTIMESTAMP");
    }
    for predicate in predicates {
        assert_native_rows(&mut conn, PLAIN_VS, predicate);
    }
}

/// Scenario: The clock functions fold in the zone that Exasol uses when the session zone matches
#[test]
fn a_projected_tstz_value_keeps_its_fractional_digits() {
    let mut conn = setup();
    session_in_db_timezone(&mut conn);
    let mut precisions = vec![3];
    if engine_honors_declared_precision(&mut conn) {
        precisions.extend([0, 6, 9]);
    }
    for precision in precisions {
        let value = format!(
            "CAST(TIMESTAMP '2024-03-01 10:00:00.245223789' AS TIMESTAMP({precision}) \
             WITH LOCAL TIME ZONE)"
        );
        let through_vs =
            conn.query_columns(&format!("SELECT {value} FROM {} WHERE ID = 1", table(VS)));
        let native = conn.query_columns(&format!("SELECT {value}"));
        assert_eq!(through_vs, native, "{value} through {VS}");
    }
}

/// Scenario: SESSION_TIME_ZONE sets the zone of the session-zone functions
#[test]
fn session_time_zone_sets_the_zone_of_the_session_zone_functions() {
    const TOKYO_VS: &str = "CLOCK_VS_TOKYO";
    let mut conn = setup();
    assert_ne!(db_timezone(&mut conn), TOKYO, "fixture precondition");
    create_clock_vs(
        &mut conn,
        VsProps::new(TOKYO_VS, E2E_CLOCK_NAMESPACE).with_property("SESSION_TIME_ZONE", TOKYO),
    );
    set_session_zone(&mut conn, TOKYO);
    for function in CLOCK_FUNCTIONS {
        assert_matches_native(&mut conn, TOKYO_VS, function, function);
    }
}

/// Scenario: A session zone that differs from the folding zone gets the folding zone's values
#[test]
fn a_differing_session_zone_gets_the_folding_zone_values() {
    const SET_VS: &str = "CLOCK_VS_SET";
    let mut conn = setup();
    assert_ne!(db_timezone(&mut conn), TOKYO, "fixture precondition");
    let local_is_tstz = localtimestamp_is_tstz(&mut conn);
    create_clock_vs(&mut conn, VsProps::new(SET_VS, E2E_CLOCK_NAMESPACE));
    set_session_zone(&mut conn, TOKYO);
    let count = |conn: &mut ExaConn| {
        conn.query_scalar_i64(&format!(
            "SELECT COUNT(*) FROM {} WHERE TS > {TSTZ_BOUNDARY}",
            table(SET_VS)
        ))
    };

    // SYSDATE and SYSTIMESTAMP are the native DBTIMEZONE values.
    assert_matches_native(&mut conn, SET_VS, "CURRENT_DATE", "SYSDATE");
    if local_is_tstz {
        assert_matches_native(&mut conn, SET_VS, "LOCALTIMESTAMP", "LOCALTIMESTAMP");
    } else {
        assert_matches_native(&mut conn, SET_VS, "LOCALTIMESTAMP", "SYSTIMESTAMP");
        let off = seconds_off(&mut conn, SET_VS, "LOCALTIMESTAMP", "LOCALTIMESTAMP");
        assert!(
            off >= 3600.0,
            "LOCALTIMESTAMP must not be the Tokyo wall clock, {off}s off"
        );
    }
    assert_matches_native(&mut conn, SET_VS, "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP");
    // In this Tokyo session the literal is 01:00 UTC, 01:00 or 02:00 in DBTIMEZONE, so every
    // row's TS lies after it; Tokyo's 10:00 keeps two.
    assert_eq!(
        count(&mut conn),
        4,
        "the literal must compare as the DBTIMEZONE wall clock"
    );

    conn.execute(&format!(
        "ALTER VIRTUAL SCHEMA {SET_VS} SET SESSION_TIME_ZONE = '{TOKYO}'"
    ));
    assert_matches_native(&mut conn, SET_VS, "CURRENT_DATE", "CURRENT_DATE");
    let native = conn.query_scalar_i64(&format!(
        "SELECT COUNT(*) FROM {ORACLE} WHERE TS > {TSTZ_BOUNDARY}"
    ));
    assert_eq!(
        native, 2,
        "fixture precondition: Tokyo's 10:00 keeps rows 3 and 4"
    );
    assert_eq!(
        count(&mut conn),
        native,
        "after SET SESSION_TIME_ZONE the literal compares in Tokyo"
    );
}

/// Scenario: A projected clock function keeps the aggregate split
#[test]
fn a_projected_clock_function_keeps_the_aggregate_split() {
    let mut conn = setup();
    session_in_db_timezone(&mut conn);
    let t = table(VS);
    let cases = [
        (
            format!(
                "SELECT CURRENT_TIMESTAMP AS RUN_AT, REGION, COUNT(*) AS N FROM {t} GROUP BY REGION"
            ),
            format!("SELECT REGION, COUNT(*) AS N FROM {t} GROUP BY REGION"),
            "SELECT REGION, N, ABS(SECONDS_BETWEEN(RUN_AT, CURRENT_TIMESTAMP)) <= 60 FROM ({query}) \
             ORDER BY REGION",
            vec![vec!["eu", "2", "true"], vec!["us", "2", "true"]],
        ),
        (
            format!("SELECT SYSDATE AS RUN_AT, COUNT(*) AS N FROM {t}"),
            format!("SELECT COUNT(*) AS N FROM {t}"),
            "SELECT N, RUN_AT = SYSDATE FROM ({query})",
            vec![vec!["4", "true"]],
        ),
    ];
    for (query, without_clock, check, expected) in cases {
        let partial = |pushed: &str| pushed.contains("PARTIAL_");
        let pushed = explain_virtual_sql(&mut conn, &query);
        let plain = explain_virtual_sql(&mut conn, &without_clock);
        assert!(
            partial(&plain),
            "fixture precondition, {without_clock}: {plain}"
        );
        assert!(
            partial(&pushed),
            "{query} must aggregate per shard: {pushed}"
        );
        assert_eq!(
            pushed.contains("\"group_keys\":["),
            plain.contains("\"group_keys\":["),
            "{query}: {pushed}"
        );
        let columns = conn.query_columns(&check.replace("{query}", &query));
        let rows: Vec<Vec<String>> = (0..columns[0].len())
            .map(|row| {
                columns
                    .iter()
                    .map(|column| match &column[row] {
                        serde_json::Value::String(text) => text.clone(),
                        other => other.to_string(),
                    })
                    .collect()
            })
            .collect();
        assert_eq!(rows, expected, "{query}");
    }
}

/// Scenario: Every clock function of a statement folds from one reading
#[test]
fn every_clock_function_of_a_statement_folds_from_one_reading() {
    let mut conn = setup();
    let (t1, t2) = (table(VS), vs_table(VS, E2E_CLOCK_TABLE_2));
    let join = format!(
        "SELECT a.ID FROM {t1} a JOIN {t2} b ON a.ID = b.ID \
         WHERE a.TS < SYSTIMESTAMP AND b.TS < SYSTIMESTAMP"
    );
    let union =
        format!("SELECT SYSTIMESTAMP AS X FROM {t1} UNION ALL SELECT SYSTIMESTAMP FROM {t2}");
    for statement in [&join, &union] {
        let literals: BTreeSet<String> = pushdown_statements(&mut conn, statement)
            .iter()
            .flat_map(|pushed| folded_timestamps(pushed))
            .collect();
        assert_eq!(
            literals.len(),
            1,
            "{statement} must fold one instant: {literals:?}"
        );
    }
    // Nano's SQL process aborts on an aggregate over a UNION ALL of virtual tables, so the rows
    // are compared here.
    let values: BTreeSet<String> = conn.query_columns(&union)[0]
        .iter()
        .map(ToString::to_string)
        .collect();
    assert_eq!(values.len(), 1, "{union} must return one value: {values:?}");
}

/// Scenario: A numeric database time zone folds at its offset
#[test]
fn a_numeric_tz_folds_at_its_offset() {
    const SCHEMA: &str = "CLOCK_TZ_NUMERIC";
    const NUMERIC_VS: &str = "CLOCK_VS_TZ_NUMERIC";
    let mut conn = setup();
    create_schema_and_scripts_with_env(&mut conn, SCHEMA, &[("TZ", "UTC-7")], &[]);
    create_clock_vs(
        &mut conn,
        VsProps::new(NUMERIC_VS, E2E_CLOCK_NAMESPACE).with_adapter_schema(SCHEMA),
    );
    assert_matches_native(
        &mut conn,
        NUMERIC_VS,
        "SYSTIMESTAMP",
        "ADD_HOURS(CONVERT_TZ(SYSTIMESTAMP, DBTIMEZONE, 'UTC'), 7)",
    );
}

/// Scenario: An unreadable TZ fails only a query that needs it
#[test]
fn an_unreadable_tz_fails_only_a_query_that_needs_it() {
    let mut conn = setup();
    let cases = [
        (
            "CLOCK_TZ_BROKEN",
            "CLOCK_VS_TZ_BROKEN",
            "NOT/A_ZONE",
            "TZ 'NOT/A_ZONE'",
        ),
        ("CLOCK_TZ_EMPTY", "CLOCK_VS_TZ_EMPTY", "", "TZ ''"),
    ];
    for (schema, vs, tz, named) in cases {
        create_schema_and_scripts_with_env(&mut conn, schema, &[("TZ", tz)], &[]);
        create_clock_vs(
            &mut conn,
            VsProps::new(vs, E2E_CLOCK_NAMESPACE).with_adapter_schema(schema),
        );
        let t = table(vs);
        let sysdate = format!("SELECT ID FROM {t} WHERE D <= SYSDATE");
        let tstz = format!("SELECT ID FROM {t} WHERE TS > {TSTZ_BOUNDARY}");
        let session_clock = format!("SELECT ID FROM {t} WHERE D <= CURRENT_DATE");
        for sql in [&sysdate, &tstz] {
            let error = error_text(&conn.try_execute(sql), sql);
            assert!(
                error.contains(TZ_REFUSAL) && error.contains(named),
                "{sql} must fail naming {named}: {error}"
            );
        }
        assert_eq!(
            ids(&mut conn, &format!("SELECT ID FROM {t} WHERE ID = 1")),
            [1]
        );
        set_session_zone(&mut conn, "UTC");
        assert_eq!(
            assert_instant_rows(&mut conn, vs, &format!("TZ > {TSTZ_BOUNDARY}")),
            [3, 4],
            "the instant comparison reads no TZ"
        );

        conn.execute(&format!(
            "ALTER VIRTUAL SCHEMA {vs} SET SESSION_TIME_ZONE = 'EUROPE/BERLIN'"
        ));
        set_session_zone(&mut conn, "EUROPE/BERLIN");
        assert_eq!(
            ids(&mut conn, &tstz).len(),
            2,
            "{tstz} must run once SESSION_TIME_ZONE is set"
        );
        assert_eq!(ids(&mut conn, &session_clock), [1, 2, 3]);
        let error = error_text(&conn.try_execute(&sysdate), &sysdate);
        assert!(
            error.contains(TZ_REFUSAL) && error.contains(named),
            "{sysdate} must still fail naming {named}: {error}"
        );
    }
}
