//! Text conversions under non-default session settings: each query over the virtual table must
//! match the same query over a native copy, and take the path the case names.

use crate::common::e2e_harness::*;
use crate::common::exasol_ws::ExaConn;
use crate::common::seed::{CHAR_PAD_COL, E2E_CHAR_PAD_TABLE};
use crate::fixtures::{LAKEHOUSE_VS, TYPED_COLUMNS, lakehouse_vs, typed_table, vs_table};

const DMY: &str = "NLS_DATE_FORMAT = 'DD.MM.YYYY'";
const YDM: &str = "NLS_DATE_FORMAT = 'YYYY-DD-MM'";
const TS_DMY: &str = "NLS_TIMESTAMP_FORMAT = 'DD.MM.YYYY HH24:MI:SS.FF6'";
const COMMA: &str = "NLS_NUMERIC_CHARACTERS = ',.'";
const DEFAULT: &str = "";

const FILTER_IN_SCAN: &str = r#""filter":""#;
const SPLIT: &str = "PARTIAL_";

/// `(session setting, query over {T}, marker, in_scan)`: the pushdown must contain `marker` when
/// `in_scan` holds and must not otherwise. An empty marker leaves the path unasserted.
type Case = (&'static str, &'static str, &'static str, bool);

const CASES: &[Case] = &[
    (
        DMY,
        "SELECT ID FROM {T} WHERE C_DATE LIKE '2024%'",
        FILTER_IN_SCAN,
        false,
    ),
    (
        DMY,
        "SELECT ID FROM {T} WHERE CAST(C_DATE AS VARCHAR(10)) LIKE '2024%'",
        FILTER_IN_SCAN,
        false,
    ),
    (
        DMY,
        "SELECT ID, CAST(C_DATE AS VARCHAR(10)) FROM {T}",
        "",
        false,
    ),
    (
        DMY,
        "SELECT CAST(C_DATE AS VARCHAR(10)), COUNT(*) FROM {T} GROUP BY CAST(C_DATE AS VARCHAR(10))",
        SPLIT,
        false,
    ),
    (
        DMY,
        "SELECT C_QTY, MAX(CAST(C_DATE AS VARCHAR(10))) FROM {T} GROUP BY C_QTY HAVING MAX(CAST(C_DATE AS VARCHAR(10))) > '01'",
        "",
        false,
    ),
    (
        COMMA,
        "SELECT ID FROM {T} WHERE CAST(C_DECIMAL_A AS VARCHAR(20)) LIKE '%.%'",
        FILTER_IN_SCAN,
        false,
    ),
    (
        DEFAULT,
        "SELECT ID, CAST(C_TS AS VARCHAR(40)) FROM {T}",
        "",
        false,
    ),
    (
        DEFAULT,
        "SELECT ID FROM {T} WHERE CAST(C_TS AS VARCHAR(40)) LIKE '2024-01-01 %'",
        FILTER_IN_SCAN,
        false,
    ),
    (
        DEFAULT,
        "SELECT ID FROM {T} WHERE CAST(C_BOOL AS VARCHAR(2000000)) = 'TRUE'",
        FILTER_IN_SCAN,
        true,
    ),
    (
        DEFAULT,
        "SELECT C_DECIMAL_A || '' K, COUNT(*) FROM {T} GROUP BY C_DECIMAL_A || ''",
        "",
        false,
    ),
    (
        DMY,
        "SELECT a.ID FROM {T} a JOIN {T} b ON SUBSTR(CAST(a.C_DATE AS VARCHAR(2000000)), 1, 4) = CAST(b.ID + 2023 AS VARCHAR(2000000))",
        "",
        false,
    ),
    (
        YDM,
        "SELECT C_DATE, COUNT(*) FROM {T} WHERE ID IN (2, 4) GROUP BY C_DATE",
        SPLIT,
        true,
    ),
    (
        DMY,
        "SELECT C_DATE, COUNT(*) FROM {T} GROUP BY C_DATE",
        SPLIT,
        true,
    ),
    (
        DMY,
        "SELECT C_DATE, SUM(C_QTY) FROM {T} GROUP BY C_DATE HAVING SUM(C_QTY) > 2 ORDER BY C_DATE DESC",
        SPLIT,
        true,
    ),
    (
        TS_DMY,
        "SELECT C_TS, COUNT(*) FROM {T} GROUP BY C_TS",
        SPLIT,
        true,
    ),
    (
        COMMA,
        "SELECT C_DECIMAL_A, COUNT(*) FROM {T} GROUP BY C_DECIMAL_A",
        SPLIT,
        true,
    ),
    (
        COMMA,
        "SELECT C_DOUBLE, COUNT(*) FROM {T} GROUP BY C_DOUBLE",
        SPLIT,
        true,
    ),
    (
        DEFAULT,
        "SELECT ID, C_DECIMAL_A || '' FROM {T} WHERE ID = 1",
        "",
        false,
    ),
    (
        DEFAULT,
        "SELECT ID, C_BOOL || 'x', C_DECIMAL_A || '' FROM {T} WHERE ID = 1",
        "",
        false,
    ),
    (
        DEFAULT,
        "SELECT ID, C_BOOL || 'x', C_DOUBLE || '' FROM {T} WHERE ID = 1",
        "",
        false,
    ),
    (
        DEFAULT,
        "SELECT ID, C_BOOL || 'x', C_DECIMAL_A || 'x' FROM {T} WHERE ID = 1",
        "",
        false,
    ),
    (
        DMY,
        "SELECT ID FROM {T} WHERE C_DATE = '2024-01-01'",
        FILTER_IN_SCAN,
        false,
    ),
    (
        DMY,
        "SELECT ID FROM {T} WHERE TO_DATE('2024-01-0' || (MOD(ID, 9) + 1)) = C_DATE",
        FILTER_IN_SCAN,
        false,
    ),
    (
        DMY,
        "SELECT ID FROM {T} WHERE COALESCE(C_DATE, '2024-01-02') = C_DATE",
        FILTER_IN_SCAN,
        false,
    ),
    (
        DMY,
        "SELECT ID FROM {T} WHERE DAYS_BETWEEN(C_DATE, '2024-01-01') > 0",
        FILTER_IN_SCAN,
        false,
    ),
    (
        YDM,
        "SELECT ID FROM {T} WHERE GREATEST(C_DATE, '2024-01-02') = C_DATE",
        FILTER_IN_SCAN,
        false,
    ),
    (
        COMMA,
        "SELECT ID FROM {T} WHERE CAST(C_QTY || '.5' AS DECIMAL(10,1)) > 0",
        FILTER_IN_SCAN,
        false,
    ),
    (
        COMMA,
        "SELECT ID FROM {T} WHERE CAST(C_QTY || '.5' AS DOUBLE) > 0",
        FILTER_IN_SCAN,
        false,
    ),
    (
        DEFAULT,
        "SELECT ID FROM {T} WHERE C_QTY + '1' > 2",
        FILTER_IN_SCAN,
        false,
    ),
    (
        DMY,
        "SELECT ID FROM {T} WHERE C_DATE = DATE '2024-01-02'",
        FILTER_IN_SCAN,
        true,
    ),
    (
        COMMA,
        "SELECT ID FROM {T} WHERE CAST(C_QTY AS VARCHAR(2000000)) LIKE '1%'",
        FILTER_IN_SCAN,
        true,
    ),
    (
        DEFAULT,
        "SELECT ID FROM {T} WHERE C_QTY IN ('3', '007')",
        FILTER_IN_SCAN,
        true,
    ),
    (
        DEFAULT,
        "SELECT ID, CAST(C_QTY AS VARCHAR(21)) FROM {T}",
        "",
        false,
    ),
    (
        COMMA,
        "SELECT UPPER(C_QTY), COUNT(*) FROM {T} GROUP BY UPPER(C_QTY)",
        SPLIT,
        true,
    ),
    (DEFAULT, "SELECT MAX(UPPER(C_QTY)) FROM {T}", "", false),
    (DEFAULT, "SELECT COUNT(UPPER(C_QTY)) FROM {T}", "", false),
    (
        DEFAULT,
        "SELECT CAST(C_DECIMAL_A AS VARCHAR(20)), COUNT(*) FROM {T} GROUP BY CAST(C_DECIMAL_A AS VARCHAR(20))",
        SPLIT,
        false,
    ),
    (
        DEFAULT,
        "SELECT ID, UPPER(C_DOUBLE * 1.5E0) FROM {T}",
        "",
        false,
    ),
    (
        DEFAULT,
        "SELECT ID, CAST(C_TS + INTERVAL '1' HOUR AS VARCHAR(30)) FROM {T}",
        "",
        false,
    ),
    (
        DEFAULT,
        "SELECT ID, CAST(C_VARCHAR AS CHAR(5)) || '|' FROM {T}",
        "",
        false,
    ),
    (
        DEFAULT,
        "SELECT C_QTY, TIMESTAMP '2024-01-01 00:00:00', SUM(ID) FROM {T} GROUP BY C_QTY",
        SPLIT,
        true,
    ),
    (
        DEFAULT,
        "SELECT ID FROM {T} WHERE LOCATE('a', C_VARCHAR, 2) = 2",
        FILTER_IN_SCAN,
        false,
    ),
    (
        DEFAULT,
        "SELECT INSTR(C_VARCHAR, 'a', 2), COUNT(*) FROM {T} GROUP BY INSTR(C_VARCHAR, 'a', 2)",
        SPLIT,
        false,
    ),
];

/// Rows sorted unless the query orders them, or the error class: the rejected value and the
/// relation name it quotes depend on row order and plan shape.
fn outcome(
    conn: &mut ExaConn,
    sql: &str,
) -> Result<(Vec<serde_json::Value>, Vec<Vec<String>>), String> {
    let resp = conn.try_execute(sql);
    if resp["status"] == "error" {
        let text = error_text(&resp, sql);
        return Err(text
            .split([';', '('])
            .next()
            .unwrap_or_default()
            .trim()
            .to_string());
    }
    let (types, mut rows) = typed_result(conn, sql);
    if !sql.contains("ORDER BY") {
        rows.sort();
    }
    Ok((types, rows))
}

fn session(setting: &str) -> ExaConn {
    let mut conn = exa_conn();
    if !setting.is_empty() {
        conn.execute(&format!("ALTER SESSION SET {setting}"));
    }
    conn
}

fn mismatch(table: &str, (setting, query, marker, in_scan): &Case) -> Option<String> {
    let mut conn = session(setting);
    let vs_query = query.replace("{T}", table);
    let actual = outcome(&mut conn, &vs_query);
    let expected = outcome(&mut conn, &query.replace("{T}", NATIVE_COPY));
    if actual != expected {
        return Some(format!(
            "[{setting}] `{query}`: got {actual:?}, native {expected:?}"
        ));
    }
    if marker.is_empty() {
        return None;
    }
    let plan = explain_virtual_sql(&mut conn, &vs_query);
    (plan.contains(marker) != *in_scan).then(|| {
        format!("[{setting}] `{query}`: `{marker}` in the pushdown must be {in_scan}: {plan}")
    })
}

/// Scenario: Text the session format converts runs in Exasol
/// Scenario: Integer and boolean text runs in the scan
/// Scenario: A group key travels with its exact type
#[test]
fn text_conversions_match_native_under_session_settings() {
    lakehouse_vs();
    let table = typed_table();
    create_native_copy(&mut exa_conn(), &table, TYPED_COLUMNS);

    let failures: Vec<String> = CASES
        .iter()
        .filter_map(|case| mismatch(&table, case))
        .collect();

    assert!(
        failures.is_empty(),
        "{} of {} cases differ:\n{}",
        failures.len(),
        CASES.len(),
        failures.join("\n")
    );
}

/// Scenario: A digits-only literal compares with an integer as a number
#[test]
fn digits_literal_beyond_an_int_column_matches_no_row_in_the_scan() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let query = format!("SELECT ID FROM {LAKEHOUSE_VS}.ALL_TYPES WHERE C_INT = '2147483648'");

    assert_eq!(conn.query_row_count(&query), 0);
    assert!(explain_virtual_sql(&mut conn, &query).contains(FILTER_IN_SCAN));
}

/// Scenario: A CAST of text to a shorter VARCHAR runs unchecked in the scan
#[test]
fn shorter_varchar_cast_runs_unchecked_in_the_scan() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let table = vs_table(LAKEHOUSE_VS, E2E_CHAR_PAD_TABLE);
    let short = format!("{table} WHERE LENGTH({CHAR_PAD_COL}) < 10");

    let trailing_space = format!(
        "SELECT {CHAR_PAD_COL} FROM {short} AND CAST({CHAR_PAD_COL} AS VARCHAR(3)) = 'ab '"
    );
    assert_eq!(
        conn.query_row_count(&trailing_space),
        0,
        "native Exasol returns 1 row"
    );
    assert!(explain_virtual_sql(&mut conn, &trailing_space).contains(FILTER_IN_SCAN));

    assert_query_fails(
        &mut conn,
        &format!("SELECT CAST({CHAR_PAD_COL} AS VARCHAR(3)) FROM {short}"),
        &["right truncation"],
    );

    let over_length = format!(
        "SELECT {CHAR_PAD_COL} FROM {table} WHERE CAST({CHAR_PAD_COL} AS VARCHAR(3)) IS NOT NULL"
    );
    assert_eq!(conn.query_row_count(&over_length), 4, "native Exasol fails");
}
