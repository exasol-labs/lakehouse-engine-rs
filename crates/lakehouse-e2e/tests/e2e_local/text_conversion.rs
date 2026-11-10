//! Text conversions under non-default session settings: each query over the virtual table must
//! match the same query over a native copy, and take the path the case names.

use crate::common::e2e_harness::PathCheck::{Has, Lacks, NScan, Split, Wrapper};
use crate::common::e2e_harness::*;
use crate::common::seed::{CHAR_PAD_COL, CHAR_PAD_TOTAL_ROWS};
use crate::fixtures::{LAKEHOUSE_VS, TYPED_COLUMNS, lakehouse_vs, typed_table, vs_char_pad_table};

const YDM: &str = "NLS_DATE_FORMAT = 'YYYY-DD-MM'";
const TS_DMY: &str = "NLS_TIMESTAMP_FORMAT = 'DD.MM.YYYY HH24:MI:SS.FF6'";
const COMMA: &str = "NLS_NUMERIC_CHARACTERS = ',.'";

/// `query` over `{T}`, run in a session with `setting` applied, must match native Exasol and take
/// `path`. Native Exasol returns rows for [`CASES`] and raises an error for [`ERROR_CASES`].
struct Case {
    setting: &'static str,
    query: &'static str,
    path: PathCheck<'static>,
}

const CASES: &[Case] = &[
    Case {
        setting: DMY,
        query: "SELECT ID FROM {T} WHERE C_DATE LIKE '2024%'",
        path: Lacks(SCAN_FILTER),
    },
    Case {
        setting: DMY,
        query: "SELECT ID FROM {T} WHERE CAST(C_DATE AS VARCHAR(10)) LIKE '2024%'",
        path: Lacks(SCAN_FILTER),
    },
    Case {
        setting: DMY,
        query: "SELECT ID, CAST(C_DATE AS VARCHAR(10)) FROM {T}",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: DMY,
        query: "SELECT ID, LOWER(C_DATE) FROM {T}",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: DMY,
        query: "SELECT CAST(C_DATE AS VARCHAR(10)), COUNT(*) FROM {T} GROUP BY CAST(C_DATE AS VARCHAR(10))",
        path: Wrapper,
    },
    Case {
        setting: DMY,
        query: "SELECT C_QTY, MAX(CAST(C_DATE AS VARCHAR(10))) FROM {T} GROUP BY C_QTY HAVING MAX(CAST(C_DATE AS VARCHAR(10))) > '01'",
        path: Wrapper,
    },
    Case {
        setting: COMMA,
        query: "SELECT ID FROM {T} WHERE CAST(C_DECIMAL_A AS VARCHAR(20)) LIKE '%.%'",
        path: Lacks(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, CAST(C_DECIMAL_A AS VARCHAR(20)) FROM {T}",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, ID || '-' || C_DECIMAL_A FROM {T}",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, LENGTH(C_DECIMAL_A) FROM {T}",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, LTRIM(C_DECIMAL_A) FROM {T}",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, INSTR(C_DECIMAL_A, '.') FROM {T}",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE LENGTH(C_DECIMAL_A) > 4",
        path: Lacks(SCAN_FILTER),
    },
    Case {
        setting: COMMA,
        query: "SELECT ID, CAST(C_DECIMAL_A AS VARCHAR(20)) FROM {T}",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: COMMA,
        query: "SELECT ID, ID || '-' || C_DECIMAL_A FROM {T}",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: COMMA,
        query: "SELECT ID, LENGTH(C_DECIMAL_A) FROM {T}",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: COMMA,
        query: "SELECT ID, LTRIM(C_DECIMAL_A) FROM {T}",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: COMMA,
        query: "SELECT ID, INSTR(C_DECIMAL_A, '.') FROM {T}",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: COMMA,
        query: "SELECT ID FROM {T} WHERE LENGTH(C_DECIMAL_A) > 4",
        path: Lacks(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, CAST(C_TS AS VARCHAR(40)) FROM {T}",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE CAST(C_TS AS VARCHAR(40)) LIKE '2024-01-01 %'",
        path: Lacks(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE CAST(C_BOOL AS VARCHAR(2000000)) = 'TRUE'",
        path: Has(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT C_DECIMAL_A || '' K, COUNT(*) FROM {T} GROUP BY C_DECIMAL_A || ''",
        path: Wrapper,
    },
    Case {
        setting: DMY,
        query: "SELECT a.ID FROM {T} a JOIN {T} b ON SUBSTR(CAST(a.C_DATE AS VARCHAR(2000000)), 1, 4) = CAST(b.ID + 2023 AS VARCHAR(2000000))",
        path: NScan(2),
    },
    Case {
        setting: YDM,
        query: "SELECT C_DATE, COUNT(*) FROM {T} WHERE ID IN (2, 4) GROUP BY C_DATE",
        path: Split,
    },
    Case {
        setting: DMY,
        query: "SELECT C_DATE, COUNT(*) FROM {T} GROUP BY C_DATE",
        path: Split,
    },
    Case {
        setting: DMY,
        query: "SELECT C_DATE, SUM(C_QTY) FROM {T} GROUP BY C_DATE HAVING SUM(C_QTY) > 2 ORDER BY C_DATE DESC",
        path: Split,
    },
    Case {
        setting: TS_DMY,
        query: "SELECT C_TS, COUNT(*) FROM {T} GROUP BY C_TS",
        path: Split,
    },
    Case {
        setting: COMMA,
        query: "SELECT C_DECIMAL_A, COUNT(*) FROM {T} GROUP BY C_DECIMAL_A",
        path: Split,
    },
    Case {
        setting: COMMA,
        query: "SELECT C_DOUBLE, COUNT(*) FROM {T} GROUP BY C_DOUBLE",
        path: Split,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, C_DECIMAL_A || '' FROM {T} WHERE ID = 1",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, C_BOOL || 'x', C_DECIMAL_A || '' FROM {T} WHERE ID = 1",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, C_BOOL || 'x', C_DOUBLE || '' FROM {T} WHERE ID = 1",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, C_BOOL || 'x', C_DECIMAL_A || 'x' FROM {T} WHERE ID = 1",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: TS_DMY,
        query: "SELECT ID FROM {T} WHERE TO_TIMESTAMP('01.01.2024 00:00:00.' || (MOD(ID, 9) + 1) || '00000') >= C_TS",
        path: Lacks(SCAN_FILTER),
    },
    Case {
        setting: YDM,
        query: "SELECT ID FROM {T} WHERE GREATEST(C_DATE, '2024-01-02') = C_DATE",
        path: Lacks(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE C_QTY + '1' > 2",
        path: Lacks(SCAN_FILTER),
    },
    Case {
        setting: DMY,
        query: "SELECT ID FROM {T} WHERE C_DATE = DATE '2024-01-02'",
        path: Has(SCAN_FILTER),
    },
    Case {
        setting: COMMA,
        query: "SELECT ID FROM {T} WHERE CAST(C_QTY AS VARCHAR(2000000)) LIKE '1%'",
        path: Has(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE C_QTY IN ('3', '007')",
        path: Has(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE C_QTY BETWEEN '2' AND '004'",
        path: Has(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, CAST(C_QTY AS VARCHAR(21)) FROM {T}",
        path: Has(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, CAST(C_QTY AS VARCHAR(5)) FROM {T}",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE CAST(C_QTY AS VARCHAR(5)) LIKE '1%'",
        path: Lacks(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, CAST(C_BOOL AS VARCHAR(4)) FROM {T} WHERE C_BOOL",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: COMMA,
        query: "SELECT UPPER(C_QTY), COUNT(*) FROM {T} GROUP BY UPPER(C_QTY)",
        path: Split,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT MAX(UPPER(C_QTY)) FROM {T}",
        path: Split,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT COUNT(UPPER(C_QTY)) FROM {T}",
        path: Split,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT CAST(C_DECIMAL_A AS VARCHAR(20)), COUNT(*) FROM {T} GROUP BY CAST(C_DECIMAL_A AS VARCHAR(20))",
        path: Wrapper,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, UPPER(C_DOUBLE * 1.5E0) FROM {T}",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, CAST(C_TS + INTERVAL '1' HOUR AS VARCHAR(30)) FROM {T}",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, CAST(C_VARCHAR AS CHAR(5)) || '|' FROM {T}",
        path: Lacks(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT C_QTY, TIMESTAMP '2024-01-01 00:00:00', SUM(ID) FROM {T} GROUP BY C_QTY",
        path: Split,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE LOCATE('a', C_VARCHAR, 2) = 2",
        path: Lacks(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT INSTR(C_VARCHAR, 'a', 2), COUNT(*) FROM {T} GROUP BY INSTR(C_VARCHAR, 'a', 2)",
        path: Wrapper,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE LOCATE('a', C_VARCHAR) = 2",
        path: Has(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE INSTR(C_VARCHAR, 'a') = 2",
        path: Has(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE CAST(C_VARCHAR AS CHAR(5)) = C_VARCHAR",
        path: Lacks(SCAN_FILTER),
    },
];

const ERROR_CASES: &[Case] = &[
    Case {
        setting: DMY,
        query: "SELECT ID FROM {T} WHERE C_DATE = '2024-01-01'",
        path: Lacks(SCAN_FILTER),
    },
    Case {
        setting: DMY,
        query: "SELECT ID FROM {T} WHERE TO_DATE('2024-01-0' || (MOD(ID, 9) + 1)) = C_DATE",
        path: Lacks(SCAN_FILTER),
    },
    Case {
        setting: DMY,
        query: "SELECT ID FROM {T} WHERE COALESCE(C_DATE, '2024-01-02') = C_DATE",
        path: Lacks(SCAN_FILTER),
    },
    Case {
        setting: DMY,
        query: "SELECT ID FROM {T} WHERE DAYS_BETWEEN(C_DATE, '2024-01-01') > 0",
        path: Lacks(SCAN_FILTER),
    },
    Case {
        setting: COMMA,
        query: "SELECT ID FROM {T} WHERE CAST(C_QTY || '.5' AS DECIMAL(10,1)) > 0",
        path: Lacks(SCAN_FILTER),
    },
    Case {
        setting: COMMA,
        query: "SELECT ID FROM {T} WHERE CAST(C_QTY || '.5' AS DOUBLE) > 0",
        path: Lacks(SCAN_FILTER),
    },
];

fn mismatch(table: &str, case: &Case, expect: Expect) -> Option<String> {
    let mut conn = exa_session(case.setting);
    native_copy_mismatch(&mut conn, table, NATIVE_COPY, case.query, case.path, expect)
        .map(|failure| format!("[{}] {failure}", case.setting))
}

/// Scenario: Text the session format converts runs in Exasol
/// Scenario: Integer and boolean text runs in the scan
/// Scenario: A digits-only literal compares with an integer as a number
/// Scenario: A group key travels with its exact type
/// Scenario: A CAST of text to CHAR runs in Exasol
/// Scenario: String scalar functions translate to DataFusion string calls
/// Scenario: A widened derived projection routes to a native wrapper on every path
/// Scenario: A join condition or filter that converts a date to text takes the N-scan fallback
#[test]
fn text_conversions_match_native_under_session_settings() {
    lakehouse_vs();
    let table = typed_table();
    create_native_copy(&mut exa_conn(), &table, TYPED_COLUMNS);

    let cases = CASES
        .iter()
        .map(|case| (case, Expect::Rows))
        .chain(ERROR_CASES.iter().map(|case| (case, Expect::Error)));
    let failures: Vec<String> = cases
        .clone()
        .filter_map(|(case, expect)| mismatch(&table, case, expect))
        .collect();

    assert!(
        failures.is_empty(),
        "{} of {} cases differ:\n{}",
        failures.len(),
        cases.count(),
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
    assert!(explain_virtual_sql(&mut conn, &query).contains(SCAN_FILTER));
}

/// Scenario: A digits-only literal wider than the integer's precision runs in Exasol
#[test]
fn digits_literal_wider_than_an_int_column_matches_native() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let table = format!("{LAKEHOUSE_VS}.ALL_TYPES");
    create_native_copy(&mut conn, &table, "C_INT");
    let cases = [
        ("C_INT IN ('7', '8', '99999999999')", Expect::Error),
        ("C_INT * 1 IN ('1', '99999999999')", Expect::Error),
        ("C_INT = '99999999999'", Expect::Rows),
        ("C_INT BETWEEN '-5' AND '99999999999'", Expect::Rows),
        ("C_INT IN ('1', '00000000002')", Expect::Rows),
        ("C_INT IN ('10', 8)", Expect::Rows),
        ("C_INT + 1 IN ('1', '99999999999')", Expect::Rows),
        (
            "CASE WHEN C_INT > 0 THEN C_INT ELSE 0 END IN ('1', '99999999999')",
            Expect::Rows,
        ),
    ];
    let failures: Vec<String> = cases
        .iter()
        .filter_map(|(filter, expect)| {
            let query = format!("SELECT ID FROM {{T}} WHERE {filter}");
            native_copy_mismatch(
                &mut conn,
                &table,
                NATIVE_COPY,
                &query,
                PathCheck::Unchecked,
                *expect,
            )
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Scenario: A CAST of text to a shorter VARCHAR runs unchecked in the scan
#[test]
fn shorter_varchar_cast_runs_unchecked_in_the_scan() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let table = vs_char_pad_table();
    let short = format!("{table} WHERE LENGTH({CHAR_PAD_COL}) < 10");

    let trailing_space = format!(
        "SELECT {CHAR_PAD_COL} FROM {short} AND CAST({CHAR_PAD_COL} AS VARCHAR(3)) = 'ab '"
    );
    assert_eq!(
        conn.query_row_count(&trailing_space),
        0,
        "native Exasol returns 1 row"
    );
    assert!(explain_virtual_sql(&mut conn, &trailing_space).contains(SCAN_FILTER));

    assert_query_fails(
        &mut conn,
        &format!("SELECT CAST({CHAR_PAD_COL} AS VARCHAR(3)) FROM {short}"),
        &["right truncation"],
    );

    let over_length = format!(
        "SELECT {CHAR_PAD_COL} FROM {table} WHERE CAST({CHAR_PAD_COL} AS VARCHAR(3)) IS NOT NULL"
    );
    assert_eq!(
        conn.query_row_count(&over_length),
        CHAR_PAD_TOTAL_ROWS as i64,
        "native Exasol fails"
    );
}
