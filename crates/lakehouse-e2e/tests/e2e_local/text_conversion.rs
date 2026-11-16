//! Text and BOOLEAN conversions under the case's session settings, and `JSON_VALUE`: each query
//! over the virtual table must match the same query over a native copy, and take the path the
//! case names.

use crate::common::e2e_harness::PathCheck::{Has, Lacks, NScan, Split, Wrapper};
use crate::common::e2e_harness::*;
use crate::common::exasol_ws::ExaConn;
use crate::common::seed::{CHAR_PAD_COL, CHAR_PAD_TOTAL_ROWS, JSON_DOCS};
use crate::fixtures::{
    LAKEHOUSE_VS, TYPED_COLUMNS, json_table, lakehouse_vs, typed_table, vs_char_pad_table,
};

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
        query: "SELECT ID, CAST(CASE WHEN ID > 2 THEN C_QTY ELSE ID END AS VARCHAR(21)) FROM {T}",
        path: Has(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, CAST(CASE WHEN ID > 2 THEN 7 ELSE -12 END AS VARCHAR(3)) FROM {T}",
        path: Has(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, CAST(GREATEST(7, ID) AS VARCHAR(2)) FROM {T}",
        path: Lacks(SCAN_ITEM),
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
    Case {
        setting: COMMA,
        query: "SELECT AVG(CAST(ID AS VARCHAR(30))) FROM {T}",
        path: Wrapper,
    },
    Case {
        setting: COMMA,
        query: "SELECT C_QTY, SUM(CAST(ID AS VARCHAR(30))) FROM {T} GROUP BY C_QTY",
        path: Wrapper,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT AVG(C_BOOL), SUM(C_BOOL) FROM {T}",
        path: Wrapper,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT C_QTY, AVG(C_BOOL) FROM {T} GROUP BY C_QTY",
        path: Wrapper,
    },
    Case {
        setting: COMMA,
        query: "SELECT SUM(C_DECIMAL_A), SUM(C_DECIMAL_A * C_QTY) FROM {T}",
        path: Split,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT SUM(NULL), AVG(NULL) FROM {T}",
        path: Wrapper,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT C_QTY, SUM(NULL), AVG(NULL) FROM {T} GROUP BY C_QTY",
        path: Wrapper,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT SUM(CAST(NULL AS DECIMAL(10,0))) FROM {T}",
        path: Wrapper,
    },
    Case {
        setting: COMMA,
        query: "SELECT ID, CAST(C_BOOL AS DECIMAL(18,0)), CAST(C_BOOL AS DECIMAL(5,2)) FROM {T}",
        path: Has(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT CAST(C_BOOL AS INTEGER) K, COUNT(*) FROM {T} GROUP BY CAST(C_BOOL AS INTEGER) ORDER BY 1 DESC LIMIT 2",
        path: Split,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT C_QTY, SUM(CAST(C_BOOL AS INTEGER)) FROM {T} GROUP BY C_QTY",
        path: Split,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT COUNT(*) FROM {T} WHERE C_BOOL = 1",
        path: Split,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE C_BOOL = 2 OR C_BOOL < 0.5",
        path: Has(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE C_BOOL BETWEEN 1 AND C_QTY OR C_BOOL >= C_PRICE",
        path: Has(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, CASE C_BOOL WHEN 0 THEN 'no' WHEN 1 THEN 'yes' END FROM {T}",
        path: Has(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, CASE C_QTY WHEN C_BOOL THEN 'eq' WHEN 5 THEN 'five' END FROM {T}",
        path: Has(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE C_QTY IN (TRUE, 5)",
        path: Has(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE C_BOOL IN (0, 1)",
        path: Lacks(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT a.ID, b.ID FROM {T} a JOIN {T} b ON a.ID = b.ID WHERE a.C_BOOL = 1 AND CAST(b.C_BOOL AS DECIMAL(1,0)) = 1",
        path: Has(BROADCAST_JOIN),
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
    Case {
        setting: COMMA,
        query: "SELECT AVG(C_VARCHAR) FROM {T}",
        path: Wrapper,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE C_BOOL IN (1, 2)",
        path: Lacks(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, CAST(C_BOOL AS DECIMAL(2,2)) FROM {T}",
        path: Lacks(SCAN_ITEM),
    },
];

fn mismatch(table: &str, case: &Case, expect: Expect) -> Option<String> {
    let mut conn = exa_session(case.setting);
    native_copy_mismatch(&mut conn, table, NATIVE_COPY, case.query, case.path, expect)
        .map(|failure| format!("[{}] {failure}", case.setting))
}

/// Scenario: Text the session format converts runs in Exasol
/// Scenario: Integer and boolean text runs in the scan
/// Scenario: A JSON_VALUE without a DEFAULT ON ERROR stops reading once no path can select more
#[test]
fn json_value_fast_read_differs_from_native_as_documented() {
    lakehouse_vs();
    let table = json_table();
    let mut conn = exa_conn();
    let rows = |conn: &mut ExaConn, sql: &str| -> Vec<Vec<String>> {
        conn.query_columns(sql)
            .iter()
            .map(|column| column.iter().map(value_to_string).collect())
            .collect()
    };
    // Document 6 repeats `locale`, and document 7 ends without its closing brace.
    let calls = "JSON_VALUE(DOC, '$.locale'), JSON_VALUE(DOC, '$.order.id')";
    let scan = rows(
        &mut conn,
        &format!("SELECT ID, {calls} FROM {table} WHERE ID IN (6, 7) ORDER BY ID"),
    );
    let ids = vec!["6".to_string(), "7".to_string()];
    let big = "12345678901234567890123".to_string();
    let de = "de_DE".to_string();
    assert_eq!(
        scan,
        vec![ids, vec![de.clone(), de], vec![big.clone(), "44".into()]]
    );
    for (id, locale, order_id) in [(6, "null", big.as_str()), (7, "null", "null")] {
        let doc = JSON_DOCS[id - 1].expect("a document").replace('\'', "''");
        let native = rows(
            &mut conn,
            &format!(
                "SELECT {} FROM DUAL",
                calls.replace("DOC", &format!("'{doc}'"))
            ),
        );
        assert_eq!(
            native,
            vec![vec![locale.to_string()], vec![order_id.to_string()]],
            "{id}"
        );
    }
}

/// Scenario: A digits-only literal compares with an integer as a number
/// Scenario: A group key travels with its exact type
/// Scenario: A CAST of text to CHAR runs in Exasol
/// Scenario: String scalar functions translate to DataFusion string calls
/// Scenario: A widened derived projection routes to a native wrapper on every path
/// Scenario: A join condition or filter that converts a date to text takes the N-scan fallback
/// Scenario: A numeric aggregate over text or BOOLEAN runs in Exasol
/// Scenario: A numeric aggregate over a bare NULL runs in Exasol
/// Scenario: A BOOLEAN converts to 1 or 0 where Exasol converts it to a number
#[test]
fn text_conversions_match_native_under_session_settings() {
    lakehouse_vs();
    assert_cases_match_native(&typed_table(), TYPED_COLUMNS, CASES, ERROR_CASES);
}

/// Native Exasol returns rows for `cases` and raises an error for `error_cases`.
fn assert_cases_match_native(table: &str, columns: &str, cases: &[Case], error_cases: &[Case]) {
    create_native_copy(&mut exa_conn(), table, columns);
    let cases = cases
        .iter()
        .map(|case| (case, Expect::Rows))
        .chain(error_cases.iter().map(|case| (case, Expect::Error)));
    let failures: Vec<String> = cases
        .clone()
        .filter_map(|(case, expect)| mismatch(table, case, expect))
        .collect();

    assert!(
        failures.is_empty(),
        "{} of {} cases differ:\n{}",
        failures.len(),
        cases.count(),
        failures.join("\n")
    );
}

/// The scan's projection when a declined `JSON_VALUE` leaves Exasol only the document to read.
const DOCUMENT_ONLY: &str = r#""projection":["ID","DOC"]"#;

/// Without a `DEFAULT ... ON ERROR`, documents 6 and 7 read as
/// `json_value_fast_read_differs_from_native_as_documented` checks, so `$.locale` cases skip them.
const JSON_VALUE_CASES: &[Case] = &[
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT COUNT(JSON_VALUE(DOC, '$.locale')), MAX(JSON_VALUE(DOC, '$.locale')), COUNT(JSON_VALUE(DOC, '$.order.id' DEFAULT 'bad' ON ERROR)) FROM {T}",
        path: Split,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, JSON_VALUE(DOC, '$.locale'), JSON_VALUE(DOC, '$.locale'), JSON_VALUE(DOC, '$.order.id' DEFAULT 'bad' ON ERROR) FROM {T}",
        path: Has(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, JSON_VALUE(DOC, '$.order.id' DEFAULT 'bad' ON ERROR) FROM {T} WHERE JSON_VALUE(DOC, '$.locale') = 'de_DE'",
        path: Has(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, JSON_VALUE(DOC, '$.locale') FROM {T} WHERE ID NOT IN (6, 7)",
        path: Has(r#""projection":["ID",{"expr""#),
    },
    Case {
        setting: COMMA,
        query: "SELECT ID, JSON_VALUE(DOC, '$.order.id'), JSON_VALUE(DOC, '$.order.price'), JSON_VALUE(DOC, '$.note'), JSON_VALUE(DOC, '$.active') FROM {T}",
        path: Has(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, JSON_VALUE(DOC, '$.order.tags[1]' RETURNING VARCHAR(5) DEFAULT 'none' ON EMPTY DEFAULT 'bad' ON ERROR) FROM {T}",
        path: Has(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, JSON_VALUE(DOC, '$.order' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR), JSON_VALUE(DOC, '$.order[1].id' DEFAULT 'empty' ON EMPTY) FROM {T}",
        path: Has(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, JSON_VALUE(DOC, '$.note' RETURNING VARCHAR(5) DEFAULT 'long' ON ERROR) FROM {T}",
        path: Has(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, JSON_VALUE(DOC, '$.active' RETURNING BOOLEAN), JSON_VALUE(DOC, '$.missing' RETURNING BOOLEAN DEFAULT TRUE ON EMPTY DEFAULT FALSE ON ERROR) FROM {T}",
        path: Has(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE JSON_VALUE(DOC, '$.locale') = 'de_DE' AND ID NOT IN (6, 7)",
        path: Has(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, JSON_VALUE(DOC, '$.locale' DEFAULT 'bad' ON ERROR), JSON_VALUE(DOC, '$.order.id') FROM {T}",
        path: Has(SCAN_ITEM),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, JSON_VALUE(DOC, '$.locale' DEFAULT 'bad' ON ERROR) FROM {T} ORDER BY 2, 1",
        path: Lacks(r#"},"DOC"]"#),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE JSON_VALUE(DOC, '$.active' RETURNING BOOLEAN)",
        path: Has(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE JSON_VALUE(DOC, '$.note') IS NULL",
        path: Has(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE JSON_VALUE(DOC, '$.locale' DEFAULT 'none' ON EMPTY DEFAULT 'bad' ON ERROR) IN ('none', 'bad')",
        path: Has(SCAN_FILTER),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT JSON_VALUE(DOC, '$.locale'), COUNT(*) FROM {T} WHERE ID NOT IN (6, 7) GROUP BY JSON_VALUE(DOC, '$.locale')",
        path: Split,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT MAX(JSON_VALUE(DOC, '$.locale')) FROM {T}",
        path: Split,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT LENGTH(MIN(JSON_VALUE(DOC, '$.note' RETURNING VARCHAR(5)))) FROM {T}",
        path: Split,
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, JSON_VALUE(DOC, '$.order.id' RETURNING DECIMAL(18,0)) FROM {T}",
        path: Has(DOCUMENT_ONLY),
    },
    Case {
        setting: COMMA,
        query: "SELECT ID, JSON_VALUE(DOC, '$.order.id' RETURNING DECIMAL(18,0)) FROM {T}",
        path: Has(DOCUMENT_ONLY),
    },
    Case {
        setting: COMMA,
        query: "SELECT ID, JSON_VALUE(DOC, '$.order.price' RETURNING DOUBLE) FROM {T}",
        path: Has(DOCUMENT_ONLY),
    },
    Case {
        setting: DMY,
        query: "SELECT ID, JSON_VALUE(DOC, '$.day' RETURNING DATE) FROM {T}",
        path: Has(DOCUMENT_ONLY),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, JSON_VALUE(DOC, '$.day' RETURNING TIMESTAMP) FROM {T}",
        path: Has(DOCUMENT_ONLY),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, JSON_VALUE(DOC, '$.locale' RETURNING VARCHAR(10) ASCII) FROM {T}",
        path: Has(DOCUMENT_ONLY),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, JSON_VALUE(DOC, '$.order.tags[*]') FROM {T}",
        path: Has(DOCUMENT_ONLY),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID FROM {T} WHERE JSON_VALUE(DOC, '$.order.id' RETURNING DECIMAL(18,0)) > 42",
        path: Lacks(SCAN_FILTER),
    },
];

const JSON_VALUE_ERROR_CASES: &[Case] = &[
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, JSON_VALUE(DOC, '$.order' ERROR ON ERROR) FROM {T}",
        path: Has(DOCUMENT_ONLY),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, JSON_VALUE(DOC, '$.missing' ERROR ON EMPTY) FROM {T}",
        path: Has(DOCUMENT_ONLY),
    },
    Case {
        setting: DEFAULT_SESSION,
        query: "SELECT ID, JSON_VALUE(DOC, '$.note' RETURNING VARCHAR(3) DEFAULT 'toolong' ON ERROR) FROM {T}",
        path: Has(DOCUMENT_ONLY),
    },
];

/// Scenario: JSON_VALUE with a session-free form runs in the scan
/// Scenario: JSON_VALUE in the scan reads the document and the path as Exasol does
/// Scenario: JSON_VALUE in the scan converts the item as Exasol does
/// Scenario: A JSON_VALUE the scan cannot compute runs in Exasol over the document column
/// Scenario: A JSON_VALUE without a DEFAULT ON ERROR stops reading once no path can select more
#[test]
fn json_value_matches_native() {
    lakehouse_vs();
    assert_cases_match_native(
        &json_table(),
        "DOC",
        JSON_VALUE_CASES,
        JSON_VALUE_ERROR_CASES,
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
