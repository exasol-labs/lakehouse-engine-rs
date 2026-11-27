//! Empty strings read as NULL (#203): each query over the empty-string table must return what it
//! returns over a native copy, where Exasol stored every empty string as NULL, and take the path
//! the case names.

use crate::common::e2e_harness::PathCheck::Has;
use crate::common::e2e_harness::*;
use crate::common::seed::{EMPTY_STRING_FILES, EMPTY_STRING_TABLE};
use crate::fixtures::{EMPTY_STRING_TEXT_VS, EMPTY_STRING_VS, empty_string_vs, vs_table};

/// The scan reads `S` with its empty strings as NULL.
const S_AS_NULL: &str = r#"nullif(\"S\", '''')"#;
/// The scan spec's per-shard sort.
const SHARD_SORT: &str = r#""order_by":[{"#;

fn table() -> String {
    vs_table(EMPTY_STRING_VS, EMPTY_STRING_TABLE)
}

/// The scan spec's filter when it is exactly `sql`, the DataFusion text before SQL quoting.
fn scan_filter(sql: &str) -> String {
    let embedded = sql.replace('"', r#"\""#).replace('\'', "''");
    format!(r#""filter":"{embedded}""#)
}

/// Each `(query, marker)` names its table `{T}`, must return the native copy's rows, and must
/// push a statement that holds `marker`.
#[track_caller]
fn assert_match_native<Q: AsRef<str> + Sync, M: AsRef<str> + Sync>(cases: &[(Q, M)]) {
    empty_string_vs();
    let mut conn = exa_conn();
    create_native_copy(&mut conn, &table(), "S, K, B");
    let table = table();
    let failures = case_failures(cases, exa_conn, |conn, (query, marker)| {
        native_copy_mismatch(
            conn,
            &table,
            NATIVE_COPY,
            query.as_ref(),
            Has(marker.as_ref()),
            Expect::Rows,
        )
    });
    assert!(
        failures.is_empty(),
        "{} of {} cases differ:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}

/// A case per `(filter, scan filter)`: `SELECT ID` under `filter`, pushed as exactly that scan
/// filter.
fn filter_cases(cases: &[(&str, &str)]) -> Vec<(String, String)> {
    cases
        .iter()
        .map(|(filter, scan)| {
            (
                format!("SELECT ID FROM {{T}} WHERE {filter}"),
                scan_filter(scan),
            )
        })
        .collect()
}

/// Scenario: A text function that returns an empty string returns NULL
#[test]
fn text_function_empty_result_is_null() {
    let rtrim = r#"nullif(rtrim(nullif(\"S\", ''''), nullif(\"S\", '''')), '''')"#;
    let ltrim = r#"nullif(ltrim(nullif(\"S\", ''''), nullif(\"S\", '''')), '''')"#;
    assert_match_native(&[
        ("SELECT COUNT(*) FROM {T} WHERE RTRIM(S, S) IS NULL", rtrim),
        ("SELECT COUNT(*) FROM {T} WHERE LTRIM(S, S) IS NULL", ltrim),
        (
            "SELECT COUNT(*) FROM {T} WHERE LENGTH(RTRIM(S, S)) = 0",
            rtrim,
        ),
        ("SELECT LENGTH(RTRIM(S, S)) FROM {T} WHERE ID = 1", rtrim),
        (
            "SELECT NVL(RTRIM(S, S), 'EMPTY') FROM {T} WHERE ID = 1",
            rtrim,
        ),
    ]);
}

/// Scenario: A stored empty string reads as NULL in every computed expression
#[test]
fn stored_empty_string_reads_as_null() {
    assert_match_native(&[
        ("SELECT ID, LENGTH(S) FROM {T}", S_AS_NULL),
        ("SELECT ID, NVL(S, 'E') || '!' FROM {T}", S_AS_NULL),
        ("SELECT ID, S IS NULL FROM {T}", SCAN_ITEM),
        (
            "SELECT ID, CASE WHEN S < 'a' THEN 1 ELSE 0 END FROM {T}",
            S_AS_NULL,
        ),
        ("SELECT COUNT(S), MIN(S), MAX(S) FROM {T}", S_AS_NULL),
        ("SELECT COUNT(DISTINCT S) FROM {T}", "LAKEHOUSE_SCAN"),
        ("SELECT S, COUNT(*) FROM {T} GROUP BY S", PARTIAL),
        ("SELECT K, MIN(S) FROM {T} GROUP BY K", S_AS_NULL),
        // A bare column is emitted as stored, and arrives in Exasol as NULL.
        ("SELECT ID, S FROM {T}", r#""projection":["ID","S"]"#),
    ]);
}

/// Scenario: A filter over a bare string column keeps the bare column
#[test]
fn bare_string_filter_keeps_the_bare_column() {
    let mut cases = filter_cases(&[
        ("S = 'TRUCK'", r#"("S" = 'TRUCK')"#),
        // Exasol sends `a > b` as `b < a`.
        ("S > 'a'", r#"('a' < "S")"#),
        ("S IN ('TRUCK', 'zulu')", r#"("S" IN ('TRUCK', 'zulu'))"#),
        ("S BETWEEN 'a' AND 'z'", r#"("S" BETWEEN 'a' AND 'z')"#),
        ("S <> 'TRUCK'", r#"(("S" <> 'TRUCK') AND ("S" <> ''))"#),
        ("S < 'a'", r#"(("S" < 'a') AND ("S" <> ''))"#),
        ("S LIKE '%u%'", r#"(("S" LIKE '%u%') AND ("S" <> ''))"#),
        (
            "ID > 1 AND (S < 'a' OR S = 'zulu')",
            r#"((1 < "ID") AND ((("S" < 'a') AND ("S" <> '')) OR ("S" = 'zulu')))"#,
        ),
        (
            "NOT (S IN ('TRUCK', 'zulu'))",
            r#"(NOT (nullif("S", '') IN ('TRUCK', 'zulu')))"#,
        ),
    ]);
    // In the select list a FALSE result differs from a NULL one.
    let select_items = [
        (
            "SELECT ID, S = 'TRUCK' FROM {T}",
            r#"(nullif(\"S\", '''') = ''TRUCK'')"#,
        ),
        (
            "SELECT ID, CASE WHEN S <> 'TRUCK' THEN 1 ELSE 0 END FROM {T}",
            r#"WHEN (nullif(\"S\", '''') <> ''TRUCK'')"#,
        ),
    ];
    cases.extend(select_items.map(|(query, marker)| (query.into(), marker.into())));
    assert_match_native(&cases);
}

/// Scenario: IS NULL and IS NOT NULL predicates translate correctly
#[test]
fn null_test_over_string_column_keeps_the_bare_column() {
    let mut cases = filter_cases(&[
        ("S IS NULL", r#"(("S" IS NULL) OR ("S" = ''))"#),
        ("S IS NOT NULL", r#"(("S" IS NOT NULL) AND ("S" <> ''))"#),
        (
            "ID > 0 AND NOT (S IS NULL)",
            r#"((0 < "ID") AND (NOT (("S" IS NULL) OR ("S" = ''))))"#,
        ),
    ]);
    cases.push((
        "SELECT ID FROM {T} WHERE UPPER(S) IS NULL".into(),
        r#"(nullif(upper(nullif(\"S\", '''')), '''') IS NULL)"#.into(),
    ));
    assert_match_native(&cases);
}

/// Scenario: An equality of two string columns never matches empty strings
#[test]
fn string_key_equality_never_matches_empty_strings() {
    let mut cases = filter_cases(&[
        ("S = K", r#"(("S" = "K") AND ("S" <> ''))"#),
        ("S < K", r#"(nullif("S", '') < nullif("K", ''))"#),
    ]);
    // The join compares the bare keys and adds that one is not empty.
    cases.push((
        "SELECT a.ID, b.ID FROM {T} a JOIN {T} b ON a.S = b.K".into(),
        r#" = \"LHS_T1\".\"K\") AND (\"LHS_T0\".\"S\" <> ''''))"#.into(),
    ));
    assert_match_native(&cases);
}

/// Scenario: REPLACE reads a NULL argument as the empty string
#[test]
fn replace_reads_null_argument_as_empty() {
    assert_match_native(&[
        ("SELECT ID, REPLACE(S, 'U') FROM {T}", "replace("),
        ("SELECT ID, REPLACE(S, 'U', '') FROM {T}", "replace("),
        ("SELECT ID, REPLACE(S, 'U', NULL) FROM {T}", "replace("),
        (
            "SELECT COUNT(*) FROM {T} WHERE REPLACE(S, 'U') = 'TRCK'",
            SCAN_FILTER,
        ),
        ("SELECT ID, REPLACE(S, K) FROM {T}", "replace("),
        ("SELECT ID, REPLACE(S, 'U', K) FROM {T}", "replace("),
        ("SELECT ID, REPLACE(K, 'U', 'x') FROM {T}", "replace("),
        (
            "SELECT ID FROM {T} WHERE REPLACE(S, S) IS NULL",
            SCAN_FILTER,
        ),
    ]);
}

/// `K` is NULL in some rows and empty in others, and `LENGTH(K)` is NULL in both.
/// Scenario: A text function with a NULL argument returns what native Exasol returns
#[test]
fn text_function_null_argument_matches_native() {
    let calls = "TRIM(K); TRIM(S, K); TRIM(K, 'x'); LTRIM(K); LTRIM(S, K); LTRIM(K, 'x'); \
                 RTRIM(K); RTRIM(S, K); RTRIM(K, 'x'); TRANSLATE(K, 'U', 'x'); \
                 TRANSLATE(S, K, 'x'); TRANSLATE(S, 'U', K); LPAD(K, 8); LPAD(S, LENGTH(K)); \
                 LPAD(K, 8, 'x'); LPAD(S, LENGTH(K), 'x'); LPAD(S, 8, K); RPAD(K, 8); \
                 RPAD(S, LENGTH(K)); RPAD(K, 8, 'x'); RPAD(S, LENGTH(K), 'x'); RPAD(S, 8, K); \
                 INSTR(K, 'U'); INSTR(S, K); LOCATE('U', K); LOCATE(K, S); SUBSTR(K, 2); \
                 SUBSTR(S, LENGTH(K)); SUBSTR(K, 1, 2); SUBSTR(S, LENGTH(K), 2); \
                 SUBSTR(S, 1, LENGTH(K)); LEFT(K, 2); LEFT(S, LENGTH(K)); RIGHT(K, 2); \
                 RIGHT(S, LENGTH(K)); REPEAT(K, 2); REPEAT(S, LENGTH(K)); REVERSE(K); UPPER(K); \
                 LOWER(K); INITCAP(K); LENGTH(K)";
    let mut cases: Vec<(String, &str)> = calls
        .split("; ")
        .map(|call| (format!("SELECT ID, {call} FROM {{T}}"), SCAN_ITEM))
        .collect();
    // Exasol folds a call with a NULL literal to NULL before it sends the request.
    let folded = [
        "RTRIM(S, NULL)",
        "RTRIM(S, '')",
        "LPAD(S, 8, NULL)",
        "SUBSTR(S, NULL)",
        "INSTR(S, NULL)",
    ];
    cases.extend(folded.map(|call| (format!("SELECT ID, {call} FROM {{T}}"), "LAKEHOUSE_SCAN")));
    assert_match_native(&cases);
}

/// The first data file holds the empty values and no NULL, so a null count prunes it.
/// Scenario: A null test keeps the files that hold empty values
#[test]
fn null_test_keeps_file_of_empty_strings() {
    empty_string_vs();
    let mut conn = exa_conn();
    let rows = || EMPTY_STRING_FILES.iter().flat_map(|file| file.iter());
    let null_or_empty = |value: Option<&[u8]>| value.is_none_or(<[u8]>::is_empty);
    let strings = rows()
        .filter(|row| null_or_empty(row.1.map(str::as_bytes)))
        .count() as i64;
    let binaries = rows().filter(|row| null_or_empty(row.3)).count() as i64;
    let cases = [
        (EMPTY_STRING_VS, "S IS NULL", strings),
        (EMPTY_STRING_VS, "NOT (S IS NOT NULL)", strings),
        (EMPTY_STRING_VS, "B IS NULL", binaries),
        (EMPTY_STRING_TEXT_VS, "B IS NULL", binaries),
    ];
    for (vs, filter, expected) in cases {
        let query = format!(
            "SELECT COUNT(*) FROM {} WHERE {filter}",
            vs_table(vs, EMPTY_STRING_TABLE)
        );
        let plan = isolated_pushdown_statement(&mut conn, &query);
        assert!(
            plan.contains(SCAN_FILTER),
            "the scan must apply {filter} on {vs}: {plan}"
        );
        assert_eq!(
            conn.query_scalar_i64(&query),
            expected,
            "{filter} on {vs} counts the NULL rows and the rows of the file that holds only \
             empty values"
        );
    }
}

/// Without the ranking, the shard of the first data file ranks its two empty strings first and
/// cuts `event-01`, which the global top three needs.
/// Scenario: A per-shard top-N ranks a stored empty string as NULL
#[test]
fn per_shard_top_n_ranks_empty_string_as_null() {
    assert_match_native(&[
        (
            "SELECT S FROM {T} ORDER BY S ASC NULLS LAST LIMIT 3",
            SHARD_SORT,
        ),
        (
            "SELECT S FROM {T} ORDER BY S DESC NULLS FIRST LIMIT 5",
            SHARD_SORT,
        ),
    ]);
}
