//! `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, and `ADD_MINUTES` over the typed table: each query over
//! the virtual table must match the same query over a native copy, and the scan must compute the
//! forms it advertises.

use crate::common::e2e_harness::PathCheck::{Has, Lacks, Unchecked};
use crate::common::e2e_harness::*;
use crate::common::exasol_ws::ExaConn;
use crate::fixtures::{TYPED_COLUMNS, lakehouse_vs, typed_table};
use chrono::{Duration, NaiveDate};

const FUNCTIONS: [&str; 4] = ["ADD_DAYS", "ADD_WEEKS", "ADD_HOURS", "ADD_MINUTES"];

/// A leap day, a month end, and a year end sit among the arguments.
const ARGUMENTS: [&str; 8] = [
    "C_DATE",
    "C_TS",
    "DATE '2024-02-29'",
    "DATE '2023-12-31'",
    "TIMESTAMP '2024-01-31 23:59:59.999999'",
    "TIMESTAMP '2024-12-31 23:30:00.5'",
    "CAST(C_TS AS DATE)",
    "ADD_DAYS(C_DATE, 1)",
];

const COUNTS: [&str; 6] = [
    "ID",
    "C_DOUBLE",
    "-C_DOUBLE",
    "C_DECIMAL_A",
    "C_BOOL",
    "NULL",
];

/// The counts a filter, a grouping, and an aggregate repeat: an integer, a DOUBLE, and NULL.
const REPEATED_COUNTS: [&str; 3] = ["ID", "C_DOUBLE", "NULL"];

const SCAN_FUNCTION: &str = "vs_add_";

struct DateAddCase {
    query: String,
    path: PathCheck<'static>,
}

impl DateAddCase {
    fn new(query: impl Into<String>, path: PathCheck<'static>) -> Self {
        Self {
            query: query.into(),
            path,
        }
    }
}

/// A call over a literal argument and a NULL count never reaches the scan, because Exasol folds
/// it to NULL.
fn scan_path(argument: &str, count: &str) -> PathCheck<'static> {
    if count == "NULL" && (argument.starts_with("DATE") || argument.starts_with("TIMESTAMP")) {
        Unchecked
    } else {
        Has(SCAN_FUNCTION)
    }
}

fn calls<'a>(counts: &'a [&'a str]) -> impl Iterator<Item = (String, PathCheck<'static>)> + 'a {
    FUNCTIONS.into_iter().flat_map(move |function| {
        ARGUMENTS.into_iter().flat_map(move |argument| {
            counts.iter().map(move |count| {
                (
                    format!("{function}({argument}, {count})"),
                    scan_path(argument, count),
                )
            })
        })
    })
}

fn row_cases() -> Vec<DateAddCase> {
    let mut cases = Vec::new();
    for (call, path) in calls(&COUNTS) {
        cases.push(DateAddCase::new(
            format!("SELECT ID, {call} FROM {{T}}"),
            path,
        ));
    }
    for (call, path) in calls(&REPEATED_COUNTS) {
        cases.push(DateAddCase::new(
            format!("SELECT ID FROM {{T}} WHERE {call} > TIMESTAMP '2024-01-03 12:00:00'"),
            path,
        ));
        cases.push(DateAddCase::new(
            format!("SELECT {call}, COUNT(*) FROM {{T}} GROUP BY {call}"),
            path,
        ));
        cases.push(DateAddCase::new(
            format!("SELECT MAX({call}) FROM {{T}}"),
            path,
        ));
    }
    // A NULL argument returns NULL before a count beyond the limits is checked.
    cases.extend(
        [
            "SELECT ID, ADD_DAYS(C_DATE, C_DOUBLE * 1e300) FROM {T} WHERE C_DATE IS NULL",
            "SELECT ID, ADD_WEEKS(C_TS, C_DOUBLE * 1e300) FROM {T} WHERE C_TS IS NULL",
            "SELECT ID, ADD_HOURS(C_DATE, C_DOUBLE * 1e300) FROM {T} WHERE C_DATE IS NULL",
            "SELECT ID, ADD_MINUTES(C_TS, C_DOUBLE * 1e300) FROM {T} WHERE C_TS IS NULL",
        ]
        .map(|query| DateAddCase::new(query, Has(SCAN_FUNCTION))),
    );
    cases.extend(
        [
            "SELECT ID, ADD_DAYS(CASE WHEN ID > 2 THEN C_DATE END, 1) FROM {T}",
            "SELECT ID, ADD_DAYS(CASE WHEN ID > 2 THEN C_TS END, 1) FROM {T}",
        ]
        .map(|query| DateAddCase::new(query, Has(SCAN_FUNCTION))),
    );
    // A scalar over a split aggregate renders in Exasol's merge wrapper.
    cases.extend(
        [
            "SELECT ADD_DAYS(MAX(C_DATE), 1) FROM {T}",
            "SELECT ADD_DAYS(MAX(C_TS), 1) FROM {T}",
            "SELECT ADD_DAYS(MIN(C_DATE), 1) FROM {T}",
            "SELECT ADD_DAYS(MIN(C_TS), 1) FROM {T}",
        ]
        .map(|query| DateAddCase::new(query, Unchecked)),
    );
    cases
}

/// The literal is 100 nanoseconds after the result for `C_DATE` 2024-01-01, which the scan's whole
/// microseconds never equal.
fn finer_literal_cases() -> Vec<DateAddCase> {
    [
        "SELECT ID FROM {T} WHERE ADD_HOURS(C_DATE, 1) = TIMESTAMP '2024-01-01 01:00:00.0000001'",
        "SELECT ID FROM {T} WHERE TIMESTAMP '2024-01-01 01:00:00.0000001' = ADD_HOURS(C_DATE, 1)",
        "SELECT ID FROM {T} WHERE ADD_HOURS(C_DATE, 1) >= TIMESTAMP '2024-01-01 01:00:00.0000001'",
        "SELECT ID FROM {T} WHERE ADD_HOURS(C_DATE, 1) BETWEEN TIMESTAMP '2024-01-01 01:00:00.0000001' AND TIMESTAMP '2024-01-02 01:00:00'",
        "SELECT ID FROM {T} WHERE ADD_MINUTES(C_DATE, 60) IN (TIMESTAMP '2024-01-01 01:00:00.0000001')",
    ]
    .map(|query| DateAddCase::new(query, Has(SCAN_FUNCTION)))
    .into()
}

fn declined_cases() -> Vec<DateAddCase> {
    [
        "SELECT ID, ADD_DAYS(CASE WHEN ID > 2 THEN C_DATE ELSE DATE '2024-03-01' END, 1) FROM {T}",
        "SELECT ID, ADD_HOURS(DATE_TRUNC('month', C_DATE), 1) FROM {T}",
        "SELECT ID FROM {T} WHERE ADD_DAYS(DATE_TRUNC('month', C_TS), 1) > DATE '2024-01-02'",
    ]
    .map(|query| DateAddCase::new(query, Lacks(SCAN_FUNCTION)))
    .into()
}

/// Overflow past 9999-12-31 and underflow before 0001-01-01, which native Exasol raises.
fn error_cases() -> Vec<DateAddCase> {
    let mut cases = Vec::new();
    for call in [
        "ADD_DAYS(C_DATE, 3000000)",
        "ADD_WEEKS(C_DATE, 500000)",
        "ADD_DAYS(C_DATE, -800000)",
        "ADD_HOURS(C_DATE, ID * 1e15)",
        "ADD_MINUTES(C_DATE, ID * -1e15)",
        "ADD_HOURS(C_DATE, ID * 1e30)",
        "ADD_MINUTES(C_DATE, ID * -1e30)",
    ] {
        for query in [
            format!("SELECT ID, {call} FROM {{T}}"),
            format!("SELECT ID FROM {{T}} WHERE {call} IS NOT NULL"),
            format!("SELECT MAX({call}) FROM {{T}}"),
        ] {
            cases.push(DateAddCase::new(query, Has(SCAN_FUNCTION)));
        }
    }
    // Exasol fails a constant count beyond 36 digits of nanoseconds before it reads a row.
    cases.extend(
        [
            "SELECT ID, ADD_HOURS(C_DATE, 1e24) FROM {T}",
            "SELECT ID, ADD_MINUTES(C_DATE, -1e30) FROM {T}",
            "SELECT ID, ADD_HOURS(C_DATE, 1e30) FROM {T} WHERE C_DATE IS NULL",
        ]
        .map(|query| DateAddCase::new(query, Lacks(SCAN_FUNCTION))),
    );
    cases
}

fn mismatch(conn: &mut ExaConn, table: &str, case: &DateAddCase, expect: Expect) -> Option<String> {
    native_copy_mismatch(conn, table, NATIVE_COPY, &case.query, case.path, expect)
}

/// Scenario: The typed pass tells a DATE from a TIMESTAMP
/// Scenario: ADD_DAYS and ADD_WEEKS over a DATE return a DATE from the scan
/// Scenario: ADD_HOURS and ADD_MINUTES over a DATE return a timestamp from the scan
/// Scenario: The four functions over a TIMESTAMP keep its fractional seconds in the scan
/// Scenario: The scan rounds a fractional count as Exasol does
/// Scenario: A result outside Exasol's datetime range fails the query
/// Scenario: A comparison with a TIMESTAMP literal finer than a microsecond returns native rows
/// Scenario: A form the scan cannot match runs in Exasol
/// Scenario: The four functions render verbatim in the Exasol dialect
#[test]
fn date_add_functions_match_native() {
    lakehouse_vs();
    let table = typed_table();
    let mut conn = exa_conn();
    create_native_copy(&mut conn, &table, TYPED_COLUMNS);

    let rows = row_cases()
        .into_iter()
        .chain(finer_literal_cases())
        .chain(declined_cases())
        .map(|case| (case, Expect::Rows));
    let errors = error_cases().into_iter().map(|case| (case, Expect::Error));
    let cases: Vec<_> = rows.chain(errors).collect();

    let failures: Vec<String> = cases
        .iter()
        .filter_map(|(case, expect)| mismatch(&mut conn, &table, case, *expect))
        .collect();

    assert!(
        failures.is_empty(),
        "{} of {} cases differ:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}

/// Scenario: A date before 1582-10-15 follows the proleptic Gregorian calendar in the scan
#[test]
fn date_add_before_the_gregorian_switch_follows_the_proleptic_calendar() {
    lakehouse_vs();
    let table = typed_table();
    let mut conn = exa_conn();
    let query =
        format!("SELECT ADD_DAYS(C_DATE, -200000) FROM {table} WHERE C_DATE = DATE '2024-01-01'");

    let expected = (NaiveDate::from_ymd_opt(2024, 1, 1).expect("a valid date")
        - Duration::days(200_000))
    .format("%Y-%m-%d")
    .to_string();
    let columns = conn.query_columns(&query);
    let returned: Vec<String> = columns[0].iter().map(value_to_string).collect();

    assert!(
        !returned.is_empty() && returned.iter().all(|date| *date == expected),
        "ADD_DAYS(2024-01-01, -200000) must return the proleptic Gregorian date {expected} in \
         every row, got {returned:?}"
    );
    let pushdown = isolated_pushdown_statement(&mut conn, &query);
    assert!(
        pushdown.contains("vs_add_days"),
        "the scan must compute ADD_DAYS: {pushdown}"
    );
}
