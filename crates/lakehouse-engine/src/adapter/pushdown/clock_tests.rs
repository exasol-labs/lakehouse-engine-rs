use super::super::request::{FromClause, SortElement};
use super::*;
use crate::adapter::pushdown::test_support::{
    ast, inner_join, order_by_element, parsed_pushdown, pushdown_request, table_ref,
};
use crate::types::exasol_type::ExasolType;
use chrono::TimeZone as _;
use serde_json::Value as Json;
use vs_expression::test_support::{
    agg_item_expr, compare, date_lit, lit, num_lit, numbered_col, predicate_equal, scalar_fn,
    table_col, timestamp_lit,
};

/// The operands of the comparison `tree`.
fn operands(tree: &Expr) -> (&Expr, &Expr) {
    match tree {
        Expr::Compare { left, right, .. } => (left, right),
        other => panic!("not a comparison: {other:?}"),
    }
}

/// 2024-01-15 23:30:45.123456789 UTC: 18:30 on the 15th in New York (`TZ`), 08:30 on the 16th
/// in Tokyo (`SESSION_TIME_ZONE`), so each folded value shows the zone it was folded in.
fn instant() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2024, 1, 15, 23, 30, 45).unwrap()
        + chrono::Duration::nanoseconds(123_456_789)
}

fn clock(tz: Option<&str>, session_time_zone: Option<&str>) -> StatementClock {
    StatementClock::new(
        instant(),
        tz.map(str::to_string),
        session_time_zone.map(|name| TimeZone::exasol_name(name).expect("a zone name")),
    )
}

fn tokyo_clock() -> StatementClock {
    clock(Some("America/New_York"), Some("Asia/Tokyo"))
}

fn tstz_lit(value: &str) -> Json {
    lit("literal_timestamputc", value)
}

fn columns() -> [(&'static str, ExasolType); 2] {
    [("D", ExasolType::Date), ("TS", ExasolType::timestamp(3))]
}

fn ts() -> Json {
    numbered_col("T", 1, "TS")
}

/// The request's filter after `clock` folds `ts < right`.
fn folded_filter(clock: &StatementClock, right: Json) -> Result<Expr, UdfError> {
    let pushdown = pushdown_request("T", &columns())
        .filter(compare("predicate_less", ts(), right))
        .build_pushdown();
    let folded = clock.fold(pushdown)?;
    Ok(operands(folded.filter().expect("the filter stays"))
        .1
        .clone())
}

#[test]
fn folds_each_clock_function_into_its_literal() {
    let precision = |digits: u64| vec![num_lit(digits.to_string())];
    let cases = [
        (
            tokyo_clock(),
            scalar_fn("CURRENT_DATE", []),
            date_lit("2024-01-16"),
        ),
        (
            clock(Some("America/New_York"), None),
            scalar_fn("CURRENT_DATE", []),
            date_lit("2024-01-15"),
        ),
        (
            tokyo_clock(),
            scalar_fn("SYSDATE", []),
            date_lit("2024-01-15"),
        ),
        (
            tokyo_clock(),
            scalar_fn("SYSTIMESTAMP", []),
            timestamp_lit("2024-01-15 18:30:45.123"),
        ),
        (
            tokyo_clock(),
            scalar_fn("SYSTIMESTAMP", precision(6)),
            timestamp_lit("2024-01-15 18:30:45.123456"),
        ),
        (
            tokyo_clock(),
            scalar_fn("SYSTIMESTAMP", precision(0)),
            timestamp_lit("2024-01-15 18:30:45"),
        ),
        (
            tokyo_clock(),
            scalar_fn("LOCALTIMESTAMP", precision(3)),
            timestamp_lit("2024-01-16 08:30:45.123"),
        ),
        (
            tokyo_clock(),
            scalar_fn("CURRENT_TIMESTAMP", precision(9)),
            timestamp_lit("2024-01-16 08:30:45.123456789"),
        ),
        (
            clock(Some("UTC-7"), None),
            scalar_fn("CURRENT_TIMESTAMP", []),
            timestamp_lit("2024-01-16 06:30:45.123"),
        ),
    ];
    for (clock, call, expected) in cases {
        let folded = folded_filter(&clock, call.clone())
            .unwrap_or_else(|e| panic!("{call} must fold: {e:?}"));
        assert_eq!(folded, ast(&expected), "{call}");
    }
}

/// Exasol shows a projected TSTZ value in the session zone, so only a TSTZ-declared select-list
/// item keeps the UTC instant; anywhere else the value compares as the folding-zone wall clock.
#[test]
fn tstz_values_fold_to_the_wall_clock_outside_tstz_select_items() {
    let tstz = ExasolType::TimestampWithLocalTimeZone { precision: 3 };
    let pushdown = pushdown_request("T", &columns())
        .item(scalar_fn("CURRENT_TIMESTAMP", []), tstz)
        .item(tstz_lit("2024-03-01 09:00:00.000"), tstz)
        .item(scalar_fn("LOCALTIMESTAMP", []), ExasolType::timestamp(3))
        .filter(compare(
            "predicate_less",
            ts(),
            tstz_lit("2024-03-01 09:00:00.000"),
        ))
        .field(
            "having",
            compare("predicate_less", scalar_fn("CURRENT_TIMESTAMP", []), ts()),
        )
        .build_pushdown();

    let folded = tokyo_clock().fold(pushdown).expect("the request folds");

    assert_eq!(
        folded.select_items().expect("a select list"),
        [
            ast(&tstz_lit("2024-01-15 23:30:45.123")),
            ast(&tstz_lit("2024-03-01 09:00:00.000")),
            ast(&timestamp_lit("2024-01-16 08:30:45.123")),
        ]
    );
    assert_eq!(
        *operands(folded.filter().expect("the filter stays")).1,
        ast(&timestamp_lit("2024-03-01 18:00:00.000"))
    );
    assert_eq!(
        *operands(folded.having().expect("the HAVING stays")).0,
        ast(&timestamp_lit("2024-01-16 08:30:45.123"))
    );
}

#[test]
fn a_request_without_a_clock_or_tstz_node_never_reads_tz() {
    let unreadable = clock(None, Some("Asia/Tokyo"));
    let plain = timestamp_lit("2024-01-01 00:00:00");
    let folds = |node: Json| {
        folded_filter(&unreadable, node.clone())
            .unwrap_or_else(|e| panic!("{node} must fold without TZ: {e:?}"))
    };
    assert_eq!(folds(plain.clone()), ast(&plain));
    assert_eq!(
        folds(scalar_fn("CURRENT_DATE", [])),
        ast(&date_lit("2024-01-16"))
    );
    let error = folded_filter(&unreadable, scalar_fn("SYSDATE", []))
        .expect_err("SYSDATE needs the TZ zone");
    assert!(
        matches!(&error, UdfError::User(message) if message.contains("TZ is unset")),
        "{error:?}"
    );
}

/// A clock key folds into a constant, which no sort order needs, so the fold drops it as
/// parsing drops any constant key.
#[test]
fn a_folded_clock_sort_key_is_dropped() {
    let pushdown = pushdown_request("T", &columns())
        .field(
            "orderBy",
            serde_json::json!([
                order_by_element(scalar_fn("CURRENT_DATE", []), true, true),
                order_by_element(ts(), true, true),
            ]),
        )
        .build_pushdown();

    let folded = tokyo_clock().fold(pushdown).expect("the request folds");

    assert_eq!(
        folded.order_by(),
        [SortElement {
            expression: ast(&ts()),
            ascending: true,
            nulls_last: true,
        }]
    );
}

#[test]
fn tstz_values_fold_in_a_join_condition_and_an_aggregate_argument() {
    let (request, _) = pushdown_request("T", &columns())
        .table("U", &columns())
        .item(
            agg_item_expr(
                "MAX",
                compare(
                    "predicate_less",
                    table_col("T", "TS"),
                    scalar_fn("CURRENT_TIMESTAMP", []),
                ),
                false,
            ),
            ExasolType::Boolean,
        )
        .field(
            "from",
            inner_join(
                table_ref("T"),
                table_ref("U"),
                predicate_equal(table_col("T", "TS"), tstz_lit("2024-03-01 09:00:00.000")),
            ),
        )
        .build();

    let folded = tokyo_clock()
        .fold(parsed_pushdown(&request))
        .expect("the request folds");
    let Some(FromClause::Join { condition, .. }) = folded.from() else {
        panic!("the join stays: {:?}", folded.from());
    };
    assert_eq!(
        *operands(condition).1,
        ast(&timestamp_lit("2024-03-01 18:00:00.000"))
    );
    let Expr::Aggregate(max) = &folded.select_items().expect("a select list")[0] else {
        panic!("the MAX stays");
    };
    assert_eq!(
        *operands(&max.arguments[0]).1,
        ast(&timestamp_lit("2024-01-16 08:30:45.123"))
    );
}
