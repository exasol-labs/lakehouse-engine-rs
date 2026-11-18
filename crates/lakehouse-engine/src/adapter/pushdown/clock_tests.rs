use super::*;
use crate::adapter::column_notes::ColumnDeclaration;
use crate::adapter::pushdown::test_support::{
    PushdownRequestBuilder, inner_join, nullable_field, order_by_element, parsed_pushdown,
    pushdown_request, table_ref, with_notes,
};
use crate::types::exasol_type::ExasolType;
use chrono::TimeZone as _;
use vs_expression::test_support::{
    agg_item_expr, between, case_when, compare, date_lit, in_list, lit, num_lit, numbered_col,
    predicate_equal, scalar_fn, table_col, timestamp_lit,
};

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

/// The request JSON `clock` folds on Exasol 2025.
fn folded(clock: &StatementClock, request: &Json) -> Result<Json, UdfError> {
    folded_on(ExasolVersion::Exasol2025, clock, request)
}

fn folded_on(
    version: ExasolVersion,
    clock: &StatementClock,
    request: &Json,
) -> Result<Json, UdfError> {
    Ok(clock
        .fold(request, parsed_pushdown(request), version)?
        .json()
        .clone())
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
fn folded_filter(clock: &StatementClock, right: Json) -> Result<Json, UdfError> {
    let (request, _) = pushdown_request("T", &columns())
        .filter(compare("predicate_less", ts(), right))
        .build();
    Ok(folded(clock, &request)?["filter"]["right"].clone())
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
        assert_eq!(folded, expected, "{call}");
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
        .build();

    let json = folded(&tokyo_clock(), &pushdown.0).expect("the request folds");

    assert_eq!(
        json["selectList"],
        serde_json::json!([
            tstz_lit("2024-01-15 23:30:45.123"),
            tstz_lit("2024-03-01 09:00:00.000"),
            timestamp_lit("2024-01-16 08:30:45.123"),
        ])
    );
    assert_eq!(
        json["filter"]["right"],
        timestamp_lit("2024-03-01 18:00:00.000")
    );
    assert_eq!(
        json["having"]["left"],
        timestamp_lit("2024-01-16 08:30:45.123")
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
    assert_eq!(folds(plain.clone()), plain);
    assert_eq!(folds(scalar_fn("CURRENT_DATE", [])), date_lit("2024-01-16"));
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
        .build();

    let json = folded(&tokyo_clock(), &pushdown.0).expect("the request folds");

    assert_eq!(
        json["orderBy"],
        serde_json::json!([order_by_element(ts(), true, true)])
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

    let json = folded(&tokyo_clock(), &request).expect("the request folds");

    assert_eq!(
        json["from"]["condition"]["right"],
        timestamp_lit("2024-03-01 18:00:00.000")
    );
    assert_eq!(
        json["selectList"][0]["arguments"][0]["right"],
        timestamp_lit("2024-01-16 08:30:45.123")
    );
}

fn instant_columns() -> [(&'static str, ExasolType); 3] {
    [
        ("D", ExasolType::Date),
        ("TS", ExasolType::timestamp(3)),
        ("TZ", ExasolType::timestamp(3)),
    ]
}

fn tz() -> Json {
    numbered_col("T", 2, "TZ")
}

/// The request `build` makes over `T`, whose `TZ` column CREATE declared from the source type
/// `tag`.
fn with_tz_source(
    tag: &str,
    build: impl FnOnce(PushdownRequestBuilder) -> PushdownRequestBuilder,
) -> Json {
    let (request, _) = build(pushdown_request("T", &instant_columns())).build();
    let declaration = ColumnDeclaration {
        source_type: nullable_field("tz", tag),
        partition: None,
        refused: None,
    };
    with_notes(request, "T", &[("TZ".to_string(), declaration.encode())])
}

/// A comparison shape, the filter it makes over `TZ` and a value, and where the value lands.
type Shape = (&'static str, fn(Json) -> Json, &'static [&'static str]);

#[test]
fn tstz_values_compared_with_an_instant_column_fold_to_utc() {
    let literal = tstz_lit("2024-03-01 09:00:00.000");
    let utc_literal = timestamp_lit("2024-03-01 09:00:00.000");
    let tokyo_literal = timestamp_lit("2024-03-01 18:00:00.000");
    let utc_now = timestamp_lit("2024-01-15 23:30:45.123");
    let tokyo_now = timestamp_lit("2024-01-16 08:30:45.123");
    let new_york_now = timestamp_lit("2024-01-15 18:30:45.123");
    let (exasol8, exasol2025) = (ExasolVersion::Exasol8, ExasolVersion::Exasol2025);
    // (value, version, folded against an instant column, folded against any other column)
    let values = [
        (literal, exasol2025, &utc_literal, &tokyo_literal),
        (
            scalar_fn("CURRENT_TIMESTAMP", []),
            exasol2025,
            &utc_now,
            &tokyo_now,
        ),
        (
            scalar_fn("LOCALTIMESTAMP", []),
            exasol8,
            &utc_now,
            &tokyo_now,
        ),
        (
            scalar_fn("LOCALTIMESTAMP", []),
            exasol2025,
            &tokyo_now,
            &tokyo_now,
        ),
        (
            scalar_fn("SYSTIMESTAMP", []),
            exasol2025,
            &new_york_now,
            &new_york_now,
        ),
    ];
    let shapes: [Shape; 6] = [
        (
            "TZ = v",
            |v| compare("predicate_equal", tz(), v),
            &["/filter/right"],
        ),
        (
            "v <> TZ",
            |v| compare("predicate_notequal", v, tz()),
            &["/filter/left"],
        ),
        (
            "TZ < v",
            |v| compare("predicate_less", tz(), v),
            &["/filter/right"],
        ),
        (
            "v < TZ",
            |v| compare("predicate_less", v, tz()),
            &["/filter/left"],
        ),
        (
            "TZ BETWEEN v AND v",
            |v| between(tz(), v.clone(), v),
            &["/filter/left", "/filter/right"],
        ),
        (
            "TZ IN (v)",
            |v| in_list(tz(), [v]),
            &["/filter/arguments/0"],
        ),
    ];
    let tags = [
        ("timestamptz_ms", true),
        ("timestamptz_us", true),
        ("timestamptz_ns", true),
        ("timestamp_us", false),
    ];
    for (tag, instant) in tags {
        for (value, version, against_instant, otherwise) in &values {
            let expected = if instant { against_instant } else { otherwise };
            for (shape, filter, pointers) in &shapes {
                let request = with_tz_source(tag, |b| b.filter(filter(value.clone())));
                let json = folded_on(*version, &tokyo_clock(), &request)
                    .unwrap_or_else(|e| panic!("{tag} {shape} {value} must fold: {e:?}"));
                for pointer in *pointers {
                    assert_eq!(
                        json.pointer(pointer),
                        Some(*expected),
                        "{tag} {version:?} {shape} with v = {value}"
                    );
                }
            }
        }
    }
}

/// Only a TSTZ value set directly against the instant column folds to UTC: an expression over
/// the column, and every value inside a TSTZ-typed select-list item, keep the folding-zone rule.
#[test]
fn only_a_value_compared_directly_with_an_instant_column_folds_to_utc() {
    let tstz = ExasolType::TimestampWithLocalTimeZone { precision: 3 };
    let now = || scalar_fn("CURRENT_TIMESTAMP", []);
    let request = with_tz_source("timestamptz_us", |b| {
        b.item(compare("predicate_less", tz(), now()), ExasolType::Boolean)
            .item(
                case_when([compare("predicate_less", tz(), now())], [now(), tz()]),
                tstz,
            )
            .filter(compare(
                "predicate_less",
                scalar_fn("ADD_HOURS", [tz(), num_lit("1")]),
                tstz_lit("2024-03-01 09:00:00.000"),
            ))
            .field("having", compare("predicate_less", now(), tz()))
    });

    let json = folded(&tokyo_clock(), &request).expect("the request folds");

    let utc_now = timestamp_lit("2024-01-15 23:30:45.123");
    assert_eq!(json["selectList"][0]["right"], utc_now);
    assert_eq!(
        json["selectList"][1]["arguments"][0]["right"],
        tstz_lit("2024-01-15 23:30:45.123")
    );
    assert_eq!(json["having"]["left"], utc_now);
    assert_eq!(
        json["filter"]["right"],
        timestamp_lit("2024-03-01 18:00:00.000")
    );
}

#[test]
fn a_tstz_value_compared_with_an_instant_column_never_reads_tz() {
    let unreadable = clock(None, None);
    let request = with_tz_source("timestamptz_us", |b| {
        b.filter(compare(
            "predicate_less",
            tz(),
            scalar_fn("CURRENT_TIMESTAMP", []),
        ))
    });
    let json = folded(&unreadable, &request).expect("an instant comparison folds without TZ");
    assert_eq!(
        json["filter"]["right"],
        timestamp_lit("2024-01-15 23:30:45.123")
    );
}
