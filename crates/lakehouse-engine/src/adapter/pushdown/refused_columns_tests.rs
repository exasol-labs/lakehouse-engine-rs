use crate::adapter::pushdown::test_support::{predicate_equal, str_lit, table_col};

use super::*;
use crate::adapter::pushdown::test_support::{
    PushdownRequestBuilder, pushdown_request, unread_refused, user_message,
};
use crate::types::exasol_type::ExasolType;

fn shape_col() -> RefusedColumn {
    unread_refused("Parquet", "shape_col", None, "geometry")
}

const VARIANT_REASON: &str = "Delta column 'variant_col' has type 'variant', whose on-disk form is \
     an opaque (metadata, value) binary pair this engine cannot render as a meaningful value";

fn refused_column(column_name: &str, reason: &str) -> RefusedColumn {
    RefusedColumn {
        column_name: column_name.to_string(),
        reason: reason.to_string(),
    }
}

fn stats_all_types() -> PushdownRequestBuilder {
    pushdown_request(
        "STATS_ALL_TYPES",
        &[
            ("INT_COL", ExasolType::decimal(10, 0)),
            ("SHAPE_COL", ExasolType::WIDEST_VARCHAR),
            ("MAP_COL", ExasolType::WIDEST_VARCHAR),
            ("NESTED_STRUCT", ExasolType::WIDEST_VARCHAR),
            ("VARIANT_COL", ExasolType::WIDEST_VARCHAR),
        ],
    )
}

fn column_node(name: &str) -> Json {
    table_col("STATS_ALL_TYPES", name)
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[test]
fn a_request_touching_no_refused_column_is_admitted() {
    let int_col_present =
        serde_json::json!({"type": "predicate_is_not_null", "expression": column_node("INT_COL")});
    let request = stats_all_types()
        .field("type", "select")
        .filter(int_col_present)
        .column(column_node("INT_COL"))
        .build()
        .0;
    let projection = [ProjectionItem::from("INT_COL")];

    let gated = ensure_no_refused_column_referenced(&request, Some(&projection), &[shape_col()]);

    assert!(
        gated.is_ok(),
        "a request that neither reads nor emits SHAPE_COL must plan, even though the table \
         declares it, got: {:?}",
        gated.err()
    );
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[test]
fn a_table_with_no_refused_column_admits_every_request() {
    let request = stats_all_types()
        .field("type", serde_json::json!("select"))
        .column(column_node("SHAPE_COL"))
        .column(column_node("MAP_COL"))
        .build()
        .0;
    let projection = [
        ProjectionItem::from("SHAPE_COL"),
        ProjectionItem::from("MAP_COL"),
    ];

    let gated = ensure_no_refused_column_referenced(&request, Some(&projection), &[]);

    assert!(
        gated.is_ok(),
        "a format reader that refused nothing must gate nothing, got: {:?}",
        gated.err()
    );
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[test]
fn a_refused_column_buried_in_a_nested_predicate_is_refused_with_its_reason() {
    let request = stats_all_types()
        .field("type", serde_json::json!("select"))
        .filter(serde_json::json!({
            "type": "predicate_and",
            "expressions": [
                {"type": "predicate_is_not_null", "expression": column_node("INT_COL")},
                predicate_equal(
                    serde_json::json!({
                            "type": "function_scalar",
                            "name": "UPPER",
                            "arguments": [{
                                "type": "function_scalar_case",
                                "arguments": [column_node("SHAPE_COL")],
                            }],
                        }),
                    str_lit("x"),
                ),
            ],
        }))
        .column(column_node("INT_COL"))
        .build()
        .0;
    let projection = [ProjectionItem::from("INT_COL")];

    let err = ensure_no_refused_column_referenced(&request, Some(&projection), &[shape_col()])
        .expect_err("a filter reaching SHAPE_COL at any depth must be refused");

    let message = user_message(err);
    assert!(
        message.contains(&shape_col().reason),
        "refusal must carry the reader's own reason for the column, got: {message}"
    );
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[test]
fn a_full_row_projection_refuses_a_column_the_request_json_never_names() {
    let int_col_present =
        serde_json::json!({"type": "predicate_is_not_null", "expression": column_node("INT_COL")});
    let request = stats_all_types()
        .field("type", "select")
        .filter(int_col_present)
        .build()
        .0;
    let projection = [
        ProjectionItem::from("INT_COL"),
        ProjectionItem::from("SHAPE_COL"),
        ProjectionItem::from("MAP_COL"),
    ];

    let err = ensure_no_refused_column_referenced(&request, Some(&projection), &[shape_col()])
        .expect_err("a full-base-row projection emitting SHAPE_COL must be refused");

    let message = user_message(err);
    assert!(
        message.contains(&shape_col().reason),
        "refusal must carry the reader's own reason for the emitted column, got: {message}"
    );
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[test]
fn a_widened_projection_from_an_aggregate_select_list_is_not_unioned_into_the_touched_set() {
    let request = stats_all_types()
        .field("type", "select")
        .count_star()
        .build()
        .0;

    let gated = ensure_no_refused_column_referenced(&request, None, &[shape_col()]);

    assert!(
        gated.is_ok(),
        "COUNT(*) reads no column value, so a widened projection must not stand in \
         for \"columns this request touches\": {:?}",
        gated.err()
    );
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[test]
fn a_widened_projection_is_still_refused_when_the_request_itself_names_a_refused_column() {
    let request = stats_all_types()
        .field("type", serde_json::json!("select"))
        .item(
            serde_json::json!({
                "type": "function_aggregate",
                "name": "MAX",
                "arguments": [column_node("SHAPE_COL")],
                "distinct": false,
            }),
            ExasolType::WIDEST_VARCHAR,
        )
        .build()
        .0;

    let err = ensure_no_refused_column_referenced(&request, None, &[shape_col()])
        .expect_err("an aggregate whose argument names the refused column must be refused");

    assert!(
        user_message(err).contains(&shape_col().reason),
        "withholding the widened projection must not withhold the walk's own finding"
    );
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[test]
fn every_refused_column_a_request_touches_is_named_in_one_error() {
    let request = stats_all_types()
        .field("type", serde_json::json!("select"))
        .order_by(serde_json::json!([{
            "type": "order_by_element",
            "expression": column_node("VARIANT_COL"),
            "isAscending": true,
            "nullsLast": true,
        }]))
        .column(column_node("INT_COL"))
        .build()
        .0;
    let projection = [
        ProjectionItem::from("INT_COL"),
        ProjectionItem::from("SHAPE_COL"),
    ];

    let err = ensure_no_refused_column_referenced(
        &request,
        Some(&projection),
        &[shape_col(), refused_column("variant_col", VARIANT_REASON)],
    )
    .expect_err("a request reaching two refused columns must be refused");

    let message = user_message(err);
    let binary_position = message
        .find(&shape_col().reason)
        .expect("refusal must carry the emitted column's reason");
    let variant_position = message
        .find(VARIANT_REASON)
        .expect("refusal must carry the ordered-by column's reason");
    assert!(
        binary_position < variant_position,
        "one error must name both columns in schema order, got: {message}"
    );
}

#[test]
fn only_shape_col_refuses_requests_in_the_stats_all_types_shape() {
    let refused = [shape_col()];
    let nested_request = stats_all_types()
        .field("type", serde_json::json!("select"))
        .column(column_node("MAP_COL"))
        .column(column_node("NESTED_STRUCT"))
        .build()
        .0;
    let nested_projection = [
        ProjectionItem::from("MAP_COL"),
        ProjectionItem::from("NESTED_STRUCT"),
    ];

    let gated =
        ensure_no_refused_column_referenced(&nested_request, Some(&nested_projection), &refused);
    assert!(
        gated.is_ok(),
        "a rendered nested column must not be gated, got: {:?}",
        gated.err()
    );

    let binary_request = stats_all_types()
        .field("type", serde_json::json!("select"))
        .column(column_node("MAP_COL"))
        .column(column_node("SHAPE_COL"))
        .build()
        .0;

    let err = ensure_no_refused_column_referenced(&binary_request, None, &refused)
        .expect_err("a request naming SHAPE_COL must still be refused");
    assert!(user_message(err).contains(&shape_col().reason));
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[test]
fn a_lower_cased_column_reference_matches_a_refused_column() {
    let request = stats_all_types()
        .field("type", serde_json::json!("select"))
        .column(table_col("STATS_ALL_TYPES", "shape_col"))
        .build()
        .0;
    let projection = [ProjectionItem::from("INT_COL")];

    let err = ensure_no_refused_column_referenced(&request, Some(&projection), &[shape_col()])
        .expect_err("the request's case must not decide whether the gate matches");

    assert!(
        user_message(err).contains(&shape_col().reason),
        "a lower-cased reference must match a refused column of the same name"
    );
}
