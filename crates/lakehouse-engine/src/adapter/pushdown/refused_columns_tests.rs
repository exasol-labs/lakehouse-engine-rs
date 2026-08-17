use serde_json::json;

use super::*;

const BINARY_REASON: &str = "Delta column 'binary_col' has type 'binary', which this engine \
     refuses rather than casting to text; real JSON rendering is tracked as issue #350";
const MAP_REASON: &str = "Delta column 'map_col' has type 'map', which arrow-cast reports no \
     cast to text for; real JSON rendering is tracked as issue #350";

fn refused_column(column_name: &str, reason: &str) -> RefusedColumn {
    RefusedColumn {
        column_name: column_name.to_string(),
        reason: reason.to_string(),
    }
}

fn user_message(err: UdfError) -> String {
    match err {
        UdfError::User(message) => message,
        other => panic!("expected UdfError::User, got {other:?}"),
    }
}

/// The `stats_all_types` shape: Exasol's catalog declares both the mappable and
/// the refused columns, so a request can name either.
fn involved_tables() -> Json {
    json!([{
        "name": "STATS_ALL_TYPES",
        "columns": [
            {"name": "INT_COL", "dataType": {"type": "decimal", "precision": 10, "scale": 0}},
            {"name": "BINARY_COL", "dataType": {"type": "varchar", "size": 2000000}},
            {"name": "MAP_COL", "dataType": {"type": "varchar", "size": 2000000}},
        ],
    }])
}

fn column_node(name: &str) -> Json {
    json!({"type": "column", "name": name, "tableName": "STATS_ALL_TYPES"})
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[test]
fn a_request_touching_no_refused_column_is_admitted() {
    let request = json!({
        "involvedTables": involved_tables(),
        "pushdownRequest": {
            "type": "select",
            "selectList": [column_node("INT_COL")],
            "filter": {"type": "predicate_is_not_null", "expression": column_node("INT_COL")},
        },
    });
    let projection = [ProjectionItem::Column("INT_COL".to_string())];

    let gated = ensure_no_refused_column_referenced(
        &request,
        Some(&projection),
        &[refused_column("binary_col", BINARY_REASON)],
    );

    assert!(
        gated.is_ok(),
        "a request that neither reads nor emits BINARY_COL must plan, even though the table \
         declares it, got: {:?}",
        gated.err()
    );
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[test]
fn a_table_with_no_refused_column_admits_every_request() {
    let request = json!({
        "involvedTables": involved_tables(),
        "pushdownRequest": {
            "type": "select",
            "selectList": [column_node("BINARY_COL"), column_node("MAP_COL")],
        },
    });
    let projection = [
        ProjectionItem::Column("BINARY_COL".to_string()),
        ProjectionItem::Column("MAP_COL".to_string()),
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
    let request = json!({
        "involvedTables": involved_tables(),
        "pushdownRequest": {
            "type": "select",
            "selectList": [column_node("INT_COL")],
            "filter": {
                "type": "predicate_and",
                "expressions": [
                    {"type": "predicate_is_not_null", "expression": column_node("INT_COL")},
                    {
                        "type": "predicate_equal",
                        "left": {
                            "type": "function_scalar",
                            "name": "UPPER",
                            "arguments": [{
                                "type": "function_scalar_case",
                                "arguments": [column_node("BINARY_COL")],
                            }],
                        },
                        "right": {"type": "literal_string", "value": "x"},
                    },
                ],
            },
        },
    });
    let projection = [ProjectionItem::Column("INT_COL".to_string())];

    let err = ensure_no_refused_column_referenced(
        &request,
        Some(&projection),
        &[refused_column("binary_col", BINARY_REASON)],
    )
    .expect_err("a filter reaching BINARY_COL at any depth must be refused");

    let message = user_message(err);
    assert!(
        message.contains(BINARY_REASON),
        "refusal must carry the reader's own reason for the column, got: {message}"
    );
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[test]
fn a_full_row_projection_refuses_a_column_the_request_json_never_names() {
    let request = json!({
        "involvedTables": involved_tables(),
        "pushdownRequest": {
            "type": "select",
            "filter": {"type": "predicate_is_not_null", "expression": column_node("INT_COL")},
        },
    });
    let projection = [
        ProjectionItem::Column("INT_COL".to_string()),
        ProjectionItem::Column("BINARY_COL".to_string()),
        ProjectionItem::Column("MAP_COL".to_string()),
    ];

    let err = ensure_no_refused_column_referenced(
        &request,
        Some(&projection),
        &[refused_column("binary_col", BINARY_REASON)],
    )
    .expect_err("a full-base-row projection emitting BINARY_COL must be refused");

    let message = user_message(err);
    assert!(
        message.contains(BINARY_REASON),
        "refusal must carry the reader's own reason for the emitted column, got: {message}"
    );
}

/// Scenario: A refused column refuses only the requests that read or emit it
///
/// `COUNT(*)` widens `extract_projection`'s output to the synthetic full base
/// row (an aggregate select list, per `project_columns`), but names no column
/// anywhere in the request JSON. Unlike a real `SELECT *` (whose projection IS
/// the emitted row, proven above), a widened projection is withheld — passed as
/// `None` — because it is a placeholder `build_dispatch_sql` never reads for a
/// decomposable aggregate; unioning it would refuse a query that reads no column
/// value at all.
#[test]
fn a_widened_projection_from_an_aggregate_select_list_is_not_unioned_into_the_touched_set() {
    let request = json!({
        "involvedTables": involved_tables(),
        "pushdownRequest": {
            "type": "select",
            "selectList": [
                {"type": "function_aggregate", "name": "COUNT", "arguments": [], "distinct": false},
            ],
        },
    });

    let gated = ensure_no_refused_column_referenced(
        &request,
        None,
        &[refused_column("binary_col", BINARY_REASON)],
    );

    assert!(
        gated.is_ok(),
        "COUNT(*) reads no column value, so a widened projection must not stand in \
         for \"columns this request touches\": {:?}",
        gated.err()
    );
}

/// Scenario: A refused column refuses only the requests that read or emit it
///
/// The complementary half of the widened branch, and the whole safety argument for
/// withholding a widened projection: the blind walk alone must still refuse an
/// aggregate whose own `arguments` name a refused column. If the walk ever stopped
/// reaching aggregate arguments, filters, or GROUP BY items on a widened request,
/// `MAX(BINARY_COL)` would be silently admitted and every admit-side test would
/// still pass.
#[test]
fn a_widened_projection_is_still_refused_when_the_request_itself_names_a_refused_column() {
    let request = json!({
        "involvedTables": involved_tables(),
        "pushdownRequest": {
            "type": "select",
            "selectList": [{
                "type": "function_aggregate",
                "name": "MAX",
                "arguments": [column_node("BINARY_COL")],
                "distinct": false,
            }],
        },
    });

    let err = ensure_no_refused_column_referenced(
        &request,
        None,
        &[refused_column("binary_col", BINARY_REASON)],
    )
    .expect_err("an aggregate whose argument names the refused column must be refused");

    assert!(
        user_message(err).contains(BINARY_REASON),
        "withholding the widened projection must not withhold the walk's own finding"
    );
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[test]
fn every_refused_column_a_request_touches_is_named_in_one_error() {
    let request = json!({
        "involvedTables": involved_tables(),
        "pushdownRequest": {
            "type": "select",
            "selectList": [column_node("INT_COL")],
            "orderBy": [{
                "type": "order_by_element",
                "expression": column_node("MAP_COL"),
                "isAscending": true,
                "nullsLast": true,
            }],
        },
    });
    let projection = [
        ProjectionItem::Column("INT_COL".to_string()),
        ProjectionItem::Column("BINARY_COL".to_string()),
    ];

    let err = ensure_no_refused_column_referenced(
        &request,
        Some(&projection),
        &[
            refused_column("binary_col", BINARY_REASON),
            refused_column("map_col", MAP_REASON),
        ],
    )
    .expect_err("a request reaching two refused columns must be refused");

    let message = user_message(err);
    let binary_position = message
        .find(BINARY_REASON)
        .expect("refusal must carry the emitted column's reason");
    let map_position = message
        .find(MAP_REASON)
        .expect("refusal must carry the ordered-by column's reason");
    assert!(
        binary_position < map_position,
        "one error must name both columns in schema order, got: {message}"
    );
}

/// Scenario: A refused column refuses only the requests that read or emit it
#[test]
fn a_lower_cased_column_reference_matches_a_refused_column() {
    let request = json!({
        "involvedTables": involved_tables(),
        "pushdownRequest": {
            "type": "select",
            "selectList": [json!({
                "type": "column", "name": "binary_col", "tableName": "STATS_ALL_TYPES",
            })],
        },
    });
    let projection = [ProjectionItem::Column("INT_COL".to_string())];

    let err = ensure_no_refused_column_referenced(
        &request,
        Some(&projection),
        &[refused_column("binary_col", BINARY_REASON)],
    )
    .expect_err("the request's case must not decide whether the gate matches");

    assert!(
        user_message(err).contains(BINARY_REASON),
        "a lower-cased reference must match a refused column of the same name"
    );
}
