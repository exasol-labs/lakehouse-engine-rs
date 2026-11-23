use super::*;
use serde_json::json;

fn request(fields: Json) -> PushdownRequest {
    let mut json = json!({"selectListDataTypes": []});
    for (key, value) in fields.as_object().expect("fields are an object") {
        json[key] = value.clone();
    }
    PushdownRequest::parse(json).expect("the fixture parses")
}

fn column(name: &str) -> Json {
    json!({"type": "column", "name": name, "tableName": "T"})
}

#[test]
fn limit_and_offset_are_read_from_the_limit_clause() {
    let cases = [
        (json!({}), None, 0),
        (json!({"numElements": 42, "offset": 3}), None, 0),
        (json!({"limit": {"numElements": 42}}), Some(42), 0),
        (
            json!({"limit": {"numElements": 12, "offset": 3}}),
            Some(12),
            3,
        ),
    ];
    for (fields, limit, offset) in cases {
        let pushdown = request(fields.clone());
        assert_eq!(pushdown.limit(), limit, "{fields}");
        assert_eq!(pushdown.offset(), offset, "{fields}");
    }
}

#[test]
fn a_json_null_clause_is_an_absent_clause() {
    let pushdown = request(json!({"filter": null, "having": null, "groupBy": null}));
    assert_eq!(pushdown.filter(), None);
    assert_eq!(pushdown.having(), None);
    assert!(pushdown.group_by().is_empty());
    assert_eq!(pushdown.clauses(&Clause::ALL).count(), 0);
}

#[test]
fn clauses_yields_only_the_requested_clauses_the_request_carries() {
    let pushdown = request(json!({
        "filter": column("A"),
        "groupBy": [column("B")],
        "limit": {"numElements": 1},
    }));
    let clauses: Vec<&Json> = pushdown.clauses(&Clause::ALL).collect();
    assert_eq!(clauses, [&column("A"), &json!([column("B")])]);
    assert_eq!(
        pushdown.clauses(&Clause::WRAPPER).collect::<Vec<_>>(),
        [&json!([column("B")])],
        "the wrapper clauses leave the WHERE out"
    );
}

#[test]
fn map_clauses_rewrites_only_the_named_clauses_it_carries() {
    let pushdown = request(json!({"filter": column("A"), "having": column("B")}));
    let rewritten = pushdown.map_clauses(&[Clause::Filter, Clause::OrderBy], |_| column("Z"));
    assert_eq!(rewritten.filter(), Some(&column("Z")));
    assert_eq!(rewritten.having(), Some(&column("B")));
    assert!(
        rewritten.order_by().is_empty(),
        "an absent clause stays absent"
    );
}

#[test]
fn a_rewrite_that_makes_every_sort_key_constant_drops_the_order_by() {
    let pushdown = request(json!({
        "orderBy": [{"expression": column("A"), "isAscending": true, "nullsLast": true}],
    }));
    let rewritten = pushdown.map_clauses(&[Clause::OrderBy], |order_by| {
        let mut order_by = order_by.clone();
        order_by[0]["expression"] = json!({"type": "literal_exactnumeric", "value": "1"});
        order_by
    });
    assert!(rewritten.order_by().is_empty());
}

#[test]
fn try_map_clauses_names_each_clause_and_stops_at_the_first_error() {
    let pushdown = request(json!({"filter": column("A"), "having": column("B")}));
    let mut seen = Vec::new();
    let result = pushdown.try_map_clauses(&Clause::ALL, |clause, _| {
        seen.push(clause);
        Err(clause)
    });
    assert_eq!(result.err(), Some(Clause::Filter));
    assert_eq!(seen, [Clause::Filter]);
}

#[test]
fn with_clause_sets_a_clause_and_keeps_the_select_types() {
    let pushdown = PushdownRequest::parse(json!({
        "selectList": [column("A")],
        "selectListDataTypes": [{"type": "BOOLEAN"}],
    }))
    .expect("the fixture parses");
    let filtered = pushdown.with_clause(Clause::Filter, column("B"));
    assert_eq!(filtered.filter(), Some(&column("B")));
    assert_eq!(filtered.select_types(), pushdown.select_types());
}

#[test]
fn selecting_keeps_only_the_where() {
    let pushdown = request(json!({
        "filter": column("A"),
        "groupBy": [column("B")],
        "limit": {"numElements": 1},
    }));
    let selected = pushdown
        .selecting(vec![column("C")], &[ExasolType::Boolean])
        .expect("the selection parses");
    assert_eq!(selected.select_items(), Some([column("C")].as_slice()));
    assert_eq!(selected.select_types(), [ExasolType::Boolean]);
    assert_eq!(selected.filter(), Some(&column("A")));
    assert!(selected.group_by().is_empty());
    assert_eq!(selected.limit(), None);
}
