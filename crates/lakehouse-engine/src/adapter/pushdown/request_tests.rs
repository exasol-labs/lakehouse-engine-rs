use super::*;
use crate::adapter::pushdown::test_support::user_message;
use serde_json::json;

/// `base` with each of `fields` set.
fn with(mut base: Json, fields: Json) -> Json {
    for (key, value) in fields.as_object().expect("fields are an object") {
        base[key] = value.clone();
    }
    base
}

fn request(fields: Json) -> PushdownRequest {
    PushdownRequest::parse(&with(json!({"selectListDataTypes": []}), fields))
        .expect("the fixture parses")
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
    assert_eq!(pushdown.expressions(&Clause::ALL).len(), 0);
}

#[test]
fn a_malformed_request_fails_naming_its_field() {
    let column = json!({"type": "column", "name": "ID", "tableName": "T"});
    let table = json!({"type": "table", "name": "T"});
    let pushdown = |fields: Json| {
        let pushdown = with(json!({"selectListDataTypes": [], "from": table}), fields);
        json!({"pushdownRequest": pushdown})
    };
    let join = |fields: Json| {
        let join = json!({
            "type": "join", "join_type": "inner", "left": table, "right": table, "condition": column,
        });
        pushdown(json!({"from": with(join, fields)}))
    };
    let sort = |element: Json| pushdown(json!({"orderBy": [element]}));
    let cases = [
        (
            pushdown(json!({"from": {"name": "T"}})),
            "the pushdown request's from.type is missing",
        ),
        (
            pushdown(json!({"from": {"type": "subselect"}})),
            "the pushdown request's from.type names the FROM node type subselect, which the \
             adapter does not plan",
        ),
        (
            pushdown(json!({"from": {"type": "table", "name": "T", "alias": 7}})),
            "the pushdown request's from.alias is not a string",
        ),
        (
            join(json!({"join_type": "cross"})),
            "the pushdown request's from.join_type names the join type cross, which the adapter \
             does not plan",
        ),
        (
            join(json!({"left": null})),
            "the pushdown request's from.left is missing",
        ),
        (
            join(json!({"condition": null})),
            "the pushdown request's from.condition is missing",
        ),
        (
            sort(json!({"isAscending": true, "nullsLast": true})),
            "the pushdown request's orderBy[0].expression is missing",
        ),
        (
            sort(json!({"expression": column, "nullsLast": true})),
            "the pushdown request's orderBy[0].isAscending is missing",
        ),
        (
            sort(json!({"expression": column, "isAscending": true})),
            "the pushdown request's orderBy[0].nullsLast is missing",
        ),
        (
            pushdown(json!({"orderBy": {}})),
            "the pushdown request's orderBy is not an array",
        ),
        (
            pushdown(json!({"groupBy": {}})),
            "the pushdown request's groupBy is not an array",
        ),
        (
            pushdown(json!({"limit": {"numElements": -1}})),
            "the pushdown request's limit.numElements is not a whole number",
        ),
        (
            pushdown(json!({"limit": {"numElements": 1, "offset": "2"}})),
            "the pushdown request's limit.offset is not a whole number",
        ),
        (
            pushdown(json!({"aggregationType": "rollup"})),
            "the pushdown request's aggregationType names the aggregation type rollup",
        ),
        (
            pushdown(json!({"aggregationType": 1})),
            "the pushdown request's aggregationType is not a string",
        ),
        (
            json!({"pushdownRequest": {"selectListDataTypes": []}, "involvedTables": {}}),
            "the pushdown request's involvedTables is not an array",
        ),
        (
            json!({"pushdownRequest": {"selectListDataTypes": []}, "involvedTables": [{}]}),
            "the pushdown request's involvedTables[0].name is missing",
        ),
    ];
    for (request, expected) in cases {
        let error = AdapterRequest::parse(&request).expect_err(&format!("{request} must fail"));
        assert_eq!(user_message(error), expected, "{request}");
    }
}
