use super::*;
use serde_json::json;

fn request(fields: Json) -> PushdownRequest {
    let mut json = json!({"selectListDataTypes": []});
    for (key, value) in fields.as_object().expect("fields are an object") {
        json[key] = value.clone();
    }
    PushdownRequest::parse(json).expect("the fixture parses")
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
