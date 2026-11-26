use super::*;
use crate::test_support::{ast, col};
use serde_json::json;

/// `base` with each of `fields` set.
fn with(mut base: Json, fields: Json) -> Json {
    for (key, value) in fields.as_object().expect("fields are an object") {
        base[key] = value.clone();
    }
    base
}

#[test]
fn a_request_reads_into_its_clauses_and_involved_tables() {
    let table = |name: &str| json!({"type": "table", "name": name});
    let request = json!({
        "pushdownRequest": {
            "from": {
                "type": "join", "join_type": "left_outer",
                "left": with(table("T"), json!({"alias": "A"})), "right": table("U"),
                "condition": col("ID"),
            },
            "selectList": [col("ID")],
            "selectListDataTypes": [{"type": "DATE"}],
            "filter": col("F"),
            "groupBy": [col("G")],
            "having": col("H"),
            "orderBy": [{"expression": col("O"), "isAscending": false, "nullsLast": true}],
            "limit": {"numElements": 12, "offset": 3},
            "aggregationType": "group_by",
        },
        "involvedTables": [{
            "name": "T", "adapterNotes": "ns.t",
            "columns": [{"name": "ID", "dataType": {"type": "DATE"}, "adapterNotes": "note"}],
        }],
    });
    let parsed = parse_request(&request).expect("the request parses");
    let leaf = |name: &str, alias: Option<&str>| {
        Box::new(FromClause::Table {
            name: name.into(),
            alias: alias.map(str::to_string),
        })
    };
    let from = FromClause::Join {
        kind: JoinKind::LeftOuter,
        left: leaf("T", Some("A")),
        right: leaf("U", None),
        condition: ast(&col("ID")),
    };
    assert_eq!(from.conditions(), [&ast(&col("ID"))]);
    assert_eq!(
        parsed,
        ParsedRequest {
            involved_tables: vec![InvolvedTable {
                name: "T".into(),
                adapter_notes: Some("ns.t".into()),
                columns: vec![InvolvedColumn {
                    name: "ID".into(),
                    data_type: ExasolType::Date,
                    adapter_notes: Some("note".into()),
                }],
            }],
            pushdown: ParsedPushdown {
                from: Some(from),
                select_list: Some(vec![ast(&col("ID"))]),
                select_types: Some(vec![ExasolType::Date]),
                filter: Some(ast(&col("F"))),
                group_by: vec![ast(&col("G"))],
                having: Some(ast(&col("H"))),
                order_by: vec![SortElement {
                    expression: ast(&col("O")),
                    ascending: false,
                    nulls_last: true,
                }],
                limit: Some(12),
                offset: 3,
                aggregation_type: Some(AggregationType::GroupBy),
            },
        }
    );
}

#[test]
fn an_absent_or_json_null_clause_reads_as_absent() {
    for pushdown in [
        json!({}),
        json!({"from": null, "selectList": null, "selectListDataTypes": null, "filter": null,
               "groupBy": null, "having": null, "orderBy": null, "limit": null,
               "aggregationType": null}),
    ] {
        let parsed = parse_request(&json!({"pushdownRequest": pushdown})).expect("it parses");
        assert_eq!(parsed.involved_tables, []);
        assert_eq!(
            parsed.pushdown,
            ParsedPushdown {
                from: None,
                select_list: None,
                select_types: None,
                filter: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: 0,
                aggregation_type: None,
            },
            "{pushdown}"
        );
    }
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
    let involving = |table: Json| json!({"pushdownRequest": {"selectListDataTypes": []}, "involvedTables": [table]});
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
        (
            involving(json!({"name": "T", "adapterNotes": {"name": "a.b"}})),
            "the pushdown request's involvedTables[0].adapterNotes is not a string",
        ),
        (
            involving(json!({"name": "T", "columns": [{"dataType": {"type": "DATE"}}]})),
            "the pushdown request's involvedTables[0].columns[0].name is missing",
        ),
        (
            involving(json!({"name": "T", "columns": [{"name": "C"}]})),
            "the pushdown request's involvedTables[0].columns[0].dataType is missing",
        ),
        (
            json!({"involvedTables": []}),
            "the pushdown request's pushdownRequest is missing",
        ),
        (
            pushdown(json!({"selectListDataTypes": {}})),
            "the pushdown request's selectListDataTypes is not an array",
        ),
    ];
    for (request, expected) in cases {
        match parse_request(&request) {
            Err(UdfError::User(message)) => assert_eq!(message, expected, "{request}"),
            other => panic!("{request} must fail to parse, got {other:?}"),
        }
    }
}
