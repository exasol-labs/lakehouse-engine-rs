use super::super::support::quote_ident;
use super::super::test_support::*;
use super::*;

fn relation(col_types: &[(String, String)]) -> EmptyRelation {
    EmptyRelation::single_scan(&serde_json::json!({}), col_types)
}

#[test]
fn empty_file_list_returns_empty_select() {
    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
    let types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
    let resp = empty_pushdown_sql(&proj, &types);
    let sql = resp["sql"].as_str().unwrap();
    assert!(sql.contains("WHERE 1=0"));
    assert!(sql.contains("CAST(NULL AS DECIMAL(20,0))"));
}

#[test]
fn empty_pushdown_sql_repeated_literals_unique_aliases() {
    let proj_cols: Vec<ProjectionItem> = vec![
        ProjectionItem::Expr { expr: "1".into() },
        ProjectionItem::Column("NAME".into()),
        ProjectionItem::Expr { expr: "1".into() },
    ];
    let proj_types = vec![
        "DECIMAL(18,0)".to_string(),
        "VARCHAR(2000000)".to_string(),
        "DECIMAL(18,0)".to_string(),
    ];
    let resp = empty_pushdown_sql(&proj_cols, &proj_types);
    let sql = resp["sql"].as_str().unwrap();

    assert_eq!(
        sql.matches("CAST(NULL AS").count(),
        3,
        "must emit three CAST(NULL AS ...) items, one per select-list item: {sql}"
    );
    assert!(
        sql.contains(&format!("AS {}", quote_ident("_LH_PROJ_0"))),
        "position 0's literal must get a positional-unique alias: {sql}"
    );
    assert!(
        sql.contains(&format!("AS {}", quote_ident("NAME"))),
        "the column item must keep its real quoted name: {sql}"
    );
    assert!(
        sql.contains(&format!("AS {}", quote_ident("_LH_PROJ_2"))),
        "position 2's literal must get a distinct positional-unique alias: {sql}"
    );
    assert!(
        !sql.contains(&format!("AS {}", quote_ident("1"))),
        "must never alias a literal by its rendered value text (would collide): {sql}"
    );
}

#[test]
fn empty_grouped_sql_emits_zero_rows_in_grouped_shape() {
    let select_items = vec![
        GroupedSelectItem::GroupKey {
            group_key_slot: 0,
            select_index: 0,
        },
        GroupedSelectItem::Aggregate {
            plan_slot: 0,
            select_index: 1,
        },
    ];
    let group_key_types = vec!["DECIMAL(20,0)".to_string()];
    let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
    let resp = empty_grouped_sql(&group_key_types, &aggregate_types, &select_items);
    let sql = resp["sql"].as_str().unwrap();
    assert!(
        sql.contains("WHERE 1=0"),
        "grouped empty is zero rows: {sql}"
    );
    assert!(
        sql.contains("CAST(NULL AS DECIMAL(20,0))"),
        "group-key column typed from group_key_types: {sql}"
    );
    assert!(
        sql.contains("CAST(NULL AS DECIMAL(18,0))"),
        "aggregate column typed from aggregate_types: {sql}"
    );
    let select_clause = sql
        .strip_prefix("SELECT ")
        .and_then(|s| s.split(" FROM").next())
        .unwrap();
    assert_eq!(
        select_clause.matches("CAST(NULL AS").count(),
        2,
        "one output column per grouped select item: {sql}"
    );
}

#[test]
fn empty_grouped_sql_includes_constant_projection_column() {
    let select_items = vec![
        GroupedSelectItem::GroupKey {
            group_key_slot: 0,
            select_index: 0,
        },
        GroupedSelectItem::Constant {
            select_index: 1,
            projection: "CAST(NULL AS BOOLEAN)".to_string(),
        },
        GroupedSelectItem::Aggregate {
            plan_slot: 0,
            select_index: 2,
        },
    ];
    let group_key_types = vec!["DECIMAL(20,0)".to_string()];
    let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
    let resp = empty_grouped_sql(&group_key_types, &aggregate_types, &select_items);
    let sql = resp["sql"].as_str().unwrap();
    let select_clause = sql
        .strip_prefix("SELECT ")
        .and_then(|s| s.split(" FROM").next())
        .unwrap();
    let columns: Vec<&str> = select_clause.split(", ").collect();
    assert_eq!(
        columns,
        vec![
            "CAST(NULL AS DECIMAL(20,0))",
            "CAST(NULL AS BOOLEAN)",
            "CAST(NULL AS DECIMAL(18,0))",
        ],
        "constant column is reused verbatim in select-list order: {sql}"
    );
}

#[test]
fn empty_result_sql_dispatches_by_plan_shape() {
    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
    let proj_types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
    let col_types = vec![("AMOUNT".to_string(), "DECIMAL(18,2)".to_string())];

    let grouped = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [{"type": "column", "name": "K"}],
        "selectList": [
            {"type": "column", "name": "K"},
            agg_item("COUNT", None, false),
        ],
        "selectListDataTypes": [
            {"type": "decimal", "precision": 20, "scale": 0},
            {"type": "decimal", "precision": 18, "scale": 0},
        ],
    });
    let grouped_sql = empty_result_sql(
        &grouped,
        &proj,
        &proj_types,
        false,
        &col_types,
        &relation(&col_types),
    )
    .unwrap()["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        grouped_sql.contains("WHERE 1=0"),
        "grouped shape is zero rows: {grouped_sql}"
    );

    let text_col_types = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];
    for (column, ty, col_types) in [
        ("AMOUNT", "DECIMAL(18,2)", &col_types),
        ("NAME", "VARCHAR(2000000)", &text_col_types),
    ] {
        let single = serde_json::json!({
            "aggregationType": "single_group",
            "selectList": [agg_item("SUM", Some(column), false)],
            "selectListDataTypes": [{"type": "decimal", "precision": 36, "scale": 2}],
        });
        let sql = empty_result_sql(
            &single,
            &proj,
            &proj_types,
            false,
            col_types,
            &relation(col_types),
        )
        .unwrap()["sql"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            sql,
            format!(
                "SELECT SUM(\"{column}\") FROM (SELECT CAST(NULL AS {ty}) AS \"{column}\" \
                 FROM DUAL WHERE 1=0) AS \"LHS_T0\""
            )
        );
    }
}

#[test]
fn empty_single_group_evaluates_constants_and_having_over_the_empty_relation() {
    let col_types = vec![("ID".to_string(), "DECIMAL(19,0)".to_string())];
    let request = serde_json::json!({
        "aggregationType": "single_group",
        "selectList": [
            {"type": "literal_string", "value": "X"},
            agg_item("COUNT", None, false),
        ],
        "having": {
            "type": "predicate_equal",
            "left": agg_item("COUNT", None, false),
            "right": {"type": "literal_exactnumeric", "value": 0},
        },
    });
    let sql = empty_result_sql(&request, &[], &[], false, &col_types, &relation(&col_types))
        .unwrap()["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        sql,
        "SELECT 'X', COUNT(*) FROM (SELECT CAST(NULL AS DECIMAL(19,0)) AS \"ID\" FROM DUAL \
         WHERE 1=0) AS \"LHS_T0\" HAVING (COUNT(*) = 0)"
    );
}

#[test]
fn empty_files_grouped_non_numeric_aggregate_uses_selectlist_shape() {
    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
    let proj_types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
    let col_types = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];

    let grouped_non_numeric = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [{"type": "column", "name": "K"}],
        "selectList": [
            {"type": "column", "name": "K"},
            agg_item("SUM", Some("name"), false),
        ],
        "selectListDataTypes": [
            {"type": "decimal", "precision": 20, "scale": 0},
            {"type": "decimal", "precision": 36, "scale": 2},
        ],
    });

    let row_sql = empty_result_sql(
        &grouped_non_numeric,
        &proj,
        &proj_types,
        false,
        &col_types,
        &relation(&col_types),
    )
    .unwrap()["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        row_sql,
        "SELECT CAST(NULL AS DECIMAL(20,0)), CAST(NULL AS DECIMAL(36,2)) FROM DUAL WHERE 1=0",
        "declined grouped aggregate over zero files must produce the selectList-typed \
         empty shape (matching the qualified wrapper), not the full base row"
    );
}

#[test]
fn empty_files_grouped_non_numeric_aggregate_with_having_yields_typed_empty() {
    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
    let proj_types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
    let col_types = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];

    let grouped_having = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [{"type": "column", "name": "K"}],
        "selectList": [
            {"type": "column", "name": "K"},
            agg_item("SUM", Some("name"), false),
        ],
        "selectListDataTypes": [
            {"type": "decimal", "precision": 20, "scale": 0},
            {"type": "decimal", "precision": 36, "scale": 2},
        ],
        "having": {"type": "predicate_greater"},
    });

    let row_sql = empty_result_sql(
        &grouped_having,
        &proj,
        &proj_types,
        false,
        &col_types,
        &relation(&col_types),
    )
    .unwrap()["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        row_sql,
        "SELECT CAST(NULL AS DECIMAL(20,0)), CAST(NULL AS DECIMAL(36,2)) FROM DUAL WHERE 1=0",
        "declined grouped aggregate with HAVING over zero files must produce the same \
         selectList-typed empty shape as the wrapper it now falls through to, not an error"
    );
}

#[test]
fn empty_result_sql_widened_row_scan_uses_select_list_types() {
    let pushdown_req = serde_json::json!({
        "selectList": [
            {"type": "function_scalar", "name": "LENGTH", "arguments": [
                {"type": "column", "name": "SCORE", "tableName": "T"}]},
        ],
        "selectListDataTypes": [{"type": "decimal", "precision": 18, "scale": 0}],
    });
    let col_types = vec![
        ("ID".to_string(), "DECIMAL(20,0)".to_string()),
        ("NAME".to_string(), "VARCHAR(2000000)".to_string()),
        ("SCORE".to_string(), "DOUBLE PRECISION".to_string()),
    ];
    assert!(
        matches!(
            classify_request_shape(&pushdown_req, &col_types),
            RequestShape::RowScan
        ),
        "the fixture must classify as RowScan for this test to exercise its arm"
    );

    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into(), "SCORE".into()];
    let proj_types: Vec<String> = col_types.iter().map(|(_, t)| t.clone()).collect();

    let widened = empty_result_sql(
        &pushdown_req,
        &proj,
        &proj_types,
        true,
        &col_types,
        &relation(&col_types),
    )
    .expect("the widened empty row-scan result must build")["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        widened, "SELECT CAST(NULL AS DECIMAL(18,0)) FROM DUAL WHERE 1=0",
        "a widened row-scan projection over zero files must produce ONE \
         selectListDataTypes-typed column, not the 3-column base row: {widened}"
    );

    let not_widened = empty_result_sql(
        &pushdown_req,
        &proj,
        &proj_types,
        false,
        &col_types,
        &relation(&col_types),
    )
    .expect("the non-widened empty row-scan result must build")["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        not_widened,
        empty_pushdown_sql(&proj, &proj_types)["sql"]
            .as_str()
            .unwrap(),
        "the non-widened path must stay byte-identical to the full-row empty \
         shape: {not_widened}"
    );
}

#[test]
fn empty_grouped_sql_casts_a_varchar_scalar_over_aggregate() {
    let select_items = vec![GroupedSelectItem::ScalarOverAggregate {
        select_index: 0,
        node: serde_json::json!({}),
        declared_type: "VARCHAR(2000000)".to_string(),
    }];
    let resp = empty_grouped_sql(&[], &[], &select_items);
    assert_eq!(
        resp["sql"].as_str().unwrap(),
        "SELECT CAST(NULL AS VARCHAR(2000000)) FROM DUAL WHERE 1=0"
    );
}
