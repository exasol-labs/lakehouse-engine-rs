use super::super::support::quote_ident;
use super::super::test_support::*;
use super::*;
use crate::scan::spec::ProjectionItem;

fn relation(col_types: &[(String, ExasolType)]) -> EmptyRelation {
    EmptyRelation::single_scan(&serde_json::json!({}), &column_types(col_types))
}

#[test]
fn empty_file_list_returns_empty_select() {
    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
    let types = vec![ExasolType::decimal(20, 0), ExasolType::WIDEST_VARCHAR];
    let resp = empty_pushdown_sql(&projected_columns(&proj, &types));
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
        ExasolType::decimal(18, 0),
        ExasolType::WIDEST_VARCHAR,
        ExasolType::decimal(18, 0),
    ];
    let resp = empty_pushdown_sql(&projected_columns(&proj_cols, &proj_types));
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
fn empty_select_list_casts_a_null_to_each_declared_type_in_order() {
    let resp = empty_select_list_typed_sql(&[
        ExasolType::decimal(20, 0),
        ExasolType::Boolean,
        ExasolType::decimal(18, 0),
    ]);
    assert_eq!(
        resp["sql"],
        "SELECT CAST(NULL AS DECIMAL(20,0)), CAST(NULL AS BOOLEAN), CAST(NULL AS DECIMAL(18,0)) \
         FROM DUAL WHERE 1=0"
    );
}

#[test]
fn empty_result_sql_dispatches_by_plan_shape() {
    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
    let proj_types = vec![ExasolType::decimal(20, 0), ExasolType::WIDEST_VARCHAR];
    let col_types = vec![("AMOUNT".to_string(), ExasolType::decimal(18, 2))];

    let grouped = pushdown_request("T", &[("K", ExasolType::decimal(20, 0))])
        .group_by(vec![col("K")])
        .column(col("K"))
        .count_star()
        .build_pushdown();
    let grouped_sql = empty_result_sql(
        &grouped,
        &projected_columns(&proj, &proj_types),
        false,
        ScanTranslator::new(&column_types(&col_types)),
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

    let text_col_types = vec![("NAME".to_string(), ExasolType::WIDEST_VARCHAR)];
    for (column, ty, col_types) in [
        ("AMOUNT", "DECIMAL(18,2)", &col_types),
        ("NAME", "VARCHAR(2000000)", &text_col_types),
    ] {
        let single = pushdown_request("T", NO_COLUMNS)
            .single_group()
            .item(
                agg_item("SUM", Some(column), false),
                ExasolType::decimal(36, 2),
            )
            .build_pushdown();
        let sql = empty_result_sql(
            &single,
            &projected_columns(&proj, &proj_types),
            false,
            ScanTranslator::new(&column_types(col_types)),
            &relation(col_types),
        )
        .unwrap()["sql"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            sql,
            format!(
                "SELECT CAST(SUM(\"{column}\") AS DECIMAL(36,2)) FROM (SELECT CAST(NULL AS {ty}) \
                 AS \"{column}\" \
                 FROM DUAL WHERE 1=0) AS \"LHS_T0\""
            )
        );
    }
}

#[test]
fn empty_single_group_evaluates_constants_and_having_over_the_empty_relation() {
    let col_types = vec![("ID".to_string(), ExasolType::decimal(19, 0))];
    let request = pushdown_request("T", NO_COLUMNS)
        .single_group()
        .field("having", predicate_equal(count_star(), num_lit(0)))
        .item(str_lit("X"), ExasolType::WIDEST_VARCHAR)
        .count_star()
        .build_pushdown();
    let sql = empty_result_sql(
        &request,
        &projected_columns(&[], &[]),
        false,
        ScanTranslator::new(&column_types(&col_types)),
        &relation(&col_types),
    )
    .unwrap()["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        sql,
        "SELECT CAST('X' AS VARCHAR(2000000)), CAST(COUNT(*) AS DECIMAL(18,0)) FROM (SELECT \
         CAST(NULL AS DECIMAL(19,0)) AS \"ID\" FROM DUAL \
         WHERE 1=0) AS \"LHS_T0\" HAVING (COUNT(*) = 0)"
    );
}

#[test]
fn empty_files_grouped_non_numeric_aggregate_uses_selectlist_shape() {
    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
    let proj_types = vec![ExasolType::decimal(20, 0), ExasolType::WIDEST_VARCHAR];
    let col_types = vec![("NAME".to_string(), ExasolType::WIDEST_VARCHAR)];

    let grouped_non_numeric = pushdown_request("T", &[("K", ExasolType::decimal(20, 0))])
        .group_by(vec![col("K")])
        .column(col("K"))
        .item(
            agg_item("SUM", Some("name"), false),
            ExasolType::decimal(36, 2),
        )
        .build_pushdown();

    let row_sql = empty_result_sql(
        &grouped_non_numeric,
        &projected_columns(&proj, &proj_types),
        false,
        ScanTranslator::new(&column_types(&col_types)),
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
    let proj_types = vec![ExasolType::decimal(20, 0), ExasolType::WIDEST_VARCHAR];
    let col_types = vec![("NAME".to_string(), ExasolType::WIDEST_VARCHAR)];

    let grouped_having = pushdown_request("T", &[("K", ExasolType::decimal(20, 0))])
        .group_by(vec![col("K")])
        .field("having", serde_json::json!({"type": "predicate_greater"}))
        .column(col("K"))
        .item(
            agg_item("SUM", Some("name"), false),
            ExasolType::decimal(36, 2),
        )
        .build_pushdown();

    let row_sql = empty_result_sql(
        &grouped_having,
        &projected_columns(&proj, &proj_types),
        false,
        ScanTranslator::new(&column_types(&col_types)),
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
    let pushdown_req = pushdown_request("T", NO_COLUMNS)
        .item(
            scalar_fn("LENGTH", [table_col("T", "SCORE")]),
            ExasolType::decimal(18, 0),
        )
        .build_pushdown();
    let col_types = vec![
        ("ID".to_string(), ExasolType::decimal(20, 0)),
        ("NAME".to_string(), ExasolType::WIDEST_VARCHAR),
        ("SCORE".to_string(), ExasolType::DoublePrecision),
    ];
    assert!(
        matches!(
            classify_request_shape(
                &pushdown_req,
                ScanTranslator::new(&column_types(&col_types))
            ),
            RequestShape::RowScan
        ),
        "the fixture must classify as RowScan for this test to exercise its arm"
    );

    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into(), "SCORE".into()];
    let proj_types: Vec<ExasolType> = col_types.iter().map(|(_, t)| *t).collect();

    let widened = empty_result_sql(
        &pushdown_req,
        &projected_columns(&proj, &proj_types),
        true,
        ScanTranslator::new(&column_types(&col_types)),
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
        &projected_columns(&proj, &proj_types),
        false,
        ScanTranslator::new(&column_types(&col_types)),
        &relation(&col_types),
    )
    .expect("the non-widened empty row-scan result must build")["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        not_widened,
        empty_pushdown_sql(&projected_columns(&proj, &proj_types))["sql"]
            .as_str()
            .unwrap(),
        "the non-widened path must stay byte-identical to the full-row empty \
         shape: {not_widened}"
    );
}
