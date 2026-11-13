use super::super::support::quote_ident;
use super::super::test_support::*;
use super::*;
use crate::scan::spec::ProjectionItem;

/// The empty-result SQL a single-table `request` returns over zero files, for the derived
/// projection `proj` typed `proj_types`.
fn empty_sql(
    request: &Json,
    proj: &[ProjectionItem],
    proj_types: &[ExasolType],
    widened: bool,
) -> String {
    let scan = scan_of(request);
    empty_result_sql(
        &parsed_pushdown(request),
        &projected_columns(proj, proj_types),
        widened,
        scan,
        &EmptyRelation::single_scan(request, scan.columns()),
    )
    .expect("the empty result must build")["sql"]
        .as_str()
        .expect("the empty result carries sql")
        .to_string()
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

    let request = pushdown_request("T", &[("K", ExasolType::decimal(20, 0))])
        .group_by(vec![col("K")])
        .column(col("K"))
        .count_star()
        .build()
        .0;
    let grouped_sql = empty_sql(&request, &proj, &proj_types, false);
    assert!(
        grouped_sql.contains("WHERE 1=0"),
        "grouped shape is zero rows: {grouped_sql}"
    );

    for (column, ty, exa_type) in [
        ("AMOUNT", "DECIMAL(18,2)", ExasolType::decimal(18, 2)),
        ("NAME", "VARCHAR(2000000)", ExasolType::WIDEST_VARCHAR),
    ] {
        let request = pushdown_request("T", &[(column, exa_type)])
            .single_group()
            .item(
                agg_item("SUM", Some(column), false),
                ExasolType::decimal(36, 2),
            )
            .build()
            .0;
        let sql = empty_sql(&request, &proj, &proj_types, false);
        assert_eq!(
            sql,
            format!(
                "SELECT CAST(SUM(\"LHS_T0\".\"{column}\") AS DECIMAL(36,2)) FROM (SELECT CAST(NULL AS {ty}) \
                 AS \"{column}\" \
                 FROM DUAL WHERE 1=0) AS \"LHS_T0\" GROUP BY ()"
            )
        );
    }
}

#[test]
fn empty_single_group_evaluates_constants_and_having_over_the_empty_relation() {
    let request = pushdown_request("T", &[("ID", ExasolType::decimal(19, 0))])
        .single_group()
        .field("having", predicate_equal(count_star(), num_lit(0)))
        .item(str_lit("X"), ExasolType::WIDEST_VARCHAR)
        .count_star()
        .build()
        .0;
    let sql = empty_sql(&request, &[], &[], false);
    assert_eq!(
        sql,
        "SELECT CAST('X' AS VARCHAR(2000000)), CAST(COUNT(*) AS DECIMAL(18,0)) FROM (SELECT \
         CAST(NULL AS DECIMAL(19,0)) AS \"ID\" FROM DUAL \
         WHERE 1=0) AS \"LHS_T0\" GROUP BY () HAVING (COUNT(*) = 0)"
    );
}

#[test]
fn empty_files_grouped_non_numeric_aggregate_uses_selectlist_shape() {
    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
    let proj_types = vec![ExasolType::decimal(20, 0), ExasolType::WIDEST_VARCHAR];

    let request = pushdown_request(
        "T",
        &[
            ("K", ExasolType::decimal(20, 0)),
            ("NAME", ExasolType::WIDEST_VARCHAR),
        ],
    )
    .group_by(vec![col("K")])
    .column(col("K"))
    .item(
        agg_item("SUM", Some("name"), false),
        ExasolType::decimal(36, 2),
    )
    .build()
    .0;
    let row_sql = empty_sql(&request, &proj, &proj_types, false);
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

    let request = pushdown_request(
        "T",
        &[
            ("K", ExasolType::decimal(20, 0)),
            ("NAME", ExasolType::WIDEST_VARCHAR),
        ],
    )
    .group_by(vec![col("K")])
    .field(
        "having",
        compare(
            "predicate_greater",
            agg_item("SUM", Some("name"), false),
            num_lit(1),
        ),
    )
    .column(col("K"))
    .item(
        agg_item("SUM", Some("name"), false),
        ExasolType::decimal(36, 2),
    )
    .build()
    .0;
    let row_sql = empty_sql(&request, &proj, &proj_types, false);
    assert_eq!(
        row_sql,
        "SELECT CAST(NULL AS DECIMAL(20,0)), CAST(NULL AS DECIMAL(36,2)) FROM DUAL WHERE 1=0",
        "declined grouped aggregate with HAVING over zero files must produce the same \
         selectList-typed empty shape as the wrapper it now falls through to, not an error"
    );
}

#[test]
fn empty_result_sql_widened_row_scan_uses_select_list_types() {
    let col_types = [
        ("ID", ExasolType::decimal(20, 0)),
        ("NAME", ExasolType::WIDEST_VARCHAR),
        ("SCORE", ExasolType::DoublePrecision),
    ];
    let request = pushdown_request("T", &col_types)
        .item(
            scalar_fn("LENGTH", [table_col("T", "SCORE")]),
            ExasolType::decimal(18, 0),
        )
        .build()
        .0;
    let (pushdown_req, scan) = (parsed_pushdown(&request), scan_of(&request));
    assert!(
        matches!(
            classify_request_shape(&pushdown_req, scan),
            RequestShape::RowScan
        ),
        "the fixture must classify as RowScan for this test to exercise its arm"
    );

    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into(), "SCORE".into()];
    let proj_types: Vec<ExasolType> = col_types.iter().map(|(_, t)| *t).collect();

    let widened = empty_sql(&request, &proj, &proj_types, true);
    assert_eq!(
        widened, "SELECT CAST(NULL AS DECIMAL(18,0)) FROM DUAL WHERE 1=0",
        "a widened row-scan projection over zero files must produce ONE \
         selectListDataTypes-typed column, not the 3-column base row: {widened}"
    );

    let not_widened = empty_sql(&request, &proj, &proj_types, false);
    assert_eq!(
        not_widened,
        empty_pushdown_sql(&projected_columns(&proj, &proj_types))["sql"]
            .as_str()
            .unwrap(),
        "the non-widened path must stay byte-identical to the full-row empty \
         shape: {not_widened}"
    );
}
