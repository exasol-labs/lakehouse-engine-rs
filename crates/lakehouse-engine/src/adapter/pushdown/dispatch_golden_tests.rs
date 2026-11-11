//! Golden dispatch-SQL fixtures under `testdata/dispatch_golden/`, compared by
//! full-string `assert_eq!`. The `empty_*` fixtures carry no `storage` value, so a
//! diff in one of them is always a regression, never an expected update.

use super::test_support::{
    PushdownRequestBuilder, agg_item, agg_item_expr, broadcast_sides, column_types, count_star,
    float_div, parsed_pushdown, pd, projected_columns, pushdown_request, request_sides,
    request_typed, round_of, sample_scan_storage, sample_storage, with_identifier_notes,
};
use super::*;
use crate::adapter::pushdown::test_support::{
    aliased_col, col, compare, customer_orders_join, num_lit, string_equal, table_col,
};
use crate::scan::spec::ProjectionItem;
use crate::types::exasol_type::ExasolType;

fn fixed_shards() -> Vec<Vec<FileEntry>> {
    vec![
        vec![FileEntry::new("data/part-0.parquet", 1_000)],
        vec![FileEntry::new("data/part-1.parquet", 2_000)],
        vec![FileEntry::new("data/part-2.parquet", 1_500)],
    ]
}

fn base_col_types() -> Vec<(String, ExasolType)> {
    EVENTS_COLUMNS
        .iter()
        .map(|(name, exa_type)| (name.to_string(), *exa_type))
        .collect()
}

const EVENTS_COLUMNS: [(&str, ExasolType); 4] = [
    ("REGION", ExasolType::WIDEST_VARCHAR),
    ("NAME", ExasolType::WIDEST_VARCHAR),
    ("AMOUNT", ExasolType::decimal(18, 2)),
    ("ID", ExasolType::decimal(20, 0)),
];

fn events_request() -> PushdownRequestBuilder {
    pushdown_request("EVENTS", &EVENTS_COLUMNS)
}

pub(super) fn grouped_request() -> (Json, Vec<ExasolType>) {
    events_request()
        .group_by(vec![col("REGION")])
        .column(col("REGION"))
        .item(
            agg_item("SUM", Some("AMOUNT"), false),
            ExasolType::decimal(36, 2),
        )
        .build()
}

/// HAVING matches no projected plan, so it routes to `GroupByWrapper` (#195).
fn unmergeable_having_request() -> (Json, Vec<ExasolType>) {
    let (mut request, item_types) = grouped_request();
    request["pushdownRequest"]["having"] = compare("predicate_greater", count_star(), num_lit(0));
    (request, item_types)
}

/// Non-numeric SUM target: declines grouped decomposition to the qualified wrapper.
fn group_by_fallback_request() -> (Json, Vec<ExasolType>) {
    events_request()
        .group_by(vec![col("REGION")])
        .column(col("REGION"))
        .item(
            agg_item("SUM", Some("NAME"), false),
            ExasolType::decimal(30, 4),
        )
        .build()
}

pub(super) fn lone_count_distinct_request() -> (Json, Vec<ExasolType>) {
    events_request()
        .item(
            agg_item("COUNT", Some("ID"), true),
            ExasolType::decimal(18, 0),
        )
        .build()
}

fn multi_count_distinct_decline_request() -> (Json, Vec<ExasolType>) {
    events_request()
        .item(
            agg_item("COUNT", Some("ID"), true),
            ExasolType::decimal(18, 0),
        )
        .item(
            agg_item("COUNT", Some("NAME"), true),
            ExasolType::decimal(18, 0),
        )
        .build()
}

pub(super) fn row_scan_request() -> (Json, Vec<ExasolType>) {
    events_request()
        .column(col("REGION"))
        .column(col("AMOUNT"))
        .build()
}

fn row_scan_projection() -> (Vec<ProjectionItem>, Vec<ExasolType>) {
    (
        vec![
            ProjectionItem::Column("REGION".into()),
            ProjectionItem::Column("AMOUNT".into()),
        ],
        vec![ExasolType::WIDEST_VARCHAR, ExasolType::decimal(18, 2)],
    )
}

pub(super) fn single_group_agg_request() -> (Json, Vec<ExasolType>) {
    events_request()
        .item(
            agg_item("SUM", Some("AMOUNT"), false),
            ExasolType::decimal(36, 2),
        )
        .build()
}

/// An inner `DISTINCT` declines the decomposition, so the projection widens (#194).
fn nested_aggregate_decline_request() -> (Json, Vec<ExasolType>) {
    events_request()
        .item(
            round_of(agg_item("SUM", Some("AMOUNT"), true), 2),
            ExasolType::decimal(36, 2),
        )
        .build()
}

fn single_group_scalar_over_aggregate_request() -> (Json, Vec<ExasolType>) {
    events_request()
        .item(
            round_of(agg_item("SUM", Some("AMOUNT"), false), 2),
            ExasolType::decimal(36, 2),
        )
        .build()
}

/// The nested `COUNT(*)` must dedup against the bare item's partial column.
fn single_group_scalar_over_aggregate_dedup_request() -> (Json, Vec<ExasolType>) {
    events_request()
        .count_star()
        .item(round_of_sum_over_count(), ExasolType::decimal(36, 2))
        .build()
}

/// `ROUND(SUM(AMOUNT) / COUNT(*), 2)`.
fn round_of_sum_over_count() -> Json {
    round_of(
        float_div(agg_item("SUM", Some("AMOUNT"), false), count_star()),
        2,
    )
}

fn single_group_scalar_over_aggregate_interleaved_request() -> (Json, Vec<ExasolType>) {
    events_request()
        .item(
            agg_item("SUM", Some("AMOUNT"), false),
            ExasolType::decimal(36, 2),
        )
        .item(round_of_sum_over_count(), ExasolType::decimal(9, 4))
        .count_star()
        .build()
}

fn single_group_scalar_over_variance_request() -> (Json, Vec<ExasolType>) {
    events_request()
        .item(
            round_of(agg_item("VARIANCE", Some("AMOUNT"), false), 4),
            ExasolType::decimal(36, 4),
        )
        .build()
}

/// Every golden shape has no ORDER BY and derives one projection item per
/// select-list item, so `has_order_by` and the widening flag are always `false`.
fn dispatch_sql(
    request: &Json,
    proj_cols: Vec<ProjectionItem>,
    proj_types: Vec<ExasolType>,
    filter: Option<String>,
    limit: Option<u64>,
) -> String {
    let pushdown_req = pd(request);
    dispatch_sql_with_pushdown_req(
        request,
        &pushdown_req,
        proj_cols,
        proj_types,
        base_col_types(),
        filter,
        limit,
    )
}

/// Derives the projection through `project_columns`, so the golden proves the
/// fixture itself widens.
fn dispatch_sql_widened(request: &Json) -> String {
    let pushdown = parsed_pushdown(request);
    let (projection, widened) = super::support::project_columns(
        &pushdown,
        ScanTranslator::new(&column_types(&base_col_types())),
    )
    .expect("the fixture must project");
    assert!(
        widened,
        "this fixture must exercise the widening guard, not a per-item projection"
    );
    let result = build_dispatch_sql(
        request,
        &pushdown,
        projection,
        widened,
        ScanTranslator::new(&column_types(&base_col_types())),
        None,
        None,
        None,
        false,
        &fixed_shards(),
        "s3://warehouse/db/events".to_string(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        &sample_scan_storage(),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        4,
        8192,
        2,
        0.6,
        200,
        8,
        false,
    )
    .expect("build_dispatch_sql must succeed for this fixture");
    result["sql"]
        .as_str()
        .expect("pushdown response must carry a sql field")
        .to_string()
}

/// Takes `pushdown_req` explicitly so callers can pass a stripped or alias-carrying
/// body (#193).
fn dispatch_sql_with_pushdown_req(
    request: &Json,
    pushdown_req: &Json,
    proj_cols: Vec<ProjectionItem>,
    proj_types: Vec<ExasolType>,
    col_types: Vec<(String, ExasolType)>,
    filter: Option<String>,
    limit: Option<u64>,
) -> String {
    let result = build_dispatch_sql(
        request,
        &PushdownRequest::parse(pushdown_req.clone())
            .expect("the fixture declares a valid selectListDataTypes"),
        projected_columns(&proj_cols, &proj_types),
        false,
        ScanTranslator::new(&column_types(&col_types)),
        filter,
        None,
        limit,
        false,
        &fixed_shards(),
        "s3://warehouse/db/events".to_string(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        &sample_scan_storage(),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        4,
        8192,
        2,
        0.6,
        200,
        8,
        false,
    )
    .expect("build_dispatch_sql must succeed for this fixture");
    result["sql"]
        .as_str()
        .expect("pushdown response must carry a sql field")
        .to_string()
}

fn empty_sql(request: &Json, proj_cols: &[ProjectionItem], proj_types: &[ExasolType]) -> String {
    let result = empty_result_sql(
        &parsed_pushdown(request),
        &projected_columns(proj_cols, proj_types),
        false,
        ScanTranslator::new(&column_types(&base_col_types())),
        &EmptyRelation::single_scan(request, &column_types(&base_col_types())),
    )
    .expect("empty_result_sql must succeed for this golden fixture");
    result["sql"]
        .as_str()
        .expect("pushdown response must carry a sql field")
        .to_string()
}

#[test]
fn grouped_aggregate_matches_golden() {
    let actual = dispatch_sql(
        &grouped_request().0,
        Vec::new(),
        Vec::new(),
        Some(r#"("AMOUNT" > 100)"#.to_string()),
        Some(50),
    );
    let expected = include_str!("testdata/dispatch_golden/grouped_aggregate.sql");
    assert_eq!(actual, expected);
}

#[test]
fn group_by_fallback_matches_golden() {
    let actual = dispatch_sql(
        &group_by_fallback_request().0,
        Vec::new(),
        Vec::new(),
        None,
        None,
    );
    let expected = include_str!("testdata/dispatch_golden/group_by_fallback.sql");
    assert_eq!(actual, expected);
}

#[test]
fn lone_count_distinct_matches_golden() {
    let actual = dispatch_sql(
        &lone_count_distinct_request().0,
        Vec::new(),
        Vec::new(),
        None,
        Some(10),
    );
    let expected = include_str!("testdata/dispatch_golden/lone_count_distinct.sql");
    assert_eq!(actual, expected);
}

#[test]
fn multi_count_distinct_decline_matches_golden() {
    let actual = dispatch_sql(
        &multi_count_distinct_decline_request().0,
        Vec::new(),
        Vec::new(),
        None,
        None,
    );
    let expected = include_str!("testdata/dispatch_golden/multi_count_distinct_decline.sql");
    assert_eq!(actual, expected);
}

#[test]
fn single_group_row_scan_matches_golden() {
    let (proj_cols, proj_types) = row_scan_projection();
    let actual = dispatch_sql(
        &row_scan_request().0,
        proj_cols,
        proj_types,
        Some(r#"("REGION" = 'EU')"#.to_string()),
        Some(100),
    );
    let expected = include_str!("testdata/dispatch_golden/single_group_row_scan.sql");
    assert_eq!(actual, expected);
}

#[test]
fn nested_aggregate_decline_matches_qualified_wrapper_golden() {
    let actual = dispatch_sql_widened(&nested_aggregate_decline_request().0);
    let expected = include_str!("testdata/dispatch_golden/nested_aggregate_decline.sql");
    assert_eq!(actual, expected);
}

#[test]
fn single_group_scalar_over_aggregate_matches_golden() {
    let actual = dispatch_sql(
        &single_group_scalar_over_aggregate_request().0,
        Vec::new(),
        Vec::new(),
        None,
        None,
    );
    let expected = include_str!("testdata/dispatch_golden/single_group_scalar_over_aggregate.sql");
    assert_eq!(actual, expected);
}

#[test]
fn single_group_scalar_over_aggregate_dedup_matches_golden() {
    let actual = dispatch_sql(
        &single_group_scalar_over_aggregate_dedup_request().0,
        Vec::new(),
        Vec::new(),
        None,
        None,
    );
    let expected =
        include_str!("testdata/dispatch_golden/single_group_scalar_over_aggregate_dedup.sql");
    assert_eq!(actual, expected);
}

#[test]
fn single_group_scalar_over_aggregate_interleaved_matches_golden() {
    let actual = dispatch_sql(
        &single_group_scalar_over_aggregate_interleaved_request().0,
        Vec::new(),
        Vec::new(),
        None,
        None,
    );
    let expected =
        include_str!("testdata/dispatch_golden/single_group_scalar_over_aggregate_interleaved.sql");
    assert_eq!(actual, expected);
}

#[test]
fn single_group_scalar_over_variance_matches_golden() {
    let actual = dispatch_sql(
        &single_group_scalar_over_variance_request().0,
        Vec::new(),
        Vec::new(),
        None,
        None,
    );
    let expected = include_str!("testdata/dispatch_golden/single_group_scalar_over_variance.sql");
    assert_eq!(actual, expected);
}

#[test]
fn empty_grouped_matches_golden() {
    let actual = empty_sql(&grouped_request().0, &[], &[]);
    let expected = include_str!("testdata/dispatch_golden/empty_grouped.sql");
    assert_eq!(actual, expected);
}

#[test]
fn empty_group_by_wrapper_matches_golden() {
    let actual = empty_sql(&group_by_fallback_request().0, &[], &[]);
    let expected = include_str!("testdata/dispatch_golden/empty_group_by_wrapper.sql");
    assert_eq!(actual, expected);
}

#[test]
fn empty_unmergeable_having_matches_group_by_wrapper_golden() {
    let (request, _) = unmergeable_having_request();
    let actual = empty_sql(&request, &[], &[]);
    let expected = include_str!("testdata/dispatch_golden/empty_unmergeable_having.sql");
    assert_eq!(actual, expected);

    let shape = classify_request_shape(
        &parsed_pushdown(&request),
        ScanTranslator::new(&column_types(&base_col_types())),
    );
    assert!(
        matches!(shape, RequestShape::GroupByWrapper),
        "an unmergeable HAVING must route to GroupByWrapper, not stay classified as \
         Grouped: {shape:?}"
    );
}

#[test]
fn empty_single_group_agg_matches_golden() {
    let actual = empty_sql(&single_group_agg_request().0, &[], &[]);
    let expected = include_str!("testdata/dispatch_golden/empty_single_group_agg.sql");
    assert_eq!(actual, expected);
}

#[test]
fn empty_row_scan_matches_golden() {
    let (proj_cols, proj_types) = row_scan_projection();
    let actual = empty_sql(&row_scan_request().0, &proj_cols, &proj_types);
    let expected = include_str!("testdata/dispatch_golden/empty_row_scan.sql");
    assert_eq!(actual, expected);
}

#[test]
fn empty_single_group_scalar_over_aggregate_matches_golden() {
    let actual = empty_sql(&single_group_scalar_over_aggregate_request().0, &[], &[]);
    let expected =
        include_str!("testdata/dispatch_golden/empty_single_group_scalar_over_aggregate.sql");
    assert_eq!(actual, expected);
}

/// Every column node carries `tableAlias: "E"`, as for `FROM EVENTS e` (#193).
fn aliased_grouped_request() -> (Json, Vec<ExasolType>) {
    let column = |name: &str| aliased_col("EVENTS", "E", name);
    events_request()
        .group_by(vec![column("REGION")])
        .column(column("REGION"))
        .item(
            agg_item_expr("SUM", column("AMOUNT"), false),
            ExasolType::decimal(36, 2),
        )
        .build()
}

#[test]
fn aliased_single_table_group_by_renders_bare_group_key_and_select_expr() {
    let request = aliased_grouped_request().0;
    let raw_pushdown_req = pd(&request);
    let stripped_pushdown_req = strip_table_alias(&raw_pushdown_req);

    let actual = dispatch_sql_with_pushdown_req(
        &request,
        &stripped_pushdown_req,
        Vec::new(),
        Vec::new(),
        base_col_types(),
        Some(r#"("AMOUNT" > 100)"#.to_string()),
        Some(50),
    );

    assert!(
        actual.contains(r#""group_keys":["\"REGION\""]"#),
        "GROUP BY key must render the bare column name, never alias-qualified: {actual}"
    );
    assert!(
        actual.contains(r#"{"kind":"sum","column":"AMOUNT"}"#),
        "the SUM aggregate argument must render the bare column name: {actual}"
    );
    assert!(
        !actual.contains(r#""E"."#),
        "no clause may carry the stale Exasol alias qualifier \"E\": {actual}"
    );
    let expected = include_str!("testdata/dispatch_golden/grouped_aggregate.sql");
    assert_eq!(
        actual, expected,
        "an aliased request must render byte-identical to the unaliased golden once \
         tableAlias is stripped upstream — the grouped partial/merge path never reads \
         tableName either, so its surviving presence changes nothing"
    );
}

fn aliased_multi_count_distinct_decline_request() -> (Json, Vec<ExasolType>) {
    let column = |name: &str| aliased_col("EVENTS", "E", name);
    let count_distinct = |name: &str| agg_item_expr("COUNT", column(name), true);
    events_request()
        .item(count_distinct("ID"), ExasolType::decimal(18, 0))
        .item(count_distinct("NAME"), ExasolType::decimal(18, 0))
        .build()
}

#[test]
fn aliased_multi_count_distinct_fallback_qualifies_lhs_t0_regardless_of_alias_presence() {
    let request = aliased_multi_count_distinct_decline_request().0;
    let raw_pushdown_req = pd(&request);
    let stripped_pushdown_req = strip_table_alias(&raw_pushdown_req);

    let sql_with_alias = dispatch_sql_with_pushdown_req(
        &request,
        &raw_pushdown_req,
        Vec::new(),
        Vec::new(),
        base_col_types(),
        None,
        None,
    );
    let sql_stripped = dispatch_sql_with_pushdown_req(
        &request,
        &stripped_pushdown_req,
        Vec::new(),
        Vec::new(),
        base_col_types(),
        None,
        None,
    );

    assert_eq!(
        sql_with_alias, sql_stripped,
        "a caller-supplied stale tableAlias must have no effect on the fallback \
         wrapper's rendered SQL — it re-qualifies from tableName unconditionally"
    );
    assert!(
        sql_with_alias.contains(r#""LHS_T0"."ID""#)
            && sql_with_alias.contains(r#""LHS_T0"."NAME""#),
        "the wrapper must qualify every column reference to its own subquery alias \
         LHS_T0, whether or not the request carried a tableAlias: {sql_with_alias}"
    );
}

// Covers every `AggKind` and every partial-column arity (1, 2, 3) in one request,
// over a dedicated column universe with a numeric SCORE and a MIN/MAX target TS.

fn all_agg_kinds_col_types() -> Vec<(String, ExasolType)> {
    ALL_AGG_KINDS_COLUMNS
        .iter()
        .map(|(name, exa_type)| (name.to_string(), *exa_type))
        .collect()
}

const ALL_AGG_KINDS_COLUMNS: [(&str, ExasolType); 4] = [
    ("ID", ExasolType::decimal(20, 0)),
    ("SCORE", ExasolType::DoublePrecision),
    ("TS", ExasolType::BARE_TIMESTAMP),
    ("REGION", ExasolType::WIDEST_VARCHAR),
];

fn all_agg_kinds_request() -> PushdownRequestBuilder {
    pushdown_request("EVENTS", &ALL_AGG_KINDS_COLUMNS)
}

fn all_agg_kinds_select_list() -> Vec<(Json, ExasolType)> {
    let count = ExasolType::decimal(18, 0);
    let double = ExasolType::DoublePrecision;
    let timestamp = ExasolType::BARE_TIMESTAMP;
    vec![
        (count_star(), count),
        (agg_item("COUNT", Some("id"), false), count),
        (agg_item("SUM", Some("score"), false), double),
        (agg_item("MIN", Some("ts"), false), timestamp),
        (agg_item("MAX", Some("ts"), false), timestamp),
        (agg_item("AVG", Some("score"), false), double),
        (agg_item("STDDEV", Some("score"), false), double),
        (agg_item("STDDEV_POP", Some("score"), false), double),
        (agg_item("VARIANCE", Some("score"), false), double),
        (agg_item("VAR_POP", Some("score"), false), double),
    ]
}

fn single_group_all_agg_kinds_request() -> (Json, Vec<ExasolType>) {
    all_agg_kinds_select_list()
        .into_iter()
        .fold(all_agg_kinds_request(), |request, (item, exa_type)| {
            request.item(item, exa_type)
        })
        .build()
}

/// The group key at ordinal 0 shifts the aggregates by one, exercising the
/// plan-ordinal versus column-ordinal distinction.
fn grouped_all_agg_kinds_request() -> (Json, Vec<ExasolType>) {
    all_agg_kinds_select_list()
        .into_iter()
        .fold(
            all_agg_kinds_request()
                .group_by(vec![col("REGION")])
                .column(col("REGION")),
            |request, (item, exa_type)| request.item(item, exa_type),
        )
        .build()
}

fn dispatch_sql_with_col_types(request: &Json, col_types: Vec<(String, ExasolType)>) -> String {
    dispatch_sql_with_pushdown_req(
        request,
        &pd(request),
        Vec::new(),
        Vec::new(),
        col_types,
        None,
        None,
    )
}

#[test]
fn single_group_all_agg_kinds_matches_golden() {
    let actual = dispatch_sql_with_col_types(
        &single_group_all_agg_kinds_request().0,
        all_agg_kinds_col_types(),
    );
    let expected = include_str!("testdata/dispatch_golden/single_group_all_agg_kinds.sql");
    assert_eq!(actual, expected);
}

#[test]
fn grouped_all_agg_kinds_matches_golden() {
    let actual = dispatch_sql_with_col_types(
        &grouped_all_agg_kinds_request().0,
        all_agg_kinds_col_types(),
    );
    let expected = include_str!("testdata/dispatch_golden/grouped_all_agg_kinds.sql");
    assert_eq!(actual, expected);
}

use super::joins::{JoinScanRequestConfig, build_broadcast_join_sql, build_n_scan_join_sql};

fn two_table_join_request(filter: Option<Json>) -> (Json, Vec<ExasolType>) {
    let mut request = pushdown_request(
        "CUSTOMER",
        &[
            ("C_CUSTKEY", ExasolType::decimal(20, 0)),
            ("C_NAME", ExasolType::varchar(100)),
        ],
    )
    .table(
        "ORDERS",
        &[
            ("O_CUSTKEY", ExasolType::decimal(20, 0)),
            ("O_ORDERDATE", ExasolType::Date),
        ],
    )
    .field("type", "select")
    .field("from", customer_orders_join())
    .column(table_col("CUSTOMER", "C_NAME"))
    .column(table_col("ORDERS", "O_ORDERDATE"));
    if let Some(f) = filter {
        request = request.filter(f);
    }
    let (mut request, item_types) = request.build();
    request["schemaMetadataInfo"] = serde_json::json!({"properties": {}});
    (
        with_identifier_notes(
            request,
            &[("CUSTOMER", "lh.customer"), ("ORDERS", "lh.orders")],
        ),
        item_types,
    )
}

fn two_table_detected_join(request: &Json) -> DetectedJoin {
    match detect_join(request, &pd(request)).expect("two-table join must be detected") {
        JoinShape::Join(join) => join,
        other => panic!("expected a detected join, got {other:?}"),
    }
}

fn resolved_customer_side() -> ResolvedJoinSide {
    ResolvedJoinSide {
        table_name: "CUSTOMER".to_string(),
        table_identifier: "lh.customer".to_string(),
        table_root: "s3://warehouse/lh/customer".to_string(),
        files: vec![FileEntry::new("s3://w/c-0.parquet", 10)],
        logical_schema: vec![LogicalField {
            field_id: Some(1),
            name: "CUSTOMER_KEY".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        }],
        name_mapping: Vec::new(),
        effective_storage: sample_storage(),
        partition_columns: Vec::new(),
        total_bytes: 10,
        refused_columns: Vec::new(),
        column_types: ColumnTypes::default(),
    }
}

fn resolved_orders_side() -> ResolvedJoinSide {
    ResolvedJoinSide {
        table_name: "ORDERS".to_string(),
        table_identifier: "lh.orders".to_string(),
        table_root: "s3://warehouse/lh/orders".to_string(),
        files: vec![FileEntry::new("s3://w/o-0.parquet", 100)],
        logical_schema: vec![LogicalField {
            field_id: Some(1),
            name: "ORDERS_KEY".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        }],
        name_mapping: Vec::new(),
        effective_storage: sample_storage(),
        partition_columns: Vec::new(),
        total_bytes: 100,
        refused_columns: Vec::new(),
        column_types: ColumnTypes::default(),
    }
}

fn join_scan_tuning() -> JoinScanRequestConfig<'static> {
    JoinScanRequestConfig {
        cluster_nodes: 1,
        parallelism_factor: 1,
        df_target_partitions: 1,
        df_batch_size: 8192,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 0,
        s3_max_connections: 1,
        binary_as_string: false,
        connection: &super::test_support::TEST_CONNECTION,
    }
}

#[test]
fn filterless_request_emits_unchanged_sql_at_all_three_sites() {
    let (proj_cols, proj_types) = row_scan_projection();
    let single_table_sql = dispatch_sql(&row_scan_request().0, proj_cols, proj_types, None, None);
    assert_eq!(
        single_table_sql,
        include_str!("testdata/dispatch_golden/filterless_single_table.sql")
    );

    let (request, _) = two_table_join_request(None);
    let join = two_table_detected_join(&request);
    let rendered = render_broadcast_join(
        &parsed_pushdown(&request),
        &join,
        &request_sides(&request, &join),
    )
    .expect("render_broadcast_join must not error for a well-formed request")
    .expect("a filterless equi-join must stay broadcast-eligible");
    let sides = broadcast_sides(resolved_orders_side(), resolved_customer_side(), 0);
    let broadcast_sql = build_broadcast_join_sql(
        &sides,
        &rendered,
        &join_scan_tuning(),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    )
    .expect("selecting the wire storage must succeed")
    .expect("an unbounded broadcast join must build");
    assert_eq!(
        broadcast_sql,
        include_str!("testdata/dispatch_golden/filterless_broadcast_join.sql")
    );

    let sides = vec![resolved_customer_side(), resolved_orders_side()];
    let n_scan_sql = build_n_scan_join_sql(
        &parsed_pushdown(&request),
        &join,
        &request_typed(&request, &sides),
        &join_scan_tuning(),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    )
    .expect("build_n_scan_join_sql must succeed for this fixture");
    assert_eq!(
        n_scan_sql,
        include_str!("testdata/dispatch_golden/filterless_n_scan_join.sql")
    );
}

#[test]
fn rendering_filter_emits_unchanged_wrapper_free_scan() {
    let (proj_cols, proj_types) = row_scan_projection();
    let single_table_sql = dispatch_sql(
        &row_scan_request().0,
        proj_cols,
        proj_types,
        Some(r#"("REGION" = 'EU')"#.to_string()),
        None,
    );
    assert_eq!(
        single_table_sql,
        include_str!("testdata/dispatch_golden/rendering_single_table.sql")
    );

    let renderable_filter = string_equal("CUSTOMER", "C_NAME", "ACME");
    let (request, _) = two_table_join_request(Some(renderable_filter));
    let join = two_table_detected_join(&request);
    let rendered = render_broadcast_join(
        &parsed_pushdown(&request),
        &join,
        &request_sides(&request, &join),
    )
    .expect("render_broadcast_join must not error for a well-formed request")
    .expect("an equi-join with a rendering filter must stay broadcast-eligible");
    let sides = broadcast_sides(resolved_orders_side(), resolved_customer_side(), 0);
    let broadcast_sql = build_broadcast_join_sql(
        &sides,
        &rendered,
        &join_scan_tuning(),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    )
    .expect("selecting the wire storage must succeed")
    .expect("an unbounded broadcast join must build");
    assert_eq!(
        broadcast_sql,
        include_str!("testdata/dispatch_golden/rendering_broadcast_join.sql")
    );

    let sides = vec![resolved_customer_side(), resolved_orders_side()];
    let n_scan_sql = build_n_scan_join_sql(
        &parsed_pushdown(&request),
        &join,
        &request_typed(&request, &sides),
        &join_scan_tuning(),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    )
    .expect("build_n_scan_join_sql must succeed for this fixture");
    assert_eq!(
        n_scan_sql,
        include_str!("testdata/dispatch_golden/rendering_n_scan_join.sql")
    );
}
