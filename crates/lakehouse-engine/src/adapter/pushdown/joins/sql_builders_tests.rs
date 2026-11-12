use super::super::super::support::{DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME};
use super::super::planning::{
    JoinLeaf, JoinShape, classify_join_window, detect_join, select_broadcast_sides,
};
use super::super::tests::{
    broadcast_projection, detected_join, equi_condition, join_request, legs_from_leaves,
    nq3_join_request, request_legs, resolved_side, self_join_request, three_table_join_request,
    two_scan_tuning,
};
use super::*;
use crate::adapter::pushdown::test_support::*;
use crate::scan::spec::{ColumnRef, SortKey, StorageBackend, StorageProps};
use crate::types::exasol_type::{ExasolType, exasol_type_to_json};

/// The N-scan wrapper over the two-table `request`, its CUSTOMER and ORDERS sides one file each.
fn n_scan_sql(request: &Json) -> Result<String, UdfError> {
    let sides = vec![
        resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
        resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
    ];
    build_n_scan_join_sql(
        &parsed_pushdown(request),
        &detected_join(request),
        &request_typed(request, &sides),
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
}

/// Q1 shape: `(SUPPLIER ⋈ NATION) ⋈ REGION`.
fn q1_join_request() -> Json {
    with_identifier_notes(
        pushdown_request(
            "SUPPLIER",
            &[
                ("S_SUPPKEY", ExasolType::decimal(20, 0)),
                ("S_NATIONKEY", ExasolType::decimal(20, 0)),
                ("S_NAME", ExasolType::varchar(100)),
            ],
        )
        .table(
            "NATION",
            &[
                ("N_NATIONKEY", ExasolType::decimal(20, 0)),
                ("N_REGIONKEY", ExasolType::decimal(20, 0)),
            ],
        )
        .table(
            "REGION",
            &[
                ("R_REGIONKEY", ExasolType::decimal(20, 0)),
                ("R_NAME", ExasolType::varchar(100)),
            ],
        )
        .field("type", serde_json::json!("select"))
        .field(
            "from",
            inner_join(
                inner_join(
                    table_ref("SUPPLIER"),
                    table_ref("NATION"),
                    predicate_equal(
                        table_col("SUPPLIER", "S_NATIONKEY"),
                        table_col("NATION", "N_NATIONKEY"),
                    ),
                ),
                table_ref("REGION"),
                predicate_equal(
                    table_col("NATION", "N_REGIONKEY"),
                    table_col("REGION", "R_REGIONKEY"),
                ),
            ),
        )
        .column(table_col("SUPPLIER", "S_NAME"))
        .column(table_col("REGION", "R_NAME"))
        .request_field("schemaMetadataInfo", serde_json::json!({"properties": {}}))
        .build()
        .0,
        &[
            ("SUPPLIER", "lh.supplier"),
            ("NATION", "lh.nation"),
            ("REGION", "lh.region"),
        ],
    )
}

#[test]
fn broadcast_join_declines_widened_projection() {
    let mut request = join_request(Json::Null, equi_condition());
    request = selecting(
        request,
        &[(
            scalar_fn("UPPER", [table_col("CUSTOMER", "C_NAME")]),
            ExasolType::varchar(100),
        )],
    );
    let pushdown_req = pd(&request);
    assert!(matches!(
        classify_join_window(&pushdown_req),
        JoinWindowPlan::Unbounded
    ));
    let detected = detected_join(&request);

    let mut ok_req = request.clone();
    ok_req["pushdownRequest"]["selectListDataTypes"] =
        serde_json::json!([exasol_type_to_json(&ExasolType::varchar(100))]);
    let (_, _, ok_widened) = broadcast_projection(&ok_req);
    assert!(!ok_widened, "the control fixture must NOT widen");
    assert!(
        render_broadcast_join(
            &parsed_pushdown(&ok_req),
            &detected,
            &request_sides(&ok_req, &detected)
        )
        .expect("the control must not error")
        .is_some(),
        "the control must render a broadcast plan, so only the widening differs"
    );

    request["pushdownRequest"]["selectListDataTypes"] = serde_json::json!([exasol_type_to_json(
        &ExasolType::TimestampWithLocalTimeZone { precision: 3 }
    )]);
    let (projection, _, widened) = broadcast_projection(&request);
    assert!(
        widened && projection.len() == 4,
        "precondition: the one-item select list must widen to the 4-column \
         two-table base row"
    );

    assert!(
        render_broadcast_join(
            &parsed_pushdown(&request),
            &detected,
            &request_sides(&request, &detected)
        )
        .expect("a widened projection is a clean decline, NOT an error")
        .is_none(),
        "a widened projection must decline broadcast rendering (Ok(None)) so the \
         request falls through to the unified N-scan fallback"
    );
}

#[test]
fn broadcast_declines_on_a_declined_filter() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["filter"] =
        declined_second_predicate(table_col("ORDERS", "O_ORDERDATE"));
    let detected = detected_join(&request);

    assert!(
        render_broadcast_join(
            &parsed_pushdown(&request),
            &detected,
            &request_sides(&request, &detected)
        )
        .expect("a declined filter is a clean decline, not an error")
        .is_none(),
        "a declined filter must decline the broadcast plan (Ok(None)) so the \
         request falls through to the N-scan fallback, which self-applies it"
    );
}

#[test]
fn broadcast_declines_instr_with_start_position_argument() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["filter"] = compare(
        "predicate_greater",
        scalar_fn(
            "INSTR",
            [table_col("CUSTOMER", "C_NAME"), str_lit("b"), num_lit(3)],
        ),
        num_lit(0),
    );
    let detected = detected_join(&request);

    let outcome = render_broadcast_join(
        &parsed_pushdown(&request),
        &detected,
        &request_sides(&request, &detected),
    )
    .expect("a type-declined filter is a clean decline, not an error");
    assert!(
        outcome.is_none(),
        "INSTR with a start-position argument must decline the broadcast plan \
         (Ok(None)) so Exasol evaluates it natively via the N-scan fallback's \
         residual WHERE"
    );
}

#[test]
fn broadcast_absent_filter_stays_eligible() {
    let absent_request = join_request(Json::Null, equi_condition());
    let absent_detected = detected_join(&absent_request);
    let absent = render_broadcast_join(
        &parsed_pushdown(&absent_request),
        &absent_detected,
        &request_sides(&absent_request, &absent_detected),
    )
    .expect("an absent filter must not error")
    .expect("an absent filter must keep the broadcast plan");
    assert!(
        absent.filter.is_none(),
        "an absent filter must carry no scan-spec filter: {:?}",
        absent.filter
    );
}

/// Exasol keeps the `TRUE` a folded `'' IS NULL` leaves in a conjunction, so once the leg takes
/// `C_CUSTKEY > 1` the residual is that lone `TRUE`.
#[test]
fn a_lone_true_residual_renders_as_the_outer_where() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["filter"] = and([
        compare(
            "predicate_less",
            num_lit(1),
            table_col("CUSTOMER", "C_CUSTKEY"),
        ),
        bool_lit(true),
    ]);

    let sql = n_scan_sql(&request).expect("a lone TRUE residual must render, not error");

    assert!(sql.ends_with(" WHERE (TRUE)"), "{sql}");
}

#[test]
fn two_table_join_falls_back_to_unified_n_scan_wrapper() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["filter"] = compare(
        "predicate_greater",
        table_col("ORDERS", "O_ORDERDATE"),
        date_lit("1995-01-01"),
    );
    let sql = n_scan_sql(&request).expect("the two-table unified fallback must build");

    for alias in ["LHS_T0", "LHS_T1"] {
        assert!(
            sql.contains(&format!(r#"AS "{alias}""#)),
            "both side fan-outs must appear as aliased derived-table subqueries: {sql}"
        );
    }
    assert!(
        sql.contains(r#"AS "LHS_T1" ON (("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY"))"#),
        "the equi-condition must attach table-qualified as the join point's ON clause: {sql}"
    );
    assert!(
        sql.contains(r#"SELECT "LHS_T0"."C_NAME", "LHS_T1"."O_ORDERDATE" FROM"#),
        "the cross-table projection must drive the outer SELECT in order: {sql}"
    );
    assert!(
        sql.contains("'1995-01-01'"),
        "the ORDERS-side-local filter must be pushed into that leg's fan-out: {sql}"
    );
    assert!(
        !sql.contains(" WHERE "),
        "every side-local filter is pushed into its leg, so no residual outer WHERE: {sql}"
    );
    assert!(sql.contains("INNER JOIN"), "{sql}");
    assert!(
        !sql.contains("\"join\":{"),
        "the fallback must not embed a broadcast join block: {sql}"
    );
}

#[test]
fn colliding_columns_render_qualified_unified_wrapper_without_error() {
    let request = with_identifier_notes(
        pushdown_request(
            "EVENTS",
            &[
                ("ID", ExasolType::decimal(18, 0)),
                ("SCORE", ExasolType::DoublePrecision),
            ],
        )
        .table(
            "LABELS",
            &[
                ("ID", ExasolType::decimal(18, 0)),
                ("LABEL", ExasolType::varchar(100)),
            ],
        )
        .field("type", serde_json::json!("select"))
        .field(
            "from",
            inner_join(
                table_ref("EVENTS"),
                table_ref("LABELS"),
                predicate_equal(table_col("EVENTS", "ID"), table_col("LABELS", "ID")),
            ),
        )
        .column(table_col("EVENTS", "ID"))
        .column(table_col("LABELS", "LABEL"))
        .request_field("schemaMetadataInfo", serde_json::json!({"properties": {}}))
        .build()
        .0,
        &[("EVENTS", "lh.events"), ("LABELS", "lh.labels")],
    );

    let detected = detected_join(&request);
    let sides = vec![
        resolved_side("EVENTS", vec![("s3://w/e-0.parquet", 100)]),
        resolved_side("LABELS", vec![("s3://w/l-0.parquet", 10)]),
    ];
    let sql = build_n_scan_join_sql(
        &parsed_pushdown(&request),
        &detected,
        &request_typed(&request, &sides),
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("the qualified unified fallback must build despite the column-name collision");

    assert!(
        sql.contains(r#"("LHS_T0"."ID" = "LHS_T1"."ID")"#),
        "the equi-condition must be table-qualified, never bare/ambiguous: {sql}"
    );
    assert!(
        sql.contains(r#""LHS_T0"."ID""#) && sql.contains(r#""LHS_T1"."LABEL""#),
        "the projection must be table-qualified per owning side: {sql}"
    );
    assert!(sql.contains("INNER JOIN"), "{sql}");
}

/// Scenario: A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper
#[test]
fn build_n_scan_join_sql_produces_qualified_n_scan_wrapper() {
    let request = three_table_join_request();
    let multi = match detect_join(&request, &pd(&request)).expect("detected join shape") {
        JoinShape::Join(m) => m,
        other => panic!("expected Join, got {other:?}"),
    };
    let sides = vec![
        resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
        resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
        resolved_side("LINEITEM", vec![("s3://w/l-0.parquet", 500)]),
    ];
    let sql = build_n_scan_join_sql(
        &parsed_pushdown(&request),
        &multi,
        &request_typed(&request, &sides),
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("an all-inner N-scan wrapper must build, never Err");

    for alias in ["LHS_T0", "LHS_T1", "LHS_T2"] {
        assert!(
            sql.contains(&format!(r#"AS "{alias}""#)),
            "missing distinct fan-out alias {alias}: {sql}"
        );
    }
    assert!(
        sql.contains(r#""LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY""#),
        "first join condition must be table-qualified: {sql}"
    );
    assert!(
        sql.contains(r#""LHS_T1"."O_ORDERKEY" = "LHS_T2"."L_ORDERKEY""#),
        "second join condition must be table-qualified: {sql}"
    );
    assert_eq!(
        sql.matches("INNER JOIN").count(),
        2,
        "conditions must attach across a two-hop INNER JOIN … ON chain: {sql}"
    );
    assert!(
        sql.contains(r#""LHS_T0"."C_NAME""#) && sql.contains(r#""LHS_T2"."L_QUANTITY""#),
        "the select list must be qualified to each column's owning side: {sql}"
    );
}

#[test]
fn build_n_scan_join_sql_for_q1_shape_supplier_nation_region() {
    let request = q1_join_request();
    let multi = match detect_join(&request, &pd(&request)).expect("detected join shape") {
        JoinShape::Join(m) => m,
        other => panic!("expected Join, got {other:?}"),
    };
    let sides = vec![
        resolved_side("SUPPLIER", vec![("s3://w/s-0.parquet", 10)]),
        resolved_side("NATION", vec![("s3://w/n-0.parquet", 5)]),
        resolved_side("REGION", vec![("s3://w/r-0.parquet", 2)]),
    ];
    let sql = build_n_scan_join_sql(
        &parsed_pushdown(&request),
        &multi,
        &request_typed(&request, &sides),
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("the Q1-shape (supplier⋈nation⋈region) must build, never Err");

    for alias in ["LHS_T0", "LHS_T1", "LHS_T2"] {
        assert!(
            sql.contains(&format!(r#"AS "{alias}""#)),
            "missing distinct fan-out alias {alias}: {sql}"
        );
    }
    assert!(
        sql.contains(r#""LHS_T0"."S_NATIONKEY" = "LHS_T1"."N_NATIONKEY""#),
        "first join condition must be table-qualified: {sql}"
    );
    assert!(
        sql.contains(r#""LHS_T1"."N_REGIONKEY" = "LHS_T2"."R_REGIONKEY""#),
        "second join condition must be table-qualified: {sql}"
    );
}

#[test]
fn build_n_scan_join_sql_for_nq3_shape_part_partsupp_supplier_nation() {
    let request = nq3_join_request();
    let multi = match detect_join(&request, &pd(&request)).expect("detected join shape") {
        JoinShape::Join(m) => m,
        other => panic!("expected Join, got {other:?}"),
    };
    let sides = vec![
        resolved_side("PART", vec![("s3://w/p-0.parquet", 10)]),
        resolved_side("PARTSUPP", vec![("s3://w/ps-0.parquet", 40)]),
        resolved_side("SUPPLIER", vec![("s3://w/s-0.parquet", 5)]),
        resolved_side("NATION", vec![("s3://w/n-0.parquet", 3)]),
    ];
    let sql = build_n_scan_join_sql(
        &parsed_pushdown(&request),
        &multi,
        &request_typed(&request, &sides),
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("the NQ3-shape (part⋈partsupp⋈supplier⋈nation) must build, never Err");

    for alias in ["LHS_T0", "LHS_T1", "LHS_T2", "LHS_T3"] {
        assert!(
            sql.contains(&format!(r#"AS "{alias}""#)),
            "missing distinct fan-out alias {alias}: {sql}"
        );
    }
    assert!(
        sql.contains(r#""LHS_T0"."P_PARTKEY" = "LHS_T1"."PS_PARTKEY""#),
        "first join condition must be table-qualified: {sql}"
    );
    assert!(
        sql.contains(r#""LHS_T1"."PS_SUPPKEY" = "LHS_T2"."S_SUPPKEY""#),
        "second join condition must be table-qualified: {sql}"
    );
    assert!(
        sql.contains(r#""LHS_T2"."S_NATIONKEY" = "LHS_T3"."N_NATIONKEY""#),
        "third join condition must be table-qualified: {sql}"
    );
}

#[test]
fn build_n_scan_join_sql_renders_qualified_when_three_tables_share_column_name() {
    let request = with_identifier_notes(
        pushdown_request(
            "EVENTS",
            &[
                ("ID", ExasolType::decimal(18, 0)),
                ("SCORE", ExasolType::DoublePrecision),
            ],
        )
        .table(
            "LABELS",
            &[
                ("ID", ExasolType::decimal(18, 0)),
                ("LABEL", ExasolType::varchar(100)),
            ],
        )
        .table(
            "TAGS",
            &[
                ("ID", ExasolType::decimal(18, 0)),
                ("TAG_NAME", ExasolType::varchar(100)),
            ],
        )
        .field("type", serde_json::json!("select"))
        .field(
            "from",
            inner_join(
                inner_join(
                    table_ref("EVENTS"),
                    table_ref("LABELS"),
                    predicate_equal(table_col("EVENTS", "ID"), table_col("LABELS", "ID")),
                ),
                table_ref("TAGS"),
                predicate_equal(table_col("LABELS", "ID"), table_col("TAGS", "ID")),
            ),
        )
        .column(table_col("EVENTS", "ID"))
        .column(table_col("LABELS", "LABEL"))
        .column(table_col("TAGS", "TAG_NAME"))
        .request_field("schemaMetadataInfo", serde_json::json!({"properties": {}}))
        .build()
        .0,
        &[
            ("EVENTS", "lh.events"),
            ("LABELS", "lh.labels"),
            ("TAGS", "lh.tags"),
        ],
    );
    let multi = match detect_join(&request, &pd(&request)).expect("detected join shape") {
        JoinShape::Join(m) => m,
        other => panic!("expected Join, got {other:?}"),
    };
    let sides = vec![
        resolved_side("EVENTS", vec![("s3://w/e-0.parquet", 100)]),
        resolved_side("LABELS", vec![("s3://w/l-0.parquet", 10)]),
        resolved_side("TAGS", vec![("s3://w/t-0.parquet", 10)]),
    ];
    let sql = build_n_scan_join_sql(
        &parsed_pushdown(&request),
        &multi,
        &request_typed(&request, &sides),
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("three tables sharing an ID column must still build, never Err");

    assert!(
        sql.contains(r#""LHS_T0"."ID" = "LHS_T1"."ID""#),
        "first condition must be table-qualified, never bare/ambiguous: {sql}"
    );
    assert!(
        sql.contains(r#""LHS_T1"."ID" = "LHS_T2"."ID""#),
        "second condition must be table-qualified, never bare/ambiguous: {sql}"
    );
    assert!(
        sql.starts_with(r#"SELECT "LHS_T0"."ID", "LHS_T1"."LABEL", "LHS_T2"."TAG_NAME" FROM "#),
        "the outer SELECT list must qualify the shared ID column, never bare: {sql}"
    );
}

#[test]
fn self_join_renders_each_occurrence_as_its_own_leg() {
    for leg_aliases in [[Some("A"), Some("B")], [None, Some("B")]] {
        let request = self_join_request(&leg_aliases);
        let sides = vec![
            resolved_side("FACT_ORDERS", vec![("s3://w/f0.parquet", 10)]),
            resolved_side("FACT_ORDERS", vec![("s3://w/f1.parquet", 10)]),
        ];
        let sql = build_n_scan_join_sql(
            &parsed_pushdown(&request),
            &detected_join(&request),
            &request_typed(&request, &sides),
            &two_scan_tuning(),
            "SCAN",
            "DISTRIBUTE",
        )
        .expect("a two-leg self-join must build through the unified fallback, never Err");

        assert!(
            sql.contains(r#"ON (("LHS_T0"."O_ORDERKEY" = "LHS_T1"."O_ORDERKEY"))"#),
            "leg_aliases {leg_aliases:?}: the ON must compare two DISTINCT occurrences, never \
             the pre-fix tautology over one collapsed leg: {sql}"
        );
        assert!(
            sql.starts_with(r#"SELECT "LHS_T0"."O_ORDERKEY" FROM"#),
            "leg_aliases {leg_aliases:?}: the select list must qualify by the referencing \
             occurrence's own leg, not the last-write-wins collapse that made every item \
             `LHS_T1`: {sql}"
        );
    }
}

#[test]
fn three_leg_self_join_attaches_each_condition_at_its_own_join_point() {
    let request = self_join_request(&[Some("A"), Some("B"), Some("C")]);
    let sides = vec![
        resolved_side("FACT_ORDERS", vec![("s3://w/f0.parquet", 10)]),
        resolved_side("FACT_ORDERS", vec![("s3://w/f1.parquet", 10)]),
        resolved_side("FACT_ORDERS", vec![("s3://w/f2.parquet", 10)]),
    ];
    let sql = build_n_scan_join_sql(
        &parsed_pushdown(&request),
        &detected_join(&request),
        &request_typed(&request, &sides),
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("a three-leg self-join must build through the unified fallback, never Err");

    assert!(
        !sql.contains("1=1"),
        "no join point may fall back to a tautology when a real condition attaches: {sql}"
    );
    assert_eq!(
        sql.matches(r#""LHS_T0"."O_ORDERKEY" = "LHS_T1"."O_ORDERKEY""#)
            .count(),
        1,
        "the A=B condition must attach exactly once, at the first join point: {sql}"
    );
    assert_eq!(
        sql.matches(r#""LHS_T1"."O_ORDERKEY" = "LHS_T2"."O_ORDERKEY""#)
            .count(),
        1,
        "the B=C condition must attach exactly once, at the second join point, never \
         duplicated onto the first: {sql}"
    );
}

/// `O_CUSTKEY` of the self-join occurrence aliased `alias`.
fn custkey(alias: &str) -> Json {
    aliased_col("FACT_ORDERS", alias, "O_CUSTKEY")
}

#[test]
fn n_scan_wrapper_qualifies_every_clause_by_leg() {
    fn sum_of(alias: &str) -> Json {
        agg_item_expr("SUM", custkey(alias), false)
    }
    let mut request = self_join_request(&[Some("A"), Some("B")]);
    request["pushdownRequest"]["aggregationType"] = serde_json::json!("group_by");
    request = selecting(
        request,
        &[
            (custkey("A"), ExasolType::decimal(20, 0)),
            (sum_of("B"), ExasolType::decimal(36, 0)),
        ],
    );
    request["pushdownRequest"]["groupBy"] = serde_json::json!([custkey("A")]);
    request["pushdownRequest"]["having"] = compare("predicate_greater", sum_of("B"), num_lit(10));
    request["pushdownRequest"]["orderBy"] =
        serde_json::json!([order_by_element(custkey("A"), true, false)]);

    let sides = vec![
        resolved_side("FACT_ORDERS", vec![("s3://w/f0.parquet", 10)]),
        resolved_side("FACT_ORDERS", vec![("s3://w/f1.parquet", 10)]),
    ];
    let sql = build_n_scan_join_sql(
        &parsed_pushdown(&request),
        &detected_join(&request),
        &request_typed(&request, &sides),
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("every clause must render against its own occurrence's leg");

    assert!(
        sql.contains(
            r#"SELECT "LHS_T0"."O_CUSTKEY", CAST(SUM("LHS_T1"."O_CUSTKEY") AS DECIMAL(36,0))"#
        ),
        "the select list must qualify each item to its own occurrence: {sql}"
    );
    assert!(
        sql.contains(r#"GROUP BY "LHS_T0"."O_CUSTKEY""#),
        "GROUP BY must qualify to occurrence A: {sql}"
    );
    assert!(
        sql.contains(r#"HAVING (SUM("LHS_T1"."O_CUSTKEY") > 10)"#),
        "HAVING must qualify to occurrence B, nested inside the aggregate argument: {sql}"
    );
    assert!(
        sql.contains(r#"ORDER BY "LHS_T0"."O_CUSTKEY" ASC NULLS FIRST"#),
        "ORDER BY must qualify to occurrence A: {sql}"
    );
}

#[test]
fn conditions_attach_by_leg_set_and_leg_local_filters_partition_exactly() {
    let mut request = self_join_request(&[Some("A"), Some("B")]);
    request["pushdownRequest"]["filter"] = and([
        compare("predicate_greater", custkey("A"), num_lit(100)),
        predicate_equal(custkey("B"), num_lit(7)),
    ]);
    let sides = vec![
        resolved_side("FACT_ORDERS", vec![("s3://w/f0.parquet", 10)]),
        resolved_side("FACT_ORDERS", vec![("s3://w/f1.parquet", 10)]),
    ];
    let sql = build_n_scan_join_sql(
        &parsed_pushdown(&request),
        &detected_join(&request),
        &request_typed(&request, &sides),
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("a fully leg-partitioned self-join filter must build");

    assert!(
        sql.contains(r#"ON (("LHS_T0"."O_ORDERKEY" = "LHS_T1"."O_ORDERKEY"))"#),
        "the join condition must attach across the two occurrences: {sql}"
    );
    assert!(
        !sql.contains(" WHERE "),
        "both conjuncts are leg-local, so no cross-leg residual remains in the outer WHERE: {sql}"
    );

    let legs: Vec<Json> = sql
        .split('\'')
        .skip(1)
        .step_by(2)
        .step_by(2)
        .map(|blob| serde_json::from_str(blob).expect("each leg's common blob is valid JSON"))
        .collect();
    assert_eq!(legs.len(), 2, "one common blob per leg: {sql}");
    let occurrence_a_filter = legs[0]
        .get("filter")
        .and_then(|f| f.as_str())
        .expect("occurrence A's leg carries its own ScanSpec.filter");
    let occurrence_b_filter = legs[1]
        .get("filter")
        .and_then(|f| f.as_str())
        .expect("occurrence B's leg carries its own ScanSpec.filter");
    assert!(
        occurrence_a_filter.contains("100") && !occurrence_a_filter.contains('7'),
        "occurrence A's leg-local filter must carry only its own conjunct: {occurrence_a_filter}"
    );
    assert!(
        occurrence_b_filter.contains('7') && !occurrence_b_filter.contains("100"),
        "occurrence B's leg-local filter must carry only its own conjunct: {occurrence_b_filter}"
    );
}

#[test]
fn above_threshold_join_falls_back_inner_join_on() {
    let request = join_request(Json::Null, equi_condition());
    let sql = n_scan_sql(&request).expect("the above-threshold two-table fallback must build");

    assert!(
        sql.contains("INNER JOIN"),
        "the fallback FROM must be an INNER JOIN chain, not a comma cross-join: {sql}"
    );
    assert!(
        sql.contains(r#"AS "LHS_T0" INNER JOIN"#),
        "the first leg must be the left side of the INNER JOIN chain: {sql}"
    );
    assert!(
        sql.contains(r#"AS "LHS_T1" ON (("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY"))"#),
        "the equi-condition must attach table-qualified as the join point's ON clause: {sql}"
    );
    assert!(
        !sql.contains(r#"AS "LHS_T0", "#),
        "the legacy comma cross-join between legs must be gone: {sql}"
    );
}

#[test]
fn three_table_join_inner_join_on_chain() {
    let request = three_table_join_request();
    let detected = detected_join(&request);
    let sides = vec![
        resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
        resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
        resolved_side("LINEITEM", vec![("s3://w/l-0.parquet", 500)]),
    ];
    let sql = build_n_scan_join_sql(
        &parsed_pushdown(&request),
        &detected,
        &request_typed(&request, &sides),
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("the three-table inner-join chain must build");

    assert_eq!(
        sql.matches("INNER JOIN").count(),
        2,
        "N=3 tables → a two-hop INNER JOIN chain: {sql}"
    );
    assert!(
        sql.contains(r#"AS "LHS_T1" ON (("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY"))"#),
        "the first condition attaches at the join point bringing LHS_T1 into scope: {sql}"
    );
    assert!(
        sql.contains(r#"AS "LHS_T2" ON (("LHS_T1"."O_ORDERKEY" = "LHS_T2"."L_ORDERKEY"))"#),
        "the second condition attaches at the join point bringing LHS_T2 into scope: {sql}"
    );
    assert!(
        !sql.contains(" WHERE "),
        "every condition lives in an ON clause and there is no residual filter, so no \
         outer WHERE: {sql}"
    );
}

#[test]
fn join_conditions_greedy_attach_and_side_local_pushdown() {
    let cond_n2_fact = predicate_equal(table_col("N2", "N2_KEY"), table_col("FACT", "F_N2KEY"));
    let cond_n1_fact = predicate_equal(table_col("N1", "N1_KEY"), table_col("FACT", "F_N1KEY"));
    let request = with_identifier_notes(
        pushdown_request(
            "N1",
            &[
                ("N1_KEY", ExasolType::decimal(20, 0)),
                ("N1_NAME", ExasolType::varchar(100)),
            ],
        )
        .table("N2", &[("N2_KEY", ExasolType::decimal(20, 0))])
        .table(
            "FACT",
            &[
                ("F_N1KEY", ExasolType::decimal(20, 0)),
                ("F_N2KEY", ExasolType::decimal(20, 0)),
                ("F_VALUE", ExasolType::decimal(20, 0)),
            ],
        )
        .field("type", serde_json::json!("select"))
        .field(
            "from",
            inner_join(
                table_ref("N1"),
                inner_join(table_ref("N2"), table_ref("FACT"), cond_n2_fact),
                cond_n1_fact,
            ),
        )
        .filter(and([
            string_equal("N1", "N1_NAME", "ACME"),
            compare(
                "predicate_greater",
                table_col("FACT", "F_VALUE"),
                table_col("N1", "N1_KEY"),
            ),
        ]))
        .column(table_col("N1", "N1_NAME"))
        .column(table_col("FACT", "F_VALUE"))
        .request_field("schemaMetadataInfo", serde_json::json!({"properties": {}}))
        .build()
        .0,
        &[("N1", "lh.n1"), ("N2", "lh.n2"), ("FACT", "lh.fact")],
    );
    let detected = detected_join(&request);
    let sides = vec![
        resolved_side("N1", vec![("s3://w/n1-0.parquet", 10)]),
        resolved_side("N2", vec![("s3://w/n2-0.parquet", 10)]),
        resolved_side("FACT", vec![("s3://w/f-0.parquet", 500)]),
    ];
    let sql = build_n_scan_join_sql(
        &parsed_pushdown(&request),
        &detected,
        &request_typed(&request, &sides),
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("the star-shape greedy-attach fallback must build");

    assert!(
        sql.contains(r#"AS "LHS_T1" ON 1=1"#),
        "a join point with no newly-resolvable condition must render ON 1=1: {sql}"
    );
    assert!(
        sql.contains(r#"AS "LHS_T2" ON (("LHS_T1"."N2_KEY" = "LHS_T2"."F_N2KEY")) AND (("LHS_T0"."N1_KEY" = "LHS_T2"."F_N1KEY"))"#),
        "both FACT-touching conditions must attach greedily at the final join point: {sql}"
    );

    assert!(
        sql.contains("'ACME'"),
        "the side-local conjunct must be pushed into its leg's fan-out: {sql}"
    );
    let where_clause = &sql[sql
        .find(" WHERE ")
        .expect("the cross-table residual must remain in an outer WHERE")..];
    assert!(
        !where_clause.contains("ACME"),
        "the side-local conjunct must NOT be duplicated in the outer WHERE: {sql}"
    );
    assert!(
        where_clause.contains(r#""LHS_T2"."F_VALUE""#)
            && where_clause.contains(r#""LHS_T0"."N1_KEY""#),
        "the cross-table residual conjunct must render qualified in the outer WHERE: {sql}"
    );
}

fn n_scan_legs_and_outer_where(request: &Json) -> (String, String) {
    let sql = n_scan_sql(request).expect("the two-table unified fallback must build");
    match sql.find(" WHERE ") {
        Some(at) => (sql[..at].to_string(), sql[at..].to_string()),
        None => (sql, String::new()),
    }
}

fn n_scan_request_with_filter(filter: Json) -> Json {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["filter"] = filter;
    request
}

#[test]
fn join_like_over_varchar_side_column_pushes_down_unchanged() {
    let request = n_scan_request_with_filter(like(table_col("CUSTOMER", "C_NAME"), "A%"));
    let detected = detected_join(&request);

    let rendered = render_broadcast_join(
        &parsed_pushdown(&request),
        &detected,
        &request_sides(&request, &detected),
    )
    .expect("a VARCHAR LIKE subject must not error")
    .expect("a VARCHAR LIKE subject must keep the broadcast plan");
    let filter = rendered
        .filter
        .expect("the LIKE conjunct must still render as a scan-spec filter");
    assert!(
        filter.contains(r#""LHS_T0"."C_NAME" LIKE"#) && !filter.contains("CAST("),
        "a VARCHAR LIKE subject must render unchanged, with no spurious CAST: {filter}"
    );

    let (legs, outer) = n_scan_legs_and_outer_where(&request);
    assert!(
        legs.contains(r#"\"C_NAME\" LIKE"#) && !legs.contains("CAST("),
        "the same conjunct must reach the CUSTOMER fan-out leg unchanged: {legs}"
    );
    assert!(
        outer.is_empty(),
        "a conjunct both join sites accept must not also land in the outer \
         WHERE: {outer}"
    );
}

#[test]
fn join_instr_beyond_two_args_declines_at_both_join_sites() {
    let filter = compare(
        "predicate_greater",
        scalar_fn(
            "INSTR",
            [table_col("CUSTOMER", "C_NAME"), str_lit("b"), num_lit(3)],
        ),
        num_lit(0),
    );
    let request = n_scan_request_with_filter(filter);

    let (legs, outer) = n_scan_legs_and_outer_where(&request);

    assert!(
        outer.contains(r#""LHS_T0"."C_NAME""#) && outer.contains("INSTR("),
        "the type-declined INSTR conjunct must be self-applied table-qualified \
         in the outer WHERE: {outer}"
    );
    assert!(
        !legs.contains("INSTR("),
        "it must NOT also reach the CUSTOMER fan-out leg — that is the tree \
         the arity guard rejects: {legs}"
    );
}

#[test]
fn aggregate_over_join_renders_exasol_aggregate_over_unified_wrapper() {
    let mut request = join_request(Json::Null, equi_condition());
    request = selecting(
        request,
        &[
            (count_star(), ExasolType::decimal(18, 0)),
            (
                agg_item_expr("MIN", table_col("ORDERS", "O_ORDERDATE"), false),
                ExasolType::Date,
            ),
        ],
    );

    assert!(matches!(
        classify_join_window(&pd(&request)),
        JoinWindowPlan::ExasolPostProcessed
    ));

    let sql = n_scan_sql(&request).expect("aggregate-over-join must build the unified wrapper");

    assert!(sql.contains("COUNT(*)"), "COUNT(*) must be rendered: {sql}");
    assert!(
        sql.contains(r#"MIN("LHS_T1"."O_ORDERDATE")"#),
        "MIN must qualify its argument to the owning side: {sql}"
    );
    assert!(
        sql.starts_with(
            r#"SELECT CAST(COUNT(*) AS DECIMAL(18,0)), CAST(MIN("LHS_T1"."O_ORDERDATE") AS DATE) FROM"#
        ),
        "the outer SELECT must be exactly the two aggregate columns: {sql}"
    );
    assert!(
        sql.contains("INNER JOIN") && !sql.contains("\"join\":{"),
        "aggregate-over-join is an INNER JOIN chain fallback, never a broadcast block: {sql}"
    );
}

fn seam_legs() -> JoinLegs {
    let leaves: Vec<JoinLeaf> = ["CUSTOMER", "ORDERS", "LINEITEM"]
        .iter()
        .map(|name| JoinLeaf {
            table_name: (*name).to_string(),
            table_alias: None,
            table_identifier: format!("lh.{}", name.to_lowercase()),
        })
        .collect();
    legs_from_leaves(leaves)
}

#[test]
fn render_expression_qualified_renders_scalar_over_aggregate() {
    let legs = seam_legs();
    let sum_case = agg_item_expr(
        "SUM",
        scalar_fn(
            "CASE",
            [
                string_equal("LINEITEM", "L_RETURNFLAG", "R"),
                num_lit(1),
                num_lit(0),
            ],
        ),
        false,
    );
    let count_star = count_star();
    let item = scalar_fn(
        "ROUND",
        [
            scalar_fn(
                "FLOAT_DIV",
                [
                    scalar_fn("MULT", [lit("literal_double", 100.0), sum_case]),
                    count_star,
                ],
            ),
            num_lit(2),
        ],
    );

    let sql = render_expression_qualified(&item, &legs);
    assert!(
        sql.contains(r#"SUM(CASE WHEN ("LHS_T2"."L_RETURNFLAG" = 'R') THEN 1 ELSE 0 END)"#),
        "the nested SUM(CASE ...) must render with its column table-qualified: {sql}"
    );
    assert!(
        sql.contains("COUNT(*)"),
        "the nested COUNT(*) must render as the star case: {sql}"
    );
}

#[test]
fn render_expression_qualified_top_level_aggregate_byte_compatible() {
    let legs = seam_legs();

    let sum = agg_item_expr("SUM", table_col("ORDERS", "O_TOTALPRICE"), false);
    assert_eq!(
        render_expression_qualified(&sum, &legs),
        r#"SUM("LHS_T1"."O_TOTALPRICE")"#
    );

    let count_star = count_star();
    assert_eq!(render_expression_qualified(&count_star, &legs), "COUNT(*)");

    let count_distinct = agg_item_expr("COUNT", table_col("CUSTOMER", "C_CUSTKEY"), true);
    assert_eq!(
        render_expression_qualified(&count_distinct, &legs),
        r#"COUNT(DISTINCT "LHS_T0"."C_CUSTKEY")"#
    );
}

#[test]
fn qualified_count_distinct_cast_char_renders_exasol_char_target() {
    let legs = seam_legs();
    let item = agg_item_expr(
        "COUNT",
        cast_to(
            ExasolType::Char {
                size: 20,
                ascii: true,
            },
            table_col("CUSTOMER", "C_VARCHAR"),
        ),
        true,
    );
    let sql = render_expression_qualified(&item, &legs);
    assert!(
        sql.contains("CHAR(20) ASCII"),
        "Exasol-parsed qualified wrapper needs the declared length-qualified \
         CHAR CAST target: {sql}"
    );
    assert!(
        !sql.contains("AS VARCHAR)"),
        "must NOT emit a bare length-less VARCHAR (Exasol rejects it): {sql}"
    );
    assert!(
        !sql.contains("VARCHAR(20)"),
        "must NOT collapse the declared CHAR target to VARCHAR(20) (#192): {sql}"
    );
    assert!(
        sql.contains(r#"COUNT(DISTINCT CAST("LHS_T0"."C_VARCHAR" AS CHAR(20) ASCII))"#),
        "full qualified COUNT(DISTINCT CAST(...)) shape must match: {sql}"
    );
}

#[test]
fn n_scan_join_select_list_renders_exasol_char_target() {
    let mut request = join_request(Json::Null, equi_condition());
    request = selecting(
        request,
        &[
            (
                cast_to(
                    ExasolType::Char {
                        size: 20,
                        ascii: true,
                    },
                    table_col("CUSTOMER", "C_NAME"),
                ),
                ExasolType::Char {
                    size: 20,
                    ascii: true,
                },
            ),
            (table_col("ORDERS", "O_ORDERDATE"), ExasolType::Date),
        ],
    );
    request["pushdownRequest"]["having"] = compare("predicate_greater", count_star(), num_lit(1));

    assert!(matches!(
        classify_join_window(&pd(&request)),
        JoinWindowPlan::ExasolPostProcessed
    ));

    let sql =
        n_scan_sql(&request).expect("a CAST-to-CHAR select item must build the unified wrapper");

    assert!(
        sql.contains(r#"CAST("LHS_T0"."C_NAME" AS CHAR(20) ASCII)"#),
        "the N-scan wrapper's SELECT list must declare the CHAR target: {sql}"
    );
    assert!(
        !sql.contains("VARCHAR(20)") && !sql.contains("AS VARCHAR)"),
        "must neither collapse to VARCHAR(20) nor emit a bare VARCHAR: {sql}"
    );
}

#[test]
fn order_by_over_join_renders_qualified_in_unified_wrapper() {
    let mut request = join_request(Json::Null, equi_condition());
    request = selecting(
        request,
        &[(table_col("CUSTOMER", "C_NAME"), ExasolType::varchar(100))],
    );
    request["pushdownRequest"]["orderBy"] = serde_json::json!([order_by_element(
        table_col("ORDERS", "O_ORDERDATE"),
        true,
        false
    ),]);

    assert!(matches!(
        classify_join_window(&pd(&request)),
        JoinWindowPlan::Ordered { .. }
    ));

    let sql = n_scan_sql(&request).expect("ordered unified wrapper must build");
    assert!(
        sql.contains(r#"ORDER BY "LHS_T1"."O_ORDERDATE" ASC NULLS FIRST"#),
        "ORDER BY must be table-qualified with explicit direction/nulls: {sql}"
    );
}

#[test]
fn order_by_expression_renders_qualified_in_unified_wrapper() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["orderBy"] = serde_json::json!([order_by_element(
        scalar_fn("UPPER", [table_col("ORDERS", "O_ORDERDATE")]),
        false,
        true
    ),]);

    assert!(matches!(
        classify_join_window(&pd(&request)),
        JoinWindowPlan::ExasolPostProcessed
    ));

    let sql = n_scan_sql(&request)
        .expect("ordered unified wrapper must build for a renderable expression sort key");
    assert!(
        sql.contains(r#"ORDER BY UPPER("LHS_T1"."O_ORDERDATE") DESC NULLS LAST"#),
        "the expression ORDER BY key must be table-qualified with explicit \
         direction/nulls, not declined: {sql}"
    );
}

#[test]
fn join_window_classification_covers_every_forcing_and_served_shape() {
    let plain = join_request(Json::Null, equi_condition());
    assert!(matches!(
        classify_join_window(&pd(&plain)),
        JoinWindowPlan::Unbounded
    ));

    let mut limited = join_request(Json::Null, equi_condition());
    limited["pushdownRequest"]["limit"] = serde_json::json!({"numElements": 10});
    assert!(matches!(
        classify_join_window(&pd(&limited)),
        JoinWindowPlan::BareLimit(10)
    ));

    let mut offset_without_order = join_request(Json::Null, equi_condition());
    offset_without_order["pushdownRequest"]["limit"] =
        serde_json::json!({"numElements": 10, "offset": 5});
    assert!(matches!(
        classify_join_window(&pd(&offset_without_order)),
        JoinWindowPlan::ExasolPostProcessed
    ));

    let mut ordered = join_request(Json::Null, equi_condition());
    ordered["pushdownRequest"]["orderBy"] = serde_json::json!([order_by_element(
        table_col("ORDERS", "O_ORDERDATE"),
        true,
        false
    ),]);
    assert!(matches!(
        classify_join_window(&pd(&ordered)),
        JoinWindowPlan::Ordered { .. }
    ));

    let mut ordered_by_expression = join_request(Json::Null, equi_condition());
    ordered_by_expression["pushdownRequest"]["orderBy"] = serde_json::json!([order_by_element(
        scalar_fn("UPPER", [table_col("ORDERS", "O_ORDERDATE")]),
        false,
        true
    ),]);
    assert!(matches!(
        classify_join_window(&pd(&ordered_by_expression)),
        JoinWindowPlan::ExasolPostProcessed
    ));

    let mut aggregate_item = join_request(Json::Null, equi_condition());
    aggregate_item = selecting(
        aggregate_item,
        &[(count_star(), ExasolType::decimal(18, 0))],
    );
    assert!(matches!(
        classify_join_window(&pd(&aggregate_item)),
        JoinWindowPlan::ExasolPostProcessed
    ));

    let mut grouped = join_request(Json::Null, equi_condition());
    grouped["pushdownRequest"]["groupBy"] = serde_json::json!([table_col("CUSTOMER", "C_NAME")]);
    assert!(matches!(
        classify_join_window(&pd(&grouped)),
        JoinWindowPlan::ExasolPostProcessed
    ));

    let mut group_by_aggregation = join_request(Json::Null, equi_condition());
    group_by_aggregation["pushdownRequest"]["aggregationType"] = serde_json::json!("group_by");
    assert!(matches!(
        classify_join_window(&pd(&group_by_aggregation)),
        JoinWindowPlan::ExasolPostProcessed
    ));

    let mut having = join_request(Json::Null, equi_condition());
    having["pushdownRequest"]["having"] = compare("predicate_greater", count_star(), num_lit(1));
    assert!(matches!(
        classify_join_window(&pd(&having)),
        JoinWindowPlan::ExasolPostProcessed
    ));
}

#[test]
fn golden_broadcast_join_sql_unchanged() {
    let fact = resolved_side("LINEITEM", vec![("s3://w/l-0.parquet", 1000)]);
    let mut dimension = resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 10)]);
    // Deliberately distinct from the fact side's backend.
    dimension.effective_storage = StorageBackend::S3(StorageProps {
        endpoint: "http://minio-dim:9000".into(),
        region: "us-east-2".into(),
        access_key: "dimadmin".into(),
        secret_key: "dimadmin".into(),
        allow_http: true,
        ..Default::default()
    });
    let sides = broadcast_sides(fact, dimension, 1);
    let rendered = RenderedJoinPushdown {
        filter: Some(r#"("LHS_T0"."L_QUANTITY" > 5)"#.to_string()),
        ..rendered_join(
            r#"("LHS_T0"."L_ORDERKEY" = "LHS_T1"."O_ORDERKEY")"#,
            &[
                ProjectedColumn {
                    item: lineitem_column("L_ORDERKEY"),
                    exa_type: ExasolType::decimal(20, 0),
                },
                ProjectedColumn {
                    item: orders_column("O_ORDERDATE"),
                    exa_type: ExasolType::Date,
                },
            ],
        )
    };
    let actual =
        build_broadcast_join_sql(&sides, &rendered, &two_scan_tuning(), "SCAN", "DISTRIBUTE")
            .expect("selecting the wire storage must succeed")
            .expect("an unbounded broadcast join must build");
    assert_eq!(
        actual,
        r#"SELECT SCAN('{"table_root":"s3://warehouse/lh/lineitem","projection":[{"relation":"LHS_T0","name":"L_ORDERKEY"},{"relation":"LHS_T1","name":"O_ORDERDATE"}],"filter":"(\"LHS_T0\".\"L_QUANTITY\" > 5)","logical_schema":[{"field_id":1,"name":"LINEITEM_KEY","arrow_type":"int64","nullable":false}],"join":{"table_root":"s3://warehouse/lh/orders","files":[["s3://w/o-0.parquet",10]],"logical_schema":[{"field_id":1,"name":"ORDERS_KEY","arrow_type":"int64","nullable":false}],"join_type":"inner","fact_alias":"LHS_T0","dimension_alias":"LHS_T1","condition":"(\"LHS_T0\".\"L_ORDERKEY\" = \"LHS_T1\".\"O_ORDERKEY\")","storage":{"connection":{"name":"LAKEHOUSE_CATALOG_CREDS","allow_http":true}}},"storage":{"connection":{"name":"LAKEHOUSE_CATALOG_CREDS","allow_http":true}},"df_target_partitions":1,"df_batch_size":8192,"df_threads_per_udf":1,"memory_pool_fraction":0.6,"instance_overhead_mb":0,"s3_max_connections":1}', '[["s3://w/l-0.parquet",1000]]') EMITS ("LHS_T0_L_ORDERKEY" DECIMAL(20,0), "LHS_T1_O_ORDERDATE" DATE)"#
    );
}

#[test]
fn broadcast_carries_each_sides_own_storage() {
    let mut fact = resolved_side("LINEITEM", vec![("s3://w/l-0.parquet", 1000)]);
    fact.effective_storage = StorageBackend::S3(StorageProps {
        endpoint: "http://minio-fact:9000".into(),
        region: "us-east-1".into(),
        access_key: "factadmin".into(),
        secret_key: "factadmin".into(),
        allow_http: true,
        ..Default::default()
    });
    let mut dimension = resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 10)]);
    dimension.effective_storage = StorageBackend::S3(StorageProps {
        endpoint: "http://minio-dim:9000".into(),
        region: "us-east-2".into(),
        access_key: "dimadmin".into(),
        secret_key: "dimadmin".into(),
        allow_http: true,
        ..Default::default()
    });
    let sides = broadcast_sides(fact.clone(), dimension.clone(), 1);
    let rendered = rendered_join(
        r#"("LHS_T0"."L_ORDERKEY" = "LHS_T1"."O_ORDERKEY")"#,
        &[ProjectedColumn {
            item: lineitem_column("L_ORDERKEY"),
            exa_type: ExasolType::decimal(20, 0),
        }],
    );
    let sql = build_broadcast_join_sql(
        &sides,
        &rendered,
        &JoinScanRequestConfig {
            connection: &crate::adapter::pushdown::test_support::TEST_VENDED_CONNECTION,
            ..two_scan_tuning()
        },
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("selecting the wire storage must succeed")
    .expect("an unbounded broadcast join must build");
    let common: serde_json::Value =
        serde_json::from_str(common_arg_literal(&sql)).expect("common blob is valid JSON");

    let key = test_sealing_key();
    for (label, wire, expected) in [
        (
            "common.storage",
            &common["storage"],
            &fact.effective_storage,
        ),
        (
            "join.storage",
            &common["join"]["storage"],
            &dimension.effective_storage,
        ),
    ] {
        let selected: ScanStorage =
            serde_json::from_value(wire.clone()).expect("the wire value is a ScanStorage");
        let ScanStorage::Sealed { payload, .. } = &selected else {
            panic!("a vended side must be sealed, got {selected:?} for {label}");
        };
        assert_eq!(
            &crate::scan::sealed::unseal_storage(payload, &key)
                .expect("the envelope must open under the fixture key"),
            expected,
            "{label} must seal that side's OWN backend"
        );
    }
    assert_ne!(
        common["storage"], common["join"]["storage"],
        "the two sides must carry two distinct envelopes"
    );
}

/// LINEITEM is leg 0 and the fact, ORDERS leg 1 and the dimension, in every fixture below.
fn lineitem_column(name: &str) -> ProjectionItem {
    ProjectionItem::Column(ColumnRef::qualified("LHS_T0", name))
}

fn orders_column(name: &str) -> ProjectionItem {
    ProjectionItem::Column(ColumnRef::qualified("LHS_T1", name))
}

/// A single fact file means a single shard, so any ` FROM (` in the result is the
/// ordering wrapper's.
fn broadcast_sql_projecting(
    projection: &[ProjectedColumn],
    window: JoinWindowPlan,
) -> Option<String> {
    let sides = broadcast_sides(
        resolved_side("LINEITEM", vec![("s3://w/l-0.parquet", 1000)]),
        resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 10)]),
        1,
    );
    let rendered = RenderedJoinPushdown {
        window,
        ..rendered_join(
            r#"("LHS_T0"."L_ORDERKEY" = "LHS_T1"."O_ORDERKEY")"#,
            projection,
        )
    };
    build_broadcast_join_sql(&sides, &rendered, &two_scan_tuning(), "SCAN", "DISTRIBUTE")
        .expect("selecting the wire storage must succeed")
}

fn broadcast_window_sql(window: JoinWindowPlan) -> Option<String> {
    broadcast_sql_projecting(
        &[ProjectedColumn {
            item: lineitem_column("L_ORDERKEY"),
            exa_type: ExasolType::decimal(20, 0),
        }],
        window,
    )
}

fn broadcast_common_blob(sql: &str) -> Json {
    serde_json::from_str(common_arg_literal(sql)).expect("common blob is valid JSON")
}

fn ascending_key(relation: &str, column: &str) -> ParsedSortKey {
    ParsedSortKey::Column(SortKey {
        column: ColumnRef::qualified(relation, column),
        ascending: true,
        nulls_last: true,
    })
}

fn assert_shards_carry_no_window(sql: &str) {
    let common = broadcast_common_blob(sql);
    assert!(
        common.get("limit").is_none(),
        "a non-zero-offset or unlimited ordered shard must stay uncapped: {common}"
    );
    assert!(
        common.get("order_by").is_none(),
        "a non-zero-offset or unlimited ordered shard must stay unsorted: {common}"
    );
    assert!(
        common["join"].get("post_join_limit").is_none(),
        "a non-zero-offset or unlimited ordered shard must carry no post-join cap: {common}"
    );
    assert!(
        common["join"].get("post_join_order_by").is_none(),
        "a non-zero-offset or unlimited ordered shard must carry no post-join ordering: {common}"
    );
}

#[test]
fn broadcast_bare_limit_caps_each_shard_and_the_merge() {
    let unbounded =
        broadcast_window_sql(JoinWindowPlan::Unbounded).expect("an unbounded join must build");
    let capped =
        broadcast_window_sql(JoinWindowPlan::BareLimit(7)).expect("a bare-limit join must build");

    let common = broadcast_common_blob(&capped);
    assert_eq!(common["join"]["post_join_limit"], serde_json::json!(7));
    assert!(
        common.get("limit").is_none(),
        "a post-join cap must never become a scan-side limit: {common}"
    );

    assert!(
        !unbounded.contains(" FROM ("),
        "an unordered broadcast plan wraps nothing: {unbounded}"
    );
    assert!(
        !capped.contains(" FROM ("),
        "a bare-limit broadcast plan wraps nothing: {capped}"
    );

    let condition = r#""condition":"(\"LHS_T0\".\"L_ORDERKEY\" = \"LHS_T1\".\"O_ORDERKEY\")""#;
    assert_eq!(
        capped,
        format!(
            "{} LIMIT 7",
            unbounded.replace(condition, &format!("{condition},\"post_join_limit\":7"))
        ),
        "a bare cap must change the plan by exactly the join-block key and the merge LIMIT"
    );
}

#[test]
fn broadcast_ordered_zero_offset_limit_bounds_each_shard_to_its_top_n() {
    let sql = broadcast_window_sql(JoinWindowPlan::Ordered {
        keys: vec![ascending_key("LHS_T0", "L_ORDERKEY")],
        limit: Some(5),
        offset: 0,
    })
    .expect("a projected bare-column ordering stays broadcast-eligible");

    let common = broadcast_common_blob(&sql);
    assert_eq!(
        common["join"]["post_join_order_by"],
        serde_json::json!([{
            "column": {"relation": "LHS_T0", "name": "L_ORDERKEY"},
            "ascending": true,
            "nulls_last": true,
        }]),
        "the join block must carry the sort key with its flags: {common}"
    );
    assert_eq!(
        common["join"]["post_join_limit"],
        serde_json::json!(5),
        "the join block must carry the per-shard cap: {common}"
    );
    assert!(
        common.get("limit").is_none(),
        "a per-shard cap must never become a scan-side limit: {common}"
    );
    assert!(
        common.get("order_by").is_none(),
        "a per-shard ordering must never become a scan-side order_by: {common}"
    );

    assert!(
        sql.ends_with(r#") ORDER BY "LHS_T0_L_ORDERKEY" ASC NULLS LAST LIMIT 5"#),
        "the wrapper must still carry the global ordering and window: {sql}"
    );
    assert_eq!(
        sql.matches(" LIMIT ").count(),
        1,
        "the merge carries no LIMIT of its own, only the wrapper's: {sql}"
    );
}

#[test]
fn broadcast_ordered_bounds_shards_with_keys_from_either_side() {
    let sql = broadcast_sql_projecting(
        &[
            ProjectedColumn {
                item: lineitem_column("L_ORDERKEY"),
                exa_type: ExasolType::decimal(20, 0),
            },
            ProjectedColumn {
                item: orders_column("O_ORDERDATE"),
                exa_type: ExasolType::Date,
            },
        ],
        JoinWindowPlan::Ordered {
            keys: vec![
                ParsedSortKey::Column(SortKey {
                    column: ColumnRef::qualified("LHS_T1", "O_ORDERDATE"),
                    ascending: false,
                    nulls_last: false,
                }),
                ascending_key("LHS_T0", "L_ORDERKEY"),
            ],
            limit: Some(5),
            offset: 0,
        },
    )
    .expect("both keys are members of the broadcast projection");

    let common = broadcast_common_blob(&sql);
    assert_eq!(
        common["join"]["post_join_order_by"],
        serde_json::json!([
            {"column": {"relation": "LHS_T1", "name": "O_ORDERDATE"}, "ascending": false, "nulls_last": false},
            {"column": {"relation": "LHS_T0", "name": "L_ORDERKEY"}, "ascending": true, "nulls_last": true},
        ]),
        "the join block must carry both keys, in pushed order, each with its own flags: {common}"
    );
}

#[test]
fn broadcast_zero_offset_request_takes_the_bounded_path() {
    let order_by = serde_json::json!([order_by_element(
        table_col("ORDERS", "O_ORDERDATE"),
        true,
        false
    ),]);

    let mut zero_offset = join_request(Json::Null, equi_condition());
    zero_offset["pushdownRequest"]["orderBy"] = order_by.clone();
    zero_offset["pushdownRequest"]["limit"] = serde_json::json!({"numElements": 5, "offset": 0});

    let mut no_offset_key = join_request(Json::Null, equi_condition());
    no_offset_key["pushdownRequest"]["orderBy"] = order_by;
    no_offset_key["pushdownRequest"]["limit"] = serde_json::json!({"numElements": 5});

    let build = |request: &Json| {
        let rendered = render_broadcast_join(
            &parsed_pushdown(request),
            &detected_join(request),
            &request_sides(request, &detected_join(request)),
        )
        .expect("renders")
        .expect("an ordered equi-join renders");
        broadcast_sql_projecting(
            &[ProjectedColumn {
                item: orders_column("O_ORDERDATE"),
                exa_type: ExasolType::Date,
            }],
            rendered.window,
        )
        .expect("the ordered key is a member of the broadcast projection")
    };

    let sql_zero_offset = build(&zero_offset);
    let sql_no_offset_key = build(&no_offset_key);

    assert_eq!(
        sql_zero_offset, sql_no_offset_key,
        "a present zero offset must classify and render identically to an absent offset key"
    );
    assert!(
        broadcast_common_blob(&sql_zero_offset)["join"]
            .get("post_join_order_by")
            .is_some(),
        "the bounded path must carry a post-join ordering: {sql_zero_offset}"
    );
}

#[test]
fn broadcast_ordered_without_limit_wraps_fan_out_with_no_window() {
    let sql = broadcast_window_sql(JoinWindowPlan::Ordered {
        keys: vec![ascending_key("LHS_T0", "L_ORDERKEY")],
        limit: None,
        offset: 0,
    })
    .expect("a projected bare-column ordering stays broadcast-eligible");

    assert!(
        sql.ends_with(r#") ORDER BY "LHS_T0_L_ORDERKEY" ASC NULLS LAST"#),
        "an ORDER BY without a window must end at the ordering: {sql}"
    );
    assert_shards_carry_no_window(&sql);
}

#[test]
fn broadcast_ordered_renders_limit_and_offset_on_the_wrapper_only() {
    let sql = broadcast_window_sql(JoinWindowPlan::Ordered {
        keys: vec![ascending_key("LHS_T0", "L_ORDERKEY")],
        limit: Some(5),
        offset: 3,
    })
    .expect("a projected bare-column ordering stays broadcast-eligible");

    assert!(
        sql.ends_with(r#") ORDER BY "LHS_T0_L_ORDERKEY" ASC NULLS LAST LIMIT 5 OFFSET 3"#),
        "the window must render on the wrapper, after the ordering: {sql}"
    );
    assert_shards_carry_no_window(&sql);
}

#[test]
fn broadcast_ordered_unprojected_key_downgrades_to_the_fallback() {
    assert!(
        broadcast_window_sql(JoinWindowPlan::Ordered {
            keys: vec![ascending_key("LHS_T1", "O_ORDERDATE")],
            limit: Some(5),
            offset: 0,
        })
        .is_none(),
        "an ORDER BY key outside the projection must fall through to the N-scan wrapper"
    );
}

#[test]
fn fallback_leg_fan_out_spec_never_carries_a_limit_or_sort() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["limit"] = serde_json::json!({"numElements": 10, "offset": 4});
    request["pushdownRequest"]["orderBy"] = serde_json::json!([order_by_element(
        table_col("CUSTOMER", "C_NAME"),
        true,
        false
    )]);
    let sql = n_scan_sql(&request).expect("the two-table unified fallback must build");

    let legs: Vec<Json> = sql
        .split('\'')
        .skip(1)
        .step_by(2)
        .step_by(2)
        .map(|blob| serde_json::from_str(blob).expect("each leg's common blob is valid JSON"))
        .collect();
    assert_eq!(legs.len(), 2, "one common blob per leg: {sql}");
    for leg in &legs {
        assert!(leg.get("limit").is_none(), "a leg is never capped: {leg}");
        assert!(
            leg.get("order_by").is_none(),
            "a leg is never sorted: {leg}"
        );
        assert!(
            leg.get("join").is_none(),
            "a leg carries no join block, so no post-join cap can exist on it: {leg}"
        );
    }
    assert!(
        sql.ends_with(" LIMIT 10 OFFSET 4"),
        "the window belongs to the outer wrapper: {sql}"
    );
}

#[test]
fn golden_n_scan_join_sql_unchanged() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["filter"] = and([
        string_equal("CUSTOMER", "C_NAME", "ACME"),
        compare(
            "predicate_greater",
            table_col("ORDERS", "O_ORDERDATE"),
            date_lit("1995-01-01"),
        ),
        compare(
            "predicate_greater",
            table_col("CUSTOMER", "C_CUSTKEY"),
            table_col("ORDERS", "O_CUSTKEY"),
        ),
    ]);
    let actual = n_scan_sql(&request).expect("the two-table unified fallback must build");
    assert_eq!(
        actual,
        r#"SELECT "LHS_T0"."C_NAME", "LHS_T1"."O_ORDERDATE" FROM (SELECT SCAN('{"table_root":"s3://warehouse/lh/customer","projection":["C_CUSTKEY","C_NAME"],"filter":"(\"C_NAME\" = ''ACME'')","logical_schema":[{"field_id":1,"name":"CUSTOMER_KEY","arrow_type":"int64","nullable":false}],"storage":{"connection":{"name":"LAKEHOUSE_CATALOG_CREDS","allow_http":true}},"df_target_partitions":1,"df_batch_size":8192,"df_threads_per_udf":1,"memory_pool_fraction":0.6,"instance_overhead_mb":0,"s3_max_connections":1}', '[["s3://w/c-0.parquet",10]]') EMITS ("C_CUSTKEY" DECIMAL(20,0), "C_NAME" VARCHAR(100))) AS "LHS_T0" INNER JOIN (SELECT SCAN('{"table_root":"s3://warehouse/lh/orders","projection":["O_CUSTKEY","O_ORDERDATE"],"filter":"(\"O_ORDERDATE\" > DATE ''1995-01-01'')","logical_schema":[{"field_id":1,"name":"ORDERS_KEY","arrow_type":"int64","nullable":false}],"storage":{"connection":{"name":"LAKEHOUSE_CATALOG_CREDS","allow_http":true}},"df_target_partitions":1,"df_batch_size":8192,"df_threads_per_udf":1,"memory_pool_fraction":0.6,"instance_overhead_mb":0,"s3_max_connections":1}', '[["s3://w/o-0.parquet",100]]') EMITS ("O_CUSTKEY" DECIMAL(20,0), "O_ORDERDATE" DATE)) AS "LHS_T1" ON (("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY")) WHERE (("LHS_T0"."C_CUSTKEY" > "LHS_T1"."O_CUSTKEY"))"#
    );
}

#[test]
fn golden_grouped_qualified_fallback_sql_unchanged() {
    let request = pushdown_request(
        "CUSTOMER",
        &[
            ("C_CUSTKEY", ExasolType::decimal(20, 0)),
            ("C_NAME", ExasolType::varchar(100)),
        ],
    )
    .group_by(vec![table_col("CUSTOMER", "C_NAME")])
    .column(table_col("CUSTOMER", "C_NAME"))
    .count_star()
    .build()
    .0;
    let pushdown_req = parsed_pushdown(&request);
    let fan_out_spec = CommonScanSpec {
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    };
    let col_types = first_table_column_types(&request);
    let fan_out = FanOutProjection::new(fan_out_spec, &col_types);
    let actual = build_qualified_single_table_fallback_sql(
        &request,
        &pushdown_req,
        &fan_out,
        &[vec![("s3://w/c-0.parquet".to_string(), 10u64)]],
        "SCAN",
        "DISTRIBUTE",
        None,
    )
    .expect("the grouped qualified fallback must build");
    assert_eq!(
        actual,
        r#"SELECT "LHS_T0"."C_NAME", CAST(COUNT(*) AS DECIMAL(18,0)) FROM (SELECT SCAN('{"projection":["C_CUSTKEY","C_NAME"],"storage":{"inline":{"s3":{"endpoint":"http://minio:9000","region":"us-east-1","access_key":"minioadmin","secret_key":"minioadmin","allow_http":true,"path_style":true}}},"df_target_partitions":1,"df_batch_size":8192,"df_threads_per_udf":1,"memory_pool_fraction":0.6,"instance_overhead_mb":200,"s3_max_connections":8}', '[["s3://w/c-0.parquet",10]]') EMITS ("C_CUSTKEY" DECIMAL(20,0), "C_NAME" VARCHAR(100))) AS "LHS_T0" GROUP BY "LHS_T0"."C_NAME""#
    );
}

#[test]
fn fallback_projection_narrows_to_referenced_columns() {
    fn col(name: &str, ty: ExasolType) -> (String, ExasolType) {
        (name.to_string(), ty)
    }
    fn fan_out_base() -> CommonScanSpec {
        CommonScanSpec {
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        }
    }
    fn proj_names(proj: &[ProjectionItem]) -> Vec<String> {
        proj.iter()
            .map(|p| match p {
                ProjectionItem::Column(n) => n.output_name(),
                ProjectionItem::Expr { .. } => panic!("narrowing must yield columns only"),
            })
            .collect()
    }

    let shards = [vec![("s3://wh/f0.parquet".to_string(), 1u64)]];

    let all_cols = vec![
        col("GK", ExasolType::varchar(10)),
        col("REGION", ExasolType::varchar(10)),
        col("HCOL", ExasolType::decimal(18, 0)),
        col("OCOL", ExasolType::decimal(18, 0)),
        col("FCOL", ExasolType::decimal(18, 0)),
        col("IRRELEVANT_COL", ExasolType::varchar(10)),
    ];
    let having = compare(
        "predicate_greater",
        agg_item_expr("SUM", table_col("T", "HCOL"), false),
        num_lit(10),
    );
    let order_by = serde_json::json!([order_by_element(table_col("T", "OCOL"), true, false)]);
    let region_flag = scalar_fn(
        "CASE",
        [string_equal("T", "REGION", "R"), num_lit(1), num_lit(0)],
    );
    let sum_case = agg_item_expr("SUM", region_flag, false);
    let rich_req = pushdown_request("T", &all_cols)
        .group_by(vec![table_col("T", "GK")])
        .field("having", having)
        .order_by(order_by)
        .filter(predicate_equal(table_col("T", "FCOL"), num_lit(5)))
        .column(table_col("T", "GK"))
        .item(sum_case.clone(), ExasolType::WIDEST_VARCHAR)
        .build_pushdown();
    let (proj, types) = split_projection(
        &referenced_columns(rich_req.json(), &column_types(&all_cols)).projection(),
    );
    let names = proj_names(&proj);
    for expected in ["GK", "REGION", "HCOL", "OCOL", "FCOL"] {
        assert!(
            names.contains(&expected.to_string()),
            "#160: {expected} is referenced (select/aggregate-arg/CASE/HAVING/ORDER BY/\
             filter) and MUST be surfaced: {names:?}"
        );
    }
    assert!(
        !names.contains(&"IRRELEVANT_COL".to_string()),
        "#160: an unreferenced base-table column must be narrowed out, never the \
         full schema: {names:?}"
    );
    assert_eq!(
        names.len(),
        5,
        "narrowed to EXACTLY the 5 referenced columns, not the full 6-column \
         base-table schema: {names:?}"
    );
    assert_eq!(
        types.len(),
        5,
        "types stay positionally aligned with columns"
    );

    let grouped_all = vec![
        col("GK", ExasolType::varchar(10)),
        col("REGION", ExasolType::varchar(10)),
        col("IRRELEVANT_COL", ExasolType::varchar(10)),
    ];
    let grouped_request = pushdown_request("T", &grouped_all)
        .group_by(vec![table_col("T", "GK")])
        .column(table_col("T", "GK"))
        .item(sum_case, ExasolType::WIDEST_VARCHAR)
        .build()
        .0;
    let grouped_req = parsed_pushdown(&grouped_request);
    let grouped = referenced_columns(grouped_req.json(), &column_types(&grouped_all));
    let (gproj, _) = split_projection(&grouped.projection());
    assert_eq!(
        proj_names(&gproj),
        vec!["GK".to_string(), "REGION".to_string()],
        "the grouped inner scan narrows to GK + REGION (nested in SUM(CASE ...)), \
         never IRRELEVANT_COL"
    );
    let gfan_out = FanOutProjection::new(fan_out_base(), &grouped);
    let gsql = build_qualified_single_table_fallback_sql(
        &grouped_request,
        &grouped_req,
        &gfan_out,
        &shards,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        None,
    )
    .expect("grouped decline wrapper must build");
    assert!(
        !gsql.contains("IRRELEVANT_COL"),
        "#160: the grouped wrapper's inner scan must NOT emit the unreferenced \
         column: {gsql}"
    );
    assert!(
        gsql.contains("REGION") && gsql.contains(" GROUP BY ") && gsql.contains(r#"AS "LHS_T0""#),
        "the grouped wrapper renders the aggregate over the narrowed aliased scan: {gsql}"
    );

    let sg_all = vec![
        col("A", ExasolType::varchar(10)),
        col("B", ExasolType::varchar(10)),
        col("IRRELEVANT_COL", ExasolType::varchar(10)),
    ];
    let sg_request = pushdown_request("T", &sg_all)
        .item(
            agg_item_expr("COUNT", table_col("T", "A"), true),
            ExasolType::WIDEST_VARCHAR,
        )
        .item(
            agg_item_expr("COUNT", table_col("T", "B"), true),
            ExasolType::WIDEST_VARCHAR,
        )
        .build()
        .0;
    let sg_req = parsed_pushdown(&sg_request);
    let single_group = referenced_columns(sg_req.json(), &column_types(&sg_all));
    let (sproj, _) = split_projection(&single_group.projection());
    assert_eq!(
        proj_names(&sproj),
        vec!["A".to_string(), "B".to_string()],
        "the single-group Case 2/3 inner scan narrows to A + B, never IRRELEVANT_COL"
    );
    let sfan_out = FanOutProjection::new(fan_out_base(), &single_group);
    let ssql = build_qualified_single_table_fallback_sql(
        &sg_request,
        &sg_req,
        &sfan_out,
        &shards,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        None,
    )
    .expect("single-group Case 2/3 wrapper must build");
    assert!(
        !ssql.contains("IRRELEVANT_COL"),
        "#160: the single-group Case 2/3 wrapper's inner scan must NOT emit the \
         unreferenced column: {ssql}"
    );
    assert_eq!(
        ssql.matches("COUNT(DISTINCT").count(),
        2,
        "both COUNT(DISTINCT) aggregates spliced verbatim into the wrapper: {ssql}"
    );
    assert!(
        !ssql.contains(" GROUP BY ") && ssql.contains(r#"AS "LHS_T0""#),
        "the single-group wrapper renders NO GROUP BY over the aliased scan: {ssql}"
    );
}

#[test]
fn no_select_list_wire_forms_all_keep_the_full_base_row() {
    let all_cols = vec![
        ("GK".to_string(), ExasolType::varchar(10)),
        ("FCOL".to_string(), ExasolType::decimal(18, 0)),
        ("IRRELEVANT_COL".to_string(), ExasolType::varchar(10)),
    ];
    let filter = predicate_equal(table_col("T", "FCOL"), num_lit(5));
    let full_row: Vec<ProjectionItem> = all_cols
        .iter()
        .map(|(name, _)| ProjectionItem::from(name.clone()))
        .collect();
    let full_types: Vec<ExasolType> = all_cols.iter().map(|(_, ty)| *ty).collect();

    for req in [
        serde_json::json!({"filter": filter.clone()}),
        serde_json::json!({"selectList": [], "filter": filter.clone()}),
        serde_json::json!({"selectList": null, "filter": filter.clone()}),
        serde_json::json!({"selectList": "*", "filter": filter.clone()}),
    ] {
        let (proj, types) =
            split_projection(&referenced_columns(&req, &column_types(&all_cols)).projection());
        assert_eq!(
            proj, full_row,
            "no select list ⇒ `SELECT *` ⇒ the full base row, so the wrapper's own \
             no-select-list SELECT arm and this projection agree on the arity \
             Exasol validates: {req}"
        );
        assert_eq!(
            types, full_types,
            "types stay positionally aligned with the full base row: {req}"
        );
    }
}

#[test]
fn referenced_columns_narrows_with_a_real_select_list() {
    let all_cols = [
        ("GK", ExasolType::varchar(10)),
        ("FCOL", ExasolType::decimal(18, 0)),
        ("IRRELEVANT_COL", ExasolType::varchar(10)),
    ];
    let req = pushdown_request("T", &all_cols)
        .filter(predicate_equal(table_col("T", "FCOL"), num_lit(5)))
        .column(table_col("T", "GK"))
        .build_pushdown();

    let (proj, types) =
        split_projection(&referenced_columns(req.json(), &column_types(&all_cols)).projection());
    assert_eq!(
        proj,
        vec![ProjectionItem::from("GK"), ProjectionItem::from("FCOL"),],
        "select-list ∪ filter columns only — the unreferenced column stays out"
    );
    assert_eq!(
        types,
        vec![ExasolType::varchar(10), ExasolType::decimal(18, 0)]
    );
}

#[test]
fn referenced_columns_falls_back_to_first_column() {
    let all_cols = vec![
        ("A".to_string(), ExasolType::decimal(18, 0)),
        ("B".to_string(), ExasolType::varchar(10)),
    ];
    let req = pushdown_request("T", NO_COLUMNS)
        .filter(serde_json::json!(null))
        .field("having", serde_json::json!(null))
        .item(num_lit(1), ExasolType::WIDEST_VARCHAR)
        .build_pushdown();
    let (proj, types) =
        split_projection(&referenced_columns(req.json(), &column_types(&all_cols)).projection());
    assert_eq!(
        proj,
        vec![ProjectionItem::from("A")],
        "no referenced source column ⇒ exactly one column, the first of all_cols"
    );
    assert_eq!(types, vec![ExasolType::decimal(18, 0)]);
}

/// The qualified single-table wrapper over one shard of `request`'s first table.
fn qualified_fallback_for(request: PushdownRequestBuilder) -> Result<Json, UdfError> {
    let (request, _) = request.build();
    let base = CommonScanSpec {
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    };
    qualified_single_table_fallback_pushdown(
        &request,
        &parsed_pushdown(&request),
        &base,
        None,
        &[vec![FileEntry::new("s3://w/f-0.parquet", 10)]],
        &first_table_column_types(&request),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        None,
    )
}

fn table_t_with_id() -> PushdownRequestBuilder {
    pushdown_request("T", &[("ID", ExasolType::decimal(20, 0))])
}

#[test]
fn qualified_single_table_wrapper_projects_tstz_literal_converted_to_session_zone() {
    let request = table_t_with_id().item(
        lit("literal_timestamputc", "2024-03-01 09:00:00"),
        ExasolType::TimestampWithLocalTimeZone { precision: 3 },
    );

    let resp = qualified_fallback_for(request)
        .expect("a TSTZ-declared literal must render via the qualified wrapper");
    let sql = resp["sql"].as_str().expect("sql field");
    assert!(
        sql.contains(
            "CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 09:00:00', 'UTC', SESSIONTIMEZONE) \
             AS TIMESTAMP WITH LOCAL TIME ZONE)"
        ),
        "the projected TSTZ literal must be converted into the caller's session \
         zone and re-declared TSTZ: {sql}"
    );
}

/// `T (ID, C_TS)`, the table the declined-filter wrapper tests read.
fn declined_filter_table() -> PushdownRequestBuilder {
    pushdown_request(
        "T",
        &[
            ("ID", ExasolType::decimal(20, 0)),
            ("C_TS", ExasolType::BARE_TIMESTAMP),
        ],
    )
}

#[test]
fn single_table_wrapper_renders_declined_predicate_in_exasol_dialect() {
    let request = declined_filter_table()
        .filter(declined_second_predicate(table_col("T", "C_TS")))
        .order_by(serde_json::json!([order_by_element(
            table_col("T", "ID"),
            true,
            true
        )]))
        .limit(5)
        .column(table_col("T", "ID"))
        .build()
        .0;

    let sql = declined_filter_wrapper_sql(&request);

    let where_at = sql
        .find(r#"AS "LHS_T0" WHERE "#)
        .unwrap_or_else(|| panic!("the wrapper must self-apply the predicate: {sql}"));
    assert!(
        sql[where_at..].contains("SECOND(") && sql[where_at..].contains(r#""LHS_T0"."C_TS""#),
        "the WHERE must carry the declined predicate, table-qualified against the \
         wrapper alias: {sql}"
    );
    let order_at = sql
        .find(" ORDER BY ")
        .unwrap_or_else(|| panic!("the wrapper must render the pushed ORDER BY: {sql}"));
    let limit_at = sql
        .find(" LIMIT ")
        .unwrap_or_else(|| panic!("the wrapper must render the pushed LIMIT: {sql}"));
    assert!(
        where_at < order_at && where_at < limit_at,
        "the WHERE must precede the ORDER BY and the LIMIT so it filters before \
         sorting and truncating: {sql}"
    );
    assert!(
        !sql.contains(r#""filter""#),
        "the fan-out scan spec must carry no filter — the declined predicate is \
         applied exactly once, in the wrapper: {sql}"
    );
}

#[test]
fn declined_filter_self_apply_renders_tstz_literal_via_convert_tz() {
    let declined = compare(
        "predicate_greater",
        table_col("T", "C_TS"),
        lit("literal_timestamputc", "2024-03-01 09:00:00"),
    );
    let request = declined_filter_table()
        .filter(declined)
        .column(table_col("T", "ID"))
        .build()
        .0;

    let sql = declined_filter_wrapper_sql(&request);

    assert!(
        sql.contains(
            r#"WHERE ("LHS_T0"."C_TS" > CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 09:00:00', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE))"#
        ),
        "the self-applied predicate must compare against a genuine TSTZ \
         expression, so Exasol applies its own TIMESTAMP-vs-TSTZ coercion \
         rule to C_TS exactly as it would natively: {sql}"
    );
}

#[test]
fn outer_wrapper_renders_qualified_expression_sort_key() {
    let request = pushdown_request(
        "T",
        &[
            ("ID", ExasolType::decimal(20, 0)),
            ("NAME", ExasolType::varchar(100)),
        ],
    )
    .field("groupBy", serde_json::json!([table_col("T", "ID")]))
    .order_by(serde_json::json!([order_by_element(
        scalar_fn("UPPER", [table_col("T", "NAME")]),
        false,
        true
    )]))
    .limit(4)
    .column(table_col("T", "ID"))
    .build()
    .0;
    let (pushdown_req, legs) = planned(&request);
    let leg = first_table_column_types(&request);

    let clauses = outer_wrapper_clauses(&pushdown_req, &legs, &[&leg])
        .expect("a renderable expression sort key must render, not decline");

    assert!(
        clauses
            .trailing
            .contains(r#"ORDER BY UPPER("LHS_T0"."NAME") DESC NULLS LAST"#),
        "the expression sort key must be table-qualified with explicit \
         direction/nulls: {}",
        clauses.trailing
    );
    let order_at = clauses
        .trailing
        .find("ORDER BY")
        .expect("the trailing clauses must carry the ORDER BY");
    let limit_at = clauses
        .trailing
        .find("LIMIT")
        .expect("the trailing clauses must carry the LIMIT");
    assert!(
        order_at < limit_at,
        "LIMIT must follow ORDER BY, or it truncates before the sort: {}",
        clauses.trailing
    );
}

/// The wrapper's trailing clauses for `request`, whose every leg is an `EVENTS` occurrence.
fn seam_trailing(request: &Json) -> Result<String, UdfError> {
    let events = column_types(&events_columns());
    let (pushdown, legs) = planned(request);
    let leg_count = match detect_join(request, pushdown.json()).expect("a plannable request") {
        JoinShape::Join(join) => join.tables.len(),
        JoinShape::NotAJoin => 1,
    };
    outer_wrapper_clauses(&pushdown, &legs, &vec![&events; leg_count])
        .map(|clauses| clauses.trailing)
}

fn events_columns() -> [(&'static str, ExasolType); 3] {
    [
        ("ID", ExasolType::decimal(20, 0)),
        ("NAME", ExasolType::varchar(100)),
        ("SCORE", ExasolType::DoublePrecision),
    ]
}

/// `EVENTS`, the table the seam fixtures read.
fn events() -> PushdownRequestBuilder {
    pushdown_request("EVENTS", &events_columns())
}

/// [`events`] joined to itself as occurrences `A` and `B` on `ID`.
fn events_self_join() -> PushdownRequestBuilder {
    events().field(
        "from",
        inner_join(
            table_ref_as("EVENTS", Some("A")),
            table_ref_as("EVENTS", Some("B")),
            predicate_equal(
                aliased_col("EVENTS", "A", "ID"),
                aliased_col("EVENTS", "B", "ID"),
            ),
        ),
    )
}

/// `request`'s parsed pushdown and the legs production binds for it.
fn planned(request: &Json) -> (PushdownRequest, JoinLegs) {
    (parsed_pushdown(request), request_legs(request))
}

/// The four live-captured offset-carrying shapes reaching this seam (#191 capture rows
/// 8-11); each carries an `orderBy`.
fn offset_carrying_seam_fixtures() -> Vec<(&'static str, Json, &'static str)> {
    let id = table_col("EVENTS", "ID");
    let mod_key = scalar_fn("MOD", [id.clone(), num_lit(4)]);
    let ascending = |expr: &Json| order_by_element(expr.clone(), true, false);
    let count_distinct_id = agg_item_expr("COUNT", id.clone(), true);
    let count_star = count_star();
    let id_a = aliased_col("EVENTS", "A", "ID");
    let score_a = aliased_col("EVENTS", "A", "SCORE");
    let name_a = aliased_col("EVENTS", "A", "NAME");
    let shape = |name, builder: PushdownRequestBuilder, tail| (name, builder.build().0, tail);

    vec![
        shape(
            "row 8: grouped COUNT(DISTINCT) qualified wrapper",
            events()
                .group_by(vec![mod_key.clone()])
                .order_by(serde_json::json!([ascending(&mod_key)]))
                .field("limit", serde_json::json!({"numElements": 2, "offset": 1}))
                .item(mod_key.clone(), ExasolType::WIDEST_VARCHAR)
                .item(count_distinct_id, ExasolType::WIDEST_VARCHAR),
            r#" ORDER BY MOD("LHS_T0"."ID", 4) ASC NULLS FIRST LIMIT 2 OFFSET 1"#,
        ),
        shape(
            "row 9: self-join, ORDER BY ordinal over a projected column",
            events_self_join()
                .order_by(serde_json::json!([ascending(&id_a)]))
                .field("limit", serde_json::json!({"numElements": 5, "offset": 2}))
                .column(id_a.clone())
                .column(score_a),
            r#" ORDER BY "LHS_T0"."ID" ASC NULLS FIRST LIMIT 5 OFFSET 2"#,
        ),
        shape(
            "row 10: self-join, sort key outside the select list",
            events_self_join()
                .order_by(serde_json::json!([ascending(&id_a)]))
                .field("limit", serde_json::json!({"numElements": 5, "offset": 2}))
                .column(name_a),
            r#" ORDER BY "LHS_T0"."ID" ASC NULLS FIRST LIMIT 5 OFFSET 2"#,
        ),
        shape(
            "row 11: self-join + GROUP BY",
            events_self_join()
                .group_by(vec![id_a.clone()])
                .order_by(serde_json::json!([ascending(&id_a)]))
                .field("limit", serde_json::json!({"numElements": 5, "offset": 2}))
                .column(id_a.clone())
                .item(count_star, ExasolType::WIDEST_VARCHAR),
            r#" ORDER BY "LHS_T0"."ID" ASC NULLS FIRST LIMIT 5 OFFSET 2"#,
        ),
    ]
}

#[test]
fn qualified_wrapper_renders_limit_offset() {
    for (shape, request, expected_tail) in offset_carrying_seam_fixtures() {
        let trailing = seam_trailing(&request)
            .unwrap_or_else(|e| panic!("{shape} must render, not decline: {e}"));
        assert!(
            trailing.ends_with(expected_tail),
            "{shape}: trailing must end with `{expected_tail}`, got `{trailing}`"
        );
    }
}

#[test]
fn qualified_wrapper_never_renders_offset_without_order_by() {
    let mut cases: Vec<(&str, Json)> = offset_carrying_seam_fixtures()
        .into_iter()
        .map(|(shape, request, _)| (shape, request))
        .collect();
    let control = events()
        .limit(5)
        .column(table_col("EVENTS", "ID"))
        .build()
        .0;
    cases.push(("control: limit, no orderBy, no offset", control));

    for (shape, request) in cases {
        let trailing = seam_trailing(&request)
            .unwrap_or_else(|e| panic!("{shape} must render, not decline: {e}"));
        if let Some(offset_at) = trailing.find("OFFSET") {
            let order_at = trailing.find("ORDER BY").unwrap_or(usize::MAX);
            assert!(
                order_at < offset_at,
                "{shape}: an OFFSET with no preceding ORDER BY is sqlCode 42000: \
                 `{trailing}`"
            );
        }
    }
}

#[test]
fn qualified_wrapper_zero_offset_renders_byte_identical_limit() {
    let limited = |limit: Json| {
        events()
            .field("limit", limit)
            .column(table_col("EVENTS", "ID"))
            .build()
            .0
    };
    let bare = seam_trailing(&limited(serde_json::json!({"numElements": 5})))
        .expect("a plain limited request must render");
    assert_eq!(bare, " LIMIT 5");

    // Exasol normalises `OFFSET 0` away; an explicit zero must still render identically.
    let zero = seam_trailing(&limited(serde_json::json!({"numElements": 5, "offset": 0})))
        .expect("an explicit zero offset must render");
    assert_eq!(zero, bare);
}

fn events_column(name: &str) -> Json {
    aliased_col("EVENTS", "E", name)
}

fn labels_column(name: &str) -> Json {
    aliased_col("LABELS", "L", name)
}

/// `EVENTS e JOIN LABELS l ON e.ID = l.ID`: the legs share ID, SCORE, and PAYLOAD. SCORE is
/// DECIMAL on EVENTS (leg 0) and VARCHAR on LABELS (leg 1), so a bare-name lookup's first
/// match types LABELS' SCORE as DECIMAL.
fn shared_names_request(select_list: &[Json], pushdown_extra: Json) -> Json {
    let mut request = pushdown_request(
        "EVENTS",
        &[
            ("ID", ExasolType::decimal(18, 0)),
            ("SCORE", ExasolType::decimal(18, 0)),
            ("PAYLOAD", ExasolType::varchar(20)),
        ],
    )
    .table(
        "LABELS",
        &[
            ("ID", ExasolType::decimal(18, 0)),
            ("SCORE", ExasolType::varchar(10)),
            ("PAYLOAD", ExasolType::varchar(20)),
        ],
    )
    .field("type", "select")
    .field(
        "from",
        inner_join(
            serde_json::json!({"name": "EVENTS", "type": "table", "alias": "E"}),
            serde_json::json!({"name": "LABELS", "type": "table", "alias": "L"}),
            predicate_equal(events_column("ID"), labels_column("ID")),
        ),
    )
    .request_field("schemaMetadataInfo", serde_json::json!({"properties": {}}));
    for item in select_list {
        request = request.column(item.clone());
    }
    if let Json::Object(extra) = pushdown_extra {
        for (key, value) in extra {
            request = request.field(&key, value);
        }
    }
    with_identifier_notes(
        request.build().0,
        &[("EVENTS", "lh.events"), ("LABELS", "lh.labels")],
    )
}

/// EVENTS (leg 0) is the fact and LABELS (leg 1) the dimension; `None` is a decline.
fn shared_names_broadcast_sql(request: &Json) -> Option<String> {
    shared_names_broadcast_sql_over(
        request,
        resolved_side("EVENTS", vec![("s3://w/e-0.parquet", 1000)]),
        resolved_side("LABELS", vec![("s3://w/l-0.parquet", 10)]),
    )
}

/// [`shared_names_broadcast_sql`] over the given sides, each typed by the columns `request`
/// declares and by its own declared schema.
fn shared_names_broadcast_sql_over(
    request: &Json,
    events: ResolvedJoinSide,
    labels: ResolvedJoinSide,
) -> Option<String> {
    let typed = request_typed(request, &[events, labels]);
    let rendered =
        render_broadcast_join(&parsed_pushdown(request), &detected_join(request), &typed)
            .expect("a well-formed request renders without error")?;
    let sides = broadcast_sides(typed[0].clone(), typed[1].clone(), 1);
    build_broadcast_join_sql(&sides, &rendered, &two_scan_tuning(), "SCAN", "DISTRIBUTE")
        .expect("selecting the wire storage must succeed")
}

fn ordered_by(select_list: &[Json], key: Json) -> Json {
    shared_names_request(
        select_list,
        serde_json::json!({
            "orderBy": [order_by_element(key, true, true)],
            "limit": {"numElements": 3},
        }),
    )
}

#[test]
fn order_by_on_other_legs_same_named_column_declines_broadcast() {
    let other_leg = ordered_by(&[events_column("ID")], labels_column("ID"));
    assert_eq!(
        shared_names_broadcast_sql(&other_leg),
        None,
        "ORDER BY l.ID must not bind to the projected e.ID, which carries the other leg's values"
    );

    let own_leg = ordered_by(
        &[events_column("ID"), labels_column("ID")],
        labels_column("ID"),
    );
    let sql =
        shared_names_broadcast_sql(&own_leg).expect("l.ID is projected, so the key binds to it");
    assert_eq!(
        broadcast_common_blob(&sql)["join"]["post_join_order_by"],
        serde_json::json!([{
            "column": {"relation": "LHS_T1", "name": "ID"},
            "ascending": true,
            "nulls_last": true,
        }]),
        "the shard's ordering carries the key's own pair: {sql}"
    );
    assert!(
        sql.ends_with(r#") ORDER BY "LHS_T1_ID" ASC NULLS LAST LIMIT 3"#),
        "the wrapper orders by the key's own output column, not LHS_T0_ID: {sql}"
    );
}

#[test]
fn shared_column_name_with_different_types_declares_each_legs_type() {
    let request = shared_names_request(
        &[events_column("SCORE"), labels_column("SCORE")],
        Json::Null,
    );
    let sql = shared_names_broadcast_sql(&request).expect("a shared name is no reason to decline");
    assert!(
        sql.ends_with(r#"EMITS ("LHS_T0_SCORE" DECIMAL(18,0), "LHS_T1_SCORE" VARCHAR(10))"#),
        "each output column declares its own leg's type: {sql}"
    );

    let like = |column: Json, pattern: &str| {
        shared_names_request(
            &[events_column("ID"), labels_column("ID")],
            serde_json::json!({
                "filter": like(column, pattern),
            }),
        )
    };
    let varchar_leg = shared_names_broadcast_sql(&like(labels_column("SCORE"), "a%"))
        .expect("LIKE on the VARCHAR leg's SCORE resolves its own leg's type and keeps the plan");
    assert!(
        varchar_leg.contains(r#""filter":"(\"LHS_T1\".\"SCORE\" LIKE ''a%'')""#),
        "the filter reads LABELS' own SCORE: {varchar_leg}"
    );
    let integer_leg = shared_names_broadcast_sql(&like(events_column("SCORE"), "1%"))
        .expect("LIKE on the integer leg's SCORE converts it and keeps the plan");
    assert!(
        integer_leg.contains(
            r#""filter":"(CAST(CAST(\"LHS_T0\".\"SCORE\" AS DECIMAL(38,0)) AS VARCHAR) LIKE ''1%'')""#
        ),
        "the filter converts EVENTS' own integer SCORE: {integer_leg}"
    );
}

#[test]
fn binary_detection_resolves_a_shared_column_name_per_leg() {
    let mut events = resolved_side("EVENTS", vec![("s3://w/e-0.parquet", 1000)]);
    events.logical_schema = vec![nullable_field("payload", "binary")];
    let mut labels = resolved_side("LABELS", vec![("s3://w/l-0.parquet", 10)]);
    labels.logical_schema = vec![nullable_field("payload", "utf8")];
    let filter_on = |column: Json| {
        let request = shared_names_request(
            &[events_column("ID"), labels_column("ID")],
            serde_json::json!({
                "filter": predicate_equal(column, str_lit("0a1b")),
            }),
        );
        shared_names_broadcast_sql_over(&request, events.clone(), labels.clone()).expect("renders")
    };
    let binary_leg = filter_on(events_column("PAYLOAD"));
    assert!(
        binary_leg.contains(r#""filter":"(\"LHS_T0\".\"_LH_BYTES_PAYLOAD\" = X''0a1b'')""#),
        "the binary leg compares its own relation's raw bytes: {binary_leg}"
    );
    let text_leg = filter_on(labels_column("PAYLOAD"));
    assert!(
        text_leg.contains(r#""filter":"(\"LHS_T1\".\"PAYLOAD\" = ''0a1b'')""#),
        "the VARCHAR leg's same-named column stays a text comparison: {text_leg}"
    );
}

#[test]
fn output_name_over_identifier_limit_declines_broadcast() {
    let projecting = |name: &str| {
        let mut request = join_request(Json::Null, equi_condition());
        request["involvedTables"][0]["columns"]
            .as_array_mut()
            .unwrap()
            .push(involved_column_json(name, ExasolType::varchar(10)));
        request = selecting(
            request,
            &[(table_col("CUSTOMER", name), ExasolType::varchar(10))],
        );
        render_broadcast_join(
            &parsed_pushdown(&request),
            &detected_join(&request),
            &request_sides(&request, &detected_join(&request)),
        )
        .expect("an identifier-limit decline is not an error")
    };
    let at_limit = "C".repeat(121);
    let rendered = projecting(&at_limit).expect("LHS_T0_ plus 121 characters is exactly 128");
    assert_eq!(
        split_projection(&rendered.projection).0,
        vec![ProjectionItem::Column(ColumnRef::qualified(
            "LHS_T0", at_limit
        ))]
    );
    assert!(
        projecting(&"Ä".repeat(121)).is_some(),
        "the limit counts characters, not the 2-byte encoding of each"
    );
    assert!(
        projecting(&"C".repeat(122)).is_none(),
        "a 129-character output name exceeds Exasol's identifier limit"
    );
}

#[test]
fn self_join_renders_each_occurrence_as_its_own_relation() {
    for aliases in [[Some("A"), Some("B")], [Some("A"), None]] {
        let request = self_join_request(&aliases);
        let rendered = render_broadcast_join(
            &parsed_pushdown(&request),
            &detected_join(&request),
            &request_sides(&request, &detected_join(&request)),
        )
        .expect("renders")
        .expect("a self-join is no reason to decline");
        assert_eq!(
            rendered.condition, r#"("LHS_T0"."O_ORDERKEY" = "LHS_T1"."O_ORDERKEY")"#,
            "{aliases:?}: the condition compares the two occurrences, never a column with itself"
        );
        assert_eq!(
            split_projection(&rendered.projection).0,
            vec![ProjectionItem::Column(ColumnRef::qualified(
                "LHS_T0",
                "O_ORDERKEY"
            ))]
        );
    }
}

#[test]
fn join_block_names_the_dimension_legs_alias() {
    let request = join_request(Json::Null, equi_condition());
    let rendered = render_broadcast_join(
        &parsed_pushdown(&request),
        &detected_join(&request),
        &request_sides(&request, &detected_join(&request)),
    )
    .expect("renders")
    .expect("an equi-join renders");
    let aliases_with = |customer_bytes: u64, orders_bytes: u64| {
        let sides = select_broadcast_sides(
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", customer_bytes)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", orders_bytes)]),
            u64::MAX,
        );
        let sql = build_broadcast_join_sql(&sides, &rendered, &two_scan_tuning(), "S", "D")
            .expect("selecting the wire storage must succeed")
            .expect("an unbounded join builds");
        let join = broadcast_common_blob(&sql)["join"].clone();
        (join["fact_alias"].clone(), join["dimension_alias"].clone())
    };
    assert_eq!(
        aliases_with(100, 10),
        (serde_json::json!("LHS_T0"), serde_json::json!("LHS_T1")),
        "the smaller second leg is the dimension"
    );
    assert_eq!(
        aliases_with(10, 100),
        (serde_json::json!("LHS_T1"), serde_json::json!("LHS_T0")),
        "the smaller first leg is the dimension"
    );
}
