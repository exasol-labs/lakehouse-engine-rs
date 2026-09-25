use super::super::super::support::{DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME};
use super::super::ineligible_join_decline;
use super::super::planning::{
    IneligibleJoinReason, JoinLeaf, JoinShape, classify_join_window, detect_join,
};
use super::super::tests::{
    detected_join, equi_condition, join_request, legs_from_leaves, nq3_join_request, resolved_side,
    self_join_request, three_table_join_request, two_scan_tuning,
};
use super::*;
use crate::adapter::pushdown::test_support::*;
use crate::scan::spec::{SortKey, StorageBackend, StorageProps};
use vs_expression::{render_expression_exasol_safe, render_expression_safe};

/// Q1 shape: `(SUPPLIER ⋈ NATION) ⋈ REGION`.
fn q1_join_request() -> Json {
    serde_json::json!({
        "involvedTables": [
            {"name": "SUPPLIER", "columns": [
                {"name": "S_SUPPKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "S_NATIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "S_NAME", "dataType": {"type": "varchar", "size": 100}}]},
            {"name": "NATION", "columns": [
                {"name": "N_NATIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "N_REGIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]},
            {"name": "REGION", "columns": [
                {"name": "R_REGIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "R_NAME", "dataType": {"type": "varchar", "size": 100}}]},
        ],
        "pushdownRequest": {
            "type": "select",
            "from": {"type": "join", "join_type": "inner",
                "left": {"type": "join", "join_type": "inner",
                    "left": {"name": "SUPPLIER", "type": "table"},
                    "right": {"name": "NATION", "type": "table"},
                    "condition": {"type": "predicate_equal",
                        "left": {"type": "column", "name": "S_NATIONKEY", "tableName": "SUPPLIER"},
                        "right": {"type": "column", "name": "N_NATIONKEY", "tableName": "NATION"}}},
                "right": {"name": "REGION", "type": "table"},
                "condition": {"type": "predicate_equal",
                    "left": {"type": "column", "name": "N_REGIONKEY", "tableName": "NATION"},
                    "right": {"type": "column", "name": "R_REGIONKEY", "tableName": "REGION"}}},
            "selectList": [
                {"type": "column", "name": "S_NAME", "tableName": "SUPPLIER"},
                {"type": "column", "name": "R_NAME", "tableName": "REGION"}],
        },
        "schemaMetadataInfo": {"properties": {}, "adapterNotes":
            serde_json::json!({"TABLE_MAP":
                {"SUPPLIER": "lh.supplier", "NATION": "lh.nation", "REGION": "lh.region"}})
                .to_string()},
    })
}

/// Scenario: A join outside the broadcast contract is declined safely
#[test]
fn join_outside_contract_declined_safely() {
    for reason in [
        IneligibleJoinReason::NotInnerJoinType,
        IneligibleJoinReason::UnsupportedShape,
    ] {
        let err = ineligible_join_decline(reason);
        match err {
            UdfError::User(msg) => {
                assert!(
                    msg.contains("join pushdown declined") && msg.contains("cannot"),
                    "ineligible reason {reason:?} must be a plain hard-error decline: {msg}"
                );
                assert!(
                    !msg.contains("retry"),
                    "ineligible reason {reason:?} must NOT claim a native retry: {msg}"
                );
            }
            other => panic!("ineligible join must be a User decline, got {other:?}"),
        }
    }

    let outer = join_request(
        serde_json::json!({"join_type": "left_outer"}),
        equi_condition(),
    );
    assert!(
        matches!(
            detect_join(&outer, &pd(&outer)),
            Ok(JoinShape::Ineligible(
                IneligibleJoinReason::NotInnerJoinType
            ))
        ),
        "an outer join must classify Ineligible so the decline path is taken"
    );

    let mut request = join_request(Json::Null, equi_condition());
    for table_idx in [0, 1] {
        request["involvedTables"][table_idx]["columns"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "name": "SHARED_COL",
                "dataType": {"type": "varchar", "size": 10}
            }));
    }
    let detected = detected_join(&request);
    let rendered = render_broadcast_join(&request, &pd(&request), &detected)
        .expect("guard failure is a decline, not an error");
    assert!(
        rendered.is_none(),
        "overlapping column names must decline broadcast rendering (Ok(None))"
    );
}

/// Scenario: A widened projection declines broadcast for the N-scan wrapper (#196, #234)
#[test]
fn broadcast_join_declines_widened_projection() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["selectList"] = serde_json::json!([
        {"type": "function_scalar", "name": "UPPER", "arguments": [
            {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"}]},
    ]);
    let pushdown_req = pd(&request);
    assert!(matches!(
        classify_join_window(&pushdown_req),
        JoinWindowPlan::Unbounded
    ));
    let detected = detected_join(&request);

    let mut ok_req = request.clone();
    ok_req["pushdownRequest"]["selectListDataTypes"] =
        serde_json::json!([{"type": "varchar", "size": 100}]);
    let (_, _, ok_widened) =
        extract_join_projection(&ok_req, &pd(&ok_req), &detected).expect("projection derives");
    assert!(!ok_widened, "the control fixture must NOT widen");
    assert!(
        render_broadcast_join(&ok_req, &pd(&ok_req), &detected)
            .expect("the control must not error")
            .is_some(),
        "the control must render a broadcast plan, so only the widening differs"
    );

    request["pushdownRequest"]["selectListDataTypes"] =
        serde_json::json!([{"type": "timestamp", "withLocalTimeZone": true}]);
    let pushdown_req = pd(&request);
    let (projection, _, widened) =
        extract_join_projection(&request, &pushdown_req, &detected).expect("projection derives");
    assert!(
        widened && projection.len() == 4,
        "precondition: the one-item select list must widen to the 4-column \
         two-table base row"
    );

    assert!(
        render_broadcast_join(&request, &pushdown_req, &detected)
            .expect("a widened projection is a clean decline, NOT an error")
            .is_none(),
        "a widened projection must decline broadcast rendering (Ok(None)) so the \
         request falls through to the unified N-scan fallback"
    );
}

/// Scenario: An unrenderable filter declines broadcast; an absent filter stays eligible
#[test]
fn broadcast_declines_on_unrenderable_filter_stays_eligible_when_absent() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["filter"] = serde_json::json!({
        "type": "predicate_greater",
        "left": {
            "type": "function_scalar",
            "name": "SECOND",
            "arguments": [
                {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
                {"type": "literal_exactnumeric", "value": 3}
            ]
        },
        "right": {"type": "literal_exactnumeric", "value": 1}
    });
    let detected = detected_join(&request);

    assert!(
        render_broadcast_join(&request, &pd(&request), &detected)
            .expect("a declined filter is a clean decline, not an error")
            .is_none(),
        "an unrenderable filter must decline the broadcast plan (Ok(None)) so the \
         request falls through to the N-scan fallback, which self-applies it"
    );

    let absent_request = join_request(Json::Null, equi_condition());
    let absent_detected = detected_join(&absent_request);
    assert!(
        render_broadcast_join(&absent_request, &pd(&absent_request), &absent_detected)
            .expect("an absent filter must not error")
            .is_some(),
        "an absent filter must NOT decline broadcast eligibility"
    );
}

/// Scenario: LIKE over a DECIMAL side column declines broadcast (#207, #215)
#[test]
fn broadcast_declines_like_over_decimal_side_column() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["filter"] = serde_json::json!({
        "type": "predicate_like",
        "expression": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
        "pattern": {"type": "literal_string", "value": "1%"}
    });
    let detected = detected_join(&request);

    let outcome = render_broadcast_join(&request, &pd(&request), &detected)
        .expect("a type-declined filter is a clean decline, not an error");
    assert!(
        outcome.is_none(),
        "LIKE over a DECIMAL side column must decline the broadcast plan \
         (Ok(None)) so the request falls through to the N-scan fallback, which \
         self-applies it"
    );
}

/// Scenario: LIKE over a DATE side column keeps broadcast with a CAST-to-VARCHAR subject
#[test]
fn broadcast_keeps_plan_and_casts_like_over_date_side_column() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["filter"] = serde_json::json!({
        "type": "predicate_like",
        "expression": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
        "pattern": {"type": "literal_string", "value": "1995%"}
    });
    let detected = detected_join(&request);

    let rendered = render_broadcast_join(&request, &pd(&request), &detected)
        .expect("a DATE LIKE subject must not error")
        .expect("a DATE LIKE subject must keep the broadcast plan, not decline");
    let filter = rendered
        .filter
        .expect("the rewritten LIKE must still render as a scan-spec filter");
    assert!(
        filter.contains(r#"CAST("O_ORDERDATE" AS VARCHAR)"#) && filter.contains("LIKE"),
        "the DATE subject must be rewrapped in CAST-to-VARCHAR form before the \
         LIKE: {filter}"
    );
}

/// Scenario: INSTR with a start-position argument declines broadcast (#228)
#[test]
fn broadcast_declines_instr_with_start_position_argument() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["filter"] = serde_json::json!({
        "type": "predicate_greater",
        "left": {
            "type": "function_scalar",
            "name": "INSTR",
            "arguments": [
                {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                {"type": "literal_string", "value": "b"},
                {"type": "literal_exactnumeric", "value": 3}
            ]
        },
        "right": {"type": "literal_exactnumeric", "value": 0}
    });
    let detected = detected_join(&request);

    let outcome = render_broadcast_join(&request, &pd(&request), &detected)
        .expect("a type-declined filter is a clean decline, not an error");
    assert!(
        outcome.is_none(),
        "INSTR with a start-position argument must decline the broadcast plan \
         (Ok(None)) so Exasol evaluates it natively via the N-scan fallback's \
         residual WHERE"
    );
}

/// Scenario: Absent and trivially-true filters stay broadcast-eligible with no scan filter
#[test]
fn broadcast_absent_and_trivially_true_filter_stay_eligible() {
    let absent_request = join_request(Json::Null, equi_condition());
    let absent_detected = detected_join(&absent_request);
    let absent = render_broadcast_join(&absent_request, &pd(&absent_request), &absent_detected)
        .expect("an absent filter must not error")
        .expect("an absent filter must keep the broadcast plan");
    assert!(
        absent.filter.is_none(),
        "an absent filter must carry no scan-spec filter: {:?}",
        absent.filter
    );

    let mut trivial_request = join_request(Json::Null, equi_condition());
    trivial_request["pushdownRequest"]["filter"] =
        serde_json::json!({"type": "literal_bool", "value": true});
    let trivial_detected = detected_join(&trivial_request);
    let trivial = render_broadcast_join(&trivial_request, &pd(&trivial_request), &trivial_detected)
        .expect("a trivially-true filter must not error")
        .expect("a trivially-true filter must keep the broadcast plan");
    assert!(
        trivial.filter.is_none(),
        "a trivially-true filter must carry no scan-spec filter: {:?}",
        trivial.filter
    );
}

/// Scenario: A two-table join falls back to the unified N-scan wrapper
#[test]
fn two_table_join_falls_back_to_unified_n_scan_wrapper() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["filter"] = serde_json::json!({
        "type": "predicate_greater",
        "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
        "right": {"type": "literal_string", "value": "1995-01-01"}
    });
    let detected = detected_join(&request);
    let sides = vec![
        resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
        resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
    ];
    let sql = build_n_scan_join_sql(
        &request,
        &pd(&request),
        &detected,
        &sides,
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("the two-table unified fallback must build");

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

/// Scenario: A trivially-true residual emits no outer WHERE and does not error
#[test]
fn trivially_true_residual_emits_no_outer_where_and_does_not_error() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["filter"] =
        serde_json::json!({"type": "literal_bool", "value": true});
    let detected = detected_join(&request);
    let sides = vec![
        resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
        resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
    ];

    let sql = build_n_scan_join_sql(
        &request,
        &pd(&request),
        &detected,
        &sides,
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("a trivially-true residual is a correct no-op, never a hard error");

    assert!(
        !sql.contains(" WHERE "),
        "a trivially-true residual must emit NO outer WHERE: {sql}"
    );
}

/// Scenario: Colliding column names render a qualified unified wrapper without error
#[test]
fn colliding_columns_render_qualified_unified_wrapper_without_error() {
    let request = serde_json::json!({
        "involvedTables": [
            {"name": "EVENTS", "columns": [
                {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                {"name": "SCORE", "dataType": {"type": "double"}}]},
            {"name": "LABELS", "columns": [
                {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                {"name": "LABEL", "dataType": {"type": "varchar", "size": 100}}]},
        ],
        "pushdownRequest": {
            "type": "select",
            "from": {"type": "join", "join_type": "inner",
                "left": {"name": "EVENTS", "type": "table"},
                "right": {"name": "LABELS", "type": "table"},
                "condition": {"type": "predicate_equal",
                    "left": {"type": "column", "name": "ID", "tableName": "EVENTS"},
                    "right": {"type": "column", "name": "ID", "tableName": "LABELS"}}},
            "selectList": [
                {"type": "column", "name": "ID", "tableName": "EVENTS"},
                {"type": "column", "name": "LABEL", "tableName": "LABELS"}],
        },
        "schemaMetadataInfo": {"properties": {}, "adapterNotes":
            serde_json::json!({"TABLE_MAP": {"EVENTS": "lh.events", "LABELS": "lh.labels"}})
                .to_string()},
    });

    let left = involved_table_columns(&request, "EVENTS");
    let right = involved_table_columns(&request, "LABELS");
    assert!(!disjoint_schema_guard(&left, &right));
    let detected = detected_join(&request);
    assert!(
        render_broadcast_join(&request, &pd(&request), &detected)
            .unwrap()
            .is_none()
    );

    let sides = vec![
        resolved_side("EVENTS", vec![("s3://w/e-0.parquet", 100)]),
        resolved_side("LABELS", vec![("s3://w/l-0.parquet", 10)]),
    ];
    let sql = build_n_scan_join_sql(
        &request,
        &pd(&request),
        &detected,
        &sides,
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
        &request,
        &pd(&request),
        &multi,
        &sides,
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

/// Scenario: The N-scan builder handles the Q1 shape
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
        &request,
        &pd(&request),
        &multi,
        &sides,
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

/// Scenario: The N-scan builder handles the four-table NQ3 shape
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
        &request,
        &pd(&request),
        &multi,
        &sides,
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

/// Scenario: Three tables sharing a column name render fully qualified
#[test]
fn build_n_scan_join_sql_renders_qualified_when_three_tables_share_column_name() {
    let request = serde_json::json!({
        "involvedTables": [
            {"name": "EVENTS", "columns": [
                {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                {"name": "SCORE", "dataType": {"type": "double"}}]},
            {"name": "LABELS", "columns": [
                {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                {"name": "LABEL", "dataType": {"type": "varchar", "size": 100}}]},
            {"name": "TAGS", "columns": [
                {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                {"name": "TAG_NAME", "dataType": {"type": "varchar", "size": 100}}]},
        ],
        "pushdownRequest": {
            "type": "select",
            "from": {"type": "join", "join_type": "inner",
                "left": {"type": "join", "join_type": "inner",
                    "left": {"name": "EVENTS", "type": "table"},
                    "right": {"name": "LABELS", "type": "table"},
                    "condition": {"type": "predicate_equal",
                        "left": {"type": "column", "name": "ID", "tableName": "EVENTS"},
                        "right": {"type": "column", "name": "ID", "tableName": "LABELS"}}},
                "right": {"name": "TAGS", "type": "table"},
                "condition": {"type": "predicate_equal",
                    "left": {"type": "column", "name": "ID", "tableName": "LABELS"},
                    "right": {"type": "column", "name": "ID", "tableName": "TAGS"}}},
            "selectList": [
                {"type": "column", "name": "ID", "tableName": "EVENTS"},
                {"type": "column", "name": "LABEL", "tableName": "LABELS"},
                {"type": "column", "name": "TAG_NAME", "tableName": "TAGS"}],
        },
        "schemaMetadataInfo": {"properties": {}, "adapterNotes":
            serde_json::json!({"TABLE_MAP":
                {"EVENTS": "lh.events", "LABELS": "lh.labels", "TAGS": "lh.tags"}})
                .to_string()},
    });
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
        &request,
        &pd(&request),
        &multi,
        &sides,
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

/// Scenario: A self-join renders each occurrence as its own leg (#361)
#[test]
fn self_join_renders_each_occurrence_as_its_own_leg() {
    for leg_aliases in [[Some("A"), Some("B")], [None, Some("B")]] {
        let request = self_join_request(&leg_aliases);
        let sides = vec![
            resolved_side("FACT_ORDERS", vec![("s3://w/f0.parquet", 10)]),
            resolved_side("FACT_ORDERS", vec![("s3://w/f1.parquet", 10)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected_join(&request),
            &sides,
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

/// Scenario: A three-leg self-join attaches each condition at its own join point
#[test]
fn three_leg_self_join_attaches_each_condition_at_its_own_join_point() {
    let request = self_join_request(&[Some("A"), Some("B"), Some("C")]);
    let sides = vec![
        resolved_side("FACT_ORDERS", vec![("s3://w/f0.parquet", 10)]),
        resolved_side("FACT_ORDERS", vec![("s3://w/f1.parquet", 10)]),
        resolved_side("FACT_ORDERS", vec![("s3://w/f2.parquet", 10)]),
    ];
    let sql = build_n_scan_join_sql(
        &request,
        &pd(&request),
        &detected_join(&request),
        &sides,
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

/// Scenario: An unattributable column reference is a hard error naming the column
#[test]
fn unattributable_column_reference_is_a_hard_error_naming_the_column() {
    let mut request = self_join_request(&[Some("A"), Some("B")]);
    request["pushdownRequest"]["selectList"] = serde_json::json!([
        {"type": "column", "name": "O_ORDERKEY", "tableName": "FACT_ORDERS", "tableAlias": "Z"}
    ]);
    let sides = vec![
        resolved_side("FACT_ORDERS", vec![("s3://w/f0.parquet", 10)]),
        resolved_side("FACT_ORDERS", vec![("s3://w/f1.parquet", 10)]),
    ];

    let err = build_n_scan_join_sql(
        &request,
        &pd(&request),
        &detected_join(&request),
        &sides,
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect_err("a reference no leg key matches must be a hard error, never an arbitrary guess");

    match err {
        UdfError::User(msg) => {
            assert!(msg.contains("O_ORDERKEY"), "{msg}");
            assert!(msg.contains("FACT_ORDERS"), "{msg}");
            assert!(
                msg.contains("could not be attributed to a join leg"),
                "{msg}"
            );
            assert!(msg.contains("hard error, not a native re-plan"), "{msg}");
        }
        other => panic!("expected a User decline, got {other:?}"),
    }
}

/// Scenario: A leg count disagreeing with the resolved sides declines naming both counts
#[test]
fn a_leg_count_disagreeing_with_the_resolved_sides_declines_naming_both_counts() {
    let request = self_join_request(&[Some("A"), Some("B")]);
    let sides = vec![resolved_side(
        "FACT_ORDERS",
        vec![("s3://w/f0.parquet", 10)],
    )];

    let err = build_n_scan_join_sql(
        &request,
        &pd(&request),
        &detected_join(&request),
        &sides,
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect_err("a leg index that cannot index the resolved sides must decline, not panic");

    match err {
        UdfError::User(msg) => assert_eq!(
            msg,
            "join pushdown declined: leg count (2) and resolved-side count (1) disagree, \
             so a leg index cannot index the resolved sides; this is a hard error, not a \
             native re-plan"
        ),
        other => panic!("expected a User decline, got {other:?}"),
    }
}

/// Scenario: Every outer-wrapper clause on a self-join qualifies by its own occurrence's leg
#[test]
fn n_scan_wrapper_qualifies_every_clause_by_leg() {
    fn col(alias: &str) -> Json {
        serde_json::json!({
            "type": "column", "name": "O_CUSTKEY", "tableName": "FACT_ORDERS", "tableAlias": alias
        })
    }
    fn sum_of(alias: &str) -> Json {
        serde_json::json!({
            "type": "function_aggregate", "name": "SUM", "distinct": false,
            "arguments": [col(alias)]
        })
    }
    let mut request = self_join_request(&[Some("A"), Some("B")]);
    request["pushdownRequest"]["aggregationType"] = serde_json::json!("group_by");
    request["pushdownRequest"]["selectList"] = serde_json::json!([col("A"), sum_of("B")]);
    request["pushdownRequest"]["groupBy"] = serde_json::json!([col("A")]);
    request["pushdownRequest"]["having"] = serde_json::json!({
        "type": "predicate_greater", "left": sum_of("B"),
        "right": {"type": "literal_exactnumeric", "value": 10},
    });
    request["pushdownRequest"]["orderBy"] = serde_json::json!([
        {"expression": col("A"), "isAscending": true, "nullsLast": false},
    ]);

    let sides = vec![
        resolved_side("FACT_ORDERS", vec![("s3://w/f0.parquet", 10)]),
        resolved_side("FACT_ORDERS", vec![("s3://w/f1.parquet", 10)]),
    ];
    let sql = build_n_scan_join_sql(
        &request,
        &pd(&request),
        &detected_join(&request),
        &sides,
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("every clause must render against its own occurrence's leg");

    assert!(
        sql.contains(r#"SELECT "LHS_T0"."O_CUSTKEY", SUM("LHS_T1"."O_CUSTKEY")"#),
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

/// Scenario: Self-join leg-local filters partition exactly and the condition attaches to ON
#[test]
fn conditions_attach_by_leg_set_and_leg_local_filters_partition_exactly() {
    fn col(alias: &str) -> Json {
        serde_json::json!({
            "type": "column", "name": "O_CUSTKEY", "tableName": "FACT_ORDERS", "tableAlias": alias
        })
    }
    let mut request = self_join_request(&[Some("A"), Some("B")]);
    request["pushdownRequest"]["filter"] = serde_json::json!({
        "type": "predicate_and",
        "expressions": [
            {"type": "predicate_greater", "left": col("A"),
             "right": {"type": "literal_exactnumeric", "value": 100}},
            {"type": "predicate_equal", "left": col("B"),
             "right": {"type": "literal_exactnumeric", "value": 7}},
        ],
    });
    let sides = vec![
        resolved_side("FACT_ORDERS", vec![("s3://w/f0.parquet", 10)]),
        resolved_side("FACT_ORDERS", vec![("s3://w/f1.parquet", 10)]),
    ];
    let sql = build_n_scan_join_sql(
        &request,
        &pd(&request),
        &detected_join(&request),
        &sides,
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

/// Scenario: The two-table above-threshold fallback renders an INNER JOIN … ON chain
#[test]
fn above_threshold_join_falls_back_inner_join_on() {
    let request = join_request(Json::Null, equi_condition());
    let detected = detected_join(&request);
    let sides = vec![
        resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
        resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
    ];
    let sql = build_n_scan_join_sql(
        &request,
        &pd(&request),
        &detected,
        &sides,
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("the above-threshold two-table fallback must build");

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

/// Scenario: A three-table join renders a two-hop INNER JOIN … ON chain
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
        &request,
        &pd(&request),
        &detected,
        &sides,
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

/// Scenario: Conditions greedy-attach by table set and side-local conjuncts push into their leg
#[test]
fn join_conditions_greedy_attach_and_side_local_pushdown() {
    let cond_n2_fact = serde_json::json!({
        "type": "predicate_equal",
        "left": {"type": "column", "name": "N2_KEY", "tableName": "N2"},
        "right": {"type": "column", "name": "F_N2KEY", "tableName": "FACT"}});
    let cond_n1_fact = serde_json::json!({
        "type": "predicate_equal",
        "left": {"type": "column", "name": "N1_KEY", "tableName": "N1"},
        "right": {"type": "column", "name": "F_N1KEY", "tableName": "FACT"}});
    let request = serde_json::json!({
        "involvedTables": [
            {"name": "N1", "columns": [
                {"name": "N1_KEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "N1_NAME", "dataType": {"type": "varchar", "size": 100}}]},
            {"name": "N2", "columns": [
                {"name": "N2_KEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]},
            {"name": "FACT", "columns": [
                {"name": "F_N1KEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "F_N2KEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "F_VALUE", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]},
        ],
        "pushdownRequest": {
            "type": "select",
            "from": {"type": "join", "join_type": "inner",
                "left": {"name": "N1", "type": "table"},
                "right": {"type": "join", "join_type": "inner",
                    "left": {"name": "N2", "type": "table"},
                    "right": {"name": "FACT", "type": "table"},
                    "condition": cond_n2_fact},
                "condition": cond_n1_fact},
            "selectList": [
                {"type": "column", "name": "N1_NAME", "tableName": "N1"},
                {"type": "column", "name": "F_VALUE", "tableName": "FACT"}],
            "filter": {"type": "predicate_and", "expressions": [
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "N1_NAME", "tableName": "N1"},
                 "right": {"type": "literal_string", "value": "ACME"}},
                {"type": "predicate_greater",
                 "left": {"type": "column", "name": "F_VALUE", "tableName": "FACT"},
                 "right": {"type": "column", "name": "N1_KEY", "tableName": "N1"}}]},
        },
        "schemaMetadataInfo": {"properties": {}, "adapterNotes":
            serde_json::json!({"TABLE_MAP":
                {"N1": "lh.n1", "N2": "lh.n2", "FACT": "lh.fact"}})
                .to_string()},
    });
    let detected = detected_join(&request);
    let sides = vec![
        resolved_side("N1", vec![("s3://w/n1-0.parquet", 10)]),
        resolved_side("N2", vec![("s3://w/n2-0.parquet", 10)]),
        resolved_side("FACT", vec![("s3://w/f-0.parquet", 500)]),
    ];
    let sql = build_n_scan_join_sql(
        &request,
        &pd(&request),
        &detected,
        &sides,
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
    let detected = detected_join(request);
    let sides = vec![
        resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
        resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
    ];
    let sql = build_n_scan_join_sql(
        request,
        &pd(request),
        &detected,
        &sides,
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("the two-table unified fallback must build");
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

fn like_over(column: &str, table: &str, pattern: &str) -> Json {
    serde_json::json!({
        "type": "predicate_like",
        "expression": {"type": "column", "name": column, "tableName": table},
        "pattern": {"type": "literal_string", "value": pattern}
    })
}

/// Scenario: A type-declined side-local conjunct moves to the outer WHERE (#207, #215)
#[test]
fn n_scan_type_declined_side_local_conjunct_moves_to_outer_where() {
    let request = n_scan_request_with_filter(like_over("O_CUSTKEY", "ORDERS", "1%"));

    let (legs, outer) = n_scan_legs_and_outer_where(&request);

    assert!(
        outer.contains(r#""LHS_T1"."O_CUSTKEY""#) && outer.contains("LIKE"),
        "the type-declined conjunct must be self-applied table-qualified in the \
         outer WHERE: {outer}"
    );
    assert!(
        !legs.contains("LIKE"),
        "it must NOT also reach the fan-out leg — that is the tree DataFusion \
         cannot coerce: {legs}"
    );
}

/// Scenario: A side-local DATE LIKE reaches its leg as a CAST
#[test]
fn n_scan_date_like_side_local_conjunct_reaches_leg_as_cast() {
    let request = n_scan_request_with_filter(like_over("O_ORDERDATE", "ORDERS", "1995%"));

    let (legs, outer) = n_scan_legs_and_outer_where(&request);

    assert!(
        legs.contains(r#"CAST(\"O_ORDERDATE\" AS VARCHAR)"#) && legs.contains("LIKE"),
        "the DATE LIKE subject must reach its leg CAST to VARCHAR: {legs}"
    );
    assert!(
        outer.is_empty(),
        "a conjunct its leg applies must not ALSO be applied by the outer \
         wrapper: {outer}"
    );
}

/// Scenario: A type-accepted conjunct still pushes when a same-side sibling declines
#[test]
fn n_scan_type_accepted_side_local_conjunct_still_pushes_when_a_sibling_declines() {
    let request = n_scan_request_with_filter(serde_json::json!({
        "type": "predicate_and",
        "expressions": [
            like_over("O_ORDERDATE", "ORDERS", "1995%"),
            like_over("O_CUSTKEY", "ORDERS", "1%"),
        ],
    }));

    let (legs, outer) = n_scan_legs_and_outer_where(&request);

    assert_eq!(
        legs.matches("LIKE").count(),
        1,
        "exactly the type-accepted LIKE may reach the legs: {legs}"
    );
    assert!(
        legs.contains(r#"CAST(\"O_ORDERDATE\" AS VARCHAR)"#),
        "and it must arrive rewritten: {legs}"
    );
    assert_eq!(
        outer.matches("LIKE").count(),
        1,
        "exactly the type-declined LIKE may reach the outer WHERE: {outer}"
    );
    assert!(
        outer.contains(r#""LHS_T1"."O_CUSTKEY""#),
        "and it must be the DECIMAL one, table-qualified: {outer}"
    );
}

/// Scenario: The leg/residual partition stays total and disjoint with the type screen
#[test]
fn n_scan_leg_residual_partition_is_total_and_disjoint_with_type_screen() {
    let request = n_scan_request_with_filter(serde_json::json!({
        "type": "predicate_and",
        "expressions": [
            {"type": "predicate_equal",
             "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
             "right": {"type": "literal_string", "value": "ACME"}},
            like_over("O_ORDERDATE", "ORDERS", "1995%"),
            like_over("C_CUSTKEY", "CUSTOMER", "1%"),
            {"type": "predicate_greater",
             "left": {"type": "function_scalar", "name": "SECOND", "arguments": [
                 {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
                 {"type": "literal_exactnumeric", "value": 3}]},
             "right": {"type": "literal_exactnumeric", "value": 1}},
            {"type": "predicate_greater",
             "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
             "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"}},
        ],
    }));

    let (legs, outer) = n_scan_legs_and_outer_where(&request);

    assert_eq!(
        legs.matches("ACME").count(),
        1,
        "conjunct 1 belongs to the CUSTOMER leg, exactly once: {legs}"
    );
    assert!(
        !outer.contains("ACME"),
        "conjunct 1 must not be double-applied in the outer WHERE: {outer}"
    );
    assert_eq!(
        legs.matches(r#"CAST(\"O_ORDERDATE\" AS VARCHAR)"#).count(),
        1,
        "conjunct 2 belongs to the ORDERS leg, rewritten, exactly once: {legs}"
    );
    assert_eq!(
        legs.matches("LIKE").count(),
        1,
        "conjunct 2 is the ONLY LIKE any leg may carry: {legs}"
    );
    assert_eq!(
        outer.matches("LIKE").count(),
        1,
        "conjunct 3 is the only LIKE the outer WHERE carries: {outer}"
    );
    assert!(
        outer.contains("SECOND"),
        "conjunct 4 belongs to the outer WHERE: {outer}"
    );
    assert!(
        !legs.contains("SECOND"),
        "conjunct 4 must not reach a leg: {legs}"
    );
    assert!(
        outer.contains(r#""LHS_T0"."C_CUSTKEY" > "LHS_T1"."O_CUSTKEY""#),
        "conjunct 5 belongs to the outer WHERE, table-qualified: {outer}"
    );
}

/// Scenario: LIKE over a VARCHAR side column pushes down unchanged at both join sites
#[test]
fn join_like_over_varchar_side_column_pushes_down_unchanged() {
    let request = n_scan_request_with_filter(like_over("C_NAME", "CUSTOMER", "A%"));
    let detected = detected_join(&request);

    let rendered = render_broadcast_join(&request, &pd(&request), &detected)
        .expect("a VARCHAR LIKE subject must not error")
        .expect("a VARCHAR LIKE subject must keep the broadcast plan");
    let filter = rendered
        .filter
        .expect("the LIKE conjunct must still render as a scan-spec filter");
    assert!(
        filter.contains(r#""C_NAME" LIKE"#) && !filter.contains("CAST("),
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

/// Scenario: DECIMAL stringification renders trimmed at both join sites (#211)
#[test]
fn join_decimal_stringification_renders_trimmed_at_both_join_sites() {
    let mut request = join_request(Json::Null, equi_condition());
    request["involvedTables"][1]["columns"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!(
            {"name": "O_TOTALPRICE", "dataType": {"type": "decimal", "precision": 20, "scale": 2}}
        ));
    let filter = serde_json::json!({
        "type": "predicate_greater",
        "left": {
            "type": "function_scalar",
            "name": "LENGTH",
            "arguments": [{"type": "column", "name": "O_TOTALPRICE", "tableName": "ORDERS"}]
        },
        "right": {"type": "literal_exactnumeric", "value": 3}
    });
    request["pushdownRequest"]["filter"] = filter;
    let detected = detected_join(&request);

    let trim_wrapper = "regexp_replace(regexp_replace(CAST(";

    let rendered = render_broadcast_join(&request, &pd(&request), &detected)
        .expect("LENGTH(DECIMAL) > 3 must not error")
        .expect("a renderable decimal-stringification rewrite must keep the broadcast plan");
    let filter = rendered
        .filter
        .expect("the rewritten filter must still render as a scan-spec filter");
    assert!(
        filter.contains(trim_wrapper),
        "the broadcast filter must carry the decimal_to_varchar_exasol trim \
         form: {filter}"
    );

    let (legs, _outer) = n_scan_legs_and_outer_where(&request);
    assert!(
        legs.contains(trim_wrapper),
        "the ORDERS fan-out leg must carry the same trim form: {legs}"
    );
}

/// Scenario: INSTR beyond two arguments declines at both join sites (#228)
#[test]
fn join_instr_beyond_two_args_declines_at_both_join_sites() {
    let filter = serde_json::json!({
        "type": "predicate_greater",
        "left": {
            "type": "function_scalar",
            "name": "INSTR",
            "arguments": [
                {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                {"type": "literal_string", "value": "b"},
                {"type": "literal_exactnumeric", "value": 3}
            ]
        },
        "right": {"type": "literal_exactnumeric", "value": 0}
    });
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

/// Scenario: An aggregate over a join renders an Exasol aggregate over the unified wrapper
#[test]
fn aggregate_over_join_renders_exasol_aggregate_over_unified_wrapper() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["selectList"] = serde_json::json!([
        {"type": "function_aggregate", "name": "COUNT", "arguments": []},
        {"type": "function_aggregate", "name": "MIN", "arguments": [
            {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"}]},
    ]);

    assert!(matches!(
        classify_join_window(&pd(&request)),
        JoinWindowPlan::ExasolPostProcessed
    ));

    let detected = detected_join(&request);
    let sides = vec![
        resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
        resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
    ];
    let sql = build_n_scan_join_sql(
        &request,
        &pd(&request),
        &detected,
        &sides,
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("aggregate-over-join must build the unified wrapper");

    assert!(sql.contains("COUNT(*)"), "COUNT(*) must be rendered: {sql}");
    assert!(
        sql.contains(r#"MIN("LHS_T1"."O_ORDERDATE")"#),
        "MIN must qualify its argument to the owning side: {sql}"
    );
    assert!(
        sql.starts_with(r#"SELECT COUNT(*), MIN("LHS_T1"."O_ORDERDATE") FROM"#),
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

fn single_scan_legs(table_name: &str) -> JoinLegs {
    JoinLegs::for_single_scan(&serde_json::json!({
        "involvedTables": [{ "name": table_name }]
    }))
}

/// Scenario: A scalar function over aggregates renders qualified, never declining
#[test]
fn render_expression_qualified_renders_scalar_over_aggregate() {
    let legs = seam_legs();
    let sum_case = serde_json::json!({
        "type": "function_aggregate", "name": "SUM", "distinct": false,
        "arguments": [{
            "type": "function_scalar", "name": "CASE", "arguments": [
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "L_RETURNFLAG", "tableName": "LINEITEM"},
                 "right": {"type": "literal_string", "value": "R"}},
                {"type": "literal_exactnumeric", "value": 1},
                {"type": "literal_exactnumeric", "value": 0}]}]
    });
    let count_star = serde_json::json!({
        "type": "function_aggregate", "name": "COUNT", "arguments": [], "distinct": false
    });
    let item = serde_json::json!({
        "type": "function_scalar", "name": "ROUND", "arguments": [
            {"type": "function_scalar", "name": "FLOAT_DIV", "arguments": [
                {"type": "function_scalar", "name": "MULT", "arguments": [
                    {"type": "literal_double", "value": 100.0},
                    sum_case]},
                count_star]},
            {"type": "literal_exactnumeric", "value": 2}]
    });

    let sql = render_expression_qualified(&item, &legs)
        .expect("every reference names exactly one leg")
        .expect("a scalar-over-aggregate item must render, never decline to None");
    assert!(
        sql.contains(r#"SUM(CASE WHEN ("LHS_T2"."L_RETURNFLAG" = 'R') THEN 1 ELSE 0 END)"#),
        "the nested SUM(CASE ...) must render with its column table-qualified: {sql}"
    );
    assert!(
        sql.contains("COUNT(*)"),
        "the nested COUNT(*) must render as the star case: {sql}"
    );
}

/// Scenario: A top-level bare aggregate renders byte-compatibly through the unified seam
#[test]
fn render_expression_qualified_top_level_aggregate_byte_compatible() {
    let legs = seam_legs();

    let sum = serde_json::json!({
        "type": "function_aggregate", "name": "SUM", "distinct": false,
        "arguments": [{"type": "column", "name": "O_TOTALPRICE", "tableName": "ORDERS"}]
    });
    assert_eq!(
        render_expression_qualified(&sum, &legs)
            .expect("every reference names exactly one leg")
            .as_deref(),
        Some(r#"SUM("LHS_T1"."O_TOTALPRICE")"#)
    );

    let count_star = serde_json::json!({
        "type": "function_aggregate", "name": "COUNT", "arguments": [], "distinct": false
    });
    assert_eq!(
        render_expression_qualified(&count_star, &legs)
            .expect("every reference names exactly one leg")
            .as_deref(),
        Some("COUNT(*)")
    );

    let count_distinct = serde_json::json!({
        "type": "function_aggregate", "name": "COUNT", "distinct": true,
        "arguments": [{"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"}]
    });
    assert_eq!(
        render_expression_qualified(&count_distinct, &legs)
            .expect("every reference names exactly one leg")
            .as_deref(),
        Some(r#"COUNT(DISTINCT "LHS_T0"."C_CUSTKEY")"#)
    );
}

/// Scenario: Qualified COUNT(DISTINCT CAST(col AS CHAR(20))) keeps the CHAR(20) ASCII target (#192)
#[test]
fn qualified_count_distinct_cast_char_renders_exasol_char_target() {
    let legs = seam_legs();
    let item = serde_json::json!({
        "type": "function_aggregate", "name": "COUNT", "distinct": true,
        "arguments": [{
            "type": "function_scalar_cast", "name": "CAST",
            "arguments": [{"type": "column", "name": "C_VARCHAR", "tableName": "CUSTOMER"}],
            "dataType": {"type": "CHAR", "size": 20, "characterSet": "ASCII"}
        }]
    });
    let sql = render_expression_qualified(&item, &legs)
        .expect("every reference names exactly one leg")
        .expect("COUNT(DISTINCT CAST(col AS CHAR(20))) must render for the qualified wrapper");
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

/// Scenario: The N-scan wrapper's SELECT list keeps the CHAR(20) ASCII target (#192)
#[test]
fn n_scan_join_select_list_renders_exasol_char_target() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["selectList"] = serde_json::json!([
        {"type": "function_scalar_cast", "name": "CAST",
         "arguments": [{"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"}],
         "dataType": {"type": "CHAR", "size": 20, "characterSet": "ASCII"}},
        {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
    ]);
    request["pushdownRequest"]["having"] =
        serde_json::json!({"type": "literal_bool", "value": true});

    assert!(matches!(
        classify_join_window(&pd(&request)),
        JoinWindowPlan::ExasolPostProcessed
    ));

    let detected = detected_join(&request);
    let sides = vec![
        resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
        resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
    ];
    let sql = build_n_scan_join_sql(
        &request,
        &pd(&request),
        &detected,
        &sides,
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("a CAST-to-CHAR select item must build the unified wrapper");

    assert!(
        sql.contains(r#"CAST("LHS_T0"."C_NAME" AS CHAR(20) ASCII)"#),
        "the N-scan wrapper's SELECT list must declare the CHAR target: {sql}"
    );
    assert!(
        !sql.contains("VARCHAR(20)") && !sql.contains("AS VARCHAR)"),
        "must neither collapse to VARCHAR(20) nor emit a bare VARCHAR: {sql}"
    );
}

/// Scenario: A bare-column ORDER BY over a join renders qualified in the unified wrapper
#[test]
fn order_by_over_join_renders_qualified_in_unified_wrapper() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["selectList"] = serde_json::json!([
        {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
    ]);
    request["pushdownRequest"]["orderBy"] = serde_json::json!([
        {"expression": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
         "isAscending": true, "nullsLast": false},
    ]);

    assert!(matches!(
        classify_join_window(&pd(&request)),
        JoinWindowPlan::Ordered { .. }
    ));

    let detected = detected_join(&request);
    let sides = vec![
        resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
        resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
    ];
    let sql = build_n_scan_join_sql(
        &request,
        &pd(&request),
        &detected,
        &sides,
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("ordered unified wrapper must build");
    assert!(
        sql.contains(r#"ORDER BY "LHS_T1"."O_ORDERDATE" ASC NULLS FIRST"#),
        "ORDER BY must be table-qualified with explicit direction/nulls: {sql}"
    );
}

/// Scenario: An expression ORDER BY over a join renders qualified in the unified wrapper (#198)
#[test]
fn order_by_expression_renders_qualified_in_unified_wrapper() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["orderBy"] = serde_json::json!([
        {"expression": {"type": "function_scalar", "name": "UPPER", "arguments": [
            {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"}]},
         "isAscending": false, "nullsLast": true},
    ]);

    assert!(matches!(
        classify_join_window(&pd(&request)),
        JoinWindowPlan::ExasolPostProcessed
    ));

    let detected = detected_join(&request);
    let sides = vec![
        resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
        resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
    ];
    let sql = build_n_scan_join_sql(
        &request,
        &pd(&request),
        &detected,
        &sides,
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("ordered unified wrapper must build for a renderable expression sort key");
    assert!(
        sql.contains(r#"ORDER BY UPPER("LHS_T1"."O_ORDERDATE") DESC NULLS LAST"#),
        "the expression ORDER BY key must be table-qualified with explicit \
         direction/nulls, not declined: {sql}"
    );
}

/// Scenario: Join window classification covers every served and Exasol-post-processed shape
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
    ordered["pushdownRequest"]["orderBy"] = serde_json::json!([
        {"expression": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
         "isAscending": true, "nullsLast": false},
    ]);
    assert!(matches!(
        classify_join_window(&pd(&ordered)),
        JoinWindowPlan::Ordered { .. }
    ));

    let mut ordered_by_expression = join_request(Json::Null, equi_condition());
    ordered_by_expression["pushdownRequest"]["orderBy"] = serde_json::json!([
        {"expression": {"type": "function_scalar", "name": "UPPER", "arguments": [
            {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"}]},
         "isAscending": false, "nullsLast": true},
    ]);
    assert!(matches!(
        classify_join_window(&pd(&ordered_by_expression)),
        JoinWindowPlan::ExasolPostProcessed
    ));

    let mut aggregate_item = join_request(Json::Null, equi_condition());
    aggregate_item["pushdownRequest"]["selectList"] =
        serde_json::json!([{"type": "function_aggregate", "name": "COUNT", "arguments": []}]);
    assert!(matches!(
        classify_join_window(&pd(&aggregate_item)),
        JoinWindowPlan::ExasolPostProcessed
    ));

    let mut grouped = join_request(Json::Null, equi_condition());
    grouped["pushdownRequest"]["groupBy"] =
        serde_json::json!([{"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"}]);
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
    having["pushdownRequest"]["having"] =
        serde_json::json!({"type": "literal_bool", "value": true});
    assert!(matches!(
        classify_join_window(&pd(&having)),
        JoinWindowPlan::ExasolPostProcessed
    ));
}

/// Scenario: The window is classified before the broadcast render that would error
#[test]
fn aggregate_over_join_classifies_before_the_render_that_would_error() {
    let mut request = join_request(Json::Null, equi_condition());
    for table in request["involvedTables"].as_array_mut().unwrap() {
        table["columns"] = serde_json::json!([]);
    }
    request["pushdownRequest"]["selectList"] =
        serde_json::json!([{"type": "function_aggregate", "name": "COUNT", "arguments": []}]);
    let pushdown_req = pd(&request);

    assert!(matches!(
        classify_join_window(&pushdown_req),
        JoinWindowPlan::ExasolPostProcessed
    ));
    assert!(render_broadcast_join(&request, &pushdown_req, &detected_join(&request)).is_err());
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
    let sides = JoinSides {
        fact,
        dimension,
        broadcast_eligible: true,
    };
    let rendered = RenderedJoinPushdown {
        condition: r#"("L_ORDERKEY" = "O_ORDERKEY")"#.to_string(),
        filter: Some(r#"("L_QUANTITY" > 5)"#.to_string()),
        projection: vec![
            ProjectionItem::Column("L_ORDERKEY".to_string()),
            ProjectionItem::Column("O_ORDERDATE".to_string()),
        ],
        projection_types: vec!["DECIMAL(20,0)".to_string(), "DATE".to_string()],
    };
    let actual = build_broadcast_join_sql(
        &sides,
        &rendered,
        JoinWindowPlan::Unbounded,
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("selecting the wire storage must succeed")
    .expect("an unbounded broadcast join must build");
    assert_eq!(
        actual,
        r#"SELECT SCAN('{"table_root":"s3://warehouse/lh/lineitem","projection":["L_ORDERKEY","O_ORDERDATE"],"filter":"(\"L_QUANTITY\" > 5)","logical_schema":[{"field_id":1,"name":"LINEITEM_KEY","arrow_type":"int64","nullable":false}],"join":{"table_root":"s3://warehouse/lh/orders","files":[["s3://w/o-0.parquet",10]],"logical_schema":[{"field_id":1,"name":"ORDERS_KEY","arrow_type":"int64","nullable":false}],"join_type":"inner","condition":"(\"L_ORDERKEY\" = \"O_ORDERKEY\")","storage":{"connection":{"name":"LAKEHOUSE_CATALOG_CREDS","allow_http":true}}},"storage":{"connection":{"name":"LAKEHOUSE_CATALOG_CREDS","allow_http":true}},"df_target_partitions":1,"df_batch_size":8192,"df_threads_per_udf":1,"memory_pool_fraction":0.6,"instance_overhead_mb":0,"s3_max_connections":1}', '[["s3://w/l-0.parquet",1000]]') EMITS ("L_ORDERKEY" DECIMAL(20,0), "O_ORDERDATE" DATE)"#
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
    let sides = JoinSides {
        fact: fact.clone(),
        dimension: dimension.clone(),
        broadcast_eligible: true,
    };
    let rendered = RenderedJoinPushdown {
        condition: r#"("L_ORDERKEY" = "O_ORDERKEY")"#.to_string(),
        filter: None,
        projection: vec![ProjectionItem::Column("L_ORDERKEY".to_string())],
        projection_types: vec!["DECIMAL(20,0)".to_string()],
    };
    let sql = build_broadcast_join_sql(
        &sides,
        &rendered,
        JoinWindowPlan::Unbounded,
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

/// A single fact file means a single shard, so any ` FROM (` in the result is the
/// ordering wrapper's.
fn broadcast_window_sql(window: JoinWindowPlan) -> Option<String> {
    let sides = JoinSides {
        fact: resolved_side("LINEITEM", vec![("s3://w/l-0.parquet", 1000)]),
        dimension: resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 10)]),
        broadcast_eligible: true,
    };
    let rendered = RenderedJoinPushdown {
        condition: r#"("L_ORDERKEY" = "O_ORDERKEY")"#.to_string(),
        filter: None,
        projection: vec![ProjectionItem::Column("L_ORDERKEY".to_string())],
        projection_types: vec!["DECIMAL(20,0)".to_string()],
    };
    build_broadcast_join_sql(
        &sides,
        &rendered,
        window,
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("selecting the wire storage must succeed")
}

fn broadcast_common_blob(sql: &str) -> Json {
    serde_json::from_str(common_arg_literal(sql)).expect("common blob is valid JSON")
}

fn ascending_key(column: &str) -> ParsedSortKey {
    ParsedSortKey::Column(SortKey {
        column: column.to_string(),
        ascending: true,
        nulls_last: true,
    })
}

fn assert_shards_carry_no_window(sql: &str) {
    let common = broadcast_common_blob(sql);
    assert!(
        common.get("limit").is_none(),
        "an ordered shard must stay uncapped: {common}"
    );
    assert!(
        common.get("order_by").is_none(),
        "an ordered shard must stay unsorted: {common}"
    );
    assert!(
        common["join"].get("post_join_limit").is_none(),
        "an ordered shard must carry no post-join cap: {common}"
    );
}

/// Scenario: A bare LIMIT caps each shard's post-join output and the merge, never the fact scan
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

    let condition = r#""condition":"(\"L_ORDERKEY\" = \"O_ORDERKEY\")""#;
    assert_eq!(
        capped,
        format!(
            "{} LIMIT 7",
            unbounded.replace(condition, &format!("{condition},\"post_join_limit\":7"))
        ),
        "a bare cap must change the plan by exactly the join-block key and the merge LIMIT"
    );
}

/// Scenario: An ordered window wraps the fan-out once and leaves shards unbounded
#[test]
fn broadcast_ordered_wraps_fan_out_and_leaves_shards_unbounded() {
    let sql = broadcast_window_sql(JoinWindowPlan::Ordered {
        keys: vec![ascending_key("L_ORDERKEY")],
        limit: Some(5),
        offset: 0,
    })
    .expect("a projected bare-column ordering stays broadcast-eligible");

    assert!(
        sql.starts_with(r#"SELECT "L_ORDERKEY" FROM (SELECT SCAN("#),
        "the wrapper names only the visible columns over the fan-out: {sql}"
    );
    assert_shards_carry_no_window(&sql);
}

/// Scenario: A bare ORDER BY renders the wrapper's ordering with no window
#[test]
fn broadcast_ordered_without_limit_wraps_fan_out_with_no_window() {
    let sql = broadcast_window_sql(JoinWindowPlan::Ordered {
        keys: vec![ascending_key("L_ORDERKEY")],
        limit: None,
        offset: 0,
    })
    .expect("a projected bare-column ordering stays broadcast-eligible");

    assert!(
        sql.ends_with(r#") ORDER BY "L_ORDERKEY" ASC NULLS LAST"#),
        "an ORDER BY without a window must end at the ordering: {sql}"
    );
    assert_shards_carry_no_window(&sql);
}

/// Scenario: LIMIT and OFFSET render on the wrapper only, never per shard
#[test]
fn broadcast_ordered_renders_limit_and_offset_on_the_wrapper_only() {
    let sql = broadcast_window_sql(JoinWindowPlan::Ordered {
        keys: vec![ascending_key("L_ORDERKEY")],
        limit: Some(5),
        offset: 3,
    })
    .expect("a projected bare-column ordering stays broadcast-eligible");

    assert!(
        sql.ends_with(r#") ORDER BY "L_ORDERKEY" ASC NULLS LAST LIMIT 5 OFFSET 3"#),
        "the window must render on the wrapper, after the ordering: {sql}"
    );
    assert_shards_carry_no_window(&sql);
}

/// Scenario: An ORDER BY key outside the projection downgrades to the N-scan fallback
#[test]
fn broadcast_ordered_unprojected_key_downgrades_to_the_fallback() {
    assert!(
        broadcast_window_sql(JoinWindowPlan::Ordered {
            keys: vec![ascending_key("O_ORDERDATE")],
            limit: Some(5),
            offset: 0,
        })
        .is_none(),
        "an ORDER BY key outside the projection must fall through to the N-scan wrapper"
    );
}

/// Scenario: An Ordered plan rendering no ORDER BY is a programming error
#[test]
#[should_panic(expected = "must render an ORDER BY")]
fn broadcast_ordered_plan_rendering_no_order_by_is_a_programming_error() {
    let _ = broadcast_window_sql(JoinWindowPlan::Ordered {
        keys: Vec::new(),
        limit: Some(5),
        offset: 0,
    });
}

/// Scenario: A fallback leg never carries a limit, sort, or join block
#[test]
fn fallback_leg_fan_out_spec_never_carries_a_limit_or_sort() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["limit"] = serde_json::json!({"numElements": 10, "offset": 4});
    request["pushdownRequest"]["orderBy"] = serde_json::json!([{
        "type": "order_by_element",
        "expression": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
        "isAscending": true,
        "nullsLast": false,
    }]);
    let sides = vec![
        resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
        resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
    ];
    let sql = build_n_scan_join_sql(
        &request,
        &pd(&request),
        &detected_join(&request),
        &sides,
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("the two-table unified fallback must build");

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

/// Scenario: A join with no repeated table renders byte-identical golden SQL
#[test]
fn golden_n_scan_join_sql_unchanged() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["filter"] = serde_json::json!({
        "type": "predicate_and",
        "expressions": [
            {"type": "predicate_equal",
             "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
             "right": {"type": "literal_string", "value": "ACME"}},
            {"type": "predicate_greater",
             "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
             "right": {"type": "literal_string", "value": "1995-01-01"}},
            {"type": "predicate_greater",
             "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
             "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"}},
        ],
    });
    let detected = detected_join(&request);
    let sides = vec![
        resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
        resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
    ];
    let actual = build_n_scan_join_sql(
        &request,
        &pd(&request),
        &detected,
        &sides,
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("the two-table unified fallback must build");
    assert_eq!(
        actual,
        r#"SELECT "LHS_T0"."C_NAME", "LHS_T1"."O_ORDERDATE" FROM (SELECT SCAN('{"table_root":"s3://warehouse/lh/customer","projection":["C_CUSTKEY","C_NAME"],"filter":"(\"C_NAME\" = ''ACME'')","logical_schema":[{"field_id":1,"name":"CUSTOMER_KEY","arrow_type":"int64","nullable":false}],"storage":{"connection":{"name":"LAKEHOUSE_CATALOG_CREDS","allow_http":true}},"df_target_partitions":1,"df_batch_size":8192,"df_threads_per_udf":1,"memory_pool_fraction":0.6,"instance_overhead_mb":0,"s3_max_connections":1}', '[["s3://w/c-0.parquet",10]]') EMITS ("C_CUSTKEY" DECIMAL(20,0), "C_NAME" VARCHAR(100))) AS "LHS_T0" INNER JOIN (SELECT SCAN('{"table_root":"s3://warehouse/lh/orders","projection":["O_CUSTKEY","O_ORDERDATE"],"filter":"(\"O_ORDERDATE\" > ''1995-01-01'')","logical_schema":[{"field_id":1,"name":"ORDERS_KEY","arrow_type":"int64","nullable":false}],"storage":{"connection":{"name":"LAKEHOUSE_CATALOG_CREDS","allow_http":true}},"df_target_partitions":1,"df_batch_size":8192,"df_threads_per_udf":1,"memory_pool_fraction":0.6,"instance_overhead_mb":0,"s3_max_connections":1}', '[["s3://w/o-0.parquet",100]]') EMITS ("O_CUSTKEY" DECIMAL(20,0), "O_ORDERDATE" DATE)) AS "LHS_T1" ON (("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY")) WHERE (("LHS_T0"."C_CUSTKEY" > "LHS_T1"."O_CUSTKEY"))"#
    );
}

#[test]
fn golden_grouped_qualified_fallback_sql_unchanged() {
    let request = serde_json::json!({
        "involvedTables": [{"name": "CUSTOMER", "columns": [
            {"name": "C_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
            {"name": "C_NAME", "dataType": {"type": "varchar", "size": 100}},
        ]}],
    });
    let pushdown_req = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [{"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"}],
        "selectList": [
            {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
            agg_item("COUNT", None, false),
        ],
    });
    let fan_out_spec = ScanSpec {
        common: CommonScanSpec {
            projection: vec![
                ProjectionItem::Column("C_CUSTKEY".to_string()),
                ProjectionItem::Column("C_NAME".to_string()),
            ],
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![],
    };
    let proj_types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(100)".to_string()];
    let fan_out = FanOutProjection::new(&fan_out_spec, &proj_types).expect("aligned fan-out");
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
        r#"SELECT "LHS_T0"."C_NAME", COUNT(*) FROM (SELECT SCAN('{"projection":["C_CUSTKEY","C_NAME"],"storage":{"inline":{"s3":{"endpoint":"http://minio:9000","region":"us-east-1","access_key":"minioadmin","secret_key":"minioadmin","allow_http":true,"path_style":true}}},"df_target_partitions":1,"df_batch_size":8192,"df_threads_per_udf":1,"memory_pool_fraction":0.6,"instance_overhead_mb":200,"s3_max_connections":8}', '[["s3://w/c-0.parquet",10]]') EMITS ("C_CUSTKEY" DECIMAL(20,0), "C_NAME" VARCHAR(100))) AS "LHS_T0" GROUP BY "LHS_T0"."C_NAME""#
    );
}

/// Scenario: The six qualified N-scan render-decline messages keep their exact text
#[test]
fn golden_n_scan_render_decline_messages_unchanged() {
    fn user_message(err: UdfError) -> String {
        match err {
            UdfError::User(msg) => msg,
            other => panic!("expected a User decline, got {other:?}"),
        }
    }
    let unrenderable = || serde_json::json!({"type": "totally_unsupported_node_type"});
    let legs = legs_from_leaves(Vec::new());

    let select_list_req = serde_json::json!({ "selectList": [unrenderable()] });
    let msg = user_message(
        n_scan_join_select_items(&select_list_req, &legs, &[])
            .expect_err("an unrenderable select-list item must decline"),
    );
    assert_eq!(
        msg,
        "join pushdown declined: a select-list item could not be rendered for the \
         qualified N-scan join; this is a hard error, not a native re-plan"
    );

    let mut no_columns_request = join_request(Json::Null, equi_condition());
    no_columns_request["involvedTables"][1]["columns"] = serde_json::json!([]);
    let no_columns_sides = vec![
        resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
        resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
    ];
    let msg = user_message(
        build_n_scan_join_sql(
            &no_columns_request,
            &pd(&no_columns_request),
            &detected_join(&no_columns_request),
            &no_columns_sides,
            &two_scan_tuning(),
            "SCAN",
            "DISTRIBUTE",
        )
        .expect_err("an involved table with no column metadata must decline"),
    );
    assert_eq!(
        msg,
        "join pushdown declined: an involved table carries no column metadata, so the \
         unaccelerated N-scan fallback cannot be built; this is a hard error, not a \
         native re-plan"
    );

    let bad_condition_request = join_request(Json::Null, unrenderable());
    let bad_condition_sides = vec![
        resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
        resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
    ];
    let msg = user_message(
        build_n_scan_join_sql(
            &bad_condition_request,
            &pd(&bad_condition_request),
            &detected_join(&bad_condition_request),
            &bad_condition_sides,
            &two_scan_tuning(),
            "SCAN",
            "DISTRIBUTE",
        )
        .expect_err("an unrenderable join condition must decline"),
    );
    assert_eq!(
        msg,
        "join pushdown declined: a join condition could not be rendered against the \
         qualified N-scan schema; this is a hard error, not a native re-plan"
    );

    let group_by_req = serde_json::json!({ "groupBy": [unrenderable()] });
    let msg = user_message(
        qualified_join_group_by(&group_by_req, &legs)
            .expect_err("an unrenderable GROUP BY key must decline"),
    );
    assert_eq!(
        msg,
        "join pushdown declined: a GROUP BY key could not be rendered for the qualified \
         N-scan join; this is a hard error, not a native re-plan"
    );

    let having_req = serde_json::json!({ "having": unrenderable() });
    let msg = user_message(
        qualified_join_having(&having_req, &legs)
            .expect_err("an unrenderable HAVING expression must decline"),
    );
    assert_eq!(
        msg,
        "join pushdown declined: HAVING could not be rendered for the qualified \
         N-scan join; this is a hard error, not a native re-plan"
    );

    let order_by_req = serde_json::json!({
        "orderBy": [{
            "isAscending": true,
            "nullsLast": false,
            "expression": unrenderable(),
        }],
    });
    let msg = user_message(
        qualified_join_order_by(&order_by_req, &legs)
            .expect_err("an unrenderable ORDER BY expression must decline"),
    );
    assert_eq!(
        msg,
        "join pushdown declined: an ORDER BY key could not be rendered for the qualified \
         N-scan join; this is a hard error, not a native re-plan"
    );
}

/// Scenario: Both decline wrappers narrow the inner scan to referenced columns only (#160)
#[test]
fn fallback_projection_narrows_to_referenced_columns() {
    fn col(name: &str, ty: &str) -> (String, String) {
        (name.to_string(), ty.to_string())
    }
    fn spec_with(projection: Vec<ProjectionItem>) -> ScanSpec {
        ScanSpec {
            common: CommonScanSpec {
                projection,
                storage: ScanStorage::Inline(sample_storage()),
                ..Default::default()
            },
            files: vec![],
        }
    }
    fn proj_names(proj: &[ProjectionItem]) -> Vec<String> {
        proj.iter()
            .map(|p| match p {
                ProjectionItem::Column(n) => n.clone(),
                ProjectionItem::Expr { .. } => panic!("narrowing must yield columns only"),
            })
            .collect()
    }

    let request = serde_json::json!({"involvedTables": [{"name": "T"}]});
    let shards = [vec![("s3://wh/f0.parquet".to_string(), 1u64)]];

    let all_cols = vec![
        col("GK", "VARCHAR(10)"),
        col("REGION", "VARCHAR(10)"),
        col("HCOL", "DECIMAL(18,0)"),
        col("OCOL", "DECIMAL(18,0)"),
        col("FCOL", "DECIMAL(18,0)"),
        col("IRRELEVANT_COL", "VARCHAR(10)"),
    ];
    let rich_req = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [{"type": "column", "name": "GK", "tableName": "T"}],
        "selectList": [
            {"type": "column", "name": "GK", "tableName": "T"},
            {"type": "function_aggregate", "name": "SUM", "distinct": false, "arguments": [
                {"type": "function_scalar", "name": "CASE", "arguments": [
                    {"type": "predicate_equal",
                     "left": {"type": "column", "name": "REGION", "tableName": "T"},
                     "right": {"type": "literal_string", "value": "R"}},
                    {"type": "literal_exactnumeric", "value": 1},
                    {"type": "literal_exactnumeric", "value": 0}]}]}
        ],
        "having": {"type": "predicate_greater",
            "left": {"type": "function_aggregate", "name": "SUM", "distinct": false,
                     "arguments": [{"type": "column", "name": "HCOL", "tableName": "T"}]},
            "right": {"type": "literal_exactnumeric", "value": 10}},
        "orderBy": [{"expression": {"type": "column", "name": "OCOL", "tableName": "T"},
                     "isAscending": true, "nullsLast": false}],
        "filter": {"type": "predicate_equal",
            "left": {"type": "column", "name": "FCOL", "tableName": "T"},
            "right": {"type": "literal_exactnumeric", "value": 5}},
    });
    let (proj, types) = referenced_column_projection(&rich_req, &all_cols);
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
        col("GK", "VARCHAR(10)"),
        col("REGION", "VARCHAR(10)"),
        col("IRRELEVANT_COL", "VARCHAR(10)"),
    ];
    let grouped_req = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [{"type": "column", "name": "GK", "tableName": "T"}],
        "selectList": [
            {"type": "column", "name": "GK", "tableName": "T"},
            {"type": "function_aggregate", "name": "SUM", "distinct": false, "arguments": [
                {"type": "function_scalar", "name": "CASE", "arguments": [
                    {"type": "predicate_equal",
                     "left": {"type": "column", "name": "REGION", "tableName": "T"},
                     "right": {"type": "literal_string", "value": "R"}},
                    {"type": "literal_exactnumeric", "value": 1},
                    {"type": "literal_exactnumeric", "value": 0}]}]}
        ],
    });
    let (gproj, gtypes) = referenced_column_projection(&grouped_req, &grouped_all);
    assert_eq!(
        proj_names(&gproj),
        vec!["GK".to_string(), "REGION".to_string()],
        "the grouped inner scan narrows to GK + REGION (nested in SUM(CASE ...)), \
         never IRRELEVANT_COL"
    );
    let gspec = spec_with(gproj);
    let gfan_out = FanOutProjection::new(&gspec, &gtypes).expect("aligned grouped fan-out");
    let gsql = build_qualified_single_table_fallback_sql(
        &request,
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
        col("A", "VARCHAR(10)"),
        col("B", "VARCHAR(10)"),
        col("IRRELEVANT_COL", "VARCHAR(10)"),
    ];
    let sg_req = serde_json::json!({
        "selectList": [
            {"type": "function_aggregate", "name": "COUNT", "distinct": true,
             "arguments": [{"type": "column", "name": "A", "tableName": "T"}]},
            {"type": "function_aggregate", "name": "COUNT", "distinct": true,
             "arguments": [{"type": "column", "name": "B", "tableName": "T"}]},
        ],
    });
    let (sproj, stypes) = referenced_column_projection(&sg_req, &sg_all);
    assert_eq!(
        proj_names(&sproj),
        vec!["A".to_string(), "B".to_string()],
        "the single-group Case 2/3 inner scan narrows to A + B, never IRRELEVANT_COL"
    );
    let sspec = spec_with(sproj);
    let sfan_out = FanOutProjection::new(&sspec, &stypes).expect("aligned single-group fan-out");
    let ssql = build_qualified_single_table_fallback_sql(
        &request,
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

/// Scenario: Every no-select-list wire form keeps the full base row
#[test]
fn no_select_list_wire_forms_all_keep_the_full_base_row() {
    let all_cols = vec![
        ("GK".to_string(), "VARCHAR(10)".to_string()),
        ("FCOL".to_string(), "DECIMAL(18,0)".to_string()),
        ("IRRELEVANT_COL".to_string(), "VARCHAR(10)".to_string()),
    ];
    let filter = serde_json::json!({
        "type": "predicate_equal",
        "left": {"type": "column", "name": "FCOL", "tableName": "T"},
        "right": {"type": "literal_exactnumeric", "value": 5},
    });
    let full_row: Vec<ProjectionItem> = all_cols
        .iter()
        .map(|(name, _)| ProjectionItem::Column(name.clone()))
        .collect();
    let full_types: Vec<String> = all_cols.iter().map(|(_, ty)| ty.clone()).collect();

    for req in [
        serde_json::json!({"filter": filter.clone()}),
        serde_json::json!({"selectList": [], "filter": filter.clone()}),
        serde_json::json!({"selectList": null, "filter": filter.clone()}),
        serde_json::json!({"selectList": "*", "filter": filter.clone()}),
    ] {
        let (proj, types) = referenced_column_projection(&req, &all_cols);
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

/// Scenario: A real select list beside a filter still narrows (#160)
#[test]
fn referenced_column_projection_narrows_with_a_real_select_list() {
    let all_cols = vec![
        ("GK".to_string(), "VARCHAR(10)".to_string()),
        ("FCOL".to_string(), "DECIMAL(18,0)".to_string()),
        ("IRRELEVANT_COL".to_string(), "VARCHAR(10)".to_string()),
    ];
    let req = serde_json::json!({
        "selectList": [{"type": "column", "name": "GK", "tableName": "T"}],
        "filter": {
            "type": "predicate_equal",
            "left": {"type": "column", "name": "FCOL", "tableName": "T"},
            "right": {"type": "literal_exactnumeric", "value": 5},
        },
    });

    let (proj, types) = referenced_column_projection(&req, &all_cols);
    assert_eq!(
        proj,
        vec![
            ProjectionItem::Column("GK".to_string()),
            ProjectionItem::Column("FCOL".to_string()),
        ],
        "select-list ∪ filter columns only — the unreferenced column stays out"
    );
    assert_eq!(
        types,
        vec!["VARCHAR(10)".to_string(), "DECIMAL(18,0)".to_string()]
    );
}

/// Scenario: A request naming no source column falls back to the first column only
#[test]
fn referenced_column_projection_falls_back_to_first_column() {
    let all_cols = vec![
        ("A".to_string(), "DECIMAL(18,0)".to_string()),
        ("B".to_string(), "VARCHAR(10)".to_string()),
    ];
    let req = serde_json::json!({
        "selectList": [{"type": "literal_exactnumeric", "value": 1}],
        "filter": null,
        "having": null,
    });
    let (proj, types) = referenced_column_projection(&req, &all_cols);
    assert_eq!(
        proj,
        vec![ProjectionItem::Column("A".to_string())],
        "no referenced source column ⇒ exactly one column, the first of all_cols"
    );
    assert_eq!(types, vec!["DECIMAL(18,0)".to_string()]);
}

/// Scenario: An untranslatable N-scan select item is a hard error (#196)
#[test]
fn n_scan_join_untranslatable_select_item_is_hard_error() {
    let unknown = serde_json::json!({"type": "no_such_node_type_in_either_dialect"});
    assert!(
        render_expression_safe(&unknown).is_none()
            && render_expression_exasol_safe(&unknown).is_none(),
        "the fixture node must be untranslatable under BOTH dialects"
    );

    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["selectList"] = serde_json::json!([
        {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
        unknown,
    ]);
    let pushdown_req = pd(&request);
    let detected = detected_join(&request);
    assert!(
        render_broadcast_join(&request, &pushdown_req, &detected)
            .expect("the broadcast decline is never an error")
            .is_none(),
        "the widened projection declines broadcast, so the request reaches the \
         N-scan fallback"
    );

    let sides = vec![
        resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
        resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
    ];
    let err = build_n_scan_join_sql(
        &request,
        &pushdown_req,
        &detected,
        &sides,
        &two_scan_tuning(),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    )
    .expect_err("an untranslatable select-list item must be a hard error");
    assert!(
        matches!(&err, UdfError::User(msg) if msg.contains("select-list item could not be rendered")),
        "the refusal must come from the select-list render site: {err}"
    );
}

/// Scenario: An untranslatable single-table wrapper select item is a hard error (#196)
#[test]
fn qualified_single_table_untranslatable_select_item_is_hard_error() {
    let request = serde_json::json!({
        "involvedTables": [{"name": "T", "columns": [
            {"name": "ID", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]}],
    });
    let pushdown_req = serde_json::json!({
        "selectList": [
            {"type": "column", "name": "ID", "tableName": "T"},
            {"type": "no_such_node_type_in_either_dialect"},
        ],
    });
    let col_types = vec![("ID".to_string(), "DECIMAL(20,0)".to_string())];
    let base = CommonScanSpec {
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    };
    let shards = vec![vec![FileEntry::new("s3://w/f-0.parquet", 10)]];

    let err = qualified_single_table_fallback_pushdown(
        &request,
        &pushdown_req,
        &base,
        None,
        &shards,
        &col_types,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        None,
    )
    .expect_err("an untranslatable select-list item must be a hard error");
    assert!(
        matches!(&err, UdfError::User(msg) if msg.contains("select-list item could not be rendered")),
        "the refusal must come from the shared select-list render site: {err}"
    );
}

/// Scenario: A projected TSTZ literal converts into the session zone (#218)
#[test]
fn qualified_single_table_wrapper_projects_tstz_literal_converted_to_session_zone() {
    let request = serde_json::json!({
        "involvedTables": [{"name": "T", "columns": [
            {"name": "ID", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]}],
    });
    let pushdown_req = serde_json::json!({
        "selectList": [
            {"type": "literal_timestamputc", "value": "2024-03-01 09:00:00"},
        ],
        "selectListDataTypes": [
            {"type": "timestamp", "withLocalTimeZone": true},
        ],
    });
    let col_types = vec![("ID".to_string(), "DECIMAL(20,0)".to_string())];
    let base = CommonScanSpec {
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    };
    let shards = vec![vec![FileEntry::new("s3://w/f-0.parquet", 10)]];

    let resp = qualified_single_table_fallback_pushdown(
        &request,
        &pushdown_req,
        &base,
        None,
        &shards,
        &col_types,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        None,
    )
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

fn declined_filter_request() -> Json {
    serde_json::json!({
        "involvedTables": [{"name": "T", "columns": [
            {"name": "ID", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
            {"name": "C_TS", "dataType": {"type": "timestamp"}},
        ]}],
    })
}

/// `filter` stays `None`: on the decline route the predicate lives only in the
/// wrapper's `WHERE`.
fn declined_filter_fan_out_spec() -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            projection: vec![
                ProjectionItem::Column("ID".to_string()),
                ProjectionItem::Column("C_TS".to_string()),
            ],
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![],
    }
}

fn declined_filter_proj_types() -> Vec<String> {
    vec!["DECIMAL(20,0)".to_string(), "TIMESTAMP".to_string()]
}

fn declined_filter_wrapper_sql(
    pushdown_req: &Json,
    declined: Option<&Json>,
) -> Result<String, UdfError> {
    let spec = declined_filter_fan_out_spec();
    let proj_types = declined_filter_proj_types();
    let fan_out = FanOutProjection::new(&spec, &proj_types)?;
    build_qualified_single_table_fallback_sql(
        &declined_filter_request(),
        pushdown_req,
        &fan_out,
        &[vec![("s3://w/f-0.parquet".to_string(), 10u64)]],
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        declined,
    )
}

/// Scenario: Misaligned proj_types fail FanOutProjection construction naming both lengths
#[test]
fn fan_out_projection_rejects_misaligned_proj_types() {
    let spec = declined_filter_fan_out_spec();
    let types = vec!["DECIMAL(20,0)".to_string()];

    let err = FanOutProjection::new(&spec, &types)
        .expect_err("a proj_types list shorter than the projection must fail construction");
    let text = err.to_string();
    assert!(
        text.contains('2') && text.contains('1'),
        "the error must name both lengths: {text}"
    );
}

/// DataFusion declines this on arity while Exasol renders it (live-verified).
fn second_arity_predicate() -> Json {
    serde_json::json!({
        "type": "predicate_greater",
        "left": {"type": "function_scalar", "name": "SECOND", "arguments": [
            {"type": "column", "name": "C_TS", "tableName": "T"},
            {"type": "literal_exactnumeric", "value": 3},
        ]},
        "right": {"type": "literal_exactnumeric", "value": 1},
    })
}

/// Scenario: A DataFusion-declined WHERE self-applies in Exasol dialect before ORDER BY/LIMIT
#[test]
fn single_table_wrapper_renders_declined_predicate_in_exasol_dialect() {
    let declined = second_arity_predicate();
    assert!(
        render_expression_safe(&declined).is_none()
            && render_expression_exasol_safe(&declined).is_some(),
        "fixture precondition: SECOND(C_TS, 3) must decline for DataFusion and \
         render for Exasol"
    );
    let pushdown_req = serde_json::json!({
        "selectList": [{"type": "column", "name": "ID", "tableName": "T"}],
        "filter": declined.clone(),
        "orderBy": [{
            "type": "order_by_element",
            "expression": {"type": "column", "name": "ID", "tableName": "T"},
            "isAscending": true,
            "nullsLast": true,
        }],
        "limit": {"numElements": 5},
    });

    let sql = declined_filter_wrapper_sql(&pushdown_req, Some(&declined))
        .expect("a declined predicate that renders for Exasol must build the wrapper");

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

/// Scenario: A self-applied TSTZ literal predicate renders via CONVERT_TZ, matching native
#[test]
fn declined_filter_self_apply_renders_tstz_literal_via_convert_tz() {
    let declined = serde_json::json!({
        "type": "predicate_greater",
        "left": {"type": "column", "name": "C_TS", "tableName": "T"},
        "right": {"type": "literal_timestamputc", "value": "2024-03-01 09:00:00"},
    });
    assert!(
        render_expression_exasol_safe(&declined).is_some(),
        "fixture precondition: the predicate must render for Exasol"
    );
    let pushdown_req = serde_json::json!({
        "selectList": [{"type": "column", "name": "ID", "tableName": "T"}],
    });

    let sql = declined_filter_wrapper_sql(&pushdown_req, Some(&declined))
        .expect("the predicate must self-apply");

    assert!(
        sql.contains(
            r#"WHERE ("LHS_T0"."C_TS" > CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 09:00:00', 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE))"#
        ),
        "the self-applied predicate must compare against a genuine TSTZ \
         expression, so Exasol applies its own TIMESTAMP-vs-TSTZ coercion \
         rule to C_TS exactly as it would natively: {sql}"
    );
}

/// Scenario: A trivially-true declined predicate emits no WHERE
#[test]
fn single_table_wrapper_trivially_true_declined_predicate_emits_no_where() {
    let trivially_true = serde_json::json!({"type": "literal_bool", "value": true});
    let pushdown_req = serde_json::json!({
        "selectList": [{"type": "column", "name": "ID", "tableName": "T"}],
    });

    let sql = declined_filter_wrapper_sql(&pushdown_req, Some(&trivially_true))
        .expect("a trivially-true predicate must not fail the wrapper");

    assert!(
        !sql.contains(" WHERE "),
        "a trivially-true predicate must emit no WHERE clause at all: {sql}"
    );
}

/// Scenario: A declined predicate rendering in neither dialect is a hard error
#[test]
fn single_table_wrapper_errors_when_declined_predicate_renders_in_neither_dialect() {
    let unrenderable = serde_json::json!({"type": "no_such_node_type_in_either_dialect"});
    let pushdown_req = serde_json::json!({
        "selectList": [{"type": "column", "name": "ID", "tableName": "T"}],
    });

    let err = declined_filter_wrapper_sql(&pushdown_req, Some(&unrenderable))
        .expect_err("a predicate applicable nowhere must fail the query");

    assert!(
        matches!(&err, UdfError::User(msg) if msg.contains("neither dialect")),
        "the refusal must name the both-dialects decline: {err}"
    );
    assert!(
        matches!(&err, UdfError::User(msg) if msg.contains("no_such_node_type_in_either_dialect")),
        "the refusal must name the offending predicate tree: {err}"
    );
}

/// Scenario: The outer wrapper renders a qualified expression sort key before LIMIT (#198)
#[test]
fn outer_wrapper_renders_qualified_expression_sort_key() {
    let legs = single_scan_legs("T");
    let cols_per_leg = vec![vec![
        ("ID".to_string(), "DECIMAL(20,0)".to_string()),
        ("NAME".to_string(), "VARCHAR(100)".to_string()),
    ]];
    let pushdown_req = serde_json::json!({
        "selectList": [{"type": "column", "name": "ID", "tableName": "T"}],
        "groupBy": [{"type": "column", "name": "ID", "tableName": "T"}],
        "orderBy": [{
            "type": "order_by_element",
            "expression": {"type": "function_scalar", "name": "UPPER", "arguments": [
                {"type": "column", "name": "NAME", "tableName": "T"}]},
            "isAscending": false,
            "nullsLast": true
        }],
        "limit": {"numElements": 4}
    });

    let clauses = outer_wrapper_clauses(&pushdown_req, &legs, &cols_per_leg)
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

fn seam_trailing(pushdown_req: &Json, legs: &JoinLegs) -> Result<String, UdfError> {
    let cols = vec![
        ("ID".to_string(), "DECIMAL(20,0)".to_string()),
        ("NAME".to_string(), "VARCHAR(100)".to_string()),
        ("SCORE".to_string(), "DOUBLE PRECISION".to_string()),
    ];
    let cols_per_leg = vec![cols; legs.leg_count()];
    outer_wrapper_clauses(pushdown_req, legs, &cols_per_leg).map(|clauses| clauses.trailing)
}

/// The four live-captured offset-carrying shapes reaching this seam (#191 capture rows
/// 8-11), each with the binding production builds for it; each carries an `orderBy`.
fn offset_carrying_seam_fixtures() -> Vec<(&'static str, Json, JoinLegs, &'static str)> {
    let single_scan = single_scan_legs("EVENTS");
    let self_join = legs_from_leaves(vec![
        JoinLeaf {
            table_name: "EVENTS".to_string(),
            table_alias: Some("A".to_string()),
            table_identifier: "lh.events".to_string(),
        },
        JoinLeaf {
            table_name: "EVENTS".to_string(),
            table_alias: Some("B".to_string()),
            table_identifier: "lh.events".to_string(),
        },
    ]);

    let id = serde_json::json!({"type": "column", "name": "ID", "tableName": "EVENTS"});
    let mod_key = serde_json::json!({
        "type": "function_scalar", "name": "MOD",
        "arguments": [id.clone(), {"type": "literal_exactnumeric", "value": 4}]
    });
    let ascending = |expr: &Json| {
        serde_json::json!({
            "type": "order_by_element", "expression": expr,
            "isAscending": true, "nullsLast": false
        })
    };
    let count_distinct_id = serde_json::json!({
        "type": "function_aggregate", "name": "COUNT",
        "arguments": [id.clone()], "distinct": true
    });
    let count_star = serde_json::json!({
        "type": "function_aggregate", "name": "COUNT", "arguments": [], "distinct": false
    });
    let id_a = serde_json::json!({"type": "column", "name": "ID", "tableName": "EVENTS", "tableAlias": "A"});
    let score_a = serde_json::json!({"type": "column", "name": "SCORE", "tableName": "EVENTS", "tableAlias": "A"});
    let name_a = serde_json::json!({"type": "column", "name": "NAME", "tableName": "EVENTS", "tableAlias": "A"});

    vec![
        (
            "row 8: grouped COUNT(DISTINCT) qualified wrapper",
            serde_json::json!({
                "aggregationType": "group_by",
                "selectList": [mod_key.clone(), count_distinct_id],
                "groupBy": [mod_key.clone()],
                "orderBy": [ascending(&mod_key)],
                "limit": {"numElements": 2, "offset": 1},
            }),
            single_scan.clone(),
            r#" ORDER BY MOD("LHS_T0"."ID", 4) ASC NULLS FIRST LIMIT 2 OFFSET 1"#,
        ),
        (
            "row 9: self-join, ORDER BY ordinal over a projected column",
            serde_json::json!({
                "selectList": [id_a.clone(), score_a],
                "orderBy": [ascending(&id_a)],
                "limit": {"numElements": 5, "offset": 2},
            }),
            self_join.clone(),
            r#" ORDER BY "LHS_T0"."ID" ASC NULLS FIRST LIMIT 5 OFFSET 2"#,
        ),
        (
            "row 10: self-join, sort key outside the select list",
            serde_json::json!({
                "selectList": [name_a],
                "orderBy": [ascending(&id_a)],
                "limit": {"numElements": 5, "offset": 2},
            }),
            self_join.clone(),
            r#" ORDER BY "LHS_T0"."ID" ASC NULLS FIRST LIMIT 5 OFFSET 2"#,
        ),
        (
            "row 11: self-join + GROUP BY",
            serde_json::json!({
                "aggregationType": "group_by",
                "selectList": [id_a.clone(), count_star],
                "groupBy": [id_a.clone()],
                "orderBy": [ascending(&id_a)],
                "limit": {"numElements": 5, "offset": 2},
            }),
            self_join,
            r#" ORDER BY "LHS_T0"."ID" ASC NULLS FIRST LIMIT 5 OFFSET 2"#,
        ),
    ]
}

/// Scenario: The qualified wrapper renders LIMIT … OFFSET after ORDER BY (#191)
#[test]
fn qualified_wrapper_renders_limit_offset() {
    for (shape, pushdown_req, legs, expected_tail) in offset_carrying_seam_fixtures() {
        let trailing = seam_trailing(&pushdown_req, &legs)
            .unwrap_or_else(|e| panic!("{shape} must render, not decline: {e}"));
        assert!(
            trailing.ends_with(expected_tail),
            "{shape}: trailing must end with `{expected_tail}`, got `{trailing}`"
        );
    }
}

/// Scenario: The qualified wrapper never renders OFFSET without a preceding ORDER BY
#[test]
fn qualified_wrapper_never_renders_offset_without_order_by() {
    let mut cases: Vec<(&str, Json, JoinLegs)> = offset_carrying_seam_fixtures()
        .into_iter()
        .map(|(shape, req, legs, _)| (shape, req, legs))
        .collect();
    cases.push((
        "control: limit, no orderBy, no offset",
        serde_json::json!({
            "selectList": [{"type": "column", "name": "ID", "tableName": "EVENTS"}],
            "limit": {"numElements": 5},
        }),
        single_scan_legs("EVENTS"),
    ));

    for (shape, pushdown_req, legs) in cases {
        let trailing = seam_trailing(&pushdown_req, &legs)
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

/// Scenario: A request with no orderBy and no offset renders a byte-identical LIMIT (#191)
#[test]
fn qualified_wrapper_zero_offset_renders_byte_identical_limit() {
    let legs = single_scan_legs("EVENTS");
    let mut pushdown_req = serde_json::json!({
        "selectList": [{"type": "column", "name": "ID", "tableName": "EVENTS"}],
        "limit": {"numElements": 5},
    });
    let bare = seam_trailing(&pushdown_req, &legs).expect("a plain limited request must render");
    assert_eq!(bare, " LIMIT 5");

    // Exasol normalises `OFFSET 0` away; an explicit zero must still render identically.
    pushdown_req["limit"] = serde_json::json!({"numElements": 5, "offset": 0});
    let zero = seam_trailing(&pushdown_req, &legs).expect("an explicit zero offset must render");
    assert_eq!(zero, bare);
}
