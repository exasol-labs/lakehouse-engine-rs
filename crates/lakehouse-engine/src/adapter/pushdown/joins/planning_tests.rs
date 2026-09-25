use super::super::render_broadcast_join;
use super::super::tests::{
    detected_join, equi_condition, join_request, nq3_join_request, resolved_side,
    self_join_request, three_table_join_request,
};
use super::*;
use crate::adapter::pushdown::test_support::*;

/// Scenario: A two-table inner equi-join is detected with both identifiers from TABLE_MAP
#[test]
fn genuine_inner_equi_join_is_detected_with_both_idents() {
    let request = join_request(Json::Null, equi_condition());
    let pushdown_req = pd(&request);

    let shape = detect_join(&request, &pushdown_req).expect("TABLE_MAP has both tables");
    match shape {
        JoinShape::Join(join) => {
            assert_eq!(join.tables.len(), 2);
            assert_eq!(join.tables[0].table_name, "CUSTOMER");
            assert_eq!(join.tables[1].table_name, "ORDERS");
            assert_eq!(join.tables[0].table_identifier, "lh.customer");
            assert_eq!(join.tables[1].table_identifier, "lh.orders");
            assert_eq!(join.conditions, vec![equi_condition()]);
        }
        other => panic!("expected Join, got {other:?}"),
    }
}

/// Scenario: A request with no from field is not a join
#[test]
fn plain_single_table_request_is_not_a_join() {
    let request = nq4_request();
    let shape = detect_join(&request, &pd(&request)).expect("not a join, no TABLE_MAP lookup");
    assert_eq!(shape, JoinShape::NotAJoin);
}

/// Scenario: A from clause that is a plain table reference is not a join
#[test]
fn from_table_node_is_not_a_join() {
    let mut request = nq4_request();
    request["pushdownRequest"]["from"] = serde_json::json!({"name": "LINEITEM", "type": "table"});
    let shape = detect_join(&request, &pd(&request)).expect("not a join");
    assert_eq!(shape, JoinShape::NotAJoin);
}

/// Scenario: Outer joins are ineligible
#[test]
fn outer_join_is_ineligible() {
    for outer in ["left_outer", "right_outer", "full_outer"] {
        let request = join_request(serde_json::json!({"join_type": outer}), equi_condition());
        let shape = detect_join(&request, &pd(&request)).expect("shape decline, no Err");
        assert_eq!(
            shape,
            JoinShape::Ineligible(IneligibleJoinReason::NotInnerJoinType),
            "join_type '{outer}' must be ineligible, not broadcast-eligible"
        );
    }
}

/// Scenario: A non-equi two-table inner join is served by the unified fallback
#[test]
fn non_equi_two_table_join_is_served_by_unified_fallback() {
    let condition = serde_json::json!({
        "type": "predicate_less",
        "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
        "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"},
    });
    let request = join_request(Json::Null, condition.clone());
    match detect_join(&request, &pd(&request)).expect("served, not declined") {
        JoinShape::Join(join) => {
            assert_eq!(join.tables.len(), 2);
            assert_eq!(join.conditions, vec![condition]);
        }
        other => panic!("expected Join (unified fallback), got {other:?}"),
    }
}

/// Scenario: A three-table inner join is the unified Join shape
#[test]
fn three_table_inner_join_is_unified_join() {
    let request = three_table_join_request();
    let shape = detect_join(&request, &pd(&request)).expect("all leaves are in TABLE_MAP");
    match shape {
        JoinShape::Join(join) => {
            let names: Vec<&str> = join.tables.iter().map(|t| t.table_name.as_str()).collect();
            assert_eq!(names, ["CUSTOMER", "ORDERS", "LINEITEM"]);
            let idents: Vec<&str> = join
                .tables
                .iter()
                .map(|t| t.table_identifier.as_str())
                .collect();
            assert_eq!(idents, ["lh.customer", "lh.orders", "lh.lineitem"]);
            assert_eq!(join.conditions.len(), 2, "N-1 conditions for N=3 tables");
        }
        other => panic!("expected Join, got {other:?}"),
    }
}

/// Scenario: A non-inner join node anywhere in the tree is ineligible
#[test]
fn non_inner_node_in_join_tree_is_ineligible() {
    let mut request = three_table_join_request();
    request["pushdownRequest"]["from"]["left"]["join_type"] = serde_json::json!("left_outer");
    let shape = detect_join(&request, &pd(&request)).expect("shape decline, no Err");
    assert_eq!(
        shape,
        JoinShape::Ineligible(IneligibleJoinReason::NotInnerJoinType)
    );
}

/// Scenario: A multi-table leaf absent from TABLE_MAP is a hard error
#[test]
fn multi_table_leaf_absent_from_table_map_is_err() {
    let mut request = three_table_join_request();
    request["schemaMetadataInfo"]["adapterNotes"] = Json::String(
        serde_json::json!({"TABLE_MAP": {"CUSTOMER": "lh.customer", "ORDERS": "lh.orders"}})
            .to_string(),
    );
    let err = detect_join(&request, &pd(&request))
        .expect_err("LINEITEM is absent from TABLE_MAP: must be Err, not a decline");
    assert!(
        err.to_string().contains("LINEITEM"),
        "error must name the unmapped table: {err}"
    );
}

/// Scenario: A four-table inner join is the unified Join shape
#[test]
fn four_table_inner_join_is_unified_join() {
    let request = nq3_join_request();
    let shape = detect_join(&request, &pd(&request)).expect("all leaves are in TABLE_MAP");
    match shape {
        JoinShape::Join(join) => {
            let names: Vec<&str> = join.tables.iter().map(|t| t.table_name.as_str()).collect();
            assert_eq!(names, ["PART", "PARTSUPP", "SUPPLIER", "NATION"]);
            let idents: Vec<&str> = join
                .tables
                .iter()
                .map(|t| t.table_identifier.as_str())
                .collect();
            assert_eq!(
                idents,
                ["lh.part", "lh.partsupp", "lh.supplier", "lh.nation"]
            );
            assert_eq!(join.conditions.len(), 3, "N-1 conditions for N=4 tables");
        }
        other => panic!("expected Join, got {other:?}"),
    }
}

/// Scenario: Detection follows the from tree, not the involvedTables count
#[test]
fn detect_join_follows_from_tree_not_involved_tables_count() {
    let mut request = join_request(Json::Null, equi_condition());
    request["involvedTables"].as_array_mut().unwrap().push(serde_json::json!({
        "name": "NATION",
        "columns": [{"name": "N_NATIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}],
    }));
    match detect_join(&request, &pd(&request)).expect("tree-driven, no decline") {
        JoinShape::Join(join) => {
            let names: Vec<&str> = join.tables.iter().map(|t| t.table_name.as_str()).collect();
            assert_eq!(names, ["CUSTOMER", "ORDERS"], "only the from-tree leaves");
        }
        other => panic!("expected Join over the two from-tree tables, got {other:?}"),
    }
}

/// Scenario: A join whose table is absent from TABLE_MAP is a hard error, not a decline
#[test]
fn join_with_unmapped_table_is_an_error() {
    let mut request = join_request(Json::Null, equi_condition());
    request["schemaMetadataInfo"]["adapterNotes"] =
        Json::String(serde_json::json!({"TABLE_MAP": {"CUSTOMER": "lh.customer"}}).to_string());
    let err = detect_join(&request, &pd(&request))
        .expect_err("ORDERS is absent from TABLE_MAP: must be Err, not a decline");
    assert!(
        err.to_string().contains("ORDERS"),
        "error must name the unmapped table: {err}"
    );
}

fn leaf_keys(join: &DetectedJoin) -> Vec<(&str, Option<&str>)> {
    join.tables
        .iter()
        .map(|leaf| (leaf.table_name.as_str(), leaf.table_alias.as_deref()))
        .collect()
}

/// Scenario: A two-leg self-join's leaves carry their own aliases
#[test]
fn two_leg_self_join_leaves_carry_their_own_aliases() {
    let request = self_join_request(&[Some("A"), Some("B")]);
    let join = detected_join(&request);

    assert_eq!(
        leaf_keys(&join),
        [("FACT_ORDERS", Some("A")), ("FACT_ORDERS", Some("B"))]
    );
    assert_eq!(
        join.tables[0].table_identifier, join.tables[1].table_identifier,
        "both occurrences resolve to the same catalog table"
    );
}

/// Scenario: An unaliased self-join leg collects None and stays distinct
#[test]
fn unaliased_self_join_leg_collects_none_and_stays_distinct() {
    let request = self_join_request(&[None, Some("B")]);
    let join = detected_join(&request);

    let keys = leaf_keys(&join);
    assert_eq!(keys, [("FACT_ORDERS", None), ("FACT_ORDERS", Some("B"))]);
    assert_ne!(keys[0], keys[1], "the two legs must remain distinguishable");
}

/// Scenario: A mixed-case leaf alias is retained verbatim
#[test]
fn mixed_case_leaf_alias_is_retained_verbatim() {
    let request = self_join_request(&[Some("myAlias"), Some("B")]);
    let join = detected_join(&request);

    assert_eq!(join.tables[0].table_alias.as_deref(), Some("myAlias"));
}

/// Scenario: A three-leg left-deep self-join collects one aliased leaf per occurrence
#[test]
fn three_leg_left_deep_self_join_collects_one_aliased_leaf_per_occurrence() {
    let request = self_join_request(&[Some("A"), Some("B"), Some("C")]);
    let join = detected_join(&request);

    assert_eq!(
        leaf_keys(&join),
        [
            ("FACT_ORDERS", Some("A")),
            ("FACT_ORDERS", Some("B")),
            ("FACT_ORDERS", Some("C")),
        ]
    );
    assert_eq!(join.conditions.len(), 2, "N-1 conditions for N=3 legs");
}

/// Scenario: An unaliased two-table join's leaves carry no alias
#[test]
fn unaliased_two_table_join_leaves_carry_no_alias() {
    let request = join_request(Json::Null, equi_condition());
    let join = detected_join(&request);

    assert_eq!(leaf_keys(&join), [("CUSTOMER", None), ("ORDERS", None)]);
}

const BROADCAST_MAX: u64 = 134_217_728;

/// Scenario: A side's total bytes is the saturating sum of its file sizes
#[test]
fn resolved_side_sums_file_bytes_saturating() {
    assert_eq!(
        resolved_side("ORDERS", vec![("a", 100), ("b", 250), ("c", 4)]).total_bytes,
        354
    );
    assert_eq!(resolved_side("EMPTY", vec![]).total_bytes, 0);
    assert_eq!(
        resolved_side("HUGE", vec![("x", u64::MAX), ("y", 1)]).total_bytes,
        u64::MAX
    );
}

/// Scenario: The smaller left side is the dimension and carries its resolved payload
#[test]
fn dimension_is_left_when_left_side_is_smaller() {
    let customer = resolved_side("CUSTOMER", vec![("c1", 1_000)]);
    let orders = resolved_side("ORDERS", vec![("o1", 50_000), ("o2", 50_000)]);
    let sides = select_broadcast_sides(customer, orders, BROADCAST_MAX);

    assert_eq!(sides.dimension.table_name, "CUSTOMER");
    assert_eq!(sides.fact.table_name, "ORDERS");
    assert_eq!(sides.dimension.total_bytes, 1_000);
    assert_eq!(sides.fact.total_bytes, 100_000);
    assert!(
        sides.broadcast_eligible,
        "1000 bytes is well under the 128 MiB threshold"
    );
    assert_eq!(sides.dimension.table_identifier, "lh.customer");
    assert_eq!(sides.fact.table_identifier, "lh.orders");
    assert_eq!(sides.dimension.files, vec![FileEntry::new("c1", 1_000)]);
    assert_eq!(sides.dimension.table_root, "s3://warehouse/lh/customer");
    assert_eq!(sides.dimension.logical_schema.len(), 1);
    assert_eq!(sides.dimension.effective_storage, sample_storage());
}

/// Scenario: Selection is by byte size, not FROM-clause position
#[test]
fn dimension_is_right_when_right_side_is_smaller() {
    let orders = resolved_side("ORDERS", vec![("o1", 50_000), ("o2", 50_000)]);
    let customer = resolved_side("CUSTOMER", vec![("c1", 1_000)]);
    let sides = select_broadcast_sides(orders, customer, BROADCAST_MAX);

    assert_eq!(sides.dimension.table_name, "CUSTOMER");
    assert_eq!(sides.fact.table_name, "ORDERS");
    assert_eq!(sides.dimension.total_bytes, 1_000);
    assert!(sides.broadcast_eligible);
}

/// Scenario: A dimension over the threshold is not broadcast-eligible
#[test]
fn dimension_over_threshold_is_not_broadcast_eligible() {
    let part = resolved_side("PART", vec![("p1", 200)]);
    let lineitem = resolved_side("LINEITEM", vec![("l1", 900)]);
    let sides = select_broadcast_sides(part, lineitem, 100);

    assert_eq!(
        sides.dimension.table_name, "PART",
        "PART (200 bytes) is the smaller side"
    );
    assert_eq!(sides.fact.table_name, "LINEITEM");
    assert!(
        !sides.broadcast_eligible,
        "dimension total 200 > threshold 100: not broadcast-eligible"
    );
}

/// Scenario: The broadcast threshold boundary is inclusive
#[test]
fn threshold_boundary_is_inclusive() {
    let at = select_broadcast_sides(
        resolved_side("DIM", vec![("d", 100)]),
        resolved_side("FACT", vec![("f", 10_000)]),
        100,
    );
    assert!(
        at.broadcast_eligible,
        "dimension == threshold must be eligible"
    );

    let over = select_broadcast_sides(
        resolved_side("DIM", vec![("d", 101)]),
        resolved_side("FACT", vec![("f", 10_000)]),
        100,
    );
    assert!(
        !over.broadcast_eligible,
        "dimension == threshold + 1 must not be eligible"
    );
}

/// Scenario: An empty side is the eligible dimension
#[test]
fn empty_side_is_the_eligible_dimension() {
    let empty = resolved_side("EMPTYDIM", vec![]);
    let fact = resolved_side("FACT", vec![("f", 5_000)]);
    let sides = select_broadcast_sides(empty, fact, BROADCAST_MAX);

    assert_eq!(sides.dimension.table_name, "EMPTYDIM");
    assert_eq!(sides.dimension.total_bytes, 0);
    assert!(sides.dimension.files.is_empty());
    assert!(sides.broadcast_eligible);
}

/// Scenario: An exact byte-size tie breaks to the first argument
#[test]
fn equal_size_tie_breaks_to_first_argument() {
    let a = resolved_side("SELF_A", vec![("s", 4_242)]);
    let b = resolved_side("SELF_B", vec![("s", 4_242)]);
    let sides = select_broadcast_sides(a, b, BROADCAST_MAX);

    assert_eq!(sides.dimension.table_name, "SELF_A");
    assert_eq!(sides.fact.table_name, "SELF_B");
    assert_eq!(sides.dimension.total_bytes, sides.fact.total_bytes);
}

/// Scenario: A self-join is never broadcast-eligible
#[test]
fn self_join_is_never_broadcast_eligible() {
    let request = self_join_request(&[Some("A"), Some("B")]);
    let join = detected_join(&request);

    let left = involved_table_columns(&request, &join.tables[0].table_name);
    let right = involved_table_columns(&request, &join.tables[1].table_name);
    assert!(
        !disjoint_schema_guard(&left, &right),
        "a self-join's two occurrences declare the same columns, so the guard must decline it"
    );

    let rendered = render_broadcast_join(&request, &pd(&request), &join)
        .expect("no column-metadata error for a well-formed self-join request");
    assert!(
        rendered.is_none(),
        "a self-join must never reach the broadcast path, only the clean N-scan fall-through"
    );
}
