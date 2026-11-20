use super::super::tests::{
    detected_join, equi_condition, join_request, nq3_join_request, resolved_side,
    self_join_request, three_table_join_request,
};
use super::*;
use crate::adapter::pushdown::test_support::*;

#[test]
fn genuine_inner_equi_join_is_detected_with_both_idents() {
    let request = join_request(Json::Null, equi_condition());
    let pushdown_req = pd(&request);

    let shape = detect_join(&request, &pushdown_req).expect("both legs carry an identifier note");
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

#[test]
fn plain_single_table_request_is_not_a_join() {
    let request = nq4_request();
    let shape = detect_join(&request, &pd(&request)).expect("not a join, no identifier lookup");
    assert_eq!(shape, JoinShape::NotAJoin);
}

/// `ON c = o AND c < o` arrives as one `predicate_and` condition, and detection keeps it whole:
/// moving `c < o` to WHERE is `plan_join`'s step, not detection's.
#[test]
fn a_non_equi_conjunct_stays_in_the_one_join_condition() {
    let condition = and([
        equi_condition(),
        compare(
            "predicate_less",
            table_col("CUSTOMER", "C_CUSTKEY"),
            table_col("ORDERS", "O_CUSTKEY"),
        ),
    ]);
    let request = join_request(Json::Null, condition.clone());
    match detect_join(&request, &pd(&request)).expect("a join with a non-equi conjunct is detected")
    {
        JoinShape::Join(join) => {
            assert_eq!(join.tables.len(), 2);
            assert_eq!(join.conditions, vec![condition]);
        }
        other => panic!("expected Join, got {other:?}"),
    }
}

#[test]
fn three_table_inner_join_is_unified_join() {
    let request = three_table_join_request();
    let shape =
        detect_join(&request, &pd(&request)).expect("every leaf carries an identifier note");
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

/// Scenario: Each join leg resolves its catalog identifier from its own identifier note
#[test]
fn each_leg_reads_its_identifier_from_its_own_involved_table() {
    let mut request = three_table_join_request();
    request["involvedTables"].as_array_mut().unwrap().reverse();
    let request = with_identifier_notes(
        request,
        &[
            ("CUSTOMER", "a.customer"),
            ("ORDERS", "b.orders"),
            ("LINEITEM", "c.lineitem"),
        ],
    );

    let join = detected_join(&request);

    let idents: Vec<(&str, &str)> = join
        .tables
        .iter()
        .map(|leaf| (leaf.table_name.as_str(), leaf.table_identifier.as_str()))
        .collect();
    assert_eq!(
        idents,
        [
            ("CUSTOMER", "a.customer"),
            ("ORDERS", "b.orders"),
            ("LINEITEM", "c.lineitem")
        ]
    );
}

/// Scenario: Each join leg resolves its catalog identifier from its own identifier note
#[test]
fn a_leg_without_an_identifier_note_is_err() {
    let mut request = three_table_join_request();
    request["involvedTables"][2]
        .as_object_mut()
        .unwrap()
        .remove("adapterNotes");
    let err = detect_join(&request, &request["pushdownRequest"])
        .expect_err("LINEITEM has no note: must be Err, not a decline");
    let message = err.to_string();
    assert!(
        message.contains("LINEITEM") && message.contains("ALTER VIRTUAL SCHEMA ... REFRESH"),
        "error must name the table and the refresh: {message}"
    );
}

#[test]
fn four_table_inner_join_is_unified_join() {
    let request = nq3_join_request();
    let shape =
        detect_join(&request, &pd(&request)).expect("every leaf carries an identifier note");
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

fn leaf_keys(join: &DetectedJoin) -> Vec<(&str, Option<&str>)> {
    join.tables
        .iter()
        .map(|leaf| (leaf.table_name.as_str(), leaf.table_alias.as_deref()))
        .collect()
}

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

#[test]
fn unaliased_self_join_leg_collects_none_and_stays_distinct() {
    let request = self_join_request(&[None, Some("B")]);
    let join = detected_join(&request);

    let keys = leaf_keys(&join);
    assert_eq!(keys, [("FACT_ORDERS", None), ("FACT_ORDERS", Some("B"))]);
    assert_ne!(keys[0], keys[1], "the two legs must remain distinguishable");
}

#[test]
fn mixed_case_leaf_alias_is_retained_verbatim() {
    let request = self_join_request(&[Some("myAlias"), Some("B")]);
    let join = detected_join(&request);

    assert_eq!(join.tables[0].table_alias.as_deref(), Some("myAlias"));
}

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

#[test]
fn unaliased_two_table_join_leaves_carry_no_alias() {
    let request = join_request(Json::Null, equi_condition());
    let join = detected_join(&request);

    assert_eq!(leaf_keys(&join), [("CUSTOMER", None), ("ORDERS", None)]);
}

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
