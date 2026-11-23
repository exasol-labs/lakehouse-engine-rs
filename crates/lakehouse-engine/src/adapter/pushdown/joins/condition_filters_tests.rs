use super::super::attribution::JoinLegs;
use super::super::planning::{DetectedJoin, JoinLeaf};
use super::*;
use crate::adapter::pushdown::test_support::{
    aliased_col, and, compare, num_lit, or, predicate_equal, scalar_fn, str_lit, table_col,
    table_ref, table_ref_as,
};

fn leaf(table_name: &str, table_alias: Option<&str>) -> JoinLeaf {
    JoinLeaf {
        table_name: table_name.to_string(),
        table_alias: table_alias.map(str::to_string),
        table_identifier: format!("lh.{}", table_name.to_ascii_lowercase()),
    }
}

fn legs(leaves: &[JoinLeaf]) -> JoinLegs {
    DetectedJoin {
        tables: leaves.to_vec(),
        conditions: Vec::new(),
    }
    .legs()
}

/// Legs 0, 1, 2: `A`, `B`, `C`.
fn three_legs() -> JoinLegs {
    legs(&[leaf("A", None), leaf("B", None), leaf("C", None)])
}

/// One table joined to itself as `X` (leg 0) and `Y` (leg 1).
fn self_join() -> JoinLegs {
    legs(&[leaf("T", Some("X")), leaf("T", Some("Y"))])
}

fn plus(left: Json, right: Json) -> Json {
    scalar_fn("ADD", [left, right])
}

fn join(join_type: &str, left: Json, right: Json, condition: Json) -> Json {
    serde_json::json!({
        "type": "join", "join_type": join_type, "left": left, "right": right, "condition": condition
    })
}

fn inner(left: Json, right: Json, condition: Json) -> Json {
    join("inner", left, right, condition)
}

fn request(from: Json, filter: Option<Json>) -> PushdownRequest {
    let mut json = serde_json::json!({
        "type": "select", "from": from, "selectListDataTypes": []
    });
    if let Some(filter) = filter {
        json["filter"] = filter;
    }
    PushdownRequest::parse(json).expect("a whole-row request")
}

fn ab_key() -> Json {
    predicate_equal(table_col("A", "K"), table_col("B", "K"))
}

fn bc_key() -> Json {
    predicate_equal(table_col("B", "K"), table_col("C", "K"))
}

fn a_filter() -> Json {
    predicate_equal(table_col("A", "R"), str_lit("x"))
}

fn b_filter() -> Json {
    compare("predicate_less", table_col("B", "N"), num_lit(5))
}

fn c_filter() -> Json {
    predicate_equal(table_col("C", "R"), str_lit("y"))
}

/// The equality cases both tests share: (case, legs, conjunct, join equality, join key).
fn equality_cases() -> Vec<(&'static str, JoinLegs, Json, bool, bool)> {
    vec![
        ("bare columns", three_legs(), ab_key(), true, true),
        (
            "bare columns written right to left",
            three_legs(),
            predicate_equal(table_col("B", "K"), table_col("A", "K")),
            true,
            true,
        ),
        (
            "an expression side",
            three_legs(),
            predicate_equal(table_col("A", "K"), plus(table_col("B", "K"), num_lit(1))),
            true,
            true,
        ),
        (
            "a same-leg equality",
            three_legs(),
            predicate_equal(table_col("A", "K"), table_col("A", "J")),
            false,
            false,
        ),
        (
            "a side reading two legs",
            three_legs(),
            predicate_equal(
                plus(table_col("A", "K"), table_col("B", "K")),
                table_col("C", "K"),
            ),
            true,
            false,
        ),
        (
            "sides sharing a leg",
            three_legs(),
            predicate_equal(
                plus(table_col("A", "K"), table_col("B", "K")),
                table_col("A", "J"),
            ),
            false,
            false,
        ),
        (
            "a side reading no column",
            three_legs(),
            predicate_equal(table_col("A", "K"), num_lit(1)),
            false,
            false,
        ),
        (
            "a self-join's two occurrences",
            self_join(),
            predicate_equal(aliased_col("T", "X", "K"), aliased_col("T", "Y", "K")),
            true,
            true,
        ),
        (
            "one occurrence of a self-join",
            self_join(),
            predicate_equal(aliased_col("T", "X", "K"), aliased_col("T", "X", "J")),
            false,
            false,
        ),
        (
            "a comparison across legs",
            three_legs(),
            compare("predicate_less", table_col("A", "K"), table_col("B", "K")),
            false,
            false,
        ),
    ]
}

#[test]
fn a_join_equality_reads_disjoint_non_empty_leg_sets() {
    for (case, legs, conjunct, expected, _) in equality_cases() {
        assert_eq!(is_join_equality(&conjunct, &legs), expected, "{case}");
    }
}

#[test]
fn a_join_key_reads_one_leg_on_each_side() {
    for (case, legs, conjunct, _, expected) in equality_cases() {
        assert_eq!(is_join_key(&conjunct, &legs), expected, "{case}");
    }
}

#[test]
fn a_condition_of_join_keys_only_is_a_broadcast_condition() {
    let legs = three_legs();
    let composite = and([
        ab_key(),
        predicate_equal(table_col("B", "J"), table_col("A", "J")),
    ]);
    let with_filter = and([ab_key(), a_filter()]);

    assert!(holds_only_join_keys(&ab_key(), &legs));
    assert!(holds_only_join_keys(&composite, &legs));
    assert!(!holds_only_join_keys(&with_filter, &legs));
}

#[test]
fn filters_of_a_nested_three_leg_tree_move_to_the_top_level_filter() {
    let from = inner(
        inner(table_ref("A"), table_ref("B"), and([ab_key(), a_filter()])),
        table_ref("C"),
        and([bc_key(), c_filter(), b_filter()]),
    );

    let moved = move_condition_filters(&request(from, None), &three_legs())
        .expect("both conditions hold a filter");

    let expected_from = inner(
        inner(table_ref("A"), table_ref("B"), ab_key()),
        table_ref("C"),
        bc_key(),
    );
    assert_eq!(clauses(&moved)["from"], expected_from);
    assert_eq!(
        clauses(&moved)["filter"],
        and([a_filter(), c_filter(), b_filter()])
    );
}

#[test]
fn moved_filters_are_conjoined_after_the_existing_filter() {
    let existing = and([b_filter(), or([a_filter(), c_filter()])]);
    let from = inner(table_ref("A"), table_ref("B"), and([ab_key(), a_filter()]));

    let moved = move_condition_filters(&request(from, Some(existing)), &three_legs())
        .expect("the condition holds a filter");

    assert_eq!(
        clauses(&moved)["filter"],
        and([b_filter(), or([a_filter(), c_filter()]), a_filter()])
    );
}

#[test]
fn several_join_equalities_stay_as_one_conjunction() {
    let second_key = predicate_equal(table_col("A", "J"), table_col("B", "J"));
    let from = inner(
        table_ref("A"),
        table_ref("B"),
        and([ab_key(), a_filter(), second_key.clone()]),
    );

    let moved = move_condition_filters(&request(from, None), &three_legs())
        .expect("the condition holds a filter");

    assert_eq!(
        clauses(&moved)["from"]["condition"],
        and([ab_key(), second_key])
    );
    assert_eq!(clauses(&moved)["filter"], a_filter());
}

#[test]
fn a_self_join_filter_keeps_its_occurrence_alias() {
    let key = predicate_equal(aliased_col("T", "X", "K"), aliased_col("T", "Y", "K"));
    let filter = predicate_equal(aliased_col("T", "Y", "R"), str_lit("x"));
    let from = inner(
        table_ref_as("T", Some("X")),
        table_ref_as("T", Some("Y")),
        and([key.clone(), filter.clone()]),
    );

    let moved = move_condition_filters(&request(from, None), &self_join())
        .expect("the condition holds a filter");

    assert_eq!(clauses(&moved)["from"]["condition"], key);
    assert_eq!(clauses(&moved)["filter"], filter);
}

#[test]
fn a_condition_without_a_join_equality_or_a_filter_is_left_whole() {
    let keyless = and([
        compare("predicate_less", table_col("A", "K"), table_col("B", "K")),
        a_filter(),
    ]);
    for (case, condition) in [("no join equality", keyless), ("keys only", ab_key())] {
        let from = inner(table_ref("A"), table_ref("B"), condition);

        let moved = move_condition_filters(&request(from, None), &three_legs());

        assert!(moved.is_none(), "{case}");
    }
}

#[test]
fn a_keyless_join_keeps_its_condition_while_another_join_moves() {
    let keyless = and([
        compare("predicate_less", table_col("B", "K"), table_col("C", "K")),
        c_filter(),
    ]);
    let from = inner(
        inner(table_ref("A"), table_ref("B"), and([ab_key(), a_filter()])),
        table_ref("C"),
        keyless.clone(),
    );

    let moved = move_condition_filters(&request(from, None), &three_legs())
        .expect("the lower condition holds a filter");

    assert_eq!(clauses(&moved)["from"]["condition"], keyless);
    assert_eq!(clauses(&moved)["filter"], a_filter());
}

/// Scenario: The filters of a join on an outer join's null-supplying side stay in its condition
#[test]
fn outer_joins_and_null_supplying_subtrees_keep_their_conditions() {
    let ab = || inner(table_ref("A"), table_ref("B"), and([ab_key(), a_filter()]));
    let ab_moved = || inner(table_ref("A"), table_ref("B"), ab_key());
    let bc_cond = || and([bc_key(), c_filter()]);
    // (case, from, expected from, expected filter)
    let cases: Vec<(&str, Json, Json, Option<Json>)> = vec![
        (
            "left_outer",
            join(
                "left_outer",
                table_ref("A"),
                table_ref("B"),
                and([ab_key(), a_filter()]),
            ),
            join(
                "left_outer",
                table_ref("A"),
                table_ref("B"),
                and([ab_key(), a_filter()]),
            ),
            None,
        ),
        (
            "right_outer",
            join(
                "right_outer",
                table_ref("A"),
                table_ref("B"),
                and([ab_key(), a_filter()]),
            ),
            join(
                "right_outer",
                table_ref("A"),
                table_ref("B"),
                and([ab_key(), a_filter()]),
            ),
            None,
        ),
        (
            "full_outer",
            join(
                "full_outer",
                table_ref("A"),
                table_ref("B"),
                and([ab_key(), a_filter()]),
            ),
            join(
                "full_outer",
                table_ref("A"),
                table_ref("B"),
                and([ab_key(), a_filter()]),
            ),
            None,
        ),
        (
            "inner on the null-supplying side of a left outer join",
            join("left_outer", table_ref("C"), ab(), bc_key()),
            join("left_outer", table_ref("C"), ab(), bc_key()),
            None,
        ),
        (
            "inner on the null-supplying side of a right outer join",
            join("right_outer", ab(), table_ref("C"), bc_key()),
            join("right_outer", ab(), table_ref("C"), bc_key()),
            None,
        ),
        (
            "inner on the left side of a full outer join",
            join("full_outer", ab(), table_ref("C"), bc_key()),
            join("full_outer", ab(), table_ref("C"), bc_key()),
            None,
        ),
        (
            "inner on the right side of a full outer join",
            join("full_outer", table_ref("C"), ab(), bc_key()),
            join("full_outer", table_ref("C"), ab(), bc_key()),
            None,
        ),
        (
            "inner on the preserved side of a left outer join",
            join("left_outer", ab(), table_ref("C"), bc_key()),
            join("left_outer", ab_moved(), table_ref("C"), bc_key()),
            Some(a_filter()),
        ),
        (
            "inner on the preserved side of a right outer join",
            join("right_outer", table_ref("C"), ab(), bc_key()),
            join("right_outer", table_ref("C"), ab_moved(), bc_key()),
            Some(a_filter()),
        ),
        (
            "inner above a right outer join",
            inner(
                join("right_outer", table_ref("A"), table_ref("B"), ab_key()),
                table_ref("C"),
                bc_cond(),
            ),
            inner(
                join("right_outer", table_ref("A"), table_ref("B"), ab_key()),
                table_ref("C"),
                bc_key(),
            ),
            Some(c_filter()),
        ),
    ];
    for (case, from, expected_from, expected_filter) in cases {
        let original = request(from, None);

        let moved = move_condition_filters(&original, &three_legs());

        let result = moved.as_ref().unwrap_or(&original);
        assert_eq!(result.from(), Some(&expected_from), "{case}");
        assert_eq!(result.filter(), expected_filter.as_ref(), "{case}");
    }
}

/// The clauses a condition move rewrites, as one object to assert on.
fn clauses(pushdown: &PushdownRequest) -> Json {
    serde_json::json!({"from": pushdown.from(), "filter": pushdown.filter()})
}
