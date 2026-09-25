use super::*;

impl super::JoinLegs {
    fn resolve(&self, node: &Json) -> ColumnLeg {
        self.resolve_column(node.as_object().expect("a column node is a JSON object"))
    }
}

fn leaf(table_name: &str, table_alias: Option<&str>) -> JoinLeaf {
    JoinLeaf {
        table_name: table_name.to_string(),
        table_alias: table_alias.map(str::to_string),
        table_identifier: format!("lh.{}", table_name.to_ascii_lowercase()),
    }
}

fn column(table_name: &str, table_alias: Option<&str>, name: &str) -> Json {
    let mut node = serde_json::json!({
        "type": "column",
        "name": name,
        "tableName": table_name,
    });
    if let Some(alias) = table_alias {
        node["tableAlias"] = Json::String(alias.to_string());
    }
    node
}

fn literal(value: i64) -> Json {
    serde_json::json!({ "type": "literal_exactnumeric", "value": value.to_string() })
}

fn equal(left: Json, right: Json) -> Json {
    serde_json::json!({ "type": "predicate_equal", "left": left, "right": right })
}

/// `FROM FACT_ORDERS a JOIN FACT_ORDERS b` (#361's shape).
fn self_join_legs() -> JoinLegs {
    JoinLegs::from_leaves(&[
        leaf("FACT_ORDERS", Some("A")),
        leaf("FACT_ORDERS", Some("B")),
    ])
}

fn two_table_legs() -> JoinLegs {
    JoinLegs::from_leaves(&[leaf("CUSTOMER", Some("C")), leaf("ORDERS", Some("O"))])
}

/// Scenario: Self-joined table occurrences are told apart by their aliases
#[test]
fn exact_table_name_and_alias_pair_selects_its_own_leg() {
    let legs = self_join_legs();

    let first = legs.resolve(&column("FACT_ORDERS", Some("A"), "O_ORDERKEY"));
    let second = legs.resolve(&column("FACT_ORDERS", Some("B"), "O_ORDERKEY"));

    assert_eq!(first, ColumnLeg::Leg(0));
    assert_eq!(second, ColumnLeg::Leg(1));
}

/// Scenario: An absent alias is a distinct leg key
#[test]
fn absent_alias_is_a_distinct_leg_key() {
    let legs = JoinLegs::from_leaves(&[leaf("FACT_ORDERS", None), leaf("FACT_ORDERS", Some("B"))]);

    let unaliased = legs.resolve(&column("FACT_ORDERS", None, "O_ORDERKEY"));
    let aliased = legs.resolve(&column("FACT_ORDERS", Some("B"), "O_ORDERKEY"));

    assert_eq!(unaliased, ColumnLeg::Leg(0));
    assert_eq!(aliased, ColumnLeg::Leg(1));
}

/// Scenario: A table name naming exactly one leg resolves without consulting an alias
#[test]
fn single_leg_table_name_resolves_without_consulting_an_alias() {
    let legs = two_table_legs();

    let no_alias = legs.resolve(&column("CUSTOMER", None, "C_CUSTKEY"));
    let foreign_alias = legs.resolve(&column("ORDERS", Some("STALE"), "O_CUSTKEY"));

    assert_eq!(no_alias, ColumnLeg::Leg(0));
    assert_eq!(foreign_alias, ColumnLeg::Leg(1));
}

/// Scenario: Alias matching is verbatim, not case-folded
#[test]
fn alias_matching_is_verbatim_not_case_folded() {
    let legs = JoinLegs::from_leaves(&[
        leaf("FACT_ORDERS", Some("myAlias")),
        leaf("FACT_ORDERS", Some("B")),
    ]);

    let verbatim = legs.resolve(&column("FACT_ORDERS", Some("myAlias"), "O_ORDERKEY"));
    let folded = legs.resolve(&column("FACT_ORDERS", Some("MYALIAS"), "O_ORDERKEY"));

    assert_eq!(verbatim, ColumnLeg::Leg(0));
    assert_eq!(folded, ColumnLeg::Unattributable);
}

/// Scenario: A column of an undeclared table, or with no tableName, is left unqualified
#[test]
fn column_of_a_table_no_leg_declares_is_left_unqualified() {
    let legs = self_join_legs();
    let foreign = column("DIM_CUSTOMER", Some("C"), "C_CUSTKEY");

    let untagged = legs.resolve(&serde_json::json!({ "type": "column", "name": "X" }));

    assert_eq!(legs.resolve(&foreign), ColumnLeg::NoLeg);
    assert_eq!(untagged, ColumnLeg::NoLeg);
    assert_eq!(
        legs.qualify(&foreign).expect("no leg is not a failure"),
        foreign
    );
}

/// Scenario: An unattributable column is reported, never resolved to an arbitrary leg
#[test]
fn unattributable_column_is_reported_rather_than_resolved() {
    let legs = self_join_legs();

    let no_alias = legs.resolve(&column("FACT_ORDERS", None, "O_ORDERKEY"));
    let unknown_alias = legs.resolve(&column("FACT_ORDERS", Some("Z"), "O_ORDERKEY"));

    assert_eq!(no_alias, ColumnLeg::Unattributable);
    assert_eq!(unknown_alias, ColumnLeg::Unattributable);
}

/// Scenario: Each self-join occurrence is qualified with its own leg alias
#[test]
fn qualify_tags_each_self_join_occurrence_with_its_own_leg_alias() {
    let legs = self_join_legs();
    let condition = equal(
        column("FACT_ORDERS", Some("A"), "O_ORDERKEY"),
        column("FACT_ORDERS", Some("B"), "O_ORDERKEY"),
    );

    let qualified = legs.qualify(&condition).expect("both occurrences resolve");

    assert_eq!(
        qualified["left"]["tableAlias"],
        Json::String("LHS_T0".into())
    );
    assert_eq!(
        qualified["right"]["tableAlias"],
        Json::String("LHS_T1".into())
    );
}

/// Scenario: Qualification overwrites the request alias and leaves the input untouched
#[test]
fn qualify_overwrites_the_request_alias_and_leaves_the_input_untouched() {
    let legs = two_table_legs();
    let expr = column("ORDERS", Some("O"), "O_CUSTKEY");

    let qualified = legs.qualify(&expr).expect("resolves to its one leg");

    assert_eq!(qualified["tableAlias"], Json::String("LHS_T1".into()));
    assert_eq!(expr["tableAlias"], Json::String("O".into()));
}

/// Scenario: Qualification reaches columns nested in arrays and function nodes
#[test]
fn qualify_reaches_columns_nested_in_arrays_and_function_nodes() {
    let legs = self_join_legs();
    let expr = serde_json::json!({
        "type": "function_scalar",
        "name": "UPPER",
        "arguments": [column("FACT_ORDERS", Some("B"), "O_COMMENT")],
    });

    let qualified = legs.qualify(&expr).expect("the nested occurrence resolves");

    assert_eq!(
        qualified["arguments"][0]["tableAlias"],
        Json::String("LHS_T1".into())
    );
}

/// Scenario: Qualification fails on an unattributable column, naming it
#[test]
fn qualify_fails_on_an_unattributable_column_naming_it() {
    let legs = self_join_legs();
    let expr = serde_json::json!({
        "type": "predicate_not",
        "expression": column("FACT_ORDERS", None, "O_ORDERKEY"),
    });

    let err = legs.qualify(&expr).expect_err("no leg key matches");

    let message = err.to_string();
    assert!(message.contains("O_ORDERKEY"), "{message}");
    assert!(message.contains("FACT_ORDERS"), "{message}");
}

/// Scenario: A cross-leg condition references every leg it touches
#[test]
fn legs_referenced_reports_every_leg_a_tree_touches() {
    let legs = self_join_legs();
    let condition = equal(
        column("FACT_ORDERS", Some("A"), "O_ORDERKEY"),
        column("FACT_ORDERS", Some("B"), "O_ORDERKEY"),
    );

    let referenced = legs.legs_referenced(&condition);

    assert_eq!(referenced.legs, BTreeSet::from([0, 1]));
    assert!(!referenced.has_unattributed);
    assert!(referenced.any_column);
}

/// Scenario: A column no leg key matches flags the tree as unattributed
#[test]
fn legs_referenced_flags_an_unattributed_column() {
    let legs = self_join_legs();
    let conjunct = equal(column("FACT_ORDERS", None, "O_ORDERKEY"), literal(7));

    let referenced = legs.legs_referenced(&conjunct);

    assert!(referenced.has_unattributed);
    assert!(referenced.any_column);
    assert!(referenced.legs.is_empty());
}

/// Scenario: A literal-only expression is flagged column-free, not unattributed
#[test]
fn legs_referenced_flags_a_column_free_expression() {
    let legs = self_join_legs();

    let referenced = legs.legs_referenced(&equal(literal(1), literal(1)));

    assert!(!referenced.any_column);
    assert!(!referenced.has_unattributed);
    assert!(referenced.legs.is_empty());
}

/// Scenario: A cross-leg condition attaches at the highest leg it references
#[test]
fn attachment_leg_is_the_highest_leg_a_cross_leg_condition_references() {
    let legs = JoinLegs::from_leaves(&[
        leaf("FACT_ORDERS", Some("A")),
        leaf("FACT_ORDERS", Some("B")),
        leaf("FACT_ORDERS", Some("C")),
    ]);
    let condition = equal(
        column("FACT_ORDERS", Some("A"), "O_ORDERKEY"),
        column("FACT_ORDERS", Some("C"), "O_ORDERKEY"),
    );

    assert_eq!(legs.attachment_leg(&condition), Some(2));
}

/// Scenario: A single-leg condition attaches at its own leg, never the first
#[test]
fn attachment_leg_is_the_only_leg_a_single_leg_condition_references() {
    let legs = self_join_legs();
    let condition = equal(column("FACT_ORDERS", Some("B"), "O_ORDERKEY"), literal(7));

    assert_eq!(legs.attachment_leg(&condition), Some(1));
}

/// Scenario: A column-free condition has no attachment leg
#[test]
fn attachment_leg_is_none_for_a_column_free_condition() {
    let legs = self_join_legs();

    assert_eq!(legs.attachment_leg(&equal(literal(1), literal(1))), None);
}

/// Scenario: A condition with any unattributed column has no attachment leg
#[test]
fn attachment_leg_is_none_when_any_column_is_unattributed() {
    let legs = self_join_legs();
    let condition = equal(
        column("FACT_ORDERS", Some("B"), "O_ORDERKEY"),
        column("FACT_ORDERS", Some("Z"), "O_ORDERKEY"),
    );

    assert_eq!(legs.attachment_leg(&condition), None);
}

/// Scenario: A conjunct on one self-join occurrence is local to that leg only
#[test]
fn conjunct_leg_is_the_single_leg_every_column_resolves_to() {
    let legs = self_join_legs();
    let conjunct = equal(column("FACT_ORDERS", Some("B"), "O_ORDERKEY"), literal(7));

    assert_eq!(legs.conjunct_leg(&conjunct), Some(1));
}

/// Scenario: Cross-leg, unattributed, and column-free conjuncts are local to no leg
#[test]
fn conjunct_leg_is_none_for_a_cross_leg_unattributed_or_column_free_conjunct() {
    let legs = self_join_legs();
    let cross_leg = equal(
        column("FACT_ORDERS", Some("A"), "O_ORDERKEY"),
        column("FACT_ORDERS", Some("B"), "O_ORDERKEY"),
    );
    let unattributed = equal(column("FACT_ORDERS", None, "O_ORDERKEY"), literal(7));
    let foreign = equal(column("DIM_CUSTOMER", Some("C"), "C_CUSTKEY"), literal(7));

    assert_eq!(legs.conjunct_leg(&cross_leg), None);
    assert_eq!(legs.conjunct_leg(&unattributed), None);
    assert_eq!(legs.conjunct_leg(&foreign), None);
    assert_eq!(legs.conjunct_leg(&equal(literal(1), literal(1))), None);
}

/// Scenario: The single-scan binding maps every involved table onto leg zero
#[test]
fn single_scan_binding_maps_every_involved_table_onto_leg_zero() {
    let request = serde_json::json!({
        "involvedTables": [{ "name": "FACT_ORDERS" }, { "name": "DIM_CUSTOMER" }],
    });
    let legs = JoinLegs::for_single_scan(&request);

    let first = legs.resolve(&column("FACT_ORDERS", Some("A"), "O_ORDERKEY"));
    let second = legs.resolve(&column("DIM_CUSTOMER", None, "C_CUSTKEY"));
    let outsider = legs.resolve(&column("SOMETHING_ELSE", None, "X"));
    let qualified = legs
        .qualify(&column("FACT_ORDERS", Some("A"), "O_ORDERKEY"))
        .expect("the single scan can never be unattributable");

    assert_eq!(legs.leg_count(), 1);
    assert_eq!(first, ColumnLeg::Leg(0));
    assert_eq!(second, ColumnLeg::Leg(0));
    assert_eq!(outsider, ColumnLeg::NoLeg);
    assert_eq!(qualified["tableAlias"], Json::String("LHS_T0".into()));
}

/// Scenario: A repeated involved table still collapses onto leg zero
#[test]
fn repeated_involved_table_still_collapses_onto_leg_zero() {
    let request = serde_json::json!({
        "involvedTables": [{ "name": "FACT_ORDERS" }, { "name": "FACT_ORDERS" }],
    });
    let legs = JoinLegs::for_single_scan(&request);

    let resolved = legs.resolve(&column("FACT_ORDERS", Some("A"), "O_ORDERKEY"));

    assert_eq!(resolved, ColumnLeg::Leg(0));
}

/// Scenario: A request without involvedTables qualifies nothing
#[test]
fn single_scan_binding_without_involved_tables_qualifies_nothing() {
    let legs = JoinLegs::for_single_scan(&serde_json::json!({}));
    let expr = column("FACT_ORDERS", Some("A"), "O_ORDERKEY");

    assert_eq!(legs.resolve(&expr), ColumnLeg::NoLeg);
    assert_eq!(legs.qualify(&expr).expect("no leg is not a failure"), expr);
}

/// Scenario: Leg aliases are LHS_T{index} in FROM-tree traversal order
#[test]
fn leg_alias_is_the_wrapper_subquery_alias_of_that_leg() {
    let legs = JoinLegs::from_leaves(&[
        leaf("FACT_ORDERS", Some("A")),
        leaf("FACT_ORDERS", Some("B")),
        leaf("DIM_CUSTOMER", Some("C")),
    ]);

    assert_eq!(legs.leg_count(), 3);
    assert_eq!(legs.leg_alias(0), "LHS_T0");
    assert_eq!(legs.leg_alias(1), "LHS_T1");
    assert_eq!(legs.leg_alias(2), "LHS_T2");
}
