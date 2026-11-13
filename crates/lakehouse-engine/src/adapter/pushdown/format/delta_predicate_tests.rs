use super::*;
use crate::adapter::pushdown::test_support::{
    and, between, binary_lit, bool_lit, by_name, col, compare, date_lit, in_list, is_not_null,
    is_null, like, not, num_lit, or, predicate_equal, str_lit, timestamp_lit,
};
use delta_kernel::schema::{PrimitiveType, StructField, StructType};
use serde_json::json;

fn test_schema() -> StructType {
    StructType::try_new([StructField::not_null(
        "id",
        DataType::Primitive(PrimitiveType::Integer),
    )])
    .expect("schema with one integer field must build")
}

/// `node` translated against [`test_schema`].
fn translate(node: &Json) -> Option<Predicate> {
    to_delta_predicate(node, &by_name(&test_schema()))
}

#[test]
fn every_comparison_translates_and_flips_to_its_mirror_with_the_column_on_the_right() {
    let column = || Expression::column(["id"]);
    let five = || Expression::literal(5i32);

    let cases = [
        (
            "predicate_equal",
            Predicate::eq(column(), five()),
            Predicate::eq(column(), five()),
        ),
        (
            "predicate_less",
            Predicate::lt(column(), five()),
            Predicate::gt(column(), five()),
        ),
        (
            "predicate_lessequal",
            Predicate::le(column(), five()),
            Predicate::ge(column(), five()),
        ),
        (
            "predicate_greater",
            Predicate::gt(column(), five()),
            Predicate::lt(column(), five()),
        ),
        (
            "predicate_greaterequal",
            Predicate::ge(column(), five()),
            Predicate::le(column(), five()),
        ),
    ];

    for (kind, column_left, column_right) in cases {
        let left = json!({"type": kind, "left": col("ID"), "right": num_lit(5)});
        let right = json!({"type": kind, "left": num_lit(5), "right": col("ID")});

        assert_eq!(
            translate(&left),
            Some(column_left),
            "{kind} with the column on the left"
        );
        assert_eq!(
            translate(&right),
            Some(column_right),
            "{kind} with the column on the right"
        );
    }
}

/// Scenario: A not-equal comparison prunes
#[test]
fn notequal_translates() {
    let node = compare("predicate_notequal", col("ID"), num_lit(5));

    assert_eq!(
        translate(&node),
        Some(Predicate::ne(
            Expression::column(["id"]),
            Expression::literal(5i32)
        ))
    );
}

#[test]
fn is_null_on_a_column_translates() {
    let node = is_null(col("ID"));
    let pred = translate(&node).expect("IS NULL on an existing column must translate");

    let expected = Predicate::is_null(Expression::column(["id"]));
    assert_eq!(pred, expected);
}

#[test]
fn is_not_null_on_a_column_translates() {
    let node = is_not_null(col("ID"));
    let pred = translate(&node).expect("IS NOT NULL on an existing column must translate");

    let expected = Predicate::is_not_null(Expression::column(["id"]));
    assert_eq!(pred, expected);
}

#[test]
fn not_negates_its_translatable_child() {
    let child = predicate_equal(col("ID"), num_lit(5));
    let node = not(child);
    let pred = translate(&node).expect("NOT over a translatable child must translate");

    let expected = Predicate::not(Predicate::eq(
        Expression::column(["id"]),
        Expression::literal(5i32),
    ));
    assert_eq!(pred, expected);
}

#[test]
fn not_of_an_untranslatable_child_returns_none() {
    let child = predicate_equal(col("ID"), str_lit("5"));
    let node = not(child);

    assert_eq!(translate(&node), None);
}

#[test]
fn not_over_an_and_that_dropped_a_conjunct_returns_none() {
    let child = and([
        predicate_equal(col("ID"), num_lit(5)),
        predicate_equal(col("ID"), str_lit("7")),
    ]);
    let node = not(child);

    assert_eq!(translate(&node), None);
}

#[test]
fn not_over_a_between_that_dropped_a_bound_returns_none() {
    let child = between(col("ID"), num_lit(1), str_lit("ten"));
    let node = not(child);

    assert_eq!(translate(&node), None);
}

#[test]
fn not_over_a_fully_translatable_and_negates_the_whole_conjunction() {
    let child = and([
        compare("predicate_greaterequal", col("ID"), num_lit(1)),
        compare("predicate_lessequal", col("ID"), num_lit(9)),
    ]);
    let node = not(child);
    let pred = translate(&node).expect("NOT over a fully translatable AND must translate");

    let expected = Predicate::not(Predicate::and_from([
        Predicate::ge(Expression::column(["id"]), Expression::literal(1i32)),
        Predicate::le(Expression::column(["id"]), Expression::literal(9i32)),
    ]));
    assert_eq!(pred, expected);
}

fn schema_with_struct_field() -> StructType {
    StructType::try_new([
        StructField::not_null("id", DataType::Primitive(PrimitiveType::Integer)),
        StructField::not_null(
            "nested",
            DataType::Struct(Box::new(
                StructType::try_new([StructField::not_null(
                    "inner",
                    DataType::Primitive(PrimitiveType::Integer),
                )])
                .expect("inner schema must build"),
            )),
        ),
    ])
    .expect("schema with a struct field must build")
}

#[test]
fn resolve_column_matches_case_insensitively() {
    let schema = test_schema();

    let resolved = resolve_column("ID", &by_name(&schema)).expect("existing column must resolve");

    assert_eq!(resolved, ("id", &PrimitiveType::Integer));
}

#[test]
fn resolve_column_returns_none_for_unknown_column() {
    let schema = test_schema();

    assert_eq!(resolve_column("MISSING", &by_name(&schema)), None);
}

#[test]
fn resolve_column_returns_none_for_non_primitive_column() {
    let schema = schema_with_struct_field();

    assert_eq!(resolve_column("NESTED", &by_name(&schema)), None);
}

#[test]
fn a_literal_becomes_the_scalar_of_its_columns_kernel_type() {
    let decimal = DecimalType::try_new(10, 2).expect("decimal(10,2)");
    let cases = [
        (
            bool_lit(true),
            PrimitiveType::Boolean,
            Scalar::Boolean(true),
        ),
        (num_lit(7), PrimitiveType::Byte, Scalar::Byte(7)),
        (num_lit(300), PrimitiveType::Short, Scalar::Short(300)),
        (num_lit(5), PrimitiveType::Integer, Scalar::Integer(5)),
        (
            num_lit("9007199254740993"),
            PrimitiveType::Long,
            Scalar::Long(9_007_199_254_740_993),
        ),
        (num_lit("1.5"), PrimitiveType::Float, Scalar::Float(1.5)),
        (num_lit("1.5"), PrimitiveType::Double, Scalar::Double(1.5)),
        (
            num_lit("5.25"),
            PrimitiveType::Decimal(decimal),
            Scalar::Decimal(DecimalData::try_new(525, decimal).expect("5.25")),
        ),
        (
            str_lit("abc"),
            PrimitiveType::String,
            Scalar::String("abc".into()),
        ),
        (
            date_lit("2024-01-15"),
            PrimitiveType::Date,
            Scalar::Date(19_737),
        ),
        (
            timestamp_lit("2024-01-15 10:30:00"),
            PrimitiveType::Timestamp,
            Scalar::Timestamp(1_705_314_600_000_000),
        ),
        (
            timestamp_lit("2024-01-15 10:30:00"),
            PrimitiveType::TimestampNtz,
            Scalar::TimestampNtz(1_705_314_600_000_000),
        ),
    ];
    for (literal, primitive, scalar) in cases {
        assert_eq!(
            literal_to_scalar(&literal, &primitive),
            Some(scalar),
            "{literal}"
        );
    }
}

#[test]
fn and_of_two_translatable_children_folds_a_conjunction() {
    let node = and([
        compare("predicate_greaterequal", col("ID"), num_lit(5)),
        compare("predicate_less", col("ID"), num_lit(9)),
    ]);
    let pred = translate(&node).expect("an AND of two translatable conjuncts must translate");

    let expected = Predicate::and(
        Predicate::ge(Expression::column(["id"]), Expression::literal(5i32)),
        Predicate::lt(Expression::column(["id"]), Expression::literal(9i32)),
    );
    assert_eq!(pred, expected);
}

#[test]
fn and_with_untranslatable_child_keeps_translatable_conjunct() {
    let node = and([
        predicate_equal(col("ID"), num_lit(5)),
        predicate_equal(col("ID"), str_lit("7")),
    ]);
    let pred = translate(&node).expect("an AND must keep the conjuncts it can translate");

    let expected = Predicate::eq(Expression::column(["id"]), Expression::literal(5i32));
    assert_eq!(pred, expected);
}

#[test]
fn and_all_untranslatable_returns_none_not_a_true_predicate() {
    let node = and([
        predicate_equal(col("ID"), str_lit("5")),
        predicate_equal(col("ID"), str_lit("7")),
    ]);

    assert_eq!(translate(&node), None);
}

#[test]
fn and_over_an_empty_expression_list_returns_none() {
    let node = and([]);

    assert_eq!(translate(&node), None);
}

#[test]
fn or_of_two_translatable_children_folds_a_disjunction() {
    let node = or([
        predicate_equal(col("ID"), num_lit(5)),
        predicate_equal(col("ID"), num_lit(7)),
    ]);
    let pred = translate(&node).expect("an OR of two translatable disjuncts must translate");

    let expected = Predicate::or(
        Predicate::eq(Expression::column(["id"]), Expression::literal(5i32)),
        Predicate::eq(Expression::column(["id"]), Expression::literal(7i32)),
    );
    assert_eq!(pred, expected);
}

#[test]
fn or_with_untranslatable_child_returns_none() {
    let node = or([
        predicate_equal(col("ID"), num_lit(5)),
        predicate_equal(col("ID"), str_lit("7")),
    ]);

    assert_eq!(translate(&node), None);
}

#[test]
fn or_over_an_empty_expression_list_returns_none() {
    let node = or([]);

    assert_eq!(translate(&node), None);
}

#[test]
fn empty_and_fold_returns_none_rather_than_the_kernel_true_identity() {
    assert_eq!(Predicate::and_from([]), Predicate::literal(true));

    assert_eq!(fold_and(std::iter::empty(), Predicate::and_from), None);
}

#[test]
fn empty_or_fold_returns_none_rather_than_the_kernel_false_identity() {
    assert_eq!(Predicate::or_from([]), Predicate::literal(false));

    assert_eq!(fold_or(std::iter::empty(), Predicate::or_from), None);
}

#[test]
fn fold_and_over_only_untranslatable_children_returns_none() {
    assert_eq!(fold_and([None, None], Predicate::and_from), None);
}

#[test]
fn fold_or_over_only_untranslatable_children_returns_none() {
    assert_eq!(fold_or([None, None], Predicate::or_from), None);
}

#[test]
fn in_list_translates_to_an_or_chain_of_equalities() {
    let node = in_list(col("ID"), [num_lit(1), num_lit(2), num_lit(3)]);
    let pred = translate(&node).expect("an IN list of translatable literals must translate");

    let expected = Predicate::or_from([
        Predicate::eq(Expression::column(["id"]), Expression::literal(1i32)),
        Predicate::eq(Expression::column(["id"]), Expression::literal(2i32)),
        Predicate::eq(Expression::column(["id"]), Expression::literal(3i32)),
    ]);
    assert_eq!(pred, expected);
}

#[test]
fn empty_in_list_returns_none_not_a_false_predicate() {
    let node = in_list(col("ID"), []);

    assert_eq!(translate(&node), None);
}

#[test]
fn in_with_type_mismatch_element_returns_none() {
    let node = in_list(col("ID"), [num_lit(1), str_lit("two")]);

    assert_eq!(translate(&node), None);
}

#[test]
fn between_translates_to_a_lower_and_upper_bound_conjunction() {
    let node = between(col("ID"), num_lit(1), num_lit(10));
    let pred = translate(&node).expect("a BETWEEN with two convertible bounds must translate");

    let expected = Predicate::and_from([
        Predicate::ge(Expression::column(["id"]), Expression::literal(1i32)),
        Predicate::le(Expression::column(["id"]), Expression::literal(10i32)),
    ]);
    assert_eq!(pred, expected);
}

#[test]
fn between_keeps_the_convertible_bound_when_the_other_fails() {
    let node = between(col("ID"), num_lit(1), str_lit("ten"));
    let pred = translate(&node).expect("a BETWEEN must keep the bound it can translate");

    let expected = Predicate::ge(Expression::column(["id"]), Expression::literal(1i32));
    assert_eq!(pred, expected);
}

#[test]
fn between_with_both_bounds_unconvertible_returns_none() {
    let node = between(col("ID"), str_lit("one"), str_lit("ten"));

    assert_eq!(translate(&node), None);
}

#[test]
fn a_filter_column_is_emitted_under_its_paired_fields_current_name() {
    let renamed = StructField::nullable("b", DataType::Primitive(PrimitiveType::Integer));
    let columns = [("a", &renamed)];
    let on_a = predicate_equal(col("A"), num_lit(7));
    let on_b = predicate_equal(col("B"), num_lit(7));

    assert_eq!(
        to_delta_predicate(&on_a, &columns),
        Some(Predicate::eq(
            Expression::column(["b"]),
            Expression::literal(7i32)
        )),
        "the declared column binds to the field it is paired with, under that field's name"
    );
    assert_eq!(
        to_delta_predicate(&on_b, &columns),
        None,
        "a column no declared pair names constrains nothing"
    );
}

/// Scenario: A hex-literal filter on a binary column prunes files at plan time
#[test]
fn a_binary_literal_prunes_a_binary_column_by_its_bytes() {
    let schema = StructType::try_new([StructField::nullable(
        "b",
        DataType::Primitive(PrimitiveType::Binary),
    )])
    .expect("schema with one binary field must build");
    let equal_to = |literal: Json| predicate_equal(col("B"), literal);

    assert_eq!(
        to_delta_predicate(&equal_to(binary_lit("00abff")), &by_name(&schema)),
        Some(Predicate::eq(
            Expression::column(["b"]),
            Expression::literal(Scalar::Binary(vec![0x00, 0xab, 0xff]))
        ))
    );
    assert_eq!(
        to_delta_predicate(&equal_to(str_lit("00abff")), &by_name(&schema)),
        None,
        "only the retyped literal names bytes"
    );
}

fn translate_on_text(node: &Json) -> Option<Predicate> {
    let schema = StructType::try_new([StructField::nullable(
        "s",
        DataType::Primitive(PrimitiveType::String),
    )])
    .expect("schema with one string field must build");
    to_delta_predicate(node, &by_name(&schema))
}

fn on_s(compare: fn(Expression, Expression) -> Predicate, value: &str) -> Predicate {
    compare(
        Expression::column(["s"]),
        Expression::literal(Scalar::String(value.into())),
    )
}

/// Scenario: A LIKE pattern prunes by its literal prefix
#[test]
fn a_like_prefix_prunes_as_the_range_up_to_its_successor() {
    let range = Predicate::and_from([on_s(Predicate::ge, "ab"), on_s(Predicate::lt, "ac")]);

    assert_eq!(
        translate_on_text(&like(col("S"), "ab%")),
        Some(range.clone())
    );
    assert_eq!(
        translate_on_text(&not(like(col("S"), "ab%"))),
        Some(Predicate::not(range)),
        "a prefix followed by `%` alone is exact, so its negation prunes"
    );
    assert_eq!(
        translate_on_text(&like(col("S"), "abc")),
        Some(on_s(Predicate::eq, "abc"))
    );
}

/// Scenario: A LIKE pattern prunes by its literal prefix
#[test]
fn a_like_with_a_wildcard_after_its_prefix_never_prunes_by_its_negation() {
    let range = Predicate::and_from([on_s(Predicate::ge, "ab"), on_s(Predicate::lt, "ac")]);

    assert_eq!(translate_on_text(&like(col("S"), "ab_d%")), Some(range));
    assert_eq!(translate_on_text(&not(like(col("S"), "ab_d%"))), None);
    assert_eq!(translate_on_text(&like(col("S"), "%ab")), None);
}

#[test]
fn a_prefix_successor_skips_surrogates_and_carries_past_the_last_code_point() {
    assert_eq!(successor("ab"), Some("ac".into()));
    assert_eq!(successor("a\u{D7FF}"), Some("a\u{E000}".into()));
    assert_eq!(successor("a\u{10FFFF}"), Some("b".into()));
    assert_eq!(successor("\u{10FFFF}"), None);
}
