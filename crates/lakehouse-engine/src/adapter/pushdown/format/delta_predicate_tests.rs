use super::*;
use delta_kernel::schema::{PrimitiveType, StructField};
use serde_json::json;

fn test_schema() -> StructType {
    StructType::try_new([StructField::not_null(
        "id",
        DataType::Primitive(PrimitiveType::Integer),
    )])
    .expect("schema with one integer field must build")
}

fn col(name: &str) -> Json {
    json!({"type": "column", "name": name})
}

fn int_lit(v: i64) -> Json {
    json!({"type": "literal_exactnumeric", "value": v})
}

#[test]
fn equal_on_integer_column_translates() {
    let schema = test_schema();

    let node = json!({"type": "predicate_equal", "left": col("ID"), "right": int_lit(5)});
    let pred =
        to_delta_predicate(&node, &schema).expect("equality on an integer column must translate");

    let expected = Predicate::eq(Expression::column(["id"]), Expression::literal(5i32));
    assert_eq!(pred, expected);
}

#[test]
fn less_on_integer_column_translates() {
    let schema = test_schema();

    let node = json!({"type": "predicate_less", "left": col("ID"), "right": int_lit(5)});
    let pred =
        to_delta_predicate(&node, &schema).expect("less-than on an integer column must translate");

    let expected = Predicate::lt(Expression::column(["id"]), Expression::literal(5i32));
    assert_eq!(pred, expected);
}

#[test]
fn lessequal_on_integer_column_translates() {
    let schema = test_schema();

    let node = json!({"type": "predicate_lessequal", "left": col("ID"), "right": int_lit(5)});
    let pred = to_delta_predicate(&node, &schema)
        .expect("less-than-or-equal on an integer column must translate");

    let expected = Predicate::le(Expression::column(["id"]), Expression::literal(5i32));
    assert_eq!(pred, expected);
}

#[test]
fn greater_on_integer_column_translates() {
    let schema = test_schema();

    let node = json!({"type": "predicate_greater", "left": col("ID"), "right": int_lit(5)});
    let pred = to_delta_predicate(&node, &schema)
        .expect("greater-than on an integer column must translate");

    let expected = Predicate::gt(Expression::column(["id"]), Expression::literal(5i32));
    assert_eq!(pred, expected);
}

#[test]
fn greaterequal_on_integer_column_translates() {
    let schema = test_schema();

    let node = json!({"type": "predicate_greaterequal", "left": col("ID"), "right": int_lit(5)});
    let pred = to_delta_predicate(&node, &schema)
        .expect("greater-than-or-equal on an integer column must translate");

    let expected = Predicate::ge(Expression::column(["id"]), Expression::literal(5i32));
    assert_eq!(pred, expected);
}

#[test]
fn less_with_column_on_right_flips_to_greater() {
    let schema = test_schema();

    let node = json!({"type": "predicate_less", "left": int_lit(5), "right": col("ID")});
    let pred = to_delta_predicate(&node, &schema)
        .expect("less-than with the column on the right must translate via the flipped operator");

    let expected = Predicate::gt(Expression::column(["id"]), Expression::literal(5i32));
    assert_eq!(pred, expected);
}

#[test]
fn every_comparison_with_the_column_on_the_right_flips_to_its_mirror() {
    let schema = test_schema();
    let column = || Expression::column(["id"]);
    let five = || Expression::literal(5i32);

    let cases = [
        ("predicate_equal", Predicate::eq(column(), five())),
        ("predicate_less", Predicate::gt(column(), five())),
        ("predicate_lessequal", Predicate::ge(column(), five())),
        ("predicate_greater", Predicate::lt(column(), five())),
        ("predicate_greaterequal", Predicate::le(column(), five())),
    ];

    for (kind, expected) in cases {
        let node = json!({"type": kind, "left": int_lit(5), "right": col("ID")});

        assert_eq!(
            to_delta_predicate(&node, &schema),
            Some(expected),
            "{kind} with the column on the right"
        );
    }
}

#[test]
fn notequal_returns_none() {
    let schema = test_schema();

    let node = json!({"type": "predicate_notequal", "left": col("ID"), "right": int_lit(5)});

    assert_eq!(to_delta_predicate(&node, &schema), None);
}

#[test]
fn is_null_on_a_column_translates() {
    let schema = test_schema();

    let node = json!({"type": "predicate_is_null", "expression": col("ID")});
    let pred =
        to_delta_predicate(&node, &schema).expect("IS NULL on an existing column must translate");

    let expected = Predicate::is_null(Expression::column(["id"]));
    assert_eq!(pred, expected);
}

#[test]
fn is_not_null_on_a_column_translates() {
    let schema = test_schema();

    let node = json!({"type": "predicate_is_not_null", "expression": col("ID")});
    let pred = to_delta_predicate(&node, &schema)
        .expect("IS NOT NULL on an existing column must translate");

    let expected = Predicate::is_not_null(Expression::column(["id"]));
    assert_eq!(pred, expected);
}

#[test]
fn not_negates_its_translatable_child() {
    let schema = test_schema();

    let child = json!({"type": "predicate_equal", "left": col("ID"), "right": int_lit(5)});
    let node = json!({"type": "predicate_not", "expression": child});
    let pred =
        to_delta_predicate(&node, &schema).expect("NOT over a translatable child must translate");

    let expected = Predicate::not(Predicate::eq(
        Expression::column(["id"]),
        Expression::literal(5i32),
    ));
    assert_eq!(pred, expected);
}

#[test]
fn not_of_an_untranslatable_child_returns_none() {
    let schema = test_schema();

    let child = json!({"type": "predicate_notequal", "left": col("ID"), "right": int_lit(5)});
    let node = json!({"type": "predicate_not", "expression": child});

    assert_eq!(to_delta_predicate(&node, &schema), None);
}

#[test]
fn not_over_an_and_that_dropped_a_conjunct_returns_none() {
    let schema = test_schema();

    let child = json!({"type": "predicate_and", "expressions": [
        {"type": "predicate_equal", "left": col("ID"), "right": int_lit(5)},
        {"type": "predicate_notequal", "left": col("ID"), "right": int_lit(7)},
    ]});
    let node = json!({"type": "predicate_not", "expression": child});

    assert_eq!(to_delta_predicate(&node, &schema), None);
}

#[test]
fn not_over_a_between_that_dropped_a_bound_returns_none() {
    let schema = test_schema();

    let child = json!({
        "type": "predicate_between",
        "expression": col("ID"),
        "left": int_lit(1),
        "right": lit("literal_string", json!("ten")),
    });
    let node = json!({"type": "predicate_not", "expression": child});

    assert_eq!(to_delta_predicate(&node, &schema), None);
}

#[test]
fn not_over_a_fully_translatable_and_negates_the_whole_conjunction() {
    let schema = test_schema();

    let child = json!({"type": "predicate_and", "expressions": [
        {"type": "predicate_greaterequal", "left": col("ID"), "right": int_lit(1)},
        {"type": "predicate_lessequal", "left": col("ID"), "right": int_lit(9)},
    ]});
    let node = json!({"type": "predicate_not", "expression": child});
    let pred = to_delta_predicate(&node, &schema)
        .expect("NOT over a fully translatable AND must translate");

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

    let resolved = resolve_column("ID", &schema).expect("existing column must resolve");

    assert_eq!(resolved, ("id", &PrimitiveType::Integer));
}

#[test]
fn resolve_column_returns_none_for_unknown_column() {
    let schema = test_schema();

    assert_eq!(resolve_column("MISSING", &schema), None);
}

#[test]
fn resolve_column_returns_none_for_non_primitive_column() {
    let schema = schema_with_struct_field();

    assert_eq!(resolve_column("NESTED", &schema), None);
}

fn lit(kind: &str, value: Json) -> Json {
    json!({"type": kind, "value": value})
}

fn decimal_type(precision: u8, scale: u8) -> DecimalType {
    DecimalType::try_new(precision, scale).expect("decimal type must be valid")
}

fn decimal_of(unscaled: i128, precision: u8, scale: u8) -> Scalar {
    Scalar::Decimal(
        DecimalData::try_new(unscaled, decimal_type(precision, scale))
            .expect("decimal data must be valid"),
    )
}

const TIMESTAMP_MICROS: i64 = 1_705_314_600_000_000;

#[test]
fn boolean_literal_becomes_boolean_scalar() {
    let json_true = lit("literal_bool", json!(true));
    let string_false = lit("literal_bool", json!("FALSE"));
    let number_true = lit("literal_bool", json!(1));

    assert_eq!(
        literal_to_scalar(&json_true, &PrimitiveType::Boolean),
        Some(Scalar::Boolean(true))
    );
    assert_eq!(
        literal_to_scalar(&string_false, &PrimitiveType::Boolean),
        Some(Scalar::Boolean(false))
    );
    assert_eq!(
        literal_to_scalar(&number_true, &PrimitiveType::Boolean),
        Some(Scalar::Boolean(true))
    );
}

#[test]
fn unrecognized_boolean_spelling_yields_no_scalar() {
    let node = lit("literal_bool", json!("yes"));

    assert_eq!(literal_to_scalar(&node, &PrimitiveType::Boolean), None);
}

#[test]
fn exactnumeric_literal_becomes_integer_scalar() {
    let node = lit("literal_exactnumeric", json!(5));

    assert_eq!(
        literal_to_scalar(&node, &PrimitiveType::Integer),
        Some(Scalar::Integer(5))
    );
}

#[test]
fn exactnumeric_string_literal_becomes_long_scalar() {
    let node = lit("literal_exactnumeric", json!("9007199254740993"));

    assert_eq!(
        literal_to_scalar(&node, &PrimitiveType::Long),
        Some(Scalar::Long(9_007_199_254_740_993))
    );
}

#[test]
fn exactnumeric_literal_becomes_narrow_integer_scalar() {
    let node = lit("literal_exactnumeric", json!(300));

    assert_eq!(
        literal_to_scalar(&node, &PrimitiveType::Short),
        Some(Scalar::Short(300))
    );
    assert_eq!(
        literal_to_scalar(&lit("literal_exactnumeric", json!(7)), &PrimitiveType::Byte),
        Some(Scalar::Byte(7))
    );
}

#[test]
fn integer_literal_outside_the_column_range_yields_no_scalar() {
    let node = lit("literal_exactnumeric", json!(300));

    assert_eq!(literal_to_scalar(&node, &PrimitiveType::Byte), None);
}

#[test]
fn double_literal_becomes_double_scalar() {
    let node = lit("literal_double", json!(1.5));

    assert_eq!(
        literal_to_scalar(&node, &PrimitiveType::Double),
        Some(Scalar::Double(1.5))
    );
}

#[test]
fn double_literal_becomes_float_scalar_on_a_float_column() {
    let node = lit("literal_double", json!(1.5));

    assert_eq!(
        literal_to_scalar(&node, &PrimitiveType::Float),
        Some(Scalar::Float(1.5))
    );
}

#[test]
fn double_literal_not_exactly_representable_as_float_yields_no_scalar() {
    let node = lit("literal_double", json!(0.1234567890123));

    assert_eq!(literal_to_scalar(&node, &PrimitiveType::Float), None);
}

#[test]
fn string_literal_becomes_string_scalar() {
    let node = lit("literal_string", json!("abc"));

    assert_eq!(
        literal_to_scalar(&node, &PrimitiveType::String),
        Some(Scalar::String("abc".to_string()))
    );
}

#[test]
fn date_literal_becomes_days_since_the_epoch() {
    let node = lit("literal_date", json!("2024-01-15"));

    assert_eq!(
        literal_to_scalar(&node, &PrimitiveType::Date),
        Some(Scalar::Date(19737))
    );
}

#[test]
fn timestamp_literal_becomes_microseconds_on_a_zoneless_column() {
    let node = lit("literal_timestamp", json!("2024-01-15 10:30:00"));

    assert_eq!(
        literal_to_scalar(&node, &PrimitiveType::TimestampNtz),
        Some(Scalar::TimestampNtz(TIMESTAMP_MICROS))
    );
}

#[test]
fn timestamp_literal_becomes_microseconds_on_a_utc_adjusted_column() {
    let node = lit("literal_timestamp", json!("2024-01-15 10:30:00"));

    assert_eq!(
        literal_to_scalar(&node, &PrimitiveType::Timestamp),
        Some(Scalar::Timestamp(TIMESTAMP_MICROS))
    );
}

#[test]
fn utc_timestamp_literal_becomes_microseconds_on_a_utc_adjusted_column() {
    let node = lit("literal_timestamp_utc", json!("2024-01-15T10:30:00Z"));

    assert_eq!(
        literal_to_scalar(&node, &PrimitiveType::Timestamp),
        Some(Scalar::Timestamp(TIMESTAMP_MICROS))
    );
}

#[test]
fn utc_timestamp_literal_yields_no_scalar_on_a_zoneless_column() {
    let node = lit("literal_timestamp_utc", json!("2024-01-15T10:30:00Z"));

    assert_eq!(literal_to_scalar(&node, &PrimitiveType::TimestampNtz), None);
}

#[test]
fn exactnumeric_literal_rescales_to_the_decimal_column_scale() {
    let whole = lit("literal_exactnumeric", json!(5));
    let fractional = lit("literal_exactnumeric", json!("5.25"));
    let exponent = lit("literal_double", json!("1e2"));
    let prim = PrimitiveType::Decimal(decimal_type(10, 2));

    assert_eq!(
        literal_to_scalar(&whole, &prim),
        Some(decimal_of(500, 10, 2))
    );
    assert_eq!(
        literal_to_scalar(&fractional, &prim),
        Some(decimal_of(525, 10, 2))
    );
    assert_eq!(
        literal_to_scalar(&exponent, &prim),
        Some(decimal_of(10_000, 10, 2))
    );
}

#[test]
fn decimal_literal_finer_than_the_column_scale_yields_no_scalar() {
    let node = lit("literal_exactnumeric", json!("5.125"));
    let prim = PrimitiveType::Decimal(decimal_type(10, 2));

    assert_eq!(literal_to_scalar(&node, &prim), None);
}

#[test]
fn decimal_literal_exceeding_the_column_precision_yields_no_scalar() {
    let node = lit("literal_exactnumeric", json!(12345));
    let prim = PrimitiveType::Decimal(decimal_type(3, 0));

    assert_eq!(literal_to_scalar(&node, &prim), None);
}

#[test]
fn empty_string_literal_yields_no_scalar() {
    assert_eq!(
        literal_to_scalar(&lit("literal_string", json!("")), &PrimitiveType::String),
        None
    );
    assert_eq!(
        literal_to_scalar(&lit("literal_date", json!("")), &PrimitiveType::Date),
        None
    );
    assert_eq!(
        literal_to_scalar(
            &lit("literal_timestamp", json!("")),
            &PrimitiveType::TimestampNtz
        ),
        None
    );
}

#[test]
fn literal_the_column_type_cannot_represent_yields_no_scalar() {
    assert_eq!(
        literal_to_scalar(
            &lit("literal_string", json!("abc")),
            &PrimitiveType::Integer
        ),
        None
    );
    assert_eq!(
        literal_to_scalar(
            &lit("literal_exactnumeric", json!(5)),
            &PrimitiveType::String
        ),
        None
    );
    assert_eq!(
        literal_to_scalar(&lit("literal_bool", json!(true)), &PrimitiveType::Integer),
        None
    );
    assert_eq!(
        literal_to_scalar(
            &lit("literal_date", json!("2024-01-15")),
            &PrimitiveType::Long
        ),
        None
    );
}

#[test]
fn unknown_literal_kind_yields_no_scalar() {
    let node = lit("literal_interval", json!("1-2"));

    assert_eq!(literal_to_scalar(&node, &PrimitiveType::String), None);
}

#[test]
fn and_of_two_translatable_children_folds_a_conjunction() {
    let schema = test_schema();

    let node = json!({"type": "predicate_and", "expressions": [
        {"type": "predicate_greaterequal", "left": col("ID"), "right": int_lit(5)},
        {"type": "predicate_less", "left": col("ID"), "right": int_lit(9)},
    ]});
    let pred = to_delta_predicate(&node, &schema)
        .expect("an AND of two translatable conjuncts must translate");

    let expected = Predicate::and(
        Predicate::ge(Expression::column(["id"]), Expression::literal(5i32)),
        Predicate::lt(Expression::column(["id"]), Expression::literal(9i32)),
    );
    assert_eq!(pred, expected);
}

#[test]
fn and_with_untranslatable_child_keeps_translatable_conjunct() {
    let schema = test_schema();

    let node = json!({"type": "predicate_and", "expressions": [
        {"type": "predicate_equal", "left": col("ID"), "right": int_lit(5)},
        {"type": "predicate_notequal", "left": col("ID"), "right": int_lit(7)},
    ]});
    let pred = to_delta_predicate(&node, &schema)
        .expect("an AND must keep the conjuncts it can translate");

    let expected = Predicate::eq(Expression::column(["id"]), Expression::literal(5i32));
    assert_eq!(pred, expected);
}

#[test]
fn and_all_untranslatable_returns_none_not_a_true_predicate() {
    let schema = test_schema();

    let node = json!({"type": "predicate_and", "expressions": [
        {"type": "predicate_notequal", "left": col("ID"), "right": int_lit(5)},
        {"type": "predicate_notequal", "left": col("ID"), "right": int_lit(7)},
    ]});

    assert_eq!(to_delta_predicate(&node, &schema), None);
}

#[test]
fn and_over_an_empty_expression_list_returns_none() {
    let schema = test_schema();

    let node = json!({"type": "predicate_and", "expressions": []});

    assert_eq!(to_delta_predicate(&node, &schema), None);
}

#[test]
fn or_of_two_translatable_children_folds_a_disjunction() {
    let schema = test_schema();

    let node = json!({"type": "predicate_or", "expressions": [
        {"type": "predicate_equal", "left": col("ID"), "right": int_lit(5)},
        {"type": "predicate_equal", "left": col("ID"), "right": int_lit(7)},
    ]});
    let pred = to_delta_predicate(&node, &schema)
        .expect("an OR of two translatable disjuncts must translate");

    let expected = Predicate::or(
        Predicate::eq(Expression::column(["id"]), Expression::literal(5i32)),
        Predicate::eq(Expression::column(["id"]), Expression::literal(7i32)),
    );
    assert_eq!(pred, expected);
}

#[test]
fn or_with_untranslatable_child_returns_none() {
    let schema = test_schema();

    let node = json!({"type": "predicate_or", "expressions": [
        {"type": "predicate_equal", "left": col("ID"), "right": int_lit(5)},
        {"type": "predicate_notequal", "left": col("ID"), "right": int_lit(7)},
    ]});

    assert_eq!(to_delta_predicate(&node, &schema), None);
}

#[test]
fn or_over_an_empty_expression_list_returns_none() {
    let schema = test_schema();

    let node = json!({"type": "predicate_or", "expressions": []});

    assert_eq!(to_delta_predicate(&node, &schema), None);
}

#[test]
fn empty_and_fold_returns_none_rather_than_the_kernel_true_identity() {
    assert_eq!(Predicate::and_from([]), Predicate::literal(true));

    assert_eq!(fold_and(std::iter::empty()), None);
}

#[test]
fn empty_or_fold_returns_none_rather_than_the_kernel_false_identity() {
    assert_eq!(Predicate::or_from([]), Predicate::literal(false));

    assert_eq!(fold_or(std::iter::empty()), None);
}

#[test]
fn fold_and_over_only_untranslatable_children_returns_none() {
    assert_eq!(fold_and([None, None].into_iter()), None);
}

#[test]
fn fold_or_over_only_untranslatable_children_returns_none() {
    assert_eq!(fold_or([None, None].into_iter()), None);
}

#[test]
fn in_list_translates_to_an_or_chain_of_equalities() {
    let schema = test_schema();

    let node = json!({
        "type": "predicate_in_constlist",
        "expression": col("ID"),
        "arguments": [int_lit(1), int_lit(2), int_lit(3)],
    });
    let pred = to_delta_predicate(&node, &schema)
        .expect("an IN list of translatable literals must translate");

    let expected = Predicate::or_from([
        Predicate::eq(Expression::column(["id"]), Expression::literal(1i32)),
        Predicate::eq(Expression::column(["id"]), Expression::literal(2i32)),
        Predicate::eq(Expression::column(["id"]), Expression::literal(3i32)),
    ]);
    assert_eq!(pred, expected);
}

#[test]
fn empty_in_list_returns_none_not_a_false_predicate() {
    let schema = test_schema();

    let node = json!({
        "type": "predicate_in_constlist",
        "expression": col("ID"),
        "arguments": [],
    });

    assert_eq!(to_delta_predicate(&node, &schema), None);
}

#[test]
fn in_with_type_mismatch_element_returns_none() {
    let schema = test_schema();

    let node = json!({
        "type": "predicate_in_constlist",
        "expression": col("ID"),
        "arguments": [int_lit(1), lit("literal_string", json!("two"))],
    });

    assert_eq!(to_delta_predicate(&node, &schema), None);
}

#[test]
fn between_translates_to_a_lower_and_upper_bound_conjunction() {
    let schema = test_schema();

    let node = json!({
        "type": "predicate_between",
        "expression": col("ID"),
        "left": int_lit(1),
        "right": int_lit(10),
    });
    let pred = to_delta_predicate(&node, &schema)
        .expect("a BETWEEN with two convertible bounds must translate");

    let expected = Predicate::and_from([
        Predicate::ge(Expression::column(["id"]), Expression::literal(1i32)),
        Predicate::le(Expression::column(["id"]), Expression::literal(10i32)),
    ]);
    assert_eq!(pred, expected);
}

#[test]
fn between_keeps_the_convertible_bound_when_the_other_fails() {
    let schema = test_schema();

    let node = json!({
        "type": "predicate_between",
        "expression": col("ID"),
        "left": int_lit(1),
        "right": lit("literal_string", json!("ten")),
    });
    let pred =
        to_delta_predicate(&node, &schema).expect("a BETWEEN must keep the bound it can translate");

    let expected = Predicate::ge(Expression::column(["id"]), Expression::literal(1i32));
    assert_eq!(pred, expected);
}

#[test]
fn between_with_both_bounds_unconvertible_returns_none() {
    let schema = test_schema();

    let node = json!({
        "type": "predicate_between",
        "expression": col("ID"),
        "left": lit("literal_string", json!("one")),
        "right": lit("literal_string", json!("ten")),
    });

    assert_eq!(to_delta_predicate(&node, &schema), None);
}
