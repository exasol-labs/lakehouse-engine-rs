use super::*;
use crate::adapter::pushdown::format::literal::exact_literal;
use crate::adapter::pushdown::test_support::{
    and, binary_lit, col, compare, date_lit, in_list, is_not_null, is_null, like, not,
    nullable_field, num_lit, or, predicate_equal, str_lit, timestamp_lit,
};
use iceberg::expr::Bind;
use iceberg::spec::{NestedField, Schema, Type};
use std::sync::Arc;

/// `literal` converted as the parse converts it for a column of `primitive`, then made a datum.
fn literal_to_datum(literal: &Json, primitive: &PrimitiveType) -> Option<Datum> {
    datum(&exact_literal(literal, &arrow_type(primitive)?)?, primitive)
}

fn test_schema() -> Schema {
    Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                2,
                "amount",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                3,
                "name",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                4,
                "event_date",
                Type::Primitive(PrimitiveType::Date),
            )),
            Arc::new(NestedField::optional(
                5,
                "score",
                Type::Primitive(PrimitiveType::Double),
            )),
        ])
        .build()
        .unwrap()
}

fn declared_field(field_id: i32, name: &str) -> LogicalField {
    LogicalField {
        field_id: Some(field_id),
        ..nullable_field(name, "utf8")
    }
}

/// Translates `filter` for a table whose declaration CREATE took from `schema` itself.
fn translate_as_created(filter: &Json, schema: &Schema) -> Option<Predicate> {
    let declared: Vec<LogicalField> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| declared_field(field.id, &field.name))
        .collect();
    to_iceberg_predicate(filter, &declared, schema)
}

fn translate(filter: &Json) -> Option<Predicate> {
    translate_as_created(filter, &test_schema())
}

#[test]
fn every_leaf_and_junction_lowers_to_its_iceberg_predicate() {
    let id_is = |value: i32| Reference::new("id").equal_to(Datum::int(value));
    let amount = || Reference::new("amount");
    let name = || Reference::new("name");
    let on_amount = |kind: &str| compare(kind, col("AMOUNT"), num_lit(100));
    let cases = [
        (predicate_equal(col("ID"), num_lit(5)), id_is(5)),
        (
            on_amount("predicate_notequal"),
            amount().not_equal_to(Datum::long(100)),
        ),
        (
            on_amount("predicate_less"),
            amount().less_than(Datum::long(100)),
        ),
        (
            on_amount("predicate_lessequal"),
            amount().less_than_or_equal_to(Datum::long(100)),
        ),
        (
            on_amount("predicate_greater"),
            amount().greater_than(Datum::long(100)),
        ),
        (
            on_amount("predicate_greaterequal"),
            amount().greater_than_or_equal_to(Datum::long(100)),
        ),
        (is_null(col("NAME")), name().is_null()),
        (is_not_null(col("NAME")), name().is_not_null()),
        (
            in_list(col("NAME"), [str_lit("alice"), str_lit("bob")]),
            name().is_in([Datum::string("alice"), Datum::string("bob")]),
        ),
        (
            predicate_equal(col("EVENT_DATE"), date_lit("2024-01-15")),
            Reference::new("event_date").equal_to(Datum::date(19_737)),
        ),
        (
            and([
                predicate_equal(col("ID"), num_lit(1)),
                predicate_equal(col("ID"), num_lit(2)),
            ]),
            id_is(1).and(id_is(2)),
        ),
        (
            or([
                predicate_equal(col("ID"), num_lit(1)),
                predicate_equal(col("ID"), num_lit(2)),
            ]),
            id_is(1).or(id_is(2)),
        ),
        (
            not(predicate_equal(col("ID"), num_lit(5))),
            id_is(5).negate(),
        ),
    ];
    for (node, expected) in cases {
        assert_eq!(translate(&node), Some(expected), "{node}");
    }
}

#[test]
fn a_timestamp_literal_becomes_a_datum_at_the_columns_unit() {
    let micros = 1_705_314_600_123_456;
    let finer = timestamp_lit("2024-01-15 10:30:00.123456789");
    let exact = timestamp_lit("2024-01-15 10:30:00.123456000");
    for (primitive, datum) in [
        (PrimitiveType::Timestamp, Datum::timestamp_micros(micros)),
        (
            PrimitiveType::Timestamptz,
            Datum::timestamptz_micros(micros),
        ),
    ] {
        assert_eq!(literal_to_datum(&finer, &primitive), None, "{primitive}");
        assert_eq!(
            literal_to_datum(&exact, &primitive),
            Some(datum),
            "{primitive}"
        );
    }
    for (primitive, datum) in [
        (
            PrimitiveType::TimestampNs,
            Datum::timestamp_nanos(micros * 1_000 + 789),
        ),
        (
            PrimitiveType::TimestamptzNs,
            Datum::timestamptz_nanos(micros * 1_000 + 789),
        ),
    ] {
        assert_eq!(
            literal_to_datum(&finer, &primitive),
            Some(datum),
            "{primitive}"
        );
    }
}

/// Scenario: A numeric literal converts only when the column's type holds it exactly
#[test]
fn a_decimal_literal_binds_at_the_columns_precision_and_scale() {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            1,
            "q",
            Type::Primitive(PrimitiveType::Decimal {
                precision: 4,
                scale: 1,
            }),
        ))])
        .build()
        .unwrap();
    for (numeral, prunes) in [("47", true), ("2.50", true), ("2.55", false)] {
        let node = predicate_equal(col("Q"), num_lit(numeral));
        let predicate = translate_as_created(&node, &schema);
        assert_eq!(predicate.is_some(), prunes, "{numeral}");
        if let Some(predicate) = predicate {
            predicate
                .bind(Arc::new(schema.clone()), false)
                .unwrap_or_else(|e| panic!("{numeral} must bind to decimal(4,1): {e}"));
        }
    }
}

#[test]
fn a_float_column_takes_only_a_literal_it_holds_exactly() {
    assert_eq!(
        literal_to_datum(&num_lit("0.5"), &PrimitiveType::Float),
        Some(Datum::float(0.5_f32))
    );
    assert_eq!(
        literal_to_datum(&num_lit("0.2"), &PrimitiveType::Float),
        None
    );
}

#[test]
fn a_like_prefix_prunes_with_starts_with_and_negates_only_when_exact() {
    let starts_with = Reference::new("name").starts_with(Datum::string("ab"));
    assert_eq!(translate(&name_like("ab%")), Some(starts_with.clone()));
    assert_eq!(
        translate(&not(name_like("ab%"))),
        Some(starts_with.negate())
    );
    assert_eq!(
        translate(&name_like("ab")),
        Some(Reference::new("name").equal_to(Datum::string("ab")))
    );
}

fn name_like(pattern: &str) -> Json {
    like(col("NAME"), pattern)
}

fn int_schema(fields: &[(i32, &str)]) -> Schema {
    Schema::builder()
        .with_fields(
            fields
                .iter()
                .map(|&(id, name)| {
                    NestedField::optional(id, name, Type::Primitive(PrimitiveType::Int)).into()
                })
                .collect::<Vec<_>>(),
        )
        .build()
        .unwrap()
}

fn equal_to(column: &str, value: i64) -> Json {
    predicate_equal(col(column), num_lit(value))
}

#[test]
fn a_filter_column_whose_declared_id_was_dropped_imposes_no_constraint() {
    let declared = [declared_field(1, "id"), declared_field(2, "note")];
    let note_dropped = int_schema(&[(1, "id"), (3, "note")]);
    let on_both = and([equal_to("ID", 5), equal_to("NOTE", 1)]);

    assert_eq!(
        to_iceberg_predicate(&equal_to("NOTE", 1), &declared, &note_dropped),
        None,
        "field id 2 is gone; the later field `note` (id 3) is another column"
    );
    assert_eq!(
        to_iceberg_predicate(&on_both, &declared, &note_dropped),
        Some(Reference::new("id").equal_to(Datum::int(5))),
        "AND drops only the conjunct on the dropped id"
    );
    assert_eq!(
        to_iceberg_predicate(&not(on_both), &declared, &note_dropped),
        None,
        "NOT never negates the AND that dropped a conjunct"
    );
}

#[test]
fn a_filter_column_the_declaration_does_not_carry_imposes_no_constraint() {
    let declared = [declared_field(1, "id")];
    let added_since = int_schema(&[(1, "id"), (2, "extra")]);

    assert_eq!(
        to_iceberg_predicate(&equal_to("EXTRA", 1), &declared, &added_since),
        None
    );
}

fn binary_schema() -> Schema {
    let column = |id: i32, name: &str, ty: PrimitiveType| {
        Arc::new(NestedField::optional(id, name, Type::Primitive(ty)))
    };
    Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            column(1, "b", PrimitiveType::Binary),
            column(2, "f", PrimitiveType::Fixed(2)),
            column(3, "u", PrimitiveType::Uuid),
        ])
        .build()
        .unwrap()
}

fn equal_to_bytes(column: &str, hex: &str) -> Json {
    predicate_equal(col(column), binary_lit(hex))
}

/// Scenario: A hex-literal filter on a binary column prunes files at plan time
#[test]
fn a_binary_literal_prunes_a_binary_or_fixed_column_by_its_bytes() {
    let schema = binary_schema();
    assert_eq!(
        translate_as_created(&equal_to_bytes("B", "00abff"), &schema),
        Some(Reference::new("b").equal_to(Datum::binary([0x00, 0xab, 0xff])))
    );
    assert_eq!(
        translate_as_created(&equal_to_bytes("F", "0a0b"), &schema),
        Some(Reference::new("f").equal_to(Datum::fixed([0x0a, 0x0b])))
    );
    assert_eq!(
        translate_as_created(
            &equal_to_bytes("U", "f79c3e09677c4bbda4793f349cb785e7"),
            &schema
        ),
        None,
        "a uuid literal constrains nothing, because stored uuid bounds follow no one order"
    );
    assert_eq!(
        translate_as_created(&equal_to_bytes("F", "0a"), &schema),
        None,
        "a literal of another width than the fixed column's constrains nothing"
    );
    assert_eq!(
        translate_as_created(&predicate_equal(col("B"), str_lit("00abff")), &schema),
        None,
        "only the retyped literal names bytes"
    );
}
