use super::*;
use arrow::datatypes::{DataType, Field, TimeUnit};
use iceberg::spec::{PrimitiveType, Type};

fn nullable(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, true)
}

#[test]
fn each_arrow_type_of_the_export_yields_its_iceberg_primitive_type() {
    let cases = [
        (
            DataType::Decimal128(18, 2),
            PrimitiveType::Decimal {
                precision: 18,
                scale: 2,
            },
        ),
        (DataType::Float64, PrimitiveType::Double),
        (DataType::Boolean, PrimitiveType::Boolean),
        (DataType::Utf8, PrimitiveType::String),
        (DataType::Date32, PrimitiveType::Date),
        (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            PrimitiveType::Timestamp,
        ),
    ];
    for (arrow_type, expected) in cases {
        let schema = iceberg_schema(&Schema::new(vec![nullable("C", arrow_type.clone())]))
            .unwrap_or_else(|e| panic!("{arrow_type:?}: {e}"));
        let field = schema.field_by_name("C").expect("the column");
        assert_eq!(
            *field.field_type,
            Type::Primitive(expected),
            "{arrow_type:?}"
        );
        assert!(!field.required, "{arrow_type:?} must be optional");
    }
}

#[test]
fn field_ids_run_from_1_in_input_order() {
    let arrow = Schema::new(vec![
        nullable("B", DataType::Boolean),
        nullable("A", DataType::Date32),
        nullable("a.b", DataType::Float64),
    ]);

    let schema = iceberg_schema(&arrow).expect("primitive columns convert");

    let fields: Vec<(i32, &str)> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.id, field.name.as_str()))
        .collect();
    assert_eq!(fields, [(1, "B"), (2, "A"), (3, "a.b")]);
}
