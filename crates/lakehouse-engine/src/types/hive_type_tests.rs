use super::*;
use crate::tests::hive_type_cases::{
    UNRECOGNIZED_HIVE_TYPES, binary_hive_types, decimal, nested_hive_types, primitive_hive_types,
    struct_of,
};
use delta_kernel::schema::{ArrayType, MapType, StructField};

/// Scenario: Every Hive primitive type maps to its Spark type
/// Scenario: Nested Hive types parse recursively and render as JSON text
#[test]
fn every_hive_type_parses_to_its_spark_type_with_nullable_members() {
    let primitives = primitive_hive_types().map(|(hive_type, spark, _)| (hive_type, spark));
    let spelled_alike = [
        ("BIGINT", DataType::LONG),
        ("  BigInt  ", DataType::LONG),
        ("VarChar ( 1 )", DataType::STRING),
        ("decimal(12)", decimal(12, 0)),
        (
            "MAP < STRING , ARRAY < BIGINT > >",
            DataType::from(MapType::new(
                DataType::STRING,
                DataType::from(ArrayType::new(DataType::LONG, true)),
                true,
            )),
        ),
        (
            "struct< OrderId : bigint >",
            struct_of(vec![StructField::nullable("OrderId", DataType::LONG)]),
        ),
    ];

    for (hive_type, expected) in primitives
        .into_iter()
        .chain(nested_hive_types())
        .chain(binary_hive_types())
        .chain(spelled_alike)
    {
        assert_eq!(
            parse_hive_type(hive_type),
            Ok(expected),
            "hive type {hive_type:?}"
        );
    }
}

#[test]
fn unrecognized_and_malformed_hive_types_fail_quoting_the_type_string() {
    let malformed = [
        "   ",
        "array<int",
        "array<>",
        "struct<>",
        "struct<x int>",
        "struct<:int>",
        "struct<x:int,x:string>",
        "varchar",
        "varchar(x)",
        "decimal(0,0)",
        "decimal(5,10)",
        "decimal(39,0)",
        "decimal(300,0)",
        "decimal(10,2,1)",
        "int(10)",
        "int foo",
        "array<int>>",
    ];
    for hive_type in UNRECOGNIZED_HIVE_TYPES.into_iter().chain(malformed) {
        let error = parse_hive_type(hive_type).expect_err(&format!("{hive_type:?} must not parse"));
        assert!(
            error.contains(&format!("Hive type '{hive_type}'")),
            "the error for {hive_type:?} must quote the type string: {error}"
        );
    }
}

fn nested_arrays(levels: usize) -> String {
    format!("{}int{}", "array<".repeat(levels), ">".repeat(levels))
}

#[test]
fn nesting_up_to_the_limit_parses_and_deeper_nesting_fails_without_exhausting_the_stack() {
    assert!(parse_hive_type(&nested_arrays(MAX_NESTING_DEPTH)).is_ok());

    let too_deep = nested_arrays(MAX_NESTING_DEPTH + 1);
    let error = parse_hive_type(&too_deep).expect_err("deeper nesting is refused");
    assert!(error.contains("nests deeper than"), "{error}");

    let hostile = "array<".repeat(100_000);
    assert!(parse_hive_type(&hostile).is_err());
}
