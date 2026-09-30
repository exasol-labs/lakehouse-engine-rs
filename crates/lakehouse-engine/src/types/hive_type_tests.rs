use super::*;
use delta_kernel::schema::{ArrayType, MapType, StructField, StructType};

fn decimal(precision: u8, scale: u8) -> DataType {
    DataType::decimal(precision, scale).expect("a valid decimal")
}

fn struct_of(fields: Vec<StructField>) -> DataType {
    DataType::from(StructType::try_new(fields).expect("a valid struct"))
}

/// Scenario: Every Hive primitive type maps to its Spark type
#[test]
fn every_hive_primitive_parses_to_its_spark_type() {
    let cases = [
        ("tinyint", DataType::BYTE),
        ("smallint", DataType::SHORT),
        ("int", DataType::INTEGER),
        ("integer", DataType::INTEGER),
        ("bigint", DataType::LONG),
        ("float", DataType::FLOAT),
        ("double", DataType::DOUBLE),
        ("boolean", DataType::BOOLEAN),
        ("string", DataType::STRING),
        ("varchar(10)", DataType::STRING),
        ("char(5)", DataType::STRING),
        ("date", DataType::DATE),
        ("timestamp", DataType::TIMESTAMP_NTZ),
        ("decimal", decimal(10, 0)),
        ("decimal(10,2)", decimal(10, 2)),
        ("DECIMAL( 38 , 10 )", decimal(38, 10)),
    ];

    for (hive_type, expected) in cases {
        assert_eq!(
            parse_hive_type(hive_type),
            Ok(expected),
            "hive type {hive_type:?}"
        );
    }
}

#[test]
fn letter_case_and_surrounding_whitespace_are_ignored() {
    for hive_type in ["BIGINT", "  BigInt  ", "bigint"] {
        assert_eq!(
            parse_hive_type(hive_type),
            Ok(DataType::LONG),
            "{hive_type:?}"
        );
    }
    assert_eq!(parse_hive_type("VarChar ( 1 )"), Ok(DataType::STRING));
}

#[test]
fn a_decimal_precision_without_a_scale_takes_scale_zero() {
    assert_eq!(parse_hive_type("decimal(12)"), Ok(decimal(12, 0)));
}

/// Scenario: Nested Hive types parse recursively and render as JSON text
#[test]
fn nested_hive_types_parse_recursively_with_nullable_members() {
    let cases = [
        (
            "array<int>",
            DataType::from(ArrayType::new(DataType::INTEGER, true)),
        ),
        (
            "map<varchar(1),int>",
            DataType::from(MapType::new(DataType::STRING, DataType::INTEGER, true)),
        ),
        (
            "struct<x:int,y:string>",
            struct_of(vec![
                StructField::nullable("x", DataType::INTEGER),
                StructField::nullable("y", DataType::STRING),
            ]),
        ),
        (
            "array<struct<a:decimal(5,2)>>",
            DataType::from(ArrayType::new(
                struct_of(vec![StructField::nullable("a", decimal(5, 2))]),
                true,
            )),
        ),
        (
            "MAP < STRING , ARRAY < BIGINT > >",
            DataType::from(MapType::new(
                DataType::STRING,
                DataType::from(ArrayType::new(DataType::LONG, true)),
                true,
            )),
        ),
    ];

    for (hive_type, expected) in cases {
        assert_eq!(
            parse_hive_type(hive_type),
            Ok(expected),
            "hive type {hive_type:?}"
        );
    }
}

#[test]
fn struct_member_names_keep_their_case_and_drop_surrounding_whitespace() {
    assert_eq!(
        parse_hive_type("struct< OrderId : bigint >"),
        Ok(struct_of(vec![StructField::nullable(
            "OrderId",
            DataType::LONG
        )]))
    );
}

#[test]
fn binary_types_parse_so_the_classifier_refuses_them_as_binary() {
    assert_eq!(parse_hive_type("binary"), Ok(DataType::BINARY));
    assert_eq!(
        parse_hive_type("struct<b:binary>"),
        Ok(struct_of(vec![StructField::nullable(
            "b",
            DataType::BINARY
        )]))
    );
}

#[test]
fn unrecognized_and_malformed_hive_types_fail_quoting_the_type_string() {
    for hive_type in [
        "uniontype<int,string>",
        "interval_day_time",
        "map<int>",
        "",
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
    ] {
        let error = parse_hive_type(hive_type).expect_err(&format!("{hive_type:?} must not parse"));
        assert!(
            error.contains(&format!("'{hive_type}'")),
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
