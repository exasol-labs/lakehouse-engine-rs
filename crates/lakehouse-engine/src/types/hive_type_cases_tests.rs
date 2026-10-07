use delta_kernel::schema::{ArrayType, DataType, MapType, StructField, StructType};

pub(crate) const JSON_TEXT: &str = "VARCHAR(2000000)";

pub(crate) fn decimal(precision: u8, scale: u8) -> DataType {
    DataType::decimal(precision, scale).expect("a valid decimal")
}

pub(crate) fn struct_of(fields: Vec<StructField>) -> DataType {
    DataType::from(StructType::try_new(fields).expect("a valid struct"))
}

/// The primitive Glue columns of `glue/glue-hive-type-mapping`: each Hive type, its Spark
/// type, and the Exasol type the listing declares on an engine that declares timestamp precision.
pub(crate) fn primitive_hive_types() -> [(&'static str, DataType, &'static str); 16] {
    [
        ("tinyint", DataType::BYTE, "DECIMAL(3,0)"),
        ("smallint", DataType::SHORT, "DECIMAL(5,0)"),
        ("int", DataType::INTEGER, "DECIMAL(10,0)"),
        ("integer", DataType::INTEGER, "DECIMAL(10,0)"),
        ("bigint", DataType::LONG, "DECIMAL(20,0)"),
        ("float", DataType::FLOAT, "DOUBLE PRECISION"),
        ("double", DataType::DOUBLE, "DOUBLE PRECISION"),
        ("boolean", DataType::BOOLEAN, "BOOLEAN"),
        ("string", DataType::STRING, JSON_TEXT),
        ("varchar(10)", DataType::STRING, JSON_TEXT),
        ("char(5)", DataType::STRING, JSON_TEXT),
        ("date", DataType::DATE, "DATE"),
        ("timestamp", DataType::TIMESTAMP_NTZ, "TIMESTAMP(6)"),
        ("decimal", decimal(10, 0), "DECIMAL(10,0)"),
        ("decimal(10,2)", decimal(10, 2), "DECIMAL(10,2)"),
        ("DECIMAL( 38 , 10 )", decimal(38, 10), JSON_TEXT),
    ]
}

/// The nested Glue columns of `glue/glue-hive-type-mapping`, each declared as JSON text.
pub(crate) fn nested_hive_types() -> [(&'static str, DataType); 4] {
    [
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
    ]
}

/// These parse; the classifier refuses them per `vs-adapter/binary-column-refusal`.
pub(crate) fn binary_hive_types() -> [(&'static str, DataType); 2] {
    [
        ("binary", DataType::BINARY),
        (
            "struct<b:binary>",
            struct_of(vec![StructField::nullable("b", DataType::BINARY)]),
        ),
    ]
}

pub(crate) const UNRECOGNIZED_HIVE_TYPES: [&str; 4] =
    ["uniontype<int,string>", "interval_day_time", "map<int>", ""];
