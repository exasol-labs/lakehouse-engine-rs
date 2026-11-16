//! Builds UDF column metadata (`ColumnInfo`) as the Rust SLC reported it for an input column on
//! `exasol/docker-db:2025.1.16`, from the measured `type_name` spelling.

use exasol_udf_sdk::value::{ColumnInfo, ExaType};

/// `type_name` is a measured spelling, such as `DECIMAL(9,0)`, `VARCHAR(20) UTF8`, or
/// `TIMESTAMP(3)`. Panics on a spelling the measurement did not report.
pub fn measured_column(type_name: &str) -> ColumnInfo {
    let (typ, size, precision, scale) = measured_shape(type_name);
    ColumnInfo {
        name: "0".to_string(),
        typ,
        type_name: type_name.to_string(),
        size: Some(size),
        precision: Some(precision),
        scale: Some(scale),
    }
}

fn measured_shape(type_name: &str) -> (ExaType, u32, u32, u32) {
    if let Some(args) = arguments(type_name, "DECIMAL(") {
        let (precision, scale) = args.split_once(',').expect("DECIMAL(p,s)");
        let (precision, scale): (u32, u32) = (precision.parse().unwrap(), scale.parse().unwrap());
        let size = match precision {
            0..=9 => 4,
            10..=18 => 8,
            _ => 16,
        };
        let typ = match (precision, scale) {
            (0..=9, 0) => ExaType::Int32,
            (10..=18, 0) => ExaType::Int64,
            _ => ExaType::Numeric { precision, scale },
        };
        return (typ, size, precision, scale);
    }
    if let Some(args) = arguments(type_name, "TIMESTAMP(") {
        let precision = args.parse().unwrap();
        return (ExaType::Timestamp { precision }, 0, precision, 0);
    }
    if let Some(args) = arguments(type_name, "VARCHAR(") {
        let size = args.parse().unwrap();
        return (ExaType::String { size }, size, 0, 0);
    }
    if let Some(args) = arguments(type_name, "CHAR(") {
        let size = args.parse().unwrap();
        return (ExaType::Char { size }, size, 0, 0);
    }
    let typ = match type_name {
        "DOUBLE" => ExaType::Double,
        "BOOLEAN" => ExaType::Boolean,
        "DATE" => ExaType::Date,
        unmeasured => panic!("no measured column metadata for {unmeasured}"),
    };
    (typ, 0, 0, 0)
}

fn arguments<'a>(type_name: &'a str, prefix: &str) -> Option<&'a str> {
    let rest = type_name.strip_prefix(prefix)?;
    Some(&rest[..rest.find(')')?])
}
