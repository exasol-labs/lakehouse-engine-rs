use exasol_udf_sdk::value::{ColumnInfo, ExaType};

/// `size`, `precision` and `scale` derive from `typ`'s payload so the fixture cannot
/// describe a declaration the database would never report.
pub(in crate::scan) fn declared(types: &[(&str, ExaType)]) -> Vec<ColumnInfo> {
    types
        .iter()
        .map(|(name, typ)| ColumnInfo {
            name: (*name).to_string(),
            type_name: format!("{typ:?}"),
            size: declared_size(typ),
            precision: declared_precision(typ),
            scale: declared_scale(typ),
            typ: typ.clone(),
        })
        .collect()
}

pub(in crate::scan) fn varchar() -> ExaType {
    ExaType::String { size: 2_000_000 }
}

/// A `DECIMAL(precision, scale)` the database binned to NUMERIC.
pub(in crate::scan) fn numeric(precision: u32, scale: u32) -> ExaType {
    ExaType::Numeric { precision, scale }
}

fn declared_size(typ: &ExaType) -> Option<u32> {
    match typ {
        ExaType::String { size } | ExaType::Char { size } => Some(*size),
        _ => None,
    }
}

fn declared_precision(typ: &ExaType) -> Option<u32> {
    match typ {
        ExaType::Numeric { precision, .. } => Some(*precision),
        _ => None,
    }
}

fn declared_scale(typ: &ExaType) -> Option<u32> {
    match typ {
        ExaType::Numeric { scale, .. } => Some(*scale),
        _ => None,
    }
}
