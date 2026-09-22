//! Declared-output-column fixtures shared by the `scan` module's sibling
//! `_tests.rs` files, so one `EMITS` declaration is modelled one way.

use exasol_udf_sdk::value::{ColumnInfo, ExaType};

/// The declared output columns a call site with this `EMITS` list produces.
///
/// `typ` is the authority the emit boundary reads; `size`, `precision` and
/// `scale` are populated from that variant's own payload so the fixture cannot
/// describe a declaration the database would never report, and `name` is the
/// caller's so a failing assertion names the column under test.
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

/// `VARCHAR(2000000)` as the database reports it.
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
