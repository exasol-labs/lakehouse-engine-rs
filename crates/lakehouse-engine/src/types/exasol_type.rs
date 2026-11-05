//! `vs-expression` owns the Exasol SQL type, because both CAST dialects render it and
//! `vs-expression` cannot depend on this crate.
pub use vs_expression::exasol_type::{
    Decimal, ExasolType, MAX_DECIMAL_PRECISION, MAX_VARCHAR_SIZE, exasol_type_from_json,
    exasol_type_to_json,
};
