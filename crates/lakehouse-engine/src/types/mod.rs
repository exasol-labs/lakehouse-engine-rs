/// `vs-expression` owns the Exasol SQL type, because both CAST dialects render it and
/// `vs-expression` cannot depend on this crate.
pub use vs_expression::exasol_type;
pub(crate) mod hive_type;
pub mod mapping;
pub mod widening;
