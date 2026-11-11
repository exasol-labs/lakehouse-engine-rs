use crate::exasol_type::ExasolType;

/// Rendered SQL with the Exasol type it evaluates to. `ty` is `None` unless the type is certain:
/// guessing Exasol's typing rules fails its positional type check or changes values (#476).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlFragment {
    pub sql: String,
    pub ty: Option<ExasolType>,
}

impl SqlFragment {
    pub fn new(sql: impl Into<String>, ty: Option<ExasolType>) -> Self {
        Self {
            sql: sql.into(),
            ty,
        }
    }

    /// Exasol SQL of type `declared`: the fragment itself when it already has that type, else a
    /// CAST. Exasol type-checks every pushdown output column against its declared type.
    pub fn as_type(&self, declared: ExasolType) -> String {
        if self.ty == Some(declared) {
            self.sql.clone()
        } else {
            format!("CAST({} AS {declared})", self.sql)
        }
    }
}

#[cfg(test)]
#[path = "fragment_tests.rs"]
mod tests;
