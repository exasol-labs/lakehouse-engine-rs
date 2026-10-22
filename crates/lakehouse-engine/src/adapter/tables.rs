//! `flatten_table_name` and `catalog_identifier_string` must agree so the round-trip
//! `flatten → store → look up → parse` is deterministic at any namespace depth.

use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::CatalogTableIdent;
pub use lakehouse_catalog::catalog_identifier_string;
use std::collections::HashMap;

pub fn flatten_table_name(configured_ns: &[String], ident: &CatalogTableIdent) -> String {
    let ident_ns: &[String] = &ident.namespace;

    let sub_ns = if ident_ns.starts_with(configured_ns) {
        &ident_ns[configured_ns.len()..]
    } else {
        ident_ns
    };

    let mut parts: Vec<&str> = sub_ns.iter().map(|s| s.as_str()).collect();
    parts.push(&ident.name);
    parts.join("__").to_uppercase()
}

/// Fails when two identifiers flatten to one Exasol table name, because only one virtual table
/// could carry that name.
pub fn reject_name_collisions<'a>(
    configured_ns: &[String],
    idents: impl IntoIterator<Item = &'a CatalogTableIdent>,
) -> Result<(), UdfError> {
    let mut seen: HashMap<String, String> = HashMap::new();
    for ident in idents {
        let exasol_name = flatten_table_name(configured_ns, ident);
        let catalog_id = catalog_identifier_string(ident);
        if let Some(existing) = seen.get(&exasol_name) {
            return Err(UdfError::User(format!(
                "table name collision: '{exasol_name}' maps to both '{existing}' and '{catalog_id}'"
            )));
        }
        seen.insert(exasol_name, catalog_id);
    }
    Ok(())
}

#[cfg(test)]
#[path = "tables_tests.rs"]
mod tests;
