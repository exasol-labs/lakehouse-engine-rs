//! `flatten_table_name` and `catalog_identifier_string` must agree so the round-trip
//! `flatten → store → look up → parse` is deterministic at any namespace depth.

use lakehouse_catalog::CatalogTableIdent;
pub use lakehouse_catalog::catalog_identifier_string;

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

#[cfg(test)]
#[path = "tables_tests.rs"]
mod tests;
