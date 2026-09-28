//! `flatten_table_name` and `catalog_identifier_string` must agree so the round-trip
//! `flatten → store → look up → parse` is deterministic at any namespace depth.

use lakehouse_catalog::CatalogTableIdent;

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

/// The `TABLE_MAP` value, parsed back by `parse_table_ident`.
pub fn catalog_identifier_string(ident: &CatalogTableIdent) -> String {
    let ns: &[String] = &ident.namespace;
    let mut parts: Vec<&str> = ns.iter().map(|s| s.as_str()).collect();
    parts.push(&ident.name);
    parts.join(".")
}

#[cfg(test)]
#[path = "tables_tests.rs"]
mod tests;
