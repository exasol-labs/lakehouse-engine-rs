/// Shared table-identity helpers used by both createVirtualSchema and pushdown.
///
/// These are the single source of truth for:
/// - Flattening a catalog table identifier to an Exasol table name (`__`-joined, uppercased).
/// - Producing the original-cased, dot-joined fully-qualified identifier string stored in
///   `TABLE_MAP` and later parsed by `parse_table_ident`.
///
/// Both functions must agree so the round-trip `flatten → store → look up → parse` is
/// deterministic regardless of namespace depth.
use lakehouse_catalog::CatalogTableIdent;

/// Produce the Exasol table name for a discovered catalog table.
///
/// Takes the configured namespace as its dot-split segments (e.g. `["prod","finance"]`) and
/// a catalog table identifier discovered by recursing from that namespace.
///
/// The result is the namespace segments of `ident` BELOW the configured namespace, plus the
/// table name, all joined with `__` and uppercased.
///
/// Examples:
/// - configured `["prod","finance"]`, ident namespace `["prod","finance","eu"]` table `orders`
///   → `"EU__ORDERS"`
/// - configured `["prod","finance"]`, ident namespace `["prod","finance"]` table `orders`
///   → `"ORDERS"` (no sub-namespace segments to prepend)
///
/// Invariant: `ident`'s namespace always starts with the configured namespace segments
/// (it was discovered by recursing from there). If the prefix does not match (should not
/// happen in normal operation), the full namespace is used without stripping.
pub fn flatten_table_name(configured_ns: &[String], ident: &CatalogTableIdent) -> String {
    let ident_ns: &[String] = &ident.namespace;

    // Strip the configured namespace prefix from ident's namespace.
    let sub_ns = if ident_ns.starts_with(configured_ns) {
        &ident_ns[configured_ns.len()..]
    } else {
        ident_ns
    };

    // Collect: sub-namespace segments + table name, join with "__", uppercase.
    let mut parts: Vec<&str> = sub_ns.iter().map(|s| s.as_str()).collect();
    parts.push(&ident.name);
    parts.join("__").to_uppercase()
}

/// Produce the original-cased, dot-joined fully-qualified catalog identifier string.
///
/// This is the value stored in `TABLE_MAP` and later parsed back by `parse_table_ident`.
/// All namespace segments plus the table name are joined with `.`, preserving original casing.
///
/// Example: namespace `["prod","finance","eu"]`, table `orders` → `"prod.finance.eu.orders"`
pub fn catalog_identifier_string(ident: &CatalogTableIdent) -> String {
    let ns: &[String] = &ident.namespace;
    let mut parts: Vec<&str> = ns.iter().map(|s| s.as_str()).collect();
    parts.push(&ident.name);
    parts.join(".")
}

#[cfg(test)]
#[path = "tables_tests.rs"]
mod tests;
