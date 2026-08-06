use super::*;
use iceberg::{NamespaceIdent, TableIdent};

fn make_ident(ns: Vec<&str>, table: &str) -> TableIdent {
    TableIdent::new(
        NamespaceIdent::from_vec(ns.into_iter().map(|s| s.to_string()).collect()).unwrap(),
        table.to_string(),
    )
}

// ---------------------------------------------------------------------------
// flatten_table_name — single-level namespace
// ---------------------------------------------------------------------------

/// Single-level configured namespace, table directly in that namespace.
/// The table name is uppercased; no sub-namespace prefix.
#[test]
fn flatten_single_level_namespace() {
    let configured = vec!["prod".to_string()];
    let ident = make_ident(vec!["prod"], "orders");
    assert_eq!(flatten_table_name(&configured, &ident), "ORDERS");
}

/// Single-level configured namespace, table in a child namespace.
/// The child namespace segment is prepended with `__`.
#[test]
fn flatten_single_level_with_descendant() {
    let configured = vec!["prod".to_string()];
    let ident = make_ident(vec!["prod", "finance"], "orders");
    assert_eq!(flatten_table_name(&configured, &ident), "FINANCE__ORDERS");
}

// ---------------------------------------------------------------------------
// flatten_table_name — multi-level namespace
// ---------------------------------------------------------------------------

/// Multi-level configured namespace, table directly in that namespace.
/// No sub-namespace segments to prepend → table name only (uppercased).
#[test]
fn flatten_multilevel_namespace_direct_table() {
    let configured = vec!["prod".to_string(), "finance".to_string()];
    let ident = make_ident(vec!["prod", "finance"], "orders");
    assert_eq!(flatten_table_name(&configured, &ident), "ORDERS");
}

/// Multi-level configured namespace, table in a descendant namespace.
/// The single sub-namespace segment is prepended.
#[test]
fn flatten_multilevel_namespace_descendant() {
    let configured = vec!["prod".to_string(), "finance".to_string()];
    let ident = make_ident(vec!["prod", "finance", "eu"], "orders");
    assert_eq!(flatten_table_name(&configured, &ident), "EU__ORDERS");
}

/// Two levels of descendant below the configured namespace.
#[test]
fn flatten_multilevel_namespace_deep_descendant() {
    let configured = vec!["prod".to_string(), "finance".to_string()];
    let ident = make_ident(vec!["prod", "finance", "eu", "west"], "orders");
    assert_eq!(flatten_table_name(&configured, &ident), "EU__WEST__ORDERS");
}

// ---------------------------------------------------------------------------
// flatten_table_name — casing
// ---------------------------------------------------------------------------

/// Lowercase Iceberg names are uppercased in the Exasol name.
#[test]
fn flatten_produces_uppercase_from_lowercase_input() {
    let configured = vec!["prod".to_string(), "finance".to_string()];
    let ident = make_ident(vec!["prod", "finance", "eu"], "orders");
    let result = flatten_table_name(&configured, &ident);
    // Must be fully uppercase
    assert_eq!(result, result.to_uppercase(), "result must be uppercase");
    assert_eq!(result, "EU__ORDERS");
}

/// Mixed-case Iceberg names are uppercased.
#[test]
fn flatten_uppercases_mixed_case_input() {
    let configured = vec!["Prod".to_string(), "Finance".to_string()];
    let ident = make_ident(vec!["Prod", "Finance", "Eu"], "Orders");
    assert_eq!(flatten_table_name(&configured, &ident), "EU__ORDERS");
}

// ---------------------------------------------------------------------------
// iceberg_identifier_string — preserves original casing
// ---------------------------------------------------------------------------

/// Simple single-level namespace: identifier string is "ns.table".
#[test]
fn identifier_string_single_level() {
    let ident = make_ident(vec!["prod"], "orders");
    assert_eq!(iceberg_identifier_string(&ident), "prod.orders");
}

/// Multi-level namespace: all segments + table joined with `.`.
#[test]
fn identifier_string_multilevel() {
    let ident = make_ident(vec!["prod", "finance", "eu"], "orders");
    assert_eq!(iceberg_identifier_string(&ident), "prod.finance.eu.orders");
}

/// Identifier string preserves original lowercase casing (Iceberg names are case-sensitive).
#[test]
fn identifier_string_preserves_lowercase_casing() {
    let ident = make_ident(vec!["prod", "finance"], "orders");
    let s = iceberg_identifier_string(&ident);
    // Must be exactly the original casing, not uppercased.
    assert_eq!(s, "prod.finance.orders");
    assert!(
        s.chars().all(|c| !c.is_uppercase()),
        "must preserve lowercase: {s}"
    );
}

/// Identifier string preserves mixed-case casing.
#[test]
fn identifier_string_preserves_mixed_case() {
    let ident = make_ident(vec!["Prod", "Finance"], "Orders");
    assert_eq!(iceberg_identifier_string(&ident), "Prod.Finance.Orders");
}
