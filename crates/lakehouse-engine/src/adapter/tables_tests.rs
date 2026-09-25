use super::*;
use lakehouse_catalog::CatalogTableIdent;

fn make_ident(ns: Vec<&str>, table: &str) -> CatalogTableIdent {
    CatalogTableIdent {
        namespace: ns.into_iter().map(|s| s.to_string()).collect(),
        name: table.to_string(),
    }
}

#[test]
fn flatten_single_level_namespace() {
    let configured = vec!["prod".to_string()];
    let ident = make_ident(vec!["prod"], "orders");
    assert_eq!(flatten_table_name(&configured, &ident), "ORDERS");
}

#[test]
fn flatten_single_level_with_descendant() {
    let configured = vec!["prod".to_string()];
    let ident = make_ident(vec!["prod", "finance"], "orders");
    assert_eq!(flatten_table_name(&configured, &ident), "FINANCE__ORDERS");
}

#[test]
fn flatten_multilevel_namespace_direct_table() {
    let configured = vec!["prod".to_string(), "finance".to_string()];
    let ident = make_ident(vec!["prod", "finance"], "orders");
    assert_eq!(flatten_table_name(&configured, &ident), "ORDERS");
}

#[test]
fn flatten_multilevel_namespace_descendant() {
    let configured = vec!["prod".to_string(), "finance".to_string()];
    let ident = make_ident(vec!["prod", "finance", "eu"], "orders");
    assert_eq!(flatten_table_name(&configured, &ident), "EU__ORDERS");
}

#[test]
fn flatten_multilevel_namespace_deep_descendant() {
    let configured = vec!["prod".to_string(), "finance".to_string()];
    let ident = make_ident(vec!["prod", "finance", "eu", "west"], "orders");
    assert_eq!(flatten_table_name(&configured, &ident), "EU__WEST__ORDERS");
}

#[test]
fn flatten_produces_uppercase_from_lowercase_input() {
    let configured = vec!["prod".to_string(), "finance".to_string()];
    let ident = make_ident(vec!["prod", "finance", "eu"], "orders");
    let result = flatten_table_name(&configured, &ident);
    assert_eq!(result, result.to_uppercase(), "result must be uppercase");
    assert_eq!(result, "EU__ORDERS");
}

#[test]
fn flatten_uppercases_mixed_case_input() {
    let configured = vec!["Prod".to_string(), "Finance".to_string()];
    let ident = make_ident(vec!["Prod", "Finance", "Eu"], "Orders");
    assert_eq!(flatten_table_name(&configured, &ident), "EU__ORDERS");
}

#[test]
fn identifier_string_single_level() {
    let ident = make_ident(vec!["prod"], "orders");
    assert_eq!(catalog_identifier_string(&ident), "prod.orders");
}

#[test]
fn identifier_string_multilevel() {
    let ident = make_ident(vec!["prod", "finance", "eu"], "orders");
    assert_eq!(catalog_identifier_string(&ident), "prod.finance.eu.orders");
}

#[test]
fn identifier_string_preserves_lowercase_casing() {
    let ident = make_ident(vec!["prod", "finance"], "orders");
    let s = catalog_identifier_string(&ident);
    assert_eq!(s, "prod.finance.orders");
    assert!(
        s.chars().all(|c| !c.is_uppercase()),
        "must preserve lowercase: {s}"
    );
}

#[test]
fn identifier_string_preserves_mixed_case() {
    let ident = make_ident(vec!["Prod", "Finance"], "Orders");
    assert_eq!(catalog_identifier_string(&ident), "Prod.Finance.Orders");
}
