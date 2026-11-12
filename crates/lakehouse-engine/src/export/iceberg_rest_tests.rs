use super::*;
use lakehouse_catalog::ConnectionCreds;

fn connection() -> ExportConnection {
    ExportConnection {
        address: "https://catalog.example".into(),
        creds: ConnectionCreds::default(),
        allow_http: false,
    }
}

#[test]
fn a_table_name_without_a_namespace_is_refused_naming_it() {
    let refused = target(connection(), "orders").err().expect("a refusal");

    assert!(
        refused.to_string().contains("TABLE_NAME = 'orders'"),
        "{refused}"
    );
}

#[test]
fn a_table_name_with_a_namespace_of_several_levels_opens_a_target() {
    assert!(target(connection(), "sales.eu.orders").is_ok());
}
