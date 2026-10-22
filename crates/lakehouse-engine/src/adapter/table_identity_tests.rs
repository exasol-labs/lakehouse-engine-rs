use super::*;
use crate::adapter::pushdown::test_support::{assert_mentions, user_message};

fn request_with_note(table: &str, note: Json) -> Json {
    json!({"involvedTables": [{"name": table, "adapterNotes": note}]})
}

#[test]
fn the_encoded_note_round_trips_through_the_reader() {
    let ident = CatalogTableIdent {
        namespace: vec!["prod".into(), "finance".into()],
        name: "Straße".into(),
    };
    let request = request_with_note("STRASSE", Json::String(encode_identifier_note(&ident)));
    assert_eq!(
        read_identifier(&request, "STRASSE").unwrap(),
        "prod.finance.Straße"
    );
}

/// Scenario: Pushdown derives the scanned Iceberg table from the involved virtual table
#[test]
fn a_missing_or_unreadable_note_fails_naming_the_table_and_refresh() {
    let unreadable = [
        json!({"involvedTables": [{"name": "ORDERS"}]}),
        json!({"involvedTables": []}),
        json!({}),
        request_with_note("ORDERS", Json::Null),
        request_with_note("ORDERS", json!("")),
        request_with_note("ORDERS", json!("not json")),
        request_with_note("ORDERS", json!("null")),
        request_with_note("ORDERS", json!("[\"a.b\"]")),
        request_with_note("ORDERS", json!("\"a.b\"")),
        request_with_note("ORDERS", json!("{}")),
        request_with_note("ORDERS", json!("{\"name\": 7}")),
        request_with_note("ORDERS", json!({"name": "a.b"})),
    ];
    for request in unreadable {
        let message = user_message(read_identifier(&request, "ORDERS").unwrap_err());
        assert_mentions(&message, &["ORDERS", "ALTER VIRTUAL SCHEMA ... REFRESH"]);
    }
}

/// Scenario: Pushdown derives the scanned Iceberg table from the involved virtual table
#[test]
fn members_other_than_name_are_ignored() {
    let note = json!({"name": "lh.orders", "stamp": "1", "nested": {"name": "other"}});
    let request = request_with_note("ORDERS", Json::String(note.to_string()));
    assert_eq!(read_identifier(&request, "ORDERS").unwrap(), "lh.orders");
}

#[test]
fn the_entry_is_chosen_by_name_not_by_position() {
    let request = json!({"involvedTables": [
        {"name": "CUSTOMER", "adapterNotes": "{\"name\": \"lh.customer\"}"},
        {"name": "ORDERS", "adapterNotes": "{\"name\": \"lh.orders\"}"},
    ]});
    assert_eq!(read_identifier(&request, "ORDERS").unwrap(), "lh.orders");
    assert_eq!(
        read_identifier(&request, "CUSTOMER").unwrap(),
        "lh.customer"
    );
}
