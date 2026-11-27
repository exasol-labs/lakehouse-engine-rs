use super::*;
use crate::export::target;
use crate::export::test_support::{CONNECTION, static_key_connection};

fn connection() -> ExportConnection {
    static_key_connection("https://catalog.example", false)
}

#[test]
fn a_table_name_without_a_namespace_is_refused_naming_it() {
    let refused = target::of(connection(), "orders").err().expect("a refusal");

    assert!(
        refused.to_string().contains("TABLE_NAME = 'orders'"),
        "{refused}"
    );
}

#[test]
fn a_table_name_with_a_namespace_of_several_levels_opens_a_target() {
    assert!(target::of(connection(), "sales.eu.orders").is_ok());
}

#[test]
fn an_unwritable_connection_is_refused_naming_the_connection_and_the_field() {
    let creds = connection().creds;
    let secret = creds.secret_key.clone();
    let cases = [
        (
            "no warehouse",
            ConnectionCreds {
                token: Some("t".into()),
                ..Default::default()
            },
            vec!["CONNECTION 'LAKEHOUSE_CATALOG_CREDS'", "warehouse"],
        ),
        (
            "SigV4 signing",
            ConnectionCreds {
                use_sigv4: true,
                ..creds
            },
            vec!["CONNECTION 'LAKEHOUSE_CATALOG_CREDS'", "use_sigv4", "SigV4"],
        ),
    ];

    for (case, creds, fragments) in cases {
        let message = refuse_unwritable(CONNECTION, &creds)
            .expect_err(case)
            .to_string();
        assert!(
            message.starts_with("LAKEHOUSE_EXPORT: "),
            "{case}: {message}"
        );
        for fragment in fragments {
            assert!(
                message.contains(fragment),
                "{case}: {message} lacks {fragment}"
            );
        }
        assert!(!message.contains(&secret), "{case}: {message}");
    }
}

#[test]
fn an_unappendable_table_is_refused_naming_it_and_the_rule() {
    let table = TableIdent::from_strs(["ns", "t"]).unwrap();
    let cases = [
        (
            Refusal::FormatVersion(3),
            "table ns.t has format version 3; the export appends only to tables of format \
             version 1 or 2",
        ),
        (
            Refusal::FieldsNamedLikeColumns(vec!["c0 (void)".into()]),
            "table ns.t keeps the dropped partition field c0 (void) under the name of a column; \
             the export appends only to tables whose partition fields are not named like a column",
        ),
    ];

    for (refused, text) in cases {
        assert_eq!(
            unappendable(&table, refused).to_string(),
            format!("LAKEHOUSE_EXPORT: {text}")
        );
    }
}
