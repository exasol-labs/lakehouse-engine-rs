use super::*;
use crate::adapter::catalog_kind::CatalogKind;
use crate::adapter::pushdown::test_support::{sample_storage, statement_over};

fn built(value: u32) -> impl FnOnce() -> Result<u32, String> {
    move || Ok(value)
}

#[test]
fn only_identifiers_that_no_check_admitted_stay_unchecked() {
    let statement = statement_over(
        CatalogKind::IcebergRest,
        "http://catalog.example.com",
        sample_storage(),
    );

    statement.admit(vec!["db.checked".into(), "db.also_checked".into()]);

    assert_eq!(
        statement.unchecked(&["db.new", "db.checked", "db.other_new", "db.also_checked"]),
        ["db.new", "db.other_new"]
    );
    assert!(statement.is_admitted("db.checked"));
    assert!(!statement.is_admitted("db.new"));
}

#[test]
fn a_second_virtual_schema_name_misses_the_state() {
    let states = StatementStates::<u32>::new();

    let first_a = states.get_or_build(Some("A"), built(1)).unwrap();
    let second_a = states.get_or_build(Some("A"), built(2)).unwrap();
    let b = states.get_or_build(Some("B"), built(3)).unwrap();

    assert!(
        Arc::ptr_eq(&first_a, &second_a),
        "A's second lookup reuses A's state"
    );
    assert_eq!((*first_a, *second_a, *b), (1, 1, 3));
    assert!(!Arc::ptr_eq(&first_a, &b), "B builds a state of its own");
}

#[test]
fn a_request_without_a_schema_name_is_not_kept() {
    let states = StatementStates::<u32>::new();

    let first = states.get_or_build(None, built(1)).unwrap();
    let second = states.get_or_build(None, built(2)).unwrap();

    assert_eq!(
        (*first, *second),
        (1, 2),
        "each nameless lookup runs its own build"
    );
}

#[test]
fn a_failed_build_is_not_kept() {
    let states = StatementStates::<u32>::new();

    let failed = states.get_or_build(Some("A"), || Err("unreachable catalog".to_string()));
    let retried = states.get_or_build(Some("A"), built(5)).unwrap();

    assert_eq!(failed, Err("unreachable catalog".to_string()));
    assert_eq!(*retried, 5, "the lookup after a failed build builds again");
}
