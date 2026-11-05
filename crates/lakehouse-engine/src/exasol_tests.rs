use super::*;

#[test]
fn group_count_scales_with_nodes_and_caps_at_300() {
    assert_eq!(group_count(10, 50, usize::MAX), 300);
    assert_eq!(group_count(1, 300, usize::MAX), 300);
    assert_eq!(group_count(1, 301, usize::MAX), 300);
    assert_eq!(group_count(3, 4, usize::MAX), 12);
    assert_eq!(group_count(usize::MAX, 2, usize::MAX), 300);
}

#[test]
fn quoting_doubles_the_embedded_quote() {
    assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    assert_eq!(sql_string_literal("it's"), "'it''s'");
}

#[test]
fn connection_name_must_be_non_empty() {
    assert_eq!(catalog_connection_name(Some("C")), Some("C"));
    assert_eq!(catalog_connection_name(Some("")), None);
    assert_eq!(catalog_connection_name(None), None);
}

#[test]
fn booleans_ignore_letter_case() {
    assert_eq!(parse_boolean("True"), Some(true));
    assert_eq!(parse_boolean("fALSE"), Some(false));
    assert_eq!(parse_boolean("yes"), None);
}

#[test]
fn whole_numbers_take_digits_within_the_range_only() {
    assert_eq!(parse_whole_number::<NonZeroU8>("4").map(|n| n.get()), Ok(4));
    for bad in ["0", "-1", "+3", " 3", "1.5", "", "256", "four"] {
        assert!(parse_whole_number::<NonZeroU8>(bad).is_err(), "{bad:?}");
    }
}
